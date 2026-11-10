// Package preflight answers one question before anything is written:
// 「现在按这个配置开跑，会发生什么、会不会卡住」。
//
// 每个结论都是四态之一（ok / autofixable / warn / blocked）。blocked 只在
// 物理上做不到时使用，并且必须同时给出替代方案 —— 绝不因为"有风险"就拒绝
// （docs/DESIGN.md：有问题就提示，不要阻断）。
package preflight

import (
	"fmt"
	typecat "github.com/spencerswagger/jinliang/internal/catalog"
	"sort"
	"strings"

	"github.com/spencerswagger/jinliang/internal/core"
	"github.com/spencerswagger/jinliang/internal/source"
)

// Input is everything the control plane already knows, handed over so this
// package stays free of storage and driver dependencies.
type Input struct {
	JobName      string
	SourceKind   string
	TargetKind   string
	SourceVer    string
	TargetVer    string
	SourceErr    string // 人话的连接错误；空表示已连上
	TargetErr    string
	SourceCaps   source.CapSet
	SourceCat    *source.Catalog // nil 表示没读到（不阻断）
	TargetCat    *source.Catalog // nil 表示没读到：目标表是否存在交给运行时判断
	Tables       []string        // 用户勾选的表
	Mode         string
	Incremental  string
	OnExists     string
	BatchRows    int
	SnapshotConn int
	Lookback     string
	TLSWarnings  []string
}

// Run produces the report the UI renders as cards.
func Run(in Input) source.Report {
	var checks []source.Check
	add := func(c source.Check) { checks = append(checks, c) }

	// ── 连通性 ───────────────────────────────────────────────
	checks = append(checks, connCheck("NET-01", "源库", in.SourceKind, in.SourceVer, in.SourceErr)...)
	checks = append(checks, connCheck("NET-02", "目标库", in.TargetKind, in.TargetVer, in.TargetErr)...)

	// ── 传输加密 ─────────────────────────────────────────────
	if len(in.TLSWarnings) > 0 {
		add(source.Check{
			ID: "TLS-01", Title: "连接没有强制校验证书",
			Status: source.StatusWarn,
			Detail: strings.Join(in.TLSWarnings, "；"),
			Who:    "you",
		})
	} else {
		add(source.Check{ID: "TLS-01", Title: "传输已加密且校验证书", Status: source.StatusOK, Detail: "证书校验通过"})
	}

	// ── 类型映射：异构目标端才有损，必须开跑前就看到 ─────────
	//
	// 这条之前不存在：`catalog.Map` 只有拦截器（engine）、两个 sink 内部和 verify 在用，
	// 预检链里一条都没有。于是 `timestamptz`（丢时区）、`money`、`interval → varchar`
	// 这类**有损**映射，用户的第一次知情是「跑完之后 verify 报差异」。
	// 「知情后可接受」的前提是用户真的被告知，所以这里把它摆到开跑前。
	if in.SourceCat != nil && in.TargetKind != "" && in.TargetKind != in.SourceKind {
		checks = append(checks, typeMappingChecks(in)...)
	}

	// ── 源端能力：能不能做实时增量 ───────────────────────────
	if cap, ok := in.SourceCaps[source.CapCDC]; ok {
		needsCDC := strings.Contains(in.Mode, "continuous")
		switch {
		case cap.Supported:
			add(source.Check{ID: "CAP-01", Title: "源库可以读变更流水", Status: source.StatusOK,
				Detail: "实时增量可用"})
		case needsCDC:
			// 缺变更流 = 这个模式物理上做不到（ERRORS.md: blocked 的定义）。
			// 但我们不阻断人：卡片里给两条路——开流水，或改成定时增量。
			detail := cap.Compensation.Note
			if detail == "" {
				detail = "源库没有开启变更流，实时增量读不到任何变更"
			}
			detail += "。改成「定时增量」（指定一个水位列，如 updated_at）不需要变更流，只要 SELECT 权限"
			add(source.Check{
				ID: "CAP-01", Title: "源库读不到变更流水",
				Status: source.StatusBlocked, Code: string(core.CodeWalLevel),
				Detail: detail,
				Who:    "dba",
				SQL:    cdcFixSQL(in.SourceKind),
			})
		default:
			add(source.Check{ID: "CAP-01", Title: "本次模式不需要变更流水", Status: source.StatusOK,
				Detail: "定时增量只需要 SELECT"})
		}
	}

	// ── 表范围 ───────────────────────────────────────────────
	checks = append(checks, scopeChecks(in)...)

	// ── 模式相关 ─────────────────────────────────────────────
	if in.Mode == "interval" {
		if strings.TrimSpace(in.Incremental) == "" {
			add(source.Check{ID: "MODE-01", Title: "定时增量缺水位列", Status: source.StatusBlocked,
				Code:   string(core.CodeViewNoStream),
				Detail: "frequency=interval 必须指定 incremental_column（例如 updated_at），否则我们不知道「哪些是新数据」",
				Who:    "you"})
		} else {
			missing := tablesWithoutColumn(in.SourceCat, in.Tables, in.Incremental)
			if len(missing) > 0 {
				add(source.Check{ID: "MODE-01", Title: "部分表没有这个水位列", Status: source.StatusWarn,
					Detail:   fmt.Sprintf("%s 里没有 %s：这些表只能整表重搬", strings.Join(missing, "、"), in.Incremental),
					Affected: missing})
			} else {
				add(source.Check{ID: "MODE-01", Title: "水位列存在", Status: source.StatusOK,
					Detail: fmt.Sprintf("选中的 %d 张表都有 %s", len(in.Tables), in.Incremental)})
			}
		}
	}

	// ── 目标端同名表 ─────────────────────────────────────────
	if in.OnExists == "error" {
		existing := existingTargetTables(in.TargetCat, in.Tables)
		if len(existing) > 0 {
			add(source.Check{
				ID: "TARGET-01", Title: "目标端已经有同名表",
				Status: source.StatusBlocked, Code: string(core.CodeTargetTableExists),
				Detail:   fmt.Sprintf("%d 张表已存在（%s）。当前 on_exists=error，会停下来等你选", len(existing), preview(existing)),
				Affected: existing, Who: "you",
			})
		} else if in.TargetCat != nil {
			add(source.Check{ID: "TARGET-01", Title: "目标端没有同名表", Status: source.StatusOK, Detail: "可以直接建表"})
		} else {
			add(source.Check{ID: "TARGET-01", Title: "没读到目标端的表清单", Status: source.StatusWarn,
				Detail: "目标端连上了但读不到表：开跑时会再检查一次，若同名表存在会停在这里"})
		}
	}

	// ── 按表暂停能力 ─────────────────────────────────────────
	// 只有 PG 目标端有 paused_tables 那条状态列。别的目标端遇到「结构对不上」时
	// 没法登记暂停——这条要**在 plan 里就说清**，别等用户在半夜撞上 E1015 才知道。
	switch in.TargetKind {
	case "mysql":
		// MySQL 目标端已同构落地（docs/CONTRACT-PAUSE-AND-POSITION.md §4.2/§4.5）：
		// `_jl_meta.paused_tables` 缺列时开跑会先补列（MySQL 没有 ADD COLUMN IF NOT EXISTS，
		// 先查 information_schema 再 ALTER）；补不上就**明确失败**，不会静默跳过。
		// **只说存储层做到了什么**：引擎侧的「只停这张表」还没接到 MySQL 目标端
		// （pause_skip / reconcilePaused 在 PG 专属的 prepare 里），所以这里不许替引擎许诺，
		// 否则用户会以为「一张表结构不对，别的表还在跑」——实际是整 run 失败。
		add(source.Check{
			ID: "PAUSE-01", Title: "目标端支持记录按表暂停", Status: source.StatusWarn,
			Detail: "存储层已就绪：`_jl_meta.paused_tables` 这一列在（老库缺列时开跑会先补，" +
				"补不上会明确失败并说明原因）。**引擎侧的「只停这张表」还没接**——" +
				"在那之前遇到表结构不一致仍是整 run 失败、位点不推进",
		})
	case "postgres":
		add(source.Check{ID: "PAUSE-01", Title: "目标端支持按表暂停", Status: source.StatusOK,
			// 口径（docs/CONTRACT-PAUSE-AND-POSITION.md §1.5/§1.6）：只停**这张表**，
			// 其它表照常提交、位点照常前进；跳过窗口记账，结构对齐后整表重搬补齐。
			Detail: "结构对不上的表会被记录并只停它自己：其它表照常同步、位点照常前进，" +
				"跳过的那段在结构对齐后由整表重搬补齐"})
	}

	// ── 批次与并发 ───────────────────────────────────────────
	if in.BatchRows < 1 || in.BatchRows > 50000 {
		add(source.Check{ID: "BATCH-01", Title: "每批行数超出范围", Status: source.StatusBlocked,
			Detail: fmt.Sprintf("batch_rows=%d，必须在 1–50000 之间", in.BatchRows), Who: "you"})
	} else {
		add(source.Check{ID: "BATCH-01", Title: "每批行数在安全范围", Status: source.StatusOK,
			Detail: fmt.Sprintf("batch_rows=%d", in.BatchRows)})
	}
	if in.SnapshotConn > 16 {
		add(source.Check{ID: "CONN-01", Title: "初始化并发太高", Status: source.StatusBlocked,
			Detail: fmt.Sprintf("snapshot.conns=%d，最多 16：再高会把源库压住", in.SnapshotConn), Who: "you"})
	}

	rep := source.Report{Checks: checks}
	rep.CanStart = true
	for _, c := range checks {
		if c.Status == source.StatusBlocked {
			rep.CanStart = false
		}
	}
	return rep
}

func connCheck(id, side, kind, version, errMsg string) []source.Check {
	if errMsg != "" {
		return []source.Check{{
			ID: id, Title: side + "连不上", Status: source.StatusBlocked,
			Code: string(core.CodeConnUnreachable), Detail: errMsg, Who: "you",
		}}
	}
	v := version
	if v == "" {
		v = "版本未知"
	}
	return []source.Check{{
		ID: id, Title: side + "可以连上", Status: source.StatusOK,
		Detail: fmt.Sprintf("%s %s", kindLabel(kind), v),
	}}
}

func kindLabel(kind string) string {
	switch kind {
	case "postgres":
		return "PostgreSQL"
	case "mysql":
		return "MySQL"
	}
	return kind
}

func cdcFixSQL(kind string) string {
	if kind == "mysql" {
		return "SET GLOBAL log_bin = ON; SET GLOBAL binlog_format = ROW;  -- 需要重启一次生效"
	}
	return "ALTER SYSTEM SET wal_level = 'logical';  -- 然后重启一次实例"
}

func scopeChecks(in Input) []source.Check {
	if in.SourceCat == nil {
		return []source.Check{{ID: "SCOPE-01", Title: "没读到源端的表清单", Status: source.StatusWarn,
			Detail: "连上了但列不出表：可能是权限不足，也可能 schema 写错了"}}
	}
	if len(in.Tables) == 0 {
		return []source.Check{{ID: "SCOPE-01", Title: "还没选表", Status: source.StatusWarn,
			Detail: "至少选一张表，否则这次同步什么都不会发生", Who: "you"}}
	}
	have := map[string]bool{}
	for _, t := range in.SourceCat.Tables {
		have[t.Table] = true
	}
	var missing, noPK []string
	pk := map[string]bool{}
	for _, t := range in.SourceCat.Tables {
		pk[t.Table] = t.HasStablePK
	}
	for _, name := range in.Tables {
		if !have[name] {
			missing = append(missing, name)
			continue
		}
		if !pk[name] && in.Mode != "once" && in.Mode != "full_interval" {
			noPK = append(noPK, name)
		}
	}
	var out []source.Check
	if len(missing) > 0 {
		sort.Strings(missing)
		out = append(out, source.Check{
			ID: "SCOPE-01", Title: "有表在源端不存在", Status: source.StatusBlocked,
			Detail:   fmt.Sprintf("%s 找不到（可能改名了，或不在 %s 下）", strings.Join(missing, "、"), in.SourceCat.Tables[0].Schema),
			Affected: missing, Who: "you",
		})
	} else {
		out = append(out, source.Check{ID: "SCOPE-01", Title: "选中的表都在源端",
			Status: source.StatusOK, Detail: fmt.Sprintf("%d 张表", len(in.Tables))})
	}
	if len(noPK) > 0 {
		sort.Strings(noPK)
		out = append(out, source.Check{
			ID: "SCOPE-02", Title: "部分表没有主键", Status: source.StatusWarn,
			Detail:   fmt.Sprintf("%s 没有主键：变更只能追加不能改，重复跑会插重复行", strings.Join(noPK, "、")),
			Affected: noPK, Who: "you",
		})
	} else {
		out = append(out, source.Check{ID: "SCOPE-02", Title: "选中的表都有主键",
			Status: source.StatusOK, Detail: "可以安全地做 upsert"})
	}
	return out
}

func tablesWithoutColumn(cat *source.Catalog, tables []string, column string) []string {
	if cat == nil {
		return nil
	}
	has := map[string]bool{}
	for _, t := range cat.Tables {
		for _, c := range t.Columns {
			if strings.EqualFold(c.Name, column) {
				has[t.Table] = true
			}
		}
	}
	var out []string
	for _, name := range tables {
		if !has[name] {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

func existingTargetTables(cat *source.Catalog, tables []string) []string {
	if cat == nil {
		return nil
	}
	have := map[string]bool{}
	for _, t := range cat.Tables {
		have[t.Table] = true
	}
	var out []string
	for _, name := range tables {
		if have[name] {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

func preview(names []string) string {
	if len(names) <= 3 {
		return strings.Join(names, "、")
	}
	return strings.Join(names[:3], "、") + "…"
}

// typeMappingChecks 把一个任务范围里**有损**的列类型收集成预检项。
//
// 分两档，与引擎的拦截口径一致：
//   - 数组（映成 json）：可能写出结构体字段 → blocked（引擎那边也是拦）；
//   - 其它 NeedsConfirm（timestamptz 丢时区、money、interval → varchar）：warn。
//     它们是「知情后可接受」的代价，不是写错数据。
func typeMappingChecks(in Input) []source.Check {
	from := catalogEngine(in.SourceKind)
	to := catalogEngine(in.TargetKind)
	if from == "" || to == "" {
		return nil
	}
	want := map[string]bool{}
	for _, t := range in.Tables {
		want[t] = true
	}
	var blocked, warned []string
	for _, t := range in.SourceCat.Tables {
		if len(want) > 0 && !want[t.Table] {
			continue
		}
		for _, c := range t.Columns {
			m, ok := typecat.Map(from, to, c.Type)
			if !ok || m.Loss < typecat.NeedsConfirm {
				continue
			}
			line := fmt.Sprintf("%s.%s（%s → %s）", t.Table, c.Name, c.Type, m.Target)
			if m.Note != "" {
				line += "：" + m.Note
			}
			if strings.HasSuffix(strings.TrimSpace(c.Type), "[]") {
				blocked = append(blocked, line)
			} else {
				warned = append(warned, line)
			}
		}
	}
	var out []source.Check
	if len(blocked) > 0 {
		out = append(out, source.Check{
			ID: "TYPE-01", Title: "这些列的类型到目标端不能保证语义", Status: source.StatusBlocked,
			Detail: strings.Join(blocked, "；") +
				"。数组会被序列化成 JSON，元素若没有 JSON 表示就会写成结构体字段（看起来能用，实际不是原值）",
			Who: "you",
		})
	}
	if len(warned) > 0 {
		out = append(out, source.Check{
			ID: "TYPE-02", Title: "这些列的类型到目标端会有损转换", Status: source.StatusWarn,
			Detail: strings.Join(warned, "；") + "。知情的代价（例如丢时区），跑完可用 verify 复核",
			Who:    "you",
		})
	}
	return out
}

// catalogEngine 把连接类型映射到类型字典的引擎名。
func catalogEngine(kind string) typecat.Engine {
	switch kind {
	case "postgres":
		return typecat.Postgres
	case "mysql":
		return typecat.MySQL
	}
	return ""
}
