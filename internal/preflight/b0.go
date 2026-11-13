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
	JobName    string
	SourceKind string
	TargetKind string
	SourceVer  string
	TargetVer  string
	SourceErr  string // 人话的连接错误；空表示已连上
	TargetErr  string
	SourceCaps source.CapSet
	SourceCat  *source.Catalog // nil 表示没读到（不阻断）
	TargetCat  *source.Catalog // nil 表示没读到：目标表是否存在交给运行时判断
	Tables     []string        // 用户勾选的表
	// UniqueKeys 是用户声明的行身份（content.tables.<表>.unique_key），key 用源端裸表名
	//（与 SCOPE-02 同口径）。只用于 TARGET-02 的核对，不用来自动发现身份
	//（docs/DESIGN-NO-PRIMARY-KEY.md §4.3/§4.4）。
	UniqueKeys   map[string][]string
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

	// ── MySQL 源端存储引擎（SPEC MY-08）───────────────────────
	// MyISAM 没有事务：崩溃时目标端可能与源端不一致，但同步本身能跑——
	// 按 docs/SPEC.md:107 的拍板口径 warn 照跑并标注，不阻断。
	checks = append(checks, engineChecks(in)...)

	// ── PG 源端行标识（PG-06，无主键表进增量的前置）──────────
	checks = append(checks, pgReplicaIdentityCheck(in)...)

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

	// ── 目标端唯一键（TARGET-02）─────────────────────────────
	// 四档见 docs/DESIGN-NO-PRIMARY-KEY.md §3.4。它只回答「目标端能不能定位这一行」，
	// 与 PG-06（源端能不能解出这一行）是两道闸，不许合成一条（§6 红线 10）。
	checks = append(checks, targetUniqueKeyChecks(in)...)

	// ── 按表暂停能力 ─────────────────────────────────────────
	// 这条要**在 plan 里就说清**，别等用户在半夜撞上 E1015 才知道。
	//
	// 口径的真源是 docs/CONTRACT-PAUSE-AND-POSITION.md §1.5/§1.6 与 DECISIONS #17/#19：
	//   - 两个目标端现在都**已接**引擎侧按表暂停（PG `#93`、MySQL `#102`：`pauseReg` 绑定 +
	//     恢复四步 `planGenericRecopy` / `truncateForcedRecopy` / `finishGenericRecopy`）；
	//   - **`interval` 是例外**（§1.6 例外段 / `DECISIONS #19`）：水位轮询按页读写，没有批、
	//     没有变更流，也就没有「这一批跳过该表、位点照常前进」的位置——它**不做**按表暂停。
	//     所以 interval 下无条件说「只停它自己」是**假话**，必须换成 warn + 手工对齐的路子。
	interval := in.Mode == "interval"
	switch in.TargetKind {
	case "mysql":
		if interval {
			add(source.Check{
				ID: "PAUSE-01", Title: "定时增量在 MySQL 目标端没接", Status: source.StatusWarn,
				Detail: "`frequency=interval` 在 MySQL 目标端这一版**整体没接**（水位轮询只有 PG 目标端实现，" +
					"开跑会被明确拒绝并给出替代模式），所以「只停这张表」也无从谈起。" +
					"改成 continuous+initial / once / full_interval 之后按表暂停是生效的：" +
					"只停这张表、其它表照常、位点照常前进，结构对齐后整表重搬补齐",
			})
			break
		}
		add(source.Check{
			ID: "PAUSE-01", Title: "目标端支持按表暂停", Status: source.StatusOK,
			// 契约 §1.5/§1.6：只停**这张表**，其它表照常提交、位点照常前进；
			// 跳过窗口记在 `_jl_meta.paused_tables`，结构对齐后整表重搬补齐（`#102` 已接）。
			Detail: "结构对不上的表会被记录并只停它自己（记在目标端 `_jl_meta.paused_tables` 那一列；" +
				"老库缺列时开跑会先补，补不上会明确失败并说明原因）：其它表照常同步、位点照常前进，" +
				"跳过的那段在结构对齐后由整表重搬补齐",
		})
	case "postgres":
		if interval {
			add(source.Check{
				ID: "PAUSE-01", Title: "定时增量不做按表暂停", Status: source.StatusWarn,
				Detail: "`frequency=interval` 这一版**没有**「只停这张表」这条通道（水位轮询按页读写，" +
					"没有「跳过该表、位点照常前进」的位置）：表结构对不上时会照旧写这张表，多半直接报错。" +
					"先手工把目标端结构对齐再跑；要做到「只停一张表 + 结构对齐后自动整表重搬补齐」，" +
					"请改用 continuous+initial 或 full_interval",
			})
			break
		}
		add(source.Check{ID: "PAUSE-01", Title: "目标端支持按表暂停", Status: source.StatusOK,
			// 口径（docs/CONTRACT-PAUSE-AND-POSITION.md §1.5/§1.6）：只停**这张表**，
			// 其它表照常提交、位点照常前进；跳过窗口记账，结构对齐后整表重搬补齐。
			Detail: "结构对不上的表会被记录并只停它自己：其它表照常同步、位点照常前进，" +
				"跳过的那段在结构对齐后由整表重搬补齐"})
	case "":
		// 没读到目标端类型：不猜，也不吓人。
	default:
		add(source.Check{
			ID: "PAUSE-01", Title: "目标端不支持按表暂停", Status: source.StatusWarn,
			Detail: fmt.Sprintf("目标端是 %s：还没有暂停记录那一列——遇到表结构不一致时，"+
				"整条同步会停下来等你手工对齐（不会静默跳过，也不会假装登记成功）", in.TargetKind),
		})
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
	hasPK := map[string]bool{}
	replIdentity := map[string]string{}
	for _, t := range in.SourceCat.Tables {
		have[t.Table] = true
		hasPK[t.Table] = t.HasStablePK
		replIdentity[t.Table] = t.ReplicaIdentity
	}
	var missing, noPK []string
	for _, name := range in.Tables {
		if !have[name] {
			missing = append(missing, name)
			continue
		}
		if !hasPK[name] {
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
	out = append(out, noPKCheck(in, noPK, replIdentity))
	return out
}

// noPKCheck 是 SCOPE-02 的四档（docs/DESIGN-NO-PRIMARY-KEY.md §3.1）。
//
// 口径不许共用一句话：同一份报告里两处自相矛盾（PAUSE-01 踩过），所以按
// `mode.frequency` 分档写 Detail；`once`/`full_interval` 是 ok（整轮重搬不需要行身份），
// `interval` 是 **blocked**（每轮按水位重读同一批行，没有身份=第二轮起必然重复，
// 这是这个模式在我们手里做不到，不是「有风险但可以跑」），其余（continuous / 空）是 warn。
func noPKCheck(in Input, noPK []string, replIdentity map[string]string) source.Check {
	// S3：**已声明 unique_key** 的表不在这张卡里算「无身份」——它到底能不能定位，
	// 由 TARGET-02 核对目标端事实来裁决（对不上仍然 blocked，整体照样进不去）。
	// 不排除会出现「同一份报告里 SCOPE-02 让用户去声明 unique_key、用户声明了、
	// 这张卡还在拦他」的自相矛盾（红线 §6.7，Owl PR #120 第 4 轮）。
	//
	// 例外：`TargetCat == nil`（读不到目标端，TARGET-02 根本不出卡）→ 没人裁决，
	// 这里保守按无身份处理（blocked/warn 都还能救；放行后跑起来才炸没法救）。
	declared := 0
	if in.TargetCat != nil && len(in.UniqueKeys) > 0 {
		kept := noPK[:0]
		for _, name := range noPK {
			if len(in.UniqueKeys[name]) > 0 {
				declared++
				continue
			}
			kept = append(kept, name)
		}
		noPK = kept
	}
	if len(noPK) == 0 {
		if declared > 0 {
			return source.Check{ID: "SCOPE-02", Title: "无主键的表已经声明了 unique_key",
				Status: source.StatusOK,
				Detail: fmt.Sprintf("这 %d 张表没有主键，但你声明了 unique_key，"+
					"行身份改由目标端核对（见 TARGET-02）：核对通过就能 upsert 与删除，核对不过会被拦下来", declared)}
		}
		return source.Check{ID: "SCOPE-02", Title: "选中的表都有主键",
			Status: source.StatusOK, Detail: "可以安全地做 upsert"}
	}
	sort.Strings(noPK)
	names := strings.Join(noPK, "、")
	switch in.Mode {
	case "once", "full_interval":
		return source.Check{
			ID: "SCOPE-02", Title: "无主键表不影响这个模式",
			Status: source.StatusOK,
			Detail: names + " 没有主键：每轮整轮重搬，结果等于源端当前状态，不需要行身份。" +
				"代价：搬完之前目标端是部分数据；第二轮起会先清掉本任务写过的行再搬" +
				"（目标表是你自己建的、里面是你的数据时不会自动清空，会停下来让你选）",
			Affected: noPK,
		}
	case "interval":
		return source.Check{
			ID: "SCOPE-02", Title: "定时增量 + 无主键：每轮必然重复",
			Status: source.StatusBlocked,
			Detail: names + " 没有主键：定时增量每轮按水位把同一段行重读一遍，" +
				"没有行身份就是每轮再插一遍（第二轮起必然重复、且每轮增长）。" +
				"你能做的：① 给表加主键或唯一索引；② 在 content.tables.<表> 声明 unique_key（指向目标端已有的唯一索引）；" +
				"③ 模式改成 continuous+initial 或 once/full_interval",
			Affected: noPK, Who: "you",
			// E1017 是「无身份导致做不到」的族码（Ark 裁决）：运行时改/删与这里的
			// interval 必然重复是同一族。码表条目由 Ark 落 codes.go，到位后换 core.CodeNoIdentity。
			Code: string(core.Code("E1017")),
		}
	}
	// continuous / continuous+initial / 未指定：insert-only 能跑，改/删触发按表暂停。
	detail := names + " 没有主键：先按只追加同步（插入照常、不会阻断）；" +
		"源端一旦改动或删除这张表的行，它会**只停它自己**、其它表照常、位点照常前进，" +
		"跳过的那段等结构对齐后整表重搬补齐"
	if in.SourceKind == "postgres" {
		// §3.1：PG 源另有一句——REPLICA IDENTITY 不到位时源端解码先断，整条流停，
		// 不是「只停这张表」。这条不能省，省了卡片就是假承诺（§6 红线 9）。
		var bad []string
		for _, name := range noPK {
			if !riOK(replIdentity[name]) {
				bad = append(bad, name)
			}
		}
		if len(bad) > 0 {
			detail += "。另：源端 " + strings.Join(bad, "、") + " 的 REPLICA IDENTITY 不是 FULL（也不是 USING INDEX）——" +
				"源端在第一次改动/删除时解不出旧行会直接断开连接（整条流停，不是只停这张表），见预检 PG-06"
		}
	}
	return source.Check{
		ID: "SCOPE-02", Title: "部分表没有主键：只追加，改/删会让它暂停",
		Status: source.StatusWarn, Detail: detail, Affected: noPK, Who: "you",
	}
}

// riOK：REPLICA IDENTITY 是否足以让源端解出旧行。
// full（整行旧镜像）或 index（USING INDEX）都行；nothing / default(no-pk) / 读不到 都不行。
func riOK(v string) bool {
	return v == "full" || v == "index"
}

// pgReplicaIdentityCheck 是 PG-06（**源端**前置，docs/DESIGN-NO-PRIMARY-KEY.md §3.5）。
//
// 为什么必须有：PG 源上「无主键表收到改/删 → 只停这张表」这句承诺有前置条件——
// 源端 REPLICA IDENTITY 不是 full/index 时，walsender 在解码阶段就报
// 「does not have a replica identity」并断开；我们连事件都收不到，E1017 发不出来，
// 用户看到的是「同步连接反复断开」，而且重连会在同一段变更上再炸。
//
// 触发：源端 postgres ∧ 模式含变更流 ∧ 选中表里有 HasStablePK=false ∧ RI 不满足。
// 数据来自现有 `source.Table.ReplicaIdentity`（connect/postgres.go 已填），不新增查询。
func pgReplicaIdentityCheck(in Input) []source.Check {
	if in.SourceKind != "postgres" || in.SourceCat == nil || !strings.Contains(in.Mode, "continuous") {
		return nil
	}
	want := map[string]bool{}
	for _, t := range in.Tables {
		want[t] = true
	}
	var fixable, partitioned []string
	var alters []string
	for _, t := range in.SourceCat.Tables {
		if !want[t.Table] || t.HasStablePK || riOK(t.ReplicaIdentity) {
			continue
		}
		if t.Partitioned {
			// relreplident 是每张关系各自的属性：父表 ALTER 不覆盖叶子，而 catalog 里
			// 子分区不单列（connect/postgres.go 直接跳过）→ 本版不代做，只给模板（挂账 D10）。
			partitioned = append(partitioned, t.Table)
			continue
		}
		fixable = append(fixable, t.Table)
		alters = append(alters, fmt.Sprintf("ALTER TABLE %s.%s REPLICA IDENTITY FULL;",
			quoteIdent(t.Schema), quoteIdent(t.Table)))
	}
	if len(fixable) == 0 && len(partitioned) == 0 {
		return nil
	}
	sort.Strings(fixable)
	sort.Strings(partitioned)
	var parts []string
	if len(fixable) > 0 {
		parts = append(parts, fmt.Sprintf("%s 没有主键、REPLICA IDENTITY 也不是 FULL：源端在第一次改动/删除时"+
			"解不出旧行，会直接断开连接（整条流停，不是只停这张表）", strings.Join(fixable, "、")))
	}
	if len(partitioned) > 0 {
		parts = append(parts, fmt.Sprintf("%s 是分区表：叶子要逐张设置 REPLICA IDENTITY，本版不代做，"+
			"请对每个分区执行 ALTER TABLE <分区> REPLICA IDENTITY FULL", strings.Join(partitioned, "、")))
	}
	head := strings.Join(parts, "。")
	c := source.Check{
		ID: "PG-06", Title: "无主键表拿不到旧行标识（改/删会让整条流断开）",
		Status: source.StatusWarn,
		Detail: head + "。三条路：① 执行下面的 ALTER（需要这张表的属主权限，" +
			"拿不到就让 DBA 代跑）；② 这张表不进增量（模式改成 once / full_interval）；" +
			"③ 把它移出 content.tables。" +
			"风险：① 会让源端变更流水带上整行旧镜像，WAL 体积变大（大字段表尤其明显）",
		SQL:      strings.Join(alters, "\n"),
		Affected: append(append([]string{}, fixable...), partitioned...),
		Who:      "dba",
	}
	return []source.Check{c}
}

// quoteIdent 给 SQL 里的标识符加双引号（identifier 里的双引号翻倍）。
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// engineChecks 是 SPEC MY-08（表的存储引擎）：MySQL 源端选中表里有 MyISAM
// （含 MRG_MyISAM）时 **warn 降级**——无事务，全量不是一致性切面，照跑并标注。
//
// 判据只有 JudgeEngines 一处（engine.go）：这里不自己判「哪些表算无事务」，
// 否则文档/收尾/预检迟早三处三套说法。SourceCat 没读到时不发这条（不阻断），
// 但判据给的是 not_judged 而不是 ok。
func engineChecks(in Input) []source.Check {
	j := JudgeEngines(in.SourceKind, in.SourceCat, in.Tables)
	if j.Verdict != EngineNonTransactional {
		return nil
	}
	return []source.Check{{
		ID: "MY-08", Title: "部分表用 MyISAM 引擎（无事务）",
		Status: source.StatusWarn,
		Detail: EngineDegradeNote(j),
		SQL:    j.SQL,
		// 「你能做什么」就是上面那条 ALTER：改成 InnoDB 后这一项下一轮自动消失。
		Affected: j.Names, Who: "you",
	}}
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

// targetUniqueKeyChecks 是 TARGET-02（docs/DESIGN-NO-PRIMARY-KEY.md §3.4）：
// 「目标端有没有一条能定位这一行的唯一键」。它**不决定身份**（R1），只做两件事：
//  1. 用户声明了 unique_key 时，核对目标端真的有一条匹配的唯一索引（判据见 §4.3，五条全过才算过）；
//  2. 没声明时，把「只追加」的代价与两条出路（unique_key / once·full_interval）说清。
//
// 一张卡按档聚合：有 blocked 就 blocked，否则有 warn 就 warn（红线 §6.7：同一份报告不许两句互相矛盾）。
func targetUniqueKeyChecks(in Input) []source.Check {
	if in.TargetCat == nil || in.SourceCat == nil {
		// 任一侧目录没读到：不假装核对过（TARGET-01 / SCOPE-01 已经说过各自的缺失）。
		return nil
	}
	src := map[string]*source.Table{}
	for i := range in.SourceCat.Tables {
		src[in.SourceCat.Tables[i].Table] = &in.SourceCat.Tables[i]
	}
	tgt := map[string]*source.Table{}
	for i := range in.TargetCat.Tables {
		tgt[in.TargetCat.Tables[i].Table] = &in.TargetCat.Tables[i]
	}
	// 整轮重搬的模式（once / full_interval）不需要行身份：目标端每轮被清空重写，
	// 与 SCOPE-02「无主键表不影响这个模式」同口径——这里再说「改成 once」就是自打脸（红线 §6.7）。
	wholeTable := in.Mode == "once" || in.Mode == "full_interval"

	var blocked, warnNoPK, warnIgnored, warnNoMatch, warnOtherKey, warnNullable []string
	nullableWhy := map[string]string{}
	blockedWhy := map[string]string{}
	for _, name := range in.Tables {
		st, known := src[name]
		if !known {
			continue // SCOPE-01 会报「源端没有这张表」，这里不重复
		}
		declared := in.UniqueKeys[name]
		if wholeTable && len(declared) > 0 {
			// 整轮重搬模式每轮清空重写，行身份用不上：声明在这里不生效（写路径也不会拿它定位）。
			// 这不该拦住用户（Owl PR #120 第 2 轮 P2-1），提示一句就够。
			warnIgnored = append(warnIgnored, name)
			declared = nil
		}
		t := tgt[name]

		switch {
		case st.HasStablePK && len(declared) > 0:
			// §2.2 格 7：表已有主键，声明被忽略（照主键走），只提示一句。
			warnIgnored = append(warnIgnored, name)
		case st.HasStablePK:
			// 源端有主键：目标端将来由我们建表时自带主键；复用用户表时才可能缺键。
			if t == nil {
				continue // 目标端还没有这张表：建表时按源端主键建
			}
			if len(t.PrimaryKey) > 0 && sameColumnSet(t.PrimaryKey, st.PrimaryKey) {
				continue // 目标端主键与源端一致
			}
			switch {
			case len(t.PrimaryKey) > 0:
				// 目标端主键与源端不同组：但如果另有一条唯一索引正好是源端主键那几列，
				// upsert 定位得到（PG 的 ON CONFLICT / MySQL 的 ON DUPLICATE KEY 都认唯一索引），
				// 那就不是问题（Owl PR #120 第 2 轮 P2-2 的真库反例）。
				ok, why := uniqueKeyMatches(t, st.PrimaryKey)
				if ok {
					continue
				}
				if strings.Contains(why, "允许为空") {
					// 那组列可空：ON CONFLICT 照收（不报 42P10），但 NULL 互不相等 →
					// 带 NULL 的行不会互相冲突，重放就会插重复。真实风险 ≠ 「键不存在」。
					// Affected 只放裸表名（与其它档一致），why 进 Detail。
					warnNullable = append(warnNullable, name)
					nullableWhy[name] = why
					continue
				}
				warnOtherKey = append(warnOtherKey, name)
			case len(t.UniqueKeys) == 0:
				// 一条唯一索引都没有：upsert 直接不成立（ErrNoUniqueKey）。
				warnNoMatch = append(warnNoMatch, name)
			default:
				// 有唯一索引但不是源端主键那组。
				warnOtherKey = append(warnOtherKey, name)
			}
		case len(declared) > 0:
			if t == nil {
				// 目标表都不存在，声明自然无从核对。声明要求「目标端已有唯一索引」，
				// 我们建表只建主键（源端没有）——所以这里就是核不过。
				blocked = append(blocked, name)
				blockedWhy[name] = "目标端还没有这张表：声明 unique_key 要求目标端**已有**匹配的唯一索引；先建表并建索引，或去掉声明用 insert-only"
				continue
			}
			if ok, why := uniqueKeyMatches(t, declared); ok {
				continue
			} else {
				blocked = append(blocked, name)
				blockedWhy[name] = why
			}
		default:
			if wholeTable {
				continue // 整轮重搬：目标端每轮重写，不需要行身份
			}
			// 源端无主键、也没声明：insert-only。目标端即使有唯一索引也不会被自动使用（R1）。
			warnNoPK = append(warnNoPK, name)
		}
	}

	if len(blocked) == 0 && len(warnNoPK) == 0 && len(warnIgnored) == 0 && len(warnNoMatch) == 0 &&
		len(warnOtherKey) == 0 && len(warnNullable) == 0 {
		return []source.Check{{ID: "TARGET-02", Title: "目标端的唯一键够用", Status: source.StatusOK,
			Detail: "选中的表都能按主键（或声明的 unique_key）定位到行"}}
	}

	var parts []string
	var affected []string
	if len(blocked) > 0 {
		sort.Strings(blocked)
		for _, n := range blocked {
			parts = append(parts, fmt.Sprintf("%s：%s", n, blockedWhy[n]))
		}
		affected = append(affected, blocked...)
	}
	if len(warnNoPK) > 0 {
		sort.Strings(warnNoPK)
		parts = append(parts, fmt.Sprintf("%s 没有主键也没声明 unique_key：目标端只追加（改/删会让这张表暂停并欠一次整表重搬；重放窗口内可能重复行）。"+
			"要 upsert 就在 content.tables.<表> 里写 unique_key: [列…]（必须是目标端已有、列都非空的唯一索引/主键），或把 mode.frequency 改成 once / full_interval",
			preview(warnNoPK)))
		affected = append(affected, warnNoPK...)
	}
	if len(warnIgnored) > 0 {
		sort.Strings(warnIgnored)
		parts = append(parts, fmt.Sprintf("%s 源端已有主键（或整轮重搬模式用不上行身份）：unique_key 声明被忽略——"+
			"按主键 / 整表重搬走，行为不变（列过滤能力落地后，主键列被排掉时声明才会生效）", preview(warnIgnored)))
		affected = append(affected, warnIgnored...)
	}
	if len(warnNullable) > 0 {
		sort.Strings(warnNullable)
		whys := make([]string, 0, len(warnNullable))
		for _, n := range warnNullable {
			whys = append(whys, nullableWhy[n])
		}
		parts = append(parts, fmt.Sprintf("%s 目标端的唯一索引/主键含可空列（%s）：ON CONFLICT 照收，但 NULL 互不相等——"+
			"带 NULL 的行不会互相冲突，重放/重复投递会插出多行。你能做的：把那几列改成 NOT NULL，或换一组非空的唯一索引",
			preview(warnNullable), strings.Join(whys, "；")))
		affected = append(affected, warnNullable...)
	}
	if len(warnOtherKey) > 0 {
		sort.Strings(warnOtherKey)
		advice := "upsert 会照目标端现有的唯一键（或主键）更新，不是按源端主键；两者不重合时会改到「另一条」记录，建议把目标端主键对齐源端"
		if in.TargetKind != "mysql" {
			advice = "upsert 会失败（PostgreSQL 的 ON CONFLICT 要的那组键在目标端不存在，报 42P10）；给目标表加上与源端一致的主键或唯一索引，或把这几张表改成只追加"
		}
		parts = append(parts, fmt.Sprintf("%s 源端有主键，但目标表的主键/唯一键和它不一致：%s", preview(warnOtherKey), advice))
		affected = append(affected, warnOtherKey...)
	}
	if len(warnNoMatch) > 0 {
		sort.Strings(warnNoMatch)
		parts = append(parts, fmt.Sprintf("%s 源端有主键、目标端没有对应的唯一键：upsert 会失败（若是 append 复用了用户表，给它加与源端一致的主键，或换空的目标 schema 让任务自己建表）",
			preview(warnNoMatch)))
		affected = append(affected, warnNoMatch...)
	}

	status := source.StatusWarn
	title := "目标端的唯一键有需要你知道的地方"
	code := ""
	if len(blocked) > 0 {
		status = source.StatusBlocked
		title = "声明的 unique_key 与目标端事实不符"
		// 挂 E1017（没有行身份族码）：没有码时 engine 会回落到 E1001「源库没开变更流水」，
		// 用户会去改 wal_level——完全指错方向（Owl PR #120 P1）。
		code = string(core.CodeNoIdentity)
	}
	return []source.Check{{
		ID: "TARGET-02", Title: title, Status: status, Code: code,
		Detail:   strings.Join(parts, "；"),
		Affected: affected, Who: "you",
	}}
}

// sameColumnSet 报告两组列名是否完全相同（顺序无关，重复视作不同）。
func sameColumnSet(a, b []string) bool {
	if len(a) != len(b) || len(a) == 0 {
		return false
	}
	want := map[string]bool{}
	for _, c := range a {
		want[c] = true
	}
	if len(want) != len(a) {
		return false
	}
	for _, c := range b {
		if !want[c] {
			return false
		}
	}
	return true
}

// uniqueKeyMatches 实现 §4.3 的目标端核对判据（五条全过才算「唯一键可用」）。
// 候选索引 = 目录里的唯一索引 **∪ 主键**（主键本身就是唯一索引，`ON CONFLICT`/
// `ON DUPLICATE KEY` 都命中它；把它排除会把「目标端已有主键」判成 blocked）。
// 目录层已过滤掉部分/表达式/前缀索引（connect 的两个 Catalog），这里补两条：
//   - 列集合与声明**完全一致**（顺序无关，不多不少）；
//   - 每一列都 NOT NULL（两边都把 NULL 当互不相等；主键列天然非空）。
//
// 不过时返回一句能直接写进卡片的「哪条判据不过」，不返回布尔完事。
func uniqueKeyMatches(t *source.Table, declared []string) (bool, string) {
	nullable := map[string]bool{}
	have := map[string]bool{}
	for _, c := range t.Columns {
		nullable[c.Name] = c.Nullable
		have[c.Name] = true
	}
	for _, c := range declared {
		if !have[c] {
			return false, fmt.Sprintf("目标端没有列 %s（声明的列必须都在目标表里）", c)
		}
	}
	candidates := append([][]string{}, t.UniqueKeys...)
	if len(t.PrimaryKey) > 0 {
		// 主键列一定非空：把可空性放宽，按「非空」参与判断。
		candidates = append(candidates, t.PrimaryKey)
		for _, c := range t.PrimaryKey {
			nullable[c] = false
		}
	}
	var existing []string
	for _, key := range candidates {
		existing = append(existing, "("+strings.Join(key, ", ")+")")
		if !sameColumnSet(key, declared) {
			continue
		}
		for _, c := range key {
			if nullable[c] {
				return false, fmt.Sprintf("唯一索引 (%s) 的列 %s 允许为空：NULL 互不相等，这组列不构成唯一性", strings.Join(key, ", "), c)
			}
		}
		return true, ""
	}
	if len(existing) == 0 {
		return false, "目标端这张表没有任何可用唯一索引或主键（要求：非部分、非表达式、无前缀长度）"
	}
	return false, fmt.Sprintf("目标端唯一索引/主键是 %s，与声明 (%s) 的列集合不一致",
		strings.Join(existing, "、"), strings.Join(declared, ", "))
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
