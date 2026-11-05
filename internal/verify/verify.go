// Package verify implements the three-level consistency check from
// docs/VERIFY.md:
//
//	structure   目标表存在、列齐全、类型可映射、主键对得上
//	data        行数、抽样校验和、主键覆盖
//	consistency 源端位点与目标端已写入位点对得上
//
// 三级可以单独跑（Options.Level），也可以串起来（LevelAll）。每一级都返回
// 结构化结论（Finding + Stats），不是一句「校验通过」。
//
// 只读：structure/data 只发 SELECT；consistency 只读 slot / binlog 坐标与
// `_jl_meta.position`。本包不提供 --fix（订正属于引擎轨道）。
package verify

import (
	"context"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"time"

	"github.com/spencerswagger/jinliang/internal/catalog"
	"github.com/spencerswagger/jinliang/internal/runtime"
	"github.com/spencerswagger/jinliang/internal/source"
)

// Level names one check tier.
type Level string

// Check tiers.
const (
	LevelStructure   Level = "structure"
	LevelData        Level = "data"
	LevelConsistency Level = "consistency"
	LevelAll         Level = "all"
)

// Severity decides whether a finding fails the level.
type Severity string

// Finding severities.
const (
	SeverityError Severity = "error" // 校验不通过：确实不一致 / 无法判定
	SeverityWarn  Severity = "warn"  // 能继续，但需要人看一眼
	SeverityInfo  Severity = "info"
)

// Status is the per-level verdict.
type Status string

// Level verdicts.
const (
	StatusOK       Status = "ok"
	StatusMismatch Status = "mismatch"
	StatusError    Status = "error" // 探测本身失败（连不上/读不到）
)

// Finding is one structured conclusion.
type Finding struct {
	Code     string   `json:"code"`
	Severity Severity `json:"severity"`
	Table    string   `json:"table,omitempty"`
	Column   string   `json:"column,omitempty"`
	Detail   string   `json:"detail"`
	Source   string   `json:"source,omitempty"`
	Target   string   `json:"target,omitempty"`
	Hint     string   `json:"hint,omitempty"`
}

// LevelReport is one tier's result.
type LevelReport struct {
	Level      Level          `json:"level"`
	Status     Status         `json:"status"`
	OK         bool           `json:"ok"`
	Tables     int            `json:"tables"`
	Findings   []Finding      `json:"findings"`
	Stats      map[string]any `json:"stats,omitempty"`
	DurationMS int64          `json:"duration_ms"`
}

// Report is the whole run.
type Report struct {
	GeneratedAt string        `json:"generated_at"`
	Source      string        `json:"source,omitempty"`
	Target      string        `json:"target,omitempty"`
	JobKey      string        `json:"job_key,omitempty"`
	Levels      []LevelReport `json:"levels"`
	OK          bool          `json:"ok"`
}

// Side is the read-only view verify needs from one endpoint. *runtime.DB
// implements it; tests use a fake.
type Side interface {
	Kind() string
	Table(ctx context.Context, schema, table string) (source.Table, bool, error)
	Count(ctx context.Context, schema, table string) (int64, error)
	SamplePKs(ctx context.Context, schema, table string, pk []string, n int) ([][]any, error)
	RowByPK(ctx context.Context, schema, table string, columns, pk []string, values []any) ([]any, bool, error)
}

// Options controls one verify run.
type Options struct {
	Source Side
	Target Side

	SourceSchema string
	TargetSchema string
	Tables       []string

	// Level selects one tier; empty means LevelAll.
	Level Level

	// SampleRows is the number of primary keys compared per table.
	SampleRows int

	// Position probes for the consistency tier. Nil means "not available"
	// and the tier reports that honestly instead of passing.
	SourcePosition func(ctx context.Context) (string, error)
	TargetPosition func(ctx context.Context) (string, error)

	// MaxGapBytes tolerates a target that is behind the source slot by this
	// many bytes (a live stream always has some lag). Default 64 MiB.
	MaxGapBytes int64

	JobKey string
}

// Run executes the requested tiers.
func Run(ctx context.Context, opts Options) (Report, error) {
	if opts.Source == nil || opts.Target == nil {
		return Report{}, fmt.Errorf("verify 需要源端与目标端两个可读连接")
	}
	if len(opts.Tables) == 0 {
		// 没给表清单时校验什么？让调用方先列 catalog —— 静默空跑就是"永远绿"。
		return Report{}, fmt.Errorf("没有要校验的表：用 --tables 指定（或先枚举 catalog）")
	}
	if opts.SampleRows <= 0 {
		opts.SampleRows = 100
	}
	if opts.MaxGapBytes == 0 {
		opts.MaxGapBytes = 64 << 20
	}
	if opts.SourceSchema == "" {
		opts.SourceSchema = "public"
	}
	if opts.TargetSchema == "" {
		opts.TargetSchema = "public"
	}
	level := opts.Level
	if level == "" {
		level = LevelAll
	}
	rep := Report{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		JobKey:      opts.JobKey,
		OK:          true,
	}

	type tier struct {
		level Level
		fn    func(context.Context, Options) LevelReport
	}
	tiers := []tier{
		{LevelStructure, runStructure},
		{LevelData, runData},
		{LevelConsistency, runConsistency},
	}
	for _, t := range tiers {
		if level != LevelAll && level != t.level {
			continue
		}
		start := time.Now()
		lr := t.fn(ctx, opts)
		lr.DurationMS = time.Since(start).Milliseconds()
		if !lr.OK {
			rep.OK = false
		}
		rep.Levels = append(rep.Levels, lr)
	}
	if len(rep.Levels) == 0 {
		return Report{}, fmt.Errorf("不认识的校验级别 %q（structure / data / consistency / all）", level)
	}
	return rep, nil
}

// ── L1 结构 ────────────────────────────────────────────────────────────

func runStructure(ctx context.Context, opts Options) LevelReport {
	lr := newLevel(LevelStructure)
	for _, name := range opts.Tables {
		lr.Tables++
		srcT, ok, err := opts.Source.Table(ctx, opts.SourceSchema, name)
		if err != nil {
			lr.add(SeverityError, "probe_failed", name, "", "读源端表结构失败："+err.Error(), "", "", "")
			continue
		}
		if !ok {
			lr.add(SeverityError, "source_table_missing", name, "",
				"源端没有这张表（表名/schema 写错了，或已被删除）", opts.SourceSchema, "", "检查 --tables 与 schema")
			continue
		}
		tgtT, ok, err := opts.Target.Table(ctx, opts.TargetSchema, name)
		if err != nil {
			lr.add(SeverityError, "probe_failed", name, "", "读目标端表结构失败："+err.Error(), "", "", "")
			continue
		}
		if !ok {
			lr.add(SeverityError, "target_table_missing", name, "",
				"目标端没有这张表：初始化还没跑到建表，或建表失败",
				opts.SourceSchema, opts.TargetSchema,
				"先跑一次初始化（jinliang run），或确认 target schema 是否正确")
			continue
		}
		checkColumns(&lr, srcT, tgtT, opts)
		checkPrimaryKey(&lr, srcT, tgtT)
	}
	lr.finish()
	return lr
}

func checkColumns(lr *LevelReport, srcT, tgtT source.Table, opts Options) {
	tgtCols := map[string]source.Column{}
	for _, c := range tgtT.Columns {
		tgtCols[strings.ToLower(c.Name)] = c
	}
	srcNames := map[string]bool{}
	from := catalog.Engine(opts.Source.Kind())
	to := catalog.Engine(opts.Target.Kind())

	for _, sc := range srcT.Columns {
		srcNames[strings.ToLower(sc.Name)] = true
		tc, ok := tgtCols[strings.ToLower(sc.Name)]
		if !ok {
			lr.add(SeverityError, "missing_column", srcT.Table, sc.Name,
				fmt.Sprintf("目标表缺列（源端 %s 有，目标端没有）", sc.Type), sc.Type, "",
				"让 engine 重建/加列，或确认目标表是不是另一张表")
			continue
		}
		if !nullableSafe(sc, tc) {
			lr.add(SeverityError, "nullable_tightened", srcT.Table, sc.Name,
				"源端列允许 NULL，目标端列是 NOT NULL：遇到 NULL 会写失败",
				nullableText(sc.Nullable), nullableText(tc.Nullable),
				"把目标列改成可空，或加默认值 + 转换规则")
		}
		checkType(lr, srcT.Table, sc, tc, from, to)
	}

	var extra []string
	for _, tc := range tgtT.Columns {
		if !srcNames[strings.ToLower(tc.Name)] {
			extra = append(extra, tc.Name)
		}
	}
	if len(extra) > 0 {
		sort.Strings(extra)
		lr.add(SeverityWarn, "extra_column", srcT.Table, strings.Join(extra, ","),
			fmt.Sprintf("目标端多出 %d 列（不影响写入，但可能是结构漂移）", len(extra)),
			"", strings.Join(extra, ","), "确认这些列是否由目标端的触发器/默认值维护")
	}
}

func checkType(lr *LevelReport, table string, sc, tc source.Column, from, to catalog.Engine) {
	if from == to {
		if !sameBaseType(sc.Type, tc.Type) {
			lr.add(SeverityError, "type_incompatible", table, sc.Name,
				fmt.Sprintf("同构同步但类型不同（源 %s → 目标 %s），写入可能被截断或报错", sc.Type, tc.Type),
				sc.Type, tc.Type, "改成一致的类型，或把这一列加入转换规则")
		}
		return
	}
	m, ok := catalog.Map(from, to, sc.Type)
	if !ok {
		lr.add(SeverityWarn, "type_unmapped", table, sc.Name,
			fmt.Sprintf("类型字典没有覆盖 %s → %s（%s）：会按 text 落地并丢语义", from, to, sc.Type),
			sc.Type, tc.Type, "确认目标列类型；必要时在类型字典里补一条（先报 Ark）")
		return
	}
	if !sameBaseType(m.Target, tc.Type) {
		lr.add(SeverityError, "type_incompatible", table, sc.Name,
			fmt.Sprintf("映射结果是 %s，目标端实际是 %s", m.Target, tc.Type), sc.Type, tc.Type,
			"确认目标表是不是由这个任务建的")
		return
	}
	if m.Loss >= catalog.NeedsConfirm {
		note := m.Note
		if note == "" {
			note = "类型语义发生了变化"
		}
		lr.add(SeverityWarn, "type_needs_confirm", table, sc.Name,
			fmt.Sprintf("%s → %s 需要人工确认：%s", sc.Type, m.Target, note), sc.Type, tc.Type, "")
	}
}

func checkPrimaryKey(lr *LevelReport, srcT, tgtT source.Table) {
	if len(srcT.PrimaryKey) == 0 {
		lr.add(SeverityWarn, "no_primary_key", srcT.Table, "",
			"源表没有主键：增量只能 insert-only，数据级校验只能比行数", "", "", "")
		return
	}
	if len(tgtT.PrimaryKey) == 0 {
		lr.add(SeverityError, "pk_mismatch", srcT.Table, "",
			"源表有主键，目标表没有：upsert 与抽样校验都不成立", join(srcT.PrimaryKey), "", "")
		return
	}
	if !sameSet(srcT.PrimaryKey, tgtT.PrimaryKey) {
		lr.add(SeverityError, "pk_mismatch", srcT.Table, "",
			"主键列不一致：重放会写错行", join(srcT.PrimaryKey), join(tgtT.PrimaryKey),
			"把目标表主键改成与源端一致")
	}
}

// ── L2 数据 ────────────────────────────────────────────────────────────

func runData(ctx context.Context, opts Options) LevelReport {
	lr := newLevel(LevelData)
	for _, name := range opts.Tables {
		lr.Tables++
		srcT, ok, err := opts.Source.Table(ctx, opts.SourceSchema, name)
		if err != nil || !ok {
			lr.add(SeverityError, "source_table_missing", name, "", "读不到源表，无法校验数据", "", "", "")
			continue
		}
		tgtT, ok, err := opts.Target.Table(ctx, opts.TargetSchema, name)
		if err != nil || !ok {
			lr.add(SeverityError, "target_table_missing", name, "", "读不到目标表，无法校验数据", "", "", "")
			continue
		}

		srcCount, err := opts.Source.Count(ctx, opts.SourceSchema, name)
		if err != nil {
			lr.add(SeverityError, "probe_failed", name, "", "源端行数读不到："+err.Error(), "", "", "")
			continue
		}
		tgtCount, err := opts.Target.Count(ctx, opts.TargetSchema, name)
		if err != nil {
			lr.add(SeverityError, "probe_failed", name, "", "目标端行数读不到："+err.Error(), "", "", "")
			continue
		}
		lr.stat("rows_source", srcCount)
		lr.stat("rows_target", tgtCount)
		if srcCount != tgtCount {
			lr.add(SeverityError, "row_count_mismatch", name, "",
				fmt.Sprintf("行数不一致：源端 %d，目标端 %d（差 %+d）", srcCount, tgtCount, tgtCount-srcCount),
				fmt.Sprint(srcCount), fmt.Sprint(tgtCount), "先确认初始化是否跑完；再用 --level data 复核")
		}

		commonCols := commonColumns(srcT, tgtT)
		if len(srcT.PrimaryKey) == 0 || len(tgtT.PrimaryKey) == 0 {
			lr.add(SeverityWarn, "no_pk_sampling", name, "",
				"没有主键：跳过抽样校验和，只比行数", "", "", "")
			continue
		}
		if len(commonCols) == 0 {
			lr.add(SeverityError, "missing_column", name, "", "两端没有共同的列，无法比对", "", "", "")
			continue
		}
		n := opts.SampleRows
		if int64(n) > srcCount {
			n = int(srcCount)
		}
		if n <= 0 {
			lr.add(SeverityWarn, "empty_table", name, "", "源表是空的：没有可抽样的行", "", "", "")
			continue
		}
		pks, err := opts.Source.SamplePKs(ctx, opts.SourceSchema, name, srcT.PrimaryKey, n)
		if err != nil {
			lr.add(SeverityError, "probe_failed", name, "", "抽样主键失败："+err.Error(), "", "", "")
			continue
		}
		lr.stat("sampled", len(pks))
		digest := fnv.New64a()
		matched := 0
		for _, pkVals := range pks {
			pkDesc := fmt.Sprint(pkVals)
			srcRow, ok, err := opts.Source.RowByPK(ctx, opts.SourceSchema, name, commonCols, srcT.PrimaryKey, pkVals)
			if err != nil {
				lr.add(SeverityError, "probe_failed", name, "", "按主键读源行失败："+err.Error(), pkDesc, "", "")
				continue
			}
			if !ok {
				lr.add(SeverityError, "source_row_vanished", name, "", "抽样后源行消失（并发写入），本次跳过", pkDesc, "", "")
				continue
			}
			tgtRow, ok, err := opts.Target.RowByPK(ctx, opts.TargetSchema, name, commonCols, tgtT.PrimaryKey, pkVals)
			if err != nil {
				lr.add(SeverityError, "probe_failed", name, "", "按主键读目标行失败："+err.Error(), pkDesc, "", "")
				continue
			}
			if !ok {
				lr.add(SeverityError, "missing_row", name, "",
					"源端有这一行，目标端没有（主键覆盖缺失）", pkDesc, "missing", "确认这条变更是否进了死信")
				continue
			}
			matched++
			srcCanon := canonicalRow(commonCols, srcRow)
			tgtCanon := canonicalRow(commonCols, tgtRow)
			digest.Write([]byte(rowDigest(srcCanon)))
			if srcCanon != tgtCanon {
				col, sv, tv := firstDiff(commonCols, srcRow, tgtRow)
				lr.add(SeverityError, "row_mismatch", name, col,
					"抽样行的内容不一致（字段级差异见 source/target）", trunc(sv, 80), trunc(tv, 80),
					"优先按主键重放该行；若反复出现，检查转换规则")
			}
		}
		lr.stat("matched", matched)
		lr.stat("checksum", hex.EncodeToString(digest.Sum(nil)))
	}
	lr.finish()
	return lr
}

// ── L3 一致性 ──────────────────────────────────────────────────────────

func runConsistency(ctx context.Context, opts Options) LevelReport {
	lr := newLevel(LevelConsistency)
	if opts.SourcePosition == nil || opts.TargetPosition == nil {
		lr.add(SeverityError, "consistency_unavailable", "", "",
			"没有提供位点探测：一致性级无法判定（这不算通过）", "", "",
			"用 --job-key 让 verify 读源端 slot/binlog 与目标端 _jl_meta.position")
		lr.finish()
		return lr
	}
	srcPos, err := opts.SourcePosition(ctx)
	if err != nil {
		lr.add(SeverityError, "probe_failed", "", "", "读源端位点失败："+err.Error(), "", "", "")
		lr.finish()
		return lr
	}
	tgtPos, err := opts.TargetPosition(ctx)
	if err != nil {
		lr.add(SeverityError, "probe_failed", "", "", "读目标端位点失败："+err.Error(), "", "", "")
		lr.finish()
		return lr
	}
	srcPos, tgtPos = strings.TrimSpace(srcPos), strings.TrimSpace(tgtPos)
	lr.stat("source_position", srcPos)
	lr.stat("target_position", tgtPos)
	switch {
	case srcPos == "":
		lr.add(SeverityError, "source_position_missing", "", "",
			"源端没有位点（没有 slot / binlog 未开启 / 任务还没开始）", "", "", "先确认任务已初始化")
	case tgtPos == "":
		lr.add(SeverityError, "target_position_missing", "", "",
			"目标端 _jl_meta.position 为空：任务还没写入过任何东西", "", "", "先跑一次初始化")
	case srcPos == tgtPos:
		lr.stat("verdict", "in_sync")
	default:
		comparePosition(&lr, srcPos, tgtPos, opts.MaxGapBytes)
	}
	lr.finish()
	return lr
}

func comparePosition(lr *LevelReport, srcPos, tgtPos string, maxGap int64) {
	// PostgreSQL: XX/XX LSN，可以直接算字节差。
	if srcLSN, srcOK := runtime.ParseLSN(srcPos); srcOK {
		if tgtLSN, tgtOK := runtime.ParseLSN(tgtPos); tgtOK {
			switch {
			case tgtLSN > srcLSN:
				lr.add(SeverityError, "target_ahead", "", "",
					"目标端位点超前源端 slot：两边不是同一份数据流（或 slot 被重建过）",
					srcPos, tgtPos, "确认 job_key 与 slot 是否对应；不要继续跑")
			case srcLSN-tgtLSN > uint64(maxGap):
				lr.add(SeverityError, "position_gap", "", "",
					fmt.Sprintf("目标端落后源端 %d 字节，超过容忍上限 %d", srcLSN-tgtLSN, maxGap),
					srcPos, tgtPos, "确认同步是否卡住；正常追平后重跑 verify")
			default:
				lr.stat("gap_bytes", int64(srcLSN-tgtLSN))
				lr.add(SeverityWarn, "position_behind", "", "",
					fmt.Sprintf("目标端落后源端 %d 字节（在容忍范围内）", srcLSN-tgtLSN),
					srcPos, tgtPos, "")
			}
			return
		}
	}
	// MySQL: file:pos。
	srcFile, srcOff, srcOK := runtime.BinlogPos(srcPos)
	tgtFile, tgtOff, tgtOK := runtime.BinlogPos(tgtPos)
	if srcOK && tgtOK {
		switch {
		case srcFile == tgtFile && tgtOff > srcOff:
			lr.add(SeverityError, "target_ahead", "", "",
				"目标端位点在同一 binlog 文件里超前源端", srcPos, tgtPos, "不要继续跑，先确认 slot/job 对应关系")
		case srcFile == tgtFile:
			if srcOff-tgtOff > maxGap {
				lr.add(SeverityError, "position_gap", "", "",
					fmt.Sprintf("目标端落后源端 %d 字节，超过容忍上限 %d", srcOff-tgtOff, maxGap),
					srcPos, tgtPos, "确认同步是否卡住")
			} else if srcOff > tgtOff {
				lr.add(SeverityWarn, "position_behind", "", "",
					fmt.Sprintf("目标端落后源端 %d 字节（在容忍范围内）", srcOff-tgtOff), srcPos, tgtPos, "")
			}
		case tgtFile > srcFile:
			lr.add(SeverityError, "target_ahead", "", "",
				"目标端位点指向更新的 binlog 文件", srcPos, tgtPos, "确认源端是否被替换过")
		default:
			lr.add(SeverityWarn, "position_behind", "", "",
				"目标端位点停在更早的 binlog 文件", srcPos, tgtPos, "确认 binlog 是否被清理；必要时 --resync")
		}
		return
	}
	// 格式不认识就明确说无法判定，绝不"字符串不等也算过"。
	lr.add(SeverityError, "position_unparseable", "", "",
		"两边位点格式不同或无法解析，不能判定", srcPos, tgtPos,
		"把 --json 输出发给维护者；这属于连接器位点格式不一致")
}

// ── helpers ────────────────────────────────────────────────────────────

func newLevel(l Level) LevelReport {
	return LevelReport{Level: l, OK: true, Stats: map[string]any{}}
}

func (lr *LevelReport) add(sev Severity, code, table, column, detail, src, tgt, hint string) {
	lr.Findings = append(lr.Findings, Finding{
		Code: code, Severity: sev, Table: table, Column: column,
		Detail: detail, Source: src, Target: tgt, Hint: hint,
	})
	if sev == SeverityError {
		lr.OK = false
	}
}

func (lr *LevelReport) stat(k string, v any) { lr.Stats[k] = v }

func (lr *LevelReport) finish() {
	switch {
	case !lr.OK:
		lr.Status = StatusMismatch
		for _, f := range lr.Findings {
			if f.Code == "probe_failed" {
				lr.Status = StatusError
				break
			}
		}
	default:
		lr.Status = StatusOK
	}
}

func commonColumns(srcT, tgtT source.Table) []string {
	tgt := map[string]bool{}
	for _, c := range tgtT.Columns {
		tgt[strings.ToLower(c.Name)] = true
	}
	var out []string
	for _, c := range srcT.Columns {
		if tgt[strings.ToLower(c.Name)] {
			out = append(out, c.Name)
		}
	}
	return out
}

// canonicalRow renders values the same way on both sides, then joins them with
// a separator that cannot appear in the escaped form.
func canonicalRow(cols []string, vals []any) string {
	var b strings.Builder
	for i, v := range vals {
		if i > 0 {
			b.WriteByte('|')
		}
		b.WriteString(canonicalValue(v))
	}
	return b.String()
}

func canonicalValue(v any) string {
	switch x := v.(type) {
	case nil:
		return `\N` // 与空串 ""、字面量 "\N" 区分
	case []byte:
		return "x'" + hex.EncodeToString(x) + "'"
	case time.Time:
		return "t" + fmt.Sprint(x.UTC().UnixMicro())
	case bool:
		if x {
			return "b1"
		}
		return "b0"
	case float32:
		return "f" + fmt.Sprint(float64(x))
	case float64:
		// 定点十进制，避免二进制尾差带来的假差异。
		return "f" + fmt.Sprintf("%.15g", x)
	case int:
		return "i" + fmt.Sprint(int64(x))
	case int8, int16, int32, int64:
		return "i" + fmt.Sprint(x)
	case uint, uint8, uint16, uint32, uint64:
		return "u" + fmt.Sprint(x)
	case string:
		return "s" + x
	default:
		return "?" + fmt.Sprint(x)
	}
}

func rowDigest(canon string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(canon))
	return fmt.Sprintf("%016x", h.Sum64())
}

func firstDiff(cols []string, a, b []any) (string, string, string) {
	n := len(cols)
	if len(a) < n {
		n = len(a)
	}
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if canonicalValue(a[i]) != canonicalValue(b[i]) {
			return cols[i], canonicalValue(a[i]), canonicalValue(b[i])
		}
	}
	if len(a) != len(b) {
		return "", fmt.Sprint(len(a)), fmt.Sprint(len(b))
	}
	return "", "", ""
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func join(ss []string) string { return strings.Join(ss, ",") }

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	x := map[string]bool{}
	for _, s := range a {
		x[strings.ToLower(s)] = true
	}
	for _, s := range b {
		if !x[strings.ToLower(s)] {
			return false
		}
	}
	return true
}

func nullableText(b bool) string {
	if b {
		return "NULL allowed"
	}
	return "NOT NULL"
}

// nullableSafe: source NULL must not hit a NOT NULL target.
func nullableSafe(src, tgt source.Column) bool {
	return !(src.Nullable && !tgt.Nullable)
}

// sameBaseType compares the base type names, tolerating PG/MySQL aliases.
func sameBaseType(a, b string) bool {
	return normalizeBase(a) == normalizeBase(b)
}

func normalizeBase(t string) string {
	s := strings.ToLower(strings.TrimSpace(t))
	if i := strings.IndexByte(s, '('); i >= 0 {
		s = strings.TrimSpace(s[:i])
	}
	s = strings.Join(strings.Fields(s), " ")
	switch s {
	case "int4", "integer":
		return "int"
	case "int8":
		return "bigint"
	case "int2":
		return "smallint"
	case "bool":
		return "boolean"
	case "character varying", "varchar":
		return "varchar"
	case "character", "bpchar":
		return "char"
	case "float8", "double precision":
		return "double"
	case "float4":
		return "real"
	case "numeric":
		return "decimal"
	case "timestamp without time zone":
		return "timestamp"
	case "timestamp with time zone":
		return "timestamptz"
	case "time without time zone":
		return "time"
	case "time with time zone":
		return "timetz"
	}
	return s
}
