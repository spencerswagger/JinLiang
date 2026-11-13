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
	"errors"
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
	// Mode records what actually ran: structure/data/consistency/all/
	// aggregate_only. The human conclusion must not say "三级通过" for a
	// one-tier run (the exact "green says more than it did" bug).
	Mode string `json:"mode,omitempty"`
	// Conclusion is the exact one-line human conclusion, also emitted in JSON
	// so CI/AI consumers do not re-implement (and drift from) the CLI wording.
	Conclusion string `json:"conclusion,omitempty"`
	// Fingerprint says whether the two sides' fingerprints are comparable at
	// all (cross-engine fingerprints never are).
	Fingerprint FingerprintInfo `json:"fingerprint"`
	OK          bool            `json:"ok"`
}

// Side is the read-only view verify needs from one endpoint. *runtime.DB
// implements it; tests use a fake.
type Side interface {
	Kind() string
	Table(ctx context.Context, schema, table string) (source.Table, bool, error)
	Count(ctx context.Context, schema, table string) (int64, error)
	SamplePKs(ctx context.Context, schema, table string, pk []string, n int) ([][]any, error)
	RowByPK(ctx context.Context, schema, table string, columns, pk []string, values []any) ([]any, bool, error)
	// ScanOrdered streams every row in PK order (no PK: table order). Data-level
	// verification must compare aggregates over the full table; sampling is only
	// supplementary evidence.
	ScanOrdered(ctx context.Context, schema, table string, columns, pk []string, fn func([]any) error) error
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
	// AggregateOnly runs only the cheap SQL aggregate tier (count/sum/min/max/
	// text_len/nulls). Use it for daily checks; full row scan is for release/weekly.
	AggregateOnly bool

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
		Mode:        string(level),
		Fingerprint: FingerprintComparable(opts.Source, opts.Target),
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
	// Mode 由「实际执行的级 + 是否降级」推导，不由 flag 推导。
	// 反例：--level structure --aggregate-only 根本没跑聚合，不能标 aggregate_only；
	// --level all 遇到无主键表会降级成只聚合，不能只说「三级通过」。
	rep.Mode = deriveMode(rep.Levels)
	rep.Conclusion = Conclusion(rep)
	return rep, nil
}

// 判据（比枚举更重要）：降级必须「要么推到 Stats 让结论跟着变、要么升成
// SeverityError 让整行不通过」，**不许「跳过后仍算 OK」**。加新 level 或新降级
// 路径时先问「结论会跟着变吗」——结论由**执行记录**推导，不由配置推导；
// 一句概括永远会在某个分支上说多或说少。
//
// deriveMode records what actually ran. It is derived from the executed tiers
// and their stats, never from the requested flags: a flag can be set while the
// tier did nothing (structure + aggregate-only), and a tier can silently
// degrade because of data shape (no-PK tables).
func deriveMode(levels []LevelReport) string {
	names := make([]string, 0, len(levels))
	var data *LevelReport
	for i := range levels {
		names = append(names, string(levels[i].Level))
		if levels[i].Level == LevelData {
			data = &levels[i]
		}
	}
	base := strings.Join(names, "+")
	if len(levels) == 3 {
		base = "all"
	}
	if data == nil {
		return base
	}
	full := statInt(*data, "tables_full_scan")
	agg := statInt(*data, "tables_aggregate_only")
	if base == "all" {
		if agg > 0 {
			return "all_partial"
		}
		return "all"
	}
	switch {
	case full == 0 && agg > 0:
		return "aggregate_only"
	case agg > 0:
		return base + "_partial"
	}
	return base
}

// Conclusion renders one line that says what was actually done — and what was
// not. A single summary sentence ("三级通过") will always overclaim on some
// branch; enumerating the executed tiers is harder to lie with.
func Conclusion(rep Report) string {
	if !rep.OK {
		return "结论：有级别未通过（详见上面的 code）"
	}
	labels := map[Level]string{
		LevelStructure: "结构级", LevelData: "数据级", LevelConsistency: "一致性",
	}
	parts := make([]string, 0, len(rep.Levels))
	for _, lr := range rep.Levels {
		label := labels[lr.Level]
		if label == "" {
			label = string(lr.Level)
		}
		part := label + " ✓"
		if lr.Level == LevelData {
			full := statInt(lr, "tables_full_scan")
			agg := statInt(lr, "tables_aggregate_only")
			noPK := statInt(lr, "tables_no_pk")
			note := ""
			switch {
			case full == 0 && agg > 0:
				note = "仅聚合，未做行级全表比对"
				if noPK > 0 {
					note += "；无主键表只能聚合"
				}
			case agg > 0:
				note = fmt.Sprintf("行级全表比对 %d 表；%d 表仅聚合", full, agg)
				if noPK > 0 {
					note += "（无主键）"
				}
			case full > 0:
				note = fmt.Sprintf("行级全表比对 %d 表", full)
			}
			// 指纹不可比与「跑了几张表」无关：聚合级、混合级也要说。
			// 但措辞不能自相矛盾：只跑聚合时不能说「已改用逐行比对」。
			if v, ok := lr.Stats["fingerprint_comparable"]; ok {
				if b, ok := v.(bool); ok && !b {
					fpNote := "跨端指纹不可比，已改用逐行比对"
					if full == 0 && agg > 0 {
						fpNote = "聚合级不涉及指纹比对"
					}
					if note != "" {
						note += "；"
					}
					note += fpNote
				}
			}
			if note != "" {
				part += "（" + note + "）"
			}
		}
		parts = append(parts, part)
	}
	return "结论：" + strings.Join(parts, " / ")
}

func statInt(lr LevelReport, key string) int {
	if v, ok := lr.Stats[key]; ok {
		switch n := v.(type) {
		case int:
			return n
		case int64:
			return int(n)
		case float64:
			return int(n)
		}
	}
	return 0
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
			"这张表没有主键：本版只比行数（声明 unique_key 后仍不做行级比对，见挂账 D2）", "", "", "")
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
	fullScanTables, aggregateOnlyTables, noPKTables := 0, 0, 0
	fp := FingerprintComparable(opts.Source, opts.Target)
	lr.stat("fingerprint_comparable", fp.Comparable)
	if !fp.Comparable {
		lr.add(SeverityWarn, "fingerprint_scope_mismatch", "", "", fp.Reason, fp.SourceScope, fp.TargetScope,
			"不需要处理：verify 已自动改用逐行全表比对；这条只是告诉你不要拿两端指纹直接比")
	}
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
		if len(commonCols) == 0 {
			lr.add(SeverityError, "missing_column", name, "", "两端没有共同的列，无法比对", "", "", "")
			continue
		}
		if opts.AggregateOnly {
			aggregateOnlyTables++
			runAggregateSQL(ctx, &lr, opts, name, srcT, tgtT, commonCols)
			continue
		}
		if len(srcT.PrimaryKey) == 0 || len(tgtT.PrimaryKey) == 0 {
			noPKTables++
			aggregateOnlyTables++
			lr.add(SeverityWarn, "no_pk_aggregate", name, "",
				"没有主键：只做全表聚合比对，无法逐行定位", "", "", "")
			runAggregateOnly(ctx, &lr, opts, name, srcT, tgtT, commonCols)
			continue
		}

		fullScanTables++
		matched := runDataFull(ctx, &lr, opts, name, srcT, tgtT, commonCols)
		lr.stat("matched", matched)

		// 抽样是补充证据：只有聚合与全表扫描都没有发现差异时才跑，
		// 绝不能反过来用抽样结果当判据（Owl #44/P0 的根因）。
		if lr.OK && opts.SampleRows > 0 {
			runSampling(ctx, &lr, opts, name, srcT, tgtT, commonCols, srcCount)
		}
	}
	lr.stat("tables_full_scan", fullScanTables)
	lr.stat("tables_aggregate_only", aggregateOnlyTables)
	lr.stat("tables_no_pk", noPKTables)
	lr.finish()
	return lr
}

// runSampling is deliberately supplementary: it runs only after the full scan
// and aggregates agree, and can add evidence but never override a green.
func runSampling(ctx context.Context, lr *LevelReport, opts Options, name string,
	srcT, tgtT source.Table, commonCols []string, srcCount int64) {
	n := opts.SampleRows
	if int64(n) > srcCount {
		n = int(srcCount)
	}
	if n <= 0 {
		lr.add(SeverityWarn, "empty_table", name, "", "源表是空的：没有可抽样的行", "", "", "")
		return
	}
	pks, err := opts.Source.SamplePKs(ctx, opts.SourceSchema, name, srcT.PrimaryKey, n)
	if err != nil {
		lr.add(SeverityError, "probe_failed", name, "", "抽样主键失败："+err.Error(), "", "", "")
		return
	}
	lr.stat("sampled", len(pks))
	digest := fnv.New64a()
	matched := 0
	for _, pkVals := range pks {
		pkDesc := fmt.Sprint(pkVals)
		srcRow, ok, err := opts.Source.RowByPK(ctx, opts.SourceSchema, name, commonCols, srcT.PrimaryKey, pkVals)
		if err != nil || !ok {
			lr.add(SeverityError, "probe_failed", name, "", "抽样读源行失败："+errText(err, ok), pkDesc, "", "")
			continue
		}
		tgtRow, ok, err := opts.Target.RowByPK(ctx, opts.TargetSchema, name, commonCols, tgtT.PrimaryKey, pkVals)
		if err != nil || !ok {
			lr.add(SeverityError, "missing_row", name, "", "抽样时目标端没有这一行", pkDesc, "missing",
				"确认这条变更是否进了死信")
			continue
		}
		matched++
		agg := newTableAgg(srcT, commonCols)
		srcCanon := canonicalRowNumeric(commonCols, srcRow, agg.numeric, agg.textual)
		tgtCanon := canonicalRowNumeric(commonCols, tgtRow, agg.numeric, agg.textual)
		digest.Write([]byte(rowDigest(srcCanon)))
		if srcCanon != tgtCanon {
			col, sv, tv := firstDiffNumeric(commonCols, srcRow, tgtRow, agg.numeric, agg.textual)
			lr.add(SeverityError, "row_mismatch", name, col,
				"抽样行的内容不一致（全表扫描后又被改？）", trunc(sv, 80), trunc(tv, 80),
				"确认是否有并发写入；按主键重放该行")
		}
	}
	lr.stat("sample_matched", matched)
	lr.stat("checksum", hex.EncodeToString(digest.Sum(nil)))
}

func errText(err error, found bool) string {
	if err != nil {
		return err.Error()
	}
	if !found {
		return "行不存在"
	}
	return "未知"
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
		comparePosition(&lr, srcPos, tgtPos, opts.MaxGapBytes, opts.Source.Kind())
	}
	lr.finish()
	return lr
}

// comparePosition picks the parser by the source connector's kind: the
// position in `_jl_meta` is always written by the source side.
func comparePosition(lr *LevelReport, srcPos, tgtPos string, maxGap int64, kind string) {
	if kind == runtime.KindMySQL {
		compareBinlog(lr, srcPos, tgtPos, maxGap)
		return
	}
	// PostgreSQL: 裸 LSN 与版本化 postgres:v1:<lsn> 都要认（后者是引擎写的）。
	srcLSN, srcErr := runtime.ParseLSN(srcPos)
	tgtLSN, tgtErr := runtime.ParseLSN(tgtPos)
	if srcErr != nil || tgtErr != nil {
		if errors.Is(srcErr, runtime.ErrPositionKindMismatch) || errors.Is(tgtErr, runtime.ErrPositionKindMismatch) {
			lr.add(SeverityError, "position_kind_mismatch", "", "",
				"位点属于另一种数据库（E1010）：不是格式坏了，是目标库里的位点来自别的源类型",
				srcPos, tgtPos, "确认 job_key 与源类型是否换过；不要用 --resync 掩盖")
			return
		}
		lr.add(SeverityError, "position_unparseable", "", "",
			"两边位点格式不同或无法解析，不能判定", srcPos, tgtPos,
			"把 --json 输出发给维护者；这属于连接器位点格式不一致")
		return
	}
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
}

func compareBinlog(lr *LevelReport, srcPos, tgtPos string, maxGap int64) {
	srcFile, srcOff, srcErr := runtime.ParseBinlogPos(srcPos)
	tgtFile, tgtOff, tgtErr := runtime.ParseBinlogPos(tgtPos)
	if srcErr != nil || tgtErr != nil {
		if errors.Is(srcErr, runtime.ErrPositionKindMismatch) || errors.Is(tgtErr, runtime.ErrPositionKindMismatch) {
			lr.add(SeverityError, "position_kind_mismatch", "", "",
				"位点属于另一种数据库（E1010）：目标库里的位点来自别的源类型",
				srcPos, tgtPos, "确认 job_key 与源类型是否换过")
			return
		}
		lr.add(SeverityError, "position_unparseable", "", "",
			"两边位点格式不同或无法解析，不能判定", srcPos, tgtPos,
			"把 --json 输出发给维护者；这属于连接器位点格式不一致")
		return
	}
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
	return canonicalRowNumeric(cols, vals, nil, nil)
}

// canonicalRowNumeric is canonicalRow with the numeric-column set applied.
// numeric/decimal come back from database/sql as strings ("1.0000", "1.00");
// comparing those as text makes every cross-scale pair look different even
// though L1's big.Rat says equal. Numeric columns must use RatString().
func canonicalRowNumeric(cols []string, vals []any, numeric, textual map[string]bool) string {
	var b strings.Builder
	for i, v := range vals {
		if i > 0 {
			b.WriteByte('|')
		}
		name := ""
		if i < len(cols) {
			name = cols[i]
		}
		b.WriteString(canonicalValueFor(name, v, numeric, textual))
	}
	return b.String()
}

func canonicalValueFor(col string, v any, numeric, textual map[string]bool) string {
	key := strings.ToLower(col)
	if numeric[key] || numeric[col] {
		if r, ok := toRat(v); ok {
			return "n" + r.RatString()
		}
	}
	if textual[key] || textual[col] {
		switch x := v.(type) {
		case []byte:
			return "s" + string(x)
		case string:
			return "s" + x
		}
	}
	return canonicalValue(v)
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
	return firstDiffNumeric(cols, a, b, nil, nil)
}

func firstDiffNumeric(cols []string, a, b []any, numeric, textual map[string]bool) (string, string, string) {
	n := len(cols)
	if len(a) < n {
		n = len(a)
	}
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		av := canonicalValueFor(cols[i], a[i], numeric, textual)
		bv := canonicalValueFor(cols[i], b[i], numeric, textual)
		if av != bv {
			return cols[i], av, bv
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
