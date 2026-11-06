package verify

import (
	"context"
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/source"
)

// 这四条是「校验器会不会说谎」的唯一验收标准：目标端被动过，verify 必须判失败
// 并指到具体行/列。先写测试、确认现版本会红，再改实现。
//
// 注意 SampleRows=1：故意让抽样只覆盖第 1 行，证明「抽样通过」不能当判据。

func dataOnly(t *testing.T, src, tgt *fakeSide) Options {
	t.Helper()
	opts := baseOptions(src, tgt)
	opts.Level = LevelData
	opts.SampleRows = 1
	return opts
}

func TestAggregateCatchesOneValueChange(t *testing.T) {
	src, tgt := newFake("postgres"), newFake("postgres")
	src.add(ordersTable(), ordersRows()...)
	rows := cloneRows(ordersRows())
	rows[2]["amount"] = "31.00" // 第 3 行被改，抽样只覆盖第 1 行
	tgt.add(ordersTable(), rows...)

	rep, err := Run(context.Background(), dataOnly(t, src, tgt))
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK {
		t.Fatalf("目标端改了 1 行，verify 仍报通过：%+v", rep.Levels[0])
	}
	f := findFinding(rep, "row_mismatch")
	if f == nil || f.Table != "orders" || f.Column != "amount" || !strings.Contains(f.Detail, "主键 3") {
		t.Fatalf("必须指到具体行/列（orders.amount, 主键 3），实际 findings=%+v", rep.Levels[0].Findings)
	}
}

func TestAggregateCatchesDeletedRow(t *testing.T) {
	src, tgt := newFake("postgres"), newFake("postgres")
	src.add(ordersTable(), ordersRows()...)
	tgt.add(ordersTable(), ordersRows()[:2]...) // 删掉第 3 行

	rep, err := Run(context.Background(), dataOnly(t, src, tgt))
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK || !hasCode(rep, "row_count_mismatch") {
		t.Fatalf("删 1 行必须判失败并指出行数差异：%+v", rep.Levels[0])
	}
	f := findFinding(rep, "missing_row")
	if f == nil || f.Table != "orders" || !strings.Contains(f.Detail, "主键 3") {
		t.Fatalf("必须指到缺的那一行（主键 3），实际 findings=%+v", rep.Levels[0].Findings)
	}
}

func TestAggregateCatchesPrecisionChange(t *testing.T) {
	src, tgt := newFake("postgres"), newFake("postgres")
	tab := source.Table{
		TableRef: source.TableRef{Schema: "public", Table: "metrics"},
		Columns: []source.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "v", Type: "numeric(12,4)", Nullable: false},
		},
		PrimaryKey: []string{"id"}, HasStablePK: true,
	}
	src.add(tab,
		map[string]any{"id": int64(1), "v": "1.2340"},
		map[string]any{"id": int64(2), "v": "2.3450"},
		map[string]any{"id": int64(3), "v": "3.4560"},
	)
	tgt.add(tab,
		map[string]any{"id": int64(1), "v": "1.2340"},
		map[string]any{"id": int64(2), "v": "2.3400"}, // 2.3450 -> 2.3400：精度被截
		map[string]any{"id": int64(3), "v": "3.4560"},
	)
	opts := baseOptions(src, tgt)
	opts.Tables = []string{"metrics"}
	opts.Level = LevelData
	opts.SampleRows = 1

	rep, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK {
		t.Fatalf("精度变化没被发现：%+v", rep.Levels[0])
	}
	f := findFinding(rep, "row_mismatch")
	if f == nil || f.Table != "metrics" || f.Column != "v" || !strings.Contains(f.Detail, "主键 2") {
		t.Fatalf("必须指到具体行/列（metrics.v, 主键 2），实际 findings=%+v", rep.Levels[0].Findings)
	}
	if !hasCode(rep, "aggregate_mismatch") {
		t.Fatalf("聚合 sum/min/max 也应报不一致，实际 findings=%+v", rep.Levels[0].Findings)
	}
}

// 同一逻辑值不同 scale 必须判相等（PG numeric(12,4) 的 "1.0000" 与 MySQL
// decimal(12,2) 的 "1.00" 是同一个数；按文本比会天天假红）。
func TestNumericSameValueDifferentScaleIsEqual(t *testing.T) {
	src, tgt := newFake("postgres"), newFake("mysql")
	tab := source.Table{
		TableRef: source.TableRef{Schema: "public", Table: "amounts"},
		Columns: []source.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "v", Type: "numeric(12,4)", Nullable: false},
		},
		PrimaryKey: []string{"id"}, HasStablePK: true,
	}
	src.add(tab,
		map[string]any{"id": int64(1), "v": "1.0000"},
		map[string]any{"id": int64(2), "v": "0.10"},
	)
	tgt.add(tab,
		map[string]any{"id": int64(1), "v": "1.00"},
		map[string]any{"id": int64(2), "v": "0.1"},
	)
	opts := baseOptions(src, tgt)
	opts.Tables = []string{"amounts"}
	opts.Level = LevelData
	opts.SampleRows = 10
	rep, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK {
		t.Fatalf("同一逻辑值不同 scale 必须判相等，实际 findings=%+v", rep.Levels[0].Findings)
	}
}

func TestAggregateCatchesSwappedRows(t *testing.T) {
	src, tgt := newFake("postgres"), newFake("postgres")
	src.add(ordersTable(), ordersRows()...)
	rows := cloneRows(ordersRows())
	// 两行互换：行数不变，count/sum/min/max 也可能完全不变——只有行级/顺序敏感
	// 的证据才能发现。第 2、3 行互换 name/amount。
	rows[1]["name"], rows[2]["name"] = rows[2]["name"], rows[1]["name"]
	rows[1]["amount"], rows[2]["amount"] = rows[2]["amount"], rows[1]["amount"]
	tgt.add(ordersTable(), rows...)

	rep, err := Run(context.Background(), dataOnly(t, src, tgt))
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK {
		t.Fatalf("两行互换没被发现（count/sum/min/max 都不变）：%+v", rep.Levels[0])
	}
	if findFinding(rep, "row_mismatch") == nil {
		t.Fatalf("必须指到互换的两行，实际 findings=%+v", rep.Levels[0].Findings)
	}
	b := findFinding(rep, "block_checksum_mismatch")
	if b == nil || b.Table != "orders" || !strings.Contains(b.Detail, "第 0 块") {
		t.Fatalf("必须指到具体块（第 0 块），实际 findings=%+v", rep.Levels[0].Findings)
	}
}

func findFinding(rep Report, code string) *Finding {
	for _, lr := range rep.Levels {
		for i := range lr.Findings {
			if lr.Findings[i].Code == code {
				return &lr.Findings[i]
			}
		}
	}
	return nil
}

var _ = source.TableRef{}

// float32(0.1) 先转 float64 会带出二进制尾数；必须按 bitSize=32 格式化，
// 否则 PG real / MySQL FLOAT 列会几乎每行误报。
func TestFloat32SameValueDifferentEncoding(t *testing.T) {
	tab := source.Table{
		TableRef: source.TableRef{Schema: "public", Table: "floats"},
		Columns: []source.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "v", Type: "real", Nullable: false},
		},
		PrimaryKey: []string{"id"}, HasStablePK: true,
	}
	src, tgt := newFake("postgres"), newFake("mysql")
	src.add(tab, map[string]any{"id": int64(1), "v": float32(0.1)})
	tgt.add(tab, map[string]any{"id": int64(1), "v": "0.1"})
	opts := baseOptions(src, tgt)
	opts.Tables = []string{"floats"}
	opts.Level = LevelData
	rep, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK {
		t.Fatalf("float32(0.1) 与 \"0.1\" 必须相等：%+v", rep.Levels[0].Findings)
	}

	// 反例：不同值必须报（证明不是把所有 float 都判相等）。
	tgt2 := newFake("mysql")
	tgt2.add(tab, map[string]any{"id": int64(1), "v": "0.2"})
	opts.Target = tgt2
	rep, err = Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK {
		t.Fatal("float32(0.1) 与 \"0.2\" 不应相等")
	}
}

// 结论必须与「实际跑了几级」一致：只跑 L1 不能印「三级通过」。
func TestReportModeReflectsWhatRan(t *testing.T) {
	src, tgt := newFake("postgres"), newFake("postgres")
	src.add(ordersTable(), ordersRows()...)
	tgt.add(ordersTable(), ordersRows()...)
	opts := baseOptions(src, tgt)
	opts.Level = LevelData
	rep, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Mode != "data" {
		t.Fatalf("mode=%q, want data", rep.Mode)
	}
	opts.AggregateOnly = true
	rep, _ = Run(context.Background(), opts)
	if rep.Mode != "aggregate_only" {
		t.Fatalf("mode=%q, want aggregate_only", rep.Mode)
	}
}

// 源 numeric → 目标 text：两端按各自类型聚合，必须报类型不一致而不是
// 拿源端 numeric 集合去算目标端文本（P3）。
func TestAggregateNumericVsTextReportsMismatch(t *testing.T) {
	srcTab := source.Table{
		TableRef: source.TableRef{Schema: "public", Table: "mixed"},
		Columns: []source.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "v", Type: "numeric(12,2)", Nullable: false},
		},
		PrimaryKey: []string{"id"}, HasStablePK: true,
	}
	tgtTab := srcTab
	tgtTab.Columns = []source.Column{
		{Name: "id", Type: "bigint", Nullable: false},
		{Name: "v", Type: "text", Nullable: false},
	}
	src, tgt := newFake("postgres"), newFake("mysql")
	src.add(srcTab, map[string]any{"id": int64(1), "v": "1.00"})
	tgt.add(tgtTab, map[string]any{"id": int64(1), "v": "1.00"})
	opts := baseOptions(src, tgt)
	opts.Tables = []string{"mixed"}
	opts.Level = LevelData
	rep, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK || !hasCode(rep, "aggregate_mismatch") {
		t.Fatalf("numeric→text 必须报聚合类型不一致：%+v", rep.Levels[0].Findings)
	}
}

func TestConclusionAllNoDegradation(t *testing.T) {
	src, tgt := newFake("postgres"), newFake("postgres")
	src.add(ordersTable(), ordersRows()...)
	tgt.add(ordersTable(), ordersRows()...)
	rep, err := Run(context.Background(), baseOptions(src, tgt))
	if err != nil {
		t.Fatal(err)
	}
	if rep.Mode != "all" {
		t.Fatalf("mode=%q, want all", rep.Mode)
	}
	got := Conclusion(rep)
	want := "结论：结构级 ✓ / 数据级 ✓（行级全表比对 1 表） / 一致性 ✓"
	if got != want {
		t.Fatalf("conclusion=%q, want %q", got, want)
	}
	if rep.Conclusion != want {
		t.Fatalf("JSON conclusion=%q, want %q", rep.Conclusion, want)
	}
}

func TestLevelStructureAggregateOnlyMode(t *testing.T) {
	src, tgt := newFake("postgres"), newFake("postgres")
	src.add(ordersTable(), ordersRows()...)
	tgt.add(ordersTable(), ordersRows()...)
	opts := baseOptions(src, tgt)
	opts.Level = LevelStructure
	opts.AggregateOnly = true
	rep, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Mode != "structure" {
		t.Fatalf("--level structure --aggregate-only 没跑聚合，mode 不能是 %q", rep.Mode)
	}
	if got := Conclusion(rep); got != "结论：结构级 ✓" {
		t.Fatalf("conclusion=%q", got)
	}
}

func TestConclusionNoPKAllShowsQualifier(t *testing.T) {
	src, tgt := newFake("postgres"), newFake("postgres")
	tab := source.Table{
		TableRef: source.TableRef{Schema: "public", Table: "logs"},
		Columns: []source.Column{
			{Name: "message", Type: "text", Nullable: true},
			{Name: "n", Type: "bigint", Nullable: true},
		},
		HasStablePK: false, // 无主键：只能聚合，不能逐行比对
	}
	rows := []map[string]any{
		{"message": "a", "n": int64(1)},
		{"message": "b", "n": int64(2)},
	}
	src.add(tab, rows...)
	tgt.add(tab, rows...)
	opts := baseOptions(src, tgt)
	opts.Tables = []string{"logs"}
	rep, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Mode != "all_partial" {
		t.Fatalf("无主键表应降级为 all_partial，实际 %q", rep.Mode)
	}
	got := Conclusion(rep)
	if !strings.Contains(got, "仅聚合") || !strings.Contains(got, "无主键") {
		t.Fatalf("结论必须带「只做聚合」的限定语，实际 %q", got)
	}
	if strings.Contains(got, "结论：三级校验通过") {
		t.Fatalf("无主键降级后不能再说三级通过：%q", got)
	}
}

func TestFingerprintScopeMismatchUsesRowScan(t *testing.T) {
	src, tgt := newFake("postgres"), newFake("mysql")
	src.scope = "pg-hashtextextended-v1"
	tgt.scope = "mysql-canon-v1"
	src.add(ordersTable(), ordersRows()...)
	tgt.add(ordersTable(), ordersRows()...)
	opts := baseOptions(src, tgt)
	opts.Level = LevelData
	rep, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Fingerprint.Comparable {
		t.Fatal("PG 与 MySQL 的指纹作用域不同，不能比")
	}
	if !hasCode(rep, "fingerprint_scope_mismatch") {
		t.Fatalf("必须明确报告指纹不可比：%+v", rep.Levels[0].Findings)
	}
	if !strings.Contains(Conclusion(rep), "跨端指纹不可比") {
		t.Fatalf("结论必须说明已改用逐行比对：%q", Conclusion(rep))
	}
	if n := statInt(rep.Levels[0], "tables_full_scan"); n != 1 {
		t.Fatalf("必须真的逐行扫描了 1 表，实际 %d", n)
	}
}

func TestFingerprintSameScopeComparable(t *testing.T) {
	src, tgt := newFake("mysql"), newFake("mysql")
	src.scope = "mysql-canon-v1"
	tgt.scope = "mysql-canon-v1"
	src.add(ordersTable(), ordersRows()...)
	tgt.add(ordersTable(), ordersRows()...)
	opts := baseOptions(src, tgt)
	opts.Level = LevelData
	rep, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Fingerprint.Comparable {
		t.Fatal("同 scope 应该可比")
	}
	if hasCode(rep, "fingerprint_scope_mismatch") {
		t.Fatalf("同 scope 不该报警：%+v", rep.Levels[0].Findings)
	}
}

// 指纹不可比与「跑了几张表」无关：只跑聚合时也要在结论里说。
func TestConclusionFingerprintNoteIndependentOfTables(t *testing.T) {
	rep := Report{OK: true, Levels: []LevelReport{{
		Level: LevelData, OK: true,
		Stats: map[string]any{
			"tables_full_scan": 0, "tables_aggregate_only": 2, "tables_no_pk": 0,
			"fingerprint_comparable": false,
		},
	}}}
	got := Conclusion(rep)
	if !strings.Contains(got, "仅聚合") || !strings.Contains(got, "跨端指纹不可比") {
		t.Fatalf("聚合级结论也必须带指纹不可比：%q", got)
	}
}
