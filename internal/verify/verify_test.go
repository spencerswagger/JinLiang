package verify

import (
	"context"
	"fmt"
	"testing"

	"github.com/spencerswagger/jinliang/internal/source"
)

// fakeSide is an in-memory endpoint. The point is to prove the checks can go
// red — a verifier that only ever prints green is worse than none.
type fakeSide struct {
	kind   string
	scope  string
	tables map[string]*fakeTable
	order  []string
}

func (f *fakeSide) FingerprintScope() string { return f.scope }

type fakeTable struct {
	table source.Table
	rows  []map[string]any
}

func newFake(kind string) *fakeSide { return &fakeSide{kind: kind, tables: map[string]*fakeTable{}} }

func (f *fakeSide) Kind() string { return f.kind }

func (f *fakeSide) add(t source.Table, rows ...map[string]any) {
	f.tables[t.Table] = &fakeTable{table: t, rows: rows}
	f.order = append(f.order, t.Table)
}

func (f *fakeSide) Table(_ context.Context, _, name string) (source.Table, bool, error) {
	ft, ok := f.tables[name]
	if !ok {
		return source.Table{}, false, nil
	}
	return ft.table, true, nil
}

func (f *fakeSide) Count(_ context.Context, _, name string) (int64, error) {
	ft, ok := f.tables[name]
	if !ok {
		return 0, fmt.Errorf("no table %s", name)
	}
	return int64(len(ft.rows)), nil
}

func (f *fakeSide) SamplePKs(_ context.Context, _, name string, pk []string, n int) ([][]any, error) {
	ft, ok := f.tables[name]
	if !ok {
		return nil, fmt.Errorf("no table %s", name)
	}
	var out [][]any
	for i, row := range ft.rows {
		if i >= n {
			break
		}
		vals := make([]any, len(pk))
		for j, c := range pk {
			vals[j] = row[c]
		}
		out = append(out, vals)
	}
	return out, nil
}

func (f *fakeSide) ScanOrdered(_ context.Context, _, name string, columns, _ []string, fn func([]any) error) error {
	ft, ok := f.tables[name]
	if !ok {
		return fmt.Errorf("no table %s", name)
	}
	for _, row := range ft.rows {
		vals := make([]any, len(columns))
		for i, c := range columns {
			vals[i] = row[c]
		}
		if err := fn(vals); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeSide) RowByPK(_ context.Context, _, name string, columns, pk []string, values []any) ([]any, bool, error) {
	ft, ok := f.tables[name]
	if !ok {
		return nil, false, fmt.Errorf("no table %s", name)
	}
	for _, row := range ft.rows {
		match := true
		for i, c := range pk {
			if !valuesEqual(row[c], values[i]) {
				match = false
				break
			}
		}
		if !match {
			continue
		}
		out := make([]any, len(columns))
		for i, c := range columns {
			out[i] = row[c]
		}
		return out, true, nil
	}
	return nil, false, nil
}

func valuesEqual(a, b any) bool { return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b) }

func ordersTable() source.Table {
	return source.Table{
		TableRef: source.TableRef{Schema: "public", Table: "orders"},
		Columns: []source.Column{
			{Name: "id", Type: "bigint", Nullable: false},
			{Name: "name", Type: "text", Nullable: true},
			{Name: "amount", Type: "numeric(12,2)", Nullable: false},
		},
		PrimaryKey:  []string{"id"},
		HasStablePK: true,
	}
}

func ordersRows() []map[string]any {
	return []map[string]any{
		{"id": int64(1), "name": "alice", "amount": "10.00"},
		{"id": int64(2), "name": "bob", "amount": "20.00"},
		{"id": int64(3), "name": nil, "amount": "30.00"},
	}
}

func cloneRows(rows []map[string]any) []map[string]any {
	out := make([]map[string]any, len(rows))
	for i, r := range rows {
		cp := map[string]any{}
		for k, v := range r {
			cp[k] = v
		}
		out[i] = cp
	}
	return out
}

func passthroughPositions() (func(ctx context.Context) (string, error), func(ctx context.Context) (string, error)) {
	return func(context.Context) (string, error) { return "0/16B6C50", nil },
		func(context.Context) (string, error) { return "0/16B6C50", nil }
}

func baseOptions(src, tgt *fakeSide) Options {
	sp, tp := passthroughPositions()
	return Options{
		Source: src, Target: tgt,
		SourceSchema: "public", TargetSchema: "public",
		Tables:         []string{"orders"},
		Level:          LevelAll,
		JobKey:         "abc1234567",
		SourcePosition: sp, TargetPosition: tp,
	}
}

func TestRunAllLevelsGreen(t *testing.T) {
	src, tgt := newFake("postgres"), newFake("postgres")
	src.add(ordersTable(), ordersRows()...)
	tgt.add(ordersTable(), ordersRows()...)

	rep, err := Run(context.Background(), baseOptions(src, tgt))
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK {
		t.Fatalf("expected green, got %+v", rep)
	}
	if len(rep.Levels) != 3 {
		t.Fatalf("expected 3 levels, got %d", len(rep.Levels))
	}
	for _, lr := range rep.Levels {
		if lr.Status != StatusOK {
			t.Fatalf("level %s status %s findings %+v", lr.Level, lr.Status, lr.Findings)
		}
	}
}

func TestStructureRedOnMissingColumnAndType(t *testing.T) {
	src, tgt := newFake("postgres"), newFake("postgres")
	src.add(ordersTable(), ordersRows()...)
	bad := ordersTable()
	bad.Columns = []source.Column{
		{Name: "id", Type: "bigint", Nullable: false},
		{Name: "amount", Type: "integer", Nullable: false},
	}
	tgt.add(bad, ordersRows()...)

	opts := baseOptions(src, tgt)
	opts.Level = LevelStructure
	rep, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK {
		t.Fatal("structure check stayed green although a column is missing and another changed type")
	}
	if !hasCode(rep, "missing_column") || !hasCode(rep, "type_incompatible") {
		t.Fatalf("missing expected findings: %+v", rep.Levels[0].Findings)
	}
}

func TestDataRedOnRowCountAndValue(t *testing.T) {
	src, tgt := newFake("postgres"), newFake("postgres")
	src.add(ordersTable(), ordersRows()...)
	rows := cloneRows(ordersRows())
	rows[1]["amount"] = "20.01" // 值被改了
	rows = append(rows, map[string]any{"id": int64(4), "name": "dave", "amount": "40.00"})
	tgt.add(ordersTable(), rows...)

	opts := baseOptions(src, tgt)
	opts.Level = LevelData
	rep, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK {
		t.Fatal("data check stayed green although count and one value differ")
	}
	if !hasCode(rep, "row_count_mismatch") || !hasCode(rep, "row_mismatch") {
		t.Fatalf("missing expected findings: %+v", rep.Levels[0].Findings)
	}
}

func TestDataRedOnMissingRow(t *testing.T) {
	src, tgt := newFake("postgres"), newFake("postgres")
	src.add(ordersTable(), ordersRows()...)
	rows := cloneRows(ordersRows())
	tgt.add(ordersTable(), rows[:2]...) // 少一行

	opts := baseOptions(src, tgt)
	opts.Level = LevelData
	rep, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if !hasCode(rep, "missing_row") {
		t.Fatalf("expected missing_row, got %+v", rep.Levels[0].Findings)
	}
}

func TestConsistencyRedOnTargetAheadAndGap(t *testing.T) {
	src, tgt := newFake("postgres"), newFake("postgres")
	src.add(ordersTable(), ordersRows()...)
	tgt.add(ordersTable(), ordersRows()...)

	opts := baseOptions(src, tgt)
	opts.Level = LevelConsistency
	opts.SourcePosition = func(context.Context) (string, error) { return "0/1000", nil }
	opts.TargetPosition = func(context.Context) (string, error) { return "0/2000", nil }
	rep, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK || !hasCode(rep, "target_ahead") {
		t.Fatalf("expected target_ahead, got %+v", rep.Levels[0])
	}

	opts.MaxGapBytes = 10
	opts.SourcePosition = func(context.Context) (string, error) { return "0/1000", nil }
	opts.TargetPosition = func(context.Context) (string, error) { return "0/0", nil }
	rep, err = Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK || !hasCode(rep, "position_gap") {
		t.Fatalf("expected position_gap, got %+v", rep.Levels[0])
	}
}

func TestConsistencyRefusesToPassWithoutPositions(t *testing.T) {
	src, tgt := newFake("postgres"), newFake("postgres")
	src.add(ordersTable(), ordersRows()...)
	tgt.add(ordersTable(), ordersRows()...)
	opts := baseOptions(src, tgt)
	opts.Level = LevelConsistency
	opts.SourcePosition = nil
	rep, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK {
		t.Fatal("consistency must not pass when positions are unavailable")
	}
	if !hasCode(rep, "consistency_unavailable") {
		t.Fatalf("got %+v", rep.Levels[0].Findings)
	}
}

func TestConsistencyUnparseableIsRed(t *testing.T) {
	src, tgt := newFake("postgres"), newFake("postgres")
	src.add(ordersTable(), ordersRows()...)
	tgt.add(ordersTable(), ordersRows()...)
	opts := baseOptions(src, tgt)
	opts.Level = LevelConsistency
	opts.SourcePosition = func(context.Context) (string, error) { return "position-A", nil }
	opts.TargetPosition = func(context.Context) (string, error) { return "position-B", nil }
	rep, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK || !hasCode(rep, "position_unparseable") {
		t.Fatalf("expected position_unparseable, got %+v", rep.Levels[0])
	}
}

func hasCode(rep Report, code string) bool {
	for _, lr := range rep.Levels {
		for _, f := range lr.Findings {
			if f.Code == code {
				return true
			}
		}
	}
	return false
}

// 引擎把版本化位点写进 _jl_meta（postgres:v1:<lsn>）；一致性级必须认得，
// 否则 #13 一合，PG 任务每条都红。
func TestConsistencyAcceptsVersionedPositions(t *testing.T) {
	src, tgt := newFake("postgres"), newFake("postgres")
	src.add(ordersTable(), ordersRows()...)
	tgt.add(ordersTable(), ordersRows()...)
	opts := baseOptions(src, tgt)
	opts.Level = LevelConsistency
	opts.SourcePosition = func(context.Context) (string, error) { return "0/1000", nil }
	opts.TargetPosition = func(context.Context) (string, error) { return "postgres:v1:0/1000", nil }
	rep, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK {
		t.Fatalf("versioned position must compare cleanly: %+v", rep.Levels[0])
	}
}

// 别的连接器的位点是 E1010（换错了源），不能报成 position_unparseable。
func TestConsistencyForeignKindIsE1010(t *testing.T) {
	src, tgt := newFake("postgres"), newFake("postgres")
	src.add(ordersTable(), ordersRows()...)
	tgt.add(ordersTable(), ordersRows()...)
	opts := baseOptions(src, tgt)
	opts.Level = LevelConsistency
	opts.SourcePosition = func(context.Context) (string, error) { return "0/1000", nil }
	opts.TargetPosition = func(context.Context) (string, error) {
		return "mysql:v1;file=mysql-bin.000003;pos=1234", nil
	}
	rep, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if !hasCode(rep, "position_kind_mismatch") {
		t.Fatalf("want position_kind_mismatch, got %+v", rep.Levels[0].Findings)
	}
	if hasCode(rep, "position_unparseable") {
		t.Fatal("E1010 must not be reported as position_unparseable")
	}
}

func TestCrossEngineTypeMapping(t *testing.T) {
	src, tgt := newFake("mysql"), newFake("postgres")
	src.add(source.Table{
		TableRef:   source.TableRef{Schema: "shop", Table: "orders"},
		Columns:    []source.Column{{Name: "id", Type: "int", Nullable: false}, {Name: "name", Type: "text", Nullable: true}},
		PrimaryKey: []string{"id"}, HasStablePK: true,
	}, map[string]any{"id": int64(1), "name": "x"})
	tgt.add(source.Table{
		TableRef:   source.TableRef{Schema: "public", Table: "orders"},
		Columns:    []source.Column{{Name: "id", Type: "integer", Nullable: false}, {Name: "name", Type: "text", Nullable: true}},
		PrimaryKey: []string{"id"}, HasStablePK: true,
	}, map[string]any{"id": int64(1), "name": "x"})
	opts := baseOptions(src, tgt)
	opts.SourceSchema = "shop"
	opts.Level = LevelStructure
	rep, err := Run(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK {
		t.Fatalf("bigint -> bigint, varchar(64) -> varchar(64) should map cleanly: %+v", rep.Levels[0].Findings)
	}
	if rep.Levels[0].Status != StatusOK {
		t.Fatal("unexpected status")
	}
}
