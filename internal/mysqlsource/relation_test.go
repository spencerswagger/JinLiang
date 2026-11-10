package mysqlsource

import (
	"context"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/source"
)

func feedEvent(t *testing.T, a *Assembler, ev *Event) {
	t.Helper()
	if err := a.Feed(context.Background(), ev); err != nil {
		t.Fatalf("喂事件失败: %v", err)
	}
}

func openTx(t *testing.T, a *Assembler, gno int64, logPos uint32) {
	t.Helper()
	feedEvent(t, a, &Event{Header: Header{Type: evGTID, LogPos: logPos}, File: "mysql-bin.000001",
		GTID: &GTIDEvent{SID: txSID, GNO: gno}})
	feedEvent(t, a, &Event{Header: Header{Type: evQuery, LogPos: logPos + 10}, File: "mysql-bin.000001",
		Query: &QueryEvent{Schema: "shop", Query: "BEGIN"}})
}

func insertOne(t *testing.T, a *Assembler, tm *TableMap, logPos uint32, values ...source.Value) {
	t.Helper()
	present := make([]bool, len(values))
	for i := range present {
		present[i] = true
	}
	feedEvent(t, a, &Event{Header: Header{Type: evWriteRowsV2, LogPos: logPos}, File: "mysql-bin.000001",
		Rows: &RowsEvent{Table: tm, Version: 2, Changes: []Change{{
			Kind: source.EvInsert,
			New: Image{
				Row:     source.Row{Table: source.TableRef{Schema: tm.Schema, Table: tm.Table}, Values: values},
				Present: present,
			},
		}}}})
}

func TestRelationCarriesColumnDefinition(t *testing.T) {
	var got []Tx
	a := NewAssembler(func(ctx context.Context, tx Tx) error { got = append(got, tx); return nil })

	tm := &TableMap{TableID: 5, Schema: "shop", Table: "orders", Columns: 3,
		Types:      []byte{typeLong, typeVarchar, typeLongLong},
		Meta:       []uint16{0, 64, 0},
		Names:      []string{"id", "note", "big"},
		Nullable:   []bool{false, true, false},
		Unsigned:   []bool{false, false, true},
		PrimaryKey: []int{0},
	}
	openTx(t, a, 1, 100)
	feedEvent(t, a, &Event{Header: Header{Type: evTableMap, LogPos: 120}, File: "mysql-bin.000001", TableMap: tm})
	insertOne(t, a, tm, 140, source.Value{Kind: source.KindInt, I: 1}, source.Value{Kind: source.KindNull}, source.Value{Kind: source.KindUint, U: 9})
	feedEvent(t, a, &Event{Header: Header{Type: evXID, LogPos: 160}, File: "mysql-bin.000001", XID: &XIDEvent{XID: 1}})

	if len(got) != 1 {
		t.Fatalf("应有 1 笔事务，拿到 %d", len(got))
	}
	data := dataEvents(got[0].Events)
	if len(data) != 1 || data[0].Relation == nil {
		t.Fatalf("数据事件必须带列定义: %#v", data)
	}
	rel := data[0].Relation
	if len(rel.Columns) != 3 {
		t.Fatalf("列数应为 3，拿到 %d", len(rel.Columns))
	}
	wantTypes := []string{"int", "varchar(64)", "bigint unsigned"}
	for i, want := range wantTypes {
		if rel.Columns[i].Type != want {
			t.Fatalf("第 %d 列类型 = %q，期望 %q", i, rel.Columns[i].Type, want)
		}
	}
	if rel.Columns[0].Name != "id" || rel.Columns[1].Name != "note" {
		t.Fatalf("列名要按表映射来: %#v", rel.Columns)
	}
	if !rel.Columns[1].Nullable || rel.Columns[0].Nullable {
		t.Fatalf("可空性没带上: %#v", rel.Columns)
	}
	if len(rel.PrimaryKey) != 1 || rel.PrimaryKey[0] != "id" {
		t.Fatalf("主键没带上: %#v", rel.PrimaryKey)
	}
	// 列顺序必须和 Values 顺序一致：这是引擎对齐的唯一依据。
	if len(rel.Columns) != len(data[0].New.Values) {
		t.Fatalf("列定义 %d 列，值 %d 个：对不齐就没法解码", len(rel.Columns), len(data[0].New.Values))
	}
}

func TestRelationPointerReusedUntilDefinitionChanges(t *testing.T) {
	var got []Tx
	a := NewAssembler(func(ctx context.Context, tx Tx) error { got = append(got, tx); return nil })

	tm := &TableMap{TableID: 7, Schema: "s", Table: "t", Columns: 1,
		Types: []byte{typeLong}, Meta: []uint16{0}, Names: []string{"id"}, Nullable: []bool{false}}

	openTx(t, a, 1, 100)
	feedEvent(t, a, &Event{Header: Header{Type: evTableMap, LogPos: 110}, File: "f", TableMap: tm})
	insertOne(t, a, tm, 120, source.Value{Kind: source.KindInt, I: 1})
	insertOne(t, a, tm, 130, source.Value{Kind: source.KindInt, I: 2})
	feedEvent(t, a, &Event{Header: Header{Type: evXID, LogPos: 140}, File: "f", XID: &XIDEvent{XID: 1}})

	// 第二笔事务：没有新的表映射，仍应复用同一个指针（契约：指针相等即同一版）。
	openTx(t, a, 2, 200)
	insertOne(t, a, tm, 210, source.Value{Kind: source.KindInt, I: 3})
	feedEvent(t, a, &Event{Header: Header{Type: evXID, LogPos: 220}, File: "f", XID: &XIDEvent{XID: 2}})

	if len(got) != 2 {
		t.Fatalf("应有 2 笔事务，拿到 %d", len(got))
	}
	first := dataEvents(got[0].Events)[0].Relation
	second := dataEvents(got[0].Events)[1].Relation
	third := dataEvents(got[1].Events)[0].Relation
	if first == nil || first != second || first != third {
		t.Fatalf("同一版定义必须复用同一个指针（first=%p second=%p third=%p)", first, second, third)
	}

	// 模拟源端 DDL 后的新表映射：多一列 → 新指针，列数跟着变。
	openTx(t, a, 3, 300)
	tm2 := &TableMap{TableID: 7, Schema: "s", Table: "t", Columns: 2,
		Types: []byte{typeLong, typeVarchar}, Meta: []uint16{0, 32},
		Names: []string{"id", "note"}, Nullable: []bool{false, true}}
	feedEvent(t, a, &Event{Header: Header{Type: evTableMap, LogPos: 310}, File: "f", TableMap: tm2})
	insertOne(t, a, tm2, 320, source.Value{Kind: source.KindInt, I: 4}, source.Value{Kind: source.KindNull})
	feedEvent(t, a, &Event{Header: Header{Type: evXID, LogPos: 330}, File: "f", XID: &XIDEvent{XID: 3}})

	fourth := dataEvents(got[2].Events)[0].Relation
	if fourth == first {
		t.Fatal("表定义变了必须换新指针，否则消费方会按旧列定义解读新数据")
	}
	if len(fourth.Columns) != 2 {
		t.Fatalf("新定义应有 2 列，拿到 %d", len(fourth.Columns))
	}
	if fourth.Columns[1].Name != "note" {
		t.Fatalf("新定义的列名不对: %#v", fourth.Columns)
	}
}

func TestRelationMissingNamesStillAlign(t *testing.T) {
	// binlog_row_metadata=MINIMAL：没有列名，但列数和顺序仍必须对得上。
	tm := &TableMap{TableID: 9, Schema: "s", Table: "t", Columns: 2,
		Types: []byte{typeLong, typeTimestamp2}, Meta: []uint16{0, 6}, Nullable: []bool{false, true}}
	rel := tableFromMap(tm)
	if len(rel.Columns) != 2 {
		t.Fatalf("列数应保持 2")
	}
	if rel.Columns[0].Name == "" || rel.Columns[1].Name == "" {
		t.Fatalf("没有列名时也要给占位名字，否则没法对齐: %#v", rel.Columns)
	}
	if rel.Columns[1].Type != "timestamp(6)" {
		t.Fatalf("小数位丢了: %q", rel.Columns[1].Type)
	}
	if !rel.Columns[1].Nullable {
		t.Fatal("可空性丢了")
	}
}

func TestTypeNames(t *testing.T) {
	cases := []struct {
		typ      byte
		meta     uint16
		unsigned bool
		charset  string
		want     string
	}{
		{typeTiny, 0, true, "", "tinyint unsigned"},
		{typeInt24, 0, false, "", "mediumint"},
		{typeNewDecimal, 10<<8 | 2, false, "", "decimal(10,2)"},
		{typeDateTime2, 6, false, "", "datetime(6)"},
		{typeBit, 1<<8 | 3, false, "", "bit(11)"},
		{typeJSON, 4, false, "utf8mb4", "json"},
		// VARCHAR 的 meta 是**字节**上限：latin1 的 VARCHAR(255) 是 255，
		// utf8mb4 的 VARCHAR(64) 是 256，utf8mb3 的 VARCHAR(100) 是 300。
		{typeVarchar, 255, false, "latin1", "varchar(255)"},
		{typeVarchar, 256, false, "utf8mb4", "varchar(64)"},
		{typeVarchar, 300, false, "utf8mb3", "varchar(100)"},
		// VARBINARY 没有字符集，按字节，不许除。
		{typeVarString, 64, false, "utf8mb4", "varbinary(64)"},
		{typeBlob, 3, false, "binary", "mediumblob"},
		// TEXT 与 BLOB 共用类型字节，只有字符集能分开
		{typeBlob, 2, false, "utf8mb4", "text"},
		// CHAR 的长度也是字节
		{typeString, 0xFE<<8 | 40, false, "utf8mb4", "char(10)"},
		// VARBINARY 走 VARCHAR 的类型字节 + binary 字符集
		{typeVarchar, 32, false, "binary", "varbinary(32)"},
	}
	for _, c := range cases {
		if got := mysqlTypeName(c.typ, c.meta, c.unsigned, c.charset); got != c.want {
			t.Fatalf("类型 %d/%d/charset=%q → %q，期望 %q", c.typ, c.meta, c.charset, got, c.want)
		}
	}
}

func TestEventCommitIsLastAndUnique(t *testing.T) {
	var got []Tx
	a := NewAssembler(func(ctx context.Context, tx Tx) error { got = append(got, tx); return nil })
	tm := &TableMap{TableID: 1, Schema: "s", Table: "t", Columns: 1, Types: []byte{typeLong}}
	openTx(t, a, 1, 100)
	feedEvent(t, a, &Event{Header: Header{Type: evTableMap, LogPos: 110}, File: "f", TableMap: tm})
	insertOne(t, a, tm, 120, source.Value{Kind: source.KindInt, I: 1})
	feedEvent(t, a, &Event{Header: Header{Type: evXID, LogPos: 130}, File: "f", XID: &XIDEvent{XID: 1}})

	commits := 0
	for i, ev := range got[0].Events {
		if ev.Kind == source.EvCommit {
			commits++
			if i != len(got[0].Events)-1 {
				t.Fatal("EvCommit 必须是批次的最后一个事件")
			}
			if ev.TxID == "" {
				t.Fatal("EvCommit 必须带事务号，便于审计")
			}
		}
	}
	if commits != 1 {
		t.Fatalf("一笔事务只能有一个 EvCommit，拿到 %d", commits)
	}
	if got[0].Commit.TakenAt.IsZero() {
		t.Fatal("提交位点必须带时间")
	}
	_ = time.Now
}

func TestRelationFillsCharsetAndCollation(t *testing.T) {
	// binlog_row_metadata=FULL 会带字符集：默认排序规则 + 个别列覆盖。
	// 这两项原来只能丢掉或塞进 Type，现在按契约填进 Column.Charset/Collation。
	tm := &TableMap{
		TableID: 3, Schema: "s", Table: "t", Columns: 3,
		Types: []byte{typeLong, typeVarchar, typeVarchar},
		// VARCHAR 的 meta 是**字节**上限：utf8mb4 的 64 字符 = 256 字节。
		// 之前这里写 64 是照「字符数」写造的假数据，真 binlog 从来不这么给。
		Meta:               []uint16{0, 256, 256},
		Names:              []string{"id", "name", "note"},
		Nullable:           []bool{false, true, true},
		DefaultCollation:   45,                    // utf8mb4_general_ci
		CollationOverrides: map[int]uint64{1: 63}, // 第二个字符列（ordinal 1）用 binary
	}
	rel := tableFromMap(tm)
	if rel.Columns[0].Charset != "" || rel.Columns[0].Collation != "" {
		t.Fatalf("非字符列不该有字符集: %#v", rel.Columns[0])
	}
	if rel.Columns[1].Charset != "utf8mb4" || rel.Columns[1].Collation != "utf8mb4_general_ci" {
		t.Fatalf("第 2 列（第一个字符列）字符集不对: %#v", rel.Columns[1])
	}
	if rel.Columns[2].Charset != "binary" || rel.Columns[2].Collation != "binary" {
		t.Fatalf("列级覆盖没生效（ordinal 1 → binary）: %#v", rel.Columns[2])
	}
	// Type 仍然是完整类型文本，不掺字符集。
	if rel.Columns[1].Type != "varchar(64)" {
		t.Fatalf("Type 语义被污染了: %q", rel.Columns[1].Type)
	}
}

func TestRelationColumnCollationList(t *testing.T) {
	// 另一种形态：直接给每个字符列的排序规则（type 3）。
	tm := &TableMap{
		TableID: 4, Schema: "s", Table: "t", Columns: 2,
		Types: []byte{typeVarchar, typeVarchar}, Meta: []uint16{32, 32},
		Names: []string{"a", "b"}, Nullable: []bool{true, true},
		ColumnCollations: []uint64{255, 46},
	}
	rel := tableFromMap(tm)
	if rel.Columns[0].Collation != "utf8mb4_0900_ai_ci" {
		t.Fatalf("第 1 列排序规则 = %q", rel.Columns[0].Collation)
	}
	if rel.Columns[1].Collation != "utf8mb4_bin" {
		t.Fatalf("第 2 列排序规则 = %q", rel.Columns[1].Collation)
	}
	if rel.Columns[0].Charset != "utf8mb4" || rel.Columns[1].Charset != "utf8mb4" {
		t.Fatalf("字符集没填: %#v", rel.Columns)
	}
}

func TestCharsetMetadataParsedFromWire(t *testing.T) {
	// 从字节流一路解析到 Column.Charset（不只测结构体）。
	p, opt := newTestParser(t, true)
	cols := []testColumn{
		{typ: typeLong, name: "id"},
		{typ: typeVarchar, meta: 64, name: "name"},
	}
	body := tableMapBody(31, "shop", "users", cols, []bool{false, true}, nil, nil)
	body = withCharsetMeta(body, 45, map[int]uint64{0: 255}) // 默认 utf8mb4_general_ci，第 1 个字符列用 0900_ai_ci
	if _, err := p.Parse(frame(evTableMap, body, opt)); err != nil {
		t.Fatalf("带字符集元数据的表映射解析失败: %v", err)
	}
	tm := p.TableMap(31)
	if tm == nil {
		t.Fatal("表映射没缓存下来")
	}
	if tm.DefaultCollation != 45 {
		t.Fatalf("默认排序规则 = %d，期望 45", tm.DefaultCollation)
	}
	if got := tm.CollationOverrides[0]; got != 255 {
		t.Fatalf("列级覆盖 = %d，期望 255", got)
	}
	rel := tableFromMap(tm)
	if rel.Columns[1].Charset != "utf8mb4" || rel.Columns[1].Collation != "utf8mb4_0900_ai_ci" {
		t.Fatalf("列字符集/排序规则没填对: %#v", rel.Columns[1])
	}
}
