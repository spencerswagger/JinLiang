package mysqlsource

import (
	"testing"

	"github.com/spencerswagger/jinliang/internal/source"
)

// 5.7 的 TABLE_MAP 没有排序规则：`BINARY(4)='abcd'` 只能靠 catalog 的字符集判。
//
// 这一格是 P2 在 5.7 上的判据（CI 的 mysql 5.7 腿没有 FULL 元数据，
// 真库用例只能验「有排序规则」那条路）。
func TestFillColumnKindsCovers57(t *testing.T) {
	// 5.7 形态：没有任何排序规则元数据。
	tm := &TableMap{
		Schema: "app", Table: "t", Columns: 3,
		Types: []byte{typeString, typeVarchar, typeVarchar},
		Meta:  []uint16{0xFE<<8 | 4, 8, 64}, // BINARY(4) / VARBINARY(8) / VARCHAR(64 bytes)
	}
	// 没有 catalog 时：说不上来 —— 只能退回内容启发式（已知边界，不是常态）。
	if got := tm.columnBinaryKind(0); got != binUnknown {
		t.Fatalf("5.7 且没有 catalog 时应是 binUnknown，实际 %v", got)
	}
	if v, _, err := decodeCell(tm, 0, append([]byte{4}, []byte("abcd")...)); err != nil {
		t.Fatalf("解码失败: %v", err)
	} else if v.Kind != source.KindString {
		t.Fatalf("没有 catalog 时退回内容启发式（已知边界）：ctx=%v", v.Kind)
	}

	// 有 catalog（流开始时 information_schema 读到的列定义）：判据变精确。
	// 注意 BINARY/VARBINARY/BLOB 的 CHARACTER_SET_NAME 在 MySQL 里就是 **NULL**
	// （真库实测），所以判据不能只看字符集——没有字符集时按类型名认。
	FillColumnKinds(tm, &source.Table{
		TableRef: source.TableRef{Schema: "app", Table: "t"},
		Columns: []source.Column{
			{Name: "b4", Type: "binary(4)", Charset: ""},
			{Name: "vb", Type: "varbinary(8)", Charset: ""},
			{Name: "vc", Type: "varchar(64)", Charset: "utf8mb4"},
		},
	})
	for i, want := range []binaryKind{binBinary, binBinary, binText} {
		if got := tm.columnBinaryKind(i); got != want {
			t.Fatalf("第 %d 列 kind = %v，应为 %v（catalog 的 CHARACTER_SET_NAME 是权威）", i+1, got, want)
		}
	}
	// 补全之后 `BINARY(4)='abcd'` 必须交字节（内容恰好合法也不影响）。
	v, _, err := decodeCell(tm, 0, append([]byte{4}, []byte("abcd")...))
	if err != nil {
		t.Fatalf("解码失败: %v", err)
	}
	if v.Kind != source.KindBytes || string(v.B) != "abcd" {
		t.Fatalf("BINARY(4)='abcd' 应按字节交，实际 Kind=%v 值=%q", v.Kind, v.B)
	}
	// 类型串的基名同样要按 kind 纠正（5.7 的 binlog 只说 char/varchar）。
	rel := tableFromMap(tm)
	want := []string{"binary(4)", "varbinary(8)", "varchar(64)"}
	for i, w := range want {
		if rel.Columns[i].Type != w {
			t.Errorf("第 %d 列类型应是 %q，实际 %q（catalog 的 binary 家族是权威基名）", i+1, w, rel.Columns[i].Type)
		}
	}

	// 字符列仍然是字符串。
	v2, _, err := decodeCell(tm, 2, append([]byte{3}, []byte("abc")...))
	if err != nil {
		t.Fatalf("解码失败: %v", err)
	}
	if v2.Kind != source.KindString || v2.S != "abc" {
		t.Fatalf("VARCHAR 应交字符串，实际 Kind=%v 值=%q", v2.Kind, v2.S)
	}
}

// 列数对不上时不猜：宁可退回启发式，也不要把「第 3 列是 binary」套到别的列上。
func TestFillColumnKindsRefusesWhenColumnCountDiffers(t *testing.T) {
	tm := &TableMap{Schema: "app", Table: "t", Columns: 2,
		Types: []byte{typeVarchar, typeVarchar}, Meta: []uint16{8, 8}}
	FillColumnKinds(tm, &source.Table{
		TableRef: source.TableRef{Schema: "app", Table: "t"},
		Columns: []source.Column{
			{Name: "a", Type: "binary(4)", Charset: "binary"},
			{Name: "b", Type: "varchar(8)", Charset: "utf8mb4"},
			{Name: "c", Type: "int"}, // 列数变了（源端刚 DDL 过）
		},
	})
	if tm.ColumnKinds != nil {
		t.Fatalf("列数对不上时不许补 kind（会按序号错位），实际 %v", tm.ColumnKinds)
	}
}

// catalogBinaryKind：MySQL 对 BINARY/VARBINARY/BLOB 不给字符集（NULL），
// 只靠「字符集空 = 不知道」会让 5.7 上这几列继续走内容启发式（CI 的 5.7 腿就是这么红的）。
func TestCatalogBinaryKind(t *testing.T) {
	cases := []struct {
		col  source.Column
		want binaryKind
		why  string
	}{
		{source.Column{Type: "binary(4)"}, binBinary, "字符集 NULL + binary 类型名"},
		{source.Column{Type: "varbinary(8)"}, binBinary, "同上"},
		{source.Column{Type: "blob"}, binBinary, "BLOB 字符集也是 NULL"},
		{source.Column{Type: "mediumblob"}, binBinary, "尺寸档不影响"},
		{source.Column{Type: "geometry"}, binBinary, "几何类型按字节"},
		{source.Column{Type: "char(10)", Charset: "utf8mb4"}, binText, "有字符集就是按字符"},
		{source.Column{Type: "text", Charset: "utf8mb4"}, binText, "同上"},
		{source.Column{Type: "varchar(16)"}, binText, "类型名兜底"},
		{source.Column{Type: "int"}, binUnknown, "数值列与行列镜像无关"},
		{source.Column{Type: "datetime(6)"}, binUnknown, "时间列同上"},
	}
	for _, c := range cases {
		if got := catalogBinaryKind(c.col); got != c.want {
			t.Errorf("catalogBinaryKind(%+v) = %v，应为 %v —— %s", c.col, got, c.want, c.why)
		}
	}
}

// 优先级回归（两方向）：**这一版 binlog 的排序规则**比「流开始时的 catalog」权威。
//
// 场景：流中 `ALTER … MODIFY c VARBINARY(64)` / 反向改成 utf8mb4，列数不变，
// catalog（FillColumnKinds 的来源）还是旧的。若让 catalog 压过 binlog，
// 类型串会被纠回旧基名 → 与旧 catalog 一比「没变」→ **该暂停不暂停**，
// 值还按错的族解码（可打印的当文本写、不可打印的走 `\x..` 字面量）。
func TestColumnBinaryKindPrefersWireCollation(t *testing.T) {
	// 方向一：catalog 说字符（旧），binlog 说 binary（新）→ 必须按字节。
	tm := &TableMap{Schema: "app", Table: "t", Columns: 1,
		Types: []byte{typeVarchar}, Meta: []uint16{64},
		ColumnCollations: []uint64{binaryCollationID}}
	FillColumnKinds(tm, &source.Table{
		TableRef: source.TableRef{Schema: "app", Table: "t"},
		Columns:  []source.Column{{Name: "c", Type: "varchar(64)", Charset: "utf8mb4"}},
	})
	if got := tm.columnBinaryKind(0); got != binBinary {
		t.Fatalf("binlog 说 binary、catalog 说字符时应取 binlog（按字节），实际 %v", got)
	}
	if got := tableFromMap(tm).Columns[0].Type; got != "varbinary(64)" {
		t.Fatalf("类型串应是 varbinary(64)（否则改类型判定看不见），实际 %q", got)
	}

	// 方向二：catalog 说 binary（旧），binlog 说 utf8mb4（新）→ 必须按字符。
	tm2 := &TableMap{Schema: "app", Table: "t", Columns: 1,
		Types: []byte{typeVarchar}, Meta: []uint16{64},
		ColumnCollations: []uint64{255}} // utf8mb4_0900_ai_ci
	FillColumnKinds(tm2, &source.Table{
		TableRef: source.TableRef{Schema: "app", Table: "t"},
		Columns:  []source.Column{{Name: "c", Type: "varbinary(64)"}},
	})
	if got := tm2.columnBinaryKind(0); got != binText {
		t.Fatalf("binlog 说字符、catalog 说二进制时应取 binlog（按字符），实际 %v", got)
	}
	// 64 字节在 utf8mb4 里是 16 个字符：字符族要按字符集换算（P3 那条）。
	if got := tableFromMap(tm2).Columns[0].Type; got != "varchar(16)" {
		t.Fatalf("类型串应是 varchar(16)（64 字节 ÷ 4），实际 %q", got)
	}
}
