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

	// 有 catalog（流开始时 information_schema 读到的字符集）：判据变精确。
	FillColumnKinds(tm, &source.Table{
		TableRef: source.TableRef{Schema: "app", Table: "t"},
		Columns: []source.Column{
			{Name: "b4", Type: "binary(4)", Charset: "binary"},
			{Name: "vb", Type: "varbinary(8)", Charset: "binary"},
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
