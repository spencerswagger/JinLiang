package mysqlsource

import (
	"encoding/binary"
	"math"
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/source"
)

// —— json_binary 构造器（只给测试用，按线上格式逐字节拼）——

type jsonbEntry struct {
	key  string // 对象才有
	typ  byte
	body []byte
}

func (e jsonbEntry) encoded() []byte {
	return append([]byte{e.typ}, e.body...)
}

func jsonUint16(v uint16) jsonbEntry {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, v)
	return jsonbEntry{typ: jsonbUint16, body: b}
}

func jsonInt16(v int16) jsonbEntry {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, uint16(v))
	return jsonbEntry{typ: jsonbInt16, body: b}
}

func jsonInt64(v int64) jsonbEntry {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))
	return jsonbEntry{typ: jsonbInt64, body: b}
}

func jsonDouble(v float64) jsonbEntry {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, mathFloat64bits(v))
	return jsonbEntry{typ: jsonbDouble, body: b}
}

func mathFloat64bits(f float64) uint64 { return math.Float64bits(f) }

func jsonLiteral(v byte) jsonbEntry { return jsonbEntry{typ: jsonbLiteral, body: []byte{v}} }

func jsonString(s string) jsonbEntry {
	return jsonbEntry{typ: jsonbString, body: append(appendVarint(len(s)), s...)}
}

func jsonOpaque(fieldType byte, payload []byte) jsonbEntry {
	return jsonbEntry{typ: jsonbOpaque, body: append([]byte{fieldType}, append(appendVarint(len(payload)), payload...)...)}
}

func appendVarint(n int) []byte {
	var out []byte
	for {
		b := byte(n & 0x7f)
		n >>= 7
		if n == 0 {
			return append(out, b)
		}
		out = append(out, b|0x80)
	}
}

// buildJSONB 按线上格式拼一个容器（对象或数组），支持内联小值与 large 偏移。
func buildJSONB(containerType byte, entries []jsonbEntry) []byte {
	large := containerType == jsonbLargeObject || containerType == jsonbLargeArray
	isObject := containerType == jsonbSmallObject || containerType == jsonbLargeObject

	headerLen := 4
	keyEntryLen, valEntryLen := 0, 3
	if large {
		headerLen, valEntryLen = 8, 5
	}
	if isObject {
		keyEntryLen = 4
		if large {
			keyEntryLen = 6
		}
	}

	// 数据区：先键，再值。
	keyArea := make([]byte, 0, 64)
	valueArea := make([]byte, 0, 128)
	type placed struct {
		keyOff, keyLen int
		valOff         int
		inline         bool
		inlineBytes    []byte
	}
	layout := make([]placed, len(entries))
	dataStart := headerLen + len(entries)*keyEntryLen + len(entries)*valEntryLen
	for i, e := range entries {
		var p placed
		if isObject {
			p.keyOff = dataStart + len(keyArea)
			p.keyLen = len(e.key)
			keyArea = append(keyArea, e.key...)
		}
		if n := inlineJSONLen(e.typ, large); n > 0 && n <= len(e.body) {
			p.inline = true
			p.inlineBytes = e.body[:n]
		} else {
			p.valOff = dataStart + len(keyArea) + len(valueArea)
			valueArea = append(valueArea, e.encoded()...)
		}
		layout[i] = p
	}
	size := dataStart + len(keyArea) + len(valueArea)
	out := make([]byte, 0, size)
	if large {
		out = binary.LittleEndian.AppendUint32(out, uint32(len(entries)))
		out = binary.LittleEndian.AppendUint32(out, uint32(size))
	} else {
		out = binary.LittleEndian.AppendUint16(out, uint16(len(entries)))
		out = binary.LittleEndian.AppendUint16(out, uint16(size))
	}
	if isObject {
		for _, p := range layout {
			if large {
				out = binary.LittleEndian.AppendUint32(out, uint32(p.keyOff))
				out = binary.LittleEndian.AppendUint16(out, uint16(p.keyLen))
			} else {
				out = binary.LittleEndian.AppendUint16(out, uint16(p.keyOff))
				out = binary.LittleEndian.AppendUint16(out, uint16(p.keyLen))
			}
		}
	}
	for i, p := range layout {
		out = append(out, entries[i].typ)
		width := 2
		if large {
			width = 4
		}
		if p.inline {
			buf := make([]byte, width)
			copy(buf, p.inlineBytes)
			out = append(out, buf...)
			continue
		}
		if large {
			out = binary.LittleEndian.AppendUint32(out, uint32(p.valOff))
		} else {
			out = binary.LittleEndian.AppendUint16(out, uint16(p.valOff))
		}
	}
	out = append(out, keyArea...)
	out = append(out, valueArea...)
	return out
}

// —— 测试 ——

func TestJSONDecodeObject(t *testing.T) {
	obj := buildJSONB(jsonbSmallObject, []jsonbEntry{
		{key: "a", typ: jsonbInt16, body: []byte{1, 0}},
		{key: "b", typ: jsonbLiteral, body: []byte{jsonbLiteralTrue}},
	})
	got, err := decodeJSONText(append([]byte{jsonbSmallObject}, obj...))
	if err != nil {
		t.Fatalf("解对象失败: %v", err)
	}
	if got != `{"a":1,"b":true}` {
		t.Fatalf("对象解出来是 %q", got)
	}
}

func TestJSONDecodeArray(t *testing.T) {
	arr := buildJSONB(jsonbSmallArray, []jsonbEntry{
		jsonUint16(7),
		jsonString("x"),
		jsonLiteral(jsonbLiteralNull),
	})
	got, err := decodeJSONText(append([]byte{jsonbSmallArray}, arr...))
	if err != nil {
		t.Fatalf("解数组失败: %v", err)
	}
	if got != `[7,"x",null]` {
		t.Fatalf("数组解出来是 %q", got)
	}
}

func TestJSONDecodeScalars(t *testing.T) {
	cases := []struct {
		entry jsonbEntry
		want  string
	}{
		{jsonInt16(-3), "-3"},
		{jsonInt64(1 << 40), "1099511627776"},
		{jsonDouble(1.5), "1.5"},
		{jsonString("a\"b\nc"), `"a\"b\nc"`},
		{jsonString("中文"), `"中文"`},
		{jsonLiteral(jsonbLiteralFalse), "false"},
		{jsonLiteral(jsonbLiteralNull), "null"},
	}
	for _, c := range cases {
		got, err := decodeJSONText(c.entry.encoded())
		if err != nil {
			t.Fatalf("%s 解码失败: %v", c.want, err)
		}
		if got != c.want {
			t.Fatalf("得到 %q，期望 %q", got, c.want)
		}
	}
}

func TestJSONDecodeEscapesControlChars(t *testing.T) {
	// \u0000 必须写成转义：直接写裸 NUL 会让目标端的文本列拒绝或截断。
	got, err := decodeJSONText(jsonString("a\x00b").encoded())
	if err != nil {
		t.Fatalf("解码失败: %v", err)
	}
	if got != `"a\u0000b"` {
		t.Fatalf("控制字符没转义: %q", got)
	}
}

func TestJSONDecodeRejectsTruncatedAndUnknown(t *testing.T) {
	cases := map[string][]byte{
		"类型码不认识":  {0x7f, 1, 2, 3},
		"对象头被截断":  {jsonbSmallObject, 1, 0},
		"字符串长度越界": {jsonbString, 200, 'x'},
		"空字节":     {},
	}
	for name, raw := range cases {
		if _, err := decodeJSONText(raw); err != nil && name == "空字节" {
			// 空文档按 JSON null 处理（MySQL 非严格模式允许），不算错。
			t.Fatalf("空文档应按 null 处理，拿到错误: %v", err)
		}
		if name == "空字节" {
			continue
		}
		if _, err := decodeJSONText(raw); err == nil {
			t.Fatalf("%s：必须报错", name)
		}
	}
}

func TestJSONDecodeOpaqueDateAndDecimal(t *testing.T) {
	// 日期在 JSON 里是 opaque：写成 JSON 字符串。
	packed := make([]byte, 8)
	// 2026-11-06 00:30:00.000000：年 2026 → ym = 2026*13+11 = 26349；ymd = 26349<<5 | 6
	ym := 2026*13 + 11
	ymd := (ym << 5) | 6
	ymdhms := (ymd << 17) | (0<<12 | 30<<6 | 0)
	binary.LittleEndian.PutUint64(packed, uint64(ymdhms)<<24)
	got, err := decodeJSONText(jsonOpaque(typeDateTime, packed).encoded())
	if err != nil {
		t.Fatalf("日期 opaque 解码失败: %v", err)
	}
	if got != `"2026-11-06 00:30:00.000000"` {
		t.Fatalf("日期解出来是 %q", got)
	}

	// DECIMAL 等未实现的 opaque：明确报错并给出路（不猜、不静默）。
	_, err = decodeJSONText(jsonOpaque(typeNewDecimal, []byte{1, 2, 3}).encoded())
	if err == nil {
		t.Fatal("未实现的 opaque 必须报错")
	}
	if !strings.Contains(err.Error(), "text") || !strings.Contains(err.Error(), "exclude") {
		t.Fatalf("错误里应给出「改 text / 排除该列」的出路，拿到: %v", err)
	}
}

func TestJSONColumnDeliversKindJSON(t *testing.T) {
	// 列级：JSON 列必须交付 KindJSON + UTF-8 文本，不能是 KindBytes。
	p, opt := newTestParser(t, true)
	cols := []testColumn{{typ: typeLong, name: "id"}, {typ: typeJSON, meta: 4, name: "doc"}}
	if _, err := p.Parse(frame(evTableMap, tableMapBody(21, "s", "t", cols, []bool{false, true}, nil, nil), opt)); err != nil {
		t.Fatalf("表映射: %v", err)
	}
	doc := append([]byte{jsonbSmallObject}, buildJSONB(jsonbSmallObject, []jsonbEntry{
		{key: "k", typ: jsonbLiteral, body: []byte{jsonbLiteralTrue}},
	})...)
	img := rowImage([]bool{false, false}, le32(7), append(le32(uint32(len(doc))), doc...))
	events, err := p.Parse(frame(evWriteRowsV2, rowsBodyV2(21, 2, []bool{true, true}, [][]byte{img}), opt))
	if err != nil {
		t.Fatalf("JSON 列不该让解析失败: %v", err)
	}
	v := events[0].Rows.Changes[0].New.Row.Values[1]
	if v.Kind != source.KindJSON {
		t.Fatalf("JSON 列必须交付 KindJSON，当前 %v", v.Kind)
	}
	if v.S != `{"k":true}` {
		t.Fatalf("JSON 文本不对: %q", v.S)
	}
	if len(v.B) != 0 {
		t.Fatal("KindJSON 不该用 Value.B 承载")
	}
}

func TestJSONColumnBadValueFailsBatch(t *testing.T) {
	// 解不出来就终止本批：不许降级成文本、不许用 KindBytes 透传。
	p, opt := newTestParser(t, true)
	cols := []testColumn{{typ: typeJSON, meta: 4, name: "doc"}}
	if _, err := p.Parse(frame(evTableMap, tableMapBody(22, "s", "t", cols, []bool{true}, nil, nil), opt)); err != nil {
		t.Fatalf("表映射: %v", err)
	}
	broken := []byte{0x7f, 0xde, 0xad} // 类型码不认识
	img := rowImage([]bool{false}, append(le32(uint32(len(broken))), broken...))
	if _, err := p.Parse(frame(evWriteRowsV2, rowsBodyV2(22, 1, []bool{true}, [][]byte{img}), opt)); err == nil {
		t.Fatal("坏 JSON 必须让解析失败（终止本批）")
	}
}
