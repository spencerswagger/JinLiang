package mysqlsource

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

// MySQL 的 json_binary 解码器（sql-common/json_binary.cc 的线上格式）。
//
// 为什么连接器要自己解：binlog 行镜像里的 json 列是二进制，不是文本。契约要求
// 以 KindJSON + Value.S 交付 UTF-8 JSON 文本（见 docs/TYPE-MAPPING.md §5.1），
// sink 不做第二遍解码。用 KindBytes 透传会让目标上出现一列「像 JSON 的乱码」，
// 而全链路一声不响——那是最坏的一种"成功"。

// json_binary 的类型码。
const (
	jsonbSmallObject = 0x00
	jsonbLargeObject = 0x01
	jsonbSmallArray  = 0x02
	jsonbLargeArray  = 0x03
	jsonbLiteral     = 0x04
	jsonbInt16       = 0x05
	jsonbUint16      = 0x06
	jsonbInt32       = 0x07
	jsonbUint32      = 0x08
	jsonbInt64       = 0x09
	jsonbUint64      = 0x0a
	jsonbDouble      = 0x0b
	jsonbString      = 0x0c
	jsonbOpaque      = 0x0f
)

// JSON 字面量。
const (
	jsonbLiteralNull  = 0x00
	jsonbLiteralTrue  = 0x01
	jsonbLiteralFalse = 0x02
)

// ErrJSONDecode 是「这段二进制不是合法的 json_binary」。
//
// 处理口径（docs/TYPE-MAPPING.md §5.1 ①）：报错终止本批，**不降级为文本、
// 不写半截**。源库的 json 列本身保证合法，解不出只可能是数据损坏或格式理解错。
var ErrJSONDecode = fmt.Errorf("JSON 值解码失败")

// decodeJSONText 把 binlog 里的 json_binary 值解成 UTF-8 JSON 文本。
func decodeJSONText(raw []byte) (string, error) {
	if len(raw) == 0 {
		// 空文档：MySQL 在非严格模式下可以把空串塞进 json 列，语义上是 JSON null。
		return "null", nil
	}
	out, err := formatJSONValue(raw[0], raw[1:])
	if err != nil {
		return "", err
	}
	return out, nil
}

func formatJSONValue(typ byte, body []byte) (string, error) {
	switch typ {
	case jsonbSmallObject:
		return formatJSONObject(body, false)
	case jsonbLargeObject:
		return formatJSONObject(body, true)
	case jsonbSmallArray:
		return formatJSONArray(body, false)
	case jsonbLargeArray:
		return formatJSONArray(body, true)
	case jsonbLiteral:
		if len(body) < 1 {
			return "", fmt.Errorf("%w：字面量缺少取值", ErrJSONDecode)
		}
		switch body[0] {
		case jsonbLiteralNull:
			return "null", nil
		case jsonbLiteralTrue:
			return "true", nil
		case jsonbLiteralFalse:
			return "false", nil
		}
		return "", fmt.Errorf("%w：字面量取值 0x%02x 不认识", ErrJSONDecode, body[0])
	case jsonbInt16:
		if len(body) < 2 {
			return "", errJSONShort("int16", 2, len(body))
		}
		return strconv.FormatInt(int64(int16(binary.LittleEndian.Uint16(body))), 10), nil
	case jsonbUint16:
		if len(body) < 2 {
			return "", errJSONShort("uint16", 2, len(body))
		}
		return strconv.FormatUint(uint64(binary.LittleEndian.Uint16(body)), 10), nil
	case jsonbInt32:
		if len(body) < 4 {
			return "", errJSONShort("int32", 4, len(body))
		}
		return strconv.FormatInt(int64(int32(binary.LittleEndian.Uint32(body))), 10), nil
	case jsonbUint32:
		if len(body) < 4 {
			return "", errJSONShort("uint32", 4, len(body))
		}
		return strconv.FormatUint(uint64(binary.LittleEndian.Uint32(body)), 10), nil
	case jsonbInt64:
		if len(body) < 8 {
			return "", errJSONShort("int64", 8, len(body))
		}
		return strconv.FormatInt(int64(binary.LittleEndian.Uint64(body)), 10), nil
	case jsonbUint64:
		if len(body) < 8 {
			return "", errJSONShort("uint64", 8, len(body))
		}
		return strconv.FormatUint(binary.LittleEndian.Uint64(body), 10), nil
	case jsonbDouble:
		if len(body) < 8 {
			return "", errJSONShort("double", 8, len(body))
		}
		f := math.Float64frombits(binary.LittleEndian.Uint64(body))
		if math.IsNaN(f) || math.IsInf(f, 0) {
			// JSON 表达不了 NaN/Inf；MySQL 也不允许写入，出现就是数据坏了。
			return "", fmt.Errorf("%w：双精度值 %v 无法用 JSON 表示", ErrJSONDecode, f)
		}
		return formatJSONNumber(f), nil
	case jsonbString:
		s, rest, err := readJSONString(body)
		if err != nil {
			return "", err
		}
		if len(rest) != 0 {
			return "", fmt.Errorf("%w：字符串后面还有 %d 字节多余数据", ErrJSONDecode, len(rest))
		}
		return quoteJSONString(s)
	case jsonbOpaque:
		return formatJSONOpaque(body)
	default:
		return "", fmt.Errorf("%w：类型码 0x%02x 不认识（可能是更新版本的 MySQL 新增的）", ErrJSONDecode, typ)
	}
}

// formatJSONObject 解 small/large object：键值各一张偏移表，数据紧随其后。
func formatJSONObject(body []byte, large bool) (string, error) {
	count, size, rest, err := readJSONHeader(body, large)
	if err != nil {
		return "", err
	}
	keyEntry, valEntry := 4, 3
	if large {
		keyEntry, valEntry = 6, 5
	}
	need := count*keyEntry + count*valEntry
	if len(rest) < need {
		return "", fmt.Errorf("%w：对象说有 %d 个键，但只剩 %d 字节放不下键值表", ErrJSONDecode, count, len(rest))
	}
	if size < need+headerLen(large) || size > len(body) {
		return "", fmt.Errorf("%w：对象头声明的长度 %d 不合法（实际 %d 字节）", ErrJSONDecode, size, len(body))
	}
	payload := body[:size]
	keys := payload[headerLen(large) : headerLen(large)+count*keyEntry]
	vals := payload[headerLen(large)+count*keyEntry : headerLen(large)+count*keyEntry+count*valEntry]
	dataStart := headerLen(large) + count*keyEntry + count*valEntry

	var b strings.Builder
	b.WriteByte('{')
	for i := 0; i < count; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		var keyOff, keyLen int
		if large {
			keyOff = int(binary.LittleEndian.Uint32(keys[i*6:]))
			keyLen = int(binary.LittleEndian.Uint16(keys[i*6+4:]))
		} else {
			keyOff = int(binary.LittleEndian.Uint16(keys[i*4:]))
			keyLen = int(binary.LittleEndian.Uint16(keys[i*4+2:]))
		}
		if keyOff+keyLen > len(payload) || keyOff < dataStart {
			return "", fmt.Errorf("%w：第 %d 个键的位置越界", ErrJSONDecode, i+1)
		}
		key := string(payload[keyOff : keyOff+keyLen])
		quoted, err := quoteJSONString(key)
		if err != nil {
			return "", err
		}
		b.WriteString(quoted)
		b.WriteByte(':')
		val, err := formatJSONEntry(vals[i*valEntry:(i+1)*valEntry], payload, large)
		if err != nil {
			return "", err
		}
		b.WriteString(val)
	}
	b.WriteByte('}')
	return b.String(), nil
}

// formatJSONArray 解 small/large array：只有一张值偏移表。
func formatJSONArray(body []byte, large bool) (string, error) {
	count, size, rest, err := readJSONHeader(body, large)
	if err != nil {
		return "", err
	}
	entry := 3
	if large {
		entry = 5
	}
	if len(rest) < count*entry {
		return "", fmt.Errorf("%w：数组说有 %d 个元素，但只剩 %d 字节放不下偏移表", ErrJSONDecode, count, len(rest))
	}
	if size < headerLen(large)+count*entry || size > len(body) {
		return "", fmt.Errorf("%w：数组头声明的长度 %d 不合法（实际 %d 字节）", ErrJSONDecode, size, len(body))
	}
	payload := body[:size]
	vals := payload[headerLen(large) : headerLen(large)+count*entry]

	var b strings.Builder
	b.WriteByte('[')
	for i := 0; i < count; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		v, err := formatJSONEntry(vals[i*entry:(i+1)*entry], payload, large)
		if err != nil {
			return "", err
		}
		b.WriteString(v)
	}
	b.WriteByte(']')
	return b.String(), nil
}

func headerLen(large bool) int {
	if large {
		return 8
	}
	return 4
}

func readJSONHeader(body []byte, large bool) (count, size int, rest []byte, err error) {
	if large {
		if len(body) < 8 {
			return 0, 0, nil, errJSONShort("大容器头", 8, len(body))
		}
		count = int(binary.LittleEndian.Uint32(body[0:4]))
		size = int(binary.LittleEndian.Uint32(body[4:8]))
		return count, size, body[8:], nil
	}
	if len(body) < 4 {
		return 0, 0, nil, errJSONShort("小容器头", 4, len(body))
	}
	count = int(binary.LittleEndian.Uint16(body[0:2]))
	size = int(binary.LittleEndian.Uint16(body[2:4]))
	return count, size, body[4:], nil
}

// formatJSONEntry 解一个「类型 + 偏移或内联值」条目。
func formatJSONEntry(entry, container []byte, large bool) (string, error) {
	typ := entry[0]
	if n := inlineJSONLen(typ, large); n > 0 {
		// 小值直接内联在偏移字段里。
		return formatJSONValue(typ, entry[1:1+n])
	}
	var off int
	if large {
		if len(entry) < 5 {
			return "", errJSONShort("大容器条目", 5, len(entry))
		}
		off = int(binary.LittleEndian.Uint32(entry[1:5]))
	} else {
		if len(entry) < 3 {
			return "", errJSONShort("小容器条目", 3, len(entry))
		}
		off = int(binary.LittleEndian.Uint16(entry[1:3]))
	}
	if off <= 0 || off >= len(container) {
		return "", fmt.Errorf("%w：值偏移 %d 越界（容器 %d 字节）", ErrJSONDecode, off, len(container))
	}
	return formatJSONValue(container[off], container[off+1:])
}

// inlineJSONLen 返回可以内联在偏移字段里的固定长度；0 表示走偏移。
func inlineJSONLen(typ byte, large bool) int {
	max := 2
	if large {
		max = 4
	}
	var n int
	switch typ {
	case jsonbLiteral:
		n = 1
	case jsonbInt16, jsonbUint16:
		n = 2
	case jsonbInt32, jsonbUint32:
		n = 4
	case jsonbInt64, jsonbUint64, jsonbDouble:
		n = 8
	default:
		return 0
	}
	if n <= max {
		return n
	}
	return 0
}

// readJSONString 读变长字符串（varint 长度 + UTF-8 字节）。
func readJSONString(data []byte) (string, []byte, error) {
	length, consumed, ok := readJSONVarint(data)
	if !ok {
		return "", nil, errJSONShort("字符串长度", 1, len(data))
	}
	body := data[consumed:]
	if length < 0 || length > len(body) {
		return "", nil, fmt.Errorf("%w：字符串声称 %d 字节，实际只剩 %d", ErrJSONDecode, length, len(body))
	}
	raw := body[:length]
	if !utf8.Valid(raw) {
		return "", nil, fmt.Errorf("%w：字符串不是合法 UTF-8（源列字符集可能不是 utf8mb4）", ErrJSONDecode)
	}
	return string(raw), body[length:], nil
}

// readJSONBytes 读变长字节串（opaque 载荷是二进制，不能按 UTF-8 校验）。
func readJSONBytes(data []byte) ([]byte, []byte, error) {
	length, consumed, ok := readJSONVarint(data)
	if !ok {
		return nil, nil, errJSONShort("长度前缀", 1, len(data))
	}
	body := data[consumed:]
	if length < 0 || length > len(body) {
		return nil, nil, fmt.Errorf("%w：声称 %d 字节，实际只剩 %d", ErrJSONDecode, length, len(body))
	}
	return body[:length], body[length:], nil
}

// readJSONVarint 读 7 位一组的变长整数（最多 5 字节）。
func readJSONVarint(data []byte) (value int, consumed int, ok bool) {
	var v uint64
	for i := 0; i < 5; i++ {
		if i >= len(data) {
			return 0, 0, false
		}
		c := data[i]
		v |= uint64(c&0x7f) << (7 * uint(i))
		if c&0x80 == 0 {
			if v > math.MaxInt32 {
				return 0, 0, false
			}
			return int(v), i + 1, true
		}
	}
	return 0, 0, false
}

// formatJSONOpaque 解 MySQL 塞进 JSON 的非 JSON 原生类型（日期/时间等）。
//
// 口径：能解的按 MySQL 自己 SELECT 出来的样子写成 JSON 值；解不了的（DECIMAL/BIT
// 等）明确报错，不猜——猜错就是静默写错数字，比失败难查得多。
func formatJSONOpaque(body []byte) (string, error) {
	if len(body) < 1 {
		return "", errJSONShort("opaque 字段类型", 1, len(body))
	}
	fieldType := body[0]
	payload, rest, err := readJSONBytes(body[1:])
	if err != nil {
		return "", err
	}
	if len(rest) != 0 {
		return "", fmt.Errorf("%w：opaque 值后面还有 %d 字节", ErrJSONDecode, len(rest))
	}
	switch fieldType {
	case typeDate, typeNewDate, typeDateTime, typeTimestamp, typeTime:
		// MySQL 把日期时间在 JSON 里存成 8 字节打包值（MYSQL_TIME_to_binary）。
		if len(payload) < 8 {
			return "", fmt.Errorf("%w：日期时间值只有 %d 字节", ErrJSONDecode, len(payload))
		}
		s, err := decodePackedDateTime(payload, fieldType)
		if err != nil {
			return "", err
		}
		return quoteJSONString(s)
	default:
		return "", fmt.Errorf("%w：JSON 里嵌了 MySQL 字段类型 %d（例如 DECIMAL/BIT），本连接器还没实现；"+
			"把该列在目标端落成 text，或在 columns.exclude 里排除它", ErrJSONDecode, fieldType)
	}
}

// decodePackedDateTime 解 MYSQL_TIME 的打包形式（binlog 里的日期时间同一套位域）。
func decodePackedDateTime(raw []byte, fieldType byte) (string, error) {
	packed := binary.LittleEndian.Uint64(raw[:8])
	// 打包格式：1 bit 负标志 + 17 bit 年*13+月 + 5 bit 日 + 5 bit 时 + 6 bit 分 + 6 bit 秒 + 24 bit 微秒
	neg := packed&(1<<63) != 0
	ymdhms := int64(packed>>24) & ((1 << 39) - 1)
	micro := int(packed & 0xffffff)
	ymd := ymdhms >> 17
	ym := ymd >> 5
	hms := ymdhms % (1 << 17)
	day := int(ymd % (1 << 5))
	month := int(ym % 13)
	year := int(ym / 13)
	second := int(hms % (1 << 6))
	minute := int((hms >> 6) % (1 << 6))
	hour := int(hms >> 12)

	sign := ""
	if neg {
		sign = "-"
	}
	switch fieldType {
	case typeDate, typeNewDate:
		return fmt.Sprintf("%04d-%02d-%02d", year, month, day), nil
	case typeTime:
		return fmt.Sprintf("%s%02d:%02d:%02d.%06d", sign, hour, minute, second, micro), nil
	default:
		return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hour, minute, second, micro), nil
	}
}

// formatJSONNumber 用最短可回读的形式写 double（MySQL 自己也是这么打印的）。
func formatJSONNumber(f float64) string {
	s := strconv.FormatFloat(f, 'g', -1, 64)
	// JSON 不接受 .5 / 1. 这种写法；Go 的 'g' 不会产生，但保住这条断言。
	if strings.HasPrefix(s, ".") {
		s = "0" + s
	}
	if strings.HasSuffix(s, ".") {
		s += "0"
	}
	return s
}

// quoteJSONString 按 JSON 规则转义，且**不做 HTML 转义**（encoding/json 会把
// < > & 变成 \u003c，对数据同步来说是没必要的改写）。
func quoteJSONString(s string) (string, error) {
	if !utf8.ValidString(s) {
		return "", fmt.Errorf("%w：字符串不是合法 UTF-8", ErrJSONDecode)
	}
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		default:
			if r < 0x20 {
				fmt.Fprintf(&b, `\u%04x`, r)
				continue
			}
			b.WriteRune(r)
		}
	}
	b.WriteByte('"')
	return b.String(), nil
}

func errJSONShort(what string, want, got int) error {
	return fmt.Errorf("%w：%s需要 %d 字节，只剩 %d", ErrJSONDecode, what, want, got)
}
