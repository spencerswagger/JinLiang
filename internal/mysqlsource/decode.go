package mysqlsource

import (
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/spencerswagger/jinliang/internal/source"
)

// decodeCell reads one column value out of a row image.
//
// 返回的 source.Value 是共享类型里的那个（internal/source/contract.go），
// 不在这里另造一套 —— 类型字典由 Ark 统一维护。
func decodeCell(tm *TableMap, col int, data []byte) (source.Value, int, error) {
	if col >= len(tm.Types) {
		return source.Value{}, 0, fmt.Errorf("列号 %d 超出表映射的 %d 列", col, len(tm.Types))
	}
	typ, packedLen := realType(tm.Types[col], tm.Meta[col])
	unsigned := col < len(tm.Unsigned) && tm.Unsigned[col]
	meta := tm.Meta[col]

	switch typ {
	case typeNull:
		return nullValue(), 0, nil
	case typeTiny:
		if len(data) < 1 {
			return source.Value{}, 0, errShort(col, 1, len(data))
		}
		if unsigned {
			return source.Value{Kind: source.KindUint, U: uint64(data[0])}, 1, nil
		}
		return source.Value{Kind: source.KindInt, I: int64(int8(data[0]))}, 1, nil
	case typeShort:
		if len(data) < 2 {
			return source.Value{}, 0, errShort(col, 2, len(data))
		}
		raw := binary.LittleEndian.Uint16(data)
		if unsigned {
			return source.Value{Kind: source.KindUint, U: uint64(raw)}, 2, nil
		}
		return source.Value{Kind: source.KindInt, I: int64(int16(raw))}, 2, nil
	case typeInt24:
		if len(data) < 3 {
			return source.Value{}, 0, errShort(col, 3, len(data))
		}
		raw := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16
		if unsigned {
			return source.Value{Kind: source.KindUint, U: uint64(raw)}, 3, nil
		}
		// 24 位有符号：把第 24 位当符号位扩到 int32。
		if raw&0x800000 != 0 {
			return source.Value{Kind: source.KindInt, I: int64(int32(raw | 0xff000000))}, 3, nil
		}
		return source.Value{Kind: source.KindInt, I: int64(raw)}, 3, nil
	case typeLong:
		if len(data) < 4 {
			return source.Value{}, 0, errShort(col, 4, len(data))
		}
		raw := binary.LittleEndian.Uint32(data)
		if unsigned {
			return source.Value{Kind: source.KindUint, U: uint64(raw)}, 4, nil
		}
		return source.Value{Kind: source.KindInt, I: int64(int32(raw))}, 4, nil
	case typeLongLong:
		if len(data) < 8 {
			return source.Value{}, 0, errShort(col, 8, len(data))
		}
		raw := binary.LittleEndian.Uint64(data)
		if unsigned {
			return source.Value{Kind: source.KindUint, U: raw}, 8, nil
		}
		return source.Value{Kind: source.KindInt, I: int64(raw)}, 8, nil
	case typeFloat:
		if len(data) < 4 {
			return source.Value{}, 0, errShort(col, 4, len(data))
		}
		return source.Value{Kind: source.KindFloat, F: float64(math.Float32frombits(binary.LittleEndian.Uint32(data)))}, 4, nil
	case typeDouble:
		if len(data) < 8 {
			return source.Value{}, 0, errShort(col, 8, len(data))
		}
		return source.Value{Kind: source.KindFloat, F: math.Float64frombits(binary.LittleEndian.Uint64(data))}, 8, nil
	case typeNewDecimal:
		precision := int(meta >> 8)
		scale := int(meta & 0xff)
		text, n, err := decodeDecimal(data, precision, scale)
		if err != nil {
			return source.Value{}, 0, fmt.Errorf("第 %d 列的 DECIMAL(%d,%d) 解不出来: %w", col, precision, scale, err)
		}
		return source.Value{Kind: source.KindString, S: text}, n, nil
	case typeDecimal:
		// 5.0 之前的旧格式，MySQL 5.7+ 不会再写到 binlog 里；明确报错好过瞎猜。
		return source.Value{}, 0, fmt.Errorf("第 %d 列是旧式 DECIMAL 编码，本连接器不支持（源库版本过旧）", col)
	case typeBit:
		nbits := int(meta>>8)*8 + int(meta&0xff)
		n := (nbits + 7) / 8
		if len(data) < n {
			return source.Value{}, 0, errShort(col, n, len(data))
		}
		return source.Value{Kind: source.KindUint, U: bigEndianUint(data[:n])}, n, nil
	case typeYear:
		if len(data) < 1 {
			return source.Value{}, 0, errShort(col, 1, len(data))
		}
		if data[0] == 0 {
			return source.Value{Kind: source.KindInt, I: 0}, 1, nil
		}
		return source.Value{Kind: source.KindInt, I: int64(data[0]) + 1900}, 1, nil
	case typeDate, typeNewDate:
		if len(data) < 3 {
			return source.Value{}, 0, errShort(col, 3, len(data))
		}
		raw := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16
		day := int(raw & 31)
		month := int((raw >> 5) & 15)
		year := int(raw >> 9)
		return source.Value{Kind: source.KindString, S: fmt.Sprintf("%04d-%02d-%02d", year, month, day)}, 3, nil
	case typeTime:
		if len(data) < 3 {
			return source.Value{}, 0, errShort(col, 3, len(data))
		}
		raw := int64(data[0]) | int64(data[1])<<8 | int64(data[2])<<16
		return source.Value{Kind: source.KindString, S: fmt.Sprintf("%02d:%02d:%02d", raw/10000, (raw%10000)/100, raw%100)}, 3, nil
	case typeTime2:
		text, n, err := decodeTime2(data, meta)
		if err != nil {
			return source.Value{}, 0, err
		}
		return source.Value{Kind: source.KindString, S: text}, n, nil
	case typeDateTime:
		if len(data) < 8 {
			return source.Value{}, 0, errShort(col, 8, len(data))
		}
		raw := binary.LittleEndian.Uint64(data)
		date, tm := raw/1000000, raw%1000000
		return source.Value{Kind: source.KindString, S: fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d",
			date/10000, (date%10000)/100, date%100, tm/10000, (tm%10000)/100, tm%100)}, 8, nil
	case typeDateTime2:
		at, n, err := decodeDatetime2(data, meta)
		if err != nil {
			return source.Value{}, 0, err
		}
		return source.Value{Kind: source.KindTime, T: at}, n, nil
	case typeTimestamp:
		if len(data) < 4 {
			return source.Value{}, 0, errShort(col, 4, len(data))
		}
		return source.Value{Kind: source.KindTime, T: time.Unix(int64(binary.LittleEndian.Uint32(data)), 0).UTC()}, 4, nil
	case typeTimestamp2:
		at, n, err := decodeTimestamp2(data, meta)
		if err != nil {
			return source.Value{}, 0, err
		}
		return source.Value{Kind: source.KindTime, T: at}, n, nil
	case typeEnum:
		l := int(meta & 0xff)
		if l < 1 || l > 2 {
			return source.Value{}, 0, fmt.Errorf("第 %d 列的 ENUM 打包长度 %d 不认识", col, l)
		}
		if len(data) < l {
			return source.Value{}, 0, errShort(col, l, len(data))
		}
		var v uint64
		if l == 1 {
			v = uint64(data[0])
		} else {
			v = uint64(binary.LittleEndian.Uint16(data))
		}
		return source.Value{Kind: source.KindInt, I: int64(v)}, l, nil
	case typeSet:
		l := int(meta & 0xff)
		if l < 1 || l > 8 {
			return source.Value{}, 0, fmt.Errorf("第 %d 列的 SET 打包长度 %d 不认识", col, l)
		}
		if len(data) < l {
			return source.Value{}, 0, errShort(col, l, len(data))
		}
		return source.Value{Kind: source.KindUint, U: fixedInt(data[:l])}, l, nil
	case typeBlob, typeTinyBlob, typeMediumBlob, typeLongBlob, typeGeometry, typeVector:
		return decodeBlob(col, data, int(meta))
	case typeJSON:
		prefix := int(meta)
		if prefix <= 0 || prefix > 4 {
			return source.Value{}, 0, fmt.Errorf("第 %d 列的 JSON 长度前缀 %d 不认识", col, prefix)
		}
		if len(data) < prefix {
			return source.Value{}, 0, errShort(col, prefix, len(data))
		}
		length := int(fixedInt(data[:prefix]))
		if len(data) < prefix+length {
			return source.Value{}, 0, errShort(col, prefix+length, len(data))
		}
		// JSON 的二进制形态要按 json_binary 解成文本，这一步归类型字典（Ark）。
		// 这里原样交出去，别假装解好了：tag=KindBytes，下游能识别。
		return source.Value{Kind: source.KindBytes, B: append([]byte(nil), data[prefix:prefix+length]...)}, prefix + length, nil
	case typeVarchar, typeVarString:
		return decodeStringCell(col, data, int(meta))
	case typeString:
		return decodeStringCell(col, data, packedLen)
	case typeInvalid:
		return source.Value{}, 0, fmt.Errorf("第 %d 列的列类型是 INVALID，表结构可能在拉流期间被改了", col)
	default:
		return source.Value{}, 0, fmt.Errorf("第 %d 列的列类型 %d 还没支持（先说清楚，不静默兜底）", col, typ)
	}
}

func nullValue() source.Value { return source.Value{Kind: source.KindNull} }

func errShort(col, want, got int) error {
	return fmt.Errorf("第 %d 列需要 %d 字节，行镜像只剩 %d 字节", col, want, got)
}

func decodeStringCell(col int, data []byte, maxLen int) (source.Value, int, error) {
	var length, prefix int
	switch {
	case maxLen < 256:
		if len(data) < 1 {
			return source.Value{}, 0, errShort(col, 1, len(data))
		}
		length, prefix = int(data[0]), 1
	default:
		if len(data) < 2 {
			return source.Value{}, 0, errShort(col, 2, len(data))
		}
		length, prefix = int(binary.LittleEndian.Uint16(data)), 2
	}
	if len(data) < prefix+length {
		return source.Value{}, 0, errShort(col, prefix+length, len(data))
	}
	raw := data[prefix : prefix+length]
	// 二进制排序规则（binary/varbinary）的列原样交字节；其余按字符串。
	return stringOrBytes(raw), prefix + length, nil
}

// stringOrBytes keeps invalid UTF-8 as bytes instead of mangling it: a BLOB
// that is not text must not silently become a string with replacement chars.
func stringOrBytes(raw []byte) source.Value {
	if isPrintableUTF8(raw) {
		return source.Value{Kind: source.KindString, S: string(raw)}
	}
	return source.Value{Kind: source.KindBytes, B: append([]byte(nil), raw...)}
}

func isPrintableUTF8(b []byte) bool {
	if len(b) == 0 {
		return true
	}
	s := string(b)
	for _, r := range s {
		if r == 0xfffd {
			return false
		}
	}
	return true
}

func decodeBlob(col int, data []byte, prefix int) (source.Value, int, error) {
	if prefix < 1 || prefix > 4 {
		return source.Value{}, 0, fmt.Errorf("第 %d 列的大字段长度前缀 %d 不认识", col, prefix)
	}
	if len(data) < prefix {
		return source.Value{}, 0, errShort(col, prefix, len(data))
	}
	length := int(fixedInt(data[:prefix]))
	if len(data) < prefix+length {
		return source.Value{}, 0, errShort(col, prefix+length, len(data))
	}
	raw := data[prefix : prefix+length]
	if v := stringOrBytes(raw); v.Kind == source.KindString {
		return v, prefix + length, nil
	}
	return source.Value{Kind: source.KindBytes, B: append([]byte(nil), raw...)}, prefix + length, nil
}

// decodeDecimal implements MySQL's packed DECIMAL (strings/decimal.c).
var decimalCompressedBytes = [10]int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}

// digitsPerGroup is 9 decimal digits per 4-byte group.
const digitsPerGroup = 9

func decodeDecimal(data []byte, precision, scale int) (string, int, error) {
	if precision <= 0 || scale < 0 || scale > precision {
		return "", 0, fmt.Errorf("DECIMAL(%d,%d) 参数不合法", precision, scale)
	}
	integral := precision - scale
	intGroups := integral / digitsPerGroup
	fracGroups := scale / digitsPerGroup
	intRest := integral - intGroups*digitsPerGroup
	fracRest := scale - fracGroups*digitsPerGroup

	size := intGroups*4 + decimalCompressedBytes[intRest] + fracGroups*4 + decimalCompressedBytes[fracRest]
	if len(data) < size {
		return "", 0, errShort(0, size, len(data))
	}
	buf := append([]byte(nil), data[:size]...)

	mask := uint32(0)
	var b strings.Builder
	if buf[0]&0x80 == 0 {
		mask = 0xffffffff
		b.WriteByte('-')
	}
	buf[0] ^= 0x80

	pos := 0
	var (
		group uint32
		zero  = true
	)

	group, n := decimalGroup(buf[pos:], decimalCompressedBytes[intRest], byte(mask))
	pos += n
	if group != 0 {
		zero = false
		b.WriteString(strconv.FormatUint(uint64(group), 10))
	}
	for i := 0; i < intGroups; i++ {
		group = binary.BigEndian.Uint32(buf[pos:]) ^ mask
		pos += 4
		if zero {
			if group != 0 {
				zero = false
				b.WriteString(strconv.FormatUint(uint64(group), 10))
			}
			continue
		}
		b.WriteString(pad9(group))
	}
	if zero {
		b.WriteByte('0')
	}

	if fracGroups > 0 || fracRest > 0 {
		b.WriteByte('.')
	}
	for i := 0; i < fracGroups; i++ {
		group = binary.BigEndian.Uint32(buf[pos:]) ^ mask
		pos += 4
		b.WriteString(pad9(group))
	}
	if fracRest > 0 {
		group, n = decimalGroup(buf[pos:], decimalCompressedBytes[fracRest], byte(mask))
		pos += n
		text := strconv.FormatUint(uint64(group), 10)
		if pad := fracRest - len(text); pad > 0 {
			b.WriteString(strings.Repeat("0", pad))
		}
		b.WriteString(text)
	}
	return b.String(), size, nil
}

func decimalGroup(data []byte, size int, mask byte) (uint32, int) {
	switch size {
	case 0:
		return 0, 0
	case 1:
		return uint32(data[0] ^ mask), 1
	case 2:
		return uint32(data[1]^mask) | uint32(data[0]^mask)<<8, 2
	case 3:
		return uint32(data[2]^mask) | uint32(data[1]^mask)<<8 | uint32(data[0]^mask)<<16, 3
	case 4:
		return uint32(data[3]^mask) | uint32(data[2]^mask)<<8 | uint32(data[1]^mask)<<16 | uint32(data[0]^mask)<<24, 4
	}
	return 0, 0
}

func pad9(v uint32) string {
	s := strconv.FormatUint(uint64(v), 10)
	if len(s) >= digitsPerGroup {
		return s
	}
	return strings.Repeat("0", digitsPerGroup-len(s)) + s
}

// datetimeIntOffset shifts DATETIME2's packed value so it sorts as signed.
const datetimeIntOffset int64 = 0x8000000000

func decodeDatetime2(data []byte, dec uint16) (time.Time, int, error) {
	n := int(5 + (dec+1)/2)
	if len(data) < n {
		return time.Time{}, 0, errShort(0, n, len(data))
	}
	intPart := int64(bigEndianUint(data[:5])) - datetimeIntOffset
	var frac int64
	switch dec {
	case 1, 2:
		frac = int64(data[5]) * 10000
	case 3, 4:
		frac = int64(binary.BigEndian.Uint16(data[5:7])) * 100
	case 5, 6:
		frac = int64(bigEndianUint(data[5:8]))
	}
	if intPart == 0 {
		return time.Time{}, n, nil
	}
	tmp := intPart<<24 + frac
	ymdhms := tmp >> 24
	ymd := ymdhms >> 17
	ym := ymd >> 5
	hms := ymdhms % (1 << 17)
	day := int(ymd % (1 << 5))
	month := int(ym % 13)
	year := int(ym / 13)
	second := int(hms % (1 << 6))
	minute := int((hms >> 6) % (1 << 6))
	hour := int(hms >> 12)
	return time.Date(year, time.Month(month), day, hour, minute, second, int(frac)*1000, time.UTC), n, nil
}

const (
	timeIntOffset int64 = 0x800000
	timeOffset    int64 = 0x800000000000
)

func decodeTime2(data []byte, dec uint16) (string, int, error) {
	n := int(3 + (dec+1)/2)
	if len(data) < n {
		return "", 0, errShort(0, n, len(data))
	}
	var (
		intPart, frac, tmp int64
	)
	switch dec {
	case 1, 2:
		intPart = int64(bigEndianUint(data[:3])) - timeIntOffset
		frac = int64(data[3])
		if intPart < 0 && frac != 0 {
			intPart++
			frac -= 0x100
		}
		tmp = intPart<<24 + frac*10000
	case 3, 4:
		intPart = int64(bigEndianUint(data[:3])) - timeIntOffset
		frac = int64(binary.BigEndian.Uint16(data[3:5]))
		if intPart < 0 && frac != 0 {
			intPart++
			frac -= 0x10000
		}
		tmp = intPart<<24 + frac*100
	case 5, 6:
		tmp = int64(bigEndianUint(data[:6])) - timeOffset
	default:
		intPart = int64(bigEndianUint(data[:3])) - timeIntOffset
		tmp = intPart << 24
	}
	return formatTime2(tmp, dec), n, nil
}

func formatTime2(tmp int64, dec uint16) string {
	sign := ""
	if tmp < 0 {
		tmp = -tmp
		sign = "-"
	}
	hms := tmp >> 24
	hour := (hms >> 12) % (1 << 10)
	minute := (hms >> 6) % (1 << 6)
	second := hms % (1 << 6)
	if dec == 0 {
		return fmt.Sprintf("%s%02d:%02d:%02d", sign, hour, minute, second)
	}
	micro := tmp % (1 << 24)
	s := fmt.Sprintf("%s%02d:%02d:%02d.%06d", sign, hour, minute, second, micro)
	return s[:len(s)-(6-int(dec))]
}

func decodeTimestamp2(data []byte, dec uint16) (time.Time, int, error) {
	n := int(4 + (dec+1)/2)
	if len(data) < n {
		return time.Time{}, 0, errShort(0, n, len(data))
	}
	sec := int64(binary.BigEndian.Uint32(data[:4]))
	var usec int64
	switch dec {
	case 1, 2:
		usec = int64(data[4]) * 10000
	case 3, 4:
		usec = int64(binary.BigEndian.Uint16(data[4:6])) * 100
	case 5, 6:
		usec = int64(bigEndianUint(data[4:7]))
	}
	if sec == 0 && usec == 0 {
		return time.Time{}, n, nil
	}
	return time.Unix(sec, usec*1000).UTC(), n, nil
}

func bigEndianUint(b []byte) uint64 {
	var v uint64
	for _, c := range b {
		v = v<<8 | uint64(c)
	}
	return v
}

func microsToTime(us uint64) time.Time {
	if us == 0 {
		return time.Time{}
	}
	return time.UnixMicro(int64(us)).UTC()
}
