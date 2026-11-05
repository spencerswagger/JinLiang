package mysqlsource

import (
	"encoding/binary"
	"fmt"
)

// Column types as they appear in the table map event (MySQL sql/field.h).
const (
	typeDecimal    = 0
	typeTiny       = 1
	typeShort      = 2
	typeLong       = 3
	typeFloat      = 4
	typeDouble     = 5
	typeNull       = 6
	typeTimestamp  = 7
	typeLongLong   = 8
	typeInt24      = 9
	typeDate       = 10
	typeTime       = 11
	typeDateTime   = 12
	typeYear       = 13
	typeNewDate    = 14
	typeVarchar    = 15
	typeBit        = 16
	typeTimestamp2 = 17
	typeDateTime2  = 18
	typeTime2      = 19
	typeTypedArray = 20
	typeVector     = 242
	typeInvalid    = 243
	typeBool       = 244
	typeJSON       = 245
	typeNewDecimal = 246
	typeEnum       = 247
	typeSet        = 248
	typeTinyBlob   = 249
	typeMediumBlob = 250
	typeLongBlob   = 251
	typeBlob       = 252
	typeVarString  = 253
	typeString     = 254
	typeGeometry   = 255
)

// TableMap is one TABLE_MAP event: which table the following rows events
// describe, and how its columns are laid out on the wire.
type TableMap struct {
	TableID    uint64
	Schema     string
	Table      string
	Columns    int
	Types      []byte
	Meta       []uint16
	Nullable   []bool // nil when the server sent no null bitmap (pre-5.1.4)
	Unsigned   []bool // from optional metadata (binlog_row_metadata=FULL)
	Names      []string
	PrimaryKey []int

	// 字符集元数据（binlog_row_metadata=FULL 才有）：默认排序规则、
	// 按字符列出现顺序排的排序规则、以及对个别列的覆盖。
	DefaultCollation   uint64
	ColumnCollations   []uint64
	CollationOverrides map[int]uint64
}

// TableIDSize is fixed at 6 for every server we support (5.1+); the FDE
// declares the post-header length, which we cross-check.
const tableIDSize = 6

func parseTableMap(body []byte) (*TableMap, error) {
	if len(body) < tableIDSize+2 {
		return nil, fmt.Errorf("表映射事件只有 %d 字节", len(body))
	}
	tm := &TableMap{TableID: fixedInt(body[0:tableIDSize])}
	pos := tableIDSize + 2 // 跳过 flags

	if pos >= len(body) {
		return nil, fmt.Errorf("表映射事件缺少库名")
	}
	schemaLen := int(body[pos])
	pos++
	if pos+schemaLen+1 > len(body) {
		return nil, fmt.Errorf("表映射事件的库名越界")
	}
	tm.Schema = string(body[pos : pos+schemaLen])
	pos += schemaLen + 1 // 0x00

	if pos >= len(body) {
		return nil, fmt.Errorf("表映射事件缺少表名")
	}
	tableLen := int(body[pos])
	pos++
	if pos+tableLen+1 > len(body) {
		return nil, fmt.Errorf("表映射事件的表名越界")
	}
	tm.Table = string(body[pos : pos+tableLen])
	pos += tableLen + 1 // 0x00

	count, n, err := lenEnc(body[pos:])
	if err != nil {
		return nil, fmt.Errorf("表映射事件的列数读不出来: %w", err)
	}
	pos += n
	tm.Columns = int(count)
	if tm.Columns < 0 || tm.Columns > 4096 {
		return nil, fmt.Errorf("表映射事件说这张表有 %d 列，超出合理范围", tm.Columns)
	}
	if pos+tm.Columns > len(body) {
		return nil, fmt.Errorf("表映射事件的列类型数组越界（声称 %d 列）", tm.Columns)
	}
	tm.Types = append([]byte(nil), body[pos:pos+tm.Columns]...)
	tm.Unsigned = make([]bool, tm.Columns)
	pos += tm.Columns

	metaLen, n, err := lenEnc(body[pos:])
	if err != nil {
		return nil, fmt.Errorf("表映射事件的列元数据长度读不出来: %w", err)
	}
	pos += n
	if pos+int(metaLen) > len(body) {
		return nil, fmt.Errorf("表映射事件的列元数据越界（声称 %d 字节）", metaLen)
	}
	meta := body[pos : pos+int(metaLen)]
	pos += int(metaLen)
	if err := tm.decodeMeta(meta); err != nil {
		return nil, err
	}

	nullBytes := bitmapByteSize(tm.Columns)
	if pos+nullBytes > len(body) {
		return nil, fmt.Errorf("表映射事件的空值位图越界")
	}
	nullBitmap := body[pos : pos+nullBytes]
	pos += nullBytes
	tm.Nullable = make([]bool, tm.Columns)
	for i := range tm.Nullable {
		tm.Nullable[i] = bitSet(nullBitmap, i)
	}

	// 剩下的全是可选元数据；5.7 默认只发一部分，8.0 的 FULL 才带列名/主键。
	if pos < len(body) {
		if err := tm.decodeOptionalMeta(body[pos:]); err != nil {
			return nil, err
		}
	}
	return tm, nil
}

// decodeMeta reads the per-type fixed metadata block.
func (tm *TableMap) decodeMeta(data []byte) error {
	tm.Meta = make([]uint16, tm.Columns)
	pos := 0
	need := func(n int) error {
		if pos+n > len(data) {
			return fmt.Errorf("列元数据不够：第 %d 列需要 %d 字节，只剩 %d", pos, n, len(data)-pos)
		}
		return nil
	}
	for i, t := range tm.Types {
		switch t {
		case typeString:
			if err := need(2); err != nil {
				return err
			}
			tm.Meta[i] = uint16(data[pos])<<8 | uint16(data[pos+1])
			pos += 2
		case typeNewDecimal:
			if err := need(2); err != nil {
				return err
			}
			tm.Meta[i] = uint16(data[pos])<<8 | uint16(data[pos+1])
			pos += 2
		case typeVarString, typeVarchar, typeBit:
			if err := need(2); err != nil {
				return err
			}
			tm.Meta[i] = binary.LittleEndian.Uint16(data[pos:])
			pos += 2
		case typeBlob, typeDouble, typeFloat, typeGeometry, typeJSON, typeVector,
			typeTime2, typeDateTime2, typeTimestamp2:
			if err := need(1); err != nil {
				return err
			}
			tm.Meta[i] = uint16(data[pos])
			pos++
		case typeEnum, typeSet, typeTinyBlob, typeMediumBlob, typeLongBlob:
			if err := need(1); err != nil {
				return err
			}
			tm.Meta[i] = uint16(data[pos])
			pos++
		default:
			tm.Meta[i] = 0
		}
	}
	return nil
}

// Optional metadata TLV type codes (MySQL table_map_event.h).
const (
	optMetaSignedness            = 1
	optMetaDefaultCharset        = 2
	optMetaColumnCharset         = 3
	optMetaColumnName            = 4
	optMetaSetStrValue           = 5
	optMetaEnumStrValue          = 6
	optMetaGeometryType          = 7
	optMetaSimplePrimaryKey      = 8
	optMetaPrimaryKeyWithPrefix  = 9
	optMetaEnumSetDefaultCharset = 10
	optMetaEnumSetColumnCharset  = 11
	optMetaColumnVisibility      = 12
)

func (tm *TableMap) decodeOptionalMeta(data []byte) error {
	pos := 0
	numeric := 0
	for pos < len(data) {
		typ := data[pos]
		pos++
		length, n, err := lenEnc(data[pos:])
		if err != nil {
			return fmt.Errorf("可选元数据字段 0x%02x 的长度读不出来: %w", typ, err)
		}
		pos += n
		if pos+int(length) > len(data) {
			return fmt.Errorf("可选元数据字段 0x%02x 越界", typ)
		}
		value := data[pos : pos+int(length)]
		pos += int(length)

		switch typ {
		case optMetaSignedness:
			// 位图只覆盖数值列，每字节从高位往低位填。
			idx := 0
			for _, b := range value {
				for bit := 7; bit >= 0; bit-- {
					for idx < tm.Columns && !isNumeric(tm.Types[idx]) {
						idx++
					}
					if idx >= tm.Columns {
						break
					}
					tm.Unsigned[idx] = b&(1<<uint(bit)) != 0
					idx++
				}
			}
			numeric = idx
		case optMetaColumnName:
			p := 0
			names := make([]string, 0, tm.Columns)
			for p < len(value) {
				n := int(value[p])
				p++
				if p+n > len(value) {
					return fmt.Errorf("列名元数据越界")
				}
				names = append(names, string(value[p:p+n]))
				p += n
			}
			tm.Names = names
		case optMetaSimplePrimaryKey:
			p := 0
			for p < len(value) {
				v, n, err := lenEnc(value[p:])
				if err != nil {
					return fmt.Errorf("主键列号读不出来: %w", err)
				}
				p += n
				tm.PrimaryKey = append(tm.PrimaryKey, int(v))
			}
		case optMetaPrimaryKeyWithPrefix:
			p := 0
			for p < len(value) {
				v, n, err := lenEnc(value[p:])
				if err != nil {
					return fmt.Errorf("主键列号读不出来: %w", err)
				}
				p += n
				_, n2, err := lenEnc(value[p:])
				if err != nil {
					return fmt.Errorf("主键前缀长度读不出来: %w", err)
				}
				p += n2
				tm.PrimaryKey = append(tm.PrimaryKey, int(v))
			}
		case optMetaDefaultCharset:
			// [默认排序规则, (列序号, 排序规则) ...]
			if len(value) == 0 {
				return fmt.Errorf("字符集元数据是空的")
			}
			p := 0
			v, n, err := lenEnc(value[p:])
			if err != nil {
				return fmt.Errorf("默认排序规则读不出来: %w", err)
			}
			p += n
			tm.DefaultCollation = v
			tm.CollationOverrides = map[int]uint64{}
			for p < len(value) {
				idx, n, err := lenEnc(value[p:])
				if err != nil {
					return fmt.Errorf("覆盖项的列序号读不出来: %w", err)
				}
				p += n
				if p >= len(value) {
					return fmt.Errorf("列 %d 的排序规则缺失", idx)
				}
				col, n, err := lenEnc(value[p:])
				if err != nil {
					return fmt.Errorf("列 %d 的排序规则读不出来: %w", idx, err)
				}
				p += n
				tm.CollationOverrides[int(idx)] = col
			}
		case optMetaColumnCharset:
			p := 0
			for p < len(value) {
				v, n, err := lenEnc(value[p:])
				if err != nil {
					return fmt.Errorf("列排序规则读不出来: %w", err)
				}
				p += n
				tm.ColumnCollations = append(tm.ColumnCollations, v)
			}
		case optMetaEnumSetColumnCharset, optMetaGeometryType:
			// enum/set 与几何列的元数据：暂不使用，跳过即可（不影响列对齐）。
		default:
			// 未来版本加了新字段：跳过，不猜。
		}
	}
	_ = numeric
	return nil
}

func isNumeric(t byte) bool {
	switch t {
	case typeTiny, typeShort, typeInt24, typeLong, typeLongLong,
		typeDecimal, typeNewDecimal, typeFloat, typeDouble, typeYear, typeBool:
		return true
	}
	return false
}

// realType resolves the "packed" STRING type into what the value really is
// (CHAR / ENUM / SET) plus its length, per Table_map_log_event::print_columns.
func realType(t byte, meta uint16) (typ byte, length int) {
	if t != typeString {
		return t, 0
	}
	b0 := byte(meta >> 8)
	b1 := byte(meta & 0xff)
	if b0&0x30 != 0x30 {
		return b0 | 0x30, int(uint16(b1) | uint16((b0&0x30)^0x30)<<4)
	}
	return b0, int(meta & 0xff)
}
