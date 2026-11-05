package mysqlsource

import (
	"encoding/binary"
	"hash/crc32"
)

// Test helpers that build real binlog frames byte by byte, so the parser is
// tested against the wire format rather than against its own structs.

type frameOpts struct {
	checksum bool
	serverID uint32
	logPos   uint32
}

// frame wraps a body into a binlog event: 19-byte header (+ CRC32 trailer).
func frame(typ uint8, body []byte, opt frameOpts) []byte {
	size := headerSize + len(body)
	if opt.checksum {
		size += checksumLen
	}
	out := make([]byte, size)
	binary.LittleEndian.PutUint32(out[0:4], 1_700_000_000)
	out[4] = typ
	sid := opt.serverID
	if sid == 0 {
		sid = 1
	}
	binary.LittleEndian.PutUint32(out[5:9], sid)
	binary.LittleEndian.PutUint32(out[9:13], uint32(size))
	binary.LittleEndian.PutUint32(out[13:17], opt.logPos)
	copy(out[headerSize:], body)
	if opt.checksum {
		sum := crc32.ChecksumIEEE(out[:size-checksumLen])
		binary.LittleEndian.PutUint32(out[size-checksumLen:], sum)
	}
	return out
}

// fdeBody builds a format description for a modern server (CRC32 aware).
func fdeBody(serverVersion string, checksum uint8) []byte {
	body := make([]byte, 0, 120)
	v := make([]byte, 2)
	binary.LittleEndian.PutUint16(v, 4)
	body = append(body, v...)
	ver := make([]byte, 50)
	copy(ver, serverVersion)
	body = append(body, ver...)
	body = append(body, 0, 0, 0, 0)       // create timestamp
	body = append(body, byte(headerSize)) // header length
	// 事件类型 → 后置头长度，索引即事件类型编号。
	lens := make([]byte, 43)
	lens[evQuery] = 13
	lens[evTableMap] = 8
	lens[evWriteRowsV1], lens[evUpdateRowsV1], lens[evDeleteRowsV1] = 8, 8, 8
	lens[evWriteRowsV2], lens[evUpdateRowsV2], lens[evDeleteRowsV2] = 10, 10, 10
	lens[evHeartbeatV2] = 0
	body = append(body, lens...)
	body = append(body, checksum)
	body = append(body, 0, 0, 0, 0) // FDE 自己的 CRC 占位
	return body
}

// lenEncBytes encodes a length-encoded integer.
func lenEncBytes(n int) []byte {
	switch {
	case n < 251:
		return []byte{byte(n)}
	case n < 1<<16:
		return []byte{0xfc, byte(n), byte(n >> 8)}
	default:
		return []byte{0xfd, byte(n), byte(n >> 8), byte(n >> 16)}
	}
}

// tableIDBytes renders the 6-byte table id.
func tableIDBytes(id uint64) []byte {
	b := make([]byte, tableIDSize)
	for i := 0; i < tableIDSize; i++ {
		b[i] = byte(id >> (8 * i))
	}
	return b
}

type testColumn struct {
	typ  byte
	meta uint16
	name string
}

// tableMapBody builds a TABLE_MAP event body with optional metadata
// (column names + primary key + signedness), the way 8.0 does with
// binlog_row_metadata=FULL.
func tableMapBody(id uint64, schema, table string, cols []testColumn, nullable []bool, unsigned []bool, pk []int) []byte {
	body := append([]byte{}, tableIDBytes(id)...)
	body = append(body, 0, 0) // flags
	body = append(body, byte(len(schema)))
	body = append(body, []byte(schema)...)
	body = append(body, 0)
	body = append(body, byte(len(table)))
	body = append(body, []byte(table)...)
	body = append(body, 0)
	body = append(body, lenEncBytes(len(cols))...)

	types := make([]byte, 0, len(cols))
	meta := make([]byte, 0, len(cols)*2)
	for _, c := range cols {
		types = append(types, c.typ)
		switch c.typ {
		case typeVarchar, typeVarString, typeBit, typeString, typeNewDecimal:
			meta = append(meta, byte(c.meta), byte(c.meta>>8))
		case typeBlob, typeFloat, typeDouble, typeJSON, typeGeometry:
			meta = append(meta, byte(c.meta))
		}
	}
	body = append(body, types...)
	body = append(body, lenEncBytes(len(meta))...)
	body = append(body, meta...)

	nullBitmap := make([]byte, bitmapByteSize(len(cols)))
	for i := range cols {
		if i < len(nullable) && nullable[i] {
			nullBitmap[i/8] |= 1 << uint(i%8)
		}
	}
	body = append(body, nullBitmap...)

	var optional []byte
	if len(unsigned) > 0 {
		var bits []byte
		var cur byte
		n := 0
		for i, c := range cols {
			if !isNumeric(c.typ) {
				continue
			}
			if i < len(unsigned) && unsigned[i] {
				cur |= 1 << uint(7-n%8)
			}
			n++
			if n%8 == 0 {
				bits = append(bits, cur)
				cur = 0
			}
		}
		if n%8 != 0 {
			bits = append(bits, cur)
		}
		optional = append(optional, optMetaSignedness)
		optional = append(optional, lenEncBytes(len(bits))...)
		optional = append(optional, bits...)
	}
	if len(cols) > 0 && cols[0].name != "" {
		var names []byte
		for _, c := range cols {
			names = append(names, byte(len(c.name)))
			names = append(names, []byte(c.name)...)
		}
		optional = append(optional, optMetaColumnName)
		optional = append(optional, lenEncBytes(len(names))...)
		optional = append(optional, names...)
	}
	if len(pk) > 0 {
		var pkb []byte
		for _, idx := range pk {
			pkb = append(pkb, lenEncBytes(idx)...)
		}
		optional = append(optional, optMetaSimplePrimaryKey)
		optional = append(optional, lenEncBytes(len(pkb))...)
		optional = append(optional, pkb...)
	}
	body = append(body, optional...)
	return body
}

// rowsBodyV2 builds a WRITE_ROWS_EVENT v2 body from pre-encoded images.
func rowsBodyV2(id uint64, colCount int, included []bool, images [][]byte) []byte {
	body := append([]byte{}, tableIDBytes(id)...)
	body = append(body, 0, 0) // flags
	body = append(body, 2, 0) // extra data length（只有长度字段本身）
	body = append(body, lenEncBytes(colCount)...)
	bitmap := make([]byte, bitmapByteSize(colCount))
	for i, ok := range included {
		if ok {
			bitmap[i/8] |= 1 << uint(i%8)
		}
	}
	body = append(body, bitmap...)
	for _, img := range images {
		body = append(body, img...)
	}
	return body
}

// updateBodyV2 builds an UPDATE_ROWS_EVENT v2 body (before + after image).
func updateBodyV2(id uint64, colCount int, before, after []bool, images [][]byte) []byte {
	body := append([]byte{}, tableIDBytes(id)...)
	body = append(body, 0, 0)
	body = append(body, 2, 0)
	body = append(body, lenEncBytes(colCount)...)
	for _, incl := range [][]bool{before, after} {
		bitmap := make([]byte, bitmapByteSize(colCount))
		for i, ok := range incl {
			if ok {
				bitmap[i/8] |= 1 << uint(i%8)
			}
		}
		body = append(body, bitmap...)
	}
	for _, img := range images {
		body = append(body, img...)
	}
	return body
}

// rowImage encodes one row image: null bitmap (over included columns) + values.
func rowImage(nulls []bool, values ...[]byte) []byte {
	nullBitmap := make([]byte, bitmapByteSize(len(nulls)))
	for i, isNull := range nulls {
		if isNull {
			nullBitmap[i/8] |= 1 << uint(i%8)
		}
	}
	out := append([]byte{}, nullBitmap...)
	for i, v := range values {
		if i < len(nulls) && nulls[i] {
			continue
		}
		out = append(out, v...)
	}
	return out
}

func le32(v uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, v)
	return b
}

func le64(v uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	return b
}

// queryBody builds a QUERY_EVENT body.
func queryBody(schema, query string) []byte {
	body := make([]byte, 0, 13+len(schema)+len(query))
	body = append(body, 0, 0, 0, 0) // slave_proxy_id
	body = append(body, 0, 0, 0, 0) // execution time
	body = append(body, byte(len(schema)))
	body = append(body, 0, 0) // error code
	body = append(body, 0, 0) // status vars length
	body = append(body, []byte(schema)...)
	body = append(body, 0)
	body = append(body, []byte(query)...)
	return body
}

// gtidBody builds a GTID_EVENT body (8.0 layout, 42+ bytes).
func gtidBody(sid [16]byte, gno int64) []byte {
	body := make([]byte, 0, 42)
	body = append(body, 1) // commit flag: 有独立事务
	body = append(body, sid[:]...)
	body = append(body, le64(uint64(gno))...)
	body = append(body, 2)                   // logical timestamp typecode
	body = append(body, le64(1)...)          // last_committed
	body = append(body, le64(1)...)          // sequence_number
	body = append(body, 0, 0, 0, 0, 0, 0, 0) // immediate commit timestamp（0 = 未知）
	return body
}
