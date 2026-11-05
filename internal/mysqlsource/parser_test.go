package mysqlsource

import (
	"testing"

	"github.com/klauspost/compress/zstd"

	"github.com/spencerswagger/jinliang/internal/source"
)

func newTestParser(t *testing.T, checksum bool) (*Parser, frameOpts) {
	t.Helper()
	p := NewParser()
	alg := uint8(checksumOff)
	if checksum {
		alg = checksumCRC32
	}
	opt := frameOpts{checksum: checksum}
	fde := frame(evFormatDescription, fdeBody("8.0.28", alg), frameOpts{})
	if _, err := p.Parse(fde); err != nil {
		t.Fatalf("解析格式描述失败: %v", err)
	}
	return p, opt
}

func TestParseInsertWithTableMap(t *testing.T) {
	p, opt := newTestParser(t, true)
	cols := []testColumn{
		{typ: typeLong, name: "id"},
		{typ: typeVarchar, meta: 64, name: "name"},
	}
	tm := frame(evTableMap, tableMapBody(7, "shop", "orders", cols, []bool{false, true}, nil, []int{0}), opt)
	if _, err := p.Parse(tm); err != nil {
		t.Fatalf("解析表映射失败: %v", err)
	}
	img := rowImage([]bool{false, false}, le32(42), append([]byte{5}, []byte("alice")...))
	rows := frame(evWriteRowsV2, rowsBodyV2(7, 2, []bool{true, true}, [][]byte{img}), opt)
	events, err := p.Parse(rows)
	if err != nil {
		t.Fatalf("解析行事件失败: %v", err)
	}
	if len(events) != 1 || events[0].Rows == nil {
		t.Fatalf("没有解出行为事件: %#v", events)
	}
	ch := events[0].Rows.Changes
	if len(ch) != 1 {
		t.Fatalf("期望 1 行变更，拿到 %d", len(ch))
	}
	if ch[0].Kind != source.EvInsert {
		t.Fatalf("事件类型应为插入，拿到 %v", ch[0].Kind)
	}
	if got := ch[0].New.Row.Values[0]; got.Kind != source.KindInt || got.I != 42 {
		t.Fatalf("id 解错了: %#v", got)
	}
	if got := ch[0].New.Row.Values[1]; got.Kind != source.KindString || got.S != "alice" {
		t.Fatalf("name 解错了: %#v", got)
	}
	if ch[0].New.Row.Table.Schema != "shop" || ch[0].New.Row.Table.Table != "orders" {
		t.Fatalf("表名解错了: %#v", ch[0].New.Row.Table)
	}
	for i, ok := range ch[0].New.Present {
		if !ok {
			t.Fatalf("第 %d 列应当被标记为存在", i)
		}
	}
}

func TestParseUpdateKeepsOldAndNew(t *testing.T) {
	p, opt := newTestParser(t, true)
	cols := []testColumn{{typ: typeLong, name: "id"}, {typ: typeVarString, meta: 32, name: "note"}}
	if _, err := p.Parse(frame(evTableMap, tableMapBody(9, "s", "t", cols, []bool{false, true}, nil, nil), opt)); err != nil {
		t.Fatalf("表映射: %v", err)
	}
	before := rowImage([]bool{false, false}, le32(1), append([]byte{3}, []byte("old")...))
	after := rowImage([]bool{false, true}, le32(1), nil)
	body := updateBodyV2(9, 2, []bool{true, true}, []bool{true, false}, [][]byte{before, after})
	events, err := p.Parse(frame(evUpdateRowsV2, body, opt))
	if err != nil {
		t.Fatalf("解析更新失败: %v", err)
	}
	ch := events[0].Rows.Changes[0]
	if ch.Old.Row.Values[1].S != "old" {
		t.Fatalf("前镜像丢了: %#v", ch.Old.Row.Values[1])
	}
	if ch.New.Present[1] {
		t.Fatalf("后镜像里没写 name，不该标记为存在")
	}
	if ch.New.Row.Values[1].Kind != source.KindNull {
		t.Fatalf("缺席的列应保持零值，而不是被当成普通值")
	}
}

func TestParseUnsignedAndNull(t *testing.T) {
	p, opt := newTestParser(t, true)
	cols := []testColumn{{typ: typeLongLong, name: "big"}, {typ: typeLong, name: "maybe"}}
	tm := tableMapBody(11, "s", "t", cols, []bool{false, true}, []bool{true, true}, nil)
	if _, err := p.Parse(frame(evTableMap, tm, opt)); err != nil {
		t.Fatalf("表映射: %v", err)
	}
	img := rowImage([]bool{false, true}, le64(^uint64(0)))
	events, err := p.Parse(frame(evWriteRowsV2, rowsBodyV2(11, 2, []bool{true, true}, [][]byte{img}), opt))
	if err != nil {
		t.Fatalf("解析无符号/空值失败: %v", err)
	}
	vals := events[0].Rows.Changes[0].New.Row.Values
	if vals[0].Kind != source.KindUint || vals[0].U != ^uint64(0) {
		t.Fatalf("无符号 BIGINT 解错了: %#v", vals[0])
	}
	if vals[1].Kind != source.KindNull {
		t.Fatalf("NULL 解错了: %#v", vals[1])
	}
}

func TestParseRejectsCorruptChecksum(t *testing.T) {
	p, opt := newTestParser(t, true)
	cols := []testColumn{{typ: typeLong, name: "id"}}
	if _, err := p.Parse(frame(evTableMap, tableMapBody(3, "s", "t", cols, nil, nil, nil), opt)); err != nil {
		t.Fatalf("表映射: %v", err)
	}
	img := rowImage([]bool{false}, le32(1))
	raw := frame(evWriteRowsV2, rowsBodyV2(3, 1, []bool{true}, [][]byte{img}), opt)
	raw[len(raw)-1] ^= 0xff // 破坏 CRC
	if _, err := p.Parse(raw); err == nil {
		t.Fatal("校验和被破坏时必须报错，不能悄悄继续")
	}
}

func TestParseRowsWithoutTableMapFails(t *testing.T) {
	p, opt := newTestParser(t, true)
	img := rowImage([]bool{false}, le32(1))
	_, err := p.Parse(frame(evWriteRowsV2, rowsBodyV2(99, 1, []bool{true}, [][]byte{img}), opt))
	if err == nil {
		t.Fatal("没有表映射时解析行事件必须失败（否则会把行写到错误的表上）")
	}
}

// buildPayload frames a compressed transaction around the given events.
func buildPayload(t *testing.T, inner [][]byte, compressType byte) []byte {
	t.Helper()
	var plain []byte
	for _, e := range inner {
		plain = append(plain, e...)
	}
	payload := plain
	if compressType == compressionZSTD {
		enc, err := zstd.NewWriter(nil)
		if err != nil {
			t.Fatalf("zstd: %v", err)
		}
		payload = enc.EncodeAll(plain, nil)
		enc.Close()
	}
	body := []byte{payloadSizeField, 8}
	body = append(body, le64(uint64(len(payload)))...)
	body = append(body, payloadCompression, 1, compressType)
	body = append(body, payloadUncompSize, 8)
	body = append(body, le64(uint64(len(plain)))...)
	body = append(body, payloadHeaderEnd)
	body = append(body, payload...)
	return body
}

func innerTxn(t *testing.T, checksum bool) [][]byte {
	t.Helper()
	opt := frameOpts{checksum: checksum, logPos: 100}
	cols := []testColumn{{typ: typeLong, name: "id"}}
	sid := [16]byte{0x3e, 0x11, 0xfa, 0x47, 0x71, 0xca, 0x11, 0xe1, 0x9e, 0x33, 0xc8, 0x0a, 0xa9, 0x42, 0x95, 0x62}
	return [][]byte{
		frame(evGTID, gtidBody(sid, 5), opt),
		frame(evQuery, queryBody("s", "BEGIN"), opt),
		frame(evTableMap, tableMapBody(1, "s", "t", cols, nil, nil, nil), opt),
		frame(evWriteRowsV2, rowsBodyV2(1, 1, []bool{true}, [][]byte{rowImage([]bool{false}, le32(7))}), opt),
		frame(evXID, le64(1234), opt),
	}
}

func TestParseCompressedTransaction(t *testing.T) {
	p, opt := newTestParser(t, true)
	payload := frame(evTransactionPaylod, buildPayload(t, innerTxn(t, false), compressionZSTD), opt)
	events, err := p.Parse(payload)
	if err != nil {
		t.Fatalf("压缩事务解析失败: %v", err)
	}
	if len(events) != 5 {
		t.Fatalf("压缩事务里应有 5 个事件，拿到 %d", len(events))
	}
	if events[0].GTID == nil || events[0].GTID.GNO != 5 {
		t.Fatalf("第一个事件应是 GTID(5): %#v", events[0].GTID)
	}
	if events[3].Rows == nil || events[3].Rows.Changes[0].New.Row.Values[0].I != 7 {
		t.Fatalf("行数据没解出来")
	}
	if events[4].XID == nil || events[4].XID.XID != 1234 {
		t.Fatalf("提交事件没解出来: %#v", events[4].XID)
	}
}

func TestParseUncompressedPayload(t *testing.T) {
	p, opt := newTestParser(t, true)
	payload := frame(evTransactionPaylod, buildPayload(t, innerTxn(t, false), compressionNone), opt)
	events, err := p.Parse(payload)
	if err != nil {
		t.Fatalf("未压缩载荷解析失败: %v", err)
	}
	if len(events) != 5 {
		t.Fatalf("应有 5 个事件，拿到 %d", len(events))
	}
}

func TestCompressedUnknownAlgorithmIsExplicit(t *testing.T) {
	p, opt := newTestParser(t, true)
	payload := frame(evTransactionPaylod, buildPayload(t, innerTxn(t, false), 9), opt)
	_, err := p.Parse(payload)
	if err == nil {
		t.Fatal("不认识的压缩算法必须报错")
	}
	if !contains(err.Error(), "压缩") {
		t.Fatalf("错误信息要说清是压缩问题，拿到: %v", err)
	}
}

func TestCompressedGarbageZstdIsExplicit(t *testing.T) {
	p, opt := newTestParser(t, true)
	inner := innerTxn(t, false)
	body := []byte{payloadSizeField, 4}
	body = append(body, le32(4)...)
	body = append(body, payloadCompression, 1, compressionZSTD)
	body = append(body, payloadHeaderEnd)
	body = append(body, 0xde, 0xad, 0xbe, 0xef)
	_ = inner
	if _, err := p.Parse(frame(evTransactionPaylod, body, opt)); err == nil {
		t.Fatal("坏 zstd 数据必须报错")
	}
}

func TestQueryEventAndRotate(t *testing.T) {
	p, opt := newTestParser(t, true)
	events, err := p.Parse(frame(evQuery, queryBody("shop", "CREATE TABLE t (id int)"), opt))
	if err != nil {
		t.Fatalf("语句事件: %v", err)
	}
	if events[0].Query.Schema != "shop" || queryVerb(events[0].Query.Query) != "CREATE" {
		t.Fatalf("语句事件解错了: %#v", events[0].Query)
	}

	rotBody := append(le64(4), []byte("mysql-bin.000004")...)
	events, err = p.Parse(frame(evRotate, rotBody, opt))
	if err != nil {
		t.Fatalf("切换文件事件: %v", err)
	}
	if events[0].Rotate.NextFile != "mysql-bin.000004" || p.File() != "mysql-bin.000004" {
		t.Fatalf("切换文件后解析器的文件名没跟着变: %q", p.File())
	}
}

func TestPreviousGTIDs(t *testing.T) {
	sid := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	body := append(le64(1), sid[:]...)
	body = append(body, le64(2)...)
	body = append(body, le64(1)...)
	body = append(body, le64(5)...)
	body = append(body, le64(9)...)
	body = append(body, le64(12)...)
	set, err := parsePreviousGTIDs(body)
	if err != nil {
		t.Fatalf("文件开头事务号清单: %v", err)
	}
	if set.Count() != 9 { // 1-5 + 9-12
		t.Fatalf("事务数应为 9，拿到 %d（%s）", set.Count(), set.String())
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}
