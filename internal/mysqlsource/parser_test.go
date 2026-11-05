package mysqlsource

import (
	"testing"

	"github.com/spencerswagger/jinliang/internal/dsn"

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
	// 线上是半开区间：1-5 写成 (1,6)，9-12 写成 (9,13)。
	body = append(body, le64(1)...)
	body = append(body, le64(6)...)
	body = append(body, le64(9)...)
	body = append(body, le64(13)...)
	set, err := parsePreviousGTIDs(body)
	if err != nil {
		t.Fatalf("文件开头事务号清单: %v", err)
	}
	if set.Count() != 9 { // 1-5 + 9-12
		t.Fatalf("事务数应为 9，拿到 %d（%s）", set.Count(), set.String())
	}
	if got, want := set.String(), "01020304-0506-0708-090a-0b0c0d0e0f10:1-5:9-12"; got != want {
		t.Fatalf("区间文本 = %q，期望 %q（半开区间要还原成闭区间来显示）", got, want)
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

func TestParseRejectsMismatchedPostHeader(t *testing.T) {
	// 格式描述声明的后置头长度和我们的解析器期望不一致时，**Parse 自己必须返回错误**。
	// 只把问题存进字段、等调用方来问，等于没有校验：调用方一忘，就按错位读行镜像。
	p := NewParser()
	body := fdeBody("8.0.28", checksumCRC32)
	// 把 TABLE_MAP 的后置头从 8 改成 9（数组从事件类型 1 开始，索引是 evType-1）
	body[57+evTableMap-1] = 9
	_, err := p.Parse(frame(evFormatDescription, body, frameOpts{}))
	if err == nil {
		t.Fatal("后置头不符时 Parse 必须返回错误，而不是把事件交出去")
	}
	if !contains(err.Error(), "后置头") {
		t.Fatalf("错误信息要说清是后置头问题: %v", err)
	}
}

func TestParseAcceptsServerThatOmitsV1RowsLayouts(t *testing.T) {
	// 8.4/9.x 对 v1 行事件声明 0（它们只写 v2）：这不是布局不符，必须放行。
	p := NewParser()
	body := fdeBody("8.4.2", checksumCRC32)
	for _, typ := range []uint8{evWriteRowsV1, evUpdateRowsV1, evDeleteRowsV1} {
		body[57+typ-1] = 0
	}
	_ = body
	if _, err := p.Parse(frame(evFormatDescription, body, frameOpts{})); err != nil {
		t.Fatalf("声明为 0 表示「本服务器不写这种事件」，不该报错: %v", err)
	}
}

func TestServerIDDiffersPerJob(t *testing.T) {
	// 同机并行两个任务不能用同一个 server_id：服务端会当成「同一个副本重连」，
	// 把前一条连接踢掉（表现为时不时断流，极难与网络问题区分）。
	ep := dsn.Endpoint{Host: "127.0.0.1", Port: 3306, Database: "shop", Username: "u"}
	a1 := serverIDFor("job-a", ep)
	b1 := serverIDFor("job-b", ep)
	if a1 == b1 {
		t.Fatalf("同机两个 job 撞了同一个 server_id（%d）", a1)
	}
	if a1 < 1000 || b1 < 1000 {
		t.Fatalf("server_id 太小容易与真实副本撞号: %d / %d", a1, b1)
	}
	// 同一个 job 重启后必须是同一个号（服务端好把上次残留的连接踢掉）。
	if again := serverIDFor("job-a", ep); again != a1 {
		t.Fatalf("同一 job 的 server_id 必须稳定：%d → %d", a1, again)
	}
	// 不同源库也要不同号。
	other := dsn.Endpoint{Host: "127.0.0.1", Port: 3307, Database: "shop", Username: "u"}
	if serverIDFor("job-a", other) == a1 {
		t.Fatal("不同源库应得到不同 server_id")
	}
	// 没给 job 时随机兜底，多次调用不重复。
	seen := map[uint32]bool{}
	for i := 0; i < 8; i++ {
		id := serverIDFor("", ep)
		if id < 1000 {
			t.Fatalf("兜底 server_id %d 太小", id)
		}
		if seen[id] {
			t.Fatalf("无 job 时兜底 server_id 重复：%d", id)
		}
		seen[id] = true
	}
}

func TestPostHeaderLengthIndexing(t *testing.T) {
	// 锁死这个下标：FDE 的后置头数组**从事件类型 1 开始**（索引 = evType-1）。
	// 按 evType 直接当下标会整体偏移一格，表现是「上线后所有表都解析失败」，
	// 而且看起来像数据问题。
	fde := &FormatDescription{PostHeaderLens: make([]byte, 20)}
	fde.PostHeaderLens[evTableMap-1] = 8 // 类型 19 → 索引 18
	fde.PostHeaderLens[evQuery-1] = 13   // 类型 2 → 索引 1
	fde.PostHeaderLens[0] = 56           // 类型 1（START_V3）→ 索引 0

	if got, ok := fde.postHeaderLen(evTableMap); !ok || got != 8 {
		t.Fatalf("TABLE_MAP 后置头 = %d, %v；期望 8（索引必须是 evType-1）", got, ok)
	}
	if got, ok := fde.postHeaderLen(evQuery); !ok || got != 13 {
		t.Fatalf("QUERY 后置头 = %d, %v；期望 13", got, ok)
	}
	if got, ok := fde.postHeaderLen(1); !ok || got != 56 {
		t.Fatalf("事件类型 1 应读索引 0（拿到 %d, %v）", got, ok)
	}
	// 类型 0 不存在，不能读成索引 -1 而越界。
	if _, ok := fde.postHeaderLen(0); ok {
		t.Fatal("事件类型 0 不存在，应返回 false")
	}
	// 超出数组长度也要安全返回 false。
	if _, ok := fde.postHeaderLen(40); ok {
		t.Fatal("超出数组长度应返回 false")
	}
}

func TestPostHeaderZeroOnlyToleratedForV1Rows(t *testing.T) {
	// 「声明为 0」只对 v1 行事件放行（8.4/9.x 不写它们）。
	// 其它类型声明 0 必须仍然报错——一刀切跳过等于把校验关掉。
	p := NewParser()
	body := fdeBody("8.4.2", checksumCRC32)
	body[57+evTableMap-1] = 0 // TABLE_MAP 声明 0：读不懂
	if _, err := p.Parse(frame(evFormatDescription, body, frameOpts{})); err == nil {
		t.Fatal("TABLE_MAP 声明 0 时必须报错（那不是「服务器不写」，是我们读不懂）")
	}
	if !contains(p.HeaderError().Error(), "事件类型 19") {
		t.Fatalf("错误应指向具体的类型: %v", p.HeaderError())
	}
}
