package mysqlsource

import (
	"strings"
	"testing"
	"time"
)

func TestGTIDSetParseAndFormat(t *testing.T) {
	set, err := ParseGTIDSet("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5:7,3e11fa47-71ca-11e1-9e33-c80aa9429562:9")
	if err != nil {
		t.Fatalf("解析失败: %v", err)
	}
	// 相邻区间必须合并：1-5、7、9
	if got, want := set.String(), "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5:7:9"; got != want {
		t.Fatalf("规范化输出 = %q，期望 %q", got, want)
	}
	if set.Count() != 7 {
		t.Fatalf("事务数 = %d，期望 7", set.Count())
	}
}

func TestGTIDSetMergeSortsUUIDs(t *testing.T) {
	a, err := ParseGTIDSet("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-2")
	if err != nil {
		t.Fatal(err)
	}
	b, err := ParseGTIDSet("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:5")
	if err != nil {
		t.Fatal(err)
	}
	a.Merge(b)
	want := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:5,bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb:1-2"
	if got := a.String(); got != want {
		t.Fatalf("合并后 = %q，期望 %q（顺序必须稳定，位点要按文本比较）", got, want)
	}
}

func TestGTIDSetContains(t *testing.T) {
	have, _ := ParseGTIDSet("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-10")
	cases := []struct {
		other string
		want  bool
	}{
		{"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-10", true},
		{"3e11fa47-71ca-11e1-9e33-c80aa9429562:3", true},
		{"3e11fa47-71ca-11e1-9e33-c80aa9429562:11", false},
		{"", true},
	}
	for _, c := range cases {
		other, err := ParseGTIDSet(c.other)
		if err != nil {
			t.Fatalf("%q: %v", c.other, err)
		}
		if got := have.Contains(other); got != c.want {
			t.Fatalf("Contains(%q) = %v，期望 %v", c.other, got, c.want)
		}
	}
}

func TestGTIDSetAdd(t *testing.T) {
	set := GTIDSet{}
	if err := set.Add("3e11fa47-71ca-11e1-9e33-c80aa9429562:3"); err != nil {
		t.Fatal(err)
	}
	if err := set.Add("3e11fa47-71ca-11e1-9e33-c80aa9429562:4"); err != nil {
		t.Fatal(err)
	}
	if got := set.String(); got != "3e11fa47-71ca-11e1-9e33-c80aa9429562:3-4" {
		t.Fatalf("相邻事务号应合并，拿到 %q", got)
	}
}

func TestGTIDSetTagged(t *testing.T) {
	set, err := ParseGTIDSet("3e11fa47-71ca-11e1-9e33-c80aa9429562:tag_a:1-2")
	if err != nil {
		t.Fatalf("8.4 的 GTID tag 应能解析: %v", err)
	}
	if got := set.String(); got != "3e11fa47-71ca-11e1-9e33-c80aa9429562:tag_a:1-2" {
		t.Fatalf("带标签的集合输出 = %q", got)
	}
}

func TestGTIDSetRejectsGarbage(t *testing.T) {
	for _, bad := range []string{"not-a-uuid:1", "3e11fa47-71ca-11e1-9e33-c80aa9429562", "3e11fa47-71ca-11e1-9e33-c80aa9429562:0", "3e11fa47-71ca-11e1-9e33-c80aa9429562:5-1"} {
		if _, err := ParseGTIDSet(bad); err == nil {
			t.Fatalf("%q 应当被拒绝", bad)
		}
	}
}

func TestPositionRoundTripFileAndPos(t *testing.T) {
	at := time.Date(2026, 11, 6, 0, 30, 15, 123_456_789, time.UTC)
	p := Position{File: "mysql-bin.000003", Pos: 197, TakenAt: at, ServerID: 1}
	text := p.String()

	back, err := ParsePosition(text)
	if err != nil {
		t.Fatalf("回读失败: %v", err)
	}
	if back.File != p.File || back.Pos != p.Pos || back.ServerID != p.ServerID {
		t.Fatalf("往返丢字段: %#v vs %#v", back, p)
	}
	if !back.TakenAt.Equal(at.Truncate(time.Millisecond)) {
		t.Fatalf("取位点时间应保留到毫秒: 得到 %s，期望 %s", back.TakenAt, at)
	}
	if back.GTID.IsEmpty() != true {
		t.Fatalf("没有 GTID 时集合应为空")
	}
}

func TestPositionRoundTripGTID(t *testing.T) {
	set, err := ParseGTIDSet("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")
	if err != nil {
		t.Fatal(err)
	}
	p := Position{File: "mysql-bin.000010", Pos: 4, GTID: set, TakenAt: time.UnixMilli(1_760_000_000_123).UTC(), ServerID: 42}
	back, err := ParsePosition(p.String())
	if err != nil {
		t.Fatalf("回读失败: %v", err)
	}
	if !back.GTID.Equal(set) {
		t.Fatalf("GTID 集合往返丢了: %q", back.GTID.String())
	}
	if back.TakenAt.UnixMilli() != 1_760_000_000_123 {
		t.Fatalf("毫秒时间戳往返错了: %d", back.TakenAt.UnixMilli())
	}
}

func TestPositionMillisecondNotSecond(t *testing.T) {
	// 同一秒内的两个位点必须能区分开：秒级精度会丢掉这个差别。
	a := Position{File: "mysql-bin.000001", Pos: 100, TakenAt: time.UnixMilli(1_700_000_000_001).UTC()}
	b := Position{File: "mysql-bin.000001", Pos: 200, TakenAt: time.UnixMilli(1_700_000_000_999).UTC()}
	pa, err := ParsePosition(a.String())
	if err != nil {
		t.Fatal(err)
	}
	pb, err := ParsePosition(b.String())
	if err != nil {
		t.Fatal(err)
	}
	if pa.TakenAt.Equal(pb.TakenAt) {
		t.Fatalf("两个位点的时间被抹平了（说明只精确到秒）")
	}
	if got := pa.TakenAt.UnixMilli(); got != 1_700_000_000_001 {
		t.Fatalf("毫秒丢失: %d", got)
	}
}

func TestPositionCompareAcrossFiles(t *testing.T) {
	a := Position{File: "mysql-bin.000009", Pos: 900}
	b := Position{File: "mysql-bin.000010", Pos: 4}
	if a.Compare(b) >= 0 {
		t.Fatalf("mysql-bin.000009 必须排在 mysql-bin.000010 之前")
	}
	if b.Compare(a) <= 0 {
		t.Fatalf("反向比较也不对")
	}
	same := Position{File: "mysql-bin.000010", Pos: 4}
	if b.Compare(same) != 0 {
		t.Fatalf("相同位点应相等")
	}
}

func TestPositionParseToleratesUnknownKeys(t *testing.T) {
	p, err := ParsePosition("mysql:v1:file=mysql-bin.000002;pos=4;future=whatever;srv=7")
	if err != nil {
		t.Fatalf("多了未来字段不该读不出来: %v", err)
	}
	if p.File != "mysql-bin.000002" || p.ServerID != 7 {
		t.Fatalf("已知字段丢了: %#v", p)
	}
}

func TestPositionParseRejectsBroken(t *testing.T) {
	for _, bad := range []string{
		"", "v1", "v1;pos=4", "mysql", "mysql:v1", "mysql:v1:",
		"mysql:v2:file=x;pos=4", // 版本未知
		"v1;file=x;pos=4",       // 没有 kind 位（老格式）
		"mysql:v1:file=x;pos=abc",
	} {
		if _, err := ParsePosition(bad); err == nil {
			t.Fatalf("%q 应当被拒绝", bad)
		}
	}
}

func TestRestartFromPrefersGTID(t *testing.T) {
	set, _ := ParseGTIDSet("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")
	saved := Position{File: "mysql-bin.000003", Pos: 197, GTID: set}
	sourceSet, _ := ParseGTIDSet("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-9")

	got := RestartFrom(saved, sourceSet, 9)
	if !got.GTID.Equal(set) {
		t.Fatalf("源端还认这些事务号时，应按 GTID 续上")
	}
	if got.ServerID != 9 {
		t.Fatalf("server_id 应更新为当前源库")
	}

	// 源端没有 GTID：退回文件名+偏移，并清掉不可用的 GTID。
	plain := RestartFrom(saved, GTIDSet{}, 9)
	if !plain.GTID.IsEmpty() {
		t.Fatalf("源端没有 GTID 时必须清掉，否则会用不存在的坐标去续")
	}
	if plain.File != saved.File || plain.Pos != saved.Pos {
		t.Fatalf("退回后文件/偏移必须保留")
	}
}

func TestPositionAdvance(t *testing.T) {
	set, _ := ParseGTIDSet("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")
	p := Position{File: "mysql-bin.000001", Pos: 4}
	p.Advance("mysql-bin.000001", 512, set, time.UnixMilli(1_700_000_000_500).UTC())
	if p.Pos != 512 {
		t.Fatalf("偏移没推进")
	}
	if !p.GTID.Equal(set) {
		t.Fatalf("GTID 没写进去")
	}
	if p.TakenAt.UnixMilli() != 1_700_000_000_500 {
		t.Fatalf("时间没写进去")
	}
}

// —— 契约 delta（PR #7）：Kind 与 ParsePosition ——

func TestPositionKindMatchesEndpointKind(t *testing.T) {
	// 契约要求 Kind() 与 Endpoint.Kind() 同值，引擎拿它做「位点 vs 连接类型」对账。
	var p Position
	if got := p.Kind(); got != "mysql" {
		t.Fatalf("Position.Kind() = %q，期望 \"mysql\"（与 Endpoint.Kind() 同值）", got)
	}
	var s Source
	if p.Kind() != s.Kind() {
		t.Fatalf("Position.Kind()=%q 与 Source.Kind()=%q 必须一致，否则对账会误报 E3001", p.Kind(), s.Kind())
	}
}

func TestPositionCoordinateSeparatesTwoAxes(t *testing.T) {
	filePos := Position{File: "mysql-bin.000003", Pos: 197}
	if got := filePos.Coordinate(); got != CoordFilePos {
		t.Fatalf("没有事务号的位点应走 file+pos，拿到 %q", got)
	}
	set, err := ParseGTIDSet("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")
	if err != nil {
		t.Fatal(err)
	}
	gtidPos := Position{File: "mysql-bin.000003", Pos: 197, GTID: set}
	if got := gtidPos.Coordinate(); got != CoordGTID {
		t.Fatalf("有事务号的位点应走 GTID，拿到 %q", got)
	}
	if CoordFilePos == CoordGTID {
		t.Fatal("两套位点不能共用一个名字")
	}
}

func TestPositionCoordRoundTrip(t *testing.T) {
	set, _ := ParseGTIDSet("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")
	for _, want := range []Coordinate{CoordFilePos, CoordGTID} {
		p := Position{File: "mysql-bin.000001", Pos: 4, TakenAt: time.UnixMilli(1_700_000_000_000).UTC()}
		if want == CoordGTID {
			p.GTID = set
		}
		p.Coord = want
		text := p.String()
		if !contains(text, "coord="+string(want)) {
			t.Fatalf("位点串里应写明坐标类型: %q", text)
		}
		back, err := ParsePosition(text)
		if err != nil {
			t.Fatalf("%s 回读失败: %v", want, err)
		}
		if back.Coordinate() != want {
			t.Fatalf("坐标类型往返丢了: %q → %q", want, back.Coordinate())
		}
	}
}

func TestParsePositionRejectsInconsistentCoord(t *testing.T) {
	// 说按事务号续，却没有事务号：这种位点必须拒绝，不能拿文件名硬续。
	if _, err := ParsePosition("mysql:v1:coord=mysql_gtid;file=mysql-bin.000001;pos=4"); err == nil {
		t.Fatal("coord=mysql_gtid 却没有 gtid，应当报错")
	}
	if _, err := ParsePosition("mysql:v1:coord=oracle_scn;file=x;pos=4"); err == nil {
		t.Fatal("不认识的坐标类型应当报错")
	}
}

func TestSourceParsePositionAcceptsBothForms(t *testing.T) {
	s := &Source{}
	set, _ := ParseGTIDSet("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5")
	inputs := []Position{
		{File: "mysql-bin.000003", Pos: 197, TakenAt: time.UnixMilli(1_700_000_000_123).UTC()},
		{File: "mysql-bin.000003", Pos: 197, GTID: set, TakenAt: time.UnixMilli(1_700_000_000_123).UTC()},
	}
	for _, in := range inputs {
		got, err := s.ParsePosition(in.String())
		if err != nil {
			t.Fatalf("%q 应能解析: %v", in.String(), err)
		}
		p, ok := got.(Position)
		if !ok {
			t.Fatalf("返回类型不对: %T", got)
		}
		if p.File != in.File || p.Pos != in.Pos || !p.GTID.Equal(in.GTID) {
			t.Fatalf("往返丢字段: %#v vs %#v", p, in)
		}
		if p.Kind() != s.Kind() {
			t.Fatalf("解析出来的位点 Kind 必须与连接类型一致")
		}
	}
}

func TestSourceParsePositionRejectsForeignIsExplicit(t *testing.T) {
	s := &Source{}
	// 别人的位点：E1010（user_action：换回对应的源），不要用 E3001 把人引去重搬全量。
	foreign := []string{"postgres:v1:0/16B6C50", "oracle:v1:scn-42"}
	for _, raw := range foreign {
		_, err := s.ParsePosition(raw)
		if err == nil {
			t.Fatalf("%q 应当被拒绝", raw)
		}
		if !contains(err.Error(), "E1010") {
			t.Fatalf("%q 的错误里应给出 E1010，拿到: %v", raw, err)
		}
		if contains(err.Error(), "E3001") {
			t.Fatalf("%q 不能报 E3001（会把人引去重搬全量）: %v", raw, err)
		}
	}
	// 格式坏掉（缺段/版本未知）：E3001，因为这种位点确实只能重搬。
	malformed := []string{"", "0/16B6C50", "mysql:v1", "mysql:v9:file=x;pos=4", "mysql:v1:file=x;pos=abc"}
	for _, raw := range malformed {
		_, err := s.ParsePosition(raw)
		if err == nil {
			t.Fatalf("%q 应当被拒绝", raw)
		}
		if !contains(err.Error(), "E3001") {
			t.Fatalf("%q 的错误里应给出 E3001，拿到: %v", raw, err)
		}
	}
}

func TestParsePositionNeverPanics(t *testing.T) {
	junk := []string{
		"", ";", ";;;;", "v1", "v1;", "mysql", "mysql:", "mysql::", "mysql:v1", "mysql:v1:",
		"mysql:v1:;", "mysql:v1:file", "mysql:v1:file=", "mysql:v1:pos=", "mysql:v1:file=;pos=0",
		"mysql:v1:coord=", "mysql:v1:gtid=zzz", "mysql:v1:file=x;pos=99999999999999999999",
		"mysql:v1:file=x;pos=4;t=abc", "mysql:v1:file=x;pos=4;srv=-1", "mysql:v1:;;;",
		"mysql:v1:file=x;pos=4;coord=mysql_gtid",
		"mysql:v1:" + strings.Repeat("file=x;pos=4;", 100),
		"postgres:v1:0/16B6C50",
	}
	for _, raw := range junk {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("解析 %q 时 panic 了: %v", raw, r)
				}
			}()
			_, _ = ParsePosition(raw)
			_, _ = (&Source{}).ParsePosition(raw)
		}()
	}
}

func TestEncodeGTIDSetUsesHalfOpenIntervals(t *testing.T) {
	// COM_BINLOG_DUMP_GTID 的线上格式是半开区间 [start, stop)：
	// 1-5 必须写成 start=1, stop=6。写成 stop-1 会少一个事务，服务器就把那笔重放一遍。
	set, err := ParseGTIDSet("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5:7")
	if err != nil {
		t.Fatal(err)
	}
	raw := encodeGTIDSet(set)
	if len(raw) != 8+16+8+2*16 {
		t.Fatalf("编码长度 %d 不对（应 8+16+8+2*16）", len(raw))
	}
	if got := le64ToUint(raw[0:8]); got != 1 {
		t.Fatalf("n_sids = %d，期望 1", got)
	}
	if got := le64ToUint(raw[24:32]); got != 2 {
		t.Fatalf("n_intervals = %d，期望 2", got)
	}
	if got := le64ToUint(raw[32:40]); got != 1 {
		t.Fatalf("第一个区间起点 = %d，期望 1", got)
	}
	if got := le64ToUint(raw[40:48]); got != 6 {
		t.Fatalf("第一个区间终点 = %d，期望 6（半开区间：1-5 的 end 是 6）", got)
	}
	if got := le64ToUint(raw[48:56]); got != 7 {
		t.Fatalf("第二个区间起点 = %d，期望 7", got)
	}
	if got := le64ToUint(raw[56:64]); got != 8 {
		t.Fatalf("第二个区间终点 = %d，期望 8", got)
	}
}

func le64ToUint(b []byte) uint64 {
	var v uint64
	for i := len(b) - 1; i >= 0; i-- {
		v = v<<8 | uint64(b[i])
	}
	return v
}
