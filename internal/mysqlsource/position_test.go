package mysqlsource

import (
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
	p, err := ParsePosition("v1;file=mysql-bin.000002;pos=4;future=whatever;srv=7")
	if err != nil {
		t.Fatalf("多了未来字段不该读不出来: %v", err)
	}
	if p.File != "mysql-bin.000002" || p.ServerID != 7 {
		t.Fatalf("已知字段丢了: %#v", p)
	}
}

func TestPositionParseRejectsBroken(t *testing.T) {
	for _, bad := range []string{"", "v1", "v1;pos=4", "v2;file=x;pos=4", "v1;file=x;pos=abc"} {
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
