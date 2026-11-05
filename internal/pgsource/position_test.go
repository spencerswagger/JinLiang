package pgsource

import "testing"

func TestPositionRoundTrip(t *testing.T) {
	cases := []string{"0/0", "0/16B3748", "1/4CC43CF0", "FFFFFFFF/FFFFFFFF"}
	for _, in := range cases {
		p, err := ParsePosition(in)
		if err != nil {
			t.Fatalf("ParsePosition(%q) 失败: %v", in, err)
		}
		if got := p.String(); got != in {
			t.Fatalf("往返不一致：%q → %v → %q", in, p.LSN, got)
		}
	}
}

func TestParsePositionRejectsGarbage(t *testing.T) {
	for _, in := range []string{"LSN", "0/ZZ", "-1"} {
		if _, err := ParsePosition(in); err == nil {
			t.Fatalf("ParsePosition(%q) 应该报错，却通过了", in)
		}
	}
}

func TestParsePositionEmptyIsZero(t *testing.T) {
	p, err := ParsePosition("  ")
	if err != nil {
		t.Fatalf("空位点不该报错: %v", err)
	}
	if !p.IsZero() {
		t.Fatalf("空位点应该是零值，得到 %v", p.LSN)
	}
}

func TestPositionCompare(t *testing.T) {
	a, _ := ParsePosition("0/1000")
	b, _ := ParsePosition("0/2000")
	if a.Compare(b) != -1 || b.Compare(a) != 1 || a.Compare(a) != 0 {
		t.Fatalf("位点比较不对：%v %v", a.LSN, b.LSN)
	}
}

// 位点只能往前走：Ack 必须单调，倒退的确认会让源库提前释放变更流水（丢数据）。
func TestAckIsMonotonic(t *testing.T) {
	s := &Source{}
	p1, _ := ParsePosition("0/2000")
	p2, _ := ParsePosition("0/1000")
	s.Ack(p1)
	s.Ack(p2) // 更旧的位点：必须被忽略
	if got := s.AckedPosition(); got.String() != "0/2000" {
		t.Fatalf("Ack 不该回退：期望 0/2000，得到 %s", got.String())
	}
	s.Ack(p1)
	if got := s.AckedPosition().String(); got != "0/2000" {
		t.Fatalf("重复确认同一个位点应保持幂等，得到 %s", got)
	}
}

func TestJoinSplitCursor(t *testing.T) {
	parts := []string{"42", "2026-01-02 03:04:05+00"}
	got := splitCursor(joinCursor(parts))
	if len(got) != len(parts) {
		t.Fatalf("复合游标拆分数量不对：%v", got)
	}
	for i := range parts {
		if got[i] != parts[i] {
			t.Fatalf("第 %d 段不一致：%v != %s", i, got[i], parts[i])
		}
	}
}

func TestCursorTextKeepsTypesRoundTrippable(t *testing.T) {
	if got := cursorText(int64(42)); got != "42" {
		t.Fatalf("整数游标: %q", got)
	}
	if got := cursorText([]byte{0xde, 0xad}); got != `\xdead` {
		t.Fatalf("字节游标应为 PostgreSQL 十六进制形式: %q", got)
	}
	if got := cursorText(true); got != "t" {
		t.Fatalf("布尔游标: %q", got)
	}
}

// 安静期的安全确认点：只有「两次心跳位置相同」才给。
// 发送端还在前进时给位点，会把还没解码的变更跳过去。
func TestSafeAckPointNeedsStableKeepalives(t *testing.T) {
	s := &Source{}
	if _, ok := s.SafeAckPoint(); ok {
		t.Fatal("没有任何心跳时不该给安全位点")
	}
	s.keepEnd.Store(1000)
	if _, ok := s.SafeAckPoint(); ok {
		t.Fatal("只有一次心跳时不该给安全位点（可能还在发）")
	}
	s.keepPrev.Store(1000)
	p, ok := s.SafeAckPoint()
	if !ok || p.LSN != 1000 {
		t.Fatalf("两次相同应给出该位点，得到 %v %v", p.LSN, ok)
	}
	s.keepPrev.Store(900)
	if _, ok := s.SafeAckPoint(); ok {
		t.Fatal("位置在推进时不该给安全位点")
	}
}
