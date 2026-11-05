package runtime

import (
	"errors"
	"testing"
)

// ① 裸 LSN：没有版本壳也要能解析（旧库/手工写入）。
func TestParseLSNBare(t *testing.T) {
	got, err := ParseLSN("0/16B6C50")
	if err != nil {
		t.Fatal(err)
	}
	if want := uint64(0x16B6C50); got != want {
		t.Fatalf("ParseLSN = %x, want %x", got, want)
	}
}

// ② 引擎写的版本化 PG 位点：postgres:v1:0/16B6C50。
func TestParseLSNVersioned(t *testing.T) {
	got, err := ParseLSN("postgres:v1:0/16B6C50")
	if err != nil {
		t.Fatalf("versioned LSN must parse: %v", err)
	}
	if want := uint64(0x16B6C50); got != want {
		t.Fatalf("ParseLSN = %x, want %x", got, want)
	}
}

// ③ 两边都版本化：一致性级必须能比较，而不是 position_unparseable。
func TestCompareLSNBothVersioned(t *testing.T) {
	cmp, err := CompareLSN("postgres:v1:0/1000", "postgres:v1:0/2000")
	if err != nil {
		t.Fatalf("both versioned must compare: %v", err)
	}
	if cmp != -1 {
		t.Fatalf("cmp = %d, want -1", cmp)
	}
	cmp, err = CompareLSN("0/1000", "postgres:v1:0/1000")
	if err != nil || cmp != 0 {
		t.Fatalf("bare vs versioned equal: cmp=%d err=%v", cmp, err)
	}
}

// ④ 版本壳对、载荷坏：必须是格式错，不能当成一个 LSN。
func TestParseLSNVersionedBadPayload(t *testing.T) {
	if _, err := ParseLSN("postgres:v1:not-an-lsn"); !errors.Is(err, ErrPositionMalformed) {
		t.Fatalf("want ErrPositionMalformed, got %v", err)
	}
	if _, err := ParseLSN("postgres:v1:"); !errors.Is(err, ErrPositionMalformed) {
		t.Fatalf("empty payload: want ErrPositionMalformed, got %v", err)
	}
}

// ⑤ 别的连接器的位点传给 PG 解析器：这是 E1010（换错了源），
// 不能和「格式坏了」共用一个错误码。
func TestParseLSNRejectsForeignKind(t *testing.T) {
	for _, raw := range []string{
		"mysql:v1;file=mysql-bin.000003;pos=1234",
		"mysql:v1:mysql-bin.000003:1234",
	} {
		_, err := ParseLSN(raw)
		if !errors.Is(err, ErrPositionKindMismatch) {
			t.Errorf("ParseLSN(%q) = %v, want ErrPositionKindMismatch", raw, err)
		}
		if errors.Is(err, ErrPositionMalformed) {
			t.Errorf("ParseLSN(%q) must not be reported as malformed", raw)
		}
	}
}

func TestCompareLSN(t *testing.T) {
	if cmp, err := CompareLSN("0/1000", "0/2000"); err != nil || cmp != -1 {
		t.Fatalf("expected -1, got %d err=%v", cmp, err)
	}
	if cmp, err := CompareLSN("1/0", "0/FFFF"); err != nil || cmp != 1 {
		t.Fatalf("expected 1, got %d err=%v", cmp, err)
	}
	if _, err := CompareLSN("not-an-lsn", "0/1"); !errors.Is(err, ErrPositionMalformed) {
		t.Fatalf("unparseable LSN must return ErrPositionMalformed, got %v", err)
	}
}

func TestBinlogPosFormats(t *testing.T) {
	cases := []struct {
		in   string
		file string
		pos  int64
		ok   bool
	}{
		{"mysql-bin.000003:1234", "mysql-bin.000003", 1234, true},
		{"mysql:v1:mysql-bin.000003:1234", "mysql-bin.000003", 1234, true},
		{"mysql:v1;file=mysql-bin.000003;pos=1234", "mysql-bin.000003", 1234, true},
		{"v1;file=mysql-bin.000004;pos=99", "mysql-bin.000004", 99, true},
		{"mysql:v1;file=mysql-bin.000004;pos=99;gtid=abc", "mysql-bin.000004", 99, true},
		{"mysql:v1;file=mysql-bin.000004", "", 0, false},
		{"mysql:v1;file=;pos=12", "", 0, false},
		{"mysql:v1;file=x;pos=not-a-number", "", 0, false},
		{"gtid:3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5", "", 0, false},
		{"", "", 0, false},
	}
	for _, tc := range cases {
		file, pos, ok := BinlogPos(tc.in)
		if ok != tc.ok || file != tc.file || pos != tc.pos {
			t.Errorf("BinlogPos(%q) = (%q,%d,%v), want (%q,%d,%v)", tc.in, file, pos, ok, tc.file, tc.pos, tc.ok)
		}
	}
}

func TestParseBinlogPosForeignKind(t *testing.T) {
	_, _, err := ParseBinlogPos("postgres:v1:0/16B6C50")
	if !errors.Is(err, ErrPositionKindMismatch) {
		t.Fatalf("PG position handed to MySQL parser must be E1010, got %v", err)
	}
}
