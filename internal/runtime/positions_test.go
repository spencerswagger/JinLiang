package runtime

import "testing"

func TestBinlogPosFormats(t *testing.T) {
	cases := []struct {
		in   string
		file string
		pos  int64
		ok   bool
	}{
		{"mysql-bin.000003:1234", "mysql-bin.000003", 1234, true},
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

func TestCompareLSN(t *testing.T) {
	if cmp, ok := CompareLSN("0/1000", "0/2000"); !ok || cmp != -1 {
		t.Fatalf("expected -1, got %d ok=%v", cmp, ok)
	}
	if cmp, ok := CompareLSN("1/0", "0/FFFF"); !ok || cmp != 1 {
		t.Fatalf("expected 1, got %d ok=%v", cmp, ok)
	}
	if _, ok := CompareLSN("not-an-lsn", "0/1"); ok {
		t.Fatal("unparseable LSN must not compare")
	}
}
