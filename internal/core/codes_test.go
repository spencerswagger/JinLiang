package core

import (
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestSpecs(t *testing.T) {
	re := regexp.MustCompile(`^E[1-4][0-9]{3}$`)
	seen := map[Code]bool{}
	for _, s := range Catalogue() {
		if !re.MatchString(string(s.Code)) {
			t.Errorf("%s: code must look like E1xxx..E4xxx", s.Code)
		}
		if seen[s.Code] {
			t.Errorf("%s: duplicate code", s.Code)
		}
		seen[s.Code] = true
		if got := ClassOf(s.Code); got != s.Class {
			t.Errorf("%s: class %s does not match code prefix (%s)", s.Code, s.Class, got)
		}
		if n := utf8.RuneCountInString(s.Message); n == 0 || n > 40 {
			t.Errorf("%s: message should be short and human (%d runes)", s.Code, n)
		}
		if s.Why == "" || s.Risk == "" {
			t.Errorf("%s: four-part prompt needs why and risk", s.Code)
		}
		if len(s.Next) == 0 || len(s.Next) > 3 {
			t.Errorf("%s: needs 1-3 concrete next steps, got %d", s.Code, len(s.Next))
		}
		for _, step := range s.Next {
			if utf8.RuneCountInString(step) == 0 {
				t.Errorf("%s: empty next step", s.Code)
			}
		}
	}
	if len(seen) < 10 {
		t.Fatalf("expected a meaningful catalogue, got %d codes", len(seen))
	}
}

func TestSpecLookup(t *testing.T) {
	if _, ok := Lookup(CodeWalLevel); !ok {
		t.Fatal("CodeWalLevel must be in the catalogue")
	}
	if _, ok := Lookup("E9999"); ok {
		t.Fatal("unknown code must not resolve")
	}
}

// TestSchemaMismatchCoversBothReasons：E1015 的两条原因码（type_changed / schema_drift）
// 与「哪张表哪一列看 last_error」这句都必须在用户可见文案里，
// 否则用户只拿到一个码、不知道到底哪种情况。
func TestSchemaMismatchCoversBothReasons(t *testing.T) {
	spec, ok := Lookup(CodeSchemaMismatch)
	if !ok {
		t.Fatal("E1015 必须在码表里")
	}
	for _, want := range []string{"type_changed", "schema_drift", "last_error"} {
		if !strings.Contains(spec.Why+strings.Join(spec.Next, " "), want) {
			t.Errorf("E1015 的四段式里没提到 %q：%s / %v", want, spec.Why, spec.Next)
		}
	}
	if strings.Contains(spec.Risk, "数据会不一致") {
		t.Error("E1015 的 Risk 不该写「数据会不一致」——它是「这张表停止更新」，不是数据错了")
	}
}
