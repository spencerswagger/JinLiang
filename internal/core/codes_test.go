package core

import (
	"regexp"
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
