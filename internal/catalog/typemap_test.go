package catalog

import "testing"

func TestMap(t *testing.T) {
	cases := []struct {
		name     string
		from, to Engine
		in       string
		wantType string
		wantLoss Loss
		wantOK   bool
	}{
		{"pg bool -> mysql", Postgres, MySQL, "boolean", "tinyint(1)", Lossless, true},
		{"pg text -> mysql", Postgres, MySQL, "text", "text", Lossless, true},
		{"pg varchar keeps length", Postgres, MySQL, "varchar(255)", "varchar(255)", Lossless, true},
		{"pg varchar too wide", Postgres, MySQL, "varchar(20000)", "text", NeedsConfirm, true},
		{"pg numeric keeps scale", Postgres, MySQL, "numeric(12, 2)", "decimal(12, 2)", Lossless, true},
		{"pg timestamptz needs confirm", Postgres, MySQL, "timestamptz", "datetime(6)", NeedsConfirm, true},
		{"pg array -> json", Postgres, MySQL, "text[]", "json", NeedsConfirm, true},
		{"pg uuid", Postgres, MySQL, "uuid", "char(36)", Lossless, true},
		{"mysql tinyint(1) needs confirm", MySQL, Postgres, "tinyint(1)", "boolean", NeedsConfirm, true},
		{"mysql bigint unsigned", MySQL, Postgres, "bigint unsigned", "numeric(20,0)", NeedsConfirm, true},
		{"mysql datetime lossy", MySQL, Postgres, "datetime", "timestamp", Lossy, true},
		{"mysql json -> jsonb", MySQL, Postgres, "json", "jsonb", Lossy, true},
		{"same engine passthrough", Postgres, Postgres, "numeric(12,2)", "numeric(12,2)", Lossless, true},
		{"unknown type falls back to text", Postgres, MySQL, "tsrange", "text", Unsupported, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := Map(c.from, c.to, c.in)
			if ok != c.wantOK {
				t.Fatalf("ok = %v, want %v", ok, c.wantOK)
			}
			if got.Target != c.wantType {
				t.Errorf("target = %q, want %q", got.Target, c.wantType)
			}
			if got.Loss != c.wantLoss {
				t.Errorf("loss = %v, want %v", got.Loss, c.wantLoss)
			}
		})
	}
}

func TestNormalizeType(t *testing.T) {
	for _, c := range []struct{ in, base, args string }{
		{"VARCHAR(255)", "varchar", "255"},
		{"numeric(12, 2)", "numeric", "12, 2"},
		{"BIGINT UNSIGNED", "bigint unsigned", ""},
		{"timestamp with time zone", "timestamp with time zone", ""},
	} {
		b, a := normalizeType(c.in)
		if b != c.base || a != c.args {
			t.Errorf("normalize(%q) = (%q,%q), want (%q,%q)", c.in, b, a, c.base, c.args)
		}
	}
}
