package state

import (
	"regexp"
	"strings"
	"testing"
)

func TestJobKeyIgnoresPasswordAndName(t *testing.T) {
	a := JobKey("postgres://app:secret@db1:5432/shop", "mysql://app:pw@db2:3306/report", "public: orders*,customers")
	b := JobKey("postgres://app:rotated@db1:5432/shop", "mysql://app:newpw@db2:3306/report", "public:  orders*,customers ")
	if a != b {
		t.Fatalf("job key should ignore password and whitespace: %q != %q", a, b)
	}
	if len(a) != keyLen {
		t.Fatalf("key length = %d, want %d", len(a), keyLen)
	}
	if !regexp.MustCompile(`^[a-z2-7]+$`).MatchString(a) {
		t.Fatalf("key %q must be lowercase alphanumeric base32", a)
	}
}

func TestJobKeyDiffers(t *testing.T) {
	base := JobKey("pg://u@h/db", "my://u@h/db", "public: a")
	cases := []string{
		JobKey("pg://u@other-host/db", "my://u@h/db", "public: a"),
		JobKey("pg://u@h/db", "my://u@other/db", "public: a"),
		JobKey("pg://u@h/db", "my://u@h/db", "public: a,b"),
	}
	for i, c := range cases {
		if c == base {
			t.Errorf("case %d should differ from base", i)
		}
	}
}

func TestObjectName(t *testing.T) {
	key := JobKey("pg://u@h/db", "my://u@h/db", "public: a")
	for _, n := range []int{0, 1, 42, 999} {
		name, err := ObjectName("slot", key, n)
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if len(name) > NameMax {
			t.Fatalf("name %q too long", name)
		}
		if !strings.HasPrefix(name, "jl_"+key) {
			t.Fatalf("name %q should start with jl_<jobkey>", name)
		}
	}
	pub, err := ObjectName("publication", key, 0)
	if err != nil || pub != "jl_"+key {
		t.Fatalf("publication name = %q, %v", pub, err)
	}
}

func TestServerIDRange(t *testing.T) {
	inst, err := NewInstanceID()
	if err != nil {
		t.Fatalf("instance id: %v", err)
	}
	if len(inst) != instanceLen {
		t.Fatalf("instance len = %d, want %d", len(inst), instanceLen)
	}
	key := JobKey("pg://u@h/db", "my://u@h/db", "public: a")
	id := ServerID(inst, key)
	if id < serverIDMin || id > serverIDMax {
		t.Fatalf("server id %d out of range [%d,%d]", id, serverIDMin, serverIDMax)
	}
	if again := ServerID(inst, key); again != id {
		t.Fatalf("server id not deterministic: %d != %d", id, again)
	}
	other, _ := NewInstanceID()
	if ServerID(other, key) == id {
		t.Log("warning: two random instances produced the same server id (unlikely, but possible)")
	}
}
