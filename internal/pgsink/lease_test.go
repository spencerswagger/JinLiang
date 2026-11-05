package pgsink

import (
	"testing"
	"time"
)

func TestLeaseLabelRoundTrip(t *testing.T) {
	if got := leaseOwnerLabel("orders", 4242); got != "orders#4242" {
		t.Fatalf("租约标签格式: %q", got)
	}
	if pid, ok := parseLeasePID("orders#4242"); !ok || pid != 4242 {
		t.Fatalf("应能解析出 pid，得到 %d %v", pid, ok)
	}
	// 老记录没有 pid：不应崩，也不应误判
	for _, s := range []string{"", "orders", "orders#", "orders#abc", "orders#-1"} {
		if _, ok := parseLeasePID(s); ok {
			t.Fatalf("%q 不该被解析出 pid", s)
		}
	}
}

// Owl 会盯着这一条问：kill -9 之后重启，会不会被自己上一次的租约挡住？
func TestLeaseTakeover(t *testing.T) {
	now := time.Now()
	alive := func(pid int) bool { return pid == 999 }
	dead := func(pid int) bool { return false }
	future := now.Add(30 * time.Second)
	past := now.Add(-time.Second)

	cases := []struct {
		name     string
		cur      leaseRow
		exists   bool
		instance string
		pidAlive func(int) bool
		want     bool
	}{
		{"没有租约", leaseRow{}, false, "me", dead, true},
		{"别人持有且未过期", leaseRow{"other", "job#1", future}, true, "me", dead, false},
		{"别人持有但已过期", leaseRow{"other", "job#1", past}, true, "me", dead, true},
		{"本机崩溃进程（pid 已死）留的租约：立刻接管", leaseRow{"me", "job#777", future}, true, "me", dead, true},
		{"本机还有进程在跑（pid 活着）：拒绝", leaseRow{"me", "job#999", future}, true, "me", alive, false},
		{"自己的租约（同 pid）：续租", leaseRow{"me", "job#123", future}, true, "me", dead, true},
		{"没有 pid 的老记录：允许接管，不永久挡住", leaseRow{"me", "job", future}, true, "me", dead, true},
	}
	for _, c := range cases {
		ok, why := leaseTakeover(c.cur, c.exists, now, c.instance, 123, c.pidAlive)
		if ok != c.want {
			t.Fatalf("%s：期望 %v，得到 %v（%s）", c.name, c.want, ok, why)
		}
		if !ok && why == "" {
			t.Fatalf("%s：拒绝时必须说明是谁在跑", c.name)
		}
	}
}
