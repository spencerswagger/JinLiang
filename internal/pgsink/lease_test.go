package pgsink

import (
	"testing"
	"time"
)

func TestLeaseLabelRoundTrip(t *testing.T) {
	if got := leaseOwnerLabel("orders", 4242, "tok"); got != "orders#4242#tok" {
		t.Fatalf("租约标签格式: %q", got)
	}
	if pid, tok, ok := parseLeaseLabel("orders#4242#tok"); !ok || pid != 4242 || tok != "tok" {
		t.Fatalf("应能解析出 pid 与 token，得到 %d %q %v", pid, tok, ok)
	}
	// 老格式（没有 token）要认得
	if pid, tok, ok := parseLeaseLabel("orders#4242"); !ok || pid != 4242 || tok != "" {
		t.Fatalf("老格式解析失败：%d %q %v", pid, tok, ok)
	}
	// 任务名里带 # 也不能切错
	if pid, tok, ok := parseLeaseLabel("my#job#99#abc"); !ok || pid != 99 || tok != "abc" {
		t.Fatalf("任务名含 # 时解析失败：%d %q %v", pid, tok, ok)
	}
	// 老记录没有 pid：不应崩，也不应误判
	for _, s := range []string{"", "orders", "orders#", "orders#abc", "orders#-1"} {
		if _, _, ok := parseLeaseLabel(s); ok {
			t.Fatalf("%q 不该被解析出 pid", s)
		}
	}
}

// Owl 会盯着这一条问：kill -9 之后重启，会不会被自己上一次的租约挡住？
func TestLeaseTakeover(t *testing.T) {
	now := time.Now()
	// 999 = 另一个活着的进程；123 = 本进程（同 pid 不同 token 的那个用例）
	alive := func(pid int) bool { return pid == 999 || pid == 123 }
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
		{"本机崩溃进程（pid 已死）留的租约：立刻接管", leaseRow{"me", "job#777#tokA", future}, true, "me", dead, true},
		{"本机还有进程在跑（pid 活着）：拒绝", leaseRow{"me", "job#999#tokA", future}, true, "me", alive, false},
		{"自己的租约（同 pid 同 token）：续租", leaseRow{"me", "job#123#tokB", future}, true, "me", dead, true},
		{"同进程里的另一个 Sink（同 pid 不同 token）：拒绝", leaseRow{"me", "job#123#tokA", future}, true, "me", alive, false},
		{"没有 pid 的老记录：允许接管，不永久挡住", leaseRow{"me", "job", future}, true, "me", dead, true},
	}
	for _, c := range cases {
		ok, why := leaseTakeover(c.cur, c.exists, now, c.instance, 123, "tokB", c.pidAlive)
		if ok != c.want {
			t.Fatalf("%s：期望 %v，得到 %v（%s）", c.name, c.want, ok, why)
		}
		if !ok && why == "" {
			t.Fatalf("%s：拒绝时必须说明是谁在跑", c.name)
		}
	}
}
