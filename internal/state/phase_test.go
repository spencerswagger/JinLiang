package state

import "testing"

func TestCanTransition(t *testing.T) {
	legal := [][2]Phase{
		{Idle, Preflight},
		{Preflight, Prepare},
		{Prepare, Snapshot},
		{Prepare, Streaming},
		{Snapshot, Catchup},
		{Catchup, Streaming},
		{Streaming, Paused},
		{Paused, Streaming},
		{Paused, Snapshot},
		{Streaming, Draining},
		{Draining, Done},
		{Done, Idle},
	}
	for _, c := range legal {
		if !CanTransition(c[0], c[1]) {
			t.Errorf("%s -> %s should be legal", c[0], c[1])
		}
	}

	illegal := [][2]Phase{
		{Idle, Streaming},     // 不能跳过预检与准备
		{Snapshot, Streaming}, // 必须先 catchup 追平
		{Done, Streaming},     // 结束的任务要重跑先回 idle
		{Failed, Streaming},
		{Idle, Done},
	}
	for _, c := range illegal {
		if CanTransition(c[0], c[1]) {
			t.Errorf("%s -> %s should be illegal", c[0], c[1])
		}
	}
}

func TestLease(t *testing.T) {
	for _, p := range []Phase{Preflight, Prepare, Snapshot, Catchup, Streaming, Paused, Draining} {
		if !p.HoldsLease() {
			t.Errorf("%s should hold the lease", p)
		}
	}
	for _, p := range []Phase{Idle, Done, Failed} {
		if p.HoldsLease() {
			t.Errorf("%s must not hold the lease", p)
		}
	}
}
