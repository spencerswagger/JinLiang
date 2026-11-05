package control

import (
	"sync"
	"testing"
)

// P0-1 回归：Publish 在过去是「锁外往快照 channel 发」，cancel() 会在摘除后
// close(ch)。两者的交错就是 panic: send on closed channel，而调用方是
// go r.loop(...) 这个没有 recover 的 goroutine —— 用户关一次任务页就可能把
// 整个控制台进程带走。这个测试在 -race 下反复 subscribe/cancel + publish，
// 修复前会稳定 panic（或 race detector 报错），修复后必须安静通过。
func TestPublishAndCancelNeverRace(t *testing.T) {
	bus := NewBus(nil)
	const publishers = 4
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < publishers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					bus.Publish(Event{JobID: "job-1", Type: "phase"})
				}
			}
		}()
	}

	// 消费者：订阅后立刻取消，模拟用户关页面/CLI 断线。
	for i := 0; i < 500; i++ {
		ch, cancel := bus.Subscribe("job-1")
		// 顺手读一下，确保通道真的被用过（不是只看不读）
		select {
		case <-ch:
		default:
		}
		cancel()
	}
	close(stop)
	wg.Wait()

	// 取消之后容器必须干净：没有任何残留订阅者。
	bus.mu.Lock()
	n := len(bus.subs["job-1"])
	bus.mu.Unlock()
	if n != 0 {
		t.Fatalf("cancel 之后仍有 %d 个订阅者残留", n)
	}
}

// 同一条事件流上，取消两次不能 panic（once 语义）。
func TestSubscribeCancelIsIdempotent(t *testing.T) {
	bus := NewBus(nil)
	_, cancel := bus.Subscribe("job-1")
	cancel()
	cancel() // 第二次必须是空操作
	bus.Publish(Event{JobID: "job-1", Type: "note"})
}

// SSE 的 id 是 "<epoch>-<seq>"：重启后 seq 从头来，必须靠 epoch 区分。
func TestSSEIDRoundTrip(t *testing.T) {
	bus := NewBus(nil)
	bus.SetEpoch("abc123")
	ev := bus.Publish(Event{JobID: "j", Type: "phase"})

	epoch, seq, ok := ParseSSEID(ev.SSEID())
	if !ok || epoch != "abc123" || seq != ev.Seq {
		t.Fatalf("ParseSSEID(%q) = %q,%d,%v", ev.SSEID(), epoch, seq, ok)
	}
	if _, _, ok := ParseSSEID("not-a-number"); ok {
		t.Fatal("垃圾输入必须解析失败")
	}
	// 兼容老客户端：纯数字 id 当成「无 epoch」。
	if epoch, seq, ok := ParseSSEID("42"); !ok || epoch != "" || seq != 42 {
		t.Fatalf("legacy id 解析错: %q %d %v", epoch, seq, ok)
	}
}

// 不同 epoch 的回放绝不能把两个生命周期的 seq 混在一起。
func TestReplayRefusesForeignEpoch(t *testing.T) {
	bus := NewBus(nil)
	bus.SetEpoch("first")
	bus.Publish(Event{JobID: "j", Type: "phase"})

	if _, ok := bus.Replay("j", "another-process", 0); ok {
		t.Fatal("别的 epoch 必须被拒绝（否则序号会错位）")
	}
	events, ok := bus.Replay("j", "first", 0)
	if !ok || len(events) != 1 {
		t.Fatalf("同 epoch 回放 = %d 条, ok=%v", len(events), ok)
	}
}
