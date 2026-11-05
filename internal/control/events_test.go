package control

import (
	"sync"
	"testing"
	"time"
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

// C-5：不变量——对订阅者的发送必须非阻塞。
//
// 取消掉的、断线但 TCP 还没断的、只是不读的客户端，都不能让 Publish 停下来。
// 这个测试订阅之后**一个事件都不读**，然后连发 200 条：缓冲满了以后旧实现里的
// 裸 `ch <- ev` 会永久卡住（<-ch 没人收），而正确实现靠 default 丢弃、立刻返回。
// 环缓冲才是补档的真相。
func TestPublishNeverBlocksOnSlowSubscriber(t *testing.T) {
	bus := NewBus(nil)
	_, cancel := bus.Subscribe("job-1") // 故意不读，也不取消
	defer cancel()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 200; i++ {
			bus.Publish(Event{JobID: "job-1", Type: "progress"})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish 被不读事件的订阅者卡住了：发送必须是非阻塞的（select/default）")
	}
}

// P2 回归：环形缓冲满 200 条后，早于缓冲起点的断档必须被承认（epochOK=false
// → 走 reset），不能回放后半段却假装没漏。
func TestReplayDetectsRingGap(t *testing.T) {
	bus := NewBus(nil)
	bus.SetEpoch("e1")
	for i := 0; i < 250; i++ {
		bus.Publish(Event{JobID: "j", Type: "progress"})
	}
	// 缓冲里现在是 51..250。
	events, ok := bus.Replay("j", "e1", 10)
	if ok {
		t.Fatalf("10 号之后已经丢了 11..50，必须报补不齐（返回了 %d 条）", len(events))
	}

	// 起点在缓冲内：正常回放。
	events, ok = bus.Replay("j", "e1", 200)
	if !ok || len(events) != 50 {
		t.Fatalf("200 之后应该能补 50 条，实际 %d 条 ok=%v", len(events), ok)
	}
	if events[0].Seq != 201 {
		t.Fatalf("回放起点应为 201，实际 %d", events[0].Seq)
	}

	// 连续起点（正好是缓冲第一条的前一条）也算能补齐。
	if _, ok := bus.Replay("j", "e1", 50); !ok {
		t.Fatal("50 之后正好是 51 起，应该能补")
	}
}
