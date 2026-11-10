package engine

// 后台提交（flushLoop）失败必须**冒出来**，不许被吞掉。
//
// 踩过的坑（MySQL 目标端 + 目标表没有唯一键）：Apply 在后台 goroutine 里失败，
// flushLoop 只 `return`，于是提交再也不会发生、位点不再前进、`--stop-when-idle`
// 也等不到安静——任务停在「实时同步中」，日志里一个字都没有，只能靠人按停。
// 用户看到的是「同步卡住了」，而不是「目标表没有唯一键，加一个主键再跑」。
//
// 这条用例钉三件事：错误被记住、onError 被调用一次（调用方靠它 cancel 流）、
// 之后 Err() 一直返回同一个根因。

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/source"
)

func TestBatcherSurfacesBackgroundFlushError(t *testing.T) {
	boom := errors.New("目标表没有唯一键，无法做 upsert")
	sink := &fakeSink{failNext: boom}
	src := &fakeSource{}
	b := newTestBatcher(sink, src, 100)
	b.delay = 5 * time.Millisecond

	notified := make(chan error, 4)
	canceled := make(chan struct{}, 1)
	b.onError = func(err error) {
		notified <- err
		select {
		case canceled <- struct{}{}:
		default:
		}
	}

	_ = b.Add(context.Background(), ev(source.EvBegin, ""))
	_ = b.Add(context.Background(), ev(source.EvInsert, ""))
	_ = b.Add(context.Background(), commitEvent(src, "0/200"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		// 模拟调用方：onError 被叫到就 cancel 流。
		select {
		case <-canceled:
			cancel()
		case <-time.After(3 * time.Second):
		}
	}()
	go b.flushLoop(ctx)

	select {
	case err := <-notified:
		if !errors.Is(err, boom) {
			t.Fatalf("onError 拿到的不是真正的根因：%v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("后台提交失败没有通知调用方：任务会停在「实时同步中」，用户看不到任何错误")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && b.Err() == nil {
		time.Sleep(5 * time.Millisecond)
	}
	if err := b.Err(); !errors.Is(err, boom) {
		t.Fatalf("Err() 应返回后台提交的根因，得到 %v", err)
	}
}

// 成功提交时 Err() 必须保持 nil：别把「没有错误」也变成一条错误。
func TestBatcherErrIsNilAfterCleanFlush(t *testing.T) {
	sink := &fakeSink{}
	src := &fakeSource{}
	b := newTestBatcher(sink, src, 100)

	_ = b.Add(context.Background(), ev(source.EvBegin, ""))
	_ = b.Add(context.Background(), ev(source.EvInsert, ""))
	_ = b.Add(context.Background(), commitEvent(src, "0/300"))
	if err := b.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := b.Err(); err != nil {
		t.Fatalf("干净提交不该留下错误：%v", err)
	}
}
