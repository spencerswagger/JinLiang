package engine

// `flushLoop` 在 goroutine 里跑，它的错误没有任何返回值可走。
// 只在里面 return 等于把错误扔掉 —— **写失败而 run 报成功**是最坏的一种「看起来成功」。
// 这一格把「错误必须能交给调用方」钉死：删掉记录（或 FlushErr）就必须红。

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/pgsource"
	"github.com/spencerswagger/jinliang/internal/source"
)

// failingSink 的 Apply 必失败（模拟目标端权限被收回/约束冲突）。
type failingSink struct {
	calls int
	err   error
}

func (f *failingSink) Apply(ctx context.Context, evs []source.Event, pos source.Position) error {
	f.calls++
	return f.err
}

// quietAcker 什么都不做：这一格只关心 flush 错误有没有被带出来。
type quietAcker struct{}

func (quietAcker) Ack(source.Position)              {}
func (quietAcker) LastCommit() source.Position      { return nil }
func (quietAcker) Before(a, b source.Position) bool { return false }
func (quietAcker) IsZero(p source.Position) bool    { return true }

func TestFlushLoopErrorReachesCaller(t *testing.T) {
	want := errors.New("目标端拒绝了这一批（示例：权限被收回）")
	sink := &failingSink{err: want}
	// batchRows 放大：让这一批**不由 Add 顺手刷出去**，而是由后台 flushLoop 的定时器刷，
	// 否则测的还是 Add 的同步返回，而不是「goroutine 里的失败有没有被带出来」。
	b := newBatcher(sink, quietAcker{}, control.JobDoc{}, 1000, time.Millisecond, pgsource.Position{}, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.flushLoop(ctx)

	// 造一个「攒够一批」的源事务
	if err := b.Add(ctx, source.Event{Kind: source.EvBegin, TxID: "t1"}); err != nil {
		t.Fatal(err)
	}
	if err := b.Add(ctx, source.Event{Kind: source.EvInsert, TxID: "t1",
		Table: source.TableRef{Schema: "public", Table: "t"},
		New:   source.Row{Values: []source.Value{{Kind: source.KindString, S: "1"}}}}); err != nil {
		t.Fatal(err)
	}
	if err := b.Add(ctx, source.Event{Kind: source.EvCommit, TxID: "t1"}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := b.FlushErr(); err != nil {
			if !errors.Is(err, want) {
				t.Fatalf("要保留原始错误链，得到 %v", err)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("后台 flush 的失败没有被交给调用方（写入失败会看起来成功）")
}
