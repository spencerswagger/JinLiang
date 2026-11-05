package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/pgsource"
	"github.com/spencerswagger/jinliang/internal/source"
	"github.com/spencerswagger/jinliang/internal/state"
)

// fakeSink records what was committed; failNext makes Apply fail once.
type fakeSink struct {
	batches  [][]source.Event
	pos      []string
	failNext error
	calls    int
}

func (f *fakeSink) Apply(ctx context.Context, evs []source.Event, pos source.Position) error {
	if f.failNext != nil {
		err := f.failNext
		f.failNext = nil
		return err
	}
	f.calls++
	cp := make([]source.Event, len(evs))
	copy(cp, evs)
	f.batches = append(f.batches, cp)
	if pos != nil {
		f.pos = append(f.pos, pos.String())
	} else {
		f.pos = append(f.pos, "")
	}
	return nil
}

type fakeSource struct{ acked []string }

func (f *fakeSource) Ack(pos pgsource.Position) { f.acked = append(f.acked, pos.String()) }

func ev(kind source.EventKind, pos string) source.Event {
	e := source.Event{Kind: kind}
	if pos != "" {
		e.Position = pos
	}
	return e
}

func newTestBatcher(sink applier, src acker, rows int) *batcher {
	return newBatcher(sink, src, rows, time.Millisecond, nil)
}

// 一个源事务必须整体提交：不足 batch_rows 时也不许拆开。
func TestBatcherKeepsSourceTransactionAtomic(t *testing.T) {
	sink := &fakeSink{}
	src := &fakeSource{}
	b := newTestBatcher(sink, src, 100)

	_ = b.Add(context.Background(), ev(source.EvBegin, ""))
	_ = b.Add(context.Background(), ev(source.EvInsert, ""))
	_ = b.Add(context.Background(), ev(source.EvInsert, ""))
	_ = b.Add(context.Background(), ev(source.EvCommit, "0/100"))
	if len(sink.batches) != 0 {
		t.Fatalf("没攒够 batch_rows 就提交了：%d 批", len(sink.batches))
	}
	if err := b.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.batches) != 1 || len(sink.batches[0]) != 2 {
		t.Fatalf("期望 1 批 2 个事件，得到 %d 批 %v", len(sink.batches), sink.batches)
	}
	if sink.pos[0] != "0/100" {
		t.Fatalf("位点应该是这个事务的提交位点，得到 %s", sink.pos[0])
	}
}

// 多个源事务可以合并成一批，位点取最后一个事务的提交位点。
func TestBatcherGroupCommitUsesLastPosition(t *testing.T) {
	sink := &fakeSink{}
	src := &fakeSource{}
	b := newTestBatcher(sink, src, 100)

	for _, tx := range []struct {
		rows int
		pos  string
	}{{2, "0/100"}, {2, "0/200"}} {
		_ = b.Add(context.Background(), ev(source.EvBegin, ""))
		for i := 0; i < tx.rows; i++ {
			_ = b.Add(context.Background(), ev(source.EvInsert, ""))
		}
		_ = b.Add(context.Background(), ev(source.EvCommit, tx.pos))
	}
	if err := b.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.batches) != 1 || len(sink.batches[0]) != 4 {
		t.Fatalf("两个源事务应合并成 1 批 4 行，得到 %d 批", len(sink.batches))
	}
	if sink.pos[0] != "0/200" {
		t.Fatalf("合并批的位点必须是最后提交的那个，得到 %s", sink.pos[0])
	}
	if got := src.acked[len(src.acked)-1]; got != "0/200" {
		t.Fatalf("源端确认的位点应为 0/200，得到 %s", got)
	}
}

// 单个大事务超过 batch_rows 时立刻提交，不拆事务。
func TestBatcherFlushesOversizedTransactionImmediately(t *testing.T) {
	sink := &fakeSink{}
	b := newTestBatcher(sink, &fakeSource{}, 3)

	_ = b.Add(context.Background(), ev(source.EvBegin, ""))
	for i := 0; i < 5; i++ {
		_ = b.Add(context.Background(), ev(source.EvInsert, ""))
	}
	_ = b.Add(context.Background(), ev(source.EvCommit, "0/300"))

	if len(sink.batches) != 1 {
		t.Fatalf("大事务应在 COMMIT 时立刻提交，得到 %d 批", len(sink.batches))
	}
	if len(sink.batches[0]) != 5 {
		t.Fatalf("事务被拆开了：批里只有 %d 个事件", len(sink.batches[0]))
	}
}

// 提交失败时位点不许前移，也不许确认给源端（否则就是丢数据）。
func TestBatcherDoesNotAckWhenApplyFails(t *testing.T) {
	sink := &fakeSink{failNext: errors.New("目标库断开")}
	src := &fakeSource{}
	b := newTestBatcher(sink, src, 100)

	_ = b.Add(context.Background(), ev(source.EvBegin, ""))
	_ = b.Add(context.Background(), ev(source.EvInsert, ""))
	_ = b.Add(context.Background(), ev(source.EvCommit, "0/400"))
	if err := b.Flush(context.Background()); err == nil {
		t.Fatal("Apply 失败时 Flush 必须返回错误")
	}
	if len(src.acked) != 0 {
		t.Fatalf("提交失败还确认了位点：%v", src.acked)
	}
	// 重试成功后位点才前进
	if err := b.Flush(context.Background()); err != nil {
		t.Fatalf("重试应该成功: %v", err)
	}
	if len(src.acked) != 1 || src.acked[0] != "0/400" {
		t.Fatalf("重试成功后应确认 0/400，得到 %v", src.acked)
	}
}

// 事务外收到数据事件 = 解码错乱，必须停下而不是猜。
func TestBatcherRejectsEventOutsideTransaction(t *testing.T) {
	b := newTestBatcher(&fakeSink{}, &fakeSource{}, 100)
	if err := b.Add(context.Background(), ev(source.EvInsert, "")); err == nil {
		t.Fatal("事务外的 Insert 应该报错")
	}
}

func TestBatcherEmptyTransactionKeepsPosition(t *testing.T) {
	sink := &fakeSink{}
	src := &fakeSource{}
	b := newTestBatcher(sink, src, 100)
	_ = b.Add(context.Background(), ev(source.EvBegin, ""))
	_ = b.Add(context.Background(), ev(source.EvCommit, "0/500"))
	if err := b.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.batches) != 1 || len(sink.batches[0]) != 0 {
		t.Fatalf("空事务也要推进位点：得到 %d 批", len(sink.batches))
	}
	if sink.pos[0] != "0/500" {
		t.Fatalf("空事务提交后位点应为 0/500，得到 %s", sink.pos[0])
	}
}

// 阶段机：引擎驱动的顺序必须是合法迁移，且我们特意绕开的
// Prepare→Catchup 确实是非法迁移（否则 stream() 里的分支就没必要）。
func TestEnginePhaseOrderIsLegal(t *testing.T) {
	path := []state.Phase{state.Preflight, state.Prepare, state.Snapshot,
		state.Catchup, state.Streaming, state.Draining, state.Done}
	for i := 1; i < len(path); i++ {
		if !state.CanTransition(path[i-1], path[i]) {
			t.Fatalf("阶段 %s → %s 被判为非法，但引擎就是这么走的", path[i-1], path[i])
		}
	}
	if state.CanTransition(state.Prepare, state.Catchup) {
		t.Fatal("Prepare → Catchup 不应该合法：续跑要经过 Streaming")
	}
	if !state.CanTransition(state.Prepare, state.Streaming) {
		t.Fatal("Prepare → Streaming 必须合法：续跑直接进增量")
	}
	if !state.CanTransition(state.Snapshot, state.Paused) {
		t.Fatal("快照必须能暂停/被打断")
	}
}

func TestInitialCopyDone(t *testing.T) {
	cases := []struct {
		phase, pos string
		want       bool
	}{
		{"", "", false},
		{"prepare", "", false},
		{"snapshot", "0/100", false}, // 块写入了位点，但存量没搬完
		{"catchup", "", false},       // 没位点等于没搬
		{"catchup", "0/100", true},
		{"streaming", "0/100", true},
		{"draining", "0/100", true},
		{"done", "0/100", true},
		{"paused", "0/100", true},
		{"failed", "0/100", false},
	}
	for _, c := range cases {
		if got := initialCopyDone(c.phase, c.pos); got != c.want {
			t.Fatalf("initialCopyDone(%q,%q)=%v，期望 %v", c.phase, c.pos, got, c.want)
		}
	}
}

func TestModeAndDefaultsComeFromJobDoc(t *testing.T) {
	r := &runner{}
	if got := r.mode(); got != "continuous+initial" {
		t.Fatalf("默认模式应是 continuous+initial，得到 %s", got)
	}
	if got := r.batchRows(); got != 500 {
		t.Fatalf("默认 batch_rows 应为 500，得到 %d", got)
	}
	if got := r.chunkRows(); got != 100000 {
		t.Fatalf("默认 chunk_rows 应为 100000，得到 %d", got)
	}
}

func TestTableSelectionByPattern(t *testing.T) {
	if !included("orders", nil) || !included("orders", []string{"ord*"}) {
		t.Fatal("include 匹配失败")
	}
	if included("orders_archive", []string{"orders"}) {
		t.Fatal("精确 pattern 不该匹配别的表")
	}
	if !excluded("orders_archive", []string{"*_archive"}) {
		t.Fatal("exclude 匹配失败")
	}
}

// 位点写入是引擎里唯一“不可省”的一步：这里守住 guard，防止有人以后把
// Apply 改成不带位点。
func TestApplyAlwaysCarriesPosition(t *testing.T) {
	sink := &fakeSink{}
	b := newTestBatcher(sink, &fakeSource{}, 1)
	_ = b.Add(context.Background(), ev(source.EvBegin, ""))
	_ = b.Add(context.Background(), ev(source.EvInsert, ""))
	_ = b.Add(context.Background(), ev(source.EvCommit, "0/900"))
	if err := b.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sink.pos, []string{"0/900"}) {
		t.Fatalf("每批都必须带位点，得到 %v", sink.pos)
	}
}
