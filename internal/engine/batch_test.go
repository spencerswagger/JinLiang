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

// fakeSource 记住确认过的位点，并扮演「源端最后提交位点」这一角色：
// 位点不从 Event 上取（共享契约里 Event 没有位点字段），而是 Flush 时向源端要。
type fakeSource struct {
	acked []string
	last  pgsource.Position
}

func (f *fakeSource) Ack(pos pgsource.Position) { f.acked = append(f.acked, pos.String()) }

func (f *fakeSource) LastCommit() pgsource.Position { return f.last }

// commitEvent 模拟「源端提交了某个事务」，返回对应的 Commit 事件。
func commitEvent(src *fakeSource, pos string) source.Event {
	p, _ := pgsource.ParsePayload(pos)
	src.last = p
	return source.Event{Kind: source.EvCommit}
}

func ev(kind source.EventKind, _ string) source.Event { return source.Event{Kind: kind} }

func newTestBatcher(sink applier, src acker, rows int) *batcher {
	return newBatcher(sink, src, rows, time.Millisecond, pgsource.Position{}, nil)
}

// 一个源事务必须整体提交：不足 batch_rows 时也不许拆开。
func TestBatcherKeepsSourceTransactionAtomic(t *testing.T) {
	sink := &fakeSink{}
	src := &fakeSource{}
	b := newTestBatcher(sink, src, 100)

	_ = b.Add(context.Background(), ev(source.EvBegin, ""))
	_ = b.Add(context.Background(), ev(source.EvInsert, ""))
	_ = b.Add(context.Background(), ev(source.EvInsert, ""))
	_ = b.Add(context.Background(), commitEvent(src, "0/100"))
	if len(sink.batches) != 0 {
		t.Fatalf("没攒够 batch_rows 就提交了：%d 批", len(sink.batches))
	}
	if err := b.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 批里带事务边界（Begin + 2 条数据 + Commit）：sink 靠 EvCommit 判定「事务已提交」。
	if len(sink.batches) != 1 || len(sink.batches[0]) != 4 {
		t.Fatalf("期望 1 批 4 个事件（Begin+2 数据+Commit），得到 %d 批 %d 个事件", len(sink.batches), len(sink.batches[0]))
	}
	if sink.batches[0][0].Kind != source.EvBegin || sink.batches[0][3].Kind != source.EvCommit {
		t.Fatal("批次必须保留事务边界：首个是 EvBegin、末尾是 EvCommit")
	}
	if sink.pos[0] != "postgres:v1:0/100" {
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
		_ = b.Add(context.Background(), commitEvent(src, tx.pos))
	}
	if err := b.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.batches) != 1 || len(sink.batches[0]) != 8 {
		t.Fatalf("两个源事务应合并成 1 批 8 个事件（2×(Begin+2 数据+Commit)），得到 %d 批 %d 个事件",
			len(sink.batches), len(sink.batches[0]))
	}
	if sink.pos[0] != "postgres:v1:0/200" {
		t.Fatalf("合并批的位点必须是最后提交的那个，得到 %s", sink.pos[0])
	}
	if got := src.acked[len(src.acked)-1]; got != "postgres:v1:0/200" {
		t.Fatalf("源端确认的位点应为 0/200，得到 %s", got)
	}
}

// 单个大事务超过 batch_rows 时立刻提交，不拆事务。
func TestBatcherFlushesOversizedTransactionImmediately(t *testing.T) {
	sink := &fakeSink{}
	src := &fakeSource{}
	b := newTestBatcher(sink, src, 3)

	_ = b.Add(context.Background(), ev(source.EvBegin, ""))
	for i := 0; i < 5; i++ {
		_ = b.Add(context.Background(), ev(source.EvInsert, ""))
	}
	_ = b.Add(context.Background(), commitEvent(src, "0/300"))

	if len(sink.batches) != 1 {
		t.Fatalf("大事务应在 COMMIT 时立刻提交，得到 %d 批", len(sink.batches))
	}
	if len(sink.batches[0]) != 7 {
		t.Fatalf("事务被拆开了：批里只有 %d 个事件（应为 Begin+5 数据+Commit）", len(sink.batches[0]))
	}
}

// 提交失败时位点不许前移，也不许确认给源端（否则就是丢数据）。
func TestBatcherDoesNotAckWhenApplyFails(t *testing.T) {
	sink := &fakeSink{failNext: errors.New("目标库断开")}
	src := &fakeSource{}
	b := newTestBatcher(sink, src, 100)

	_ = b.Add(context.Background(), ev(source.EvBegin, ""))
	_ = b.Add(context.Background(), ev(source.EvInsert, ""))
	_ = b.Add(context.Background(), commitEvent(src, "0/400"))
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
	if len(src.acked) != 1 || src.acked[0] != "postgres:v1:0/400" {
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
	_ = b.Add(context.Background(), commitEvent(src, "0/500"))
	if err := b.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sink.batches) != 1 || len(sink.batches[0]) != 2 {
		t.Fatalf("空事务也要推进位点（Begin+Commit 两个事件）：得到 %d 批，%d 个事件",
			len(sink.batches), len(sink.batches[0]))
	}
	if sink.pos[0] != "postgres:v1:0/500" {
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
	src := &fakeSource{}
	b := newTestBatcher(sink, src, 1)
	_ = b.Add(context.Background(), ev(source.EvBegin, ""))
	_ = b.Add(context.Background(), ev(source.EvInsert, ""))
	_ = b.Add(context.Background(), commitEvent(src, "0/900"))
	if err := b.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(sink.pos, []string{"postgres:v1:0/900"}) {
		t.Fatalf("每批都必须带位点，得到 %v", sink.pos)
	}
}

// 安静期推位点：只能在「没有在飞的事务、没有待提交批」时进行，
// 否则会把一个还没提交完的事务跳过（丢数据）。
func TestAdvanceToRefusesWhileTransactionInFlight(t *testing.T) {
	sink := &fakeSink{}
	src := &fakeSource{}
	b := newTestBatcher(sink, src, 100)
	_ = b.Add(context.Background(), ev(source.EvBegin, ""))
	_ = b.Add(ctx(), ev(source.EvInsert, ""))

	pos, _ := pgsource.ParsePayload("0/700")
	if err := b.AdvanceTo(ctx(), pos); err != nil {
		t.Fatal(err)
	}
	if len(sink.batches) != 0 || len(src.acked) != 0 {
		t.Fatalf("事务没提交完就推了位点：batches=%d acked=%v", len(sink.batches), src.acked)
	}

	// 提交之后仍然有「待落库的批」，这时也不许绕过数据去推位点。
	_ = b.Add(ctx(), commitEvent(src, "0/700"))
	if err := b.AdvanceTo(ctx(), pos); err != nil {
		t.Fatal(err)
	}
	if len(src.acked) != 0 {
		t.Fatalf("有待落库的批时不该推位点，得到 %v", src.acked)
	}
	// 落库（数据 + 位点同事务）之后才确认。
	if err := b.Flush(ctx()); err != nil {
		t.Fatal(err)
	}
	if len(src.acked) != 1 || src.acked[0] != "postgres:v1:0/700" {
		t.Fatalf("落库后应该确认位点，得到 %v", src.acked)
	}
}

func TestAdvanceToNeverGoesBackwards(t *testing.T) {
	sink := &fakeSink{}
	src := &fakeSource{}
	b := newTestBatcher(sink, src, 100)
	start, _ := pgsource.ParsePayload("0/900")
	b.lastPos = start
	older, _ := pgsource.ParsePayload("0/800")
	if err := b.AdvanceTo(ctx(), older); err != nil {
		t.Fatal(err)
	}
	if len(src.acked) != 0 {
		t.Fatalf("位点不该倒退，却确认了 %v", src.acked)
	}
}

func ctx() context.Context { return context.Background() }

// 引擎产出的每一批都必须过 source.ValidateBatch——这条守住「事务要么整体可见、
// 要么不出现」：批里不会出现半个事务，Commit 之后的裸数据事件也一定先有 EvBegin。
func TestEveryProducedBatchPassesValidateBatch(t *testing.T) {
	sink := &fakeSink{}
	src := &fakeSource{}
	b := newTestBatcher(sink, src, 2) // 故意用小批次，逼出多次提交

	// 事务 1：2 行（刚好触发按 batch_rows 提交）
	_ = b.Add(ctx(), ev(source.EvBegin, ""))
	src.last, _ = pgsource.ParsePayload("0/100")
	_ = b.Add(ctx(), ev(source.EvInsert, ""))
	_ = b.Add(ctx(), ev(source.EvInsert, ""))
	_ = b.Add(ctx(), commitEvent(src, "0/100"))

	// 事务 2：1 行（留在 pending 里，靠 Flush 提交）
	_ = b.Add(ctx(), ev(source.EvBegin, ""))
	_ = b.Add(ctx(), ev(source.EvUpdate, ""))
	_ = b.Add(ctx(), commitEvent(src, "0/200"))

	// 事务 3：空事务（只推进位点）
	_ = b.Add(ctx(), ev(source.EvBegin, ""))
	_ = b.Add(ctx(), commitEvent(src, "0/300"))

	if err := b.Flush(ctx()); err != nil {
		t.Fatal(err)
	}
	if len(sink.batches) < 2 {
		t.Fatalf("应该至少产生 2 批，得到 %d", len(sink.batches))
	}
	for i, batch := range sink.batches {
		if err := source.ValidateBatch(batch); err != nil {
			t.Fatalf("第 %d 批没过 ValidateBatch: %v（批=%v）", i, err, kinds(batch))
		}
	}
}

func kinds(evs []source.Event) []source.EventKind {
	out := make([]source.EventKind, len(evs))
	for i, e := range evs {
		out[i] = e.Kind
	}
	return out
}
