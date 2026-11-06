package engine

import (
	"context"
	"errors"
	"github.com/spencerswagger/jinliang/internal/core"

	"github.com/jackc/pgx/v5/pgconn"

	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/control"
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
	return newBatcher(sink, src, control.JobDoc{}, rows, time.Millisecond, pgsource.Position{}, nil)
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

// 租约被接管/续不上时，必须**真的停下来**：只 emit 一句话然后继续写，
// 等于两个进程同时写同一张目标表（Owl 的必改 2）。
func TestWatchLeaseCancelsRunWhenLeaseStolen(t *testing.T) {
	r := &runner{leaseTTL: time.Minute}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.cancelRun = cancel

	done := make(chan struct{})
	go func() {
		defer close(done)
		r.watchLease(ctx, 5*time.Millisecond, func(context.Context) (bool, error) {
			return false, nil // 被接管
		})
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watchLease 应该在被接管后立刻返回")
	}
	if !r.leaseLost.Load() {
		t.Fatal("丢了租约必须置 leaseLost")
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("丢了租约必须 cancel 整个 run（否则写路径继续跑）")
	}
	if err := r.leaseLostError(); !strings.Contains(err.Error(), "租约丢了") {
		t.Fatalf("错误文案应说清是租约丢了，得到: %v", err)
	}
}

// 一次续租失败不算丢（网络抖一下就停会把任务打断），连续两次才停。
func TestWatchLeaseToleratesSingleRenewFailure(t *testing.T) {
	r := &runner{leaseTTL: time.Minute}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.cancelRun = cancel

	calls := 0
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.watchLease(ctx, 5*time.Millisecond, func(context.Context) (bool, error) {
			calls++
			if calls == 1 {
				return false, errors.New("网络抖了一下")
			}
			return true, nil
		})
	}()
	time.Sleep(80 * time.Millisecond)
	cancel()
	<-done
	if r.leaseLost.Load() {
		t.Fatal("单次失败不该判定丢租约")
	}
	if calls < 2 {
		t.Fatalf("应该继续续租，实际只调了 %d 次", calls)
	}
}

// Owl 必改 4：未提交的源事务 `cur` 必须有上限——几十万行的单事务全量驻留内存
// 就是 OOM（进程直接没了，看不出原因）。宁可报 E1011 停下。
func TestBatcherStopsOversizedTransaction(t *testing.T) {
	sink := &fakeSink{}
	src := &fakeSource{}
	b := newTestBatcher(sink, src, 1_000_000) // 让 batch_rows 永远不触发，单测只看事务上限
	b.maxTxEvents = 3
	b.maxTxBytes = 1 << 30

	_ = b.Add(ctx(), source.Event{Kind: source.EvBegin, TxID: "7"})
	var err error
	for i := 0; i < 10; i++ {
		if err = b.Add(ctx(), source.Event{Kind: source.EvInsert, TxID: "7"}); err != nil {
			break
		}
	}
	if err == nil {
		t.Fatal("单事务超过上限必须报错，不能一直吃内存")
	}
	var tooBig *txTooLargeError
	if !errors.As(err, &tooBig) {
		t.Fatalf("应该是 txTooLargeError（E1011），得到 %T（%v）", err, err)
	}
	// 四段式的基本要素：发生了什么 + 能做什么（两条真路）+ 风险
	msg := err.Error()
	for _, want := range []string{
		"事务太大",
		"拆成几个小事务",
		"JINLIANG_TX_MAX_BYTES",
		"JINLIANG_TX_MAX_EVENTS",
		"风险",
		"峰值内存",
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("文案缺 %q：%s", want, msg)
		}
	}
	// 不许暗示有兜底：spill 在代码里不存在（Ark 的 #14 已把文档改成不再提它）。
	if strings.Contains(strings.ToLower(msg), "spill") {
		t.Fatalf("文案不该提 spill（它没实现，会让人以为有兜底）：%s", msg)
	}
	if len(sink.batches) != 0 || len(src.acked) != 0 {
		t.Fatal("报错之后不许落库、也不许推进位点")
	}
}

// 单事务上限的优先级：yaml 显式值 > 环境变量 > 内置默认（由 control.TxOversizeDoc.Limits 实现）。
// 这里断言的是**接线正确**：引擎真的把任务的 doc 传下去了，而不是自己再读一遍环境变量。
func TestTxLimitsFollowControlPrecedence(t *testing.T) {
	// 只有环境变量（注意 Limits 会钳下限，所以这里用高于下限的值）
	t.Setenv("JINLIANG_TX_MAX_BYTES", "2097152") // 2 MiB
	t.Setenv("JINLIANG_TX_MAX_EVENTS", "5000")
	if b, e := txLimits(control.JobDoc{}); b != 2097152 || e != 5000 {
		t.Fatalf("环境变量没生效：%d %d", b, e)
	}

	// 非法环境变量回落默认（不是 0、不是报错）
	t.Setenv("JINLIANG_TX_MAX_EVENTS", "不是数字")
	if _, e := txLimits(control.JobDoc{}); e != int(control.DefaultTxMaxEvents) {
		t.Fatalf("非法值应回落默认，得到 %d", e)
	}

	// 低于下限会被钳到下限（100 字节 → MinTxMaxBytes）
	t.Setenv("JINLIANG_TX_MAX_BYTES", "100")
	if b, _ := txLimits(control.JobDoc{}); b != control.MinTxMaxBytes {
		t.Fatalf("低于下限应钳到 %d，得到 %d", control.MinTxMaxBytes, b)
	}

	// yaml 显式值优先于环境变量
	t.Setenv("JINLIANG_TX_MAX_BYTES", "9999999")
	n := 4 << 20 // 4 MiB，高于下限，避免被钳
	doc := control.JobDoc{Reliability: &control.ReliabilityDoc{
		TxOversize: &control.TxOversizeDoc{Bytes: &n},
	}}
	if b, _ := txLimits(doc); b != 4<<20 {
		t.Fatalf("yaml 显式值应优先，得到 %d", b)
	}
}
func TestBatcherRejectsContinuationOfCommittedTransaction(t *testing.T) {
	sink := &fakeSink{}
	src := &fakeSource{}
	b := newTestBatcher(sink, src, 100)

	// 事务 42 正常提交
	_ = b.Add(ctx(), source.Event{Kind: source.EvBegin, TxID: "42"})
	_ = b.Add(ctx(), source.Event{Kind: source.EvInsert, TxID: "42"})
	_ = b.Add(ctx(), commitEvent(src, "0/500"))
	if err := b.Flush(ctx()); err != nil {
		t.Fatal(err)
	}

	// 同一事务的后半段又来了，但没有新的 EvBegin
	err := b.Add(ctx(), source.Event{Kind: source.EvInsert, TxID: "42"})
	if err == nil {
		t.Fatal("续段没有 EvBegin 时必须拦住：否则事务只落一半")
	}
	var cont *continuationError
	if !errors.As(err, &cont) {
		t.Fatalf("应该报 continuationError，得到 %T（%v）", err, err)
	}
	if len(sink.batches) != 1 {
		t.Fatalf("拦住之后不该再落库，得到 %d 批", len(sink.batches))
	}
}

// 不同事务的裸数据事件（TxID 对不上上次提交）仍然按「事务外事件」报错，不与续段混淆。
func TestBatcherDistinguishesForeignBareEvent(t *testing.T) {
	b := newTestBatcher(&fakeSink{}, &fakeSource{}, 100)
	_ = b.Add(ctx(), source.Event{Kind: source.EvBegin, TxID: "1"})
	_ = b.Add(ctx(), commitEvent(&fakeSource{}, "0/600"))
	err := b.Add(ctx(), source.Event{Kind: source.EvInsert, TxID: "999"})
	if err == nil {
		t.Fatal("事务外的数据事件必须报错")
	}
	var cont *continuationError
	if errors.As(err, &cont) {
		t.Fatal("TxID 不是上一个已提交事务时，不该报成续段")
	}
}

// 位点 kind 取值域的对账（SplitPosition 不管这个，契约要求引擎补）：
// 以及「kind 与本任务的源不一致」→ E1010。
func TestAssertPositionKind(t *testing.T) {
	pg := pgsource.Position{}
	if err := assertPositionKind(pg, "postgres"); err != nil {
		t.Fatalf("postgres 位点在 postgres 任务上应通过: %v", err)
	}
	if err := assertPositionKind(pg, "mysql"); err == nil {
		t.Fatal("postgres 位点在 mysql 任务上必须报错")
	} else {
		var ke *positionKindError
		if !errors.As(err, &ke) {
			t.Fatalf("应该是 positionKindError（映射 E1010），得到 %T", err)
		}
	}
	// 未知 kind（foo:v1:x 能过 SplitPosition，但过不了这道对账）
	if err := assertPositionKind(fakePosition("foo"), "postgres"); err == nil {
		t.Fatal("未知的 kind 必须被拒")
	}
}

type fakePosition string

func (f fakePosition) String() string { return string(f) + ":v1:x" }
func (f fakePosition) Kind() string   { return string(f) }

// 错误分类：兜底不许再落 E4002（「我们的 bug」），否则所有未分类错误都在指责自己。
func TestClassifyErrorDoesNotDefaultToDecoderBug(t *testing.T) {
	unknown := errors.New("some database error we have not seen")
	code, classified := classifyError(unknown)
	if classified {
		t.Fatalf("未识别的错误不该被分类，得到 %s", code)
	}
	if code == core.CodeDecoderBug {
		t.Fatal("兜底不许是 E4002：那会让用户去给我们提一个不存在的 issue")
	}

	// 运行期撞上 wal_level 不够 → E1001（与预检同一口径）
	low := &pgconn.PgError{Code: "55000", Message: "logical decoding requires wal_level >= logical"}
	if code, ok := classifyError(low); !ok || code != core.CodeWalLevel {
		t.Fatalf("wal_level 不够应映射 E1001，得到 %s %v", code, ok)
	}

	// 位点不连续只认位点专属信号
	gap := &pgconn.PgError{Code: "58P01", Message: "could not open file"}
	if code, ok := classifyError(gap); !ok || code != core.CodePositionGap {
		t.Fatalf("位点不连续应为 E3001，得到 %s %v", code, ok)
	}

	// schema/表不存在 → E1013（**不能**再被判成位点不连续，也不能落 E4002）
	for _, e := range []*pgconn.PgError{
		{Code: "3F000", Message: `schema "report" does not exist`},
		{Code: "42P01", Message: `relation "public.orders" does not exist`},
	} {
		if code, ok := classifyError(e); !ok || code != core.CodeObjectMissing {
			t.Fatalf("%s 应映射 E1013，得到 %s %v", e.Code, code, ok)
		}
	}

	// 权限不够 → E1012
	denied := &pgconn.PgError{Code: "42501", Message: "permission denied for schema public"}
	if code, ok := classifyError(denied); !ok || code != core.CodeNoPrivilege {
		t.Fatalf("42501 应映射 E1012，得到 %s %v", code, ok)
	}

	// 挂码之后这两类不再走 hint()（码表里已有 why/next，避免两套口径）
	why, next := hint(denied)
	if strings.Contains(why, "权限") || strings.Contains(why, "GRANT") {
		t.Fatalf("挂了 E1012 之后 hint 不该再接管权限类，得到 why=%q next=%v", why, next)
	}
}
