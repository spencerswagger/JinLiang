package engine

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spencerswagger/jinliang/internal/pgsource"
	"github.com/spencerswagger/jinliang/internal/source"
)

// batcher assembles source events into target transactions.
//
// 两条规矩，写在类型上而不是文档里：
//  1. **一个源事务要么整体提交要么整体不提交** —— 事件按 BEGIN/COMMIT 边界缓冲，
//     批里绝不会出现半个源事务。
//  2. **位点与数据同事务** —— 一批里带的位点是「最后一个被包含的源事务的提交位点」，
//     由 pgsink.Apply 和数据写进同一个事务（docs/STATE-SCHEMA.md §2）。
//
// 提交时机：攒够 batch_rows，或距上次提交超过 batch_delay（先到者触发）。
// 目标端慢的时候批会自然变大，快的时候批会变小 —— 这就是 group commit。
// 单事务缓冲上限：与 docs/STREAMING.md §1 一致（128 MB / 100 万事件），
// 超了报 E1011 而不是硬撑（spill 未实现）。
const (
	defaultMaxTxBytes  = int64(128 << 20)
	defaultMaxTxEvents = 1_000_000
)

// txLimits reads the two knobs Data 的 #11 也认的同一组环境变量，
// 保证两个连接器的口径一致（`reliability.tx_oversize` 的数值形状还没定）。
func txLimits() (bytes int64, events int) {
	bytes, events = defaultMaxTxBytes, defaultMaxTxEvents
	if v := strings.TrimSpace(os.Getenv("JINLIANG_TX_MAX_BYTES")); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			bytes = n
		}
	}
	if v := strings.TrimSpace(os.Getenv("JINLIANG_TX_MAX_EVENTS")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			events = n
		}
	}
	return bytes, events
}

// endsWithCommit reports whether the batch closes on a committed transaction.
func endsWithCommit(evs []source.Event) bool {
	return len(evs) > 0 && evs[len(evs)-1].Kind == source.EvCommit
}

// applier / acker are the two things the batcher needs. They are interfaces so
// the transaction-assembly rules can be unit tested without a database.
type applier interface {
	Apply(ctx context.Context, evs []source.Event, pos source.Position) error
}

type acker interface {
	Ack(pos pgsource.Position)
	// LastCommit is the LSN of the last committed source transaction; the
	// batcher snapshots it on EvCommit (the shared Event struct carries no
	// position field — that contract belongs to Ark).
	LastCommit() pgsource.Position
}

type batcher struct {
	sink      applier
	src       acker
	batchRows int
	delay     time.Duration

	mu       sync.Mutex
	cur      []source.Event // 当前源事务（还没 COMMIT），首元素是 EvBegin
	txRows   int
	txBytes  int64
	inTx     bool
	pending  []source.Event // 已提交、还没落库的源事务
	pendRows int
	pendPos  pgsource.Position

	applied   int64
	lastPos   pgsource.Position
	lastFlush time.Time

	// 单事务缓冲上限（docs/STREAMING.md §1：128 MB / 100 万事件）。
	maxTxBytes  int64
	maxTxEvents int

	// onFlush fires after a successful commit (used for phase switching).
	onFlush func(applied int64, pos pgsource.Position)
	// onApplied reports rows/position to the runner.
	onApplied func(rows int64, pos pgsource.Position)
}

func newBatcher(sink applier, src acker, batchRows int, delay time.Duration,
	start pgsource.Position, onApplied func(rows int64, pos pgsource.Position)) *batcher {
	if batchRows < 1 {
		batchRows = 1
	}
	if delay <= 0 {
		delay = time.Millisecond
	}
	txBytes, txEvents := txLimits()
	return &batcher{sink: sink, src: src, batchRows: batchRows, delay: delay,
		lastPos: start, pendPos: start, onApplied: onApplied, lastFlush: time.Now(),
		maxTxBytes: txBytes, maxTxEvents: txEvents}
}

// AdvanceTo pushes the checkpoint forward without any data, for the case where
// the source is quiet: the stream saw nothing to apply, but the bookmark must
// still move or it holds WAL forever. It refuses while a transaction is in
// flight — the position inside an unfinished transaction is not a checkpoint.
func (b *batcher) AdvanceTo(ctx context.Context, pos pgsource.Position) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.inTx || len(b.cur) > 0 || len(b.pending) > 0 {
		return nil
	}
	if pos.Compare(b.lastPos) <= 0 {
		return nil
	}
	if err := b.sink.Apply(ctx, nil, pos); err != nil {
		return err
	}
	b.src.Ack(pos)
	b.lastPos = pos
	b.pendPos = pos
	b.lastFlush = time.Now()
	return nil
}

// Add takes one source event. It never blocks on the sink for long: it only
// commits when a batch is complete (docs/STREAMING.md §2).
func (b *batcher) Add(ctx context.Context, ev source.Event) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch ev.Kind {
	case source.EvBegin:
		b.inTx = true
		b.cur = b.cur[:0]
		b.cur = append(b.cur, ev) // 事务边界要传给 sink：EvCommit 是唯一提交信号
		return nil
	case source.EvCommit:
		if b.inTx && len(b.cur) > 0 {
			b.cur = append(b.cur, ev)
			b.pending = append(b.pending, b.cur...)
			b.pendRows += b.txRows
		}
		b.cur = b.cur[:0]
		b.txRows = 0
		b.inTx = false
		if b.src != nil {
			if p := b.src.LastCommit(); p.Compare(b.pendPos) > 0 {
				b.pendPos = p
			}
		}
		// 一个大事务自己就超过 batch_rows 时，立刻提交：不让内存无限长。
		if b.pendRows >= b.batchRows {
			return b.flushLocked(ctx)
		}
		return nil
	case source.EvInsert, source.EvUpdate, source.EvDelete, source.EvTruncate:
		if !b.inTx {
			// pgoutput 不会在事务外发数据；真出现就是解码错乱，宁可停下。
			return fmt.Errorf("收到事务外的事件（kind=%d）：这是解码器的问题", ev.Kind)
		}
		b.cur = append(b.cur, ev)
		b.txRows++
		b.txBytes += estimateSize(ev)
		// 单事务过大：报 E1011 停下，不硬撑（撑下去是 OOM，进程直接没了看不出原因）。
		if b.txBytes > b.maxTxBytes || b.txRows > b.maxTxEvents {
			return &txTooLargeError{rows: int64(b.txRows), bytes: b.txBytes}
		}
		return nil
	}
	return nil
}

// estimateSize 粗估一个事件占多少内存：够用来在「一个 DELETE 5000 万行」这种场景
// 下及时刹车，不需要精确到字节。
func estimateSize(ev source.Event) int64 {
	const perCell = 48 // interface header + 时间/整数的近似
	n := int64(0)
	for _, v := range ev.New.Values {
		n += perCell + int64(len(v.S)+len(v.B))
	}
	for _, v := range ev.Old.Values {
		n += perCell + int64(len(v.S)+len(v.B))
	}
	return n
}

// txTooLargeError is E1011: the source transaction does not fit in the buffer.
type txTooLargeError struct {
	rows  int64
	bytes int64
}

func (e *txTooLargeError) Error() string {
	return fmt.Sprintf("源端有个事务太大：已经缓冲约 %d 行 / %s，超过单事务上限。"+
		"你现在能做什么：1) 把它拆成几个小事务（批量 UPDATE/DELETE 改成分页做）；"+
		"2) 调大上限 JINLIANG_TX_MAX_BYTES / JINLIANG_TX_MAX_EVENTS 后重跑。"+
		"风险：调高会让峰值内存跟着涨，调得比机器内存还大就是 OOM（进程直接消失，看不出原因）；"+
		"spill-to-disk 还没实现，所以现在只能报错停下，不会硬撑",
		e.rows, humanLag(e.bytes))
}

// Flush commits whatever is pending. Safe to call concurrently (the ticker and
// the event path both do).
func (b *batcher) Flush(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.flushLocked(ctx)
}

func (b *batcher) flushLocked(ctx context.Context) error {
	// 空事务也要推进位点：不推进就会让「落后」一直涨，还会让 --stop-after
	// 收尾时的位点落后于实际已消费的位置。
	if len(b.pending) == 0 && b.pendPos.Compare(b.lastPos) <= 0 {
		return nil
	}
	pos := b.pendPos
	rows := int64(b.pendRows)
	if !endsWithCommit(b.pending) {
		// 契约：半个事务的批次必须复用上一个已提交位点，不能推进。
		pos = b.lastPos
	}
	// 数据与位点同事务提交（pgsink.Apply 内部保证）。
	if err := b.sink.Apply(ctx, b.pending, pos); err != nil {
		return err
	}
	// 只有提交成功后才把位点告诉源端：先告诉它会丢数据。
	b.src.Ack(pos)
	b.applied += rows
	b.lastPos = pos
	b.pending = b.pending[:0]
	b.pendRows = 0
	b.lastFlush = time.Now()
	if b.onApplied != nil {
		b.onApplied(rows, pos)
	}
	if b.onFlush != nil {
		b.onFlush(b.applied, pos)
	}
	return nil
}

// flushLoop commits on the batch delay even when the stream is quiet, so lag
// never sits in memory waiting for the next event.
func (b *batcher) flushLoop(ctx context.Context) {
	interval := b.delay
	if interval < 20*time.Millisecond {
		interval = 20 * time.Millisecond
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.mu.Lock()
			due := len(b.pending) > 0 && time.Since(b.lastFlush) >= b.delay
			if !due {
				b.mu.Unlock()
				continue
			}
			err := b.flushLocked(ctx)
			b.mu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

// Pending reports whether anything is waiting to be committed (rows in the
// current source transaction count too: a half transaction is not "idle").
func (b *batcher) Pending() (rows int, inTx bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	rows = b.pendRows + len(b.cur)
	if rows == 0 && b.pendPos.Compare(b.lastPos) > 0 {
		rows = 1 // 有位点没提交：不算 idle
	}
	return rows, b.inTx
}

// Applied reports how many rows this batcher wrote and the last durable position.
func (b *batcher) Applied() (int64, pgsource.Position) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.applied, b.lastPos
}
