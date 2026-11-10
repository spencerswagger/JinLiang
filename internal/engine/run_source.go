package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/mysqlsource"
	"github.com/spencerswagger/jinliang/internal/pgsource"
	"github.com/spencerswagger/jinliang/internal/source"
)

// 源端抽象：**在构造阶段按连接类型确定**，跑起来之后引擎只认接口。
//
// 为什么要有它：MySQL 源（binlog/GTID）与 PG 源（逻辑复制槽）在读的实现上完全不同，
// 但引擎要的东西只有那几件——全量分块、变更流、位点、能力。把这几件写成一个接口，
// 引擎的阶段机（预检 → 准备 → 全量 → 追赶 → 增量 → 收尾）、位点与租约语义、
// 块进度记账（`_jl_chunk`）就只有一份，不按源类型分叉。
//
// 与 targetSink 同一条纪律：接口成员由「谁在用」决定，不由「谁有」决定。
// 所以这里**没有** SlotConfirmed / CurrentLSN / TablePhysicalSize 之类——
// 那些是某一种源的内部概念，被收在两个适配器里，引擎看不到。
type runSource interface {
	Kind() string
	// ParsePosition 把目标库里的位点串还原成连接器自己的类型（格式归连接器所有）。
	ParsePosition(raw string) (source.Position, error)
	// Before 报告 a 是否严格早于 b。契约只保证「可判等」，引擎需要的顺序由
	// 连接器自己给（PG 的 LSN 序、MySQL 的「文件名数字段 → 偏移」序）。
	Before(a, b source.Position) bool
	// IsZero 报告这个位点是不是「还没定过」。
	IsZero(p source.Position) bool
	// RequiresExportedSnapshot：PG 的全量靠「导出的快照名」跨连接共享一致性视图，
	// MySQL 靠同一个连接上的一致性事务（没有名字）。引擎据此判断
	// 「拿不到快照名」在哪种源上算失败。
	RequiresExportedSnapshot() bool
	// Prepare 做自动修复动作；needSnapshot 表示这一轮要做全量（要一致性视图）。
	Prepare(ctx context.Context, needSnapshot bool) (prepOutcome, error)
	// SnapshotChunksFrom 分块读全量，from 里的表从已提交的块边界接着读。
	SnapshotChunksFrom(ctx context.Context, opt source.SnapshotOpt,
		from map[string]chunkFrom, fn chunkFn) (source.Position, error)
	// EndSnapshot 放掉全量用的读会话（越早放越好：它占着一个连接/一个事务）。
	EndSnapshot()
	Stream(ctx context.Context, from source.Position, fn source.EventHandler) error
	// LastCommit 是「已交付给下游的最后一笔源事务的提交位点」。
	LastCommit() source.Position
	SafeAckPoint() (source.Position, bool)
	Ack(pos source.Position)
	Advance(ctx context.Context, to source.Position) error
	// Now 是源端此刻的位点：增量追到它就算追平。
	Now(ctx context.Context) (source.Position, error)
	// ServerEnd 是服务端自己公布的「已经写到哪」（PG 的心跳位点）。
	// 拿不到就返回零值（MySQL 没有这个概念：binlog 只由源端自己写）。
	ServerEnd() source.Position
	// Backlog 是「还有多少没重放」的字节数；-1 表示不知道（**不返回 0**：
	// 0 会被当成「已经追平」，那是把落后藏起来）。
	Backlog(ctx context.Context, from source.Position) (int64, error)
	// Replay 判断源端还能不能从 pos 重放变更（书签/位点还在不在）。
	Replay(ctx context.Context, pos source.Position) (replayState, error)
	Count(ctx context.Context, ref source.TableRef) (int64, error)
	Fingerprint(ctx context.Context, ref source.TableRef) (string, error)
	// FingerprintScope 是行指纹的口径名。**口径不同就不能直接比**（跨引擎比对
	// 必须走逐行/分块，见 internal/verify）；空串表示「服务端原生指纹」。
	FingerprintScope() string
	Close()
}

// chunkFrom 是一张表接着读的位置（引擎自己的形状，来自目标端 _jl_chunk）。
type chunkFrom struct {
	Cursor string
	LastNo int
}

// chunkFn 消费一个块。用散参数而不是连接器的 Chunk 类型：两种源的块类型不同，
// 引擎只关心「哪张表、第几块、哪些行、边界是什么」。
type chunkFn func(ctx context.Context, ref source.TableRef, no int, rows [][]any, from, to string) error

// prepOutcome 是 Prepare 的结果。
type prepOutcome struct {
	SnapshotName    string
	ConsistentPoint source.Position
	Actions         []source.Action
}

// replayState 回答「这个位点还能不能重放」。
type replayState struct {
	// OK 表示源端还留着这段变更（PG 的槽位还在且没丢 WAL；MySQL 的 binlog 文件还在）。
	OK bool
	// Ahead 表示源端已经确认到的位点**越过**了目标位点：中间那段被清理了。
	Ahead bool
	// Confirmed 是源端自己确认到的位点（PG 的 confirmed_flush_lsn；MySQL 没有，零值）。
	Confirmed source.Position
	// Why 是给用户看的一句话（哪一条不成立）。
	Why string
}

// ── PG 适配器 ──────────────────────────────────────────────

type pgRunSource struct{ s *pgsource.Source }

func (p pgRunSource) Kind() string { return p.s.Kind() }

func (p pgRunSource) ParsePosition(raw string) (source.Position, error) {
	pos, err := p.s.ParsePosition(raw)
	if err != nil {
		return nil, err
	}
	return pos, nil
}

func (p pgRunSource) Before(a, b source.Position) bool {
	av, aok := a.(pgsource.Position)
	bv, bok := b.(pgsource.Position)
	if !aok || !bok {
		return false // 类型不认识：保守地当作「不更早」，不动位点
	}
	return av.Compare(bv) < 0
}

func (p pgRunSource) IsZero(pos source.Position) bool {
	if pos == nil {
		return true
	}
	v, ok := pos.(pgsource.Position)
	return ok && v.IsZero()
}

func (p pgRunSource) RequiresExportedSnapshot() bool { return true }

// RebuildSlot 实现 slotRebuilder：只有 continuous 模式下的 --resync 会用到。
// 语义与「下次 Prepare 会建一条新的」绑定在一起，见 pgsource.Source.RebuildSlot。
func (p pgRunSource) RebuildSlot(ctx context.Context) error { return p.s.RebuildSlot(ctx) }

func (p pgRunSource) Prepare(ctx context.Context, needSnapshot bool) (prepOutcome, error) {
	res, err := p.s.Prepare(ctx, pgsource.PrepareOptions{Snapshot: needSnapshot})
	if err != nil {
		return prepOutcome{}, err
	}
	return prepOutcome{
		SnapshotName:    res.SnapshotName,
		ConsistentPoint: res.ConsistentPoint,
		Actions:         res.Actions,
	}, nil
}

func (p pgRunSource) SnapshotChunksFrom(ctx context.Context, opt source.SnapshotOpt,
	from map[string]chunkFrom, fn chunkFn) (source.Position, error) {

	resume := make(map[string]pgsource.ChunkResume, len(from))
	for k, v := range from {
		resume[k] = pgsource.ChunkResume{Cursor: v.Cursor, LastNo: v.LastNo}
	}
	return p.s.SnapshotChunksFrom(ctx, opt, resume, func(c context.Context, ch pgsource.Chunk) error {
		return fn(c, ch.Table, ch.No, ch.Rows, ch.From, ch.To)
	})
}

func (p pgRunSource) EndSnapshot() { p.s.EndSnapshot() }

func (p pgRunSource) Stream(ctx context.Context, from source.Position, fn source.EventHandler) error {
	v, ok := from.(pgsource.Position)
	if !ok {
		if from != nil {
			return fmt.Errorf("%w：PG 源的起始位点类型是 %T", source.ErrPositionMalformed, from)
		}
	}
	return p.s.Stream(ctx, v, fn)
}

func (p pgRunSource) LastCommit() source.Position { return p.s.LastCommit() }

func (p pgRunSource) SafeAckPoint() (source.Position, bool) {
	pos, ok := p.s.SafeAckPoint()
	if !ok {
		return nil, false
	}
	return pos, true
}

func (p pgRunSource) Ack(pos source.Position) {
	if v, ok := pos.(pgsource.Position); ok {
		p.s.Ack(v)
	}
}

func (p pgRunSource) Advance(ctx context.Context, to source.Position) error {
	v, ok := to.(pgsource.Position)
	if !ok {
		return nil
	}
	return p.s.Advance(ctx, v)
}

func (p pgRunSource) Now(ctx context.Context) (source.Position, error) {
	pos, err := p.s.CurrentLSN(ctx)
	if err != nil {
		return nil, err
	}
	return pos, nil
}

func (p pgRunSource) ServerEnd() source.Position { return p.s.ServerWALEnd() }

func (p pgRunSource) Backlog(ctx context.Context, from source.Position) (int64, error) {
	return p.s.SlotLagBytes(ctx)
}

func (p pgRunSource) Replay(ctx context.Context, pos source.Position) (replayState, error) {
	stored, ok := pos.(pgsource.Position)
	if !ok {
		return replayState{OK: true}, nil // 位点读不懂：交给别处报，不在这里判「追不回来」
	}
	slot, ok, err := p.s.SlotConfirmed(ctx)
	if err != nil {
		return replayState{}, fmt.Errorf("读源端书签的确认位点失败: %w", err)
	}
	st := replayState{OK: ok}
	if !ok {
		st.Why = "源端书签不在了（或已经失效）"
		return st, nil
	}
	st.Confirmed = slot
	if !slot.IsZero() && slot.Compare(stored) > 0 {
		st.Ahead = true
		st.Why = "源端书签已经越过目标端的位点"
	}
	return st, nil
}

func (p pgRunSource) Count(ctx context.Context, ref source.TableRef) (int64, error) {
	return p.s.Count(ctx, ref)
}

func (p pgRunSource) Fingerprint(ctx context.Context, ref source.TableRef) (string, error) {
	return p.s.Fingerprint(ctx, ref)
}

// FingerprintScope 为空：PG 侧是服务端原生指纹（`count:sum(hashtextextended)`），
// 与 pgsink 的指纹同口径，可以直接比。
func (p pgRunSource) FingerprintScope() string { return "" }

func (p pgRunSource) Close() { p.s.Close() }

// ── PG 适配器的 interval 能力 ─────────────────────────────
//
// 定时增量（水位轮询）这一版只有 PG 源实现，所以这五个方法只出现在 PG 适配器上：
// 引擎按 intervalSource 断言，MySQL 源断言不过就给四段式说明（intervalUnsupported）。
// 放进 runSource 接口是不对的——那会逼 MySQL 源也实现一套它做不到的语义。

func (p pgRunSource) TableRowsEstimate(ctx context.Context, ref source.TableRef) (int64, bool, error) {
	return p.s.TableRowsEstimate(ctx, ref)
}

func (p pgRunSource) TablePhysicalSize(ctx context.Context, ref source.TableRef) (int64, error) {
	return p.s.TablePhysicalSize(ctx, ref)
}

func (p pgRunSource) MaxWatermark(ctx context.Context, ref source.TableRef, column string) (pgsource.Watermark, error) {
	return p.s.MaxWatermark(ctx, ref, column)
}

func (p pgRunSource) ReadWatermarkPage(ctx context.Context, ref source.TableRef, column string,
	from pgsource.Watermark, lookback string, limit int64) ([][]any, pgsource.Watermark, bool, error) {
	return p.s.ReadWatermarkPage(ctx, ref, column, from, lookback, limit)
}

func (p pgRunSource) SnapshotChunks(ctx context.Context, opt source.SnapshotOpt,
	fn func(context.Context, pgsource.Chunk) error) (source.Position, error) {
	return p.s.SnapshotChunks(ctx, opt, fn)
}

// ── MySQL 适配器 ───────────────────────────────────────────

type myRunSource struct {
	s *mysqlsource.Source
	// note 让适配器把「我丢掉了什么」说出来（DDL 不搬这类事实必须留痕，
	// 否则用户只看到「跑完了」而索引/约束没到目标端）。
	note func(msg string)
	// tables 是本次选中的表：全量只搬这些，变更流也只交付这些。
	// mysqlsource 的 Stream 会发**全库**的变更（binlog 里没有「只订阅某几张表」这回事），
	// 过滤必须在引擎侧做，否则「只同步一张表」的任务会悄悄写别的表。
	tables []source.Table
}

func newMyRunSource(ctx context.Context, ep dsn.Endpoint, tables []source.Table) (*myRunSource, error) {
	s, err := mysqlsource.OpenEndpoint(ctx, ep)
	if err != nil {
		return nil, err
	}
	// 把选中的表定义交给连接器：binlog_row_metadata=MINIMAL（默认值）时 binlog 里
	// 只有列类型没有列名，靠这份 catalog 按序号把列名补回来。
	s.SetKnownTables(tables)
	return &myRunSource{s: s, tables: tables}, nil
}

func (m myRunSource) Kind() string { return m.s.Kind() }

func (m myRunSource) ParsePosition(raw string) (source.Position, error) {
	pos, err := m.s.ParsePosition(raw)
	if err != nil {
		return nil, err
	}
	return pos, nil
}

func (m myRunSource) Before(a, b source.Position) bool {
	av, aok := a.(mysqlsource.Position)
	bv, bok := b.(mysqlsource.Position)
	if !aok || !bok {
		return false
	}
	return av.Compare(bv) < 0
}

func (m myRunSource) IsZero(pos source.Position) bool {
	if pos == nil {
		return true
	}
	v, ok := pos.(mysqlsource.Position)
	return ok && v.IsZero()
}

// RequiresExportedSnapshot 为 false：MySQL 的一致性视图在**同一个连接的事务**里，
// 没有可以跨连接传递的快照名。
func (m myRunSource) RequiresExportedSnapshot() bool { return false }

func (m myRunSource) Prepare(ctx context.Context, needSnapshot bool) (prepOutcome, error) {
	// 不做全量就不需要一致性视图，也不需要位点：增量从目标库的位点接着流。
	if !needSnapshot {
		return prepOutcome{}, nil
	}
	pos, err := m.s.BeginSnapshot(ctx, m.tables)
	if err != nil {
		return prepOutcome{}, err
	}
	return prepOutcome{ConsistentPoint: pos}, nil
}

func (m myRunSource) SnapshotChunksFrom(ctx context.Context, opt source.SnapshotOpt,
	from map[string]chunkFrom, fn chunkFn) (source.Position, error) {

	resume := make(map[string]mysqlsource.ChunkResume, len(from))
	for k, v := range from {
		resume[k] = mysqlsource.ChunkResume{Cursor: v.Cursor, LastNo: v.LastNo}
	}
	return m.s.SnapshotChunksFrom(ctx, opt, resume, func(c context.Context, ch mysqlsource.Chunk) error {
		return fn(c, ch.Table, ch.No, ch.Rows, ch.From, ch.To)
	})
}

func (m myRunSource) EndSnapshot() { m.s.EndSnapshot() }

func (m myRunSource) Stream(ctx context.Context, from source.Position, fn source.EventHandler) error {
	keep := make(map[string]bool, len(m.tables))
	for _, t := range m.tables {
		keep[tableKey(t.TableRef)] = true
	}
	return m.s.Stream(ctx, from, func(c context.Context, ev source.Event) error {
		switch ev.Kind {
		case source.EvDDL:
			if !keep[tableKey(ev.Table)] {
				return nil
			}
			// MySQL 的 DDL 语句（`ALTER TABLE ... ENGINE=InnoDB`、`MODIFY COLUMN`）
			// 不能原样拿到 PG 上执行：方言不同，执行了也是错的一半。这一版**不搬 DDL**，
			// 但必须说出来——静默跳过会让人以为索引/约束也过去了。
			// 行数据不受影响：新列会随 Relation 带来，目标端按「加列」白名单自己补。
			if m.note != nil {
				m.note(fmt.Sprintf("源端 DDL 没有搬到目标端（本版不搬 DDL）：%s。"+
					"数据行照常同步；索引/约束/改类型需要手工在目标端执行", clipSQL(ev.DDL)))
			}
			return nil
		case source.EvInsert, source.EvUpdate, source.EvDelete, source.EvTruncate:
			if !keep[tableKey(ev.Table)] {
				// 不在本次范围里的表：数据不交付，但事务边界照样往上走
				// （EvBegin/EvCommit 不受影响），位点因此不会卡在别人的事务上。
				return nil
			}
		}
		return fn(c, ev)
	})
}

// tableKey 是「一张表」的判等键：**必须是 schema + 表名**。
//
// 只按表名比会串库：同一个 MySQL 实例上，`业务库.orders` 与 `目标库.orders` 是两张表，
// 表名一样。MySQL 的 binlog 是实例级的（没有「只订阅某几张表」这回事），所以
// 目标库如果和源库同实例（本地验收、小规模自建很常见），引擎会读到自己刚写进去的行，
// 结果就是「把自己的写入当成源端变更再写一遍」——当时的报错是
// `Unknown column 'col_1'`：目标库那张同名表不在 catalog 里，binlog 没给列名
// （binlog_row_metadata=MINIMAL），于是拿占位名去拼 UPDATE。
func tableKey(ref source.TableRef) string { return ref.Schema + "." + ref.Table }

// clipSQL 把一条 DDL 截短到一句能进事件流的话（全文没有意义，用户有源库）。
func clipSQL(s string) string {
	const max = 160
	s = strings.TrimSpace(strings.Join(strings.Fields(s), " "))
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}

func (m myRunSource) LastCommit() source.Position { return m.s.LastCommit() }

func (m myRunSource) SafeAckPoint() (source.Position, bool) { return m.s.SafeAckPoint() }

func (m myRunSource) Ack(pos source.Position) { m.s.Ack(pos) }

func (m myRunSource) Advance(ctx context.Context, to source.Position) error {
	return m.s.Advance(ctx, to)
}

func (m myRunSource) Now(ctx context.Context) (source.Position, error) {
	pos, err := m.s.Status(ctx)
	if err != nil {
		return nil, err
	}
	return pos, nil
}

// ServerEnd 零值：MySQL 的 binlog 由源端自己写，没有「服务端公布的消费者位点」。
func (m myRunSource) ServerEnd() source.Position { return nil }

func (m myRunSource) Backlog(ctx context.Context, from source.Position) (int64, error) {
	return m.s.BacklogBytes(ctx, from)
}

func (m myRunSource) Replay(ctx context.Context, pos source.Position) (replayState, error) {
	ok, why, err := m.s.BinlogContains(ctx, pos)
	if err != nil {
		return replayState{}, err
	}
	return replayState{OK: ok, Why: why}, nil
}

func (m myRunSource) Count(ctx context.Context, ref source.TableRef) (int64, error) {
	return m.s.Count(ctx, ref)
}

func (m myRunSource) Fingerprint(ctx context.Context, ref source.TableRef) (string, error) {
	return m.s.Fingerprint(ctx, ref)
}

func (m myRunSource) FingerprintScope() string { return m.s.FingerprintScope() }

func (m myRunSource) Close() { _ = m.s.Close() }
