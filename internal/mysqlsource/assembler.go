package mysqlsource

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/spencerswagger/jinliang/internal/core"
	"github.com/spencerswagger/jinliang/internal/source"
)

// ErrTxOversize 表示源端的单笔事务超过缓冲上限（行数或字节数）。
//
// 为什么要一个可判定的哨兵：调用方（引擎/CLI）需要区分「事务过大，请拆分或调阈值」
// 和「流坏了」，前者可以带着配置继续跑，后者只能修数据。用 errors.Is(err, ErrTxOversize) 判断。
// 用户看到的码是 core.CodeTxTooLarge（E1011，user_action），文案与码表保持一致。
var ErrTxOversize = errors.New("源端事务超过缓冲上限（tx_oversize）")

// Tx is one source transaction, whole. The engine must apply it in a single
// target transaction together with Commit — that is what makes "整个事务可见
// 或完全不可见" true (docs/STATE-SCHEMA.md).
type Tx struct {
	ID         string         // GTID，或没开 GTID 时的 "file:pos"
	Events     []source.Event // 事务内按源顺序
	Commit     Position       // 提交后的位点（含 GTID 集合 + 毫秒时间）
	CommitTime time.Time
	Bytes      int
}

// TxHandler consumes committed transactions. Returning an error stops the
// stream and leaves the checkpoint where it was — the transaction is replayed
// from the source next time, which is safe because target writes are upserts.
type TxHandler func(ctx context.Context, tx Tx) error

// Assembler groups events into transactions and enforces integrity.
//
// 规矩只有一条：没等到 XID / COMMIT 就不交给下游。半截事务宁可不发，
// 也不能发一半——发一半会让下游看到源端从未存在过的状态。
type Assembler struct {
	handler TxHandler

	// OnHeartbeat is called for idle heartbeats (lag measurement).
	OnHeartbeat func(hb Heartbeat, at time.Time)

	// Collations 把 MySQL 的排序规则 id 解释成 (charset, collation) 名字；
	// 为 nil 时用内置的常见表。RunStream 会注入 information_schema 里的真表。
	Collations map[uint64]collation

	// MaxEvents / MaxBytes cap one transaction; 0 means the defaults
	// (1,000,000 events / 128 MB) from docs/STREAMING.md §1.
	//
	// 为什么必须有上限：我们整段缓冲一个事务（未提交不交付），内存就是
	// 行数 × 行宽。源端一次 DELETE 5000 万行、或 INSERT…SELECT 灌 2000 万行
	// 都是真实场景，没有上限就是 OOM 而不是一条能读的错误。
	MaxEvents int
	MaxBytes  int

	// 表定义（source.Event.Relation）：按表号缓存，定义没变就复用同一个指针，
	// 变了才换新指针——契约里「指针相等即同一版定义」的约定靠这里保证。
	relations   map[uint64]*source.Table
	relationSig map[uint64]string

	open              bool
	startedStandalone bool
	txID              string
	pending           []source.Event
	bytes             int
	commitAt          time.Time
	position          Position // 下一件事务该从这里开始
	committed         Position // 最后一次提交后的位点
	gtid              GTIDSet
	txStarted         Position
	aborted           bool
}

// NewAssembler wires a handler.
func NewAssembler(handler TxHandler) *Assembler {
	return &Assembler{
		handler:     handler,
		MaxEvents:   1_000_000,
		MaxBytes:    128 << 20,
		gtid:        GTIDSet{},
		relations:   map[uint64]*source.Table{},
		relationSig: map[uint64]string{},
	}
}

// Position returns the checkpoint of the last committed transaction.
func (a *Assembler) Position() Position { return a.committed }

// ExecutedGTID returns the GTID set accumulated so far.
func (a *Assembler) ExecutedGTID() GTIDSet { return a.gtid.Clone() }

// SetPosition tells the assembler where the stream starts, so the first
// committed transaction carries the right file/offset even if the server sends
// nothing but heartbeats for a while.
func (a *Assembler) SetPosition(p Position) {
	a.position = p
	a.committed = p
}

// SetExecutedGTID seeds the running set (from SHOW BINARY LOG STATUS or from a
// PREVIOUS_GTIDS event in the file header).
func (a *Assembler) SetExecutedGTID(g GTIDSet) { a.gtid = g.Clone() }

// Feed consumes one decoded event.
func (a *Assembler) Feed(ctx context.Context, ev *Event) error {
	if ev == nil {
		return nil
	}
	a.position.File = ev.File
	if ev.Header.LogPos >= a.position.Pos {
		a.position.Pos = ev.Header.LogPos
	}
	a.position.ServerID = ev.Header.ServerID
	if ev.Header.Timestamp != 0 {
		a.position.TakenAt = time.Unix(int64(ev.Header.Timestamp), 0).UTC()
	}

	switch {
	case ev.Heartbeat != nil:
		if a.OnHeartbeat != nil {
			a.OnHeartbeat(*ev.Heartbeat, time.Now().UTC())
		}
		return nil

	case ev.Rotate != nil:
		a.position.File = ev.Rotate.NextFile
		a.position.Pos = uint32(ev.Rotate.NextPos)
		return nil

	case ev.TableMap != nil:
		// 新的一份列定义。定义变了才换指针（契约要求同一版复用同一指针）。
		sig := relationSignature(ev.TableMap)
		if prev, ok := a.relations[ev.TableMap.TableID]; !ok || a.relationSig[ev.TableMap.TableID] != sig || prev == nil {
			cols := a.Collations
			if cols == nil {
				cols = staticCollations
			}
			a.relations[ev.TableMap.TableID] = tableFromMapWith(ev.TableMap, cols)
			a.relationSig[ev.TableMap.TableID] = sig
		}
		return nil

	case !ev.Previous.IsEmpty():
		a.gtid.Merge(ev.Previous)
		return nil

	case ev.GTID != nil:
		// 每个事务都以 GTID 事件开头（没开 GTID 时是匿名 GTID）。
		if a.open && len(a.pending) > 0 {
			last := a.pending[len(a.pending)-1]
			return fmt.Errorf("上一个事务（%s）没等到提交就又开始了新事务，流不可信："+
				"该事务已缓冲 %d 条事件（最后一条 kind=%d，位置 %s:%d）",
				a.txID, len(a.pending), last.Kind, a.position.File, a.position.Pos)
		}
		// 上一个事务是空的（没有行变更、也没等到提交事件）：直接换掉。
		// 常见于从半截位置起流，或服务端只写了一个空的 GTID 占位。
		if ev.GTID.GNO > 0 {
			if err := a.gtid.Add(ev.GTID.GTID()); err != nil {
				return err
			}
			a.txID = ev.GTID.GTID()
		} else {
			a.txID = fmt.Sprintf("%s:%d", ev.File, ev.Header.LogPos)
		}
		a.commitAt = ev.GTID.CommitTime()
		a.open = true
		a.pending = a.pending[:0]
		a.bytes = 0
		a.aborted = false
		a.txStarted = a.position
		return nil

	case ev.Rows != nil:
		if !a.open {
			return fmt.Errorf("%s.%s 的行变更出现在事务边界之外（没有 BEGIN/XID），拒绝半截应用",
				ev.Rows.Table.Schema, ev.Rows.Table.Table)
		}
		if a.aborted {
			return nil
		}
		events, n, err := toSourceEvents(ev.Rows, a.txID, a.commitAt, a.relations[ev.Rows.Table.TableID])
		if err != nil {
			return err
		}
		a.pending = append(a.pending, events...)
		a.bytes += n
		return a.checkLimits()

	case ev.Query != nil:
		return a.applyQuery(ctx, ev)

	case ev.XID != nil:
		if !a.open {
			// 空事务：MySQL 偶尔会写一个没有行变更的 XID，直接丢掉。
			return nil
		}
		if a.aborted {
			a.reset()
			return nil
		}
		return a.commit(ctx)
	}
	return nil
}

func (a *Assembler) applyQuery(ctx context.Context, ev *Event) error {
	verb := queryVerb(ev.Query.Query)
	switch verb {
	case "BEGIN":
		if a.open && len(a.pending) > 0 {
			return fmt.Errorf("上一个事务（%s）没提交就又 BEGIN 了，流不可信", a.txID)
		}
		a.open = true
		if a.txID == "" {
			a.txID = fmt.Sprintf("%s:%d", ev.File, ev.Header.LogPos)
		}
		a.pending = a.pending[:0]
		a.bytes = 0
		a.aborted = false
		a.commitAt = time.Unix(int64(ev.Header.Timestamp), 0).UTC()
		return nil
	case "COMMIT":
		if !a.open {
			return nil
		}
		if a.aborted {
			a.reset()
			return nil
		}
		return a.commit(ctx)
	case "ROLLBACK":
		// 源端回滚了：整段丢弃，一条都不许漏到下游。
		a.reset()
		return nil
	case "SAVEPOINT", "ROLLBACK TO", "RELEASE":
		// 保存点不影响事务整体可见性；语句本身不需要搬到目标端。
		return nil
	}

	// 其它语句：DDL / TRUNCATE / 授权 等。
	//
	// MySQL 里只有事务控制语句（BEGIN/COMMIT/ROLLBACK/SAVEPOINT）和语句格式的 DML
	// 会留在事务内；其余语句一律**隐式提交**，并且在 binlog 里自成一笔事务
	// （GTID + QUERY，没有 XID）。这里必须先把已缓冲的行结掉，再把这条语句当独立
	// 事务立刻提交——否则会一直等一个永远不会来的 XID，随后的新 GTID 就会撞上
	// 「上一个事务没提交」（9.x 矩阵就是这样把它炸出来的）。
	if queryEndsTransaction(verb) {
		if a.open && len(a.pending) > 0 {
			if err := a.commit(ctx); err != nil {
				return err
			}
		}
		if !a.open {
			a.open = true
			a.txID = fmt.Sprintf("%s:%d", ev.File, ev.Header.LogPos)
			a.commitAt = time.Unix(int64(ev.Header.Timestamp), 0).UTC()
		}
		a.pending = []source.Event{statementEvent(verb, ev, a.txID, a.commitAt)}
		a.bytes = len(ev.Query.Query)
		if err := a.checkLimits(); err != nil {
			return err
		}
		return a.commit(ctx)
	}

	// 语句格式的 DML（binlog_format 不是 ROW 时才会出现）：跟着事务走。
	if !a.open {
		a.open = true
		a.txID = fmt.Sprintf("%s:%d", ev.File, ev.Header.LogPos)
		a.commitAt = time.Unix(int64(ev.Header.Timestamp), 0).UTC()
	}
	a.pending = append(a.pending, statementEvent(verb, ev, a.txID, a.commitAt))
	a.bytes += len(ev.Query.Query)
	return a.checkLimits()
}

// statementEvent wraps a logged statement as a source event.
//
// **表名必须解析出来**：binlog 的 Query 事件只有「默认库 + 语句原文」，
// 而引擎按 `schema.表` 判断这条 DDL 在不在同步范围里。不解析就是现在这个样子——
// 表名恒为空、判定恒落空、源端 DDL 被**静默丢掉**（连"本版不搬 DDL"那声提示都没有）。
// 解析不出（多表语句等）时保持空表名：宁可不提示，也不要把 DDL 记到错的表头上。
func statementEvent(verb string, ev *Event, txID string, commitTS time.Time) source.Event {
	kind := source.EvDDL
	if verb == "TRUNCATE" {
		kind = source.EvTruncate
	}
	ref := source.TableRef{Schema: ev.Query.Schema}
	if schema, table, ok := ddlTargetTable(ev.Query.Query); ok {
		if schema != "" {
			ref.Schema = schema
		}
		ref.Table = table
	}
	return source.Event{
		Kind:     kind,
		Table:    ref,
		DDL:      ev.Query.Query,
		TxID:     txID,
		CommitTS: commitTS,
	}
}

// queryEndsTransaction reports whether this statement implicitly commits and
// forms its own binlog transaction.
func queryEndsTransaction(verb string) bool {
	switch verb {
	case "INSERT", "UPDATE", "DELETE", "REPLACE":
		// 语句格式的 DML：属于当前事务（我们只支持 ROW，这条是防御）。
		return false
	}
	return true
}

// txTooLargeMessage 取码表里的人话标题，保证与 UI/文档同一句。
func txTooLargeMessage() string {
	if spec, ok := core.Lookup(core.CodeTxTooLarge); ok {
		return spec.Message
	}
	return string(core.CodeTxTooLarge)
}

// TxOversizeError 是超限时的具体错误：用户看到码表里的 E1011（user_action），
// 调用方用 errors.Is(err, ErrTxOversize) 判定类别。
type TxOversizeError struct {
	Events    int
	MaxEvents int
	Bytes     int
	MaxBytes  int
	eventsHit bool
}

func (e *TxOversizeError) Unwrap() error { return ErrTxOversize }

func (e *TxOversizeError) Error() string {
	var got, limit string
	if e.eventsHit {
		got = fmt.Sprintf("已缓冲 %d 个变更事件", e.Events)
		limit = fmt.Sprintf("上限 %d 个（可用环境变量 JINLIANG_TX_MAX_EVENTS 调高）", e.MaxEvents)
	} else {
		got = "已缓冲 " + humanBytes(e.Bytes)
		limit = "上限 " + humanBytes(e.MaxBytes) + "（可用环境变量 JINLIANG_TX_MAX_BYTES 调高）"
	}
	return fmt.Sprintf("%s %s：%s，超过%s。"+
		"原因：我们整段缓冲一个事务（未提交不交付），内存 ≈ 行数 × 行宽，"+
		"源端一次删/灌几千万行就会把内存吃爆——无上限就是 OOM，而 OOM 时用户拿不到任何线索。"+
		"你能做的：① 拆小事务：分批 DELETE / INSERT（每批几千行）后重跑；"+
		"② 确实拆不了：用环境变量 JINLIANG_TX_MAX_BYTES / JINLIANG_TX_MAX_EVENTS 调高上限，并确认机器内存够。"+
		"风险：拆事务会改变源端的提交边界，业务侧要挑低峰做；调高上限则峰值内存随之上涨，"+
		"逼近机器上限时仍会 OOM",
		core.CodeTxTooLarge, txTooLargeMessage(), got, limit)
}

func humanBytes(n int) string {
	switch {
	case n < 1<<10:
		return fmt.Sprintf("%d 字节", n)
	case n < 1<<20:
		return fmt.Sprintf("%d KB", n>>10)
	default:
		return fmt.Sprintf("%d MB", n>>20)
	}
}

func (a *Assembler) checkLimits() error {
	// 超出任一上限都抛同一个类型化错误：调用方按类别处理，用户看到 E1011 的四段式。
	if a.MaxEvents > 0 && len(a.pending) > a.MaxEvents {
		return &TxOversizeError{
			Events: len(a.pending), MaxEvents: a.MaxEvents,
			Bytes: a.bytes, MaxBytes: a.MaxBytes, eventsHit: true,
		}
	}
	if a.MaxBytes > 0 && a.bytes > a.MaxBytes {
		return &TxOversizeError{
			Events: len(a.pending), MaxEvents: a.MaxEvents,
			Bytes: a.bytes, MaxBytes: a.MaxBytes,
		}
	}
	return nil
}

func (a *Assembler) commit(ctx context.Context) error {
	// 先算出「提交后应在的位置」，等下游真的写成功再落到 committed 上。
	// 反过来的话，下游失败时位点已经前进，重启就会跳过这个事务（丢数据）。
	cp := a.position
	cp.GTID = a.gtid.Clone()
	if cp.TakenAt.IsZero() {
		cp.TakenAt = time.Now().UTC()
	}
	if len(a.pending) == 0 {
		// 空事务：位置推进，但不产生批次（docs/STREAMING.md §6）。
		a.committed = cp
		a.reset()
		return nil
	}
	events := make([]source.Event, 0, len(a.pending)+2)
	// 事务边界：EvCommit 是「这个事务已提交」的唯一信号（sink.Apply 不变量）。
	// 源端回滚的事务根本不会进到这里（applyQuery 里整段丢弃），所以没有 rollback 事件。
	events = append(events, source.Event{Kind: source.EvBegin, TxID: a.txID, CommitTS: a.commitAt})
	events = append(events, a.pending...)
	events = append(events, source.Event{Kind: source.EvCommit, TxID: a.txID, CommitTS: a.commitAt})
	tx := Tx{
		ID:         a.txID,
		Events:     events,
		Commit:     cp,
		CommitTime: a.commitAt,
		Bytes:      a.bytes,
	}
	if err := a.handler(ctx, tx); err != nil {
		return err
	}
	a.committed = cp
	a.reset()
	return nil
}

func (a *Assembler) reset() {
	a.open = false
	a.aborted = false
	a.txID = ""
	a.pending = a.pending[:0]
	a.bytes = 0
	a.commitAt = time.Time{}
	a.txStarted = Position{}
}

// toSourceEvents converts decoded row images into the shared event type.
//
// 只接受完整镜像（binlog_row_image=FULL）。MINIMAL 时 UPDATE 只带改动列，
// 硬转会把没出现的列当成 NULL 写进目标端——那是静默数据损坏。
func toSourceEvents(rows *RowsEvent, txID string, commitTS time.Time, relation *source.Table) ([]source.Event, int, error) {
	out := make([]source.Event, 0, len(rows.Changes))
	size := 0
	for _, ch := range rows.Changes {
		ev := source.Event{
			Kind:     ch.Kind,
			Table:    ch.New.Row.Table,
			New:      ch.New.Row,
			Old:      ch.Old.Row,
			TxID:     txID,
			CommitTS: commitTS,
			// 每条数据事件都带上当版定义：指针相等即同一版，引擎可以按指针缓存
			// 解码计划；DDL 之后第一件事就是表映射，所以「先换定义再发数据」自动成立。
			Relation: relation,
		}
		if ev.Table.Schema == "" {
			ev.Table = ch.Old.Row.Table
		}
		switch ch.Kind {
		case source.EvInsert:
			if err := requireFull(ch.New, rows, "新增"); err != nil {
				return nil, 0, err
			}
			ev.Old = source.Row{}
		case source.EvUpdate:
			if err := requireFull(ch.New, rows, "更新后"); err != nil {
				return nil, 0, err
			}
			if err := requireFull(ch.Old, rows, "更新前"); err != nil {
				return nil, 0, err
			}
		case source.EvDelete:
			if err := requireFull(ch.Old, rows, "删除"); err != nil {
				return nil, 0, err
			}
			ev.New = source.Row{}
			ev.Table = ch.Old.Row.Table
		}
		for _, v := range ev.New.Values {
			size += valueSize(v)
		}
		for _, v := range ev.Old.Values {
			size += valueSize(v)
		}
		out = append(out, ev)
	}
	return out, size, nil
}

func requireFull(img Image, rows *RowsEvent, what string) error {
	for i, ok := range img.Present {
		if ok {
			continue
		}
		col := fmt.Sprintf("第 %d 列", i+1)
		if i < len(rows.Table.Names) {
			col = rows.Table.Names[i]
		}
		return fmt.Errorf("%s.%s 的%s镜像缺列 %s：源库 binlog_row_image 不是 FULL；"+
			"请执行 SET GLOBAL binlog_row_image='FULL' 后重新取位点", rows.Table.Schema, rows.Table.Table, what, col)
	}
	return nil
}

func valueSize(v source.Value) int {
	switch v.Kind {
	case source.KindString:
		return len(v.S) + 16
	case source.KindBytes:
		return len(v.B) + 16
	default:
		return 16
	}
}

// Flush reports whether a transaction is currently open (used by tests and by
// the shutdown path to explain why nothing was committed).
func (a *Assembler) Flush() (open bool, txID string, events int) {
	return a.open, a.txID, len(a.pending)
}
