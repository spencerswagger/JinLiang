package mysqlsource

import (
	"context"
	"fmt"
	"time"

	"github.com/spencerswagger/jinliang/internal/source"
)

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

	// MaxEvents / MaxBytes cap one transaction; 0 means the defaults
	// (1,000,000 events / 128 MB) from docs/STREAMING.md §1.
	MaxEvents int
	MaxBytes  int

	open      bool
	txID      string
	pending   []source.Event
	bytes     int
	commitAt  time.Time
	position  Position // 下一件事务该从这里开始
	committed Position // 最后一次提交后的位点
	gtid      GTIDSet
	txStarted Position
	aborted   bool
}

// NewAssembler wires a handler.
func NewAssembler(handler TxHandler) *Assembler {
	return &Assembler{
		handler:   handler,
		MaxEvents: 1_000_000,
		MaxBytes:  128 << 20,
		gtid:      GTIDSet{},
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

	case !ev.Previous.IsEmpty():
		a.gtid.Merge(ev.Previous)
		return nil

	case ev.GTID != nil:
		// 每个事务都以 GTID 事件开头（没开 GTID 时是匿名 GTID）。
		if a.open {
			return fmt.Errorf("上一个事务（%s）没等到提交就又开始了新事务，流不可信", a.txID)
		}
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
		events, n, err := toSourceEvents(ev.Rows, a.txID, a.commitAt)
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

	// 其它语句：DDL / TRUNCATE 等。事务内的语句跟着事务走，事务外的自成一件。
	se := source.Event{
		Kind:     source.EvDDL,
		Table:    source.TableRef{Schema: ev.Query.Schema},
		DDL:      ev.Query.Query,
		TxID:     a.txID,
		CommitTS: a.commitAt,
	}
	if verb == "TRUNCATE" {
		se.Kind = source.EvTruncate
	}
	if a.open {
		a.pending = append(a.pending, se)
		a.bytes += len(ev.Query.Query)
		return a.checkLimits()
	}
	// 独立语句（DDL 通常自带提交）：立刻作为单事件事务交付。
	a.open = true
	a.txID = fmt.Sprintf("%s:%d", ev.File, ev.Header.LogPos)
	a.pending = []source.Event{se}
	a.bytes = len(ev.Query.Query)
	a.commitAt = time.Unix(int64(ev.Header.Timestamp), 0).UTC()
	return a.commit(ctx)
}

func (a *Assembler) checkLimits() error {
	if a.MaxEvents > 0 && len(a.pending) > a.MaxEvents {
		return fmt.Errorf("单个事务超过 %d 个事件（源端事务过大），按 docs/STREAMING.md §1 需要 spill，交给引擎处理", a.MaxEvents)
	}
	if a.MaxBytes > 0 && a.bytes > a.MaxBytes {
		return fmt.Errorf("单个事务超过 %d MB（源端事务过大），按 docs/STREAMING.md §1 需要 spill，交给引擎处理", a.MaxBytes>>20)
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
	events := make([]source.Event, len(a.pending))
	copy(events, a.pending)
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
func toSourceEvents(rows *RowsEvent, txID string, commitTS time.Time) ([]source.Event, int, error) {
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
