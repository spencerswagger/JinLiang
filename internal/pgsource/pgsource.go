// Package pgsource reads from a PostgreSQL source: the consistency snapshot
// (EXPORT_SNAPSHOT + chunked reads) and the logical change stream (pgoutput).
//
// 两条读路径共用同一个书签（replication slot）：快照从书签导出的视图读，
// 增量从书签的起点流。书签在快照期间不推进，所以变更流水不会被清掉，
// 切换时不会丢（docs/SNAPSHOT.md §5）。
package pgsource

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/pgwire"
	"github.com/spencerswagger/jinliang/internal/source"
)

// OutputPlugin is the only decoding plugin we speak in B1.
const OutputPlugin = "pgoutput"

// Position is a PostgreSQL checkpoint: the LSN the target has durably applied.
// It serialises to the "X/Y" form PostgreSQL prints, which is what `_jl_meta`
// stores and what STATUS shows to a human.
type Position struct {
	LSN pglogrepl.LSN
}

// String is the only form that reaches the target database: "<kind>:v1:<payload>"
// (docs 契约 2026-11-05). 位点要跨版本存活，所以版本号写在串里。
func (p Position) String() string {
	return source.FormatPosition(dsn.KindPostgres, p.LSN.String())
}

// Kind reports which kind of source produced this position. It is the endpoint
// kind, never a position variant — the engine uses it to check "this checkpoint
// belongs to this source".
func (p Position) Kind() string { return dsn.KindPostgres }

// IsZero reports whether the position was never set.
func (p Position) IsZero() bool { return p.LSN == 0 }

// Compare orders two positions (-1 / 0 / 1).
func (p Position) Compare(o Position) int {
	switch {
	case p.LSN < o.LSN:
		return -1
	case p.LSN > o.LSN:
		return 1
	}
	return 0
}

// ErrPositionForeignSource means the stored checkpoint was written by another
// kind of source (a MySQL position on a PostgreSQL job). It is NOT E3001:
// 换回正确的源，而不是重搬一遍全量（E1010）。
type ErrPositionForeignSource struct {
	Want string
	Got  string
}

func (e *ErrPositionForeignSource) Error() string {
	return fmt.Sprintf("目标库里的同步进度是 %s 写的，而这次连的是 %s：换回对应的源，或重新开始同步", e.Got, e.Want)
}

// ParsePayload parses the PG-specific part of a position ("0/16B3748").
func ParsePayload(payload string) (Position, error) {
	payload = strings.TrimSpace(payload)
	if payload == "" {
		return Position{}, nil
	}
	lsn, err := pglogrepl.ParseLSN(payload)
	if err != nil {
		return Position{}, fmt.Errorf("%w：载荷 %q 不是 PostgreSQL 的 WAL 位置（形如 0/16B3748）",
			source.ErrPositionMalformed, payload)
	}
	return Position{LSN: lsn}, nil
}

// ParsePosition parses the versioned wire form "postgres:v1:0/16B3748".
// 空串表示「还没有位点」。格式不对与「这是别人的位点」分开报（上层给不同错误码）。
func ParsePosition(raw string) (Position, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return Position{}, nil
	}
	kind, payload, err := source.SplitPosition(raw)
	if err != nil {
		return Position{}, err
	}
	if kind != dsn.KindPostgres {
		return Position{}, &ErrPositionForeignSource{Want: dsn.KindPostgres, Got: kind}
	}
	return ParsePayload(payload)
}

// Options configures the source half of one job.
type Options struct {
	Source      dsn.Endpoint
	JobKey      string
	Publication string
	Slot        string
	// Catalog is the selected tables in catalog order; it defines the column
	// order every Row follows.
	Catalog source.Catalog
}

// Source is the PostgreSQL read side.
type Source struct {
	opt    Options
	conn   *pgx.Conn // 普通连接：建发布、读快照、算校验和
	ackLSN atomic.Uint64
	// serverEnd is the source's WAL end as reported by the stream (keepalive /
	// XLogData). "Caught up" means applied >= this, not "no events for a while":
	// a 2 GB backlog produces no data events for a long time while it is being
	// replayed.
	serverEnd atomic.Uint64
	// keepEnd / keepPrev are the last two keepalive positions. 两次相同 = 发送端
	// 真的闲下来了（解码位置 == 发送位置），此时把位点推到那里是安全的。
	keepEnd  atomic.Uint64
	keepPrev atomic.Uint64
	// lastCommit is the LSN of the last committed source transaction. The
	// engine reads it when it flushes a batch: Event 里没有（也不该有）位点字段，
	// 共享契约由 Ark 维护。
	lastCommit atomic.Uint64

	tables map[string]*tableInfo
	order  []string
	// snapshotName is the consistency view exported by Prepare; empty means
	// "no exported snapshot" (resumed run or snapshot disabled).
	snapshotName string
	// snapConn keeps the replication connection that created the slot alive:
	// PostgreSQL invalidates an exported snapshot the moment that session ends,
	// so it must live until the last chunk is read (pg_basebackup does the same).
	snapConn *pgx.Conn
}

type tableInfo struct {
	ref        source.TableRef
	cols       []string
	types      []string
	collations []string // 与 cols 同序；空 = 类型/库默认排序规则
	pk         []string
	colIndex   map[string]int
}

// Open dials the source (no writes, no state tables here).
func Open(ctx context.Context, opt Options) (*Source, error) {
	s := &Source{opt: opt, tables: map[string]*tableInfo{}}
	for _, t := range opt.Catalog.Tables {
		ti := &tableInfo{
			ref:      t.TableRef,
			pk:       append([]string(nil), t.PrimaryKey...),
			colIndex: map[string]int{},
		}
		for _, c := range t.Columns {
			ti.colIndex[c.Name] = len(ti.cols)
			ti.cols = append(ti.cols, c.Name)
			ti.types = append(ti.types, c.Type)
			ti.collations = append(ti.collations, c.Collation)
		}
		key := t.Schema + "." + t.Table
		s.tables[key] = ti
		s.order = append(s.order, key)
	}
	conn, err := pgwire.Connect(ctx, opt.Source)
	if err != nil {
		return nil, err
	}
	s.conn = conn
	return s, nil
}

// Close releases both connections (snapshot connection first).
func (s *Source) Close() {
	s.EndSnapshot()
	if s.conn != nil {
		_ = s.conn.Close(context.Background())
	}
}

// Kind implements the endpoint contract.
func (s *Source) Kind() string { return dsn.KindPostgres }

// ParsePosition implements source.Source: the engine hands a stored position
// string back and gets a typed position, without knowing what an LSN is.
func (s *Source) ParsePosition(raw string) (source.Position, error) {
	p, err := ParsePosition(raw)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// CurrentLSN reads the server's current write position, used to measure lag and
// to know when catchup is done.
func (s *Source) CurrentLSN(ctx context.Context) (Position, error) {
	var v string
	if err := s.conn.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&v); err != nil {
		return Position{}, err
	}
	return ParsePayload(v)
}

// ServerTime returns the source clock (lag is measured against it).
func (s *Source) ServerTime(ctx context.Context) (time.Time, error) {
	var t time.Time
	err := s.conn.QueryRow(ctx, `SELECT now()`).Scan(&t)
	return t, err
}

// ── prepare：发布 + 书签 ───────────────────────────────────────

// PrepareOptions says what this run needs from the source.
type PrepareOptions struct {
	// Snapshot asks for a consistency view (initial copy). When it is false we
	// only need the bookmark, so an existing slot is reused as-is.
	Snapshot bool
}

// PrepareResult is what preparation produced.
type PrepareResult struct {
	SnapshotName    string // 空表示没有可用的导出一致性视图
	ConsistentPoint Position
	ReusedSlot      bool // 书签已存在：说明是续跑，不需要重新全量
	SlotCreated     bool
	Actions         []source.Action
}

// Prepare creates the publication (or aligns it with the selected tables) and
// the replication slot, exporting a consistency snapshot on first run.
//
// 两种书签语义写在这里，因为它是用户能感知的差别：
//   - 持久书签（默认）：跨重启保留，重启从位点续跑，源库会为它保留变更流水。
//   - 临时书签（--slot temporary，B2 起暴露）：进程退出即消失，不留磁盘占用，
//     但停超过保留期就只能重新全量。
//
// RebuildSlot 删掉本任务的书签（不存在就什么都不做）。下一次 Prepare 会建一条新的，
// 于是「变更流从源端当前位置重新开始」。
//
// 为什么单独一个方法而不是给 Prepare 加参数：`--resync` 在 continuous 模式（只接增量、
// 不做快照）下要的正是「不导出视图、只换书签」，而 Prepare 的 Snapshot 参数同时管着
// 导出视图；把两件事绑在一起会顺手多开一条永远不会被读的快照连接。
func (s *Source) RebuildSlot(ctx context.Context) error {
	_, exists, err := s.slotPosition(ctx)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if _, err := s.conn.Exec(ctx, `SELECT pg_drop_replication_slot($1)`, s.opt.Slot); err != nil {
		return fmt.Errorf("重建书签前删除旧书签 %s 失败（可能还有进程连着它）: %w", s.opt.Slot, err)
	}
	return nil
}

func (s *Source) Prepare(ctx context.Context, popt PrepareOptions) (PrepareResult, error) {
	res := PrepareResult{}

	exists, err := s.publicationExists(ctx)
	if err != nil {
		return res, err
	}
	targets := s.publicationTargets()
	if exists {
		stmt := fmt.Sprintf(`ALTER PUBLICATION %s SET TABLE %s`, pgwire.Quote(s.opt.Publication), targets)
		if _, err := s.conn.Exec(ctx, stmt); err != nil {
			return res, fmt.Errorf("更新发布 %s 失败（需要表的属主权限）: %w", s.opt.Publication, err)
		}
		res.Actions = append(res.Actions, source.Action{
			ID: "publication:" + s.opt.Publication, What: "把发布 " + s.opt.Publication + " 的表范围对齐到本次选择",
			SQL: stmt, Applied: true, Reason: "变更流水按发布投递，范围必须和任务一致",
		})
	} else {
		stmt := fmt.Sprintf(`CREATE PUBLICATION %s FOR TABLE %s`, pgwire.Quote(s.opt.Publication), targets)
		if _, err := s.conn.Exec(ctx, stmt); err != nil {
			return res, fmt.Errorf("建发布 %s 失败（需要建对象权限）: %w", s.opt.Publication, err)
		}
		res.Actions = append(res.Actions, source.Action{
			ID: "publication:" + s.opt.Publication, What: "建了发布 " + s.opt.Publication,
			SQL: stmt, Undo: "DROP PUBLICATION " + pgwire.Quote(s.opt.Publication),
			Applied: true, Reason: "读变更流水需要一份「哪些表要投递」的清单",
		})
	}

	slot, err := s.slotState(ctx)
	if err != nil {
		return res, err
	}
	slotLSN, slotExists := slot.LSN, slot.Exists
	// 要重建书签的两种情形，判据不同但动作一样：
	//   1) 这次要导出一致性视图（全量）：上次的导出视图随进程消失，旧的不能再导；
	//   2) 书签已经 wal_status=lost：WAL 被 max_slot_wal_keep_size 丢掉了，从任何旧位点
	//      都重放不出来。这种书签**仍在 pg_replication_slots 里占着名字**，不先 drop
	//      就建会报 already exists —— 那样每次 run 都失败，重跑也治不好。
	if slotExists && (popt.Snapshot || !slot.Replayable()) {
		if _, err := s.conn.Exec(ctx, `SELECT pg_drop_replication_slot($1)`, s.opt.Slot); err != nil {
			return res, fmt.Errorf("重建书签前删除旧书签 %s 失败（可能还有进程连着它）: %w", s.opt.Slot, err)
		}
		reason := "上次全量没读完，导出的读视图随进程消失；重搬全量比在半截数据上续跑安全"
		if !slot.Replayable() {
			reason = "书签的 WAL 已经被 max_slot_wal_keep_size 丢掉（wal_status=lost），" +
				"从旧位点再也重放不出来；保留它只会让下一次建书签报「已存在」"
		}
		res.Actions = append(res.Actions, source.Action{
			ID: "drop-slot:" + s.opt.Slot, What: "重建了同步书签 " + s.opt.Slot,
			SQL: "SELECT pg_drop_replication_slot('" + s.opt.Slot + "')", Applied: true,
			Reason: reason,
		})
		slotExists = false
	}
	if slotExists {
		res.ReusedSlot = true
		if slotLSN != "" {
			pos, err := ParsePayload(slotLSN)
			if err != nil {
				return res, err
			}
			res.ConsistentPoint = pos
		}
		if res.ConsistentPoint.IsZero() {
			// 书签存在但还没确认过任何位点：用当前 WAL 位置做起点。
			cur, err := s.CurrentLSN(ctx)
			if err != nil {
				return res, err
			}
			res.ConsistentPoint = cur
		}
		return res, nil
	}

	// 首次：建书签并导出一致性视图。这两件事必须是同一次动作，否则
	// 「快照读到的数据」和「增量起点」之间会漏掉一段。
	rconn, err := pgwire.ConnectReplication(ctx, s.opt.Source)
	if err != nil {
		return res, err
	}
	snapAction := ""
	if popt.Snapshot {
		snapAction = "EXPORT_SNAPSHOT"
		s.snapConn = rconn // 视图活着的前提：这个会话不断（见 Source.snapConn）
	} else {
		defer rconn.Close(context.Background())
	}
	created, err := pglogrepl.CreateReplicationSlot(ctx, rconn.PgConn(), s.opt.Slot, OutputPlugin,
		pglogrepl.CreateReplicationSlotOptions{
			Temporary:      false,
			SnapshotAction: snapAction,
			Mode:           pglogrepl.LogicalReplication,
		})
	if err != nil {
		return res, fmt.Errorf("建同步书签 %s 失败。常见原因只有三个，按可能性排："+
			"1) 源库里已经有同名书签（pg_replication_slots 查得到，包括 wal_status=lost 的），"+
			"先 SELECT pg_drop_replication_slot('%s') 再重试；"+
			"2) 账号缺 REPLICATION 权限（ALTER ROLE … WITH REPLICATION）；"+
			"3) 书签数已满（max_replication_slots）。原始错误：%w",
			s.opt.Slot, s.opt.Slot, err)
	}
	pos, err := ParsePayload(created.ConsistentPoint)
	if err != nil {
		return res, err
	}
	res.ConsistentPoint = pos
	res.SnapshotName = created.SnapshotName
	res.SlotCreated = true
	res.Actions = append(res.Actions, source.Action{
		ID: "slot:" + s.opt.Slot, What: "建了同步书签 " + s.opt.Slot,
		SQL:     fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL %s (EXPORT_SNAPSHOT)", pgwire.Quote(s.opt.Slot), OutputPlugin),
		Undo:    "SELECT pg_drop_replication_slot('" + s.opt.Slot + "')",
		Applied: true,
		Reason:  "书签是「源库帮我留多久变更」的凭据；它同时导出全量读的一致性视图",
	})
	return res, nil
}

func (s *Source) publicationExists(ctx context.Context) (bool, error) {
	var n int
	err := s.conn.QueryRow(ctx, `SELECT count(*) FROM pg_publication WHERE pubname=$1`, s.opt.Publication).Scan(&n)
	return n > 0, err
}

func (s *Source) publicationTargets() string {
	parts := make([]string, 0, len(s.order))
	for _, key := range s.order {
		ti := s.tables[key]
		parts = append(parts, pgwire.Qualified(ti.ref.Schema, ti.ref.Table))
	}
	return strings.Join(parts, ", ")
}

// slotState is what the source can tell us about the slot. 「存在」与「能不能重放」
// 是**两件不同的事**，混成一个 bool 会出人命：
//   - 建/删书签只看「存在」：已经 wal_status=lost 的书签仍然占着名字，不先 drop 就
//     pg_create_logical_replication_slot 会报 already exists —— 于是每次 run 都失败，
//     而且不能靠重跑自愈；
//   - 决定「能不能从旧位点续」才看 wal_status：lost 表示 WAL 已被
//     max_slot_wal_keep_size 丢掉，从任何旧位点都重放不了。
type slotState struct {
	// LSN 是 confirmed_flush_lsn 原文（空 = 书签还没确认过任何位点）。
	LSN string
	// Exists 表示 pg_replication_slots 里有这个书签（lost 也算存在）。
	Exists bool
	// WalStatus 是 PG 13+ 的 wal_status（reserved / extended / unreserved / lost）。
	// 老版本取不到，留空——那时它也没有「丢 WAL」这个状态。
	WalStatus string
}

// Replayable reports whether changes can still be replayed from an old checkpoint.
func (st slotState) Replayable() bool { return st.Exists && st.WalStatus != "lost" }

// slotState reads the slot's raw state.
func (s *Source) slotState(ctx context.Context) (slotState, error) {
	var lsn, walStatus *string
	// wal_status 是 PG 13+ 的列：用 to_jsonb 取，老版本取不到就是 NULL（不写版本号分支）。
	err := s.conn.QueryRow(ctx,
		`SELECT confirmed_flush_lsn::text, to_jsonb(s)->>'wal_status'
		   FROM pg_replication_slots s WHERE slot_name=$1`,
		s.opt.Slot).Scan(&lsn, &walStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		return slotState{}, nil
	}
	if err != nil {
		return slotState{}, err
	}
	st := slotState{Exists: true}
	if lsn != nil {
		st.LSN = *lsn
	}
	if walStatus != nil {
		st.WalStatus = *walStatus
	}
	return st, nil
}

// slotPosition keeps the two-value shape for callers that only care about
// existence (建/删书签), not about replayability.
func (s *Source) slotPosition(ctx context.Context) (string, bool, error) {
	st, err := s.slotState(ctx)
	return st.LSN, st.Exists, err
}

// SlotConfirmed reports the slot's confirmed position, i.e. how far the source
// has already been acknowledged. ok=false 表示「重放不了」——书签没了、或者
// wal_status=lost（WAL 已被 max_slot_wal_keep_size 丢掉），从任何旧位点都追不回来。
//
// 注意这**不是**「书签不存在」：判断存在与否请用 slotState（建/删书签要用它）。
func (s *Source) SlotConfirmed(ctx context.Context) (Position, bool, error) {
	st, err := s.slotState(ctx)
	if err != nil || !st.Replayable() {
		return Position{}, false, err
	}
	if st.LSN == "" {
		return Position{}, true, nil // 书签在，但还没确认过任何位点
	}
	pos, err := ParsePayload(st.LSN)
	if err != nil {
		return Position{}, true, err
	}
	return pos, true, nil
}

// SlotLagBytes reports how much WAL the slot is holding back (guard metric).
func (s *Source) SlotLagBytes(ctx context.Context) (int64, error) {
	var n int64
	err := s.conn.QueryRow(ctx,
		`SELECT COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint, 0)
		 FROM pg_replication_slots WHERE slot_name=$1`, s.opt.Slot).Scan(&n)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return n, err
}

// ── 全量：分块读（块 = 一个目标事务） ─────────────────────────

// Chunk is one resumable unit of the initial copy.
//
// Rows 里是**驱动原生值**（pgx 已经解好的 int64/time.Time/[]byte/…），不是
// source.Value：全量这条路径不该把 numeric/uuid/数组先降级成字符串再还原，
// 而且共享契约（internal/source/contract.go）由 Ark 统一维护，我不往里加字段。
type Chunk struct {
	Table source.TableRef
	No    int
	Rows  [][]any
	From  string // 本块起（不含）的 keyset 游标
	To    string // 本块止（含）的 keyset 游标 = 下一块的 From，也是续跑的锚点
}

// ChunkHandler consumes one chunk; returning an error aborts the snapshot.
type ChunkHandler func(ctx context.Context, c Chunk) error

// ChunkResume is where one table continues reading.
//
// 它全部来自目标库的块标记（`_jl_chunk.range_to` + `chunk_no`），源端不存任何进度：
// 换一台机器、换一个进程，只要连到同一个目标库就能接着搬。
type ChunkResume struct {
	// Cursor is the right edge of the last committed chunk (exclusive).
	// 空 = 从表头读。
	Cursor string
	// LastNo is the last committed chunk number; the next chunk is LastNo+1.
	// 编号必须接着往上排，否则块标记会在主键上撞车、把旧块的边界覆盖掉。
	LastNo int
}

// SnapshotChunks reads the selected tables through the exported snapshot and
// hands them over chunk by chunk. Every chunk carries a stable boundary so a
// restart can tell where it stopped (docs/SNAPSHOT.md §4).
func (s *Source) SnapshotChunks(ctx context.Context, opt source.SnapshotOpt, fn ChunkHandler) (Position, error) {
	return s.SnapshotChunksFrom(ctx, opt, nil, fn)
}

// SnapshotChunksFrom is SnapshotChunks with a per-table resume point.
//
// resume 里没有的表从头读；有的话就从那一块之后接着读，块号接着旧编号往上排。
// 两种情况用的是**本事务自己的**一致性读视图：续跑时导出的快照早已随进程消失，
// 剩下的块读的是一个更晚的视图——这不会丢变更，因为增量仍然从原来的位点重放
// （见 engine.planCopy 与本文件上层说明）。
func (s *Source) SnapshotChunksFrom(ctx context.Context, opt source.SnapshotOpt,
	resume map[string]ChunkResume, fn ChunkHandler) (Position, error) {
	if opt.ChunkRows <= 0 {
		opt.ChunkRows = 100000
	}
	// 独立的读连接：SET TRANSACTION SNAPSHOT 只作用于本事务，不能与控制用连接混用。
	conn, err := pgwire.Connect(ctx, s.opt.Source)
	if err != nil {
		return Position{}, err
	}
	defer conn.Close(context.Background())

	if _, err := conn.Exec(ctx, `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY`); err != nil {
		return Position{}, fmt.Errorf("开始一致性读事务失败: %w", err)
	}
	if s.snapshotName != "" {
		if _, err := conn.Exec(ctx, "SET TRANSACTION SNAPSHOT '"+strings.ReplaceAll(s.snapshotName, "'", "''")+"'"); err != nil {
			return Position{}, fmt.Errorf("使用导出的快照失败（可能已失效）: %w", err)
		}
	}
	defer conn.Exec(context.Background(), "ROLLBACK") //nolint:errcheck

	var pos Position
	for _, key := range s.order {
		ti := s.tables[key]
		from := resume[ti.ref.Table]
		no := from.LastNo
		cursor := from.Cursor
		for {
			if err := ctx.Err(); err != nil {
				return pos, err
			}
			rows, next, err := s.readPage(ctx, conn, ti, cursor, opt.ChunkRows)
			if err != nil {
				return pos, err
			}
			if len(rows) == 0 {
				break
			}
			no++
			if err := fn(ctx, Chunk{Table: ti.ref, No: no, Rows: rows, From: cursor, To: next}); err != nil {
				return pos, err
			}
			cursor = next
			if len(rows) < int(opt.ChunkRows) {
				break
			}
		}
	}
	return pos, nil
}

// readPage reads one page with keyset pagination (never OFFSET: it degrades to
// a full scan per page on big tables). No-PK tables use ctid, which is stable
// inside the repeatable-read snapshot.
func (s *Source) readPage(ctx context.Context, conn *pgx.Conn, ti *tableInfo,
	cursor string, limit int64) ([][]any, string, error) {

	noPK := len(ti.pk) == 0
	cols := make([]string, 0, len(ti.cols)+1)
	if noPK {
		// ctid 作为游标列一起取回：无主键表只能按物理位置切块。
		cols = append(cols, "ctid::text")
	}
	for _, c := range ti.cols {
		cols = append(cols, pgwire.Quote(c))
	}
	sel := "SELECT " + strings.Join(cols, ", ") + " FROM " + pgwire.Qualified(ti.ref.Schema, ti.ref.Table)

	var args []any
	switch {
	case noPK:
		// 无主键：按 ctid 游标（只能表级重来，预检已警告过成本）
		if cursor != "" {
			sel += " WHERE ctid > $1::text::tid"
			args = append(args, cursor)
		}
		sel += fmt.Sprintf(" ORDER BY ctid LIMIT %d", limit)
	case len(ti.pk) == 1:
		if cursor != "" {
			idx := ti.colIndex[ti.pk[0]]
			sel += fmt.Sprintf(" WHERE %s > $1::text::%s", pgwire.Quote(ti.pk[0]), ti.types[idx])
			args = append(args, cursor)
		}
		sel += fmt.Sprintf(" ORDER BY %s LIMIT %d", pgwire.Quote(ti.pk[0]), limit)
	default:
		var cs []string
		for _, p := range ti.pk {
			cs = append(cs, pgwire.Quote(p))
		}
		if cursor != "" {
			var holders []string
			for i, p := range ti.pk {
				idx := ti.colIndex[p]
				holders = append(holders, fmt.Sprintf("$%d::text::%s", i+1, ti.types[idx]))
			}
			sel += fmt.Sprintf(" WHERE (%s) > (%s)", strings.Join(cs, ", "), strings.Join(holders, ", "))
			args = append(args, splitCursor(cursor)...)
		}
		sel += fmt.Sprintf(" ORDER BY %s LIMIT %d", strings.Join(cs, ", "), limit)
	}

	rows, err := conn.Query(ctx, sel, args...)
	if err != nil {
		return nil, "", fmt.Errorf("读取 %s 失败: %w", ti.ref.Table, err)
	}
	defer rows.Close()

	var out [][]any
	var lastVals []any
	var lastCtid string
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, "", err
		}
		offset := 0
		if noPK {
			if len(vals) == 0 {
				continue
			}
			lastCtid, _ = vals[0].(string)
			offset = 1
		}
		out = append(out, vals[offset:])
		if !noPK {
			lastVals = vals
		}
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	if noPK {
		return out, lastCtid, nil
	}
	if len(lastVals) == 0 {
		return out, "", nil
	}
	parts := make([]string, 0, len(ti.pk))
	for _, p := range ti.pk {
		idx := ti.colIndex[p]
		if idx >= len(lastVals) {
			return nil, "", fmt.Errorf("内部错误：%s 的游标列 %s 不在读取结果里", ti.ref.Table, p)
		}
		parts = append(parts, cursorText(lastVals[idx]))
	}
	return out, joinCursor(parts), nil
}

// cursorText renders a cursor value so it can be parsed back by the column's
// input function ($1::text::<type>).
func cursorText(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return "\\x" + hex.EncodeToString(x)
	case time.Time:
		return x.Format("2006-01-02 15:04:05.999999-07")
	case bool:
		if x {
			return "t"
		}
		return "f"
	case int16:
		return strconv.FormatInt(int64(x), 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int64:
		return strconv.FormatInt(x, 10)
	case float32:
		return strconv.FormatFloat(float64(x), 'g', -1, 32)
	case float64:
		return strconv.FormatFloat(x, 'g', -1, 64)
	default:
		return fmt.Sprint(x)
	}
}

// joinCursor encodes composite keyset boundaries. \x1f cannot appear in the
// text forms we produce (numbers, timestamps, uuids, escaped bytea).
func joinCursor(parts []string) string { return strings.Join(parts, "\x1f") }

func splitCursor(s string) []any {
	parts := strings.Split(s, "\x1f")
	out := make([]any, len(parts))
	for i, p := range parts {
		out[i] = p
	}
	return out
}

// SetSnapshotName records the consistency view handed out by Prepare.
func (s *Source) SetSnapshotName(name string) { s.snapshotName = name }

// EndSnapshot releases the replication connection that owns the exported
// snapshot. Called as soon as the last chunk is read: the stream opens its own
// replication connection and there is no point holding two.
func (s *Source) EndSnapshot() {
	if s.snapConn != nil {
		_ = s.snapConn.Close(context.Background())
		s.snapConn = nil
	}
}

// ── 增量：变更流 ──────────────────────────────────────────────

// Relation caches one published table's column layout.
type relation struct {
	ref     source.TableRef
	columns []string
	// mapTo is, per relation column, the index in the catalog column order.
	mapTo []int
	// table is this version of the column definition, handed to the engine on
	// every event (Event.Relation). 同一版定义复用同一个指针：指针相等即同一版。
	table *source.Table
}

// Stream consumes the change stream from pos until ctx is cancelled.
func (s *Source) Stream(ctx context.Context, from Position, onEvent source.EventHandler) error {
	rconn, err := pgwire.ConnectReplication(ctx, s.opt.Source)
	if err != nil {
		return err
	}
	defer rconn.Close(context.Background())
	pc := rconn.PgConn()

	s.ackLSN.Store(uint64(from.LSN))
	err = pglogrepl.StartReplication(ctx, pc, s.opt.Slot, from.LSN, pglogrepl.StartReplicationOptions{
		Mode:       pglogrepl.LogicalReplication,
		PluginArgs: []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", s.opt.Publication)},
	})
	if err != nil {
		return fmt.Errorf("启动变更流失败（书签可能已被清理）: %w", err)
	}

	rels := map[uint32]*relation{}
	standbyDeadline := time.Now().Add(time.Second)
	var inTx bool
	var curTx string // 当前事务的 TxID：数据事件也要带上，引擎据此认「续段」

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		// 每秒回一次状态：既是心跳，也是把「已落库的位点」告诉源库，
		// 源库据此释放变更流水（docs/STREAMING.md §3）。
		if time.Now().After(standbyDeadline) {
			if err := s.sendStandby(pc); err != nil {
				return err
			}
			standbyDeadline = time.Now().Add(time.Second)
		}
		recvCtx, cancel := context.WithDeadline(ctx, standbyDeadline)
		raw, err := pc.ReceiveMessage(recvCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			var netErr interface{ Timeout() bool }
			if errors.As(err, &netErr) && netErr.Timeout() {
				continue // 到点了：回去发心跳
			}
			if pgwire.IsRetryable(err) {
				return &RetryableError{Err: fmt.Errorf("变更流断开: %w", err)}
			}
			return fmt.Errorf("读变更流失败: %w", err)
		}

		switch m := raw.(type) {
		case *pgproto3.CopyData:
			if len(m.Data) == 0 || m.Data[0] != pglogrepl.PrimaryKeepaliveMessageByteID {
				if len(m.Data) > 0 && m.Data[0] == pglogrepl.XLogDataByteID {
					xld, err := pglogrepl.ParseXLogData(m.Data[1:])
					if err != nil {
						return fmt.Errorf("解析变更数据失败: %w", err)
					}
					s.serverEnd.Store(uint64(xld.ServerWALEnd))
					if err := s.handleWAL(ctx, xld.WALData, xld.WALStart, rels, &inTx, &curTx, onEvent); err != nil {
						return err
					}
				}
				continue
			}
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(m.Data[1:])
			if err != nil {
				return fmt.Errorf("解析心跳失败: %w", err)
			}
			s.serverEnd.Store(uint64(pkm.ServerWALEnd))
			s.keepPrev.Store(s.keepEnd.Swap(uint64(pkm.ServerWALEnd)))
			if pkm.ReplyRequested {
				if err := s.sendStandby(pc); err != nil {
					return err
				}
			}
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("源库报错（%s）: %s", m.Code, m.Message)
		}
	}
}

// sendStandby reports the durably applied position back to the source and asks
// for an immediate keepalive, so we always know the server's WAL end (that is
// how "caught up" is decided, and how lag is measured).
func (s *Source) sendStandby(pc *pgconn.PgConn) error {
	applied := pglogrepl.LSN(s.ackLSN.Load())
	return pglogrepl.SendStandbyStatusUpdate(context.Background(), pc, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: applied,
		WALFlushPosition: applied,
		WALApplyPosition: applied,
		ReplyRequested:   true,
	})
}

// SafeAckPoint returns a position we may advance to without decoding anything
// further: it is only offered when two consecutive keepalives reported the same
// end, which means the walsender has sent everything it decoded and is idle.
//
// 为什么需要它：源库长时间没有「我们关心的表」的变更时（比如同步 1 张表、源库
// 在写别的表），一个数据事件都不会发过来，位点就永远不前进 —— 书签占的 WAL
// 只增不减，磁盘会被慢慢吃光。这是唯一能安全推进的情形。
func (s *Source) SafeAckPoint() (Position, bool) {
	end, prev := s.keepEnd.Load(), s.keepPrev.Load()
	if end == 0 || end != prev {
		return Position{}, false
	}
	return Position{LSN: pglogrepl.LSN(end)}, true
}

// ServerWALEnd is the source's WAL end as last reported by the stream.
func (s *Source) ServerWALEnd() Position { return Position{LSN: pglogrepl.LSN(s.serverEnd.Load())} }

// Ack records the position that the target has committed. Only the engine calls
// it, and only after a successful commit (never before).
func (s *Source) Ack(pos Position) {
	for {
		cur := s.ackLSN.Load()
		if uint64(pos.LSN) <= cur {
			return
		}
		if s.ackLSN.CompareAndSwap(cur, uint64(pos.LSN)) {
			return
		}
	}
}

// LastCommit is the LSN of the most recent committed source transaction.
// The engine snapshots it at flush time, so the position it writes is always
// "the last transaction included in this batch".
func (s *Source) LastCommit() Position { return Position{LSN: pglogrepl.LSN(s.lastCommit.Load())} }

// AckedPosition reports what the source has been told we applied.
func (s *Source) AckedPosition() Position { return Position{LSN: pglogrepl.LSN(s.ackLSN.Load())} }

func (s *Source) handleWAL(ctx context.Context, data []byte, walStart pglogrepl.LSN,
	rels map[uint32]*relation, inTx *bool, curTx *string, onEvent source.EventHandler) error {

	msg, err := pglogrepl.Parse(data)
	if err != nil {
		return fmt.Errorf("解码变更失败: %w", err)
	}
	switch m := msg.(type) {
	case *pglogrepl.RelationMessage:
		rel, err := s.buildRelation(m)
		if err != nil {
			return err
		}
		rels[m.RelationID] = rel
	case *pglogrepl.BeginMessage:
		*inTx = true
		*curTx = fmt.Sprint(m.Xid)
		return onEvent(ctx, source.Event{Kind: source.EvBegin, TxID: *curTx, CommitTS: m.CommitTime})
	case *pglogrepl.CommitMessage:
		*inTx = false
		pos := Position{LSN: m.TransactionEndLSN}
		if m.TransactionEndLSN == 0 {
			pos = Position{LSN: m.CommitLSN}
		}
		s.lastCommit.Store(uint64(pos.LSN))
		s.Ack(pos)
		txID := *curTx
		*curTx = ""
		return onEvent(ctx, source.Event{
			Kind:     source.EvCommit,
			TxID:     txID,
			CommitTS: m.CommitTime,
		})
	case *pglogrepl.InsertMessage:
		rel := rels[m.RelationID]
		if rel == nil {
			return fmt.Errorf("收到未描述的表变更（发布与任务范围不一致）")
		}
		row, err := rel.row(m.Tuple)
		if err != nil {
			return err
		}
		return onEvent(ctx, source.Event{Kind: source.EvInsert, Table: rel.ref, Relation: rel.table,
			TxID: *curTx, New: row})
	case *pglogrepl.UpdateMessage:
		rel := rels[m.RelationID]
		if rel == nil {
			return fmt.Errorf("收到未描述的表变更（发布与任务范围不一致）")
		}
		row, err := rel.row(m.NewTuple)
		if err != nil {
			return err
		}
		old, err := rel.row(m.OldTuple)
		if err != nil {
			return err
		}
		return onEvent(ctx, source.Event{Kind: source.EvUpdate, Table: rel.ref, Relation: rel.table,
			TxID: *curTx, New: row, Old: old})
	case *pglogrepl.DeleteMessage:
		rel := rels[m.RelationID]
		if rel == nil {
			return fmt.Errorf("收到未描述的表变更（发布与任务范围不一致）")
		}
		old, err := rel.row(m.OldTuple)
		if err != nil {
			return err
		}
		return onEvent(ctx, source.Event{Kind: source.EvDelete, Table: rel.ref, Relation: rel.table,
			TxID: *curTx, Old: old})
	case *pglogrepl.TruncateMessage:
		for _, r := range m.RelationIDs {
			if rel := rels[r]; rel != nil {
				if err := onEvent(ctx, source.Event{Kind: source.EvTruncate, Table: rel.ref, Relation: rel.table,
					TxID: *curTx}); err != nil {
					return err
				}
			}
		}
	case *pglogrepl.TypeMessage, *pglogrepl.OriginMessage:
		// 与我们无关：类型元数据与复制源标识
	}
	return nil
}

// buildRelation maps the published relation onto the catalog column order.
func (s *Source) buildRelation(m *pglogrepl.RelationMessage) (*relation, error) {
	key := m.Namespace + "." + m.RelationName
	ti := s.tables[key]
	if ti == nil {
		return nil, fmt.Errorf("发布里出现了任务范围外的表 %s（发布和任务不同步，请重新跑一次）", key)
	}
	rel := &relation{ref: ti.ref}
	tbl := &source.Table{
		TableRef:    ti.ref,
		PrimaryKey:  append([]string(nil), ti.pk...),
		HasStablePK: len(ti.pk) > 0,
	}
	for _, col := range m.Columns {
		idx, ok := ti.colIndex[col.Name]
		if !ok {
			return nil, fmt.Errorf("源表 %s 多了列 %s：表结构在同步中途变了（白名单只允许加列，需重新预检）", key, col.Name)
		}
		rel.columns = append(rel.columns, col.Name)
		rel.mapTo = append(rel.mapTo, idx)
		typ, coll := "", ""
		if idx < len(ti.types) {
			typ = ti.types[idx]
		}
		if idx < len(ti.collations) {
			coll = ti.collations[idx]
		}
		tbl.Columns = append(tbl.Columns, source.Column{Name: col.Name, Type: typ, Collation: coll})
	}
	rel.table = tbl
	return rel, nil
}

// row expands one pgoutput tuple into catalog column order. Columns the source
// did not send (TOASTed unchanged values, or a key-only old tuple) stay NULL;
// the sink only reads the columns it needs for that event kind.
func (r *relation) row(t *pglogrepl.TupleData) (source.Row, error) {
	row := source.Row{Table: r.ref}
	if t == nil {
		return row, nil
	}
	max := 0
	for _, i := range r.mapTo {
		if i+1 > max {
			max = i + 1
		}
	}
	row.Values = make([]source.Value, max)
	for i, col := range t.Columns {
		if i >= len(r.mapTo) {
			break
		}
		idx := r.mapTo[i]
		if idx >= len(row.Values) {
			continue
		}
		switch col.DataType {
		case pglogrepl.TupleDataTypeNull, pglogrepl.TupleDataTypeToast:
			row.Values[idx] = source.Value{Kind: source.KindNull}
		default:
			row.Values[idx] = source.Value{Kind: source.KindString, S: string(col.Data)}
		}
	}
	return row, nil
}

// 位点不连续（E3001）**只能**由位点专属信号判定。
//
// 纪律（2026-11-06 裁决）：**禁止用泛化子串匹配错误类别**。这里的匹配表曾经有裸的
// "does not exist"，于是 `schema "x" does not exist`（SQLSTATE 3F000）会被判成
// 「进度不连续」，把用户引去 `--resync` 重搬全量——而真实原因是 schema 名字写错了。
// 加新条件前先问：这个信号**只**可能来自位点/变更流水吗？答不上来就别加。
func IsPositionGap(err error) bool {
	if err == nil {
		return false
	}
	if pgwire.SQLState(err) == "58P01" { // undefined_file：WAL 段已被删除
		return true
	}
	msg := strings.ToLower(err.Error())
	// 每条都必须同时出现「位点对象」和「失效动作」两类词，缺一不算。
	pairs := [][2]string{
		{"replication slot", "invalidated"},
		{"replication slot", "does not exist"},
		{"replication slot", "not found"},
		{"replication slot", "has been dropped"},
		{"requested wal segment", "has already been removed"},
		{"could not find wal", ""},
	}
	for _, p := range pairs {
		if strings.Contains(msg, p[0]) && (p[1] == "" || strings.Contains(msg, p[1])) {
			return true
		}
	}
	return false
}

// IsWalLevelTooLow reports「源库没开逻辑变更流」（E1001）。
// PG 在跑起来之后才撞上这个条件时抛 SQLSTATE 55000 + 消息含 wal_level。
// 码表的单位是「用户要做什么」而不是「在哪个阶段发现」，所以复用 E1001：
// 预检撞上要做的事与运行期撞上是同一件（ALTER SYSTEM + 重启）。
func IsWalLevelTooLow(err error) bool {
	if err == nil {
		return false
	}
	if pgwire.SQLState(err) != "55000" && pgwire.SQLState(err) != "0A000" {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "wal_level")
}

// IsNoPrivilege reports「账号没有被授予这个动作」（SQLSTATE 42501）。
func IsNoPrivilege(err error) bool {
	return err != nil && pgwire.SQLState(err) == "42501"
}

// IsObjectMissing reports「schema/表/列不存在」（3F000 / 42P01 / 42703）。
func IsObjectMissing(err error) bool {
	switch pgwire.SQLState(err) {
	case "3F000", "42P01", "42703":
		return true
	}
	return false
}

// RetryableError marks a failure the engine should retry with backoff.
type RetryableError struct{ Err error }

// Error implements error.
func (e *RetryableError) Error() string { return e.Err.Error() }

// Unwrap exposes the cause.
func (e *RetryableError) Unwrap() error { return e.Err }

// ── 校验与清理 ───────────────────────────────────────────────

// Count returns the source row count of one table.
func (s *Source) Count(ctx context.Context, ref source.TableRef) (int64, error) {
	var n int64
	err := s.conn.QueryRow(ctx, "SELECT count(*) FROM "+pgwire.Qualified(ref.Schema, ref.Table)).Scan(&n)
	return n, err
}

// Fingerprint mirrors pgsink.Fingerprint so both sides can be compared.
func (s *Source) Fingerprint(ctx context.Context, ref source.TableRef) (string, error) {
	q := fmt.Sprintf(`SELECT count(*), COALESCE(sum(hashtextextended(t::text, 0)), 0)::text
		FROM %s t`, pgwire.Qualified(ref.Schema, ref.Table))
	var n int64
	var sum string
	if err := s.conn.QueryRow(ctx, q).Scan(&n, &sum); err != nil {
		return "", err
	}
	return fmt.Sprintf("%d:%s", n, sum), nil
}

// Tables returns the selected tables in catalog order.
func (s *Source) Tables() []source.TableRef {
	out := make([]source.TableRef, 0, len(s.order))
	for _, key := range s.order {
		out = append(out, s.tables[key].ref)
	}
	return out
}

// Advance moves the slot forward without a running stream (used after a clean
// shutdown and by gc). It never moves backwards.
func (s *Source) Advance(ctx context.Context, to Position) error {
	if to.IsZero() {
		return nil
	}
	// 书签通常已经通过正常确认走得更远了（安静期的安全确认点可能领先于最后一批
	// 数据）。这时 pg_replication_slot_advance 会报「不能后退」，不是错误。
	cur, exists, err := s.slotPosition(ctx)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if cur != "" {
		if c, err := ParsePayload(cur); err == nil && to.Compare(c) <= 0 {
			return nil
		}
	}
	_, err = s.conn.Exec(ctx, `SELECT pg_replication_slot_advance($1, $2::pg_lsn)`, s.opt.Slot, to.String())
	if err != nil {
		return fmt.Errorf("推进同步书签失败: %w", err)
	}
	return nil
}

// Cleanup drops the objects this job created. It is only called when the job is
// deleted — stopping a run keeps the slot so the next run can resume.
func (s *Source) Cleanup(ctx context.Context) ([]source.Action, error) {
	var actions []source.Action
	// 只要书签在就删，**不看**能不能重放：lost 的书签同样占着
	// max_replication_slots 的名额，留着就是孤儿，只能人工清理。
	st, err := s.slotState(ctx)
	if err != nil {
		return actions, err
	}
	slotExists := st.Exists
	if slotExists {
		if _, err := s.conn.Exec(ctx, `SELECT pg_drop_replication_slot($1)`, s.opt.Slot); err != nil {
			return actions, fmt.Errorf("删除书签 %s 失败（可能还有进程连着它）: %w", s.opt.Slot, err)
		}
		actions = append(actions, source.Action{
			ID: "drop-slot:" + s.opt.Slot, What: "删了同步书签 " + s.opt.Slot,
			SQL: "SELECT pg_drop_replication_slot('" + s.opt.Slot + "')", Applied: true,
			Reason: "任务已被删除，书签继续保留只会白占磁盘与 max_replication_slots 名额",
		})
	}
	exists, err := s.publicationExists(ctx)
	if err != nil {
		return actions, err
	}
	if exists {
		if _, err := s.conn.Exec(ctx, "DROP PUBLICATION "+pgwire.Quote(s.opt.Publication)); err != nil {
			return actions, fmt.Errorf("删除发布 %s 失败: %w", s.opt.Publication, err)
		}
		actions = append(actions, source.Action{
			ID: "drop-publication:" + s.opt.Publication, What: "删了发布 " + s.opt.Publication,
			SQL: "DROP PUBLICATION " + pgwire.Quote(s.opt.Publication), Applied: true,
			Reason: "任务已被删除",
		})
	}
	return actions, nil
}
