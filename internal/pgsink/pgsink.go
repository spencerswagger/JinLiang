// Package pgsink writes into a PostgreSQL target: the `_jl_*` runtime state
// tables, the target tables themselves, and — the hard rule of this product —
// data and position in one transaction (docs/STATE-SCHEMA.md §2).
//
// 为什么位点必须和数据同一个事务：位点先落会丢（崩溃后跳过没写进去的行），
// 位点后落会重（崩溃后重放已提交的批）。只有同事务才是既不重也不丢，并且
// 这让「重放 = 幂等」不需要任何外部协调器。
package pgsink

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/spencerswagger/jinliang/internal/catalog"
	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/pgwire"
	"github.com/spencerswagger/jinliang/internal/source"
)

// SchemaVersion is the runtime state-table layout this binary writes.
const SchemaVersion = 1

// ErrNoSourceCounter means Verify was asked to compare but no source-side
// reader was injected (the B0 sink.Sink contract gives the sink no source).
var ErrNoSourceCounter = errors.New("没有注入源端计数器，无法比对")

// Options configures a target.
type Options struct {
	Target     dsn.Endpoint
	JobKey     string
	JobName    string
	InstanceID string
	// Schema is where target tables and `_jl_*` live (default public).
	Schema string
	// OnExists mirrors target.on_exists: error | append | recreate.
	OnExists string
	// Catalog is the source table list the job selected; it defines the
	// canonical column order every Row follows.
	Catalog source.Catalog
	// SourceKind 是源端的引擎类型（postgres / mysql）。
	//
	// 为什么目标端要知道：跨引擎时列类型文本是源端的（`datetime(6)`、
	// `bigint unsigned`、`tinyint`），直接拼进 CREATE TABLE 会被 PG 拒掉
	// （`type "datetime" does not exist`）。类型字典只有一份（internal/catalog），
	// 这里按它翻成 PG 类型；同引擎（空或 postgres）原样使用，行为不变。
	SourceKind catalog.Engine
	// OnDDLSkipped 在「DDL 因目标端已有同名对象而被跳过」时回调（引擎用它发一条 note）。
	// 跳过是最需要留痕的一类结果：失败会有人看，跳过不会。
	OnDDLSkipped func(stmt string)

	// ApplyBatchRows 是一条多行 upsert 里最多放几行（默认 500）。
	//
	// 逐行 Exec 时 500 行/事务 = 500 次往返，增量吞吐卡在 ~10.5k 行/s（稳态线）；
	// 拼成多行 INSERT … ON CONFLICT 实测能到 80k+。这里做成可配是因为「最佳批大小」
	// 随行宽与网络 RTT 变：宽表/高延迟链路调小更稳（单条 SQL 太长会被 planner 罚）。
	ApplyBatchRows int
}

// DefaultApplyBatchRows 是一条多行 upsert 的默认行数。
const DefaultApplyBatchRows = 500

type tableInfo struct {
	ref   source.TableRef
	cols  []string
	types []string
	// collations[i] 是第 i 列的排序规则（空 = 用类型/库的默认）。跨库必须带上：
	// 排序规则不同会让唯一索引判重与 ORDER BY 结果悄悄变样。
	collations []string
	pk         []string
	pkIdx      []int
	hasPK      bool
	colIndex   map[string]int
}

// Sink is the PostgreSQL write side. It owns one dedicated connection: writes
// are single-threaded by design (docs/STREAMING.md §1), and position+data
// atomicity is easier to reason about without a pool.
type Sink struct {
	opt  Options
	conn *pgx.Conn
	// mu serialises every use of conn. pgx connections are not safe for
	// concurrent use, and the lease-renewal goroutine writes while the data
	// path is busy ("conn busy" otherwise).
	mu     sync.Mutex
	tables map[string]*tableInfo
	order  []string

	// ownerToken 让「同一个进程里的两个 Sink」也彼此可分（否则它们同 pid、
	// 同 instance_id，第二个会把第一个的租约抢走）。
	ownerToken string

	// CountSource / FingerprintSource are injected by the engine so Verify can
	// compare both sides without this package importing the source.
	CountSource       func(ctx context.Context, ref source.TableRef) (int64, error)
	FingerprintSource func(ctx context.Context, ref source.TableRef) (string, error)
}

// Meta is the authoritative runtime state of one job, read back from the target.
type Meta struct {
	JobKey     string
	Name       string
	InstanceID string
	Phase      string
	Position   string
	SnapshotID string
	UpdatedAt  time.Time
}

// Open dials the target and makes sure the state tables exist.
func Open(ctx context.Context, opt Options) (*Sink, error) {
	if opt.Schema == "" {
		opt.Schema = "public"
	}
	if opt.OnExists == "" {
		opt.OnExists = "error"
	}
	if opt.ApplyBatchRows <= 0 {
		// 环境变量兜底：引擎还没把 job 配置传进来时也能调（运维侧热调批大小）。
		if v := os.Getenv("JINLIANG_APPLY_BATCH_ROWS"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				opt.ApplyBatchRows = n
			}
		}
	}
	if err := checkColumnBudget(opt.Catalog); err != nil {
		return nil, err
	}
	tables, order := buildTables(opt.Catalog, opt.SourceKind)
	s := &Sink{opt: opt, tables: tables, order: order, ownerToken: newOwnerToken()}
	conn, err := pgwire.Connect(ctx, opt.Target)
	if err != nil {
		return nil, err
	}
	s.conn = conn
	if err := s.ensureStateTables(ctx); err != nil {
		conn.Close(ctx)
		return nil, err
	}
	return s, nil
}

// maxPGColumnsPerTable 是 PostgreSQL 单表列数上限（真实边界：建表会被拒）。
const maxPGColumnsPerTable = 1600

// checkColumnBudget 在建连之前拦「列太多」：这是**真实会撞**的边界（宽表 + 我们自动建表），
// 提前拦比让它在写入时炸成一句驱动原始错误好。
func checkColumnBudget(cat source.Catalog) error {
	for _, t := range cat.Tables {
		if len(t.Columns) <= maxPGColumnsPerTable {
			continue
		}
		return fmt.Errorf("表 %s 有 %d 列，超过 PostgreSQL 单表 %d 列的上限。"+
			"你能做的：① 把不需要同步的列写进 content.tables.%s.columns.exclude；"+
			"② 拆分宽表后重新选表。风险：列数超限时建表/写入都会被数据库拒绝，且报错与数据无关",
			t.Table, len(t.Columns), maxPGColumnsPerTable, t.Table)
	}
	return nil
}

// buildTables indexes the catalog by "schema.table" and keeps a stable order.
func buildTables(cat source.Catalog, srcKind catalog.Engine) (map[string]*tableInfo, []string) {
	tables := map[string]*tableInfo{}
	var order []string
	for _, t := range cat.Tables {
		ti := &tableInfo{
			ref:      t.TableRef,
			pk:       append([]string(nil), t.PrimaryKey...),
			hasPK:    len(t.PrimaryKey) > 0,
			colIndex: map[string]int{},
		}
		for _, c := range t.Columns {
			ti.colIndex[c.Name] = len(ti.cols)
			ti.cols = append(ti.cols, c.Name)
			ti.types = append(ti.types, targetType(srcKind, c.Type))
			ti.collations = append(ti.collations, c.Collation)
		}
		for _, name := range ti.pk {
			if idx, ok := ti.colIndex[name]; ok {
				ti.pkIdx = append(ti.pkIdx, idx)
			}
		}
		if len(ti.pkIdx) != len(ti.pk) {
			ti.hasPK = false // 主键列不在列表里：按无主键处理（insert-only）
		}
		key := t.Schema + "." + t.Table
		tables[key] = ti
		order = append(order, key)
	}
	return tables, order
}

// targetType 把源端列类型翻成 PG 类型（跨引擎时）；字典没覆盖的原样返回，
// 让数据库自己报错——**不猜**一个可能更窄的类型把数据悄悄改掉。
func targetType(src catalog.Engine, t string) string {
	if src == "" || src == catalog.Postgres {
		return t
	}
	if m, ok := catalog.Map(src, catalog.Postgres, t); ok && m.Target != "" {
		return m.Target
	}
	return t
}

// Close releases the connection.
func (s *Sink) Close() error {
	if s.conn != nil {
		return s.conn.Close(context.Background())
	}
	return nil
}

// Kind implements the sink contract.
func (s *Sink) Kind() string { return dsn.KindPostgres }

func (s *Sink) schema() string { return pgwire.Quote(s.opt.Schema) }

func (s *Sink) stateTable(name string) string {
	return s.schema() + "." + pgwire.Quote(name)
}

func (s *Sink) ensureStateTables(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ddl := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			job_key        text PRIMARY KEY,
			name           text NOT NULL,
			instance_id    text NOT NULL,
			schema_version int  NOT NULL DEFAULT %d,
			job_hash       text NOT NULL DEFAULT '',
			schema_hash    text,
			phase          text NOT NULL,
			position       text,
			watermark      text,
			snapshot_id    text,
			started_at     timestamptz NOT NULL DEFAULT now(),
			updated_at     timestamptz NOT NULL DEFAULT now()
		)`, s.stateTable("_jl_meta"), SchemaVersion),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			job_key     text PRIMARY KEY,
			instance_id text NOT NULL,
			label       text,
			expires_at  timestamptz NOT NULL
		)`, s.stateTable("_jl_lease")),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			job_key    text NOT NULL,
			table_name text NOT NULL,
			chunk_no   int  NOT NULL,
			range_from text,
			range_to   text,
			done       boolean NOT NULL DEFAULT false,
			rows       bigint  NOT NULL DEFAULT 0,
			PRIMARY KEY (job_key, table_name, chunk_no)
		)`, s.stateTable("_jl_chunk")),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			object_name text PRIMARY KEY,
			object_kind text NOT NULL,
			job_key     text NOT NULL,
			instance_id text NOT NULL,
			created_at  timestamptz NOT NULL DEFAULT now(),
			dropped_at  timestamptz
		)`, s.stateTable("_jl_object")),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id         bigserial PRIMARY KEY,
			job_key    text NOT NULL,
			table_name text NOT NULL,
			pk         text NOT NULL,
			policy     text NOT NULL,
			source_row jsonb,
			target_row jsonb,
			resolved   boolean NOT NULL DEFAULT false,
			created_at timestamptz NOT NULL DEFAULT now()
		)`, s.stateTable("_jl_conflict")),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id         bigserial PRIMARY KEY,
			job_key    text NOT NULL,
			table_name text NOT NULL,
			reason     text NOT NULL,
			position   text,
			raw_row    jsonb,
			created_at timestamptz NOT NULL DEFAULT now()
		)`, s.stateTable("_jl_dead_letter")),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			instance_id text PRIMARY KEY,
			label       text,
			host        text,
			first_seen  timestamptz NOT NULL DEFAULT now(),
			last_seen   timestamptz NOT NULL DEFAULT now()
		)`, s.stateTable("_jl_instance")),
	}
	for _, stmt := range ddl {
		if _, err := s.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("建状态表失败（需要目标库的建表权限，且 target.schema 必须已存在）: %w", err)
		}
	}
	return nil
}

// ReadState reads `_jl_meta` for one job without opening a Sink (status uses it).
func ReadState(ctx context.Context, ep dsn.Endpoint, schema, jobKey string) (Meta, error) {
	if schema == "" {
		schema = "public"
	}
	conn, err := pgwire.Connect(ctx, ep)
	if err != nil {
		return Meta{}, err
	}
	defer conn.Close(ctx)
	q := fmt.Sprintf(`SELECT job_key, name, instance_id, phase, COALESCE(position,''),
		COALESCE(snapshot_id,''), updated_at FROM %s.%s WHERE job_key = $1`,
		pgwire.Quote(schema), pgwire.Quote("_jl_meta"))
	var m Meta
	err = conn.QueryRow(ctx, q, jobKey).Scan(&m.JobKey, &m.Name, &m.InstanceID, &m.Phase,
		&m.Position, &m.SnapshotID, &m.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Meta{}, ErrNotInitialised
	}
	return m, err
}

// ErrNotInitialised means the target has no state row for this job yet.
var ErrNotInitialised = errors.New("目标端还没有这个任务的运行状态")

// EnsureMeta inserts or refreshes the `_jl_meta` row.
func (s *Sink) EnsureMeta(ctx context.Context, jobHash, phase, position, snapshotID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 注意：phase 只在插入时用，冲突时**不改**。阶段只由 SetPhase 推进，
	// 否则「准备」这一步会把自己上一次的记录改写成 prepare，让重启误判全量没做完。
	q := fmt.Sprintf(`INSERT INTO %s (job_key, name, instance_id, schema_version, job_hash, phase, position, snapshot_id)
		VALUES ($1,$2,$3,$4,$5,COALESCE(NULLIF($6,''),'prepare'),$7,$8)
		ON CONFLICT (job_key) DO UPDATE SET name=EXCLUDED.name, instance_id=EXCLUDED.instance_id,
			position=COALESCE(EXCLUDED.position, %s.position),
			snapshot_id=COALESCE(EXCLUDED.snapshot_id, %s.snapshot_id), updated_at=now()`,
		s.stateTable("_jl_meta"), s.stateTable("_jl_meta"), s.stateTable("_jl_meta"))
	_, err := s.conn.Exec(ctx, q, s.opt.JobKey, s.opt.JobName, s.opt.InstanceID,
		SchemaVersion, jobHash, phase, nullStr(position), nullStr(snapshotID))
	return err
}

// SetPhase records the phase in `_jl_meta` (no position change: position only
// ever moves inside a data transaction).
func (s *Sink) SetPhase(ctx context.Context, phase string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	q := fmt.Sprintf(`UPDATE %s SET phase=$1, updated_at=now() WHERE job_key=$2`, s.stateTable("_jl_meta"))
	_, err := s.conn.Exec(ctx, q, phase, s.opt.JobKey)
	return err
}

// State returns the recorded phase and position. The phase is what tells a
// restart whether the initial copy ever finished: a position written during the
// snapshot only means "these chunks are durable", not "all data is here".
func (s *Sink) State(ctx context.Context) (phase, position string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	q := fmt.Sprintf(`SELECT phase, COALESCE(position,'') FROM %s WHERE job_key=$1`, s.stateTable("_jl_meta"))
	err = s.conn.QueryRow(ctx, q, s.opt.JobKey).Scan(&phase, &position)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", nil
	}
	return phase, position, err
}

// Position returns the authoritative position ("" when the job never started).
func (s *Sink) Position(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	q := fmt.Sprintf(`SELECT COALESCE(position,'') FROM %s WHERE job_key=$1`, s.stateTable("_jl_meta"))
	var pos string
	err := s.conn.QueryRow(ctx, q, s.opt.JobKey).Scan(&pos)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return pos, err
}

// ── 租约（同一 job_key 只允许一个实例写） ─────────────────────────────

// AcquireLease takes the job's lease, or reports who else holds it.
//
// 判定在 leaseTakeover 里（纯函数、有单测）。读-判-写放在**一个事务**里，并且
// `SELECT ... FOR UPDATE` 锁住那一行：否则两个进程可能同时读到「没人持有」，
// 然后都写成功——那正是 Owl 指出的「同 instance_id 无条件抢租约」的后果。
func (s *Sink) AcquireLease(ctx context.Context, ttl time.Duration) (bool, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	label := leaseOwnerLabel(s.opt.JobName, os.Getpid(), s.ownerToken)
	now := time.Now()

	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return false, "", err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	cur, exists, err := s.readLeaseForUpdate(ctx, tx)
	if err != nil {
		return false, "", err
	}
	ok, why := leaseTakeover(cur, exists, now, s.opt.InstanceID, os.Getpid(), s.ownerToken, processAlive)
	if !ok {
		return false, why, nil
	}

	if !exists {
		q := fmt.Sprintf(`INSERT INTO %s (job_key, instance_id, label, expires_at)
			VALUES ($1,$2,$3, now() + $4::interval)`, s.stateTable("_jl_lease"))
		if _, err := tx.Exec(ctx, q, s.opt.JobKey, s.opt.InstanceID, label, ttl); err != nil {
			return false, "", err
		}
	} else {
		q := fmt.Sprintf(`UPDATE %s SET instance_id=$1, label=$2, expires_at=now() + $3::interval
			WHERE job_key=$4`, s.stateTable("_jl_lease"))
		if _, err := tx.Exec(ctx, q, s.opt.InstanceID, label, ttl, s.opt.JobKey); err != nil {
			return false, "", err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return false, "", err
	}
	return true, why, nil
}

// readLeaseForUpdate locks the lease row for the duration of the decision.
func (s *Sink) readLeaseForUpdate(ctx context.Context, tx pgx.Tx) (leaseRow, bool, error) {
	q := fmt.Sprintf(`SELECT instance_id, COALESCE(label,''), expires_at FROM %s WHERE job_key=$1 FOR UPDATE`,
		s.stateTable("_jl_lease"))
	var row leaseRow
	err := tx.QueryRow(ctx, q, s.opt.JobKey).Scan(&row.InstanceID, &row.Label, &row.Expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return leaseRow{}, false, nil
	}
	if err != nil {
		return leaseRow{}, false, err
	}
	return row, true, nil
}

// newOwnerToken 给每个 Sink 一个进程内唯一、跨进程也不冲突的标识。
func newOwnerToken() string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", ownerSeq.Add(1))
	}
	return hex.EncodeToString(b[:])
}

var ownerSeq atomic.Uint32

// readLeaseLocked loads the current lease row (caller holds mu).
func (s *Sink) readLeaseLocked(ctx context.Context) (leaseRow, bool, error) {
	q := fmt.Sprintf(`SELECT instance_id, COALESCE(label,''), expires_at FROM %s WHERE job_key=$1`,
		s.stateTable("_jl_lease"))
	var row leaseRow
	err := s.conn.QueryRow(ctx, q, s.opt.JobKey).Scan(&row.InstanceID, &row.Label, &row.Expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return leaseRow{}, false, nil
	}
	if err != nil {
		return leaseRow{}, false, err
	}
	return row, true, nil
}

// RenewLease extends the lease; false means we lost it (someone took over).
func (s *Sink) RenewLease(ctx context.Context, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	q := fmt.Sprintf(`UPDATE %s SET expires_at = now() + $1::interval
		WHERE job_key=$2 AND instance_id=$3 AND label=$4`, s.stateTable("_jl_lease"))
	tag, err := s.conn.Exec(ctx, q, ttl, s.opt.JobKey, s.opt.InstanceID,
		leaseOwnerLabel(s.opt.JobName, os.Getpid(), s.ownerToken))
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// ReleaseLease drops our claim so the next run starts immediately.
func (s *Sink) ReleaseLease(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	q := fmt.Sprintf(`DELETE FROM %s WHERE job_key=$1 AND instance_id=$2 AND label=$3`,
		s.stateTable("_jl_lease"))
	_, err := s.conn.Exec(ctx, q, s.opt.JobKey, s.opt.InstanceID,
		leaseOwnerLabel(s.opt.JobName, os.Getpid(), s.ownerToken))
	return err
}

// ── 目标表结构 ─────────────────────────────────────────────────

// EnsureSchema implements the sink contract: create missing tables/columns.
func (s *Sink) EnsureSchema(ctx context.Context, _ source.Catalog) ([]source.Action, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var actions []source.Action
	for _, key := range s.order {
		ti := s.tables[key]
		exists, err := s.tableExists(ctx, ti.ref.Schema, ti.ref.Table)
		if err != nil {
			return actions, err
		}
		if exists {
			if s.opt.OnExists == "recreate" {
				if _, err := s.conn.Exec(ctx, "DROP TABLE "+pgwire.Qualified(ti.ref.Schema, ti.ref.Table)); err != nil {
					return actions, fmt.Errorf("按 recreate 清空目标表 %s 失败: %w", ti.ref.Table, err)
				}
				actions = append(actions, source.Action{
					ID: "drop:" + key, What: "清空了目标表 " + ti.ref.Table,
					SQL:  "DROP TABLE " + pgwire.Qualified(ti.ref.Schema, ti.ref.Table),
					Undo: "无法撤销（数据已删除）", Applied: true,
					Reason: "target.on_exists=recreate",
				})
			} else {
				if err := s.reconcileColumns(ctx, ti); err != nil {
					return actions, err
				}
				continue
			}
		}
		if err := s.createTable(ctx, ti); err != nil {
			return actions, err
		}
		// 登记所有权：只有「本任务建的表」才允许在重搬时清空。
		// append 模式复用用户已有表，那些表永远不会进这个名单。
		if err := s.recordObjectLocked(ctx, tableObjectName(ti.ref), "table"); err != nil {
			return actions, err
		}
		actions = append(actions, source.Action{
			ID: "create:" + key, What: "在目标端建了表 " + ti.ref.Table,
			SQL:     s.createTableSQL(ti),
			Undo:    "DROP TABLE " + pgwire.Qualified(ti.ref.Schema, ti.ref.Table),
			Applied: true,
			Reason:  "目标端没有这张表，按源端结构建一张",
		})
	}
	return actions, nil
}

func (s *Sink) tableExists(ctx context.Context, schema, table string) (bool, error) {
	var n int
	err := s.conn.QueryRow(ctx,
		`SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		 WHERE n.nspname=$1 AND c.relname=$2`, schema, table).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("检查目标表 %s 是否存在失败: %w", table, err)
	}
	return n > 0, nil
}

func (s *Sink) createTableSQL(ti *tableInfo) string {
	var b strings.Builder
	b.WriteString("CREATE TABLE " + pgwire.Qualified(ti.ref.Schema, ti.ref.Table) + " (")
	for i, col := range ti.cols {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(pgwire.Quote(col) + " " + ti.types[i])
		if coll := ti.collation(i); coll != "" {
			b.WriteString(" COLLATE " + pgwire.Quote(coll))
		}
	}
	if ti.hasPK {
		quoted := make([]string, len(ti.pk))
		for i, c := range ti.pk {
			quoted[i] = pgwire.Quote(c)
		}
		b.WriteString(", PRIMARY KEY (" + strings.Join(quoted, ", ") + ")")
	}
	b.WriteString(")")
	return b.String()
}

func (s *Sink) createTable(ctx context.Context, ti *tableInfo) error {
	if _, err := s.conn.Exec(ctx, s.createTableSQL(ti)); err != nil {
		return fmt.Errorf("在目标端建表 %s 失败: %w", ti.ref.Table, err)
	}
	return nil
}

// collation returns the explicit collation of column i ("" = type default).
func (ti *tableInfo) collation(i int) string {
	if i < len(ti.collations) {
		return ti.collations[i]
	}
	return ""
}

// reconcileColumns adds missing columns (the whitelisted "add column" DDL) and
// refuses to guess when an existing column has a different type.
func (s *Sink) reconcileColumns(ctx context.Context, ti *tableInfo) error {
	rows, err := s.conn.Query(ctx,
		`SELECT a.attname, format_type(a.atttypid, a.atttypmod),
		        COALESCE(co.collname, '')
		 FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid
		 JOIN pg_namespace n ON n.oid=c.relnamespace
		 JOIN pg_type t ON t.oid=a.atttypid
		 LEFT JOIN pg_collation co ON co.oid=a.attcollation AND a.attcollation <> t.typcollation
		 WHERE n.nspname=$1 AND c.relname=$2 AND a.attnum>0 AND NOT a.attisdropped`,
		ti.ref.Schema, ti.ref.Table)
	if err != nil {
		return err
	}
	defer rows.Close()
	have := map[string]string{}
	haveColl := map[string]string{}
	for rows.Next() {
		var name, typ, coll string
		if err := rows.Scan(&name, &typ, &coll); err != nil {
			return err
		}
		have[name] = typ
		haveColl[name] = coll
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if s.opt.OnExists == "error" {
		return fmt.Errorf("%w: 目标端已经有表 %s（%d 列）。当前设置是「撞表就停」",
			ErrTargetTableExists, ti.ref.Table, len(have))
	}
	for i, col := range ti.cols {
		typ, ok := have[col]
		if ok {
			if !compatibleType(typ, ti.types[i]) {
				// **同一种失败、两种时机不能有两种行为**：跑起来时发现类型变了 → 暂停该表；
				// prepare 时发现类型不一致却打死整任务，用户会当成两个 bug。
				// 所以这里也走 TypeChangedError（同一个原因码），由第③步统一按表暂停。
				return &TypeChangedError{TableName: ti.ref.Table,
					Detail: fmt.Sprintf("%s: %s → %s", col, typ, ti.types[i])}
			}
			if want := ti.collation(i); want != "" && haveColl[col] != want {
				// 排序规则不同 → 唯一索引判重与 ORDER BY 会不一致，而且是静默的。
				return fmt.Errorf("%w: 目标表 %s 的列 %s 排序规则是 %q，源端是 %q；"+
					"要么改成一致（ALTER TABLE ... ALTER COLUMN %s TYPE %s COLLATE %q），要么换目标 schema",
					ErrCollationMismatch, ti.ref.Table, col, haveColl[col], want, col, ti.types[i], want)
			}
			continue
		}
		def := ti.types[i]
		if coll := ti.collation(i); coll != "" {
			def += " COLLATE " + pgwire.Quote(coll)
		}
		stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s",
			pgwire.Qualified(ti.ref.Schema, ti.ref.Table), pgwire.Quote(col), def)
		if _, err := s.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("给目标表 %s 加列 %s 失败: %w", ti.ref.Table, col, err)
		}
	}
	return nil
}

// ErrTargetTableExists mirrors core.CodeTargetTableExists (E1002).
var ErrTargetTableExists = errors.New("目标端已经有同名表")

// ErrTypeMismatch means the target column cannot hold the source value as-is.
var ErrTypeMismatch = errors.New("目标表的列类型和源端不一致")

// ErrCollationMismatch means the two sides sort/compare the same text column
// differently — upsert 判重会静默走偏，必须当成结构不兼容处理。
var ErrCollationMismatch = errors.New("目标表的列排序规则和源端不一致")

func compatibleType(have, want string) bool {
	hb, _ := splitType(have)
	wb, _ := splitType(want)
	if hb != wb {
		return false
	}
	// 参数不同（numeric(12,2) vs numeric）不拦：目标端能存下就行，预检会报类型映射。
	return true
}

var typeAliases = map[string]string{
	"int4":        "integer",
	"int8":        "bigint",
	"int2":        "smallint",
	"float8":      "double precision",
	"float4":      "real",
	"bool":        "boolean",
	"varchar":     "character varying",
	"bpchar":      "character",
	"timestamptz": "timestamp with time zone",
	"timestamp":   "timestamp without time zone",
	"decimal":     "numeric",
	"timetz":      "time with time zone",
}

// splitType splits "character varying(50)" into ("character varying", "50").
func splitType(t string) (base, args string) {
	s := normalizeType(t)
	if i := strings.IndexByte(s, '('); i >= 0 {
		base = strings.TrimSpace(s[:i])
		if j := strings.LastIndexByte(s, ')'); j > i {
			args = strings.TrimSpace(s[i+1 : j])
		}
	} else {
		base = s
	}
	if canon, ok := typeAliases[base]; ok {
		base = canon
	}
	return base, args
}

func normalizeType(t string) string {
	return strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(t))), " ")
}

// TypeMapLoss reports the fidelity loss the type dictionary predicts, so the
// engine can surface "lossy" columns before copying 10M rows.
func TypeMapLoss(col source.Column) (catalog.Mapping, bool) {
	return catalog.Map(catalog.Postgres, catalog.Postgres, col.Type)
}

// ── 全量：一块一个事务（数据 + 块进度标记 + 位点） ─────────────────
//
// 块标记**就是断点续跑的凭据**：它和数据、位点在同一个事务里提交，所以
// 「已提交的块」永远完整（正在写的那一块被 kill -9 时整个回滚），重启可以从
// 最后一块的右边界接着读（见 chunk.go 与 engine.planCopy）。
//
// 导出的快照视图随进程消失，续跑剩下的块用的是**新的**一致性读视图：这不丢变更，
// 因为增量仍然从**原来那个位点**重放（续跑不许把位点往前挪）；续跑读到的更新值
// 会被同一条变更再盖一遍，删掉的行也会被重放出来。

// BulkApply writes one snapshot chunk: data, chunk marker and position commit
// together. The stage table + INSERT ... SELECT gives upsert semantics, so a
// restarted snapshot can re-read a table without duplicating rows.
func (s *Sink) BulkApply(ctx context.Context, ti *tableInfo, chunkNo int, rows [][]any, pos string, from, to string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if ti == nil {
		return fmt.Errorf("内部错误：目标端没有这张表的定义")
	}
	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if len(rows) > 0 {
		stage := "_jl_stage"
		cols := quoteList(ti.cols)
		create := fmt.Sprintf("CREATE TEMP TABLE %s (LIKE %s) ON COMMIT DROP",
			pgwire.Quote(stage), pgwire.Qualified(ti.ref.Schema, ti.ref.Table))
		if _, err := tx.Exec(ctx, create); err != nil {
			return fmt.Errorf("准备临时落地区失败: %w", err)
		}
		values := make([][]any, len(rows))
		for i, r := range rows {
			vals := make([]any, len(ti.cols))
			for j := range ti.cols {
				if j < len(r) {
					vals[j] = r[j]
				}
			}
			values[i] = vals
		}
		if _, err := tx.CopyFrom(ctx, pgx.Identifier{stage}, ti.cols, pgx.CopyFromRows(values)); err != nil {
			return fmt.Errorf("写入 %s 的一块数据失败: %w", ti.ref.Table, err)
		}
		insert := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s",
			pgwire.Qualified(ti.ref.Schema, ti.ref.Table), cols, cols, pgwire.Quote(stage))
		insert += conflictClause(ti)
		if _, err := tx.Exec(ctx, insert); err != nil {
			return fmt.Errorf("把 %s 的一块数据落库失败: %w", ti.ref.Table, err)
		}
	}

	// 块进度：这条记录就是续跑起点（和数据同一个事务，见 chunk.go）。
	ck := fmt.Sprintf(`INSERT INTO %s (job_key, table_name, chunk_no, range_from, range_to, done, rows)
		VALUES ($1,$2,$3,$4,$5,true,$6)
		ON CONFLICT (job_key, table_name, chunk_no) DO UPDATE SET done=true, rows=EXCLUDED.rows`,
		s.stateTable("_jl_chunk"))
	if _, err := tx.Exec(ctx, ck, s.opt.JobKey, ti.ref.Table, chunkNo, nullStr(from), nullStr(to), len(rows)); err != nil {
		return fmt.Errorf("记录块进度失败: %w", err)
	}

	if pos != "" {
		if err := s.setPositionTx(ctx, tx, pos); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// ApplyRowsRange 写一页 interval 增量：**数据 + 位点 + 水位** 同一个事务。
//
// 与 BulkApply 用同一套「临时表 + INSERT … SELECT … ON CONFLICT」：interval 模式下
// 同一行会因为重叠回看被重复读到，必须有 upsert 语义，否则一重读就报主键冲突。
func (s *Sink) ApplyRowsRange(ctx context.Context, ref source.TableRef, rows [][]any,
	pos, watermark string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ti := s.tables[ref.Schema+"."+ref.Table]
	if ti == nil {
		return fmt.Errorf("内部错误：目标端没有这张表的定义")
	}
	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	if len(rows) > 0 {
		stage := "_jl_stage_range"
		cols := quoteList(ti.cols)
		if _, err := tx.Exec(ctx, fmt.Sprintf("CREATE TEMP TABLE %s (LIKE %s) ON COMMIT DROP",
			pgwire.Quote(stage), pgwire.Qualified(ti.ref.Schema, ti.ref.Table))); err != nil {
			return fmt.Errorf("准备临时落地区失败: %w", err)
		}
		values := make([][]any, len(rows))
		for i, r := range rows {
			vals := make([]any, len(ti.cols))
			for j := range ti.cols {
				if j < len(r) {
					vals[j] = r[j]
				}
			}
			values[i] = vals
		}
		if _, err := tx.CopyFrom(ctx, pgx.Identifier{stage}, ti.cols, pgx.CopyFromRows(values)); err != nil {
			return fmt.Errorf("写入 %s 的一页数据失败: %w", ti.ref.Table, err)
		}
		insert := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s",
			pgwire.Qualified(ti.ref.Schema, ti.ref.Table), cols, cols, pgwire.Quote(stage))
		insert += conflictClause(ti)
		if _, err := tx.Exec(ctx, insert); err != nil {
			return fmt.Errorf("把 %s 的一页数据落库失败: %w", ti.ref.Table, err)
		}
	}
	if pos != "" {
		if err := s.setPositionTx(ctx, tx, pos); err != nil {
			return err
		}
	}
	if watermark != "" {
		q := fmt.Sprintf(`UPDATE %s SET watermark=$1, updated_at=now() WHERE job_key=$2`,
			s.stateTable("_jl_meta"))
		if _, err := tx.Exec(ctx, q, watermark, s.opt.JobKey); err != nil {
			return fmt.Errorf("记录水位失败: %w", err)
		}
	}
	return tx.Commit(ctx)
}

// Watermark 读回 `_jl_meta.watermark`（interval 模式的每表水位 JSON）。
func (s *Sink) Watermark(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := fmt.Sprintf(`SELECT COALESCE(watermark,'') FROM %s WHERE job_key=$1`, s.stateTable("_jl_meta"))
	var w string
	err := s.conn.QueryRow(ctx, q, s.opt.JobKey).Scan(&w)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return w, err
}

// ── 增量：一批一个事务（数据 + 位点） ───────────────────────────

// Apply implements the sink contract. One call = one target transaction, and
// the position advances inside it.
func (s *Sink) Apply(ctx context.Context, evs []source.Event, pos source.Position) error {
	// 契约不变量（sink.Sink.Apply 注释，2026-11-05）：事务边界由 Begin/Commit 表达，
	// 没有 rollback 事件；半个事务的批次不许推进位点。
	if err := source.ValidateBatch(evs); err != nil {
		return fmt.Errorf("这一批事件不满足事务边界不变量: %w", err)
	}
	advance := pos != nil && (len(evs) == 0 || endsWithCommit(evs))

	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	// 插入/更新按「同表连续」分组，多条拼成一条多行 upsert：这是增量吞吐的关键
	// （逐行 = 每行一次往返）。
	i := 0
	for i < len(evs) {
		ev := evs[i]
		switch ev.Kind {
		case source.EvInsert, source.EvUpdate:
			// 加列竞态：源端 DDL 之后的行带着**新的 Relation**，而目标端可能还没有那一列。
			// 先对齐列再写行——否则这一批整体失败，一张表的 DDL 把整个任务打死。
			if err := s.alignRelationTx(ctx, tx, ev); err != nil {
				return err
			}
			j := i
			for j < len(evs) && (evs[j].Kind == source.EvInsert || evs[j].Kind == source.EvUpdate) &&
				evs[j].Table == ev.Table {
				j++
			}
			if err := s.upsertBatchTx(ctx, tx, evs[i:j]); err != nil {
				return err
			}
			i = j
			continue
		case source.EvDelete:
			if err := s.deleteTx(ctx, tx, ev); err != nil {
				return err
			}
		case source.EvTruncate:
			if _, err := tx.Exec(ctx, "TRUNCATE TABLE "+pgwire.Qualified(ev.Table.Schema, ev.Table.Table)); err != nil {
				return fmt.Errorf("截断目标表 %s 失败: %w", ev.Table.Table, err)
			}
		case source.EvDDL:
			// **DDL 必须在这里执行**。之前这行注释写的是「由引擎处理」——而引擎里
			// 根本没有 EvDDL 的处理代码（`grep EvDDL internal/engine` 为空），
			// 于是源端 DDL 从来没搬到目标端：加列后整批行写失败、建索引完全静默、
			// 改类型下次 EnsureSchema 撞 ErrTypeMismatch 把整个任务打死。
			if err := s.applyDDLTx(ctx, tx, ev); err != nil {
				return err
			}
		default:
			// Begin/Commit/Heartbeat 由引擎处理，不落 SQL
		}
		i++
	}
	if advance {
		if err := s.setPositionTx(ctx, tx, pos.String()); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// ddlDestructive 是破坏性 DDL 的判定（**白名单优先**：宁可先窄）。
//
// 为什么必须有：MySQL 侧（#25 的四类白名单）对 DROP / RENAME 一律 reject，
// 而这里如果「原样执行」，DROP TABLE 会立刻删掉目标表且没有任何确认环节——
// 两侧策略相反本身就是 bug。
//
// 窄了能手工做（我们会告诉用户该执行什么）；宽了会静默丢数据。
func ddlDestructive(stmt string) bool {
	up := strings.ToUpper(strings.Join(strings.Fields(stripLeadingComments(stmt)), " "))
	banned := []string{
		"DROP TABLE", "DROP SCHEMA", "DROP COLUMN", "DROP DATABASE",
		"TRUNCATE", "RENAME ", "ALTER TABLE .* RENAME",
	}
	for _, b := range banned {
		if strings.HasPrefix(up, b) {
			return true
		}
	}
	// ALTER TABLE … DROP COLUMN / DROP CONSTRAINT / RENAME 也在其中
	if strings.HasPrefix(up, "ALTER TABLE") &&
		(strings.Contains(up, " DROP ") || strings.Contains(up, " RENAME ")) {
		return true
	}
	// DROP INDEX 单独放行是有意的：它不删数据，且重放时可以幂等跳过。
	return strings.HasPrefix(up, "DROP INDEX") == false && strings.HasPrefix(up, "DROP ")
}

// stripLeadingComments 剥掉语句**前导**的注释（`/* … */` 与 `-- …` 行注释），
// 返回第一条真正的 SQL。
//
// 为什么必须有：判定原来是 HasPrefix 匹配关键字，而 `/*x*/ DROP TABLE t`、
// `-- hi` + 换行 + `DROP TABLE t`、多行注释前缀都不以关键字开头 → 判定落空、
// 语句照旧被执行 → **目标表被删**。pg_dump / ORM 生成的 DDL 常带注释头，这不是特例。
func stripLeadingComments(stmt string) string {
	s := strings.TrimSpace(stmt)
	for {
		switch {
		case strings.HasPrefix(s, "/*"):
			if i := strings.Index(s, "*/"); i >= 0 {
				s = strings.TrimSpace(s[i+2:])
				continue
			}
			return s // 注释没闭合：原样返回，交给数据库报错
		case strings.HasPrefix(s, "--"):
			if i := strings.IndexByte(s, '\n'); i >= 0 {
				s = strings.TrimSpace(s[i+1:])
				continue
			}
			return "" // 整条都是行注释
		default:
			return s
		}
	}
}

// checkDDLAllowed 决定这条 DDL 能不能在目标端执行。
//
// 判据只有两条，都是「先窄后宽」：
//  1. **破坏性语句不执行**（交给用户手工做，我们在错误里给原句）；
//  2. **源 schema 必须等于目标 schema**——语句里的表名是源端原文，
//     跨 schema 时要么报错，要么改到同名但非目标的表（最坏）。按文本重写表名太脆弱，
//     所以先拒绝，等有真实需求再做映射。
func checkDDLAllowed(stmt, srcSchema, tgtSchema string) error {
	if ddlDestructive(stmt) {
		return fmt.Errorf("这条 DDL 会删数据，我们不替你执行（%s）。"+
			"你能做什么：1) 确认目标端也要这样改之后，手工在那条连接上执行它；"+
			"2) 或者回任务页把这张表移出同步范围。风险：手工执行是不可回滚的结构变更，删掉的列/表数据找不回来", stmt)
	}
	// 「不确定就不做」：源/目标 schema 任一为空意味着不知道该改哪张表
	//（语句可能不带前缀、会落到连接默认 schema）——在结构变更上这等于「可能改错表」。
	// 所以这里**拒绝**，而不是「未知就放行」。
	if strings.TrimSpace(srcSchema) == "" || strings.TrimSpace(tgtSchema) == "" {
		return fmt.Errorf("这条 DDL 的 schema 不确定（源 %q / 目标 %q），不跟随（%s）。"+
			"你能做什么：手工在目标端执行这条语句；或在目标端把表名写全 schema 前缀。"+
			"风险：结构变更改错表不可逆，所以我们宁可不做", srcSchema, tgtSchema, stmt)
	}
	if srcSchema != tgtSchema {

		return fmt.Errorf("源端 schema 是 %q、目标端是 %q，这条 DDL 里的表名是源端原文（%s）："+
			"直接执行可能改到同名但不相干的表。你能做什么：1) 把 target.schema 配成与源端一致；"+
			"2) 或者手工在目标端执行这条语句。风险：改错表的后果不可逆", srcSchema, tgtSchema, stmt)
	}
	return nil
}

// alignRelationTx 让目标表的列与事件里的 Relation 对齐（**只加不减**）。
//
// 场景：源端刚 ALTER TABLE ADD COLUMN，随后那一批行带着**新的 Relation**，
// 而目标端（和本 sink 的列缓存）还是旧的 → 整批行写失败、整个任务 failed。
// 一张表的 DDL 不该把任务打死。
//
// 两条规矩：
//   - **只加列**：删列/改类型是危险动作（删列丢数据、改窄类型截断），不能因为收到一条
//     事件就替用户做——那些由 applyDDLTx 显式执行或明确拒绝；
//   - **类型用 Relation 里的源端类型**：PG→PG 目标端与源端同类型，这正是 EnsureSchema
//     建表时用的同一份定义（`tableInfo.types` 就是它），不在这里另发明一套映射。
func (s *Sink) alignRelationTx(ctx context.Context, tx pgx.Tx, ev source.Event) error {
	if ev.Relation == nil || len(ev.Relation.Columns) == 0 {
		return nil
	}
	key := ev.Table.Schema + "." + ev.Table.Table
	ti := s.tables[key]
	if ti == nil {
		return fmt.Errorf("收到未知表 %s 的变更（发布里多了表？）", ev.Table.Table)
	}
	// **只比列数是不够的**：改类型与改名的列数都没变。
	// 早退会让它们走「一切正常」的路——缓存仍是旧类型/旧名字，写行时静默按旧口径编码，
	// 或者报一个看起来像「数据问题」的编码错。原来「整批失败」至少是响亮的，
	// 静默写错更坏。所以这里同时比名字与类型。
	// 比较前先把 Relation 的类型翻成目标端类型：跨引擎时 Relation 带的是源端类型文本
	// （`varchar(64)` / `datetime(6)`），而缓存里是映射后的 PG 类型——直接比会把
	// 「同一个类型」判成「改类型」，整轮失败（假红）。
	sameNames, sameTypes, keptAll, typeDiff, nameDiff := relationMatches(ti, ev.Relation, s.opt.SourceKind)

	switch {
	case sameNames && sameTypes:
		return nil // 真的没变
	case keptAll && sameTypes:
		// 原有列都还在、类型也没变——可能是**追加了新列**，也可能是**列序变化**
		//（后者 sameNames=false 但一列都不用加，只需按 Relation 顺序重建缓存）。
		// 两种情况都走这里：对齐循环会跳过已存在的列，只补新列。
		_ = nameDiff
	case sameNames && !sameTypes:
		// 改类型：本版**不跟随**（改窄会截断、改宽会静默变更语义）。
		// 第③步会把它降级成「只暂停这张表 + 明确原因码」，其它表继续跑；
		// 在那之前先响亮拒绝，别让它安静写错。
		return &TypeChangedError{TableName: ev.Table.Table, Detail: typeDiff}
	case !keptAll:
		return &SchemaDriftError{TableName: ev.Table.Table, Detail: nameDiff}
	}

	var added []string
	if len(ev.Relation.Columns) > len(ti.cols) {
		// lock_timeout 是**事务级**设置：设一次就够（原来写在每列循环里，幂等但看着像每列一个）。
		// ADD COLUMN 在数据事务内部执行会拿 ACCESS EXCLUSIVE 并持有到提交：这一批期间
		// 目标表对所有读写都不可见（verify 在读、别的任务在写都会被挡）。短超时让它
		// **快速失败并说清**，而不是排队把后面批次一起堵住。这个代价只在「Relation 真的变列」时付。
		if _, err := tx.Exec(ctx, "SET LOCAL lock_timeout = '3s'"); err != nil {
			return fmt.Errorf("设置 lock_timeout 失败: %w", err)
		}
	}
	for _, c := range ev.Relation.Columns {
		if _, ok := ti.colIndex[c.Name]; ok {
			continue
		}
		stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s",
			pgwire.Qualified(ev.Table.Schema, ev.Table.Table), pgwire.Quote(c.Name), c.Type)
		if _, err := tx.Exec(ctx, stmt); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "55P03" {
				// 锁等不到 ≠ 数据有问题。值班最容易把它归到「数据坏了」。
				return fmt.Errorf("目标表 %s 被别的会话占用，等锁超时（常见：verify 正在全表扫描）：%w。"+
					"你能做什么：等那个操作结束再重跑（位点未推进，不会重复搬）。"+
					"风险：没有（本次这一批没有写入）", ev.Table.Table, err)
			}
			return fmt.Errorf("源端新增了列 %s，但在目标表 %s 上补列失败: %w", c.Name, ev.Table.Table, err)
		}
		added = append(added, c.Name)
	}

	// 缓存跟着 Relation 走：列序以 Relation 为准（行值就是按这个顺序来的），
	// **colIndex 必须一起重建**——只改 cols/types 会让按列名找下标的路径错位。
	ti.cols = ti.cols[:0]
	ti.types = ti.types[:0]
	ti.collations = ti.collations[:0]
	ti.colIndex = map[string]int{}
	for _, c := range ev.Relation.Columns {
		ti.colIndex[c.Name] = len(ti.cols)
		ti.cols = append(ti.cols, c.Name)
		ti.types = append(ti.types, targetType(s.opt.SourceKind, c.Type))
		ti.collations = append(ti.collations, c.Collation)
	}
	ti.pkIdx = ti.pkIdx[:0]
	for _, name := range ti.pk {
		if idx, ok := ti.colIndex[name]; ok {
			ti.pkIdx = append(ti.pkIdx, idx)
		}
	}
	if len(added) > 0 {
		log.Printf("%s 跟随源端新增了 %d 列（%s）", ev.Table.Table, len(added), strings.Join(added, ", "))
	}
	return nil
}

// relationMatches 比较缓存的列定义与事件里的 Relation：**名字与类型都要比**。
//
// 三个返回值都要用，因为它们对应三种完全不同的处置：
//   - sameNames && sameTypes：真的没变（最常见的每批路径，零成本早退）；
//   - keptAll && sameTypes：原有列都还在、类型没变 → 只是**追加了新列**，走对齐；
//   - 其余：改名 / 删列 / 改类型 → 本版不跟随，响亮拒绝（第③步再降级成只暂停该表）。
//
// 只比列数会把后两种悄悄放过——缓存留着旧类型/旧名字继续写，静默写错。
func relationMatches(ti *tableInfo, rel *source.Table, src catalog.Engine) (sameNames, sameTypes, keptAll bool, typeDiff, nameDiff string) {
	byName := map[string]string{}
	for _, c := range rel.Columns {
		byName[c.Name] = targetType(src, c.Type)
	}
	sameNames = len(rel.Columns) == len(ti.cols)
	if sameNames {
		for i := range ti.cols {
			if rel.Columns[i].Name != ti.cols[i] {
				sameNames = false
				break
			}
		}
	}
	sameTypes, keptAll = true, true
	var tparts, nparts []string
	for i, name := range ti.cols {
		typ, ok := byName[name]
		if !ok {
			keptAll = false
			nparts = append(nparts, fmt.Sprintf("原 %s 已不存在", name))
			continue
		}
		if i < len(ti.types) && typ != ti.types[i] {
			sameTypes = false
			tparts = append(tparts, fmt.Sprintf("%s: %s → %s", name, ti.types[i], typ))
		}
		if i < len(rel.Columns) && rel.Columns[i].Name != name {
			nparts = append(nparts, fmt.Sprintf("第 %d 列 原 %s → 现 %s", i+1, name, rel.Columns[i].Name))
		}
	}
	// 「新增」与列数是否变化无关：改名时也是「一个没了、一个多了」，两边都要说清。
	for _, c := range rel.Columns {
		if _, ok := ti.colIndex[c.Name]; !ok {
			nparts = append(nparts, "新增 "+c.Name)
		}
	}
	typeDiff = strings.Join(tparts, "；")
	nameDiff = strings.Join(nparts, "；")
	return sameNames, sameTypes, keptAll, typeDiff, nameDiff
}

// 暂停原因码：**给机器判等用的稳定值**，不是给用户看的中文。
// 第③步的 paused_tables、恢复入口与 doctor 都要按它判等；用 message 判断会在
// 改一次文案之后全部失效（今晚已经因为「按字符串判断」返工过）。
const (
	PauseReasonTypeChanged = "type_changed" // 源端改了列类型
	PauseReasonSchemaDrift = "schema_drift" // 改名 / 删列
)

// TypeChangedError 表示源端改了列类型——本版不跟随（会截断或静默变更语义）。
// 单独成类型，方便第③步把它降级成「只暂停这张表」而不是整个任务。
type TypeChangedError struct {
	TableName string
	Detail    string
}

// Reason 实现 PausableError：第③步按它记 paused_tables 的原因码。
func (e *TypeChangedError) Reason() string { return PauseReasonTypeChanged }

// Table 实现 PausableError。
func (e *TypeChangedError) Table() string { return e.TableName }

// SchemaDriftError 表示列被改名或删掉（列数可能没变）。
type SchemaDriftError struct {
	TableName string
	Detail    string
}

func (e *SchemaDriftError) Error() string {
	return fmt.Sprintf("源端表 %s 的列发生变化（改名/删列），本版不跟随：%s。"+
		"你能做什么：1) 手工把目标表的列改成与源端一致后重跑（位点不动）；"+
		"2) 或把该表移出同步范围。风险：结构不一致时继续写会让数据与列错位", e.TableName, e.Detail)
}

// Reason 实现 PausableError。
func (e *SchemaDriftError) Reason() string { return PauseReasonSchemaDrift }

// Table 实现 PausableError。
func (e *SchemaDriftError) Table() string { return e.TableName }

// PausableError 是「这张表不能继续跟，但其它表可以继续」的错误。
// 第③步用它把「一张表的 DDL」降级成「只暂停这张表」，而不是打死整个任务。
type PausableError interface {
	error
	// Reason 是稳定原因码（机器判等，不是中文句子）。
	Reason() string
	// Table 是被暂停的那张表。放进接口是必须的：paused_tables 要存「表名 + 原因码 + 时间」，
	// 若表名只藏在各个具体类型里，第③步就得对每个类型各写一次 errors.As——加第三个类型时必漏。
	Table() string
}

func (e *TypeChangedError) Error() string {
	return fmt.Sprintf("源端表 %s 的列类型发生变化，本版不跟随：%s。"+
		"你能做什么：1) 确认目标端也要改成新类型后，手工执行 ALTER 再重跑（位点不动）；"+
		"2) 或把该表移出同步范围。"+
		"风险：类型不一致时继续写，要么被截断、要么报一个看起来像「数据问题」的编码错", e.TableName, e.Detail)
}

// applyDDLTx 在目标端执行源端的 DDL。
//
// ⚠️ 现状（务必知道）：**这条能力在真实运行里目前产生不出来**——非测试代码里只有
// `mysqlsource/assembler.go` 产出 EvDDL，而引擎的源端只做 PostgreSQL（pgoutput 不解码 DDL）。
// 也就是说今天只有测试能触发它。等接上「PG 源 DDL 捕获」或「MySQL 源 → PG 目标」时，
// 破坏性语句的拦截（checkDDLAllowed）就是唯一那道闸——这也是它必须先窄的原因。
//
// 幂等：崩溃重放时同一条 DDL 会再来一次，撞上「已经存在」的 SQLSTATE 视为成功，
// 但**跳过必须留痕**（同名不同定义也是 42P07：跳过等于静默放过）。
func (s *Sink) applyDDLTx(ctx context.Context, tx pgx.Tx, ev source.Event) error {
	stmt := strings.TrimSpace(ev.DDL)
	if stmt == "" {
		return nil
	}
	// 纯注释（`-- …` 且没有换行、或只有注释）剥完是空的：没有 SQL 可执行，
	// 别拿它去撞数据库（也不该因此判成破坏性语句）。
	if strings.TrimSpace(stripLeadingComments(stmt)) == "" {
		return nil
	}
	if err := checkDDLAllowed(stmt, ev.Table.Schema, s.opt.Schema); err != nil {
		return err
	}
	sp := fmt.Sprintf("jl_ddl_%d", time.Now().UnixNano())
	if _, err := tx.Exec(ctx, "SAVEPOINT "+pgwire.Quote(sp)); err != nil {
		return fmt.Errorf("为 DDL 建保存点失败: %w", err)
	}
	release := func() error {
		if _, err := tx.Exec(ctx, "RELEASE SAVEPOINT "+pgwire.Quote(sp)); err != nil {
			return fmt.Errorf("释放 DDL 保存点失败: %w", err)
		}
		return nil
	}
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(stripLeadingComments(stmt))), "DROP INDEX") {
		// 不删数据，但索引名是 **schema 级**的：可能删掉另一张表上（别的任务建的）同名索引。
		log.Printf("执行 DROP INDEX（索引名是 schema 级的，可能影响别的表；"+
			"若这条索引由别的任务/人工维护，删掉后**不会自动重建**，后果通常要等查询变慢才暴露）：%s", stmt)
		if s.opt.OnDDLSkipped != nil {
			s.opt.OnDDLSkipped("DROP INDEX：" + stmt)
		}
	}
	if _, err := tx.Exec(ctx, stmt); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			switch pgErr.Code {
			case "42701", "42P07", "42710", "42P16", "42723":
				if _, rerr := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT "+pgwire.Quote(sp)); rerr != nil {
					return fmt.Errorf("回滚 DDL 保存点失败: %w", rerr)
				}
				if err := release(); err != nil {
					return err
				}
				// 「跳过」比「失败」更需要留痕：失败会有人看，跳过不会。
				// 而且同名不同定义也是 42P07——我们并没有核对过定义是否一致。
				log.Printf("跳过一条 DDL（目标端已有同名对象，未核对定义是否一致）：%s", stmt)
				if s.opt.OnDDLSkipped != nil {
					s.opt.OnDDLSkipped(stmt)
				}
				return nil
			}
		}
		return fmt.Errorf("在目标端执行源端 DDL 失败（%s）: %w。"+
			"你能做什么：1) 在目标端手工执行这条语句后重跑（位点不动，不需要重搬历史）；"+
			"2) 如果这条 DDL 不该跟随到目标端，把对应对象排除出同步范围。"+
			"风险：DDL 没跟随时两端结构不一致，之后写这批表会持续失败", stmt, err)
	}
	if err := release(); err != nil {
		return err
	}
	log.Printf("已在目标端执行源端 DDL：%s", stmt)
	return nil
}

// endsWithCommit reports whether this batch closes on a committed transaction.
// 半个事务的批次（续段没有收尾）不许推进位点。
func endsWithCommit(evs []source.Event) bool {
	return len(evs) > 0 && evs[len(evs)-1].Kind == source.EvCommit
}

// upsertBatchTx 把同表连续的插入/更新拼成**一条**多行 upsert。
//
// 与单行路径共用同一个 conflictClause / castedHoldersFor，保证两条路径的
// ON CONFLICT 语义不可能分叉（分叉了就是最难查的一类 bug）。
//
// 部分失败怎么办（取舍写清）：多行 INSERT 是**一条语句**，任一行失败整条回滚、
// 整个事务回滚、位点不推进 → 引擎按批重放 → upsert 幂等吸收。我们**不做逐行死信**：
// 逐行重试等于退回 N 次往返，正好把这次优化抹掉；而且错误的行在 Postgres 的
// DETAIL/上下文里能定位到表与约束，足够人工处理。
func (s *Sink) upsertBatchTx(ctx context.Context, tx pgx.Tx, evs []source.Event) error {
	if len(evs) == 0 {
		return nil
	}
	ti := s.tables[evs[0].Table.Schema+"."+evs[0].Table.Table]
	if ti == nil {
		return fmt.Errorf("收到未知表 %s 的变更（发布里多了表？）", evs[0].Table.Table)
	}
	limit := effectiveBatchRows(s.opt.ApplyBatchRows, len(ti.cols))
	s.noteBatchLimit(ti, limit)
	for start := 0; start < len(evs); start += limit {
		end := start + limit
		if end > len(evs) {
			end = len(evs)
		}
		if err := s.execUpsertChunk(ctx, tx, ti, evs[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func (s *Sink) execUpsertChunk(ctx context.Context, tx pgx.Tx, ti *tableInfo, evs []source.Event) error {
	evs = dedupeByPK(ti, evs)
	if len(evs) == 0 {
		return nil
	}
	cols := quoteList(ti.cols)
	holders := castedHoldersFor(ti, len(evs))
	q := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		pgwire.Qualified(ti.ref.Schema, ti.ref.Table), cols, holders)
	q += conflictClause(ti)

	params := make([]any, 0, len(evs)*len(ti.cols))
	for _, ev := range evs {
		if len(ev.New.Values) != len(ti.cols) {
			return fmt.Errorf("%s 的行有 %d 列，表定义是 %d 列（源端结构变了？）",
				ti.ref.Table, len(ev.New.Values), len(ti.cols))
		}
		for i := range ti.cols {
			params = append(params, s.param(ev.New.Values[i]))
		}
	}
	// 用 SAVEPOINT 包住这条批量语句：失败后事务会进入 aborted 状态（后续语句全 25P02），
	// 只有回滚到这个保存点才能继续在同一条事务里逐行定位坏行。
	sp, err := tx.Begin(ctx)
	if err != nil {
		return fmt.Errorf("建保存点失败: %w", err)
	}
	if _, err := sp.Exec(ctx, q, params...); err == nil {
		if err := sp.Commit(ctx); err != nil {
			return fmt.Errorf("提交保存点失败: %w", err)
		}
		return nil
	} else {
		if rbErr := sp.Rollback(ctx); rbErr != nil {
			return fmt.Errorf("回滚保存点失败（%v），原始错误: %w", rbErr, err)
		}
		// 整批失败时**降级逐行重试一遍**，把坏行定位出来：
		// 毒丸行（类型越界/约束冲突）会让整批永远失败、每轮重放都失败、位点不动，
		// 而「写入 t 失败（500 行）」这种信息既没主键也没批次范围，用户无从下手。
		// 正常路径仍是 500 行一次往返——慢的这一遍只发生在已经失败的时候。
		return s.locateFailingRow(ctx, tx, ti, evs, err)
	}
}

// locateFailingRow 逐行重试以找出坏行，报告主键与原始 SQLSTATE。
func (s *Sink) locateFailingRow(ctx context.Context, tx pgx.Tx, ti *tableInfo, evs []source.Event, batchErr error) error {
	for i, ev := range evs {
		cols := quoteList(ti.cols)
		q := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
			pgwire.Qualified(ti.ref.Schema, ti.ref.Table), cols, castedHoldersFor(ti, 1))
		q += conflictClause(ti)
		params := make([]any, 0, len(ti.cols))
		for c := range ti.cols {
			params = append(params, valueOf(ev.New.Values[c]))
		}
		rowSP, spErr := tx.Begin(ctx)
		if spErr != nil {
			return fmt.Errorf("建保存点失败: %w", spErr)
		}
		if _, err := rowSP.Exec(ctx, q, params...); err != nil {
			_ = rowSP.Rollback(ctx)
			if strings.Contains(err.Error(), "25P02") {
				// 事务仍处于 aborted：说明保存点没起作用，把原错误带回去，别掩盖真相。
				return fmt.Errorf("写入 %s 失败（%d 行，%d 列；无法在事务内逐行定位：%v）: %w",
					ti.ref.Table, len(evs), len(ti.cols), err, batchErr)
			}
			return fmt.Errorf("写入 %s 失败：第 %d/%d 行（同批去重后，主键 %s）写不进去：%s。"+
				"你能做的：① 修源端这一行的数据（或按约束补默认值）后重跑；"+
				"② 该列不再同步就加进 content.tables.<表>.columns.exclude；"+
				"③ 目标端约束本身不对就改目标表。风险：跳过这一行意味着目标端会缺一条数据，语义上只能靠你确认。"+
				"（整批失败原因：%v）",
				ti.ref.Table, i+1, len(evs), s.pkOf(ti, ev), describeSQLError(err), batchErr)
		}
		if err := rowSP.Commit(ctx); err != nil {
			return fmt.Errorf("提交保存点失败: %w", err)
		}
	}
	// 逐行都能过？说明是批次层面的问题（例如参数上限/锁超时），把原错误带回去。
	return fmt.Errorf("写入 %s 失败（%d 行，%d 列，逐行重试却都能过：多半是批大小或锁的问题）: %w",
		ti.ref.Table, len(evs), len(ti.cols), batchErr)
}

// pkOf 渲染主键值，用于错误定位。
func (s *Sink) pkOf(ti *tableInfo, ev source.Event) string {
	if !ti.hasPK || len(ti.pkIdx) == 0 {
		return "无（表没有主键）"
	}
	parts := make([]string, 0, len(ti.pkIdx))
	for i, idx := range ti.pkIdx {
		var v any
		if idx < len(ev.New.Values) {
			v = valueOf(ev.New.Values[idx])
		}
		parts = append(parts, fmt.Sprintf("%s=%v", ti.pk[i], v))
	}
	return strings.Join(parts, ",")
}

// describeSQLError 把 Postgres 错误压成「SQLSTATE + 消息」，便于用户搜索与反馈。
func describeSQLError(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// 约束名经常为空（例如类型越界、权限）：渲染成「（）」比不写更让人困惑。
		if pgErr.ConstraintName != "" {
			return fmt.Sprintf("SQLSTATE %s（%s）：%s", pgErr.Code, pgErr.ConstraintName, pgErr.Message)
		}
		return fmt.Sprintf("SQLSTATE %s：%s", pgErr.Code, pgErr.Message)
	}
	return err.Error()
}

func (s *Sink) deleteTx(ctx context.Context, tx pgx.Tx, ev source.Event) error {
	ti := s.tables[ev.Table.Schema+"."+ev.Table.Table]
	if ti == nil {
		return fmt.Errorf("收到未知表 %s 的删除", ev.Table.Table)
	}
	if !ti.hasPK {
		// 无主键表没有定位依据：只提示，不猜（预检里已标 warn）
		return fmt.Errorf("表 %s 没有主键，无法安全删除目标端对应行", ev.Table.Table)
	}
	var where []string
	params := make([]any, 0, len(ti.pkIdx))
	for i, idx := range ti.pkIdx {
		where = append(where, fmt.Sprintf("%s = $%d::text::%s", pgwire.Quote(ti.pk[i]), i+1, ti.types[idx]))
		var v any
		if idx < len(ev.Old.Values) {
			v = s.param(ev.Old.Values[idx])
		}
		params = append(params, v)
	}
	q := fmt.Sprintf("DELETE FROM %s WHERE %s",
		pgwire.Qualified(ti.ref.Schema, ti.ref.Table), strings.Join(where, " AND "))
	if _, err := tx.Exec(ctx, q, params...); err != nil {
		return fmt.Errorf("删除 %s 的行失败: %w", ev.Table.Table, err)
	}
	return nil
}

// setPositionTx is the one place a position is written. It must be called with
// the transaction that carries the data — never on its own connection.
func (s *Sink) setPositionTx(ctx context.Context, tx pgx.Tx, pos string) error {
	q := fmt.Sprintf(`UPDATE %s SET position=$1, updated_at=now() WHERE job_key=$2`, s.stateTable("_jl_meta"))
	tag, err := tx.Exec(ctx, q, pos, s.opt.JobKey)
	if err != nil {
		return fmt.Errorf("推进同步进度失败: %w", err)
	}
	if tag.RowsAffected() == 0 {
		ins := fmt.Sprintf(`INSERT INTO %s (job_key, name, instance_id, schema_version, job_hash, phase, position)
			VALUES ($1,$2,$3,$4,'',$5,$6) ON CONFLICT (job_key) DO UPDATE SET position=EXCLUDED.position`,
			s.stateTable("_jl_meta"))
		if _, err := tx.Exec(ctx, ins, s.opt.JobKey, s.opt.JobName, s.opt.InstanceID, SchemaVersion, "streaming", pos); err != nil {
			return fmt.Errorf("写入同步进度失败: %w", err)
		}
	}
	return nil
}

// ── 校验 ─────────────────────────────────────────────────────

// Verify implements the sink contract: compare source and target row
// fingerprints. The source side is injected by the engine.
func (s *Sink) Verify(ctx context.Context, ref source.TableRef) (int64, error) {
	if s.CountSource == nil || s.FingerprintSource == nil {
		return 0, ErrNoSourceCounter
	}
	srcCount, err := s.CountSource(ctx, ref)
	if err != nil {
		return 0, err
	}
	dstCount, err := s.Count(ctx, ref)
	if err != nil {
		return 0, err
	}
	if srcCount != dstCount {
		d := srcCount - dstCount
		if d < 0 {
			d = -d
		}
		return d, nil
	}
	srcFP, err := s.FingerprintSource(ctx, ref)
	if err != nil {
		return 0, err
	}
	dstFP, err := s.Fingerprint(ctx, ref)
	if err != nil {
		return 0, err
	}
	if srcFP != dstFP {
		return -1, nil // 行数一样但内容不同：至少差 1 行，用 -1 表示「有差异」
	}
	return 0, nil
}

// Count returns the row count of a target table.
func (s *Sink) Count(ctx context.Context, ref source.TableRef) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var n int64
	err := s.conn.QueryRow(ctx, "SELECT count(*) FROM "+pgwire.Qualified(ref.Schema, ref.Table)).Scan(&n)
	return n, err
}

// Fingerprint returns count + order-independent row hash, computed server-side
// so we never pull 10M rows across the wire (docs/SPEC.md §6).
func (s *Sink) Fingerprint(ctx context.Context, ref source.TableRef) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	q := fmt.Sprintf(`SELECT count(*), COALESCE(sum(hashtextextended(t::text, 0)), 0)::text
		FROM %s t`, pgwire.Qualified(ref.Schema, ref.Table))
	var n int64
	var sum string
	if err := s.conn.QueryRow(ctx, q).Scan(&n, &sum); err != nil {
		return "", err
	}
	return fmt.Sprintf("%d:%s", n, sum), nil
}

// Table returns the target-side table definition by name (engine needs it for
// per-chunk writes without re-deriving the catalog).
func (s *Sink) Table(ref source.TableRef) *tableInfo {
	return s.tables[ref.Schema+"."+ref.Table]
}

// TableInfo is the exported view of a target table definition.
type TableInfo struct {
	Ref   source.TableRef
	Cols  []string
	PK    []string
	HasPK bool
}

// Info returns a copy of one table definition.
func (s *Sink) Info(ref source.TableRef) (TableInfo, bool) {
	ti := s.tables[ref.Schema+"."+ref.Table]
	if ti == nil {
		return TableInfo{}, false
	}
	return TableInfo{Ref: ti.ref, Cols: ti.cols, PK: ti.pk, HasPK: ti.hasPK}, true
}

// DoneChunks returns the finished chunk numbers per table (which tables a
// previous run touched at all — the fresh-recopy path needs exactly this).
func (s *Sink) DoneChunks(ctx context.Context) (map[string]map[int]bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	q := fmt.Sprintf(`SELECT table_name, chunk_no FROM %s WHERE job_key=$1 AND done`, s.stateTable("_jl_chunk"))
	rows, err := s.conn.Query(ctx, q, s.opt.JobKey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]map[int]bool{}
	for rows.Next() {
		var t string
		var n int
		if err := rows.Scan(&t, &n); err != nil {
			return nil, err
		}
		if out[t] == nil {
			out[t] = map[int]bool{}
		}
		out[t][n] = true
	}
	return out, rows.Err()
}

// TruncateTable empties a target table. It is only used when we know the rows
// in it came from half-finished initial copy of this job (chunk markers exist),
// never on a table the user filled.
func (s *Sink) TruncateTable(ctx context.Context, ref source.TableRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.conn.Exec(ctx, "TRUNCATE TABLE "+pgwire.Qualified(ref.Schema, ref.Table))
	return err
}

// tableObjectName is how a created target table is registered in `_jl_object`.
func tableObjectName(ref source.TableRef) string {
	return "table:" + ref.Schema + "." + ref.Table
}

// DidCreateTable reports whether this job created the target table (as opposed
// to reusing a table the user already had). 重搬全量前必须问这一句：
// append 模式复用用户表时，表里的行不全是我们的，不能清。
func (s *Sink) DidCreateTable(ctx context.Context, ref source.TableRef) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := fmt.Sprintf(`SELECT count(*) FROM %s WHERE object_name=$1 AND job_key=$2 AND dropped_at IS NULL`,
		s.stateTable("_jl_object"))
	var n int
	if err := s.conn.QueryRow(ctx, q, tableObjectName(ref), s.opt.JobKey).Scan(&n); err != nil {
		return false, err
	}
	return n > 0, nil
}

// RecordObject registers an object we created on the source side, so `gc` can
// later decide whether it is an orphan (docs/STATE-SCHEMA.md §4).
func (s *Sink) RecordObject(ctx context.Context, name, kind string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	q := fmt.Sprintf(`INSERT INTO %s (object_name, object_kind, job_key, instance_id)
		VALUES ($1,$2,$3,$4) ON CONFLICT (object_name) DO UPDATE SET dropped_at=NULL`,
		s.stateTable("_jl_object"))
	_, err := s.conn.Exec(ctx, q, name, kind, s.opt.JobKey, s.opt.InstanceID)
	return err
}

// recordObjectLocked is RecordObject for callers that already hold mu.
func (s *Sink) recordObjectLocked(ctx context.Context, name, kind string) error {
	q := fmt.Sprintf(`INSERT INTO %s (object_name, object_kind, job_key, instance_id)
		VALUES ($1,$2,$3,$4) ON CONFLICT (object_name) DO UPDATE SET dropped_at=NULL`,
		s.stateTable("_jl_object"))
	_, err := s.conn.Exec(ctx, q, name, kind, s.opt.JobKey, s.opt.InstanceID)
	return err
}

// MarkObjectDropped soft-deletes an object registration.
func (s *Sink) MarkObjectDropped(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	q := fmt.Sprintf(`UPDATE %s SET dropped_at=now() WHERE object_name=$1`, s.stateTable("_jl_object"))
	_, err := s.conn.Exec(ctx, q, name)
	return err
}

// Cleanup implements the sink contract: we create no long-lived target objects
// beyond the state tables (which are kept for history), so nothing to drop.
func (s *Sink) Cleanup(ctx context.Context) error { return nil }

// ── 小工具 ───────────────────────────────────────────────────

func quoteList(names []string) string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = pgwire.Quote(n)
	}
	return strings.Join(out, ", ")
}

// castedHolders renders $1::text::<coltype> so pgoutput's text values reach
// PostgreSQL through the column's own input function (bytea/array/json included).
func castedHolders(ti *tableInfo) string { return castedHoldersFor(ti, 1) }

// maxBindParams 是 PostgreSQL 扩展协议的参数上限（协议规定的 uint16）。
const maxBindParams = 65535

// effectiveBatchRows 把「每批行数」夹到「参数总数不超过 65535」的范围内。
//
// 多行 upsert 的参数数 = 行数 × 列数：10 列的宽表配上 100000 行/批 → 100 万参数，
// **每批必败、位点永不推进、任务永久卡死**，而报错跟数据毫无关系，用户根本无从下手。
// 所以这里按列数反算上限，宁可每批少几行。
func effectiveBatchRows(requested, cols int) int {
	limit := requested
	if limit <= 0 {
		limit = DefaultApplyBatchRows
	}
	if cols <= 0 {
		return limit
	}
	// 列数已经由 Open 校验过不超过 PG 的单表上限（1600），所以 max 最小是 40，恒 ≥1。
	if max := maxBindParams / cols; limit > max {
		return max
	}
	return limit
}

// batchLimitLogOnce 是**包级**的：interval 模式每轮都会新建 Sink，
// 挂在实例上的 once 会变成「每分钟一打」，等于没做去重。
var batchLimitLogOnce = &sync.Once{}

// noteBatchLimit 把生效值打进日志（全进程只打一次），让人一眼看出是配置问题。
func (s *Sink) noteBatchLimit(ti *tableInfo, limit int) {
	batchLimitLogOnce.Do(func() {
		asked := s.opt.ApplyBatchRows
		if asked <= 0 {
			asked = DefaultApplyBatchRows
		}
		if limit == asked {
			return
		}
		log.Printf("每批行数从 %d 降到 %d：%d 列 × %d 行 = %d 个参数，超过 PostgreSQL 的 %d 上限；"+
			"这是配置问题，不是数据问题（可调低 apply batch rows 或减少列数）",
			asked, limit, len(ti.cols), limit, limit*len(ti.cols), maxBindParams)
	})
}

// dedupeByPK 去掉同一批里**同一主键的较早版本**，只留最后一次。
//
// 为什么必须有：单行路径是逐条执行的，同一事务里两次改同一个主键 = 后写覆盖（合法）；
// 拼成一条多行 INSERT 后，同一个主键在一次语句里出现两次会让 Postgres 报
// "ON CONFLICT DO UPDATE command cannot affect row a second time" —— 整批失败、
// 重放还是失败，就成了永久卡死。留最后一条正好等于「后写覆盖」的最终状态。
//
// 语义边界（写进文档，因为它在有触发器/审计表的环境里是**可见差异**）：
// 去重后同一批内同一个主键只保留**最终版本**，所以目标端的 AFTER 触发器/审计只触发
// **一次**；逐条执行则会触发两次（中间态也可见）。最终行状态两者相同。
// 序列/自增列同理：只有存活的那一行消耗，表现为序列空洞数量的差别。
//
// 无主键表不参与（没有 ON CONFLICT，多行插入本身合法）。
func dedupeByPK(ti *tableInfo, evs []source.Event) []source.Event {
	if !ti.hasPK || len(ti.pkIdx) == 0 || len(evs) < 2 {
		return evs
	}
	last := make(map[string]int, len(evs))
	keys := make([]string, len(evs))
	for i, ev := range evs {
		var b strings.Builder
		for _, idx := range ti.pkIdx {
			if idx < len(ev.New.Values) {
				fmt.Fprintf(&b, "%v\x00", valueOf(ev.New.Values[idx]))
			}
		}
		keys[i] = b.String()
		last[keys[i]] = i
	}
	if len(last) == len(evs) {
		return evs
	}
	out := make([]source.Event, 0, len(last))
	for i, ev := range evs {
		if last[keys[i]] == i {
			out = append(out, ev)
		}
	}
	return out
}

// castedHoldersFor 生成 rows 行 × len(cols) 列的参数占位：
//
//	($1::text::t1, $2::text::t2), ($3::text::t1, $4::text::t2), …
//
// 编号必须**行优先**递增，且与 execUpsertChunk 里 params 的追加顺序一致——
// 错一位就是「值写进了别的列」，而且不会报错。
func castedHoldersFor(ti *tableInfo, rows int) string {
	perRow := make([]string, len(ti.cols))
	var out strings.Builder
	for r := 0; r < rows; r++ {
		if r > 0 {
			out.WriteString(", ")
		}
		out.WriteByte('(')
		for i := range ti.cols {
			if i > 0 {
				out.WriteString(", ")
			}
			n := r*len(ti.cols) + i + 1
			perRow[i] = fmt.Sprintf("$%d::text::%s", n, ti.types[i])
			out.WriteString(perRow[i])
		}
		out.WriteByte(')')
	}
	return out.String()
}

// conflictClause renders the upsert tail. Tables whose every column is part of
// the primary key can only be idempotent hits, hence DO NOTHING.
func conflictClause(ti *tableInfo) string {
	if !ti.hasPK {
		return ""
	}
	if len(ti.pk) >= len(ti.cols) {
		return fmt.Sprintf(" ON CONFLICT (%s) DO NOTHING", quoteList(ti.pk))
	}
	return fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET %s", quoteList(ti.pk), updateSetList(ti))
}

func updateSetList(ti *tableInfo) string {
	var sets []string
	pkSet := map[string]bool{}
	for _, p := range ti.pk {
		pkSet[p] = true
	}
	for _, c := range ti.cols {
		if pkSet[c] {
			continue
		}
		sets = append(sets, fmt.Sprintf("%s = EXCLUDED.%s", pgwire.Quote(c), pgwire.Quote(c)))
	}
	return strings.Join(sets, ", ")
}

// valueOf turns a decoded cell back into a driver parameter. It lives here (not
// on source.Value) because the shared contract is Ark's to change.
func valueOf(v source.Value) any {
	switch v.Kind {
	case source.KindInt:
		return v.I
	case source.KindUint:
		return v.U
	case source.KindFloat:
		return v.F
	case source.KindString, source.KindJSON:
		return v.S
	case source.KindBytes:
		return v.B
	case source.KindTime:
		return v.T
	case source.KindBool:
		return v.I != 0
	}
	return nil
}

// param 是「占位符参数」的唯一出口：跨引擎时转文本（占位符就是 ::text::type），
// 同引擎（PG→PG）保持原样，既有路径零改动。
func (s *Sink) param(v source.Value) any {
	if s.opt.SourceKind != "" && s.opt.SourceKind != catalog.Postgres {
		return textValue(v)
	}
	return valueOf(v)
}

// textValue 把**带类型的**值转成「文本形态的参数」。
//
// 为什么需要：upsert/delete 的占位符是 `$n::text::<目标类型>`（参数一律按文本传，
// 再让 PG 按目标列类型解析）——PG 源的变更流本身就是文本，天然合得上；而 MySQL 源
// 交付的是解码后的类型值（int64 / time.Time / []byte），pgx 没法把 int64 编成
// text 参数（`unable to encode 4444 into text format for text (OID 25)`），整批写入失败。
// 这里按 SQL 的约定把它们写成文本，**不改变语义**：解析仍由目标列类型决定。
//
// 只有跨引擎（SourceKind=mysql）时用：PG→PG 的既有路径保持原样，不引入回归面。
func textValue(v source.Value) any {
	switch v.Kind {
	case source.KindNull:
		return nil
	case source.KindInt:
		return strconv.FormatInt(v.I, 10)
	case source.KindUint:
		return strconv.FormatUint(v.U, 10)
	case source.KindFloat:
		// -1 精度：不丢有效位（PG 自己按目标列类型再收一次）。
		return strconv.FormatFloat(v.F, 'g', -1, 64)
	case source.KindString, source.KindJSON:
		return v.S
	case source.KindBytes:
		// `::text::bytea` 认的文本形态是反斜杠加 x 的十六进制；原样传 []byte 会被
		// 当成普通文本再转 bytea，编码错或数据坏。
		return "\\x" + hex.EncodeToString(v.B)
	case source.KindTime:
		// UTC + 微秒：目标列是 timestamp（无时区），与快照路径（time.Time → UTC）一致。
		return v.T.UTC().Format("2006-01-02 15:04:05.999999")
	case source.KindBool:
		if v.I != 0 {
			return "true"
		}
		return "false"
	}
	return nil
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}
