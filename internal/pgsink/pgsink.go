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
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

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
}

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
	tables, order := buildTables(opt.Catalog)
	s := &Sink{opt: opt, tables: tables, order: order}
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

// buildTables indexes the catalog by "schema.table" and keeps a stable order.
func buildTables(cat source.Catalog) (map[string]*tableInfo, []string) {
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
			ti.types = append(ti.types, c.Type)
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

// Close releases the connection.
func (s *Sink) Close() {
	if s.conn != nil {
		_ = s.conn.Close(context.Background())
	}
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
			instance_id text PRIMARY KEY,
			label       text,
			host        text,
			first_seen  timestamptz NOT NULL DEFAULT now(),
			last_seen   timestamptz NOT NULL DEFAULT now()
		)`, s.stateTable("_jl_instance")),
	}
	for _, stmt := range ddl {
		if _, err := s.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("建状态表失败（目标库账号需要建表权限）: %w", err)
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
// 判断顺序在 leaseTakeover 里（纯函数，有单测）：过期可接管；别的实例不可；
// 同一台机器上「pid 还活着」不可、「pid 已死」立刻接管（kill -9 后的重启
// 不用等 30 秒）。抢租约用一条带条件的 UPDATE，命中 0 行说明被抢先了。
func (s *Sink) AcquireLease(ctx context.Context, ttl time.Duration) (bool, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	label := leaseOwnerLabel(s.opt.JobName, os.Getpid())
	now := time.Now()

	cur, exists, err := s.readLeaseLocked(ctx)
	if err != nil {
		return false, "", err
	}
	ok, why := leaseTakeover(cur, exists, now, s.opt.InstanceID, os.Getpid(), processAlive)
	if !ok {
		return false, why, nil
	}

	if !exists {
		q := fmt.Sprintf(`INSERT INTO %s (job_key, instance_id, label, expires_at)
			VALUES ($1,$2,$3, now() + $4::interval)`, s.stateTable("_jl_lease"))
		if _, err := s.conn.Exec(ctx, q, s.opt.JobKey, s.opt.InstanceID, label, ttl); err != nil {
			if pgwire.SQLState(err) == "23505" { // unique_violation：被抢先
				return false, "另一个进程刚刚抢先拿到了租约", nil
			}
			return false, "", err
		}
		return true, why, nil
	}

	q := fmt.Sprintf(`UPDATE %s SET instance_id=$1, label=$2, expires_at=now() + $3::interval
		WHERE job_key=$4 AND (expires_at < now() OR instance_id=$1)`,
		s.stateTable("_jl_lease"))
	tag, err := s.conn.Exec(ctx, q, s.opt.InstanceID, label, ttl, s.opt.JobKey)
	if err != nil {
		return false, "", err
	}
	if tag.RowsAffected() == 0 {
		return false, "另一个实例刚刚抢先拿到了租约", nil
	}
	return true, why, nil
}

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
		WHERE job_key=$2 AND instance_id=$3`, s.stateTable("_jl_lease"))
	tag, err := s.conn.Exec(ctx, q, ttl, s.opt.JobKey, s.opt.InstanceID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// ReleaseLease drops our claim so the next run starts immediately.
func (s *Sink) ReleaseLease(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	q := fmt.Sprintf(`DELETE FROM %s WHERE job_key=$1 AND instance_id=$2`, s.stateTable("_jl_lease"))
	_, err := s.conn.Exec(ctx, q, s.opt.JobKey, s.opt.InstanceID)
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
				return fmt.Errorf("%w: 目标表 %s 的列 %s 是 %s，源端是 %s",
					ErrTypeMismatch, ti.ref.Table, col, typ, ti.types[i])
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

// ── 全量：一块一个事务（数据 + 块断点 + 位点） ───────────────────

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

	// 块断点：重启时能看出「哪些块已经落地」，也让人能核对进度。
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

	for _, ev := range evs {
		switch ev.Kind {
		case source.EvInsert, source.EvUpdate:
			if err := s.upsertTx(ctx, tx, ev); err != nil {
				return err
			}
		case source.EvDelete:
			if err := s.deleteTx(ctx, tx, ev); err != nil {
				return err
			}
		case source.EvTruncate:
			if _, err := tx.Exec(ctx, "TRUNCATE TABLE "+pgwire.Qualified(ev.Table.Schema, ev.Table.Table)); err != nil {
				return fmt.Errorf("截断目标表 %s 失败: %w", ev.Table.Table, err)
			}
		default:
			// Begin/Commit/Heartbeat/DDL 由引擎处理，不落 SQL
		}
	}
	if advance {
		if err := s.setPositionTx(ctx, tx, pos.String()); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// endsWithCommit reports whether this batch closes on a committed transaction.
// 半个事务的批次（续段没有收尾）不许推进位点。
func endsWithCommit(evs []source.Event) bool {
	return len(evs) > 0 && evs[len(evs)-1].Kind == source.EvCommit
}

func (s *Sink) upsertTx(ctx context.Context, tx pgx.Tx, ev source.Event) error {
	ti := s.tables[ev.Table.Schema+"."+ev.Table.Table]
	if ti == nil {
		return fmt.Errorf("收到未知表 %s 的变更（发布里多了表？）", ev.Table.Table)
	}
	cols := quoteList(ti.cols)
	holders := castedHolders(ti)
	q := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", pgwire.Qualified(ti.ref.Schema, ti.ref.Table), cols, holders)
	q += conflictClause(ti)
	params := make([]any, len(ti.cols))
	for i := range ti.cols {
		if i < len(ev.New.Values) {
			params[i] = valueOf(ev.New.Values[i])
		}
	}
	if _, err := tx.Exec(ctx, q, params...); err != nil {
		return fmt.Errorf("写入 %s 失败: %w", ev.Table.Table, err)
	}
	return nil
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
			v = valueOf(ev.Old.Values[idx])
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

// DoneChunks returns the finished chunk numbers per table (restart diagnostics).
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

// ResetChunks clears chunk markers: a new snapshot cannot reuse an old
// snapshot id, so the copy restarts (upsert keeps it idempotent).
func (s *Sink) ResetChunks(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	q := fmt.Sprintf(`DELETE FROM %s WHERE job_key=$1`, s.stateTable("_jl_chunk"))
	_, err := s.conn.Exec(ctx, q, s.opt.JobKey)
	return err
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
func castedHolders(ti *tableInfo) string {
	out := make([]string, len(ti.cols))
	for i := range ti.cols {
		out[i] = fmt.Sprintf("$%d::text::%s", i+1, ti.types[i])
	}
	return strings.Join(out, ", ")
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

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}
