// Package runtime reads and writes the runtime tables (`_jl_*`) that live in
// the target database, plus the minimum read-only probes verify/doctor/gc need
// on either side.
//
// 为什么要有这个包：verify / doctor / gc / 冲突与死信处理都要访问「目标端的
// 运行时库」，但引擎（internal/engine）与连接器（internal/mysqlsource）属于
// 别的轨道。这里只做两件事：
//  1. 用 database/sql 把「表结构、行数、按主键取行」这些通用动作封装起来，
//     两种方言各自实现一次，不复制连接器的变更流逻辑；
//  2. 读写 docs/STATE-SCHEMA.md 定义的 `_jl_*` 表，作为位点/对象登记/冲突/
//     死信的**唯一**访问点。
//
// 约束：不写用户表（ApplyJSONRow 只服务死信重放，且只按调用方给出的行写入）；
// 不碰 internal/control 与 internal/httpapi。
package runtime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/source"
)

// Kind values (mirror dsn.Kind* so callers do not import two packages).
const (
	KindPostgres = dsn.KindPostgres
	KindMySQL    = dsn.KindMySQL
)

// DB is one endpoint. It is safe for concurrent use (database/sql pool).
type DB struct {
	db     *sql.DB
	kind   string
	schema string // PG: schema (default public); MySQL: database
}

// Open parses a DSN, opens a pool and verifies the connection.
func Open(ctx context.Context, raw string) (*DB, error) {
	ep, err := dsn.Parse(raw)
	if err != nil {
		return nil, err
	}
	return OpenEndpoint(ctx, ep)
}

// OpenEndpoint opens a pool for an already-parsed endpoint.
func OpenEndpoint(ctx context.Context, ep dsn.Endpoint) (*DB, error) {
	var (
		db  *sql.DB
		err error
	)
	switch ep.Kind {
	case KindPostgres:
		db, err = sql.Open("pgx", ep.PostgresURL())
	case KindMySQL:
		db, err = sql.Open("mysql", ep.MySQLConfig())
	default:
		return nil, fmt.Errorf("不支持的数据库类型 %q", ep.Kind)
	}
	if err != nil {
		return nil, fmt.Errorf("初始化 %s 连接失败: %w", ep.Kind, err)
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(5 * time.Minute)
	d := &DB{db: db, kind: ep.Kind, schema: defaultSchema(ep)}
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("连不上 %s（%s）: %w", ep.Kind, ep.Redacted(), err)
	}
	return d, nil
}

// Wrap adapts an existing pool (tests use it with sqlmock-free fake servers).
func Wrap(db *sql.DB, kind, schema string) *DB {
	if schema == "" {
		schema = defaultSchemaForKind(kind)
	}
	return &DB{db: db, kind: kind, schema: schema}
}

func defaultSchema(ep dsn.Endpoint) string {
	if ep.Kind == KindPostgres {
		// PG 的「库」和「schema」是两层：运行时的表按 docs/STATE-SCHEMA.md
		// 落在 public。
		return "public"
	}
	return ep.Database
}

func defaultSchemaForKind(kind string) string {
	if kind == KindPostgres {
		return "public"
	}
	return ""
}

// Kind returns postgres / mysql.
func (d *DB) Kind() string { return d.kind }

// Schema returns the default schema this DB was opened with.
func (d *DB) Schema() string { return d.schema }

// WithSchema returns a shallow copy pointing at another schema/database.
func (d *DB) WithSchema(schema string) *DB {
	cp := *d
	if strings.TrimSpace(schema) != "" {
		cp.schema = schema
	}
	return &cp
}

// Close releases the pool.
func (d *DB) Close() error {
	if d == nil || d.db == nil {
		return nil
	}
	return d.db.Close()
}

// Ping checks the connection is still usable.
func (d *DB) Ping(ctx context.Context) error { return d.db.PingContext(ctx) }

// Version returns the human server version.
func (d *DB) Version(ctx context.Context) (string, error) {
	var v string
	q := `SELECT current_setting('server_version')`
	if d.kind == KindMySQL {
		q = `SELECT VERSION()`
	}
	if err := d.db.QueryRowContext(ctx, q).Scan(&v); err != nil {
		return "", err
	}
	return v, nil
}

func (d *DB) quoteIdent(s string) string {
	if s == "" {
		return s
	}
	if d.kind == KindMySQL {
		return "`" + strings.ReplaceAll(s, "`", "``") + "`"
	}
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// qualify builds schema.table with both parts quoted.
func (d *DB) qualify(schema, table string) string {
	if schema == "" {
		schema = d.schema
	}
	if schema == "" || (d.kind == KindPostgres && schema == d.schema) {
		// Still qualify for PG: search_path may be changed by the caller.
		return d.quoteIdent(schema) + "." + d.quoteIdent(table)
	}
	return d.quoteIdent(schema) + "." + d.quoteIdent(table)
}

func (d *DB) placeholder(n int) string {
	if d.kind == KindMySQL {
		return "?"
	}
	return "$" + strconv.Itoa(n)
}

// HasTable reports whether schema.table exists.
func (d *DB) HasTable(ctx context.Context, schema, table string) (bool, error) {
	if schema == "" {
		schema = d.schema
	}
	var n int
	var err error
	if d.kind == KindMySQL {
		err = d.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?`,
			schema, table).Scan(&n)
	} else {
		err = d.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2`,
			schema, table).Scan(&n)
	}
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// Table reads one table's columns and primary key. exists=false when absent
// (verify reports that instead of failing the whole run).
func (d *DB) Table(ctx context.Context, schema, table string) (source.Table, bool, error) {
	if schema == "" {
		schema = d.schema
	}
	exists, err := d.HasTable(ctx, schema, table)
	if err != nil {
		return source.Table{}, false, err
	}
	t := source.Table{TableRef: source.TableRef{Schema: schema, Table: table}}
	if !exists {
		return t, false, nil
	}

	var rows *sql.Rows
	if d.kind == KindMySQL {
		rows, err = d.db.QueryContext(ctx,
			`SELECT column_name, column_type, is_nullable = 'YES'
			   FROM information_schema.columns
			  WHERE table_schema = ? AND table_name = ?
			  ORDER BY ordinal_position`, schema, table)
	} else {
		rows, err = d.db.QueryContext(ctx,
			`SELECT a.attname, format_type(a.atttypid, a.atttypmod), NOT a.attnotnull
			   FROM pg_attribute a
			   JOIN pg_class c ON c.oid = a.attrelid
			   JOIN pg_namespace n ON n.oid = c.relnamespace
			  WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 AND NOT a.attisdropped
			  ORDER BY a.attnum`, schema, table)
	}
	if err != nil {
		return t, true, fmt.Errorf("读取 %s 的列失败: %w", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var c source.Column
		if err := rows.Scan(&c.Name, &c.Type, &c.Nullable); err != nil {
			return t, true, err
		}
		t.Columns = append(t.Columns, c)
	}
	if err := rows.Err(); err != nil {
		return t, true, err
	}

	pk, err := d.primaryKey(ctx, schema, table)
	if err != nil {
		return t, true, err
	}
	t.PrimaryKey = pk
	t.HasStablePK = len(pk) > 0
	return t, true, nil
}

func (d *DB) primaryKey(ctx context.Context, schema, table string) ([]string, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if d.kind == KindMySQL {
		rows, err = d.db.QueryContext(ctx,
			`SELECT column_name FROM information_schema.statistics
			  WHERE table_schema = ? AND table_name = ? AND index_name = 'PRIMARY'
			  ORDER BY seq_in_index`, schema, table)
	} else {
		rows, err = d.db.QueryContext(ctx,
			`SELECT a.attname
			   FROM pg_index i
			   JOIN pg_class c ON c.oid = i.indrelid
			   JOIN pg_namespace n ON n.oid = c.relnamespace
			   JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey)
			  WHERE n.nspname = $1 AND c.relname = $2 AND i.indisprimary
			  ORDER BY a.attnum`, schema, table)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// Count returns an exact row count.
func (d *DB) Count(ctx context.Context, schema, table string) (int64, error) {
	var n int64
	q := `SELECT COUNT(*) FROM ` + d.qualify(schema, table)
	if err := d.db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		return 0, fmt.Errorf("统计 %s 行数失败: %w", table, err)
	}
	return n, nil
}

// SamplePKs returns at most n primary-key tuples spread across the table.
//
// 单列整数主键用「等距取值 + >= 定位」，成本 O(n)；其余情况退化为按主键取前 n
// 行（确定性前缀抽样）。两种方式都在结果里如实说明，不假装是随机抽样。
func (d *DB) SamplePKs(ctx context.Context, schema, table string, pk []string, n int) ([][]any, error) {
	if len(pk) == 0 {
		return nil, errors.New("这张表没有主键，无法按主键抽样")
	}
	if n <= 0 {
		n = 100
	}
	cols := make([]string, len(pk))
	for i, c := range pk {
		cols[i] = d.quoteIdent(c)
	}
	selectList := strings.Join(cols, ", ")
	orderList := selectList
	from := d.qualify(schema, table)

	if len(pk) == 1 && d.numericColumn(ctx, schema, table, pk[0]) {
		var min, max sql.NullInt64
		if err := d.db.QueryRowContext(ctx,
			`SELECT MIN(`+cols[0]+`), MAX(`+cols[0]+`) FROM `+from).Scan(&min, &max); err == nil &&
			min.Valid && max.Valid && max.Int64 >= min.Int64 {
			span := max.Int64 - min.Int64
			step := span / int64(n+1)
			if step < 1 {
				step = 1
			}
			seen := map[int64]bool{}
			var out [][]any
			for v := min.Int64; v <= max.Int64 && len(out) < n; v += step {
				var got int64
				err := d.db.QueryRowContext(ctx,
					`SELECT `+cols[0]+` FROM `+from+` WHERE `+cols[0]+` >= `+d.placeholder(1)+
						` ORDER BY `+cols[0]+` LIMIT 1`, v).Scan(&got)
				if errors.Is(err, sql.ErrNoRows) {
					continue
				}
				if err != nil {
					return nil, err
				}
				if !seen[got] {
					seen[got] = true
					out = append(out, []any{got})
				}
			}
			if len(out) > 0 {
				return out, nil
			}
		}
	}

	rows, err := d.db.QueryContext(ctx,
		`SELECT `+selectList+` FROM `+from+` ORDER BY `+orderList+` LIMIT `+d.limitPlaceholder(1), n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out [][]any
	for rows.Next() {
		vals, err := scanInto(rows, len(pk))
		if err != nil {
			return nil, err
		}
		out = append(out, vals)
	}
	return out, rows.Err()
}

// limitPlaceholder keeps LIMIT parameterised on both dialects.
func (d *DB) limitPlaceholder(n int) string { return d.placeholder(n) }

// numericColumn reports whether a column is a plain integer type.
func (d *DB) numericColumn(ctx context.Context, schema, table, column string) bool {
	var typ string
	var err error
	if d.kind == KindMySQL {
		err = d.db.QueryRowContext(ctx,
			`SELECT data_type FROM information_schema.columns
			  WHERE table_schema = ? AND table_name = ? AND column_name = ?`,
			schema, table, column).Scan(&typ)
	} else {
		err = d.db.QueryRowContext(ctx,
			`SELECT t.typname FROM pg_attribute a
			   JOIN pg_class c ON c.oid = a.attrelid
			   JOIN pg_namespace n ON n.oid = c.relnamespace
			   JOIN pg_type t ON t.oid = a.atttypid
			  WHERE n.nspname = $1 AND c.relname = $2 AND a.attname = $3`,
			schema, table, column).Scan(&typ)
	}
	if err != nil {
		return false
	}
	switch strings.ToLower(typ) {
	case "int2", "int4", "int8", "smallint", "integer", "bigint",
		"tinyint", "smallint unsigned", "mediumint", "int", "bigint unsigned", "mediumint unsigned":
		return true
	}
	return false
}

// RowByPK fetches one row (all columns or the given list). found=false means
// the target does not have this primary key.
func (d *DB) RowByPK(ctx context.Context, schema, table string, columns, pk []string, values []any) ([]any, bool, error) {
	if len(pk) != len(values) {
		return nil, false, fmt.Errorf("主键列数 %d 与取值数 %d 不一致", len(pk), len(values))
	}
	if len(columns) == 0 {
		return nil, false, fmt.Errorf("RowByPK 需要显式的列清单（避免 SELECT * 的顺序漂移）")
	}
	quoted := make([]string, len(columns))
	for i, c := range columns {
		quoted[i] = d.quoteIdent(c)
	}
	cols := strings.Join(quoted, ", ")
	where := make([]string, len(pk))
	for i, c := range pk {
		where[i] = d.quoteIdent(c) + " = " + d.placeholder(i+1)
	}
	q := `SELECT ` + cols + ` FROM ` + d.qualify(schema, table) + ` WHERE ` + strings.Join(where, " AND ")
	row := d.db.QueryRowContext(ctx, q, values...)
	vals, err := scanInto(row, len(columns))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return vals, true, nil
}

// scanner is satisfied by both *sql.Row and *sql.Rows.
type scanner interface {
	Scan(dest ...any) error
}

// scanInto scans one row into a length-known []any.
func scanInto(sc scanner, n int) ([]any, error) {
	vals := make([]any, n)
	ptrs := make([]any, n)
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := sc.Scan(ptrs...); err != nil {
		return nil, err
	}
	return vals, nil
}

// QueryRows runs a read-only query and returns [][]any plus column names.
func (d *DB) QueryRows(ctx context.Context, query string, args ...any) ([][]any, []string, error) {
	rows, err := d.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	names, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	var out [][]any
	for rows.Next() {
		vals := make([]any, len(names))
		ptrs := make([]any, len(names))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, err
		}
		out = append(out, vals)
	}
	return out, names, rows.Err()
}

// Exec runs a statement (used by conflict resolve / dead-letter cleanup).
func (d *DB) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return d.db.ExecContext(ctx, query, args...)
}

// Tx runs fn in a transaction.
func (d *DB) Tx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// Raw exposes the pool for packages that need dialect-specific SQL (gc's
// source-side probes). Prefer the typed helpers elsewhere.
func (d *DB) Raw() *sql.DB { return d.db }
