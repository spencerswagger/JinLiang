package runtime

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Runtime table names, per docs/STATE-SCHEMA.md.
const (
	TableObject     = "_jl_object"
	TableLease      = "_jl_lease"
	TableMeta       = "_jl_meta"
	TableConflict   = "_jl_conflict"
	TableDeadLetter = "_jl_dead_letter"
	TableAction     = "_jl_action"
	TableInstance   = "_jl_instance"
	TableVersion    = "_jl_version"
	TableChunk      = "_jl_chunk"
)

// Object is one row of _jl_object: an object we created (slot/publication/…).
type Object struct {
	Name       string
	Kind       string
	JobKey     string
	InstanceID string
	CreatedAt  time.Time
}

// Lease is one row of _jl_lease.
type Lease struct {
	JobKey     string
	InstanceID string
	ExpiresAt  time.Time
}

// Meta is one row of _jl_meta (the authoritative position lives here).
type Meta struct {
	JobKey    string
	Name      string
	Phase     string
	Position  string
	UpdatedAt time.Time
}

// Conflict is one row of _jl_conflict.
type Conflict struct {
	ID        int64
	JobKey    string
	Table     string
	PK        string
	Policy    string
	SourceRow string
	TargetRow string
	Resolved  bool
	CreatedAt time.Time
}

// DeadLetter is one row of _jl_dead_letter.
type DeadLetter struct {
	ID        int64
	JobKey    string
	Table     string
	Reason    string
	Position  string
	RawRow    string
	CreatedAt time.Time
}

// Tables reports which `_jl_*` tables exist in this database.
func (d *DB) Tables(ctx context.Context) (map[string]bool, error) {
	var rows *sql.Rows
	var err error
	if d.kind == KindMySQL {
		rows, err = d.db.QueryContext(ctx,
			`SELECT table_name FROM information_schema.tables
			  WHERE table_schema = ? AND table_name LIKE '\_jl\_%'`, d.schema)
	} else {
		rows, err = d.db.QueryContext(ctx,
			`SELECT table_name FROM information_schema.tables
			  WHERE table_schema = $1 AND table_name LIKE '\_jl\_%'`, d.schema)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		out[name] = true
	}
	return out, rows.Err()
}

// Objects lists live (not soft-deleted) registered objects.
func (d *DB) Objects(ctx context.Context) ([]Object, error) {
	q := `SELECT object_name, object_kind, job_key, instance_id, created_at
	        FROM ` + d.qualify(d.schema, TableObject) + ` WHERE dropped_at IS NULL ORDER BY object_name`
	rows, err := d.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Object
	for rows.Next() {
		var o Object
		var created time.Time
		if err := rows.Scan(&o.Name, &o.Kind, &o.JobKey, &o.InstanceID, &created); err != nil {
			return nil, err
		}
		o.CreatedAt = created
		out = append(out, o)
	}
	return out, rows.Err()
}

// Leases lists every lease row (expired ones included; gc decides).
func (d *DB) Leases(ctx context.Context) ([]Lease, error) {
	q := `SELECT job_key, instance_id, expires_at FROM ` + d.qualify(d.schema, TableLease)
	rows, err := d.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Lease
	for rows.Next() {
		var l Lease
		if err := rows.Scan(&l.JobKey, &l.InstanceID, &l.ExpiresAt); err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

// Meta loads the job's row. found=false when the job never ran.
func (d *DB) Meta(ctx context.Context, jobKey string) (Meta, bool, error) {
	q := `SELECT job_key, name, phase, COALESCE(position, ''), updated_at
	        FROM ` + d.qualify(d.schema, TableMeta) + ` WHERE job_key = ` + d.placeholder(1)
	var m Meta
	err := d.db.QueryRowContext(ctx, q, jobKey).Scan(&m.JobKey, &m.Name, &m.Phase, &m.Position, &m.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Meta{}, false, nil
	}
	if err != nil {
		return Meta{}, false, err
	}
	return m, true, nil
}

// Conflicts lists conflicts for a job (empty jobKey = all), newest first.
func (d *DB) Conflicts(ctx context.Context, jobKey string, limit int) ([]Conflict, error) {
	if limit <= 0 || limit > 1000 {
		limit = 200
	}
	where := ""
	var args []any
	if jobKey != "" {
		where = ` WHERE job_key = ` + d.placeholder(1)
		args = append(args, jobKey)
	}
	q := `SELECT id, job_key, table_name, pk, policy,
	             COALESCE(source_row::text, ''), COALESCE(target_row::text, ''),
	             resolved, created_at
	        FROM ` + d.qualify(d.schema, TableConflict) + where +
		` ORDER BY id DESC LIMIT ` + strconv.Itoa(limit)
	if d.kind == KindMySQL {
		q = `SELECT id, job_key, table_name, pk, policy,
	                COALESCE(CAST(source_row AS CHAR), ''), COALESCE(CAST(target_row AS CHAR), ''),
	                resolved, created_at
	           FROM ` + d.qualify(d.schema, TableConflict) + where +
			` ORDER BY id DESC LIMIT ` + strconv.Itoa(limit)
	}
	rows, err := d.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Conflict
	for rows.Next() {
		var c Conflict
		var resolved any
		if err := rows.Scan(&c.ID, &c.JobKey, &c.Table, &c.PK, &c.Policy,
			&c.SourceRow, &c.TargetRow, &resolved, &c.CreatedAt); err != nil {
			return nil, err
		}
		c.Resolved = truthy(resolved)
		out = append(out, c)
	}
	return out, rows.Err()
}

// ResolveConflict marks one conflict resolved. The data itself is only changed
// by dead-letter retry / a future engine path; this records the decision.
func (d *DB) ResolveConflict(ctx context.Context, id int64, resolved bool) error {
	v := 0
	if resolved {
		v = 1
	}
	_, err := d.db.ExecContext(ctx,
		`UPDATE `+d.qualify(d.schema, TableConflict)+` SET resolved = `+d.placeholder(1)+
			` WHERE id = `+d.placeholder(2), v, id)
	return err
}

// DeadLetters lists dead letters for a job (empty jobKey = all), oldest first
// so retry replays in position order.
func (d *DB) DeadLetters(ctx context.Context, jobKey string, limit int) ([]DeadLetter, error) {
	if limit <= 0 || limit > 10000 {
		limit = 500
	}
	where := ""
	var args []any
	if jobKey != "" {
		where = ` WHERE job_key = ` + d.placeholder(1)
		args = append(args, jobKey)
	}
	raw := `COALESCE(raw_row::text, '')`
	if d.kind == KindMySQL {
		raw = `COALESCE(CAST(raw_row AS CHAR), '')`
	}
	q := `SELECT id, job_key, table_name, reason, COALESCE(position, ''), ` + raw + `, created_at
	        FROM ` + d.qualify(d.schema, TableDeadLetter) + where +
		` ORDER BY id ASC LIMIT ` + strconv.Itoa(limit)
	rows, err := d.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeadLetter
	for rows.Next() {
		var dl DeadLetter
		if err := rows.Scan(&dl.ID, &dl.JobKey, &dl.Table, &dl.Reason, &dl.Position, &dl.RawRow, &dl.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, dl)
	}
	return out, rows.Err()
}

// GetDeadLetter loads one dead letter by id.
func (d *DB) GetDeadLetter(ctx context.Context, id int64) (DeadLetter, bool, error) {
	raw := `COALESCE(raw_row::text, '')`
	if d.kind == KindMySQL {
		raw = `COALESCE(CAST(raw_row AS CHAR), '')`
	}
	q := `SELECT id, job_key, table_name, reason, COALESCE(position, ''), ` + raw + `, created_at
	        FROM ` + d.qualify(d.schema, TableDeadLetter) + ` WHERE id = ` + d.placeholder(1)
	var dl DeadLetter
	err := d.db.QueryRowContext(ctx, q, id).Scan(&dl.ID, &dl.JobKey, &dl.Table, &dl.Reason,
		&dl.Position, &dl.RawRow, &dl.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return DeadLetter{}, false, nil
	}
	if err != nil {
		return DeadLetter{}, false, err
	}
	return dl, true, nil
}

// DeleteDeadLetter removes a retried row (kept in _jl_action for audit).
func (d *DB) DeleteDeadLetter(ctx context.Context, id int64) error {
	_, err := d.db.ExecContext(ctx,
		`DELETE FROM `+d.qualify(d.schema, TableDeadLetter)+` WHERE id = `+d.placeholder(1), id)
	return err
}

// InsertAction leaves an audit row for something we did.
func (d *DB) InsertAction(ctx context.Context, jobKey, actionID, what, actor string) error {
	q := `INSERT INTO ` + d.qualify(d.schema, TableAction) +
		` (job_key, action_id, what, actor) VALUES (` +
		d.placeholder(1) + `, ` + d.placeholder(2) + `, ` + d.placeholder(3) + `, ` + d.placeholder(4) + `)`
	_, err := d.db.ExecContext(ctx, q, jobKey, actionID, what, actor)
	return err
}

// ApplyJSONRow upserts one row from a decoded JSON object into schema.table.
//
// 只服务「死信重放」这一条路径：调用方给出主键与整行，列名必须在目标表里
// 真实存在（防止把任意键名拼进 SQL）。与数据同事务的位点推进不在这里做 —
// 重放一条死信不改变权威位点。
func (d *DB) ApplyJSONRow(ctx context.Context, schema, table string, pk []string, row map[string]any) error {
	if len(row) == 0 {
		return errors.New("死信里没有可写入的列")
	}
	if len(pk) == 0 {
		return fmt.Errorf("目标表 %s 没有主键，死信不能按行重放（无主键表只能整表重搬）", table)
	}
	t, exists, err := d.Table(ctx, schema, table)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("目标表 %s 不存在", table)
	}
	known := map[string]bool{}
	for _, c := range t.Columns {
		known[c.Name] = true
	}
	for k := range row {
		if !known[k] {
			return fmt.Errorf("死信里的列 %q 在目标表 %s 中不存在（结构已变？）", k, table)
		}
	}
	for _, c := range pk {
		if _, ok := row[c]; !ok {
			return fmt.Errorf("死信里缺少主键列 %q", c)
		}
	}

	cols := make([]string, 0, len(row))
	for k := range row {
		cols = append(cols, k)
	}
	sort.Strings(cols)

	quoted := make([]string, len(cols))
	marks := make([]string, len(cols))
	args := make([]any, len(cols))
	for i, c := range cols {
		quoted[i] = d.quoteIdent(c)
		marks[i] = d.placeholder(i + 1)
		args[i] = normalizeJSONValue(row[c])
	}
	pkSet := map[string]bool{}
	for _, c := range pk {
		pkSet[c] = true
	}
	var updates []string
	for _, c := range cols {
		if pkSet[c] {
			continue
		}
		if d.kind == KindMySQL {
			updates = append(updates, d.quoteIdent(c)+" = VALUES("+d.quoteIdent(c)+")")
		} else {
			updates = append(updates, d.quoteIdent(c)+" = EXCLUDED."+d.quoteIdent(c))
		}
	}
	pkQuoted := make([]string, len(pk))
	for i, c := range pk {
		pkQuoted[i] = d.quoteIdent(c)
	}
	q := `INSERT INTO ` + d.qualify(schema, table) + ` (` + strings.Join(quoted, ", ") + `) VALUES (` +
		strings.Join(marks, ", ") + `) `
	if d.kind == KindMySQL {
		if len(updates) == 0 {
			q += ` ON DUPLICATE KEY UPDATE ` + pkQuoted[0] + ` = ` + pkQuoted[0]
		} else {
			q += ` ON DUPLICATE KEY UPDATE ` + strings.Join(updates, ", ")
		}
	} else {
		if len(updates) == 0 {
			q += ` ON CONFLICT (` + strings.Join(pkQuoted, ", ") + `) DO NOTHING`
		} else {
			q += ` ON CONFLICT (` + strings.Join(pkQuoted, ", ") + `) DO UPDATE SET ` + strings.Join(updates, ", ")
		}
	}
	_, err = d.db.ExecContext(ctx, q, args...)
	return err
}

// normalizeJSONValue keeps precision for integers and passes drivers values
// they can encode (json.Number must not reach the driver).
func normalizeJSONValue(v any) any {
	switch x := v.(type) {
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i
		}
		if f, err := x.Float64(); err == nil {
			return f
		}
		return string(x)
	case float64:
		if x == float64(int64(x)) {
			return int64(x)
		}
		return x
	case json.RawMessage:
		return string(x)
	}
	return v
}

func truthy(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case int64:
		return x != 0
	case []byte:
		return len(x) > 0 && x[0] != '0'
	case string:
		return x == "1" || strings.EqualFold(x, "true")
	}
	return false
}
