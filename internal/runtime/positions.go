package runtime

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

// Slot is one PostgreSQL replication slot (「同步书签」).
type Slot struct {
	Name           string
	Active         bool
	ConfirmedFlush string // LSN
	RestartLSN     string // LSN
	LagBytes       int64  // current WAL - restart_lsn
}

// Slots lists replication slots, optionally filtered by name prefix.
func (d *DB) Slots(ctx context.Context, prefix string) ([]Slot, error) {
	if d.kind != KindPostgres {
		return nil, nil // MySQL 没有 slot 这个概念
	}
	rows, err := d.db.QueryContext(ctx,
		`SELECT slot_name, active, COALESCE(confirmed_flush_lsn::text, ''),
		        COALESCE(restart_lsn::text, ''),
		        COALESCE(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint
		   FROM pg_replication_slots
		  ORDER BY slot_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Slot
	for rows.Next() {
		var s Slot
		if err := rows.Scan(&s.Name, &s.Active, &s.ConfirmedFlush, &s.RestartLSN, &s.LagBytes); err != nil {
			return nil, err
		}
		if prefix != "" && !strings.HasPrefix(s.Name, prefix) {
			continue
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// BinlogPosition returns the source's current binlog coordinate as file:pos.
// MySQL 8.4 renamed the statement; both are tried so the probe works across
// the supported families (DESIGN.md §4 capability probing, not version checks).
func (d *DB) BinlogPosition(ctx context.Context) (string, error) {
	if d.kind != KindMySQL {
		return "", nil
	}
	for _, q := range []string{`SHOW MASTER STATUS`, `SHOW BINARY LOG STATUS`} {
		rows, err := d.db.QueryContext(ctx, q)
		if err != nil {
			continue
		}
		names, err := rows.Columns()
		if err != nil {
			rows.Close()
			continue
		}
		var file, pos string
		if rows.Next() {
			vals := make([]any, len(names))
			ptrs := make([]any, len(names))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				rows.Close()
				continue
			}
			if len(vals) >= 2 {
				file = asString(vals[0])
				pos = asString(vals[1])
			}
		}
		rows.Close()
		if file != "" {
			return file + ":" + pos, nil
		}
	}
	// 退回 GTID（8.4+ 可能只暴露 GTID）
	var gtid sql.NullString
	if err := d.db.QueryRowContext(ctx, `SELECT @@gtid_executed`).Scan(&gtid); err == nil && gtid.Valid && gtid.String != "" {
		return "gtid:" + gtid.String, nil
	}
	return "", nil
}

// BinlogSettings returns log_bin / binlog_format / server_id, for doctor.
func (d *DB) BinlogSettings(ctx context.Context) (map[string]string, error) {
	out := map[string]string{}
	rows, err := d.db.QueryContext(ctx,
		`SHOW VARIABLES WHERE Variable_name IN ('log_bin','binlog_format','server_id','gtid_mode')`)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return out, err
		}
		out[strings.ToLower(k)] = v
	}
	return out, rows.Err()
}

// WalLevel returns the PostgreSQL wal_level ("" on MySQL).
func (d *DB) WalLevel(ctx context.Context) (string, error) {
	if d.kind != KindPostgres {
		return "", nil
	}
	var v string
	if err := d.db.QueryRowContext(ctx, `SELECT current_setting('wal_level')`).Scan(&v); err != nil {
		return "", err
	}
	return v, nil
}

// MaxSlotWALKeepSize returns the PG 13+ disk cap; "" when the setting does not
// exist (PG 10–12), so the caller can say "your version has no disk valve".
func (d *DB) MaxSlotWALKeepSize(ctx context.Context) string {
	if d.kind != KindPostgres {
		return ""
	}
	var v string
	if err := d.db.QueryRowContext(ctx,
		`SELECT COALESCE(current_setting('max_slot_wal_keep_size', true), '')`).Scan(&v); err != nil {
		return ""
	}
	return v
}

// ParseLSN parses PostgreSQL's "HEX/HEX" LSN into one comparable integer.
func ParseLSN(s string) (uint64, bool) {
	s = strings.TrimSpace(s)
	i := strings.IndexByte(s, '/')
	if i <= 0 || i == len(s)-1 {
		return 0, false
	}
	hi, err := strconv.ParseUint(strings.TrimSpace(s[:i]), 16, 32)
	if err != nil {
		return 0, false
	}
	lo, err := strconv.ParseUint(strings.TrimSpace(s[i+1:]), 16, 32)
	if err != nil {
		return 0, false
	}
	return hi<<32 | lo, true
}

// CompareLSN returns -1/0/1 when both sides parse. ok=false means at least one
// side is not an LSN — callers must report "无法判定", never silently pass.
func CompareLSN(a, b string) (int, bool) {
	x, ok1 := ParseLSN(a)
	y, ok2 := ParseLSN(b)
	if !ok1 || !ok2 {
		return 0, false
	}
	switch {
	case x < y:
		return -1, true
	case x > y:
		return 1, true
	}
	return 0, true
}

// BinlogPos parses "mysql-bin.000003:1234" (file, offset).
func BinlogPos(s string) (string, int64, bool) {
	i := strings.LastIndexByte(s, ':')
	if i <= 0 || i == len(s)-1 {
		return "", 0, false
	}
	n, err := strconv.ParseInt(s[i+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return s[:i], n, true
}

func asString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	case int64:
		return strconv.FormatInt(x, 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case int:
		return strconv.Itoa(x)
	}
	return fmt.Sprint(v)
}
