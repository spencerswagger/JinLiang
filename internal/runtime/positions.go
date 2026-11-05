package runtime

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/spencerswagger/jinliang/internal/source"
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

// Position parse errors. They must stay distinguishable: a position that
// belongs to another database family is E1010 (换错了源), while a corrupted
// string is E3001-ish 「无法判定」. Do not collapse them into one bool.
var (
	// ErrPositionKindMismatch means the versioned position belongs to another
	// kind (e.g. a mysql position handed to the PostgreSQL checker).
	ErrPositionKindMismatch = errors.New("位点属于另一种数据库类型（E1010）")
	// ErrPositionMalformed means the string is not a position we can parse.
	ErrPositionMalformed = errors.New("位点串格式不对")
)

// ParseLSN parses PostgreSQL's LSN into one comparable integer. It accepts
// both the bare form ("0/16B6C50") and the versioned wire format that the
// engine writes into `_jl_meta.position` ("postgres:v1:0/16B6C50").
//
// 版本化壳一律交给 source.SplitPosition（Ark 定的唯一入口），不要在这里
// 再写一遍 split —— 上一次就是漏了这层，`postgres:v1:0/16B6C50` 被按
// 「最后一个 / 之前都是十六进制」解析，整条一致性级必红。
func ParseLSN(s string) (uint64, error) {
	raw := strings.TrimSpace(s)
	payload := raw
	if kind, body, err := source.SplitPosition(raw); err == nil {
		if kind != KindPostgres {
			return 0, fmt.Errorf("%w：位点 kind=%s", ErrPositionKindMismatch, kind)
		}
		payload = body
	} else if k := positionKind(raw); k != "" && k != KindPostgres {
		// 非三段式但明显带别的连接器前缀（如 mysql:v1;file=…;pos=…）。
		return 0, fmt.Errorf("%w：位点 kind=%s", ErrPositionKindMismatch, k)
	}
	lsn, ok := parseBareLSN(payload)
	if !ok {
		return 0, fmt.Errorf("%w：%q", ErrPositionMalformed, clipPosition(raw))
	}
	return lsn, nil
}

// parseBareLSN parses "HEX/HEX" (no version shell).
func parseBareLSN(s string) (uint64, bool) {
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

// positionKind returns the leading "<kind>:" when it names a database family
// we know; "" otherwise (a bare LSN has no colon at all).
func positionKind(raw string) string {
	i := strings.IndexByte(raw, ':')
	if i <= 0 {
		return ""
	}
	switch k := raw[:i]; k {
	case KindPostgres, KindMySQL:
		return k
	}
	return ""
}

func clipPosition(s string) string {
	const max = 48
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}

// CompareLSN returns -1/0/1. The error is one of ErrPositionKindMismatch /
// ErrPositionMalformed — callers must report it, never silently pass.
func CompareLSN(a, b string) (int, error) {
	x, err := ParseLSN(a)
	if err != nil {
		return 0, err
	}
	y, err := ParseLSN(b)
	if err != nil {
		return 0, err
	}
	switch {
	case x < y:
		return -1, nil
	case x > y:
		return 1, nil
	}
	return 0, nil
}

// BinlogPos parses either a plain "mysql-bin.000003:1234" coordinate or the
// versioned form connectors write into `_jl_meta.position`:
//
//	mysql:v1;file=mysql-bin.000003;pos=1234
//	v1;file=mysql-bin.000003;pos=1234
//
// 两种格式都要认：B2/B3 一旦把带版本前缀的位点写进目标库，一致性级不能每条
// 都报 position_unparseable。带 GTID 的位点（gtid:…）这里返回 ok=false，
// 由上层明确报「无法判定」，不猜。
// ParseBinlogPos parses a MySQL binlog coordinate, accepting:
//
//	mysql-bin.000003:1234
//	mysql:v1:mysql-bin.000003:1234      (source.FormatPosition)
//	mysql:v1;file=mysql-bin.000003;pos=1234
//	v1;file=mysql-bin.000003;pos=1234
//
// 返回的错误区分「换了源」(ErrPositionKindMismatch/E1010) 与「格式坏了」
// (ErrPositionMalformed)，上层不能把两者混成同一个 position_unparseable。
func ParseBinlogPos(s string) (string, int64, error) {
	raw := strings.TrimSpace(s)
	if kind, body, err := source.SplitPosition(raw); err == nil {
		if kind != KindMySQL {
			return "", 0, fmt.Errorf("%w：位点 kind=%s", ErrPositionKindMismatch, kind)
		}
		raw = body
	} else if k := positionKind(raw); k != "" && k != KindMySQL {
		return "", 0, fmt.Errorf("%w：位点 kind=%s", ErrPositionKindMismatch, k)
	}
	if i := strings.Index(raw, "file="); i >= 0 {
		file := raw[i+len("file="):]
		if j := strings.IndexAny(file, ";,"); j >= 0 {
			file = file[:j]
		}
		file = strings.TrimSpace(file)
		posIdx := strings.Index(raw, "pos=")
		if file == "" || posIdx < 0 {
			return "", 0, fmt.Errorf("%w：%q", ErrPositionMalformed, clipPosition(raw))
		}
		posStr := raw[posIdx+len("pos="):]
		if j := strings.IndexAny(posStr, ";,"); j >= 0 {
			posStr = posStr[:j]
		}
		n, err := strconv.ParseInt(strings.TrimSpace(posStr), 10, 64)
		if err != nil {
			return "", 0, fmt.Errorf("%w：%q", ErrPositionMalformed, clipPosition(raw))
		}
		return file, n, nil
	}
	i := strings.LastIndexByte(raw, ':')
	if i <= 0 || i == len(raw)-1 {
		return "", 0, fmt.Errorf("%w：%q", ErrPositionMalformed, clipPosition(raw))
	}
	n, err := strconv.ParseInt(raw[i+1:], 10, 64)
	if err != nil {
		return "", 0, fmt.Errorf("%w：%q", ErrPositionMalformed, clipPosition(raw))
	}
	return raw[:i], n, nil
}

// BinlogPos is the bool-compatible wrapper. Prefer ParseBinlogPos when the
// caller can distinguish E1010 from a corrupted position.
func BinlogPos(s string) (string, int64, bool) {
	file, off, err := ParseBinlogPos(s)
	return file, off, err == nil
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
