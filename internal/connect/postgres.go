package connect

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/jackc/pgx/v5/stdlib" // database/sql 驱动，B1 的复制流也用同一个库

	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/source"
)

// Postgres implements source.Endpoint for PostgreSQL.
type Postgres struct {
	db *sql.DB
	ep dsn.Endpoint
}

// Kind implements source.Endpoint.
func (p *Postgres) Kind() string { return dsn.KindPostgres }

// Close releases the pool.
func (p *Postgres) Close() error { return p.db.Close() }

// Version returns the human server version, e.g. "16.2".
func (p *Postgres) Version(ctx context.Context) (string, error) {
	var v string
	if err := p.db.QueryRowContext(ctx, `SELECT current_setting('server_version')`).Scan(&v); err != nil {
		return "", err
	}
	return v, nil
}

// VersionNum returns server_version_num (e.g. 160002).
func (p *Postgres) VersionNum(ctx context.Context) (int, error) {
	var num int
	if err := p.db.QueryRowContext(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&num); err != nil {
		return 0, err
	}
	return num, nil
}

// setting reads one GUC, returning "" when the server does not have it.
func (p *Postgres) setting(ctx context.Context, name string) string {
	var v string
	if err := p.db.QueryRowContext(ctx, `SELECT current_setting($1, true)`, name).Scan(&v); err != nil {
		return ""
	}
	return v
}

// Probe implements source.Endpoint. Every capability carries its fallback so
// preflight can say "缺了也能跑，但要多花些时间" instead of refusing.
func (p *Postgres) Probe(ctx context.Context) (source.CapSet, error) {
	num, err := p.VersionNum(ctx)
	if err != nil {
		return nil, err
	}
	major := num / 10000
	walLevel := strings.ToLower(p.setting(ctx, "wal_level"))
	logical := walLevel == "logical"

	var inRecovery bool
	if err := p.db.QueryRowContext(ctx, `SELECT pg_is_in_recovery()`).Scan(&inRecovery); err != nil {
		inRecovery = false
	}
	var rolRepl, rolSuper sql.NullBool
	if err := p.db.QueryRowContext(ctx,
		`SELECT rolreplication, rolsuper FROM pg_roles WHERE rolname = current_user`,
	).Scan(&rolRepl, &rolSuper); err != nil {
		// current_user 不在 pg_roles 里（极少见）：当作没有权限，不要乐观假设
		rolRepl, rolSuper = sql.NullBool{}, sql.NullBool{}
	}
	canReplicate := rolRepl.Valid && rolRepl.Bool || (rolSuper.Valid && rolSuper.Bool)

	caps := source.CapSet{
		source.CapSnapshot: capState(true, "all", source.LevelAuto, "全量读取总是可用", ""),
		source.CapSnapshotExport: capState(major >= 9, "9.2",
			source.LevelCostly, "对端不支持导出一致性快照：初始化期间会加短暂读锁，或改用单连接读", ""),
		source.CapBinaryCopy: capState(true, "all", source.LevelAuto, "二进制 COPY 可用", ""),
		source.CapCDC: capState(logical, "9.4", source.LevelBlocked,
			"源库没开逻辑变更流（wal_level="+orUnknown(walLevel)+"），实时增量做不到；可以改用定时增量模式",
			"mode.frequency"),
		source.CapTruncate: capState(major >= 11, "11", source.LevelCostly,
			"PG 10 及更早不记录 TRUNCATE 变更：目标端可能残留已清空的表", ""),
		source.CapRowFilter: capState(major >= 15, "15", source.LevelCostly,
			"对端不支持行过滤，按表全量跟随（where 只作用于初始化）", "content.tables.*.where"),
		source.CapColumnList: capState(major >= 15, "15", source.LevelCostly,
			"对端不支持列清单，加列时会重发整行", ""),
		source.CapSlotDiskLimit: capState(major >= 13, "13", source.LevelCostly,
			"无法给书签设磁盘上限：请自己盯住磁盘，或用 guard.slot_warn_lag 告警", "guard.slot_warn_lag"),
		source.CapSlotAdvance: capState(major >= 11, "11", source.LevelAuto,
			"不能主动推进书签：只会影响 gc 的清理效率", ""),
		source.CapStreamingTx: capState(major >= 14, "14", source.LevelCostly,
			"超大事务要整段读进内存，可能吃内存", "reliability.tx_oversize"),
		source.CapTwoPhase: capState(major >= 15, "15", source.LevelCostly,
			"两阶段提交的事务会按单阶段处理，极小概率出现半提交", ""),
		source.CapGTID:           capState(false, "", source.LevelAuto, "", ""),
		source.CapBackupLock:     capState(false, "", source.LevelAuto, "", ""),
		source.CapBinlogCompress: capState(false, "", source.LevelAuto, "", ""),
	}
	// 副本与权限是两个"物理上做不到"的情况，单独标出来，让预检能给出替代方案。
	if inRecovery {
		caps[source.CapCDC] = capState(false, "", source.LevelBlocked,
			"连的是只读副本，副本上读不到变更流；请指向主库，或改用定时增量", "mode.frequency")
	} else if !canReplicate && logical {
		caps[source.CapCDC] = capState(false, "", source.LevelCostly,
			"账号没有复制权限：可以让我们用 --grant 授权，或换一个有权限的账号", "")
	}
	return caps, nil
}

// Schemas lists user schemas for the picker.
func (p *Postgres) Schemas(ctx context.Context) ([]string, error) {
	rows, err := p.db.QueryContext(ctx,
		`SELECT nspname FROM pg_namespace
		 WHERE nspname NOT LIKE 'pg\_%' AND nspname <> 'information_schema'
		 ORDER BY nspname`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// Catalog implements source.Endpoint: one schema at a time, read-only.
func (p *Postgres) Catalog(ctx context.Context, scope source.Scope) (source.Catalog, error) {
	schema := strings.TrimSpace(scope.Schema)
	if schema == "" {
		schema = "public"
	}
	type meta struct {
		kind     string
		rows     int64
		replID   string
		partFlag bool
	}
	tables := map[string]*source.Table{}
	order := []string{}
	metaBy := map[string]meta{}

	const qTables = `
		SELECT c.relname, c.relkind::text,
		       CASE WHEN c.reltuples < 0 THEN 0 ELSE c.reltuples::bigint END,
		       c.relreplident::text, c.relispartition
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relkind IN ('r','p','v','m','f')
		ORDER BY c.relname`
	rows, err := p.db.QueryContext(ctx, qTables, schema)
	if err != nil {
		return source.Catalog{}, fmt.Errorf("读取表清单失败: %w", err)
	}
	for rows.Next() {
		var name, kind, replID string
		var est int64
		var isPart bool
		if err := rows.Scan(&name, &kind, &est, &replID, &isPart); err != nil {
			rows.Close()
			return source.Catalog{}, err
		}
		if isPart {
			continue // 子分区不单列，父表标 Partitioned 就够
		}
		t := &source.Table{
			TableRef:     source.TableRef{Schema: schema, Table: name},
			RowsEstimate: est,
			Engine:       pgKindName(kind),
			Partitioned:  kind == "p",
		}
		if kind == "v" || kind == "m" {
			// 视图单独归类：它们能不能同步取决于是否有稳定主键/水位列
			t.HasStablePK = false
			metaBy[name] = meta{kind: kind, rows: est, replID: replID, partFlag: isPart}
			tables[name] = t
			order = append(order, name)
			continue
		}
		metaBy[name] = meta{kind: kind, rows: est, replID: replID, partFlag: isPart}
		tables[name] = t
		order = append(order, name)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return source.Catalog{}, err
	}

	// 排序规则单独取：attcollation 与类型自带的不同 = 这一列显式 COLLATE 过。
	// 不取的话，跨库搬过去会悄悄落到目标库的默认排序规则上——upsert 的判重
	// 和 ORDER BY 的结果都会变，而且是「看起来跑通了」的那种变。
	const qCols = `
		SELECT c.relname, a.attname, format_type(a.atttypid, a.atttypmod),
		       NOT a.attnotnull, pg_get_expr(d.adbin, d.adrelid), col_description(c.oid, a.attnum),
		       CASE WHEN a.attcollation <> t.typcollation THEN co.collname END,
		       a.atttypid::bigint
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_type t ON t.oid = a.atttypid
		LEFT JOIN pg_collation co ON co.oid = a.attcollation
		LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
		WHERE n.nspname = $1 AND c.relkind IN ('r','p','v','m','f')
		  AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY c.relname, a.attnum`
	crows, err := p.db.QueryContext(ctx, qCols, schema)
	if err != nil {
		return source.Catalog{}, fmt.Errorf("读取列信息失败: %w", err)
	}
	for crows.Next() {
		var table, col, typ string
		var nullable bool
		var typeOID int64
		var def, comment, collation sql.NullString
		if err := crows.Scan(&table, &col, &typ, &nullable, &def, &comment, &collation, &typeOID); err != nil {
			crows.Close()
			return source.Catalog{}, err
		}
		t, ok := tables[table]
		if !ok {
			continue
		}
		c := source.Column{Name: col, Type: typ, Nullable: nullable, Comment: comment.String,
			Collation: collation.String, TypeOID: typeOID}
		if def.Valid {
			s := def.String
			c.Default = &s
		}
		t.Columns = append(t.Columns, c)
	}
	crows.Close()
	if err := crows.Err(); err != nil {
		return source.Catalog{}, err
	}

	const qPK = `
		SELECT c.relname, a.attname
		FROM pg_index i
		JOIN pg_class c ON c.oid = i.indrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
		WHERE n.nspname = $1 AND i.indisprimary
		ORDER BY c.relname, array_position(i.indkey, a.attnum)`
	prows, err := p.db.QueryContext(ctx, qPK, schema)
	if err != nil {
		return source.Catalog{}, fmt.Errorf("读取主键失败: %w", err)
	}
	for prows.Next() {
		var table, col string
		if err := prows.Scan(&table, &col); err != nil {
			prows.Close()
			return source.Catalog{}, err
		}
		if t, ok := tables[table]; ok {
			t.PrimaryKey = append(t.PrimaryKey, col)
		}
	}
	prows.Close()
	if err := prows.Err(); err != nil {
		return source.Catalog{}, err
	}

	var cat source.Catalog
	for _, name := range order {
		t := tables[name]
		m := metaBy[name]
		t.HasStablePK = len(t.PrimaryKey) > 0
		t.ReplicaIdentity = pgReplicaIdentity(m.replID, t.HasStablePK)
		if m.kind == "v" || m.kind == "m" {
			cat.Views = append(cat.Views, t.TableRef)
			continue
		}
		cat.Tables = append(cat.Tables, *t)
	}
	return cat, nil
}

func pgKindName(kind string) string {
	switch kind {
	case "r":
		return "table"
	case "p":
		return "partitioned table"
	case "v":
		return "view"
	case "m":
		return "materialized view"
	case "f":
		return "foreign table"
	}
	return kind
}

func pgReplicaIdentity(id string, hasPK bool) string {
	switch id {
	case "f":
		return "full"
	case "n":
		return "nothing"
	case "i":
		return "index"
	}
	if hasPK {
		return "default(pk)"
	}
	return "default(no-pk)"
}

func orUnknown(s string) string {
	if s == "" {
		return "未知"
	}
	return s
}
