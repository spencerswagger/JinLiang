package connect

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql" // B1 的 binlog 流也用同一个库

	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/source"
)

// MySQL implements source.Endpoint for MySQL / MariaDB.
type MySQL struct {
	db *sql.DB
	ep dsn.Endpoint
}

// Kind implements source.Endpoint.
func (m *MySQL) Kind() string { return dsn.KindMySQL }

// Close releases the pool.
func (m *MySQL) Close() error { return m.db.Close() }

// Version returns VERSION() (e.g. "8.0.36" or "8.0.36-0ubuntu0.22.04.1").
func (m *MySQL) Version(ctx context.Context) (string, error) {
	var v string
	if err := m.db.QueryRowContext(ctx, `SELECT VERSION()`).Scan(&v); err != nil {
		return "", err
	}
	return v, nil
}

// Probe implements source.Endpoint.
func (m *MySQL) Probe(ctx context.Context) (source.CapSet, error) {
	ver, err := m.Version(ctx)
	if err != nil {
		return nil, err
	}
	major, minor, patch := versionParts(ver)

	var logBinRaw, binlogFormat, gtidMode sql.NullString
	_ = m.db.QueryRowContext(ctx, `SELECT @@log_bin, @@binlog_format, @@gtid_mode`).Scan(&logBinRaw, &binlogFormat, &gtidMode)
	logBin := logBinRaw.String == "1" || strings.EqualFold(logBinRaw.String, "ON")
	format := strings.ToUpper(binlogFormat.String)
	cdcReady := logBin && format == "ROW"
	gtid := strings.EqualFold(gtidMode.String, "ON")

	// 8.0.20+ / 8.4 之后若干能力才出现；用版本号判断，不猜。
	atLeast := func(maj, min, pat int) bool {
		if major != maj {
			return major > maj
		}
		if minor != min {
			return minor > min
		}
		return patch >= pat
	}

	caps := source.CapSet{
		source.CapSnapshot: capState(true, "all", source.LevelAuto, "全量读取总是可用", ""),
		source.CapSnapshotExport: capState(false, "", source.LevelCostly,
			"MySQL 没有一致性快照导出：初始化和增量之间会有极小窗口的重复，靠水位列/主键去重", ""),
		source.CapCDC: capState(cdcReady, "5.1", source.LevelBlocked,
			"源库的变更流水没开好（log_bin="+orUnknown(logBinRaw.String)+"，binlog_format="+orUnknown(format)+
				"），实时增量做不到；可以改用定时增量模式", "mode.frequency"),
		source.CapTruncate: capState(cdcReady, "5.1", source.LevelCostly,
			"TRUNCATE 在行格式下只记录语句：目标端会按「清空」处理，可能丢过滤条件", ""),
		source.CapRowFilter: capState(false, "", source.LevelCostly,
			"MySQL 的变更流不支持行过滤，只能按表全量跟随", ""),
		source.CapColumnList: capState(false, "", source.LevelCostly,
			"MySQL 的变更流不带列清单，加列时会重发整行", ""),
		source.CapGTID: capState(gtid, "5.6", source.LevelCostly,
			"没开 GTID：位点只能依赖文件名+偏移，主从切换后可能对不上", ""),
		source.CapBackupLock: capState(atLeast(8, 0, 0), "8.0",
			source.LevelCostly, "不支持 LOCK INSTANCE FOR BACKUP：初始化会加一次短暂全局读锁", ""),
		source.CapBinlogCompress: capState(atLeast(8, 0, 20), "8.0.20",
			source.LevelAuto, "变更流水不压缩：网络传输量略大", ""),
		source.CapBinaryCopy:    capState(false, "", source.LevelAuto, "", ""),
		source.CapSlotDiskLimit: capState(false, "", source.LevelAuto, "", ""),
		source.CapSlotAdvance:   capState(false, "", source.LevelAuto, "", ""),
		source.CapStreamingTx: capState(atLeast(8, 0, 20), "8.0.20", source.LevelCostly,
			"超大事务要整段读进内存，可能吃内存", "reliability.tx_oversize"),
		source.CapTwoPhase: capState(atLeast(8, 0, 0), "8.0", source.LevelCostly,
			"MySQL 的 XA 事务按单阶段处理，极小概率出现半提交", ""),
	}
	return caps, nil
}

// Schemas lists databases the account can see.
func (m *MySQL) Schemas(ctx context.Context) ([]string, error) {
	rows, err := m.db.QueryContext(ctx,
		`SELECT SCHEMA_NAME FROM information_schema.SCHEMATA
		 WHERE SCHEMA_NAME NOT IN ('information_schema','performance_schema','mysql','sys')
		 ORDER BY SCHEMA_NAME`)
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

// Catalog implements source.Endpoint for one database (MySQL has no schemas
// inside a database, so scope.Schema means the database name).
func (m *MySQL) Catalog(ctx context.Context, scope source.Scope) (source.Catalog, error) {
	db := strings.TrimSpace(scope.Schema)
	if db == "" {
		db = m.ep.Database
	}
	tables := map[string]*source.Table{}
	order := []string{}
	views := map[string]bool{}

	const qTables = `
		SELECT TABLE_NAME, TABLE_TYPE, COALESCE(TABLE_ROWS,0), COALESCE(ENGINE,'')
		FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?
		ORDER BY TABLE_NAME`
	rows, err := m.db.QueryContext(ctx, qTables, db)
	if err != nil {
		return source.Catalog{}, fmt.Errorf("读取表清单失败: %w", err)
	}
	for rows.Next() {
		var name, typ, engine string
		var est int64
		if err := rows.Scan(&name, &typ, &est, &engine); err != nil {
			rows.Close()
			return source.Catalog{}, err
		}
		isView := strings.EqualFold(typ, "VIEW")
		views[name] = isView
		tables[name] = &source.Table{
			TableRef:     source.TableRef{Schema: db, Table: name},
			RowsEstimate: est,
			Engine:       engine,
		}
		order = append(order, name)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return source.Catalog{}, err
	}

	const qCols = `
		SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE,
		       COLUMN_KEY, COLUMN_DEFAULT, COALESCE(COLUMN_COMMENT,'')
		FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ?
		ORDER BY TABLE_NAME, ORDINAL_POSITION`
	crows, err := m.db.QueryContext(ctx, qCols, db)
	if err != nil {
		return source.Catalog{}, fmt.Errorf("读取列信息失败: %w", err)
	}
	for crows.Next() {
		var table, col, typ, nullable, key, comment string
		var def sql.NullString
		if err := crows.Scan(&table, &col, &typ, &nullable, &key, &def, &comment); err != nil {
			crows.Close()
			return source.Catalog{}, err
		}
		t, ok := tables[table]
		if !ok {
			continue
		}
		c := source.Column{Name: col, Type: typ, Nullable: strings.EqualFold(nullable, "YES"), Comment: comment}
		if def.Valid {
			s := def.String
			c.Default = &s
		}
		t.Columns = append(t.Columns, c)
		if strings.EqualFold(key, "PRI") {
			t.PrimaryKey = append(t.PrimaryKey, col)
		}
	}
	crows.Close()
	if err := crows.Err(); err != nil {
		return source.Catalog{}, err
	}

	// 唯一索引（TARGET-02 的核对依据，docs/DESIGN-NO-PRIMARY-KEY.md §3.4）：
	// 只收「非主键、非前缀（SUB_PART IS NULL）、非表达式（COLUMN_NAME 非空）」的
	// 全列唯一索引。含 NULL 的列**照收**——NULL 是否构成唯一性由 preflight 按列
	// 可空性判（判据 5），目录层不替它下结论。
	const qUniq = `
		SELECT TABLE_NAME, INDEX_NAME, COLUMN_NAME
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = ? AND NON_UNIQUE = 0
		  AND INDEX_NAME <> 'PRIMARY'
		  AND SUB_PART IS NULL
		  AND COLUMN_NAME IS NOT NULL
		ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX`
	urows, err := m.db.QueryContext(ctx, qUniq, db)
	if err != nil {
		return source.Catalog{}, fmt.Errorf("读取唯一索引失败: %w", err)
	}
	type idxKey struct{ table, index string }
	uniq := map[idxKey][]string{}
	var uniqOrder []idxKey
	for urows.Next() {
		var table, index, col string
		if err := urows.Scan(&table, &index, &col); err != nil {
			urows.Close()
			return source.Catalog{}, err
		}
		k := idxKey{table, index}
		if _, ok := uniq[k]; !ok {
			uniqOrder = append(uniqOrder, k)
		}
		uniq[k] = append(uniq[k], col)
	}
	urows.Close()
	if err := urows.Err(); err != nil {
		return source.Catalog{}, err
	}
	for _, k := range uniqOrder {
		if t, ok := tables[k.table]; ok {
			t.UniqueKeys = append(t.UniqueKeys, uniq[k])
		}
	}

	var cat source.Catalog
	for _, name := range order {
		t := tables[name]
		t.HasStablePK = len(t.PrimaryKey) > 0
		if views[name] {
			cat.Views = append(cat.Views, t.TableRef)
			continue
		}
		if t.HasStablePK {
			t.ReplicaIdentity = "primary key"
		} else {
			t.ReplicaIdentity = "no-pk"
		}
		cat.Tables = append(cat.Tables, *t)
	}
	return cat, nil
}
