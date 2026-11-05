package mysqlsource

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/spencerswagger/jinliang/internal/source"
)

// Catalog implements source.Endpoint for one database (MySQL has no schema
// level inside a database, so scope.Schema is the database name).
func (s *Source) Catalog(ctx context.Context, scope source.Scope) (source.Catalog, error) {
	db := strings.TrimSpace(scope.Schema)
	if db == "" {
		db = s.ep.Database
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT TABLE_NAME, TABLE_TYPE, COALESCE(TABLE_ROWS,0), COALESCE(ENGINE,'')
		FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?
		ORDER BY TABLE_NAME`, db)
	if err != nil {
		return source.Catalog{}, fmt.Errorf("读表清单失败: %w", err)
	}
	defer rows.Close()

	var cat source.Catalog
	index := map[string]*source.Table{}
	var order []string
	views := map[string]bool{}
	for rows.Next() {
		var name, typ, engine string
		var est int64
		if err := rows.Scan(&name, &typ, &est, &engine); err != nil {
			return source.Catalog{}, err
		}
		isView := strings.EqualFold(typ, "VIEW")
		views[name] = isView
		order = append(order, name)
		if isView {
			cat.Views = append(cat.Views, source.TableRef{Schema: db, Table: name})
			continue
		}
		t := &source.Table{
			TableRef:     source.TableRef{Schema: db, Table: name},
			RowsEstimate: est,
			Engine:       engine,
		}
		index[name] = t
	}

	cols, err := s.db.QueryContext(ctx, `
		SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_DEFAULT, COALESCE(COLUMN_COMMENT,'')
		FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ?
		ORDER BY TABLE_NAME, ORDINAL_POSITION`, db)
	if err != nil {
		return source.Catalog{}, fmt.Errorf("读列信息失败: %w", err)
	}
	defer cols.Close()
	for cols.Next() {
		var table, col, typ, nullable, key, comment string
		var def sql.NullString
		if err := cols.Scan(&table, &col, &typ, &nullable, &key, &def, &comment); err != nil {
			return source.Catalog{}, err
		}
		t, ok := index[table]
		if !ok {
			continue
		}
		c := source.Column{Name: col, Type: typ, Nullable: strings.EqualFold(nullable, "YES"), Comment: comment}
		if def.Valid {
			v := def.String
			c.Default = &v
		}
		t.Columns = append(t.Columns, c)
		if strings.EqualFold(key, "PRI") {
			t.PrimaryKey = append(t.PrimaryKey, col)
		}
	}
	if err := cols.Err(); err != nil {
		return source.Catalog{}, err
	}

	for _, name := range order {
		t, ok := index[name]
		if !ok {
			continue
		}
		t.HasStablePK = len(t.PrimaryKey) > 0
		if t.HasStablePK {
			t.ReplicaIdentity = "primary key"
		} else {
			t.ReplicaIdentity = "no-pk"
		}
		cat.Tables = append(cat.Tables, *t)
	}
	return cat, nil
}

// SnapshotNote records how the consistency point was taken, so `plan` can tell
// the truth about the overlap window instead of implying a perfect cut.
type SnapshotNote struct {
	TookGlobalLock bool
	Detail         string
}

// Snapshot reads the existing rows inside one consistent read view and returns
// the checkpoint the change stream should start from.
//
// 顺序（docs/SNAPSHOT.md §3）：先拿全局读锁（毫秒级），取位点，再开一致性快照，
// 然后解锁。锁的时间只覆盖「取位点 + 开快照」两条语句；数据读取全程无锁。
// 云上禁锁时降级为「先开快照、再取位点」，靠 upsert 去重，代价写进 SnapshotNote。
func (s *Source) Snapshot(ctx context.Context, opt source.SnapshotOpt, onRow source.RowHandler) (source.Position, error) {
	if opt.ChunkRows <= 0 {
		opt.ChunkRows = 100_000
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return Position{}, err
	}
	defer conn.Close()

	probe, err := NewProber(s.db).Probe(ctx)
	if err != nil {
		return Position{}, err
	}
	stmt := probe.StatusStatement
	if stmt == "" {
		return Position{}, fmt.Errorf("读不到源库位点：既没有 SHOW BINARY LOG STATUS 也没有 SHOW MASTER STATUS，" +
			"全量搬完后无法接上增量；请先解决权限（见预检 MY-03）")
	}

	var (
		pos  Position
		note SnapshotNote
	)
	locked := false
	if stmt != "" {
		if _, err := conn.ExecContext(ctx, "FLUSH TABLES WITH READ LOCK"); err == nil {
			locked = true
			note.TookGlobalLock = true
			note.Detail = "取位点时加了毫秒级全局读锁，初始化与增量之间没有窗口"
		} else {
			note.Detail = "源库不允许全局读锁，改为先开一致性快照再取位点：接缝处可能有极小重复，靠 upsert 去重"
		}
	}
	if locked {
		pos, err = s.readStatusConn(ctx, conn, stmt)
		if err != nil {
			_, _ = conn.ExecContext(ctx, "UNLOCK TABLES")
			return Position{}, err
		}
	}
	if _, err := conn.ExecContext(ctx, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		if locked {
			_, _ = conn.ExecContext(ctx, "UNLOCK TABLES")
		}
		return Position{}, err
	}
	if _, err := conn.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
		if locked {
			_, _ = conn.ExecContext(ctx, "UNLOCK TABLES")
		}
		return Position{}, fmt.Errorf("开一致性快照失败: %w", err)
	}
	if !locked && stmt != "" {
		pos, err = s.readStatusConn(ctx, conn, stmt)
		if err != nil {
			_, _ = conn.ExecContext(ctx, "ROLLBACK")
			return Position{}, err
		}
	}
	if locked {
		if _, err := conn.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
			_, _ = conn.ExecContext(ctx, "ROLLBACK")
			return Position{}, err
		}
	}
	// 快照事务在整段读取期间保持打开；读完再结束。
	defer conn.ExecContext(context.WithoutCancel(ctx), "COMMIT")

	scope := source.Scope{}
	cat, err := s.Catalog(ctx, scope)
	if err != nil {
		return Position{}, err
	}
	for _, t := range cat.Tables {
		if err := s.snapshotTable(ctx, conn, t, opt.ChunkRows, onRow); err != nil {
			return Position{}, err
		}
	}
	s.lastNote = note
	return pos, nil
}

// LastSnapshotNote returns how the most recent Snapshot took its consistency
// point (for the plan/preflight output).
func (s *Source) LastSnapshotNote() SnapshotNote { return s.lastNote }

func (s *Source) readStatusConn(ctx context.Context, conn *sql.Conn, stmt string) (Position, error) {
	takenAt := time.Now().UTC().Truncate(time.Millisecond)
	rows, err := conn.QueryContext(ctx, stmt)
	if err != nil {
		return Position{}, fmt.Errorf("执行 %s 失败: %w", stmt, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return Position{}, err
	}
	if !rows.Next() {
		return Position{}, fmt.Errorf("源库没有返回位点")
	}
	raw := make([]sql.RawBytes, len(cols))
	dest := make([]any, len(cols))
	for i := range raw {
		dest[i] = &raw[i]
	}
	if err := rows.Scan(dest...); err != nil {
		return Position{}, err
	}
	pos := Position{TakenAt: takenAt}
	for i, name := range cols {
		v := string(raw[i])
		switch strings.ToLower(name) {
		case "file":
			pos.File = v
		case "position":
			var n uint64
			if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
				return Position{}, fmt.Errorf("位点 %q 不是数字", v)
			}
			pos.Pos = uint32(n)
		case "executed_gtid_set":
			if set, err := ParseGTIDSet(v); err == nil {
				pos.GTID = set
			}
		}
	}
	if id, ok := s.readServerID(ctx); ok {
		pos.ServerID = id
	}
	return pos, nil
}

// snapshotTable walks one table with keyset pagination on the primary key.
//
// 无主键表只能单连接顺序扫（InnoDB 的隐藏 rowid 读不到），失败整表重来；
// 这一点在预检里已经说清楚，不因为表大就拒绝。
func (s *Source) snapshotTable(ctx context.Context, conn *sql.Conn, t source.Table, chunk int64, onRow source.RowHandler) error {
	cols := make([]string, 0, len(t.Columns))
	for _, c := range t.Columns {
		cols = append(cols, quoteIdent(c.Name))
	}
	if len(cols) == 0 {
		return nil
	}
	selectList := strings.Join(cols, ", ")
	table := quoteIdent(t.Schema) + "." + quoteIdent(t.Table)
	pk := make([]string, 0, len(t.PrimaryKey))
	for _, c := range t.PrimaryKey {
		pk = append(pk, quoteIdent(c))
	}

	var (
		query string
		args  []any
	)
	if len(pk) > 0 {
		// 第一块不带 WHERE；后续按 (pk...) > (…) 的 keyset 游标推进。
		query = fmt.Sprintf("SELECT %s FROM %s ORDER BY %s LIMIT %d",
			selectList, table, strings.Join(pk, ", "), chunk)
	} else {
		query = fmt.Sprintf("SELECT %s FROM %s LIMIT %d", selectList, table, chunk)
	}

	var lastKey []source.Value
	for {
		q := query
		if len(pk) > 0 && lastKey != nil {
			args = make([]any, len(lastKey))
			for i, v := range lastKey {
				args[i] = driverValue(v)
			}
			q = fmt.Sprintf("SELECT %s FROM %s WHERE (%s) > (%s) ORDER BY %s LIMIT %d",
				selectList, table, strings.Join(pk, ", "), placeholders(len(pk)),
				strings.Join(pk, ", "), chunk)
		}
		rows, err := conn.QueryContext(ctx, q, args...)
		if err != nil {
			return fmt.Errorf("读 %s.%s 失败: %w", t.Schema, t.Table, err)
		}
		names, err := rows.Columns()
		if err != nil {
			rows.Close()
			return err
		}
		pkIdx := pkIndexes(t, names)
		n := 0
		for rows.Next() {
			vals := make([]any, len(names))
			ptrs := make([]any, len(names))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				rows.Close()
				return err
			}
			row := source.Row{
				Table:  source.TableRef{Schema: t.Schema, Table: t.Table},
				Values: make([]source.Value, len(vals)),
			}
			for i, v := range vals {
				row.Values[i] = toValue(v)
			}
			if len(pkIdx) > 0 {
				lastKey = make([]source.Value, len(pkIdx))
				for i, idx := range pkIdx {
					lastKey[i] = row.Values[idx]
				}
			}
			if err := onRow(ctx, row); err != nil {
				rows.Close()
				return err
			}
			n++
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if int64(n) < chunk || len(pk) == 0 {
			return nil
		}
	}
}

func pkIndexes(t source.Table, names []string) []int {
	out := make([]int, 0, len(t.PrimaryKey))
	for _, pk := range t.PrimaryKey {
		for i, n := range names {
			if strings.EqualFold(n, pk) {
				out = append(out, i)
				break
			}
		}
	}
	return out
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
}

func quoteIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

// toValue maps a driver-scanned Go value onto the shared Value type.
func toValue(v any) source.Value {
	switch x := v.(type) {
	case nil:
		return source.Value{Kind: source.KindNull}
	case int64:
		return source.Value{Kind: source.KindInt, I: x}
	case uint64:
		return source.Value{Kind: source.KindUint, U: x}
	case float64:
		return source.Value{Kind: source.KindFloat, F: x}
	case float32:
		return source.Value{Kind: source.KindFloat, F: float64(x)}
	case bool:
		if x {
			return source.Value{Kind: source.KindBool, I: 1}
		}
		return source.Value{Kind: source.KindBool, I: 0}
	case string:
		return source.Value{Kind: source.KindString, S: x}
	case []byte:
		if isPrintableUTF8(x) {
			return source.Value{Kind: source.KindString, S: string(x)}
		}
		return source.Value{Kind: source.KindBytes, B: append([]byte(nil), x...)}
	case time.Time:
		return source.Value{Kind: source.KindTime, T: x.UTC()}
	default:
		return source.Value{Kind: source.KindString, S: fmt.Sprint(x)}
	}
}

// driverValue turns a checkpoint value back into something the driver accepts.
func driverValue(v source.Value) any {
	switch v.Kind {
	case source.KindNull:
		return nil
	case source.KindInt:
		return v.I
	case source.KindUint:
		return v.U
	case source.KindFloat:
		return v.F
	case source.KindString:
		return v.S
	case source.KindBytes:
		return v.B
	case source.KindTime:
		return v.T
	default:
		return v.S
	}
}
