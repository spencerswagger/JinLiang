package mysqlsource

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/spencerswagger/jinliang/internal/source"
)

// 分块全量的「会话」：开一致性快照 → 分块读 → 结束。
//
// 为什么要拆成三步（而不是一个 SnapshotChunks 一把梭）：
//   - 引擎的 `_jl_meta.position` 必须在**读第一块之前**就写进目标库（与清块标记同一个
//     事务），否则崩在「搬了数据、没定位点」之间，下一次会以「首次」的名义重搬，
//     停机期间源端删掉的行就永远留在目标端；
//   - 位点只能在「同一个瞬间」拿到：先 FTWRL 读位点、再开一致性快照（拿不到 RELOAD 时
//     退化成先开快照再读位点，与旧实现一致），这一步必须与后面的读用**同一个连接**；
//   - 续跑时不需要新位点，但需要同一个读视图继续读剩下的块。
//
// 形状与 pgsource 对齐（BeginSnapshot / SnapshotChunksFrom / EndSnapshot），引擎因此
// 只需要一套块记账逻辑，不必为每种源写一遍。

// ChunkResume 是「这张表从哪接着读」。与 pgsource.ChunkResume 同形状：
// 全部来自目标库的 `_jl_chunk`（range_to + chunk_no），源端不存任何进度。
type ChunkResume struct {
	// Cursor 是上一块右边界（不含）。空 = 从表头读。
	Cursor string
	// LastNo 是上一块已提交的块号；下一块从 LastNo+1 开始编号。
	// 编号必须接着往上排，否则 `_jl_chunk` 会在主键上撞车、把旧块的边界覆盖掉。
	LastNo int
}

// snapshotSession 是一次分块全量的读会话。
type snapshotSession struct {
	conn   *sql.Conn
	pos    Position
	tables []source.Table
	ended  bool
}

var errNoSnapshotSession = fmt.Errorf("没有正在进行的快照会话（内部错误：BeginSnapshot 没被调用）")

// BeginSnapshot 开一个一致性快照并读回位点；返回的位点就是这次全量的增量起点。
//
// tables 是本次要搬的表（**只搬这些**）：引擎按 job 选表，连接器不替它做决定——
// 早期实现里这里读的是整个库的 catalog，结果「只同步一张表」的任务会悄悄搬全库。
func (s *Source) BeginSnapshot(ctx context.Context, tables []source.Table) (Position, error) {
	return s.beginSnapshot(ctx, tables, true)
}

// BeginSnapshotRead 只开一致性读视图，**不读位点、不拿全局读锁**。
//
// 续跑（引擎已经有位点、只是接着读剩下的块）用它：`FLUSH TABLES WITH READ LOCK`
// 会瞬间挡住源库的所有写入，而续跑根本不需要新位点——为了一个用不上的值去锁库，
// 等于把「续跑」变成一次小型停写事故。缺 RELOAD 权限的账号也会在这里白报一次错。
func (s *Source) BeginSnapshotRead(ctx context.Context, tables []source.Table) error {
	_, err := s.beginSnapshot(ctx, tables, false)
	return err
}

func (s *Source) beginSnapshot(ctx context.Context, tables []source.Table, needPosition bool) (Position, error) {
	if len(tables) == 0 {
		return Position{}, fmt.Errorf("没有要搬的表（source.scope / content.tables 都是空的）")
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return Position{}, err
	}
	var stmt string
	if needPosition {
		probe, err := NewProber(s.db).Probe(ctx)
		if err != nil {
			conn.Close()
			return Position{}, err
		}
		stmt = probe.StatusStatement
		if stmt == "" {
			conn.Close()
			return Position{}, capabilityError("缺「读位点」权限",
				"读不到当前位点，全量搬完没法接上增量",
				"按预检 MY-03 给账号补 REPLICATION CLIENT，然后重跑",
				"没有位点就只能整表重搬，且增量接不上")
		}
	}

	// 位点与快照必须同一瞬间：能拿全局读锁就先拿锁读位点（最严格），
	// 拿不到（缺 RELOAD / 云上禁用）就按旧行为「先开快照，再读位点」——
	// 后者的窗口极短（同一连接上的两条语句），而且位点只会比快照更晚，
	// 方向是「多重放一段」，由 upsert 幂等吸收，不会丢。
	// needPosition=false（续跑）时这两步都跳过：不锁库、不读位点。
	var pos Position
	locked := false
	if needPosition {
		if _, err := conn.ExecContext(ctx, "FLUSH TABLES WITH READ LOCK"); err == nil {
			locked = true
			pos, err = s.readStatusConn(ctx, conn, stmt)
			if err != nil {
				_, _ = conn.ExecContext(ctx, "UNLOCK TABLES")
				conn.Close()
				return Position{}, err
			}
		}
	}
	if _, err := conn.ExecContext(ctx, "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		if locked {
			_, _ = conn.ExecContext(ctx, "UNLOCK TABLES")
		}
		conn.Close()
		return Position{}, err
	}
	if _, err := conn.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
		if locked {
			_, _ = conn.ExecContext(ctx, "UNLOCK TABLES")
		}
		conn.Close()
		return Position{}, fmt.Errorf("开一致性快照失败: %w", err)
	}
	if needPosition && !locked {
		pos, err = s.readStatusConn(ctx, conn, stmt)
		if err != nil {
			_, _ = conn.ExecContext(ctx, "ROLLBACK")
			conn.Close()
			return Position{}, err
		}
	}
	if locked {
		if _, err := conn.ExecContext(ctx, "UNLOCK TABLES"); err != nil {
			_, _ = conn.ExecContext(ctx, "ROLLBACK")
			conn.Close()
			return Position{}, err
		}
	}

	s.snapMu.Lock()
	if s.snap != nil && !s.snap.ended {
		// 上一轮没结束（正常路径下 EndSnapshot 一定被 defer 调用）：先收掉，
		// 不能两个读会话同时挂在同一个 Source 上。
		old := s.snap
		s.snap = nil
		s.snapMu.Unlock()
		old.close()
		s.snapMu.Lock()
	}
	s.snap = &snapshotSession{conn: conn, pos: pos, tables: tables}
	s.snapMu.Unlock()
	return pos, nil
}

// SnapshotChunksFrom 用 BeginSnapshot 开的事务分块读，resume 里的表从游标之后接着读。
//
// 没有 BeginSnapshot 时自己开一个（引擎的通用目标端路径就是这么用的）：这样
// 「分块全量」只有一份实现，两条路径不会分叉。
func (s *Source) SnapshotChunksFrom(ctx context.Context, opt source.SnapshotOpt,
	resume map[string]ChunkResume, fn ChunkHandler) (Position, error) {

	if opt.ChunkRows <= 0 {
		opt.ChunkRows = 100_000
	}
	sess, own, err := s.currentSnapshot(ctx)
	if err != nil {
		return Position{}, err
	}
	if own {
		defer s.EndSnapshot()
	}
	for _, t := range sess.tables {
		if err := ctx.Err(); err != nil {
			return sess.pos, err
		}
		from := resume[t.Table]
		if err := s.eachChunkFrom(ctx, sess.conn, t, opt.ChunkRows, from, fn); err != nil {
			return sess.pos, err
		}
	}
	return sess.pos, nil
}

// SnapshotChunks 保持原语义（自己开会话、自己收）：不续跑、整轮搬。
func (s *Source) SnapshotChunks(ctx context.Context, opt source.SnapshotOpt, fn ChunkHandler) (Position, error) {
	tables, err := s.allTables(ctx)
	if err != nil {
		return Position{}, err
	}
	if _, err := s.BeginSnapshot(ctx, tables); err != nil {
		return Position{}, err
	}
	defer s.EndSnapshot()
	return s.SnapshotChunksFrom(ctx, opt, nil, fn)
}

// EndSnapshot 结束一致性快照（回滚事务、关连接）。可重复调用。
func (s *Source) EndSnapshot() {
	s.snapMu.Lock()
	sess := s.snap
	s.snap = nil
	s.snapMu.Unlock()
	if sess != nil {
		sess.close()
	}
}

// currentSnapshot 取当前会话；没有就自己开一个（own=true 表示调用方负责收）。
func (s *Source) currentSnapshot(ctx context.Context) (sess *snapshotSession, own bool, err error) {
	s.snapMu.Lock()
	cur := s.snap
	s.snapMu.Unlock()
	if cur != nil && !cur.ended {
		return cur, false, nil
	}
	tables, err := s.allTables(ctx)
	if err != nil {
		return nil, false, err
	}
	// 续跑（没有会话）只开读视图：不拿全局读锁、不读位点——位点已经在目标库里，
	// 这里再去锁一次源库是纯粹的风险。
	if err := s.BeginSnapshotRead(ctx, tables); err != nil {
		return nil, false, err
	}
	s.snapMu.Lock()
	cur = s.snap
	s.snapMu.Unlock()
	if cur == nil {
		return nil, false, errNoSnapshotSession
	}
	return cur, true, nil
}

func (sess *snapshotSession) close() {
	if sess == nil || sess.ended {
		return
	}
	sess.ended = true
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, _ = sess.conn.ExecContext(ctx, "ROLLBACK")
	_ = sess.conn.Close()
}

// allTables 返回「这一轮要搬的表」。
//
// 优先用引擎交过来的选表（SetKnownTables）——续跑时引擎不会开新的快照会话
// （Prepare(false)），这条路径如果去读整库 catalog，就会把**没选中的表**也搬一遍：
// 目标端没有它们的定义，直接报「目标端没有这张表的定义」，而看起来像是引擎的 bug。
// 只有调用方从没交过选表（SnapshotChunks 的旧入口）时才回落整库。
func (s *Source) allTables(ctx context.Context) ([]source.Table, error) {
	s.knownMu.Lock()
	known := append([]source.Table(nil), s.knownList...)
	s.knownMu.Unlock()
	if len(known) > 0 {
		return known, nil
	}
	cat, err := s.Catalog(ctx, source.Scope{})
	if err != nil {
		return nil, err
	}
	return cat.Tables, nil
}

// —— 游标编解码 ——
//
// 块边界（`_jl_chunk.range_to`）要能**原样**变成下一次查询的 WHERE 参数。这里必须
// **带类型**：二进制主键（BINARY(16) 存 UUID）与时间主键（DATETIME(6)）一旦被打印成
// 文本，还原出来的就是字符串，`WHERE (pk) > ('[0 0 …]')` 会按字节比——`'['`=0x5B
// 让绝大多数 UUID 落在它之前，续跑**直接跳到表尾**：已提交块之后的行不再读，却照常
// MarkTableDone、退出码 0、指纹只跟「自己读到的部分」一致。这是「看起来成功」里
// 最难查的一类，所以编码按类型分开，解码还原成驱动认的类型（二进制给 []byte，
// 时间给 MySQL 认的字面量），**不**复用 PG bytea 的 `\x…` 形态。

// cursorPart 是块边界里的一个主键值，带类型标记。
//
//	t: n=null i=int u=uint f=float s=string b=bytes(hex) d=datetime(MySQL 字面量)
type cursorPart struct {
	T string `json:"t"`
	V string `json:"v,omitempty"`
}

// EncodeCursor 把主键值编码成块边界文本（空键 → 空串 = 表头）。
func EncodeCursor(vals []source.Value) string {
	if len(vals) == 0 {
		return ""
	}
	parts := make([]cursorPart, len(vals))
	for i, v := range vals {
		parts[i] = encodeCursorPart(v)
	}
	b, err := json.Marshal(parts)
	if err != nil {
		return ""
	}
	return string(b)
}

func encodeCursorPart(v source.Value) cursorPart {
	switch v.Kind {
	case source.KindNull:
		return cursorPart{T: "n"}
	case source.KindInt:
		return cursorPart{T: "i", V: strconv.FormatInt(v.I, 10)}
	case source.KindUint:
		return cursorPart{T: "u", V: strconv.FormatUint(v.U, 10)}
	case source.KindFloat:
		return cursorPart{T: "f", V: strconv.FormatFloat(v.F, 'g', -1, 64)}
	case source.KindString, source.KindJSON:
		return cursorPart{T: "s", V: v.S}
	case source.KindBytes:
		// 十六进制：二进制列按字节比大小，还原成 []byte 才保序。
		return cursorPart{T: "b", V: hex.EncodeToString(v.B)}
	case source.KindTime:
		// MySQL 认的字面量（UTC + 微秒）。**不要**用 time.Time 的 %v 形态
		// （`… +0000 UTC`）：MySQL 解析不了，或被截到秒 → 边界行重读或漏读。
		return cursorPart{T: "d", V: v.T.UTC().Format("2006-01-02 15:04:05.999999")}
	case source.KindBool:
		if v.I != 0 {
			return cursorPart{T: "i", V: "1"}
		}
		return cursorPart{T: "i", V: "0"}
	}
	return cursorPart{T: "s", V: v.S}
}

// decodeCursor 把块边界文本还原成比较参数；读不懂就报错（**不静默从表头重读**：
// 那会把已搬完的块再搬一遍，虽然幂等，但会让「续跑」变成「整表重扫」而用户不知道）。
//
// 老边界（上一版写的纯字符串数组）仍然认：升级过程中目标库里可能还留着它。
func decodeCursor(s string) ([]any, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	var parts []cursorPart
	if err := json.Unmarshal([]byte(s), &parts); err != nil {
		var legacy []string
		if err2 := json.Unmarshal([]byte(s), &legacy); err2 == nil {
			out := make([]any, len(legacy))
			for i, p := range legacy {
				out[i] = p
			}
			return out, nil
		}
		return nil, fmt.Errorf("块边界 %q 读不懂（不是本工具写的？）：%w", s, err)
	}
	out := make([]any, len(parts))
	for i, p := range parts {
		v, err := decodeCursorPart(p)
		if err != nil {
			return nil, fmt.Errorf("块边界第 %d 个键解不开（%s=%q）: %w", i+1, p.T, p.V, err)
		}
		out[i] = v
	}
	return out, nil
}

func decodeCursorPart(p cursorPart) (any, error) {
	switch p.T {
	case "n":
		return nil, nil
	case "i":
		n, err := strconv.ParseInt(p.V, 10, 64)
		return n, err
	case "u":
		n, err := strconv.ParseUint(p.V, 10, 64)
		return n, err
	case "f":
		f, err := strconv.ParseFloat(p.V, 64)
		return f, err
	case "s":
		return p.V, nil
	case "b":
		b, err := hex.DecodeString(p.V)
		if err != nil {
			return nil, err
		}
		return b, nil
	case "d":
		return p.V, nil // MySQL 按列类型解析这个字面量
	}
	return nil, fmt.Errorf("不认识的类型标记 %q", p.T)
}

// eachChunkFrom 按 keyset 页切块，可以从 from.Cursor 之后接着读，块号从 from.LastNo+1 起。
func (s *Source) eachChunkFrom(ctx context.Context, conn *sql.Conn, t source.Table, chunk int64,
	from ChunkResume, fn ChunkHandler) error {

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
	no := from.LastNo

	if len(pk) == 0 {
		// 无主键表：没有 keyset 边界，但**必须分页**——一次 `SELECT *` 把 1000 万行
		// 读进内存就是 OOM，而且与「一页 = 一块」的自述不符。
		// 用 LIMIT/OFFSET 分页：一致性快照内数据不漂，OFFSET 的代价在无主键表上可接受
		// （本来也只能单连接顺序扫）；块边界留空表示「无序块」（引擎据此只做表级断点）。
		for offset := 0; ; offset += int(chunk) {
			q := fmt.Sprintf("SELECT %s FROM %s LIMIT %d OFFSET %d", selectList, table, chunk, offset)
			rows, err := s.scanRowsAny(ctx, conn, q, nil, t)
			if err != nil {
				return err
			}
			if len(rows) == 0 {
				return nil
			}
			no++
			if err := fn(ctx, Chunk{Table: t.TableRef, No: no, Rows: rows, From: "", To: ""}); err != nil {
				return err
			}
			if int64(len(rows)) < chunk {
				return nil
			}
		}
	}

	var lastKey []source.Value
	if from.Cursor != "" {
		args, err := decodeCursor(from.Cursor)
		if err != nil {
			return err
		}
		if len(args) != len(pk) {
			return fmt.Errorf("%s 的块边界有 %d 个键，而主键有 %d 列：位点与本任务对不上（换过主键？）",
				t.Table, len(args), len(pk))
		}
		// 续跑：游标是「上一块的右边界」，直接当上一页的 lastKey 用。
		// **必须按原类型还原**（toValue 认得 []byte / int64 / string …）：一律包成
		// KindString 会把二进制主键退化成文本，`WHERE (pk) > ('[0 0 …]')` 按字节比
		// 就跳到表尾——已提交块之后的行不再读，却照常 MarkTableDone、退出码 0。
		lastKey = make([]source.Value, len(args))
		for i, a := range args {
			lastKey[i] = toValue(a)
		}
	}

	for {
		var (
			q    string
			args []any
		)
		if lastKey == nil {
			q = fmt.Sprintf("SELECT %s FROM %s ORDER BY %s LIMIT %d", selectList, table, strings.Join(pk, ", "), chunk)
		} else {
			args = make([]any, len(lastKey))
			for i, v := range lastKey {
				args[i] = driverValue(v)
			}
			q = fmt.Sprintf("SELECT %s FROM %s WHERE (%s) > (%s) ORDER BY %s LIMIT %d",
				selectList, table, strings.Join(pk, ", "), placeholders(len(pk)), strings.Join(pk, ", "), chunk)
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
		var anyRows [][]any
		n := 0
		fromKey := keyString(lastKey)
		for rows.Next() {
			row, err := scanRow(rows, names, t)
			if err != nil {
				rows.Close()
				return err
			}
			anyRows = append(anyRows, toAnyRow(row))
			if len(pkIdx) > 0 {
				lastKey = make([]source.Value, len(pkIdx))
				for i, idx := range pkIdx {
					lastKey[i] = row.Values[idx]
				}
			}
			n++
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		if n == 0 {
			return nil
		}
		no++
		if err := fn(ctx, Chunk{Table: t.TableRef, No: no, Rows: anyRows, From: fromKey, To: keyString(lastKey)}); err != nil {
			return err
		}
		if int64(n) < chunk {
			return nil
		}
	}
}
