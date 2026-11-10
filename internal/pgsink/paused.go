package pgsink

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/pgwire"
	"github.com/spencerswagger/jinliang/internal/source"
)

// 按表暂停的存储（docs/STATE-SCHEMA.md 的 `_jl_meta`）：**一列 JSON 文本**，不是新表。
//
// 形状（一行一个 job_key）：
//
//	{"orders":{"reason":"type_changed","at":"2026-11-10T08:59:00Z","detail":"v: 目标 text / 源 bigint"}}
//
// 三条硬约束，改这段代码前先读：
//  1. **key = 目标端裸表名**，与 `_jl_chunk.table_name` **逐字节相同**（不带 schema、
//     不用源端原始大小写）。两边同源，第③步的实现与核对才不会各说一套。
//  2. **暂停登记是独立一条 UPDATE**，绝不与 `position` 同一条。位点只在数据事务里前进，
//     暂停不是数据推进的副作用——否则「暂停」会顺手把位点带走，一段变更就再没人重放。
//  3. 只删一个 key、只改一个 key：**先读后写**，绝不拿内存里的表集合重建成整列
//     （那会把别的表的记录、以及不归本轮管的记录一起抹掉）。
type PausedTable struct {
	// Reason 只放稳定码（type_changed / schema_drift），中文句子留给 Detail。
	Reason string `json:"reason"`
	// At 是**第一次**登记的时间；已有记录再登记不覆盖它（否则每次重跑都会刷新，
	// 用户就看不出这张表到底停了多久）。
	At string `json:"at"`
	// Detail 给人看：哪一列、两端各是什么。
	Detail string `json:"detail"`
}

// StoredJobHash 读回状态行里记的 job_hash（第一次建行时写的那个）。
// 暂停记录的跨轮存活要拿它比：任务语义变了，旧的「这张表停着」就未必还成立。
func (s *Sink) StoredJobHash(ctx context.Context) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var h string
	q := fmt.Sprintf(`SELECT COALESCE(job_hash,'') FROM %s WHERE job_key=$1`, s.stateTable("_jl_meta"))
	err := s.conn.QueryRow(ctx, q, s.opt.JobKey).Scan(&h)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return h, err
}

// PausedTables 读回当前记录的暂停表（status / doctor / UI 用；不看内存缓存）。
func (s *Sink) PausedTables(ctx context.Context) (map[string]PausedTable, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out, _, err := s.readPausedLocked(ctx)
	return out, err
}

// PausedDirty 表示库里的 paused_tables 不是合法 JSON（已按「无暂停」继续处理）。
// 引擎据此发一条 note：不说的话用户会以为暂停记录还在。
func (s *Sink) PausedDirty() bool { return s.pausedDirty }

// LoadPausedTables 把库里的暂停表读进内存缓存。**prepare 之后只读一次**：
// 写路径（Append/Apply）每批都要判定，不能每批查库；而暂停记录只在本轮 prepare
// 与「某表搬成」这两处变化，读一次足够。
func (s *Sink) LoadPausedTables(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, dirty, err := s.readPausedLocked(ctx)
	if err != nil {
		return err
	}
	s.pausedCache = cur
	s.pausedDirty = dirty
	return nil
}

// PauseTable 登记一张表的暂停——**唯一的写点**。
//
// key 用 ref.Table（目标端裸表名）：与 `_jl_chunk.table_name` 同源，逐字节一致。
func (s *Sink) PauseTable(ctx context.Context, ref source.TableRef, reason, detail string) error {
	if strings.TrimSpace(reason) == "" {
		// 原因码是机器判等的凭据，空值等于没登记。
		return fmt.Errorf("内部错误：登记暂停表 %s 时没有原因码", ref.Table)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	cur, _, err := s.readPausedTxLocked(ctx, tx)
	if err != nil {
		return err
	}
	at := time.Now().UTC().Format(time.RFC3339)
	if old, ok := cur[ref.Table]; ok && old.At != "" {
		at = old.At // 第一次登记的时间不覆盖：用户要能看出「停了多久」
	}
	cur[ref.Table] = PausedTable{Reason: reason, At: at, Detail: detail}
	if err := s.writePausedTxLocked(ctx, tx, cur); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if s.pausedCache == nil {
		s.pausedCache = map[string]PausedTable{}
	}
	s.pausedCache[ref.Table] = cur[ref.Table]
	return nil
}

// ClearPausedTable 删掉**这一个** key。只动它，别的表记录原样留着。
func (s *Sink) ClearPausedTable(ctx context.Context, ref source.TableRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	cur, _, err := s.readPausedTxLocked(ctx, tx)
	if err != nil {
		return err
	}
	if _, ok := cur[ref.Table]; !ok {
		return nil // 本来就没记（幂等）
	}
	delete(cur, ref.Table)
	if err := s.writePausedTxLocked(ctx, tx, cur); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	delete(s.pausedCache, ref.Table)
	return nil
}

// isPausedLocked 是写路径上的快速判定（调用方已持锁；缓存由 LoadPausedTables 装好）。
func (s *Sink) isPausedLocked(table string) bool {
	if s.pausedCache == nil {
		return false
	}
	_, ok := s.pausedCache[table]
	return ok
}

// readPausedLocked 读整列并解析。dirty=true 表示存的不是合法 JSON：
// 契约要求「按无暂停继续 + 发 note」，不能因此把整轮同步打死。
func (s *Sink) readPausedLocked(ctx context.Context) (map[string]PausedTable, bool, error) {
	var raw string
	q := fmt.Sprintf(`SELECT COALESCE(paused_tables,'') FROM %s WHERE job_key=$1`, s.stateTable("_jl_meta"))
	err := s.conn.QueryRow(ctx, q, s.opt.JobKey).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return map[string]PausedTable{}, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("读暂停表失败: %w", err)
	}
	return parsePaused(raw)
}

// readPausedTxLocked 在事务里读（FOR UPDATE）：先读后写，防两个人同时登记把对方覆盖掉。
func (s *Sink) readPausedTxLocked(ctx context.Context, tx pgx.Tx) (map[string]PausedTable, bool, error) {
	var raw string
	q := fmt.Sprintf(`SELECT COALESCE(paused_tables,'') FROM %s WHERE job_key=$1 FOR UPDATE`, s.stateTable("_jl_meta"))
	err := tx.QueryRow(ctx, q, s.opt.JobKey).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, fmt.Errorf("目标端还没有这个任务的状态行（%s），无法登记暂停", s.opt.JobKey)
	}
	if err != nil {
		return nil, false, fmt.Errorf("读暂停表失败: %w", err)
	}
	return parsePaused(raw)
}

// writePausedTxLocked 写回整列（值是**刚刚读出来的那份**，不是内存表集合重建的）。
// 影响行数必须是 1：0 行说明状态行不在了——绝不能当成「写成功了」。
func (s *Sink) writePausedTxLocked(ctx context.Context, tx pgx.Tx, cur map[string]PausedTable) error {
	blob, err := json.Marshal(cur)
	if err != nil {
		return fmt.Errorf("序列化暂停表失败: %w", err)
	}
	// 唯一一条 UPDATE：只碰 paused_tables 与 updated_at，**绝不带 position**。
	q := fmt.Sprintf(`UPDATE %s SET paused_tables=$1, updated_at=now() WHERE job_key=$2`,
		s.stateTable("_jl_meta"))
	tag, err := tx.Exec(ctx, q, string(blob), s.opt.JobKey)
	if err != nil {
		return fmt.Errorf("写暂停表失败: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("写暂停表失败：目标任务状态行不在了（job_key=%s）", s.opt.JobKey)
	}
	return nil
}

// parsePaused 解析那一列。空列 = 没暂停；脏 JSON = 当作没暂停（dirty），不阻断同步。
func parsePaused(raw string) (map[string]PausedTable, bool, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" || trimmed == "null" {
		return map[string]PausedTable{}, false, nil
	}
	var m map[string]PausedTable
	if err := json.Unmarshal([]byte(trimmed), &m); err != nil {
		return map[string]PausedTable{}, true, nil
	}
	if m == nil {
		m = map[string]PausedTable{}
	}
	return m, false, nil
}

// PausedKeys 是给日志/状态用的稳定顺序（map 的迭代顺序不能进文案与用例）。
func PausedKeys(m map[string]PausedTable) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// EnsureStateTables 只做「状态表建表 + 迁移」，不建 Sink、不抢租约、不读业务表。
//
// 为什么单独开这个口子：状态表迁移**不该依赖「这一轮有没有走到 prepare」**。
// 真实场景是——目标库还是旧 schema，用户第一次跑因为租约/预检/表不存在在 prepare 之前
// 就失败了，他看到的却是 `column "paused_tables" does not exist`：一条与真实原因无关的
// 报错，还会让他以为要手工改表。所以引擎在**任何读 `_jl_meta` 之前**先调一次本函数
// （幂等，代价是一条 ADD COLUMN IF NOT EXISTS）。
func EnsureStateTables(ctx context.Context, ep dsn.Endpoint, schema string) error {
	if strings.TrimSpace(schema) == "" {
		schema = "public"
	}
	conn, err := pgwire.Connect(ctx, ep)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	s := &Sink{conn: conn, opt: Options{Target: ep, Schema: schema}}
	return s.ensureStateTables(ctx)
}
