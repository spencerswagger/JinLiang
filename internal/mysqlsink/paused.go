package mysqlsink

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/spencerswagger/jinliang/internal/pgsink"
	"github.com/spencerswagger/jinliang/internal/source"
)

// 按表暂停的存储（MySQL 目标端，与 PG 侧**同构**：一列 JSON 文本，不是新表）。
//
// 契约：docs/CONTRACT-PAUSE-AND-POSITION.md §1.4（记录形状）与 §4.2（六个方法）。
// 形状：
//
//	{"orders":{"reason":"type_changed","at":"…","detail":"…","resume":"recopy","skipped_rows":1284}}
//
// 字段的**时态**（与 PG 侧一致，写进这里免得两边再漂）：
//
//	at          第一次登记的时刻，永不覆盖
//	reason      最新一次的原因码（稳定码：type_changed / schema_drift）
//	detail      最新一次的差异描述（人话）
//	skipped_*   累计值（跳过窗口的行数与首末位点，重登记不清零）
//
// 五条硬约束（改这段代码前先读）：
//  1. **key = 目标端裸表名**，与 `_jl_chunk.table_name` 逐字节同源（不带 schema）。
//  2. **只碰这一列**：登记是独立一条 UPDATE，绝不与 `position` 同一条——位点只能由数据事务推进。
//  3. **at / reason 不覆盖**：第一次的时刻与原因最有价值（重搬失败重登记也不改）。
//  4. **单 key 读改写**：先 `SELECT … FOR UPDATE` 再写回，绝不拿内存里的整表集合重建整列
//     （那会把别的表、以及不归本轮管的记录一起抹掉）。
//  5. **失败必须响**：状态行不存在、列不存在、UPDATE 没落地 —— 一律报错。
//     「UPDATE 影响 0 行当成功」在这里是严禁的（契约 §1.7 最后一条）。
//
// 为什么复用 `pgsink.PausedTable` 而不是在 MySQL 侧另定义一个：两边必须是**同一份 JSON 形状**，
// 一个结构体两个包各写一份，迟早会在字段名/omitempty 上分叉，而消费者的判等是按字段的。

// pausedTableColumn 是 `_jl_meta` 上那一列的名字（与 PG 侧逐字相同）。
const pausedTableColumn = "paused_tables"

// StoredJobHash 读回状态行里记的 job_hash（`EnsureMeta` 写的那一个）。
//
// 谁在用：引擎的 `reconcilePaused` 拿它做**跨轮存活判据**——任务语义变了（`job_hash` 变了），
// 旧的「这张表停着」就不再成立，要连记录一起清掉。它**不在** PauseRegistry 的六个方法里
// （契约 §4.2 只列了六个），所以 MySQL 目标端要单独实现这一个，否则片 C 拼不齐。
//
// 与 `paused_tables` 的关系（写清楚免得被误解成「同事务」）：
//   - `job_hash` 由 `EnsureMeta` 在 prepare 写（`ON DUPLICATE KEY UPDATE` 会刷新它）；
//   - `paused_tables` 由 `PauseTable` 单独一个事务写；
//   - 两者**不在同一个事务**，也不需要：判据是「读回来的 hash 是否等于本轮 hash」，
//     读晚一点只会更准（本轮 hash 在 prepare 就定了）。真正要保证的是「读得到」——
//     没有这个方法，reconcile 在 MySQL 目标端就会因为拿不到 hash 而把记录全清（或干脆不判）。
//
// 没有状态行（首次运行前）返回空串：引擎把「空」当成「没有历史」处理（`sameJob` 为真）。
func (s *Sink) StoredJobHash(ctx context.Context) (string, error) {
	var h sql.NullString
	err := s.db.QueryRowContext(ctx,
		"SELECT job_hash FROM `_jl_meta` WHERE job_key = ?", s.opt.JobKey).Scan(&h)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("读任务指纹（job_hash）失败: %w", err)
	}
	return h.String, nil
}

// PauseEnforcement 报告这个目标端的暂停**执行面**做到哪一步。
//
// MySQL 目标端今天是 `storage-only`：记录能写、能读、能清（六个方法都实现了），
// 但**引擎侧的暂停判定还没绑到这条路径上**（`pauseReg` 故意不绑，Core #99 的理由：
// 恢复路径 reconcile/强制重搬/搬成解除仍在 PG 侧，绑上去就是「流式期暂停永不解除」）。
//
// 这句话必须能被 `status` / `doctor` 读出来并转述给用户，否则最危险的形态会静默发生：
// **存储层有记录，但写路径根本不理它** —— 用户看到「已登记暂停」以为表停了，
// 实际还在写，目标端与源端不一致。
//
// 片 C 落地（pauseReg 绑定 + 恢复路径搬层就位）之后把这里改成 `engine-bound`，
// `TestIntegrationPauseViewWhenEngineNotBound` 会逼着改（它断言当前值）。
const (
	PauseEnforcementStorageOnly = "storage-only"
	PauseEnforcementEngineBound = "engine-bound"
)

// PauseEnforcement 见常量注释。
func (s *Sink) PauseEnforcement() string { return PauseEnforcementStorageOnly }

// PauseView 是「暂停」这件事**面向展示**的完整状态（status / doctor / UI 都读它）。
//
// 为什么不是直接返回 `map[string]PausedTable`：那样调用方只能看到记录，看不到
// 「这些记录到底管不管用」。三个附加事实缺一不可——
//   - Dirty：库里的 JSON 坏了（已按「无暂停」继续，但用户必须知道）；
//   - Enforcement：写路径认不认这些记录（storage-only 时**不认**）；
//   - ReadErr：读失败时把错误原样带出来，绝不返回空集合假装「没有暂停」。
type PauseView struct {
	Records     map[string]pgsink.PausedTable
	Tables      []string
	Dirty       bool
	Enforcement string
	ReadErr     error
}

// PauseSummary 汇总本任务的暂停状态（读失败也返回一个 PauseView，错误放在 ReadErr 里）。
func (s *Sink) PauseSummary(ctx context.Context) PauseView {
	v := PauseView{Enforcement: s.PauseEnforcement()}
	rec, dirty, err := s.readPaused(ctx)
	if err != nil {
		v.ReadErr = err
		return v
	}
	v.Records, v.Dirty = rec, dirty
	v.Tables = pgsink.PausedKeys(rec)
	return v
}

// Lines 把 PauseView 渲染成给人看的几行（status / doctor 直接拼进输出）。
//
// 文案原则：**先说记录，再说它管不管用**。storage-only 时必须有一句
// 「仍在被写入」，否则「已暂停」三个字本身就是假话。
func (v PauseView) Lines() []string {
	if v.ReadErr != nil {
		return []string{"暂停记录     读不到：" + v.ReadErr.Error() +
			"（不要当成「没有暂停」——先修权限/连接再判断）"}
	}
	if len(v.Tables) == 0 {
		if v.Dirty {
			return []string{"暂停记录     那一列不是合法 JSON，已按「没有暂停」继续；请把这行发给我们"}
		}
		return nil
	}
	out := []string{fmt.Sprintf("暂停记录     %d 张表：%s", len(v.Tables), strings.Join(v.Tables, "、"))}
	if v.Enforcement == PauseEnforcementStorageOnly {
		out = append(out,
			"              ⚠ 这些记录**只存在存储层**（pause enforcement="+PauseEnforcementStorageOnly+"）："+
				"当前目标端的引擎侧尚未接管暂停，被记录的表**仍在被写入**，目标端数据可能与源端不一致。",
			"              结构对齐后请重跑：本轮会整表重搬补齐，搬成之后记录才会解除。")
	}
	return out
}

// ErrPauseUnsupported 表示 MySQL 目标端这条连接**没法**登记暂停（列补不上：缺权限/只读/老库）。
//
// 它是「明确失败」的载体：引擎看到它就把这张表按「整 run 失败」处理，
// 并在 plan/preflight 里事先说明——绝不静默降级成「跳过但没记下」。
var ErrPauseUnsupported = fmt.Errorf("MySQL 目标端不支持按表暂停")

// LoadPausedTables 把库里的暂停记录读进内存缓存（引擎 prepare 时读一次）。
//
// 脏 JSON 按「没有暂停」处理并发一条 note（`PausedDirty`），与 pgsink 同口径：
// 少写比多写安全——绝不能因为一列坏了就把整条同步停死。
func (s *Sink) LoadPausedTables(ctx context.Context) error {
	cur, dirty, err := s.readPaused(ctx)
	if err != nil {
		return err
	}
	s.pausedCache, s.pausedDirty = cur, dirty
	return nil
}

// PausedTables 读回当前记录的暂停表（status / doctor 用；不看内存缓存）。
func (s *Sink) PausedTables(ctx context.Context) (map[string]pgsink.PausedTable, error) {
	cur, _, err := s.readPaused(ctx)
	return cur, err
}

// PausedDirty 报告库里那一列不是合法 JSON（已按「无暂停」处理，引擎据此发 note）。
func (s *Sink) PausedDirty() bool { return s.pausedDirty }

// PauseTable 登记一张表的暂停——**本次运行唯一的写点**（幂等 upsert 单 key）。
//
// 顺序契约（§1.5 第 2 条）：登记必须先于跳过落库。这里返回错误时，调用方必须整批失败。
func (s *Sink) PauseTable(ctx context.Context, ref source.TableRef, rec pgsink.PausedTable) error {
	if strings.TrimSpace(ref.Table) == "" {
		return fmt.Errorf("内部错误：登记暂停时表名为空（key 必须是目标端裸表名）")
	}
	if strings.TrimSpace(rec.Reason) == "" {
		// 原因码是机器判等的凭据，空值等于没登记。
		return fmt.Errorf("内部错误：登记暂停表 %s 时没有原因码", ref.Table)
	}
	if rec.At == "" {
		rec.At = time.Now().UTC().Format(time.RFC3339)
	}
	cur, err := s.mutatePaused(ctx, ref.Table, func(m map[string]pgsink.PausedTable) error {
		if old, ok := m[ref.Table]; ok {
			// **三个字段三种时态**（与 PG 侧逐字对齐，见文件头）：
			//   - at：**第一次**登记的时刻，永不覆盖（用户要能看出「停了多久」）；
			//   - reason / detail：**最新**这一次的（「这张表现在为什么停着」）；
			//   - skipped_*：累计值，重登记不清零（同一段欠账）。
			// 早先这里保留过「第一次的 reason」，但那会让 reason 与 detail 互相矛盾
			// （reason=type_changed，detail 却写着「列被改名」），诊断时反而要二次解释。
			rec.At = old.At
			if rec.Resume == "" {
				rec.Resume = old.Resume
			}
			// 跳过窗口的记账也是累计值，重登记不清零。
			if rec.SkippedRows == 0 {
				rec.SkippedRows, rec.SkippedFrom = old.SkippedRows, old.SkippedFrom
			}
			if rec.LastSkippedPos == "" {
				rec.LastSkippedPos = old.LastSkippedPos
			}
		}
		// 未知值一律按 recopy（宁可多搬一次，不可留下一张永远不一致的表）——与 pgsink 同一判据。
		rec.Resume = pgsink.NormalizedResume(rec.Resume)
		m[ref.Table] = rec
		return nil
	})
	if err != nil {
		return err
	}
	s.pausedCache = cur
	return nil
}

// NoteSkipped 记一笔「跳过窗口」的账：行数累加、`skipped_from` 首次不覆盖、`last_skipped_pos` 跟本批。
//
// 不登记的表不写（宁可不记，也不凭空造一条暂停）。
// 记账失败不该让整批失败（数据已落库、位点已前进，判失败反而制造「写成功却报错」），
// 由调用方决定只发 note——所以这里把错误如实返回，不吞。
func (s *Sink) NoteSkipped(ctx context.Context, ref source.TableRef, rows int64, pos string) error {
	if rows <= 0 && pos == "" {
		return nil
	}
	cur, err := s.mutatePaused(ctx, ref.Table, func(m map[string]pgsink.PausedTable) error {
		rec, ok := m[ref.Table]
		if !ok {
			return nil // 没登记过：不写，也不报错（调用方按 note 处理）
		}
		rec.SkippedRows += rows
		if rec.SkippedFrom == "" {
			rec.SkippedFrom = pos // 第一次的缺口起点最有价值
		}
		if pos != "" {
			rec.LastSkippedPos = pos
		}
		m[ref.Table] = rec
		return nil
	})
	if err != nil {
		return err
	}
	s.pausedCache = cur
	return nil
}

// ClearPausedTable 只删这一个 key（别的表记录留着）。
//
// 幂等：key 本来就不在 → 成功返回（「搬成清该表，搬败留记录」，重复清不算错）。
// 但**状态行不存在**是另一回事：那说明目标端的任务状态没了，必须报错而不是假装清过。
func (s *Sink) ClearPausedTable(ctx context.Context, ref source.TableRef) error {
	cur, err := s.mutatePaused(ctx, ref.Table, func(m map[string]pgsink.PausedTable) error {
		delete(m, ref.Table)
		return nil
	})
	if err != nil {
		return err
	}
	s.pausedCache = cur
	return nil
}

// MarkTableRecopyPending 把一张表标成「欠一次整表重搬」：**只清块标记，不动数据**（契约 §1.6 步 2）。
//
// 含 `chunk_no=0`（完成标记）：不清它的话，`planCopy` 会认为这张表的存量搬完了，
// 重搬永远排不进去——「暂停解除了、数据其实还缺着」。只清这一张表，别的表照旧续跑。
func (s *Sink) MarkTableRecopyPending(ctx context.Context, ref source.TableRef) error {
	// 这里**不碰** `paused_tables`（只删块标记）：补列失败是另一件事，
	// 让它把「重搬」卡在一个跟它无关的原因上，用户会照着错的提示去修。
	res, err := s.db.ExecContext(ctx,
		"DELETE FROM `_jl_chunk` WHERE job_key = ? AND table_name = ?", s.opt.JobKey, ref.Table)
	if err != nil {
		return fmt.Errorf("把 %s 标成待重搬失败（清块标记）: %w", ref.Table, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("把 %s 标成待重搬失败（读影响行数）: %w", ref.Table, err)
	}
	// 0 行是**合法**的：这张表本来就没块标记（比如暂停发生在首搬之前）。
	// 但块表本身不可读要报错——那是「以为清了、其实没清」。
	_ = n
	return nil
}

// ── 内部：读改写 ─────────────────────────────────────────────

// mutatePaused 在**一个事务**里做「读当前 JSON → 改这一个 key → 写回」。
//
// 三条不许绕过的点：
//   - `SELECT … FOR UPDATE` 锁住状态行：两个进程同时登记不会互相覆盖；
//   - 写回时 `updated_at=NOW(6)`：MySQL 在「所有列都没变」时返回 0 影响行，
//     带上时间戳才能把「真的没写进去」与「写了但值一样」分开；
//   - 影响 0 行 → 回读核对：值与我们写的一致才算幂等成功，否则报错（绝不静默）。
func (s *Sink) mutatePaused(ctx context.Context, table string,
	mut func(map[string]pgsink.PausedTable) error) (map[string]pgsink.PausedTable, error) {

	if err := s.ensurePausedColumn(ctx); err != nil {
		return nil, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("开暂停登记事务失败: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck

	var raw sql.NullString
	err = tx.QueryRowContext(ctx,
		"SELECT `"+pausedTableColumn+"` FROM `_jl_meta` WHERE job_key = ? FOR UPDATE", s.opt.JobKey).
		Scan(&raw)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("登记暂停失败：目标端的任务状态行不存在（job_key=%s）。"+
			"你能做什么：先跑一次 `jinliang run` 让它把状态行建出来（或检查目标库是不是被清过）；"+
			"风险：没有（本次没有写入任何数据）", s.opt.JobKey)
	}
	if err != nil {
		return nil, fmt.Errorf("读暂停记录失败: %w", err)
	}

	cur := map[string]pgsink.PausedTable{}
	if raw.Valid && strings.TrimSpace(raw.String) != "" {
		if jerr := json.Unmarshal([]byte(raw.String), &cur); jerr != nil {
			// 脏 JSON：按「没有暂停」继续（少写而不是多写），但要留痕。
			cur = map[string]pgsink.PausedTable{}
			s.pausedDirty = true
		}
	}
	if err := mut(cur); err != nil {
		return nil, err
	}
	body, err := json.Marshal(cur)
	if err != nil {
		return nil, fmt.Errorf("暂停记录序列化失败: %w", err)
	}

	res, err := tx.ExecContext(ctx,
		"UPDATE `_jl_meta` SET `"+pausedTableColumn+"` = ?, updated_at = NOW(6) WHERE job_key = ?",
		string(body), s.opt.JobKey)
	if err != nil {
		return nil, fmt.Errorf("写暂停记录失败: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("写暂停记录失败（读影响行数）: %w", err)
	}
	if affected == 0 {
		// 两种可能：① 值与我们写的一模一样（幂等空写，合法）；② 行在锁内被别人删了。
		// 回读核对，不靠猜。
		var got sql.NullString
		if qerr := tx.QueryRowContext(ctx,
			"SELECT `"+pausedTableColumn+"` FROM `_jl_meta` WHERE job_key = ?", s.opt.JobKey).
			Scan(&got); qerr != nil || !got.Valid || got.String != string(body) {
			return nil, fmt.Errorf("写暂停记录没生效（UPDATE 影响 0 行，且回读的内容对不上）。" +
				"常见原因：目标库是只读/只读副本、或另一个进程刚把状态行改掉。" +
				"你能做什么：确认目标库可写（`SELECT @@read_only`），或等另一个进程结束后重跑；" +
				"风险：没有（本次没有跳过任何表，也没有推进位点）")
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("提交暂停记录失败: %w", err)
	}
	_ = table
	return cur, nil
}

// readPaused 读回暂停记录（不锁）。
func (s *Sink) readPaused(ctx context.Context) (map[string]pgsink.PausedTable, bool, error) {
	if err := s.ensurePausedColumn(ctx); err != nil {
		return nil, false, err
	}
	var raw sql.NullString
	err := s.db.QueryRowContext(ctx,
		"SELECT `"+pausedTableColumn+"` FROM `_jl_meta` WHERE job_key = ?", s.opt.JobKey).Scan(&raw)
	if err == sql.ErrNoRows {
		return map[string]pgsink.PausedTable{}, false, nil // 还没有任务状态行：等于没有暂停
	}
	if err != nil {
		return nil, false, fmt.Errorf("读暂停记录失败: %w", err)
	}
	out := map[string]pgsink.PausedTable{}
	if !raw.Valid || strings.TrimSpace(raw.String) == "" {
		return out, false, nil
	}
	if jerr := json.Unmarshal([]byte(raw.String), &out); jerr != nil {
		return map[string]pgsink.PausedTable{}, true, nil // 脏 JSON → 按无暂停
	}
	return out, false, nil
}

// ensurePausedColumn 保证 `_jl_meta.paused_tables` 这一列存在（老库补列）。
//
// **MySQL 没有 `ADD COLUMN IF NOT EXISTS`**（那是 MariaDB 的方言）：8.0/8.4 执行会直接语法错。
// 所以先查 information_schema 再决定要不要 ALTER。
// 补不上（缺 ALTER 权限、只读副本、库被锁）→ `ErrPauseUnsupported` 包装的明确失败：
// 契约 §1.7 最后一条要求「`PauseTable` 明确失败 + plan 标注」，严禁静默。
func (s *Sink) ensurePausedColumn(ctx context.Context) error {
	s.pausedColMu.Lock()
	defer s.pausedColMu.Unlock()
	if s.pausedColOK {
		return nil
	}
	var n int
	err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.columns "+
			"WHERE table_schema = DATABASE() AND table_name = '_jl_meta' AND column_name = ?",
		pausedTableColumn).Scan(&n)
	if err != nil {
		return fmt.Errorf("%w：查 `_jl_meta` 有没有 `%s` 列失败: %v。"+
			"你能做什么：确认目标端账号能读 information_schema（一般都有）；"+
			"风险：没有（本次没有写入任何数据）", ErrPauseUnsupported, pausedTableColumn, err)
	}
	if n == 0 {
		if _, err := s.db.ExecContext(ctx,
			"ALTER TABLE `_jl_meta` ADD COLUMN `"+pausedTableColumn+"` text NULL"); err != nil {
			return fmt.Errorf("%w：给 `_jl_meta` 补 `%s` 列失败（目标端是 MySQL %s）。"+
				"你能做什么：① 用有 ALTER 权限的账号手工执行 "+
				"`ALTER TABLE _jl_meta ADD COLUMN paused_tables text NULL`；"+
				"② 或者换 PostgreSQL 目标端。风险：没有（本次没有跳过任何表，也没有推进位点）",
				ErrPauseUnsupported, pausedTableColumn, s.serverVersion)
		}
	}
	s.pausedColOK = true
	return nil
}

// PauseRegistryReady 供 preflight/plan 用：目标端能不能登记暂停（列在、或能补上）。
// 返回 nil = 支持；返回错误 = 那条错误本身就是四段式说明，直接展示给用户。
func (s *Sink) PauseRegistryReady(ctx context.Context) error { return s.ensurePausedColumn(ctx) }

// PauseRegistry 是契约 §4.2 的六个方法（在**本包**里再声明一遍是为了拿到编译期保证：
// 签名漂了就直接编译失败，而不是等引擎运行时 `ok == false` 静默退回「不支持」）。
//
// 为什么没放进 internal/sink：那需要把 `PausedTable` 也搬过去（pgsink 已经在用自己那份，
// 搬它等于同时改 PG 侧写路径与所有调用方）。归属留给 Ark/Core 收口，本包先钉住签名。
type PauseRegistry interface {
	LoadPausedTables(ctx context.Context) error
	PausedTables(ctx context.Context) (map[string]pgsink.PausedTable, error)
	PauseTable(ctx context.Context, ref source.TableRef, rec pgsink.PausedTable) error
	NoteSkipped(ctx context.Context, ref source.TableRef, rows int64, pos string) error
	ClearPausedTable(ctx context.Context, ref source.TableRef) error
	MarkTableRecopyPending(ctx context.Context, ref source.TableRef) error
}

var _ PauseRegistry = (*Sink)(nil)

// PausedKeys 返回记录里的表名（排序后；展示与对账用）。
func PausedKeys(m map[string]pgsink.PausedTable) []string { return pgsink.PausedKeys(m) }
