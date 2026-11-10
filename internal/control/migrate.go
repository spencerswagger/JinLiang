package control

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spencerswagger/jinliang/internal/core"
)

// 控制库迁移：手写、带版本号、幂等（docs/CONTROL-PLANE.md §8）。
//
// 规则：
//   - 只向前走。降级二进制不能读新库，只能提示用户换回新版（E3002 的四段式）。
//   - 每条迁移在自己的事务里执行，成功后写 settings.schema_version；中途失败不留半成品。
//   - 迁移只加表/加列/加索引，不删不改——老版本控制库永远能被打开。

type migration struct {
	Version int
	Name    string
	Stmts   []string
	// After 在同一事务里做「需要 Go 逻辑」的收尾（时间窗口比较、审计、索引兜底）。
	// 放在事务里是故意的：迁移版本号与它带来的数据形态必须一起提交，
	// 否则版本号已经前进、索引却没建上，下次启动再也不会补。
	After func(ctx context.Context, tx *sql.Tx, s *Store) error
	// Fn 用于需要先看当前结构再决定的迁移（例如「列不存在才加」）。
	// 与 Stmts 二选一；一起给时先跑 Stmts 再跑 Fn。
	Fn func(ctx context.Context, tx *sql.Tx) error
}

// migrations 是控制库的全部结构历史。字段名严格对齐 CONTROL-PLANE §3。
var migrations = []migration{
	{
		Version: 1,
		Name:    "b0-control-plane",
		Stmts: []string{
			// 连接：密码只存密文，UI/API 永远拿不到明文。
			`CREATE TABLE IF NOT EXISTS connections (
				id           TEXT PRIMARY KEY,
				name         TEXT NOT NULL,
				kind         TEXT NOT NULL,
				host         TEXT NOT NULL,
				port         INTEGER NOT NULL,
				database     TEXT NOT NULL,
				username     TEXT NOT NULL,
				secret_enc   BLOB NOT NULL,
				params_json  TEXT NOT NULL DEFAULT '{}',
				last_test_at TEXT, last_test_ok INTEGER, last_test_msg TEXT,
				created_at   TEXT NOT NULL, updated_at TEXT NOT NULL
			)`,
			// 任务：语义字段整体存 JSON，与 job.yaml schema 同构，便于导入导出。
			`CREATE TABLE IF NOT EXISTS jobs (
				id         TEXT PRIMARY KEY,
				job_key    TEXT NOT NULL UNIQUE,
				name       TEXT NOT NULL,
				source_id  TEXT NOT NULL REFERENCES connections(id),
				target_id  TEXT NOT NULL REFERENCES connections(id),
				spec_json  TEXT NOT NULL,
				enabled    INTEGER NOT NULL DEFAULT 0,
				created_at TEXT NOT NULL, updated_at TEXT NOT NULL
			)`,
			`CREATE TABLE IF NOT EXISTS runs (
				id         TEXT PRIMARY KEY,
				job_id     TEXT NOT NULL REFERENCES jobs(id),
				phase      TEXT NOT NULL,
				started_at TEXT NOT NULL, ended_at TEXT,
				rows_done  INTEGER DEFAULT 0,
				lag_bytes  INTEGER, lag_ms INTEGER,
				last_error TEXT,
				exit_code  INTEGER,
				-- 文档缺口（已报 hugo）：CONTROL-PLANE §3 没有字段能记录「这次是桩运行」，
				-- 而 B0 的 run 是桩。为了让历史页不假装真跑过，这里加一列；B1 起保持 0。
				stub       INTEGER NOT NULL DEFAULT 0
			)`,
			`CREATE INDEX IF NOT EXISTS idx_runs_job_started ON runs(job_id, started_at DESC)`,
			`CREATE TABLE IF NOT EXISTS settings (k TEXT PRIMARY KEY, v TEXT NOT NULL)`,
			`CREATE TABLE IF NOT EXISTS audit (
				id TEXT PRIMARY KEY, at TEXT NOT NULL, actor TEXT NOT NULL,
				action TEXT NOT NULL, detail_json TEXT
			)`,
			`CREATE INDEX IF NOT EXISTS idx_audit_at ON audit(at DESC)`,
		},
	},
	{
		Version: 2,
		Name:    "runs-liveness",
		Stmts: []string{
			// 进程被杀后，原来那行会永远 ended_at IS NULL：历史页永久显示「在跑」，
			// 而且还能再开第二个 run。三列 + 一条 partial unique index 把这件事关死。
			`ALTER TABLE runs ADD COLUMN heartbeat_at TEXT`,
			`ALTER TABLE runs ADD COLUMN owner_instance TEXT`,
			// completed | failed | stopped | interrupted —— 「手动停止」不再伪装成 exit_code=0 的正常结束。
			`ALTER TABLE runs ADD COLUMN finish_reason TEXT`,
		},
		After: closeOnlyStaleRunsThenIndex,
	},
	{
		// 版本号说明：PR #5（runs-liveness）占用了 2，本分支顺延一版。
		// 撞版本号的后果不是「报错」而是「静默跳过」——库已经是 2 时两条 v2 都会被
		// 跳过，kind 列永远不存在，runs 查询全线炸。见 migrate_versions_test.go。
		Version: 3,
		Name:    "runs-kind",
		// hugo 裁决：runs 用 kind 表达「这一轮是什么运行」（sync | verify | stub），
		// 布尔 stub 的反面是「默认真跑」——任何漏标记的新写入方都会被历史页当成真同步。
		// 兼容：stub 列保留一版（B1 收口后再删），历史行照旧可读。
		Fn: migrateRunsKind,
	},
	{
		Version: 4,
		Name:    "audit-action-index",
		Stmts: []string{
			// 诊断页按 action 取最近 N 条（ListAuditByAction）。没有这条索引时
			// EXPLAIN 是 `SCAN audit` + `USE TEMP B-TREE FOR ORDER BY`（全表扫 + 全排序），
			// 而 audit 没有保留策略、无界增长——「出问题时才打开」的页面会随时间线性变慢。
			//
			// **两个键都要**：只建 (action, at DESC) 时仍要为同秒内的 id 建临时树。
			`CREATE INDEX IF NOT EXISTS idx_audit_action_at_id ON audit(action, at DESC, id DESC)`,
		},
	},
	{
		Version: 5,
		Name:    "runs-code",
		Stmts: []string{
			// 错误码单独一列：列表页只显示「徽标 + 一句摘要」，全文留给详情页。
			// 在此之前只能把 250–300 字的 last_error 截断（#41），那是止血不是根治。
			`ALTER TABLE runs ADD COLUMN code TEXT`,
		},
	},

	// v6：`runs.paused_tables` —— 「这一轮结束时还停着哪几张表」（JSON 数组）。
	// 退出码 3 只是粗信号（整数装不下表名/欠行数/起始位点），这一列才是权威。
	// 与 `runs.code` 同一路数：先正常收尾，再补一列；补列失败不该让收尾失败。
	{
		Version: 6,
		Name:    "runs-paused-tables",
		Stmts: []string{
			`ALTER TABLE runs ADD COLUMN paused_tables TEXT`,
		},
	}}

// closeOnlyStaleRunsThenIndex 是 v2 的收尾。
//
// 关键区别（Ark C-2）：**只收尾心跳过期的**，不无条件收尾所有 open run。
// 一个只跑 200ms 的 `jinliang export` 也会打开同一个控制库；如果它把正在服务的
// serve 手里那条活 run 标成 interrupted，活跃槽就白放开了（还能再起第二个 run）。
// 心跳新鲜的留给人家的 RecoverStaleRuns（serve 启动时按同一阈值判断）。
func closeOnlyStaleRunsThenIndex(ctx context.Context, tx *sql.Tx, s *Store) error {
	cutoff := s.now().Add(-StaleAfter(s.heartbeat)).UTC().Format(time.RFC3339)
	res, err := tx.ExecContext(ctx,
		`UPDATE runs
		    SET ended_at = ?, finish_reason = 'interrupted',
		        phase = CASE WHEN phase IN ('done','failed') THEN phase ELSE 'failed' END,
		        last_error = COALESCE(last_error, '升级时发现这是一条没有心跳的遗留运行；这一轮没有正常收尾')
		  WHERE ended_at IS NULL
		    AND COALESCE(heartbeat_at, started_at) < ?`, s.nowText(), cutoff)
	if err != nil {
		return fmt.Errorf("收尾遗留运行失败: %w", err)
	}
	closed, _ := res.RowsAffected()

	// 兜底：同一任务若还残留多行「未结束」（v1 时代没有约束，理论上可能），
	// 只保留最新一条，其余结掉——否则唯一索引建不起来，整个升级会卡死。
	if _, err := tx.ExecContext(ctx,
		`UPDATE runs SET ended_at = started_at, finish_reason = COALESCE(finish_reason,'interrupted'),
		        phase = CASE WHEN phase IN ('done','failed') THEN phase ELSE 'failed' END
		  WHERE ended_at IS NULL
		    AND id NOT IN (SELECT id FROM runs r2 WHERE r2.ended_at IS NULL
		                   GROUP BY r2.job_id HAVING r2.id = MAX(r2.id))`); err != nil {
		return fmt.Errorf("清理重复活跃运行失败: %w", err)
	}

	if _, err := tx.ExecContext(ctx,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_one_active
		    ON runs(job_id) WHERE ended_at IS NULL`); err != nil {
		return fmt.Errorf("创建「一任务一活跃运行」唯一索引失败: %w", err)
	}

	if closed > 0 {
		id, _ := NewID()
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO audit (id, at, actor, action, detail_json) VALUES (?, ?, ?, ?, ?)`,
			id, s.nowText(), ActorSystem, "migrate.recover_runs",
			fmt.Sprintf(`{"closed":%d,"schema_version":2,"reason":"heartbeat stale during upgrade"}`, closed)); err != nil {
			return fmt.Errorf("写迁移审计失败: %w", err)
		}
		fmt.Fprintf(os.Stderr, "迁移 2(runs-liveness)：%d 条心跳过期的运行已标记 interrupted\n", closed)
	}
	return nil
}

// migrateRunsKind 给 runs 加 kind 并回填旧行的 stub=1 → 'stub'。
//
// 幂等：先看 sqlite_master/pragma 里有没有这一列，有就跳过 ALTER；
// 回填也只在加列成功后做一次，所以重跑不会重复写、不会报错。
func migrateRunsKind(ctx context.Context, tx *sql.Tx) error {
	has, err := columnExists(ctx, tx, "runs", "kind")
	if err != nil {
		return err
	}
	if !has {
		if _, err := tx.ExecContext(ctx,
			`ALTER TABLE runs ADD COLUMN kind TEXT NOT NULL DEFAULT 'sync'`); err != nil {
			return fmt.Errorf("给 runs 加 kind 列失败: %w", err)
		}
	}
	// 旧库里的桩运行：stub=1 的行必须是 'stub'，否则历史页会把它们当真同步。
	if _, err := tx.ExecContext(ctx,
		`UPDATE runs SET kind = 'stub' WHERE stub = 1 AND kind <> 'stub'`); err != nil {
		return fmt.Errorf("回填 runs.kind 失败: %w", err)
	}
	return nil
}

// columnExists 查 sqlite 的列清单（PRAGMA 不能用参数绑定，所以只接受我们自己的表名）。
func columnExists(ctx context.Context, tx *sql.Tx, table, column string) (bool, error) {
	switch table {
	case "runs", "jobs", "connections", "settings", "audit":
	default:
		return false, fmt.Errorf("columnExists: 不接受表名 %q", table)
	}
	rows, err := tx.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var (
			cid     int
			name    string
			ctype   string
			notnull int
			dflt    sql.NullString
			pk      int
		)
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return false, err
		}
		if strings.EqualFold(name, column) {
			return true, nil
		}
	}
	return false, rows.Err()
}

// migrate applies every migration newer than the recorded schema_version.
func (s *Store) migrate(ctx context.Context) error {
	// 先自检迁移表本身：版本号必须严格递增且唯一。
	// 两条同版本号时，已经升到该版本的库会把两条都跳过（不是报错），后来那条带来的
	// 列永远不存在——沉默的坏，直到某条查询炸掉。宁可启动就拒绝。
	for i := 1; i < len(migrations); i++ {
		if migrations[i].Version <= migrations[i-1].Version {
			return fmt.Errorf("控制库迁移表损坏：版本号 %d（%s）没有大于 %d（%s）——这是程序错误，不是你的配置问题，请把这条报给我们",
				migrations[i].Version, migrations[i].Name, migrations[i-1].Version, migrations[i-1].Name)
		}
	}
	// settings 是版本号的落脚点，先无条件建好（幂等），否则没法读版本。
	if _, err := s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS settings (k TEXT PRIMARY KEY, v TEXT NOT NULL)`); err != nil {
		return fmt.Errorf("初始化 settings 失败: %w", err)
	}
	current, err := s.SchemaVersion(ctx)
	if err != nil {
		return err
	}
	latest := migrations[len(migrations)-1].Version
	if current > latest {
		spec, _ := core.Lookup(core.CodeSchemaTooNew)
		return fmt.Errorf("%s：控制库 schema_version=%d，本二进制只认到 %d。%s",
			string(core.CodeSchemaTooNew), current, latest, strings.Join(spec.Next, "；"))
	}
	for _, m := range migrations {
		if m.Version <= current {
			continue
		}
		if err := s.applyMigration(ctx, m); err != nil {
			return fmt.Errorf("迁移 %d(%s) 失败: %w", m.Version, m.Name, err)
		}
	}
	if _, ok, err := s.Setting(ctx, SettingCreatedAt); err != nil {
		return err
	} else if !ok {
		if err := s.SetSetting(ctx, SettingCreatedAt, s.nowText()); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) applyMigration(ctx context.Context, m migration) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // 成功路径已提交，这里只是兜底
	for _, stmt := range m.Stmts {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("执行 %q 失败: %w", firstLine(stmt), err)
		}
	}
	if m.After != nil {
		if err := m.After(ctx, tx, s); err != nil {
			return err
		}
	}
	if m.Fn != nil {
		if err := m.Fn(ctx, tx); err != nil {
			return fmt.Errorf("迁移 %d 的自定义步骤失败: %w", m.Version, err)
		}
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO settings(k, v) VALUES(?, ?)
		 ON CONFLICT(k) DO UPDATE SET v = excluded.v`,
		SettingSchemaVersion, strconv.Itoa(m.Version)); err != nil {
		return err
	}
	return tx.Commit()
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 80 {
		s = s[:80] + "…"
	}
	return s
}

// MigrationPlan is what `doctor` / `serve` prints before opening: which
// versions would be applied. Empty means "already up to date".
func MigrationPlan(current int) []string {
	var out []string
	for _, m := range migrations {
		if m.Version > current {
			out = append(out, fmt.Sprintf("%d-%s", m.Version, m.Name))
		}
	}
	return out
}

// LatestSchemaVersion is the newest migration known to this binary.
func LatestSchemaVersion() int { return migrations[len(migrations)-1].Version }
