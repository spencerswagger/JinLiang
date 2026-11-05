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
}

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

// migrate applies every migration newer than the recorded schema_version.
func (s *Store) migrate(ctx context.Context) error {
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
