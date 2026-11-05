package control

import (
	"context"
	"fmt"
	"strconv"
	"strings"

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
			// 建唯一索引前先结掉余留的活跃行（迁移时本进程还没开始跑任务），
			// 否则一个历史遗留的重复活跃行会让整条迁移失败。
			`UPDATE runs
			    SET ended_at = COALESCE(heartbeat_at, started_at),
			        finish_reason = COALESCE(finish_reason, 'interrupted')
			  WHERE ended_at IS NULL`,
			`CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_one_active
			    ON runs(job_id) WHERE ended_at IS NULL`,
		},
	},
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
