package control

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spencerswagger/jinliang/internal/state"
)

// settings 表的键。值一律是字符串（整数用 strconv），因为这张表要能被
// `sqlite3 jinliang.db 'select * from settings'` 直接读懂。
const (
	SettingSchemaVersion = "schema_version"      // 迁移版本，migrate.go 维护
	SettingInstanceID    = "instance_id"         // 实例身份（docs/IDENTITY.md §4）
	SettingCreatedAt     = "created_at"          // 控制库创建时间
	SettingAdminHash     = "admin_password_hash" // argon2id PHC 串，绝不存明文口令
	SettingAdminSetAt    = "admin_password_set_at"
	SettingDefaultBatch  = "default_batch_rows" // 新建向导的默认批次
	SettingDefaultListen = "listen"             // 上次使用的监听地址（仅展示）
	// SettingBootEpoch 每次进程启动重写一次：SSE 的 id 是 "<boot_epoch>-<seq>"，
	// 客户端带着旧 epoch 重连时我们发 reset，而不是假装能补齐（events.go）。
	SettingBootEpoch = "boot_epoch"
)

// Setting reads one settings row. The bool is false when the key is absent.
func (s *Store) Setting(ctx context.Context, k string) (string, bool, error) {
	var v string
	err := s.db.QueryRowContext(ctx, `SELECT v FROM settings WHERE k = ?`, k).Scan(&v)
	if isNoRows(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return v, true, nil
}

// SetSetting upserts one settings row.
func (s *Store) SetSetting(ctx context.Context, k, v string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO settings(k, v) VALUES(?, ?)
		 ON CONFLICT(k) DO UPDATE SET v = excluded.v`, k, v)
	return err
}

// Settings returns every non-secret setting, for the settings page.
func (s *Store) Settings(ctx context.Context) (map[string]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT k, v FROM settings ORDER BY k`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		if k == SettingAdminHash {
			continue // 口令哈希不出控制层
		}
		if k == SettingBootEpoch {
			continue // 每次 serve 启动都会变，展示出来只会让人以为配置被改了
		}
		out[k] = v
	}
	return out, rows.Err()
}

// InstanceID returns this deployment's instance identity.
func (s *Store) InstanceID(ctx context.Context) (string, error) {
	v, ok, err := s.Setting(ctx, SettingInstanceID)
	if err != nil {
		return "", err
	}
	if !ok || v == "" {
		return "", fmt.Errorf("控制库缺少 instance_id（%s 可能被手工改过）", s.path)
	}
	return v, nil
}

// instanceIDValue is the cached id from Open (no DB round-trip on every run).
func (s *Store) instanceIDValue() string { return s.instanceID }

// StartBootEpoch writes a fresh epoch and pins the event bus to it.
//
// 只有 `serve` 调它：doctor/export/import 这些短命进程也会 Open 同一个控制库，
// 如果它们也重写 epoch，设置页上的 boot_epoch 会一直跳，而且一个只跑 200ms 的
// CLI 会把正在服务的那个进程的 epoch 概念搅乱（SSE id 是 <epoch>-<seq>）。
func (s *Store) StartBootEpoch(ctx context.Context) string {
	id, err := newInstanceID()
	if err != nil {
		// 生成不出 id 时保留 NewBus 的进程内随机 epoch：SSE 仍能工作，
		// 只是重启后无法与旧 id 对齐（那本来就会发 reset）。
		return s.bus.Epoch()
	}
	if err := s.SetSetting(ctx, SettingBootEpoch, id); err != nil {
		return s.bus.Epoch()
	}
	s.bus.SetEpoch(id)
	return id
}

// HeartbeatInterval is how often a running job refreshes its heartbeat.
func (s *Store) HeartbeatInterval() time.Duration { return s.heartbeat }

// StaleAfter is the window after which a silent run is considered dead:
// three missed heartbeats (configurable indirectly via Options.Heartbeat).
func StaleAfter(heartbeat time.Duration) time.Duration {
	if heartbeat <= 0 {
		heartbeat = DefaultHeartbeatInterval
	}
	return 3 * heartbeat
}

// newInstanceID delegates to internal/state so CLI runs and the control plane
// agree on the format (26-char base32, docs/IDENTITY.md §4).
func newInstanceID() (string, error) { return state.NewInstanceID() }

// AdminConfigured reports whether an administrator password exists. First start
// uses this to decide between "建管理员" and "登录".
func (s *Store) AdminConfigured(ctx context.Context) (bool, error) {
	v, ok, err := s.Setting(ctx, SettingAdminHash)
	if err != nil {
		return false, err
	}
	return ok && strings.TrimSpace(v) != "", nil
}

// SetAdminPassword hashes and stores the administrator password (argon2id).
func (s *Store) SetAdminPassword(ctx context.Context, password string) error {
	hash, err := HashPassword(password)
	if err != nil {
		return err
	}
	if err := s.SetSetting(ctx, SettingAdminHash, hash); err != nil {
		return err
	}
	return s.SetSetting(ctx, SettingAdminSetAt, s.nowText())
}

// VerifyAdminPassword checks a candidate password against the stored hash.
// A missing hash is reported as "not configured", never as "wrong password".
func (s *Store) VerifyAdminPassword(ctx context.Context, password string) (bool, error) {
	hash, ok, err := s.Setting(ctx, SettingAdminHash)
	if err != nil {
		return false, err
	}
	if !ok || hash == "" {
		return false, ErrAdminNotConfigured
	}
	return VerifyPassword(hash, password)
}

// ErrAdminNotConfigured means first start has not run yet.
var ErrAdminNotConfigured = fmt.Errorf("还没有设置管理员口令")

// SchemaVersionText is used by the settings page and doctor output.
func (s *Store) SchemaVersionText(ctx context.Context) string {
	v, err := s.SchemaVersion(ctx)
	if err != nil {
		return "?"
	}
	return fmt.Sprintf("%d/%d", v, LatestSchemaVersion())
}
