package control

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite" // 纯 Go 驱动：保持 CGO_ENABLED=0 与交叉编译（docs/BUILD.md §1A）
)

const (
	// DBFileName is the control database file inside --data-dir.
	DBFileName = "jinliang.db"

	defaultDataDirName = "jinliang"
	dirPerm            = 0o700
	filePerm           = 0o600
)

// Options configures Store.Open.
type Options struct {
	// DataDir holds jinliang.db and (by default) the encryption key.
	DataDir string
	// KeyFile / EnvKey select an external master key (see LoadCipher).
	KeyFile string
	EnvKey  string
	// Autogen lets Open create <data-dir>/key when no key source is given.
	// Read-only callers (export) pass false.
	Autogen bool
	// Now is a test seam; nil means time.Now.
	Now func() time.Time
	// Heartbeat is how often a running job refreshes runs.heartbeat_at.
	// Zero means DefaultHeartbeatInterval (30s). 过期阈值固定取 3×它
	// （StaleAfter），B1 的长事务靠这个独立 ticker 而不是阶段推进来续命。
	Heartbeat time.Duration
}

// Store is the control database plus the cipher that protects connection
// secrets. All writes go through it; nothing else opens the SQLite file.
type Store struct {
	db         *sql.DB
	dir        string
	path       string
	cipher     *Cipher
	now        func() time.Time
	bus        *Bus
	runner     *Runner
	instanceID string // settings.instance_id：审计、租约、run 的 owner 都用它
	heartbeat  time.Duration
}

// DefaultDataDir returns ~/.config/jinliang (or $XDG_CONFIG_HOME/jinliang).
func DefaultDataDir() string {
	if xdg := strings.TrimSpace(os.Getenv("XDG_CONFIG_HOME")); xdg != "" {
		return filepath.Join(xdg, defaultDataDirName)
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return filepath.Join(home, ".config", defaultDataDirName)
	}
	return "." + string(filepath.Separator) + defaultDataDirName
}

// Open opens (and if needed creates) the control database and applies pending
// migrations. Safe to call twice in the same process; safe across restarts —
// that is the whole point of putting tasks here instead of in yaml.
func Open(opts Options) (*Store, error) {
	dir := opts.DataDir
	if strings.TrimSpace(dir) == "" {
		dir = DefaultDataDir()
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if err := ensurePrivateDir(abs); err != nil {
		return nil, err
	}

	cipher, err := LoadCipher(KeyOptions{
		KeyFile: opts.KeyFile,
		DataDir: abs,
		EnvKey:  opts.EnvKey,
		Autogen: opts.Autogen,
	})
	if err != nil {
		return nil, err
	}

	path := filepath.Join(abs, DBFileName)
	dsn := "file:" + url.PathEscape(path) +
		"?_pragma=busy_timeout(5000)" +
		"&_pragma=journal_mode(WAL)" +
		"&_pragma=foreign_keys(1)" +
		"&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("打开控制库失败: %w", err)
	}
	// 单写者：SQLite 的写本来就是串行的，把连接数压到 1 可以避免
	// 「PRAGMA 只在部分连接上生效」这类坑。读也不慢，控制库很小。
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("连接控制库失败: %w", err)
	}

	s := &Store{db: db, dir: abs, path: path, cipher: cipher, now: opts.Now, heartbeat: opts.Heartbeat}
	if s.now == nil {
		s.now = time.Now
	}
	if s.heartbeat <= 0 {
		s.heartbeat = DefaultHeartbeatInterval
	}
	s.bus = NewBus(s.now)
	if err := s.migrate(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if err := s.ensureInstance(ctx); err != nil {
		db.Close()
		return nil, err
	}
	if err := os.Chmod(path, filePerm); err != nil {
		db.Close()
		return nil, err
	}
	s.runner = newRunner(s)
	return s, nil
}

// Close releases the database handle.
func (s *Store) Close() error { return s.db.Close() }

// DB exposes the handle for tests and doctor (integrity_check).
func (s *Store) DB() *sql.DB { return s.db }

// Dir is the data directory in use.
func (s *Store) Dir() string { return s.dir }

// Path is the control database file.
func (s *Store) Path() string { return s.path }

// Cipher is the secret box for connection passwords.
func (s *Store) Cipher() *Cipher { return s.cipher }

// Now returns the store clock (RFC3339 UTC strings are what we persist).
func (s *Store) Now() time.Time { return s.now() }

func (s *Store) nowText() string { return s.now().UTC().Format(time.RFC3339) }

// ensurePrivateDir creates the data directory 0700 and refuses to keep a
// world-readable one (docs/CONTROL-PLANE.md §9).
func ensurePrivateDir(dir string) error {
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return fmt.Errorf("创建数据目录 %s 失败: %w", dir, err)
	}
	info, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("%s 不是目录", dir)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		// 我们自己建的目录：静默收紧。用户手建且开了口子：收紧并说明，不阻断。
		if err := os.Chmod(dir, perm&0o700|0o700); err != nil {
			return fmt.Errorf("%s 权限是 %04o（其他用户可读），且无法收紧: %w", dir, perm, err)
		}
	}
	return nil
}

// ensureInstance makes sure settings.instance_id exists. The instance id is the
// third identity layer (docs/IDENTITY.md §4): audit, lease owner, object owner.
func (s *Store) ensureInstance(ctx context.Context) error {
	if v, ok, err := s.Setting(ctx, SettingInstanceID); err != nil {
		return err
	} else if ok {
		s.instanceID = v
		return nil
	}
	id, err := newInstanceID()
	if err != nil {
		return err
	}
	s.instanceID = id
	return s.SetSetting(ctx, SettingInstanceID, id)
}

// doctor helpers ------------------------------------------------------------

// IntegrityCheck runs SQLite's PRAGMA integrity_check.
func (s *Store) IntegrityCheck(ctx context.Context) (string, error) {
	var res string
	err := s.db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&res)
	if err != nil {
		return "", err
	}
	return res, nil
}

// SchemaVersion reports the applied migration version.
func (s *Store) SchemaVersion(ctx context.Context) (int, error) {
	v, ok, err := s.Setting(ctx, SettingSchemaVersion)
	if err != nil || !ok {
		return 0, err
	}
	return atoiDefault(v, 0), nil
}

// DBStats returns row counts and file size, for the overview number wall.
func (s *Store) DBStats(ctx context.Context) (map[string]int64, error) {
	out := map[string]int64{}
	for _, table := range []string{"connections", "jobs", "runs", "audit"} {
		var n int64
		// table names are ours, not user input
		if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&n); err != nil {
			return nil, err
		}
		out[table] = n
	}
	if info, err := os.Stat(s.path); err == nil {
		out["db_bytes"] = info.Size()
	}
	return out, nil
}

func atoiDefault(s string, def int) int {
	n := 0
	if s == "" {
		return def
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return def
		}
		n = n*10 + int(s[i]-'0')
	}
	return n
}

// isNoRows keeps error handling short in query helpers.
func isNoRows(err error) bool { return errors.Is(err, sql.ErrNoRows) }
