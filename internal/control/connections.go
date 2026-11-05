package control

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// Connection is one set of database coordinates plus an encrypted password.
//
// 安全约定（docs/CONTROL-PLANE.md §3、§5）：
//   - SecretEnc 是本进程内唯一的密码载体，任何 struct 都不带明文 Password 字段，
//     所以「不小心把明文序列化进 JSON」在类型层面就不可能发生。
//   - 需要密码的地方必须显式调 Secret(ctx)，调用点一眼可见。
type Connection struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Kind     string `json:"kind"` // postgres | mysql
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`
	// SecretEnc: AES-256-GCM(密码)，nonce 随密文走。json 里永远不出现。
	SecretEnc   []byte `json:"-"`
	ParamsJSON  string `json:"-"`
	LastTestAt  string `json:"last_test_at,omitempty"`
	LastTestOK  *bool  `json:"last_test_ok,omitempty"`
	LastTestMsg string `json:"last_test_msg,omitempty"`
	CreatedAt   string `json:"created_at"`
	UpdatedAt   string `json:"updated_at"`
}

// Params decodes params_json (sslmode / charset / 额外参数).
func (c Connection) Params() map[string]string {
	out := map[string]string{}
	if strings.TrimSpace(c.ParamsJSON) == "" {
		return out
	}
	_ = json.Unmarshal([]byte(c.ParamsJSON), &out)
	return out
}

// HasSecret reports whether a non-empty password is stored, without decrypting.
// The API only ever exposes this boolean (has_secret), never the value.
func (c Connection) HasSecret() bool { return len(c.SecretEnc) > emptySecretLen }

// emptySecretLen is the ciphertext length of an empty plaintext:
// nonce || GCM tag (no body). Anything longer than that has a real password.
const emptySecretLen = nonceLen + 16

// ConnectionInput is what the API/UI submits. Field names match the form.
//
// 密码语义：新建时空字符串是合法输入（PG 可能用 trust/peer 认证）；
// 更新时空字符串表示「保留原密码」，要清空请显式传 clear_password。
type ConnectionInput struct {
	Name     string            `json:"name"`
	Kind     string            `json:"kind"`
	Host     string            `json:"host"`
	Port     int               `json:"port"`
	Database string            `json:"database"`
	Username string            `json:"username"`
	Password string            `json:"password,omitempty"`
	Params   map[string]string `json:"params,omitempty"`
	// DSN, when set, is parsed into the fields above (headless/curl and
	// `import --connection-from-env` convenience). ParseDSN lives in
	// internal/connect and is applied by the caller before Validate.
	DSN string `json:"dsn,omitempty"`
	// ClearPassword wipes the stored secret (explicit, so an empty form field
	// can never silently destroy a working password).
	ClearPassword bool `json:"clear_password,omitempty"`
}

// Validate normalises and checks a connection input.
func (in *ConnectionInput) Validate() error {
	in.Name = strings.TrimSpace(in.Name)
	in.Kind = strings.ToLower(strings.TrimSpace(in.Kind))
	in.Host = strings.TrimSpace(in.Host)
	in.Database = strings.TrimSpace(in.Database)
	in.Username = strings.TrimSpace(in.Username)
	if in.Name == "" {
		return fmt.Errorf("连接名不能为空（给个你认得出的名字，比如「生产订单库」）")
	}
	if len(in.Name) > 64 {
		return fmt.Errorf("连接名最多 64 个字符")
	}
	if in.Kind != "postgres" && in.Kind != "mysql" {
		return fmt.Errorf("kind 必须是 postgres 或 mysql（收到 %q）", in.Kind)
	}
	if in.Host == "" {
		return fmt.Errorf("主机地址不能为空")
	}
	if in.Port <= 0 || in.Port > 65535 {
		return fmt.Errorf("端口必须在 1–65535 之间（收到 %d）", in.Port)
	}
	if in.Database == "" {
		return fmt.Errorf("数据库名不能为空")
	}
	if in.Username == "" {
		return fmt.Errorf("用户名不能为空")
	}
	// params 是自由键值（sslmode/charset/...），但口令类参数绝不能落控制库：
	// params_json 是明文，列表接口还会原样回传——`{"params":{"sslpassword":"..."}}`
	// 或 DSN 里 `?sslpassword=` 会让口令绕过 AES-GCM 直接躺在 db/-wal 里。
	// 口令只有一个入口：password 字段（加密存储）。
	for k := range in.Params {
		if isSecretParamKey(k) {
			return fmt.Errorf("%w: 参数 %q 是口令类，不能放进 params（它会明文存进控制库并被接口回传）。请用 password 字段，或从环境变量导入 DSN", ErrInvalid, k)
		}
	}
	return nil
}

// secretParamKeys 是「一看就是口令」的参数名（大小写、- 与 _ 都不敏感）。
// 宁严勿松：多拦一个不常用的参数，比让口令明文落盘好。
var secretParamKeys = []string{"password", "sslpassword", "sslkey", "passphrase"}

func isSecretParamKey(k string) bool {
	norm := strings.ToLower(strings.NewReplacer("-", "", "_", "").Replace(strings.TrimSpace(k)))
	for _, bad := range secretParamKeys {
		if norm == bad {
			return true
		}
	}
	return false
}

// CreateConnection stores a connection, encrypting the password.
func (s *Store) CreateConnection(ctx context.Context, in ConnectionInput) (Connection, error) {
	in.Name = strings.TrimSpace(in.Name)
	in.Kind = strings.ToLower(strings.TrimSpace(in.Kind))
	if err := in.Validate(); err != nil {
		return Connection{}, err
	}
	id, err := NewID()
	if err != nil {
		return Connection{}, err
	}
	// AAD = 连接 id：密文与行绑定，搬到别的行解不开（见 crypto.go）
	secret, err := s.cipher.Seal([]byte(id), []byte(in.Password))
	if err != nil {
		return Connection{}, err
	}
	params := in.Params
	if params == nil {
		params = map[string]string{}
	}
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return Connection{}, err
	}
	now := s.nowText()
	conn := Connection{
		ID: id, Name: in.Name, Kind: in.Kind, Host: in.Host, Port: in.Port,
		Database: in.Database, Username: in.Username,
		SecretEnc: secret, ParamsJSON: string(paramsJSON),
		CreatedAt: now, UpdatedAt: now,
	}
	if _, err := s.db.ExecContext(ctx,
		`INSERT INTO connections
		 (id, name, kind, host, port, database, username, secret_enc, params_json, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		conn.ID, conn.Name, conn.Kind, conn.Host, conn.Port, conn.Database, conn.Username,
		conn.SecretEnc, conn.ParamsJSON, conn.CreatedAt, conn.UpdatedAt); err != nil {
		return Connection{}, fmt.Errorf("保存连接失败: %w", err)
	}
	return conn, nil
}

// UpdateConnection modifies a connection. An empty Password keeps the stored
// secret; ClearPassword wipes it. Nothing else can destroy a working password.
func (s *Store) UpdateConnection(ctx context.Context, id string, in ConnectionInput) (Connection, error) {
	cur, err := s.GetConnection(ctx, id)
	if err != nil {
		return Connection{}, err
	}
	if err := in.Validate(); err != nil {
		return Connection{}, err
	}
	// P0-2：坐标（kind/host/port/database/username）是 job_key 的输入，
	// 而 job_key 是「同一份同步只能有一个任务」的唯一防线（docs/IDENTITY.md §2）。
	// 被任务引用时改坐标，会让已有任务与指纹脱节：重新 import 同一份 yaml 就
	// 会算出另一个指纹、变出第二个任务，B1 里就是两条写入流打同一张目标表。
	// 所以这里守卫（与 DeleteConnection 的 ErrInUse 同构），要换地址请新建连接。
	//
	// 「数引用」与「写回」在同一事务里：分开做会有 TOCTOU 窗口——刚数完是 0，
	// 另一个请求建了引用它的任务，随后这次更新就把任务身份改掉了。
	secret := cur.SecretEnc
	switch {
	case in.ClearPassword:
		if secret, err = s.cipher.Seal([]byte(id), nil); err != nil {
			return Connection{}, err
		}
	case in.Password != "":
		if secret, err = s.cipher.Seal([]byte(id), []byte(in.Password)); err != nil {
			return Connection{}, err
		}
	}
	params := in.Params
	if params == nil {
		params = cur.Params()
	}
	paramsJSON, err := json.Marshal(params)
	if err != nil {
		return Connection{}, err
	}
	now := s.nowText()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Connection{}, err
	}
	defer tx.Rollback() //nolint:errcheck // 成功路径已提交

	if coordsChanged(cur, in) {
		var n int
		if err := tx.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM jobs WHERE source_id = ? OR target_id = ?`, id, id).Scan(&n); err != nil {
			return Connection{}, err
		}
		if n > 0 {
			return Connection{}, fmt.Errorf("%w：有 %d 个任务把它当身份的一部分（要换地址：新建连接 + 新建任务，然后把旧任务停用；有运行历史的任务不能再改源与目标）", ErrIdentityLocked, n)
		}
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE connections SET name=?, kind=?, host=?, port=?, database=?, username=?,
		 secret_enc=?, params_json=?, updated_at=? WHERE id=?`,
		in.Name, in.Kind, in.Host, in.Port, in.Database, in.Username,
		secret, string(paramsJSON), now, id); err != nil {
		return Connection{}, fmt.Errorf("更新连接失败: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Connection{}, err
	}
	return s.GetConnection(ctx, id)
}

const connectionCols = `id, name, kind, host, port, database, username, secret_enc, params_json,
	COALESCE(last_test_at,''), last_test_ok, COALESCE(last_test_msg,''), created_at, updated_at`

func scanConnection(sc interface{ Scan(...any) error }) (Connection, error) {
	var c Connection
	var lastOK sql.NullInt64
	err := sc.Scan(&c.ID, &c.Name, &c.Kind, &c.Host, &c.Port, &c.Database, &c.Username,
		&c.SecretEnc, &c.ParamsJSON, &c.LastTestAt, &lastOK, &c.LastTestMsg,
		&c.CreatedAt, &c.UpdatedAt)
	if err != nil {
		return Connection{}, err
	}
	if lastOK.Valid {
		b := lastOK.Int64 != 0
		c.LastTestOK = &b
	}
	return c, nil
}

// GetConnection loads one connection by id.
func (s *Store) GetConnection(ctx context.Context, id string) (Connection, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+connectionCols+` FROM connections WHERE id = ?`, id)
	c, err := scanConnection(row)
	if isNoRows(err) {
		return Connection{}, ErrNotFound
	}
	return c, err
}

// ListConnections returns every connection, name-sorted.
func (s *Store) ListConnections(ctx context.Context) ([]Connection, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+connectionCols+` FROM connections`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Connection
	for rows.Next() {
		c, err := scanConnection(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
}

// Secret decrypts the stored password for this connection. Callers must never
// render the result, log it, or put it in an API response.
func (s *Store) Secret(ctx context.Context, id string) (string, error) {
	c, err := s.GetConnection(ctx, id)
	if err != nil {
		return "", err
	}
	raw, err := s.cipher.Open([]byte(c.ID), c.SecretEnc)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

// RecordTest stores the outcome of a connectivity probe so the list page can
// show "上次测连：成功，PostgreSQL 16.2，42ms".
func (s *Store) RecordTest(ctx context.Context, id string, ok bool, msg string) error {
	flag := 0
	if ok {
		flag = 1
	}
	_, err := s.db.ExecContext(ctx,
		`UPDATE connections SET last_test_at=?, last_test_ok=?, last_test_msg=?, updated_at=? WHERE id=?`,
		s.nowText(), flag, clip(msg, 500), s.nowText(), id)
	return err
}

// DeleteConnection removes a connection, refusing while jobs still reference it
// (four-part message lives in the API layer).
func (s *Store) DeleteConnection(ctx context.Context, id string) error {
	n, err := s.CountJobsUsingConnection(ctx, id)
	if err != nil {
		return err
	}
	if n > 0 {
		return fmt.Errorf("%w: 还有 %d 个任务在用它", ErrInUse, n)
	}
	res, err := s.db.ExecContext(ctx, `DELETE FROM connections WHERE id = ?`, id)
	if err != nil {
		return err
	}
	if rows, _ := res.RowsAffected(); rows == 0 {
		return ErrNotFound
	}
	return nil
}

// CountJobsUsingConnection counts jobs whose source or target is this connection.
func (s *Store) CountJobsUsingConnection(ctx context.Context, id string) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM jobs WHERE source_id = ? OR target_id = ?`, id, id).Scan(&n)
	return n, err
}

// coordsChanged reports whether the identity-bearing fields changed. 名字、
// 口令、额外参数不属于身份，改它们不影响任务指纹。
func coordsChanged(cur Connection, in ConnectionInput) bool {
	return cur.Kind != in.Kind ||
		cur.Host != in.Host ||
		cur.Port != in.Port ||
		cur.Database != in.Database ||
		cur.Username != in.Username
}

func clip(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}
