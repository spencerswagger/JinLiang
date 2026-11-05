// Package connect implements the read-side endpoint contract from
// internal/source for postgres and mysql: version, capability probe and catalog
// enumeration.
//
// 为什么控制面需要它：B0 的「测连」和「选表」必须真的连上、真的列出表，
// 否则 WebUI 就只是摆样子。这里的实现同时满足 source.Endpoint，
// B1 的引擎可以直接复用同一份代码，不必再写一遍 catalog 查询。
//
// 只读：本包只发 SELECT / SHOW 一类语句，绝不写入对端数据库。
package connect

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/source"
)

// Conn is an open endpoint that can be closed.
type Conn interface {
	source.Endpoint
	Close() error
}

// Open dials one endpoint lazily (database/sql pools; the first query dials).
// It does not verify connectivity — call Version or Probe for that, so the
// caller controls timeouts and can report which step failed.
func Open(ep dsn.Endpoint) (Conn, error) {
	switch ep.Kind {
	case dsn.KindPostgres:
		db, err := sql.Open("pgx", ep.PostgresURL())
		if err != nil {
			return nil, fmt.Errorf("初始化 PostgreSQL 连接失败: %w", err)
		}
		db.SetMaxOpenConns(2)
		db.SetMaxIdleConns(1)
		db.SetConnMaxLifetime(5 * time.Minute)
		return &Postgres{db: db, ep: ep}, nil
	case dsn.KindMySQL:
		db, err := sql.Open("mysql", ep.MySQLConfig())
		if err != nil {
			return nil, fmt.Errorf("初始化 MySQL 连接失败: %w", err)
		}
		db.SetMaxOpenConns(2)
		db.SetMaxIdleConns(1)
		db.SetConnMaxLifetime(5 * time.Minute)
		return &MySQL{db: db, ep: ep}, nil
	}
	return nil, fmt.Errorf("不支持的数据库类型 %q（只支持 postgres / mysql）", ep.Kind)
}

// Probe runs "can I even talk to this server" with a short deadline and returns
// a human message on failure. It is what the 「测连」 button calls.
type ProbeResult struct {
	OK              bool            `json:"ok"`
	ServerVersion   string          `json:"server_version,omitempty"`
	VersionNum      int             `json:"version_num,omitempty"`
	LatencyMS       int64           `json:"latency_ms"`
	TLS             bool            `json:"tls"`
	TLSWarning      string          `json:"tls_warning,omitempty"`
	Capabilities    map[string]bool `json:"capabilities,omitempty"`
	CapabilityNotes []string        `json:"capability_notes,omitempty"`
	Message         string          `json:"message,omitempty"`
	Step            string          `json:"step,omitempty"` // 失败在哪一步，给用户看
	RawError        string          `json:"raw_error,omitempty"`
}

// Test probes one endpoint end to end: connect → version → capabilities.
// tlsReport lets the caller mark a downgraded transport red (docs/SECURITY.md §3).
func Test(ctx context.Context, ep dsn.Endpoint, timeout time.Duration) ProbeResult {
	if timeout <= 0 {
		timeout = 8 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	res := ProbeResult{}
	if weak, why := ep.WeakTLS(); weak {
		res.TLSWarning = why
	}
	start := time.Now()
	conn, err := Open(ep)
	if err != nil {
		res.Message = err.Error()
		res.Step = "build"
		return res
	}
	defer conn.Close()

	ver, err := conn.Version(ctx)
	if err != nil {
		res.LatencyMS = time.Since(start).Milliseconds()
		res.Step = "connect"
		res.Message = humanDialError(err)
		res.RawError = err.Error()
		return res
	}
	res.ServerVersion = ver
	res.LatencyMS = time.Since(start).Milliseconds()
	res.OK = true

	caps, err := conn.Probe(ctx)
	if err != nil {
		res.Step = "probe"
		res.Message = "连上了，但读不到能力信息：" + humanDialError(err)
	} else {
		res.Capabilities = map[string]bool{}
		for cap, st := range caps {
			res.Capabilities[string(cap)] = st.Supported
			if !st.Supported && st.Compensation.Note != "" {
				res.CapabilityNotes = append(res.CapabilityNotes, st.Compensation.Note)
			}
		}
	}
	res.TLS = detectTLS(ctx, conn)
	if res.TLSWarning == "" && !res.TLS {
		res.TLSWarning = "这条连接没有加密（对端未启用 TLS）：口令与数据会以明文过网络"
	}
	return res
}

// detectTLS asks the server whether *this* session is encrypted. Absence of an
// answer is treated as "unknown, don't claim encrypted".
func detectTLS(ctx context.Context, conn Conn) bool {
	switch c := conn.(type) {
	case *Postgres:
		var ssl bool
		if err := c.db.QueryRowContext(ctx,
			`SELECT COALESCE((SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()), false)`,
		).Scan(&ssl); err == nil {
			return ssl
		}
	case *MySQL:
		var name, val string
		if err := c.db.QueryRowContext(ctx, `SHOW SESSION STATUS LIKE 'Ssl_cipher'`).Scan(&name, &val); err == nil {
			return val != ""
		}
	}
	return false
}

// HumanError turns a driver error into a one-line sentence a user can act on.
// Exported because the control plane renders it in cards (docs/ERRORS.md).
func HumanError(err error) string { return humanDialError(err) }

// humanDialError turns driver errors into something a user can act on, and
// never echoes a password.
func humanDialError(err error) string {
	if err == nil {
		return ""
	}
	msg := err.Error()
	lower := strings.ToLower(msg)
	switch {
	case isTimeout(err) || strings.Contains(lower, "timeout") || strings.Contains(lower, "deadline exceeded"):
		return "连接超时：地址或端口可能不通（防火墙/安全组/实例没开）"
	case strings.Contains(lower, "password authentication failed"),
		strings.Contains(lower, "access denied for user"):
		return "口令或用户名不对（对端拒绝登录）"
	case strings.Contains(lower, "no such host"), strings.Contains(lower, "server misbehaving"),
		strings.Contains(lower, "name resolution"):
		return "解析不到这个主机名"
	case strings.Contains(lower, "connection refused"), strings.Contains(lower, "connect: connection refused"):
		return "端口没有在监听：对端数据库可能没启动，或端口写错了"
	case strings.Contains(lower, "does not exist"), strings.Contains(lower, "unknown database"):
		return "数据库不存在（名字写错，或这个账号看不到它）"
	case strings.Contains(lower, "certificate"), strings.Contains(lower, "x509"), strings.Contains(lower, "tls"):
		return "证书校验没通过：对端用的是自签证书？可以改成 sslmode=require（有被中间人风险，界面会标红）"
	case strings.Contains(lower, "pg_hba"), strings.Contains(lower, "no pg_hba.conf entry"):
		return "对端拒绝了来自这个地址的连接（pg_hba.conf 没放行这个 IP）"
	}
	// 兜底：脱敏后原样给出，用户/我们能据此搜到原因
	return redactError(msg)
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// redactError removes anything that looks like a password from a driver string.
func redactError(msg string) string {
	if i := strings.Index(msg, "password="); i >= 0 {
		end := strings.IndexAny(msg[i:], " \n\t")
		if end < 0 {
			end = len(msg) - i
		}
		msg = msg[:i] + "password=***" + msg[i+end:]
	}
	if i := strings.Index(msg, "postgres://"); i >= 0 {
		if j := strings.Index(msg[i:], "@"); j > 0 {
			cred := msg[i:j]
			if c := strings.Index(cred, ":"); c >= 0 {
				msg = msg[:i] + cred[:c] + ":***" + msg[i+j:]
			}
		}
	}
	return msg
}

// versionParts splits "16.2" / "8.0.36" into numbers, ignoring suffixes.
func versionParts(v string) (major, minor, patch int) {
	var extra string
	if i := strings.IndexAny(v, " -("); i >= 0 {
		v, extra = v[:i], v[i:]
	}
	_ = extra
	parts := strings.Split(v, ".")
	atoi := func(s string) int {
		n := 0
		for i := 0; i < len(s); i++ {
			if s[i] < '0' || s[i] > '9' {
				break
			}
			n = n*10 + int(s[i]-'0')
		}
		return n
	}
	if len(parts) > 0 {
		major = atoi(parts[0])
	}
	if len(parts) > 1 {
		minor = atoi(parts[1])
	}
	if len(parts) > 2 {
		patch = atoi(parts[2])
	}
	return
}

// capState builds a capability state with its fallback.
func capState(supported bool, since string, level source.Level, note, key string) source.CapabilityState {
	return source.CapabilityState{
		Supported:    supported,
		SinceVersion: since,
		Compensation: source.Compensation{Level: level, Note: note, ConfigKey: key},
	}
}

// splitQualified splits "schema.table" (table may be empty).
func splitQualified(s string) (schema, table string) {
	if i := strings.IndexByte(s, '.'); i >= 0 {
		return s[:i], s[i+1:]
	}
	return s, ""
}
