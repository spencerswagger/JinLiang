// Package dsn parses and builds connection strings without carrying a password
// into anything that gets printed.
//
// 为什么单独一个包：控制面（存连接）和连接器（用连接）都要解析 DSN，
// 而它们不该互相依赖。这个包只做字符串处理，不引入任何驱动。
package dsn

import (
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

// Kinds we support in B0.
const (
	KindPostgres = "postgres"
	KindMySQL    = "mysql"
)

// Default SSLModes, chosen per docs/SECURITY.md §3: verify-full unless the user
// explicitly downgrades. A downgraded mode is surfaced as a warning, never
// silently accepted.
const (
	DefaultPGSSLMode = "verify-full"
)

// Endpoint is one database's coordinates plus its password.
//
// 这个结构**会**带着明文密码，所以它只能活在内存里、只传给它需要的人：
//   - 不要 json.Marshal 它；
//   - 打印请用 Redacted()。
type Endpoint struct {
	Kind     string
	Host     string
	Port     int
	Database string
	Username string
	Password string
	Params   map[string]string
}

// DefaultPort returns the well-known port for a kind.
func DefaultPort(kind string) int {
	switch kind {
	case KindPostgres:
		return 5432
	case KindMySQL:
		return 3306
	}
	return 0
}

// Parse accepts:
//
//	postgres://user:pass@host:5432/db?sslmode=verify-full
//	postgresql://... (alias)
//	mysql://user:pass@host:3306/db?charset=utf8mb4
//	user:pass@tcp(host:3306)/db   (go-sql-driver native form)
//
// Anything else is an error with a human sentence — the UI shows it verbatim.
func Parse(raw string) (Endpoint, error) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return Endpoint{}, fmt.Errorf("连接串是空的")
	}
	switch {
	case strings.HasPrefix(s, "postgres://"), strings.HasPrefix(s, "postgresql://"):
		return parseURL(s, KindPostgres)
	case strings.HasPrefix(s, "mysql://"):
		return parseURL(s, KindMySQL)
	case strings.Contains(s, "@tcp("):
		return parseMySQLNative(s)
	default:
		return Endpoint{}, fmt.Errorf("看不懂这个连接串（需要 postgres://… 或 mysql://…，收到 %q）", redactRaw(s))
	}
}

func parseURL(raw, kind string) (Endpoint, error) {
	if kind == KindPostgres && strings.HasPrefix(raw, "postgresql://") {
		raw = "postgres://" + strings.TrimPrefix(raw, "postgresql://")
	}
	if kind == KindMySQL {
		raw = "mysql://" + strings.TrimPrefix(raw, "mysql://")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return Endpoint{}, fmt.Errorf("连接串解析失败: %w", redactErr(err))
	}
	ep := Endpoint{Kind: kind, Params: map[string]string{}}
	ep.Host = u.Hostname()
	if u.Port() != "" {
		p, err := strconv.Atoi(u.Port())
		if err != nil {
			return Endpoint{}, fmt.Errorf("端口 %q 不是数字", u.Port())
		}
		ep.Port = p
	}
	if ep.Port == 0 {
		ep.Port = DefaultPort(kind)
	}
	ep.Database = strings.TrimPrefix(u.Path, "/")
	if u.User != nil {
		ep.Username = u.User.Username()
		if pw, ok := u.User.Password(); ok {
			ep.Password = pw
		}
	}
	for k, v := range u.Query() {
		if len(v) > 0 {
			ep.Params[k] = v[len(v)-1]
		}
	}
	if ep.Host == "" {
		return Endpoint{}, fmt.Errorf("连接串缺少主机地址")
	}
	return ep, nil
}

// parseMySQLNative handles user:pass@tcp(host:port)/dbname?params.
func parseMySQLNative(raw string) (Endpoint, error) {
	ep := Endpoint{Kind: KindMySQL, Params: map[string]string{}}
	at := strings.LastIndex(raw, "@")
	if at < 0 {
		return Endpoint{}, fmt.Errorf("连接串缺少 @")
	}
	cred, rest := raw[:at], raw[at+1:]
	if c := strings.Index(cred, ":"); c >= 0 {
		ep.Username, ep.Password = cred[:c], cred[c+1:]
	} else {
		ep.Username = cred
	}
	if !strings.HasPrefix(rest, "tcp(") {
		return Endpoint{}, fmt.Errorf("只支持 tcp(host:port) 形式的地址")
	}
	end := strings.Index(rest, ")")
	if end < 0 {
		return Endpoint{}, fmt.Errorf("地址缺少右括号")
	}
	hostport := rest[len("tcp("):end]
	host, port, err := net.SplitHostPort(hostport)
	if err != nil {
		return Endpoint{}, fmt.Errorf("地址 %q 无法解析: %w", hostport, err)
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		return Endpoint{}, fmt.Errorf("端口 %q 不是数字", port)
	}
	ep.Host, ep.Port = host, p
	rest = rest[end+1:]
	rest = strings.TrimPrefix(rest, "/")
	if q := strings.Index(rest, "?"); q >= 0 {
		db, query := rest[:q], rest[q+1:]
		ep.Database = db
		for _, kv := range strings.Split(query, "&") {
			if kv == "" {
				continue
			}
			k, v, _ := strings.Cut(kv, "=")
			if dk, derr := url.QueryUnescape(k); derr == nil {
				k = dk
			}
			if dv, derr := url.QueryUnescape(v); derr == nil {
				v = dv
			}
			ep.Params[k] = v
		}
	} else {
		ep.Database = rest
	}
	return ep, nil
}

// PostgresURL renders a libpq URL. sslmode defaults to verify-full.
func (e Endpoint) PostgresURL() string {
	params := e.copyParams()
	if _, ok := params["sslmode"]; !ok {
		params["sslmode"] = DefaultPGSSLMode
	}
	q := encodeSorted(params)
	host := net.JoinHostPort(e.Host, strconv.Itoa(e.Port))
	cred := url.UserPassword(e.Username, e.Password)
	return fmt.Sprintf("postgres://%s@%s/%s?%s", cred.String(), host, url.PathEscape(e.Database), q)
}

// MySQLConfig renders a go-sql-driver DSN.
func (e Endpoint) MySQLConfig() string {
	params := e.copyParams()
	if _, ok := params["parseTime"]; !ok {
		params["parseTime"] = "true"
	}
	if _, ok := params["charset"]; !ok {
		params["charset"] = "utf8mb4"
	}
	auth := e.Username
	if e.Password != "" {
		auth += ":" + e.Password
	}
	return fmt.Sprintf("%s@tcp(%s)/%s?%s", auth,
		net.JoinHostPort(e.Host, strconv.Itoa(e.Port)), e.Database, encodeSorted(params))
}

func (e Endpoint) copyParams() map[string]string {
	out := make(map[string]string, len(e.Params)+2)
	for k, v := range e.Params {
		out[k] = v
	}
	return out
}

// SSLMode reports the effective sslmode for postgres ("" for mysql, which uses
// the tls parameter instead).
func (e Endpoint) SSLMode() string {
	if e.Kind != KindPostgres {
		return ""
	}
	if v, ok := e.Params["sslmode"]; ok && v != "" {
		return v
	}
	return DefaultPGSSLMode
}

// WeakTLS reports whether this endpoint's transport protection is below the
// product default. The API returns it as a warning so the UI can mark it red
// (docs/SECURITY.md §3: 降级必须标红并让用户确认).
func (e Endpoint) WeakTLS() (bool, string) {
	switch e.Kind {
	case KindPostgres:
		mode := e.SSLMode()
		if mode == DefaultPGSSLMode {
			return false, ""
		}
		return true, fmt.Sprintf("当前 sslmode=%s，低于默认的 verify-full：链路上的数据可能被旁听", mode)
	case KindMySQL:
		if tls, ok := e.Params["tls"]; !ok || tls == "false" || tls == "skip-verify" {
			return true, "当前 MySQL 连接没有校验证书（tls=" + orDefault(e.Params["tls"], "false") + "），链路上的数据可能被旁听"
		}
	}
	return false, ""
}

// Redacted renders the coordinates with the password removed. Safe to log.
func (e Endpoint) Redacted() string {
	return fmt.Sprintf("%s://%s@%s:%d/%s", e.Kind, e.Username, e.Host, e.Port, e.Database)
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func encodeSorted(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, url.QueryEscape(k)+"="+url.QueryEscape(m[k]))
	}
	return strings.Join(parts, "&")
}

// redactRaw best-effort hides a password in an unparseable string.
func redactRaw(s string) string {
	if at := strings.LastIndex(s, "@"); at > 0 {
		if c := strings.Index(s, "://"); c >= 0 && c+3 < at {
			scheme, cred := s[:c+3], s[c+3:at]
			if colon := strings.Index(cred, ":"); colon >= 0 {
				return scheme + cred[:colon] + ":***" + s[at:]
			}
		}
	}
	return s
}

// redactErr strips credentials out of url.Parse errors.
func redactErr(err error) error {
	if ue, ok := err.(*url.Error); ok {
		return fmt.Errorf("%s %s", ue.Op, redactRaw(ue.URL))
	}
	return err
}
