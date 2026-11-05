package httpapi

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/netcheck"
)

// 鉴权模型（docs/SECURITY.md §0、docs/CONTROL-PLANE.md §9）：
//   - v1 单管理员，口令存 argon2id 哈希；没有多用户/角色。
//   - 浏览器用会话 cookie（HttpOnly + SameSite=Lax + 24h），写操作额外校验
//     CSRF 双提交 token。
//   - 自动化用 Authorization: Bearer <token>：登录接口返回的会话 token，
//     或启动时 --api-token / JINLIANG_API_TOKEN 给的固定 token。
//     Bearer 请求不需要 CSRF（没有 cookie 就没有跨站自动携带的问题）。

const (
	sessionCookie = "jl_session"
	csrfCookie    = "jl_csrf"
	csrfHeader    = "X-CSRF-Token"
	sessionTTL    = 24 * time.Hour
)

type role int

const (
	roleAny role = iota // 只需要"管理员已配置"
	roleUser
)

type session struct {
	created time.Time
	expires time.Time
}

type sessionStore struct {
	mu  sync.Mutex
	by  map[string]session
	now func() time.Time
	ttl time.Duration
}

func newSessionStore(now func() time.Time, ttl time.Duration) *sessionStore {
	return &sessionStore{by: map[string]session{}, now: now, ttl: ttl}
}

func (s *sessionStore) create() (string, session, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", session{}, err
	}
	token := base64.RawURLEncoding.EncodeToString(buf)
	now := s.now()
	sess := session{created: now, expires: now.Add(s.ttl)}
	s.mu.Lock()
	s.by[token] = sess
	s.mu.Unlock()
	return token, sess, nil
}

func (s *sessionStore) valid(token string) bool {
	if token == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.by[token]
	if !ok {
		return false
	}
	if s.now().After(sess.expires) {
		delete(s.by, token)
		return false
	}
	return true
}

func (s *sessionStore) drop(token string) {
	s.mu.Lock()
	delete(s.by, token)
	s.mu.Unlock()
}

// bearerToken reads the Authorization header only.
//
// 曾经允许 /events?token=… 兜底（EventSource 不能设请求头），但 query 里的凭证
// 会进访问日志、浏览器历史、Referer，所以删掉了：浏览器用 cookie（EventSource
// 自动携带），curl 用 Authorization 头。这条由 TestSSERejectsQueryToken 钉住。
func bearerToken(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if len(h) > 7 && strings.EqualFold(h[:7], "bearer ") {
		return strings.TrimSpace(h[7:])
	}
	return ""
}

// authenticated reports whether this request carries a valid credential.
func (s *Server) authenticated(r *http.Request) bool {
	if tok := bearerToken(r); tok != "" {
		if s.apiToken != "" && subtle.ConstantTimeCompare([]byte(tok), []byte(s.apiToken)) == 1 {
			return true
		}
		return s.sessions.valid(tok)
	}
	if c, err := r.Cookie(sessionCookie); err == nil && s.sessions.valid(c.Value) {
		return true
	}
	return false
}

func (s *Server) usesBearer(r *http.Request) bool { return bearerToken(r) != "" }

// csrfOK checks the double-submit token for cookie-authenticated writes.
func (s *Server) csrfOK(r *http.Request) bool {
	if s.usesBearer(r) {
		return true // Bearer 不会被浏览器自动附带，不存在 CSRF
	}
	header := r.Header.Get(csrfHeader)
	if header == "" {
		header = r.FormValue("csrf_token")
	}
	cookie, err := r.Cookie(csrfCookie)
	if err != nil || cookie.Value == "" || header == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(header), []byte(cookie.Value)) == 1
}

// wrap enforces authentication, CSRF (for writes) and content negotiation.
func (s *Server) wrap(h http.HandlerFunc, need role) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		configured, err := s.store.AdminConfigured(ctx)
		if err != nil {
			s.writeError(w, r, http.StatusInternalServerError, plainError("%v", err))
			return
		}
		if !configured {
			// 首启：还没建管理员。给一个明确的下一步，不是 401。
			s.writeError(w, r, http.StatusPreconditionRequired, apiError{
				Class:   "user_action",
				Message: "还没有设置管理员口令",
				Why:     "第一次启动需要先设一个口令，之后用浏览器或 token 访问",
				Next:    []string{"POST /api/setup {\"password\":\"...\"}", "或者用浏览器打开首页，会引导你设口令"},
				Risk:    "口令不存明文，只存 argon2id 哈希；忘了只能删控制库重建",
			})
			return
		}
		if need == roleUser && !s.authenticated(r) {
			w.Header().Set("WWW-Authenticate", `Bearer realm="jinliang"`)
			s.writeError(w, r, http.StatusUnauthorized, apiError{
				Class:   "user_action",
				Message: "需要先登录",
				Why:     "控制面能改连接与任务，所以不允许匿名访问",
				Next:    []string{"POST /api/login {\"password\":\"...\"} 拿到 token", "或带上 Authorization: Bearer <token>"},
				Risk:    "没有风险：这只是本地控制台",
			})
			return
		}
		if isWrite(r.Method) && !s.csrfOK(r) {
			s.writeError(w, r, http.StatusForbidden, apiError{
				Class:   "user_action",
				Message: "缺少 CSRF token",
				Why:     "浏览器 cookie 会被自动带上，所以写操作必须额外证明请求来自本页",
				Next:    []string{"页面会自动带上 X-CSRF-Token 头", "curl 请改用 Authorization: Bearer <token>"},
				Risk:    "无",
			})
			return
		}
		h(w, r)
	}
}

func isWrite(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	}
	return false
}

// guard is wrap for HTML pages: unauthenticated visitors get redirected to
// /login (or /setup) instead of a JSON error.
func (s *Server) guard(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		configured, err := s.store.AdminConfigured(ctx)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !configured {
			http.Redirect(w, r, "/setup", http.StatusSeeOther)
			return
		}
		if !s.authenticated(r) {
			next := url.QueryEscape(r.URL.Path)
			http.Redirect(w, r, "/login?next="+next, http.StatusSeeOther)
			return
		}
		if isWrite(r.Method) && !s.csrfOK(r) {
			s.renderPage(w, r, "error_page.html", pageData{
				Title: "操作被拒绝",
				Err: &apiError{
					Message: "缺少 CSRF token（页面可能过期了）",
					Why:     "写操作需要页面里的防伪 token，浏览器 cookie 单独不够",
					Next:    []string{"刷新页面重试"},
					Risk:    "无",
				},
			})
			return
		}
		h(w, r)
	}
}

// loginLimiter allows 5 attempts per minute per IP (docs/CONTROL-PLANE.md §9).
type loginLimiter struct {
	mu     sync.Mutex
	now    func() time.Time
	hits   map[string][]time.Time
	max    int
	window time.Duration
}

func newLoginLimiter(now func() time.Time, max int, window time.Duration) *loginLimiter {
	return &loginLimiter{now: now, hits: map[string][]time.Time{}, max: max, window: window}
}

func (l *loginLimiter) allow(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	cutoff := now.Add(-l.window)
	var kept []time.Time
	for _, t := range l.hits[ip] {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	if len(kept) >= l.max {
		l.hits[ip] = kept
		return false
	}
	l.hits[ip] = append(kept, now)
	return true
}

func (l *loginLimiter) wrap(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !l.allow(remoteIP(r)) {
			w.Header().Set("Retry-After", "60")
			writeJSON(w, http.StatusTooManyRequests, errorEnvelope{Error: apiError{
				Class:   "user_action",
				Message: "尝试次数太多，等一分钟再试",
				Why:     "限制每分钟 5 次，防止有人对着控制台暴力猜口令",
				Next:    []string{"60 秒后再试"},
				Risk:    "无",
			}})
			return
		}
		h(w, r)
	}
}

// ensureCSRFCookie sets the double-submit token cookie when it is missing, so a
// visitor who lands on /setup or /api/session gets one before any write.
//
// 首启端点也不能裸奔：CSRF 双提交要求的正是「页面给过的 token」，所以 GET 侧
// 必须先把 cookie 种上，否则 POST 侧永远校验不过。
func (s *Server) ensureCSRFCookie(w http.ResponseWriter, r *http.Request) string {
	if c, err := r.Cookie(csrfCookie); err == nil && c.Value != "" {
		return c.Value
	}
	token, err := newCSRFToken()
	if err != nil {
		return ""
	}
	http.SetCookie(w, &http.Cookie{
		Name: csrfCookie, Value: token, Path: "/",
		HttpOnly: false, SameSite: http.SameSiteLaxMode, Secure: r.TLS != nil,
		Expires: s.now().Add(sessionTTL),
	})
	return token
}

// setSession issues the session + CSRF cookies and returns the token.
func (s *Server) setSession(w http.ResponseWriter, r *http.Request) (string, error) {
	token, sess, err := s.sessions.create()
	if err != nil {
		return "", err
	}
	csrf, err := newCSRFToken()
	if err != nil {
		return "", err
	}
	secure := r.TLS != nil
	http.SetCookie(w, &http.Cookie{
		Name: sessionCookie, Value: token, Path: "/",
		HttpOnly: true, SameSite: http.SameSiteLaxMode, Secure: secure,
		Expires: sess.expires,
	})
	http.SetCookie(w, &http.Cookie{
		Name: csrfCookie, Value: csrf, Path: "/",
		HttpOnly: false, // 页面 JS 需要读出来放进请求头（双提交）
		SameSite: http.SameSiteLaxMode, Secure: secure,
		Expires: sess.expires,
	})
	return token, nil
}

func newCSRFToken() (string, error) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

func (s *Server) clearSession(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(sessionCookie); err == nil {
		s.sessions.drop(c.Value)
	}
	for _, name := range []string{sessionCookie, csrfCookie} {
		http.SetCookie(w, &http.Cookie{Name: name, Value: "", Path: "/", MaxAge: -1, HttpOnly: name == sessionCookie})
	}
}

// ── 首启与登录 ──────────────────────────────────────────────

// handleSetup creates the administrator password. Only allowed once, and only
// while no password exists (otherwise an unauthenticated caller could reset it).
//
// 首启端点同样需要 CSRF 与限速：能被跨站 POST 打中的 127.0.0.1 端点，等于把
// 「谁先设口令谁就是管理员」交给别人。token 由 GET /api/session 或 GET /setup 种下。
func (s *Server) handleSetup(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	configured, err := s.store.AdminConfigured(ctx)
	if err != nil {
		s.writeError(w, r, http.StatusInternalServerError, plainError("%v", err))
		return
	}
	if configured {
		// 已设过口令：这是幂等语义问题，先回答它，与来源无关。
		s.writeError(w, r, http.StatusConflict, apiError{
			Class:   "user_action",
			Message: "管理员口令已经设置过了",
			Why:     "首启只允许设置一次，避免别人在你之后把口令改掉",
			Next:    []string{"用现有口令登录：POST /api/login"},
			Risk:    "忘了口令只能删控制库重建（任务会丢，可用 export 的 yaml 重建）",
		})
		return
	}
	// A-1（从 #6 同批合入）：还没设口令时，只有本机能定这个口令。
	// 顺序放在 CSRF 之前——先回答「你能不能做这件事」，再谈「这个请求是不是伪造的」。
	// CSRF 挡不住直连：攻击者自己 GET 一次 /api/session 就能把 token 镜像进请求头。
	if !netcheck.IsLoopbackRequest(r) {
		s.writeError(w, r, http.StatusForbidden, setupLoopbackOnly())
		return
	}
	if !s.csrfOK(r) {
		s.writeError(w, r, http.StatusForbidden, apiError{
			Class:   "user_action",
			Message: "缺少 CSRF token",
			Why:     "首启端点也能被跨站请求打中；设口令这种写操作必须证明来自本页",
			Next:    []string{"先 GET /api/session（会种下 jl_csrf cookie），再把它的值放进 X-CSRF-Token 头", "或者直接用浏览器打开首页走引导"},
			Risk:    "无",
		})
		return
	}
	var body struct {
		Password string `json:"password"`
	}
	if err := decodeJSON(r, &body); err != nil {
		s.writeError(w, r, http.StatusBadRequest, plainError("请求体不是合法 JSON：%v", err))
		return
	}
	if len(body.Password) < control.MinPasswordLen {
		s.writeError(w, r, http.StatusBadRequest, plainError("口令至少 %d 个字符", control.MinPasswordLen))
		return
	}
	if err := s.store.SetAdminPassword(ctx, body.Password); err != nil {
		s.writeError(w, r, http.StatusBadRequest, plainError("%v", err))
		return
	}
	_ = s.store.Audit(ctx, control.ActorAPI, control.ActionAdminSet, map[string]any{"via": "api"})
	token, err := s.setSession(w, r)
	if err != nil {
		s.writeError(w, r, http.StatusInternalServerError, plainError("%v", err))
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"ok": true, "token": token,
		"next": "POST /api/connections 建第一个连接（可以只传 dsn 一条串）",
	})
}

// setupLoopbackOnly 是 A-1 的四段式文案：首启设口令只允许本机。
//
// 为什么 CSRF 在这里不够：攻击者自己 GET 一次 /api/session 就能把 jl_csrf 的值
// 镜像进请求头，双提交对他形同虚设。真正的边界是「这个请求来自哪台机器」。
func setupLoopbackOnly() apiError {
	return apiError{
		Class:   "user_action",
		Message: "首次设置口令只能在跑控制台的那台机器上做",
		Why: "控制台现在监听在别的机器也能访问的地址上，而管理员口令还没设：" +
			"谁先设口令谁就是管理员，还能拿走全部连接凭证。这一步 CSRF 挡不住",
		Next: []string{
			"在跑 serve 的那台机器上打开 http://127.0.0.1:8080/setup 设置口令",
			"或者先停掉，用 --listen 127.0.0.1 起一次，设好口令后再放开监听",
		},
		Risk: "无：这条只在还没设口令时生效，只听本机时不会触发",
	}
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var body struct {
		Password string `json:"password"`
	}
	if err := decodeJSON(r, &body); err != nil {
		s.writeError(w, r, http.StatusBadRequest, plainError("请求体不是合法 JSON：%v", err))
		return
	}
	ok, err := s.store.VerifyAdminPassword(ctx, body.Password)
	if errors.Is(err, control.ErrAdminNotConfigured) {
		s.writeError(w, r, http.StatusPreconditionRequired, apiError{
			Class: "user_action", Message: "还没有设置管理员口令",
			Next: []string{"POST /api/setup {\"password\":\"...\"}"},
		})
		return
	}
	if err != nil {
		s.writeError(w, r, http.StatusInternalServerError, plainError("%v", err))
		return
	}
	if !ok {
		s.writeError(w, r, http.StatusUnauthorized, apiError{
			Class:   "user_action",
			Message: "口令不对",
			Why:     "控制库里的哈希与这次输入不匹配",
			Next:    []string{"再输一次（注意大小写）", "如果你刚换过 key 文件或控制库，口令可能已被重置"},
			Risk:    "无",
		})
		return
	}
	token, err := s.setSession(w, r)
	if err != nil {
		s.writeError(w, r, http.StatusInternalServerError, plainError("%v", err))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "token": token, "expires_in": int(sessionTTL.Seconds())})
}

func (s *Server) handleLogout(w http.ResponseWriter, r *http.Request) {
	s.clearSession(w, r)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) handleMeta(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	instance, _ := s.store.InstanceID(ctx)
	schemaV := s.store.SchemaVersionText(ctx)
	writeJSON(w, http.StatusOK, map[string]any{
		"version":        s.version,
		"instance_id":    instance,
		"schema_version": schemaV,
		"data_dir":       s.store.Dir(),
		"key_source":     string(s.store.Cipher().KeySource()),
		"listen":         s.listen,
		"auth": map[string]any{
			"mode":         "single-admin",
			"session_ttl":  int(sessionTTL.Seconds()),
			"csrf":         "double-submit",
			"bearer":       true,
			"static_token": s.apiToken != "",
		},
	})
}
