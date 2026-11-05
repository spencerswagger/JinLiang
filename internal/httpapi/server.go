// Package httpapi serves the control plane: REST for automation, SSE for live
// progress, and the embedded WebUI for humans.
//
// 分工：这个包只做"翻译" —— 把 control 的数据变成 JSON 或 HTML，把用户的
// 点击变成 control 的调用。业务规则不写在这里。
package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/core"
)

// Options configures the server.
type Options struct {
	Store   *control.Store
	Listen  string
	Version string
	// APIToken, when set, is accepted as a static bearer token. Handy for
	// cron jobs and containers that must not open a browser (headless 可达).
	APIToken string
	// Now is a test seam.
	Now func() time.Time
}

// Server is the HTTP surface.
type Server struct {
	store    *control.Store
	mux      *http.ServeMux
	apiToken string
	sessions *sessionStore
	limiter  *loginLimiter
	pages    map[string]*pageTemplate
	partials *pageTemplate
	static   fs.FS
	version  string
	started  time.Time
	now      func() time.Time
	// listen is the address we bound to, reported by /api/meta and the UI so
	// the user can see whether the control plane is exposed to the network.
	listen string
}

// New builds the server, loading embedded templates and static assets.
func New(opts Options) (*Server, error) {
	if opts.Store == nil {
		return nil, errors.New("httpapi: Store is required")
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	s := &Server{
		store:    opts.Store,
		mux:      http.NewServeMux(),
		apiToken: opts.APIToken,
		sessions: newSessionStore(now, 24*time.Hour),
		limiter:  newLoginLimiter(now, 5, time.Minute),
		version:  opts.Version,
		started:  now(),
		now:      now,
		listen:   opts.Listen,
	}
	pages, partials, static, err := loadTemplates()
	if err != nil {
		return nil, err
	}
	s.pages, s.partials, s.static = pages, partials, static
	s.routes()
	return s, nil
}

// Handler returns the root handler with panic recovery and request logging off
// (we log only what matters: errors and auth failures, never payloads).
func (s *Server) Handler() http.Handler {
	return s.recoverer(s.mux)
}

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.writeError(w, r, http.StatusInternalServerError, apiError{
					Code:    string(core.CodeDecoderBug),
					Class:   string(core.Bug),
					Message: "服务内部出错了",
					Why:     fmt.Sprintf("%v", rec),
					Next:    []string{"把这段时间的日志留下来提 issue", "重启一次 serve 通常能先恢复"},
					Risk:    "正在跑的任务会被中断（位点已提交，可以重跑）",
				})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

func (s *Server) routes() {
	// ── 认证与首启 ──────────────────────────────────────────
	s.mux.HandleFunc("GET /api/health", s.handleHealth)
	s.mux.HandleFunc("GET /api/meta", s.wrap(s.handleMeta, roleAny))
	s.mux.HandleFunc("POST /api/setup", s.limiter.wrap(s.handleSetup))
	s.mux.HandleFunc("POST /api/login", s.limiter.wrap(s.handleLogin))
	s.mux.HandleFunc("POST /api/logout", s.wrap(s.handleLogout, roleAny))
	s.mux.HandleFunc("GET /api/session", s.handleSession)

	// ── 连接 ────────────────────────────────────────────────
	s.mux.HandleFunc("GET /api/connections", s.wrap(s.handleListConnections, roleUser))
	s.mux.HandleFunc("POST /api/connections", s.wrap(s.handleCreateConnection, roleUser))
	s.mux.HandleFunc("GET /api/connections/{id}", s.wrap(s.handleGetConnection, roleUser))
	s.mux.HandleFunc("PUT /api/connections/{id}", s.wrap(s.handleUpdateConnection, roleUser))
	s.mux.HandleFunc("DELETE /api/connections/{id}", s.wrap(s.handleDeleteConnection, roleUser))
	s.mux.HandleFunc("POST /api/connections/{id}/test", s.wrap(s.handleTestConnection, roleUser))
	s.mux.HandleFunc("GET /api/catalog", s.wrap(s.handleCatalog, roleUser))

	// ── 任务 ────────────────────────────────────────────────
	s.mux.HandleFunc("GET /api/jobs", s.wrap(s.handleListJobs, roleUser))
	s.mux.HandleFunc("POST /api/jobs", s.wrap(s.handleCreateJob, roleUser))
	s.mux.HandleFunc("GET /api/jobs/{id}", s.wrap(s.handleGetJob, roleUser))
	s.mux.HandleFunc("PUT /api/jobs/{id}", s.wrap(s.handleUpdateJob, roleUser))
	s.mux.HandleFunc("DELETE /api/jobs/{id}", s.wrap(s.handleDeleteJob, roleUser))
	s.mux.HandleFunc("GET /api/jobs/{id}/yaml", s.wrap(s.handleExportJob, roleUser))
	s.mux.HandleFunc("POST /api/jobs/{id}/preflight", s.wrap(s.handlePreflight, roleUser))
	s.mux.HandleFunc("POST /api/jobs/{id}/run", s.wrap(s.handleRun, roleUser))
	s.mux.HandleFunc("POST /api/jobs/{id}/pause", s.wrap(s.handlePause, roleUser))
	s.mux.HandleFunc("POST /api/jobs/{id}/resume", s.wrap(s.handleResume, roleUser))
	s.mux.HandleFunc("POST /api/jobs/{id}/stop", s.wrap(s.handleStop, roleUser))
	s.mux.HandleFunc("GET /api/jobs/{id}/events", s.wrap(s.handleEvents, roleUser))
	s.mux.HandleFunc("POST /api/jobs/import", s.wrap(s.handleImport, roleUser))
	s.mux.HandleFunc("GET /api/export", s.wrap(s.handleExportAll, roleUser))

	// ── 运维 ────────────────────────────────────────────────
	s.mux.HandleFunc("GET /api/runs", s.wrap(s.handleListRuns, roleUser))
	s.mux.HandleFunc("GET /api/audit", s.wrap(s.handleAudit, roleUser))
	s.mux.HandleFunc("GET /api/settings", s.wrap(s.handleGetSettings, roleUser))
	s.mux.HandleFunc("PUT /api/settings", s.wrap(s.handlePutSettings, roleUser))
	s.mux.HandleFunc("GET /api/doctor", s.wrap(s.handleDoctor, roleUser))

	// ── WebUI ───────────────────────────────────────────────
	s.mux.HandleFunc("GET /static/", s.handleStatic)
	s.mux.HandleFunc("GET /login", s.pageLogin)
	s.mux.HandleFunc("POST /login", s.pageLoginSubmit)
	s.mux.HandleFunc("GET /setup", s.pageSetup)
	s.mux.HandleFunc("POST /setup", s.pageSetupSubmit)
	s.mux.HandleFunc("POST /logout", s.pageLogout)
	s.mux.HandleFunc("GET /", s.pageOverview)
	s.mux.HandleFunc("GET /connections", s.guard(s.pageConnections))
	s.mux.HandleFunc("GET /connections/new", s.guard(s.pageConnectionNew))
	s.mux.HandleFunc("POST /connections", s.guard(s.pageConnectionCreate))
	s.mux.HandleFunc("GET /connections/{id}", s.guard(s.pageConnectionDetail))
	s.mux.HandleFunc("POST /connections/{id}/test", s.guard(s.pageConnectionTest))
	s.mux.HandleFunc("POST /connections/{id}/delete", s.guard(s.pageConnectionDelete))
	s.mux.HandleFunc("GET /jobs/new", s.guard(s.pageJobNew))
	s.mux.HandleFunc("GET /wizard/catalog", s.guard(s.pageWizardCatalog))
	s.mux.HandleFunc("POST /jobs", s.guard(s.pageJobCreate))
	s.mux.HandleFunc("GET /jobs/{id}", s.guard(s.pageJobDetail))
	s.mux.HandleFunc("GET /jobs/{id}/yaml", s.guard(s.pageJobYaml))
	s.mux.HandleFunc("POST /jobs/{id}/preflight", s.guard(s.pageJobPreflight))
	s.mux.HandleFunc("POST /jobs/{id}/run", s.guard(s.pageJobRun))
	s.mux.HandleFunc("POST /jobs/{id}/pause", s.guard(s.pageJobPause))
	s.mux.HandleFunc("POST /jobs/{id}/resume", s.guard(s.pageJobResume))
	s.mux.HandleFunc("POST /jobs/{id}/stop", s.guard(s.pageJobStop))
	s.mux.HandleFunc("POST /jobs/{id}/delete", s.guard(s.pageJobDelete))
	s.mux.HandleFunc("GET /history", s.guard(s.pageHistory))
	s.mux.HandleFunc("GET /diagnostics", s.guard(s.pageDiagnostics))
	s.mux.HandleFunc("GET /settings", s.guard(s.pageSettings))
	s.mux.HandleFunc("POST /settings/admin", s.guard(s.pageSettingsAdmin))
}

// handleHealth is unauthenticated: it answers "is the process up" and nothing
// else, so a supervisor can probe without a token.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"version": s.version,
		"uptime":  s.now().Sub(s.started).Round(time.Second).String(),
	})
}

// handleSession reports whether the caller is authenticated. The WebUI uses it
// to decide between login and app; it leaks nothing about the password state
// beyond "已设置/未设置", which the setup page needs anyway.
func (s *Server) handleSession(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	// 首启端点要 CSRF，而 CSRF token 只能由 GET 侧种下：/api/session 是所有
	// 客户端（浏览器、curl、容器脚本）的共同入口，所以在这里发。
	s.ensureCSRFCookie(w, r)
	configured, err := s.store.AdminConfigured(ctx)
	if err != nil {
		s.writeError(w, r, http.StatusInternalServerError, apiError{Message: err.Error()})
		return
	}
	authed := s.authenticated(r)
	instance, _ := s.store.InstanceID(ctx)
	writeJSON(w, http.StatusOK, map[string]any{
		"admin_configured": configured,
		"authenticated":    authed,
		"instance_id":      instance,
		"version":          s.version,
	})
}

// ── 错误信封 ────────────────────────────────────────────────

// apiError is the four-part prompt plus the machine-readable code. The UI
// renders it directly; clients can branch on code without parsing prose.
type apiError struct {
	Code    string   `json:"code,omitempty"`
	Class   string   `json:"class,omitempty"`
	Message string   `json:"message"`
	Why     string   `json:"why,omitempty"`
	Next    []string `json:"next,omitempty"`
	Risk    string   `json:"risk,omitempty"`
}

type errorEnvelope struct {
	Error apiError `json:"error"`
}

// Error makes apiError usable as an ordinary error, so handlers can return it
// from helpers without wrapping.
func (e apiError) Error() string { return e.Message }

// codeError builds an apiError from the shared catalogue, overriding message.
func codeError(code core.Code, message string) apiError {
	spec, ok := core.Lookup(code)
	if !ok {
		return apiError{Code: string(code), Message: message}
	}
	if message == "" {
		message = spec.Message
	}
	return apiError{
		Code: string(code), Class: string(spec.Class), Message: message,
		Why: spec.Why, Next: spec.Next, Risk: spec.Risk,
	}
}

// plainError is for validation problems whose message is already the answer.
func plainError(format string, args ...any) apiError {
	return apiError{
		Class:   string(core.UserAction),
		Message: fmt.Sprintf(format, args...),
	}
}

func (s *Server) writeError(w http.ResponseWriter, r *http.Request, status int, e apiError) {
	if wantsHTML(r) {
		s.renderPartial(w, "error_card", e)
		return
	}
	writeJSON(w, status, errorEnvelope{Error: e})
}

func wantsHTML(r *http.Request) bool {
	if r == nil {
		return false
	}
	if strings.Contains(r.Header.Get("HX-Request"), "true") {
		return true
	}
	accept := r.Header.Get("Accept")
	if strings.HasPrefix(r.URL.Path, "/api/") {
		return false
	}
	return strings.Contains(accept, "text/html")
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(body)
}

// decodeJSON reads a bounded JSON body and rejects unknown fields, so a typo in
// a curl call fails loudly instead of being silently ignored.
func decodeJSON(r *http.Request, dst any) error {
	const maxBody = 1 << 20
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, maxBody))
	dec.DisallowUnknownFields()
	return dec.Decode(dst)
}

// remoteIP strips the port; used by the login limiter.
func remoteIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
