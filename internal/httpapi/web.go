package httpapi

import (
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/spencerswagger/jinliang/internal/control"
)

// WebUI 打在一个二进制里：embed + html/template + 一份手写 CSS + vendored htmx。
// 没有 Node、没有构建步骤、没有 node_modules（docs/BUILD.md §1A）。
//
//go:embed web/templates/*.html web/static/*
var webFS embed.FS

type pageTemplate struct {
	*template.Template
	name string
}

type pageData struct {
	Title    string
	Nav      string
	Version  string
	Instance string
	Listen   string
	CSRF     string
	Flash    string
	Now      string
	Data     any
	Err      *apiError
}

// loadTemplates builds one template set per page (layout + page), plus one set
// for the htmx partials. Rendering a page therefore cannot accidentally pull in
// another page's blocks.
func loadTemplates() (map[string]*pageTemplate, *pageTemplate, fs.FS, error) {
	staticFS, err := fs.Sub(webFS, "web/static")
	if err != nil {
		return nil, nil, nil, err
	}
	funcs := template.FuncMap{
		"phaseLabel": control.PhaseLabel,
		// runState 用 finish_reason 覆盖 phase：`done` 既可能是「跑完了」也可能是
		// 「用户手动停止」，只看 phase 会把手动停止显示成顺利完成（Ark P0-3）。
		"runState": control.RunStateLabel,
		// 颜色与文案必须同一口径：phase=done + finish_reason=stopped 不能染成绿色。
		"runBadgeClass": control.RunBadgeClass,
		"phaseHint":     control.PhaseHint,
		"shortTime": func(s string) string {
			t, err := time.Parse(time.RFC3339, s)
			if err != nil {
				return s
			}
			return t.Local().Format("01-02 15:04:05")
		},
		"pct": func(done, total int64) int {
			if total <= 0 {
				return 0
			}
			return int(done * 100 / total)
		},
		"clip": func(s string, n int) string {
			r := []rune(s)
			if len(r) <= n {
				return s
			}
			return string(r[:n]) + "…"
		},
		"dash": func(s string) string {
			if strings.TrimSpace(s) == "" {
				return "—"
			}
			return s
		},
		"okOr": func(b *bool) string {
			if b == nil {
				return "未测过"
			}
			if *b {
				return "成功"
			}
			return "失败"
		},
	}

	entries, err := fs.ReadDir(webFS, "web/templates")
	if err != nil {
		return nil, nil, nil, err
	}
	pages := map[string]*pageTemplate{}
	var partialsTmpl *template.Template
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".html") {
			continue
		}
		tmpl, err := template.New(e.Name()).Funcs(funcs).
			ParseFS(webFS, "web/templates/layout.html", "web/templates/error_card.html", "web/templates/"+e.Name())
		if err != nil {
			return nil, nil, nil, fmt.Errorf("模板 %s 解析失败: %w", e.Name(), err)
		}
		if strings.HasSuffix(e.Name(), "_partial.html") || e.Name() == "error_card.html" || e.Name() == "layout.html" {
			continue
		}
		pages[e.Name()] = &pageTemplate{Template: tmpl, name: e.Name()}
	}
	partialsTmpl, err = template.New("partials").Funcs(funcs).
		ParseFS(webFS, "web/templates/*_partial.html", "web/templates/error_card.html")
	if err != nil {
		return nil, nil, nil, fmt.Errorf("partial 模板解析失败: %w", err)
	}
	return pages, &pageTemplate{Template: partialsTmpl, name: "partials"}, staticFS, nil
}

// renderPage renders a full page through layout.html.
func (s *Server) renderPage(w http.ResponseWriter, r *http.Request, name string, data pageData) {
	pt, ok := s.pages[name]
	if !ok {
		http.Error(w, "template not found: "+name, http.StatusInternalServerError)
		return
	}
	s.fillPage(r, &data)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := pt.ExecuteTemplate(w, "layout", data); err != nil {
		// 渲染中途失败时响应已经写了一部分，只能记日志；日志里不放数据。
		s.logf("render %s: %v", name, err)
	}
}

// renderPartial renders one htmx fragment (no layout).
func (s *Server) renderPartial(w http.ResponseWriter, name string, data any) {
	if s.partials == nil {
		http.Error(w, "no partials", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.partials.ExecuteTemplate(w, name, data); err != nil {
		s.logf("render partial %s: %v", name, err)
	}
}

func (s *Server) fillPage(r *http.Request, data *pageData) {
	if data.CSRF == "" {
		if c, err := r.Cookie(csrfCookie); err == nil {
			data.CSRF = c.Value
		}
	}
	if data.Version == "" {
		data.Version = s.version
	}
	if data.Listen == "" {
		data.Listen = s.listen
	}
	if data.Instance == "" {
		if id, err := s.store.InstanceID(r.Context()); err == nil {
			data.Instance = id
		}
	}
	data.Now = s.now().Format("2006-01-02 15:04")
}

func (s *Server) handleStatic(w http.ResponseWriter, r *http.Request) {
	if s.static == nil {
		http.NotFound(w, r)
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/static/")
	if path == "" {
		http.NotFound(w, r)
		return
	}
	// 单二进制里的静态资源，给一个温和的缓存（开发时按版本号打散）
	w.Header().Set("Cache-Control", "public, max-age=300")
	http.ServeFileFS(w, r, s.static, path)
}

// logf keeps logging dependency-free; payloads and secrets never get here.
func (s *Server) logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "jinliang: "+format+"\n", args...)
}
