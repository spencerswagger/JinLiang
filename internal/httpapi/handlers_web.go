package httpapi

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/core"
	"github.com/spencerswagger/jinliang/internal/source"
)

// ── 总览 ────────────────────────────────────────────────────

type overviewData struct {
	Jobs        []control.JobSummary
	Connections []connView
	ActiveRuns  []control.Run
	RunCount    int
	FailedRuns  int
	StubMode    bool
	Healthy     bool
	Problems    []string
}

func (s *Server) pageOverview(w http.ResponseWriter, r *http.Request) {
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
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	jobs, err := s.store.ListJobs(ctx)
	if err != nil {
		s.renderPage(w, r, "error_page.html", pageData{Title: "总览", Err: ptrErr(plainError("%v", err))})
		return
	}
	conns, _ := s.store.ListConnections(ctx)
	active, _ := s.store.ActiveRuns(ctx)
	runs, _ := s.store.ListRuns(ctx, "", 200)
	failed := 0
	for _, run := range runs {
		if run.Phase == "failed" {
			failed++
		}
	}
	doc := s.doctor(ctx)
	problems, _ := doc["problems"].([]string)
	healthy, _ := doc["healthy"].(bool)
	views := make([]connView, 0, len(conns))
	for _, c := range conns {
		used, _ := s.store.CountJobsUsingConnection(ctx, c.ID)
		views = append(views, toConnView(c, used))
	}
	s.renderPage(w, r, "overview.html", pageData{
		Title: "总览", Nav: "overview",
		Data: overviewData{
			Jobs: jobs, Connections: views, ActiveRuns: active,
			RunCount: len(runs), FailedRuns: failed, StubMode: true,
			Healthy: healthy, Problems: problems,
		},
	})
}

// ── 登录 / 首启 ─────────────────────────────────────────────

func (s *Server) pageLogin(w http.ResponseWriter, r *http.Request) {
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
	if s.authenticated(r) {
		http.Redirect(w, r, nextOr(r, "/"), http.StatusSeeOther)
		return
	}
	csrf := s.ensureCSRFCookie(w, r)
	s.renderPage(w, r, "login.html", pageData{Title: "登录", Nav: "login", CSRF: csrf})
}

func (s *Server) pageLoginSubmit(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	if !s.limiter.allow(remoteIP(r)) {
		s.renderPage(w, r, "login.html", pageData{Title: "登录", Nav: "login",
			Err: ptrErr(plainError("尝试次数太多，等一分钟再试"))})
		return
	}
	_ = r.ParseForm()
	ok, err := s.store.VerifyAdminPassword(ctx, r.FormValue("password"))
	if err != nil || !ok {
		s.renderPage(w, r, "login.html", pageData{Title: "登录", Nav: "login",
			Err: ptrErr(plainError("口令不对（连续失败会被限速）"))})
		return
	}
	if _, err := s.setSession(w, r); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, nextOr(r, "/"), http.StatusSeeOther)
}

func (s *Server) pageLogout(w http.ResponseWriter, r *http.Request) {
	s.clearSession(w, r)
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

func (s *Server) pageSetup(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	configured, err := s.store.AdminConfigured(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if configured {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	csrf := s.ensureCSRFCookie(w, r)
	s.renderPage(w, r, "setup.html", pageData{Title: "首次启动", Nav: "setup", CSRF: csrf})
}

func (s *Server) pageSetupSubmit(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	configured, _ := s.store.AdminConfigured(ctx)
	if configured {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	_ = r.ParseForm()
	if !s.csrfOK(r) {
		csrf := s.ensureCSRFCookie(w, r)
		s.renderPage(w, r, "setup.html", pageData{Title: "首次启动", Nav: "setup", CSRF: csrf,
			Err: ptrErr(apiError{
				Message: "页面过期了，请重试",
				Why:     "设置口令需要页面里的防伪 token",
				Next:    []string{"刷新这一页再提交"},
				Risk:    "无",
			})})
		return
	}
	pass := r.FormValue("password")
	again := r.FormValue("password2")
	if pass != again {
		s.renderPage(w, r, "setup.html", pageData{Title: "首次启动", Nav: "setup",
			Err: ptrErr(plainError("两次输入的口令不一致"))})
		return
	}
	if err := s.store.SetAdminPassword(ctx, pass); err != nil {
		s.renderPage(w, r, "setup.html", pageData{Title: "首次启动", Nav: "setup",
			Err: ptrErr(plainError("%v", err))})
		return
	}
	_ = s.store.Audit(ctx, control.ActorUI, control.ActionAdminSet, map[string]any{"via": "ui"})
	if _, err := s.setSession(w, r); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.Redirect(w, r, "/connections/new", http.StatusSeeOther)
}

// ── 连接页 ──────────────────────────────────────────────────

func (s *Server) pageConnections(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	conns, err := s.store.ListConnections(ctx)
	if err != nil {
		s.renderPage(w, r, "error_page.html", pageData{Title: "连接", Err: ptrErr(plainError("%v", err))})
		return
	}
	views := make([]connView, 0, len(conns))
	for _, c := range conns {
		used, _ := s.store.CountJobsUsingConnection(ctx, c.ID)
		views = append(views, toConnView(c, used))
	}
	s.renderPage(w, r, "connections.html", pageData{Title: "连接", Nav: "connections", Data: views})
}

func (s *Server) pageConnectionNew(w http.ResponseWriter, r *http.Request) {
	s.renderPage(w, r, "connection_new.html", pageData{Title: "新建连接", Nav: "connections"})
}

func (s *Server) pageConnectionCreate(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	in := control.ConnectionInput{
		Name:     r.FormValue("name"),
		Kind:     r.FormValue("kind"),
		Host:     r.FormValue("host"),
		Database: r.FormValue("database"),
		Username: r.FormValue("username"),
		Password: r.FormValue("password"),
		Params:   map[string]string{},
	}
	if port, err := strconv.Atoi(r.FormValue("port")); err == nil {
		in.Port = port
	}
	if ssl := strings.TrimSpace(r.FormValue("sslmode")); ssl != "" {
		in.Params["sslmode"] = ssl
	}
	c, err := s.store.CreateConnection(r.Context(), in)
	if err != nil {
		s.renderPage(w, r, "connection_new.html", pageData{Title: "新建连接", Nav: "connections",
			Err: ptrErr(plainError("%v", err)), Data: in})
		return
	}
	_ = s.store.Audit(r.Context(), control.ActorUI, control.ActionConnCreate, map[string]any{"connection_id": c.ID})
	// 建完立刻测一次：用户想知道"填对了没有"，而不是等以后
	if _, err := s.store.TestConnection(r.Context(), c.ID); err != nil {
		s.logf("test after create: %v", err)
	}
	http.Redirect(w, r, "/connections/"+c.ID, http.StatusSeeOther)
}

func (s *Server) pageConnectionDetail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	c, err := s.store.GetConnection(ctx, r.PathValue("id"))
	if err != nil {
		s.renderPage(w, r, "error_page.html", pageData{Title: "连接", Err: ptrErr(plainError("%v", err))})
		return
	}
	used, _ := s.store.CountJobsUsingConnection(ctx, c.ID)
	type detail struct {
		Conn connView
		Jobs []control.JobSummary
	}
	jobs, _ := s.store.ListJobs(ctx)
	var mine []control.JobSummary
	for _, j := range jobs {
		if j.SourceID == c.ID || j.TargetID == c.ID {
			mine = append(mine, j)
		}
	}
	s.renderPage(w, r, "connection_detail.html", pageData{
		Title: c.Name, Nav: "connections",
		Data: detail{Conn: toConnView(c, used), Jobs: mine},
	})
}

// pageConnectionTest renders the htmx fragment with the test result.
func (s *Server) pageConnectionTest(w http.ResponseWriter, r *http.Request) {
	res, err := s.store.TestConnection(r.Context(), r.PathValue("id"))
	if err != nil {
		s.renderPartial(w, "test_result_partial.html", map[string]any{
			"OK": false, "Message": err.Error(),
		})
		return
	}
	var spec *core.Spec
	if !res.OK {
		if sp, ok := core.Lookup(core.CodeConnUnreachable); ok {
			spec = &sp
		}
	}
	s.renderPartial(w, "test_result_partial.html", map[string]any{
		"OK": res.OK, "Version": res.ServerVersion, "Latency": res.LatencyMS,
		"Message": res.Message, "Step": res.Step, "TLS": res.TLS,
		"TLSWarning": res.TLSWarning, "Notes": res.Notes, "Spec": spec,
		"Caps": res.Capabilities,
	})
}

func (s *Server) pageConnectionDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.store.DeleteConnection(r.Context(), r.PathValue("id")); err != nil {
		s.renderPage(w, r, "error_page.html", pageData{Title: "删连接", Err: ptrErr(plainError("%v", err))})
		return
	}
	http.Redirect(w, r, "/connections", http.StatusSeeOther)
}

// ── 新建任务向导 ────────────────────────────────────────────

type jobNewData struct {
	Connections []connView
	Error       string
	Form        map[string]string
}

func (s *Server) pageJobNew(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	conns, _ := s.store.ListConnections(ctx)
	views := make([]connView, 0, len(conns))
	for _, c := range conns {
		used, _ := s.store.CountJobsUsingConnection(ctx, c.ID)
		views = append(views, toConnView(c, used))
	}
	s.renderPage(w, r, "job_new.html", pageData{Title: "新建任务", Nav: "jobs",
		Data: jobNewData{Connections: views}})
}

// pageWizardCatalog returns the table picker fragment for one connection.
func (s *Server) pageWizardCatalog(w http.ResponseWriter, r *http.Request) {
	connID := r.URL.Query().Get("connection_id")
	side := r.URL.Query().Get("side")
	if connID == "" {
		s.renderPartial(w, "catalog_tables_partial.html", map[string]any{"Error": "先选一个连接", "Side": side})
		return
	}
	res, err := s.store.Catalog(r.Context(), connID, r.URL.Query().Get("schema"))
	if err != nil {
		s.renderPartial(w, "catalog_tables_partial.html", map[string]any{"Error": err.Error(), "Side": side})
		return
	}
	s.renderPartial(w, "catalog_tables_partial.html", map[string]any{
		"Tables": res.Tables, "Views": res.Views, "Schemas": res.Schemas,
		"Schema": res.Schema, "ConnectionID": connID, "Side": side,
		"Elapsed": res.ElapsedMS, "Warning": res.Warning,
	})
}

func (s *Server) pageJobCreate(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		s.renderPage(w, r, "error_page.html", pageData{Title: "新建任务", Err: ptrErr(plainError("%v", err))})
		return
	}
	in := control.JobInput{
		Name:     r.FormValue("name"),
		SourceID: r.FormValue("source_id"),
		TargetID: r.FormValue("target_id"),
		Enabled:  r.FormValue("enabled") != "",
	}
	tables := r.Form["tables"]
	doc := control.JobDoc{}
	doc.Source.Scope = &control.ScopeDoc{Schema: r.FormValue("source_schema")}
	doc.Target.Schema = r.FormValue("target_schema")
	doc.Target.OnExists = r.FormValue("on_exists")
	doc.Target.CreateIndexes = r.FormValue("create_indexes")
	doc.Mode.Frequency = r.FormValue("frequency")
	doc.Mode.Interval = r.FormValue("interval")
	doc.Mode.IncrementalColumn = r.FormValue("incremental_column")
	doc.Mode.Lookback = r.FormValue("lookback")
	if len(tables) > 0 {
		doc.Content = &control.ContentDoc{Tables: map[string]control.TableDoc{}}
		doc.Source.Scope.Include = append([]string{}, tables...)
		for _, t := range tables {
			doc.Content.Tables[t] = control.TableDoc{}
		}
	}
	if n, err := strconv.Atoi(r.FormValue("batch_rows")); err == nil && n > 0 {
		doc.Reliability = &control.ReliabilityDoc{BatchRows: &n}
	}
	if n, err := strconv.Atoi(r.FormValue("chunk_rows")); err == nil && n > 0 {
		doc.Source.Snapshot = &control.SnapshotDoc{ChunkRows: &n}
	}
	if n, err := strconv.Atoi(r.FormValue("conns")); err == nil && n > 0 {
		if doc.Source.Snapshot == nil {
			doc.Source.Snapshot = &control.SnapshotDoc{}
		}
		doc.Source.Snapshot.Conns = &n
	}
	in.Spec = doc

	job, err := s.store.CreateJob(r.Context(), in)
	if err != nil {
		conns, _ := s.store.ListConnections(r.Context())
		views := make([]connView, 0, len(conns))
		for _, c := range conns {
			used, _ := s.store.CountJobsUsingConnection(r.Context(), c.ID)
			views = append(views, toConnView(c, used))
		}
		s.renderPage(w, r, "job_new.html", pageData{Title: "新建任务", Nav: "jobs",
			Err:  ptrErr(plainError("%v", err)),
			Data: jobNewData{Connections: views}})
		return
	}
	_ = s.store.Audit(r.Context(), control.ActorUI, control.ActionJobCreate, map[string]any{"job_id": job.ID})
	http.Redirect(w, r, "/jobs/"+job.ID, http.StatusSeeOther)
}

// ── 任务详情 ────────────────────────────────────────────────

type jobDetailData struct {
	Summary control.JobSummary
	Doc     control.JobDoc
	Runs    []control.Run
	Active  *control.Run
	Tables  []string
}

func (s *Server) pageJobDetail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sum, err := s.store.GetJobSummary(ctx, r.PathValue("id"))
	if err != nil {
		s.renderPage(w, r, "error_page.html", pageData{Title: "任务", Err: ptrErr(plainError("%v", err))})
		return
	}
	doc, err := s.store.JobDoc(ctx, sum.Job)
	if err != nil {
		s.renderPage(w, r, "error_page.html", pageData{Title: sum.Name, Err: ptrErr(plainError("%v", err))})
		return
	}
	runs, _ := s.store.ListRuns(ctx, sum.ID, 20)
	var active *control.Run
	if run, ok := s.store.Runner().Active(sum.ID); ok {
		active = &run
	}
	var tables []string
	if doc.Content != nil {
		for name := range doc.Content.Tables {
			tables = append(tables, name)
		}
	}
	s.renderPage(w, r, "job_detail.html", pageData{
		Title: sum.Name, Nav: "jobs",
		Data: jobDetailData{Summary: sum, Doc: doc, Runs: runs, Active: active, Tables: tables},
	})
}

func (s *Server) pageJobPreflight(w http.ResponseWriter, r *http.Request) {
	rep, err := s.store.PreflightJob(r.Context(), r.PathValue("id"))
	if err != nil {
		s.renderPartial(w, "error_card", plainError("%v", err))
		return
	}
	s.renderPartial(w, "preflight_partial.html", map[string]any{
		"Report": rep, "Codes": codesForChecks(rep),
	})
}

// codesForChecks resolves the four-part card for every check that carries one.
func codesForChecks(rep source.Report) map[string]core.Spec {
	out := map[string]core.Spec{}
	for _, c := range rep.Checks {
		if c.Code == "" {
			continue
		}
		if spec, ok := core.Lookup(core.Code(c.Code)); ok {
			out[c.Code] = spec
		}
	}
	return out
}

func (s *Server) pageJobRun(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := s.store.Runner().Start(r.Context(), id, control.ActorUI); err != nil {
		s.renderPartial(w, "error_card", plainError("%v", err))
		return
	}
	http.Redirect(w, r, "/jobs/"+id, http.StatusSeeOther)
}

func (s *Server) pageJobPause(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.store.Runner().Pause(r.Context(), id); err != nil {
		s.renderPartial(w, "error_card", plainError("%v", err))
		return
	}
	http.Redirect(w, r, "/jobs/"+id, http.StatusSeeOther)
}

func (s *Server) pageJobResume(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.store.Runner().Resume(r.Context(), id); err != nil {
		s.renderPartial(w, "error_card", plainError("%v", err))
		return
	}
	http.Redirect(w, r, "/jobs/"+id, http.StatusSeeOther)
}

func (s *Server) pageJobStop(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.store.Runner().Stop(r.Context(), id); err != nil {
		s.renderPartial(w, "error_card", plainError("%v", err))
		return
	}
	http.Redirect(w, r, "/jobs/"+id, http.StatusSeeOther)
}

func (s *Server) pageJobDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.store.DeleteJob(r.Context(), r.PathValue("id")); err != nil {
		s.renderPage(w, r, "error_page.html", pageData{Title: "删任务", Err: ptrErr(plainError("%v", err))})
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *Server) pageJobYaml(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	job, err := s.store.GetJob(ctx, r.PathValue("id"))
	if err != nil {
		s.renderPage(w, r, "error_page.html", pageData{Title: "导出", Err: ptrErr(plainError("%v", err))})
		return
	}
	data, err := s.store.ExportYAML(ctx, job)
	if err != nil {
		s.renderPage(w, r, "error_page.html", pageData{Title: "导出", Err: ptrErr(plainError("%v", err))})
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write(data)
}

// ── 历史 / 诊断 / 设置 ──────────────────────────────────────

type historyRow struct {
	Run  control.Run
	Job  string
	Kind string
}

func (s *Server) pageHistory(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	runs, err := s.store.ListRuns(ctx, "", 200)
	if err != nil {
		s.renderPage(w, r, "error_page.html", pageData{Title: "历史", Err: ptrErr(plainError("%v", err))})
		return
	}
	jobs, _ := s.store.ListJobs(ctx)
	names := map[string]control.JobSummary{}
	for _, j := range jobs {
		names[j.ID] = j
	}
	rows := make([]historyRow, 0, len(runs))
	for _, run := range runs {
		j := names[run.JobID]
		rows = append(rows, historyRow{Run: run, Job: j.Name, Kind: j.SourceKind})
	}
	s.renderPage(w, r, "history.html", pageData{Title: "历史", Nav: "history", Data: rows})
}

type diagnosticsData struct {
	Healthy         bool
	Problems        []string
	Integrity       string
	Counts          map[string]int64
	DBBytes         int64
	Version         string
	Instance        string
	DataDir         string
	DBPath          string
	KeySource       string
	KeyPath         string
	Listen          string
	SchemaVersion   string
	AdminConfigured bool
	ActiveRuns      int
	Audit           []control.AuditEntry
}

func (s *Server) pageDiagnostics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	doc := s.doctor(ctx)
	audit, _ := s.store.ListAudit(ctx, 50)
	str := func(k string) string { v, _ := doc[k].(string); return v }
	num := func(k string) int64 { v, _ := doc[k].(int64); return v }
	d := diagnosticsData{
		Healthy: str("integrity") == "ok", Integrity: str("integrity"),
		Version: str("version"), Instance: str("instance_id"),
		DataDir: str("data_dir"), DBPath: str("db_path"),
		KeySource: str("key_source"), KeyPath: str("key_path"),
		Listen: str("listen"), SchemaVersion: str("schema_version"),
		DBBytes: num("db_bytes"), Audit: audit,
	}
	d.Problems, _ = doc["problems"].([]string)
	d.Counts, _ = doc["counts"].(map[string]int64)
	d.AdminConfigured, _ = doc["admin_configured"].(bool)
	d.ActiveRuns, _ = doc["active_runs"].(int)
	s.renderPage(w, r, "diagnostics.html", pageData{Title: "诊断", Nav: "diagnostics", Data: d})
}

type settingsData struct {
	Settings   map[string]string
	Instance   string
	Schema     string
	DataDir    string
	DBPath     string
	KeySource  string
	KeyPath    string
	Listen     string
	Configured bool
}

func (s *Server) pageSettings(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	settings, err := s.store.Settings(ctx)
	if err != nil {
		s.renderPage(w, r, "error_page.html", pageData{Title: "设置", Err: ptrErr(plainError("%v", err))})
		return
	}
	instance, _ := s.store.InstanceID(ctx)
	configured, _ := s.store.AdminConfigured(ctx)
	s.renderPage(w, r, "settings.html", pageData{
		Title: "设置", Nav: "settings",
		Data: settingsData{
			Settings: settings, Instance: instance, Schema: s.store.SchemaVersionText(ctx),
			DataDir: s.store.Dir(), DBPath: s.store.Path(),
			KeySource: string(s.store.Cipher().KeySource()), KeyPath: s.store.Cipher().KeyPath(),
			Listen: s.listen, Configured: configured,
		},
	})
}

func (s *Server) pageSettingsAdmin(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	ctx := r.Context()
	cur := r.FormValue("current_password")
	next := r.FormValue("new_password")
	again := r.FormValue("new_password2")
	ok, err := s.store.VerifyAdminPassword(ctx, cur)
	if err != nil || !ok {
		s.renderPage(w, r, "error_page.html", pageData{Title: "设置", Nav: "settings",
			Err: ptrErr(plainError("当前口令不对"))})
		return
	}
	if next != again {
		s.renderPage(w, r, "error_page.html", pageData{Title: "设置", Nav: "settings",
			Err: ptrErr(plainError("两次输入的新口令不一致"))})
		return
	}
	if err := s.store.SetAdminPassword(ctx, next); err != nil {
		s.renderPage(w, r, "error_page.html", pageData{Title: "设置", Nav: "settings",
			Err: ptrErr(plainError("%v", err))})
		return
	}
	_ = s.store.Audit(ctx, control.ActorUI, control.ActionAdminSet, map[string]any{"via": "ui.change"})
	http.Redirect(w, r, "/settings", http.StatusSeeOther)
}

// ── 小工具 ──────────────────────────────────────────────────

func ptrErr(e apiError) *apiError { return &e }

func nextOr(r *http.Request, def string) string {
	n := r.URL.Query().Get("next")
	if n == "" || !strings.HasPrefix(n, "/") || strings.HasPrefix(n, "//") {
		return def
	}
	return n
}
