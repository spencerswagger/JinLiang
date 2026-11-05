package httpapi

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/netcheck"
)

// ── 连接 ────────────────────────────────────────────────────

// connView is the only shape a connection ever leaves the process in. There is
// no password field, by construction (docs/CONTROL-PLANE.md §4).
type connView struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Kind        string            `json:"kind"`
	Host        string            `json:"host"`
	Port        int               `json:"port"`
	Database    string            `json:"database"`
	Username    string            `json:"username"`
	Params      map[string]string `json:"params,omitempty"`
	HasSecret   bool              `json:"has_secret"`
	LastTestAt  string            `json:"last_test_at,omitempty"`
	LastTestOK  *bool             `json:"last_test_ok,omitempty"`
	LastTestMsg string            `json:"last_test_msg,omitempty"`
	CreatedAt   string            `json:"created_at"`
	UpdatedAt   string            `json:"updated_at"`
	UsedByJobs  int               `json:"used_by_jobs"`
	TLSWarning  string            `json:"tls_warning,omitempty"`
}

func toConnView(c control.Connection, usedBy int) connView {
	v := connView{
		ID: c.ID, Name: c.Name, Kind: c.Kind, Host: c.Host, Port: c.Port,
		Database: c.Database, Username: c.Username, Params: c.Params(),
		HasSecret: c.HasSecret(), LastTestAt: c.LastTestAt, LastTestOK: c.LastTestOK,
		LastTestMsg: c.LastTestMsg, CreatedAt: c.CreatedAt, UpdatedAt: c.UpdatedAt,
		UsedByJobs: usedBy,
	}
	ep := dsn.Endpoint{Kind: c.Kind, Host: c.Host, Port: c.Port, Params: c.Params()}
	if weak, why := ep.WeakTLS(); weak {
		v.TLSWarning = why
	}
	return v
}

func (s *Server) handleListConnections(w http.ResponseWriter, r *http.Request) {
	conns, err := s.store.ListConnections(r.Context())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	out := make([]connView, 0, len(conns))
	for _, c := range conns {
		used, _ := s.store.CountJobsUsingConnection(r.Context(), c.ID)
		out = append(out, toConnView(c, used))
	}
	writeJSON(w, http.StatusOK, map[string]any{"connections": out, "count": len(out)})
}

func (s *Server) handleGetConnection(w http.ResponseWriter, r *http.Request) {
	c, err := s.store.GetConnection(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	used, _ := s.store.CountJobsUsingConnection(r.Context(), c.ID)
	writeJSON(w, http.StatusOK, toConnView(c, used))
}

func (s *Server) handleCreateConnection(w http.ResponseWriter, r *http.Request) {
	in, ok := s.connectionInput(w, r)
	if !ok {
		return
	}
	c, err := s.store.CreateConnection(r.Context(), in)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	_ = s.store.Audit(r.Context(), actorOf(r), control.ActionConnCreate, map[string]any{
		"connection_id": c.ID, "kind": c.Kind, "name": c.Name,
	})
	// CONTROL-PLANE §4：保存前自动跑一次连通性探测。B0 里这一步不阻断保存
	// （网络抖动不该让用户丢掉刚填的表单），但结果会跟着响应回去。
	test, terr := s.store.TestConnection(r.Context(), c.ID)
	resp := map[string]any{"connection": toConnView(c, 0)}
	if terr == nil {
		resp["test"] = test
	}
	writeJSON(w, http.StatusCreated, resp)
}

func (s *Server) handleUpdateConnection(w http.ResponseWriter, r *http.Request) {
	in, ok := s.connectionInput(w, r)
	if !ok {
		return
	}
	c, err := s.store.UpdateConnection(r.Context(), r.PathValue("id"), in)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	_ = s.store.Audit(r.Context(), actorOf(r), control.ActionConnUpdate, map[string]any{"connection_id": c.ID})
	used, _ := s.store.CountJobsUsingConnection(r.Context(), c.ID)
	writeJSON(w, http.StatusOK, toConnView(c, used))
}

func (s *Server) handleDeleteConnection(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	c, err := s.store.GetConnection(r.Context(), id)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	if err := s.store.DeleteConnection(r.Context(), id); err != nil {
		if errors.Is(err, control.ErrInUse) {
			s.writeError(w, r, http.StatusConflict, apiError{
				Class:   "user_action",
				Message: "这个连接还在被任务使用，删不掉",
				Why:     "任务里存的是连接引用；直接删会留下指向空气的任务",
				Next:    []string{"先删掉或改掉引用它的任务（GET /api/jobs 能看到）"},
				Risk:    "无：任务运行历史不会因为删连接而丢失",
			})
			return
		}
		s.fail(w, r, err)
		return
	}
	_ = s.store.Audit(r.Context(), actorOf(r), control.ActionConnDelete, map[string]any{"connection_id": id, "name": c.Name})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "deleted": id})
}

func (s *Server) handleTestConnection(w http.ResponseWriter, r *http.Request) {
	res, err := s.store.TestConnection(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	status := http.StatusOK
	if !res.OK {
		// 测连失败不是服务端错误：对端可能只是没开。用 200 + ok:false 让
		// 客户端一次拿到完整的四段式提示。
		status = http.StatusOK
	}
	writeJSON(w, status, res)
}

func (s *Server) handleCatalog(w http.ResponseWriter, r *http.Request) {
	connID := r.URL.Query().Get("connection_id")
	if connID == "" {
		s.writeError(w, r, http.StatusBadRequest, plainError("缺少 connection_id"))
		return
	}
	res, err := s.store.Catalog(r.Context(), connID, r.URL.Query().Get("schema"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// connectionInput decodes a connection payload and turns an optional `dsn` into
// the individual fields, so `curl -d '{"dsn":"postgres://..."}'` just works.
func (s *Server) connectionInput(w http.ResponseWriter, r *http.Request) (control.ConnectionInput, bool) {
	var in control.ConnectionInput
	if err := decodeJSON(r, &in); err != nil {
		s.writeError(w, r, http.StatusBadRequest, plainError("请求体不是合法 JSON：%v", err))
		return in, false
	}
	if strings.TrimSpace(in.DSN) != "" {
		ep, err := dsn.Parse(in.DSN)
		if err != nil {
			s.writeError(w, r, http.StatusBadRequest, plainError("%v", err))
			return in, false
		}
		if in.Kind == "" {
			in.Kind = ep.Kind
		}
		if in.Host == "" {
			in.Host = ep.Host
		}
		if in.Port == 0 {
			in.Port = ep.Port
		}
		if in.Database == "" {
			in.Database = ep.Database
		}
		if in.Username == "" {
			in.Username = ep.Username
		}
		if in.Password == "" {
			in.Password = ep.Password
		}
		if in.Params == nil {
			in.Params = ep.Params
		}
		if in.Name == "" {
			in.Name = ep.Host + "/" + ep.Database
		}
	}
	if in.Port == 0 && in.Kind != "" {
		in.Port = dsn.DefaultPort(in.Kind)
	}
	return in, true
}

// ── 任务 ────────────────────────────────────────────────────

func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.store.ListJobs(r.Context())
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": jobs, "count": len(jobs)})
}

func (s *Server) handleCreateJob(w http.ResponseWriter, r *http.Request) {
	var in control.JobInput
	if err := decodeJSON(r, &in); err != nil {
		s.writeError(w, r, http.StatusBadRequest, plainError("请求体不是合法 JSON：%v", err))
		return
	}
	job, err := s.store.CreateJob(r.Context(), in)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	_ = s.store.Audit(r.Context(), actorOf(r), control.ActionJobCreate, map[string]any{
		"job_id": job.ID, "job_key": job.JobKey, "name": job.Name,
	})
	sum, _ := s.store.GetJobSummary(r.Context(), job.ID)
	writeJSON(w, http.StatusCreated, sum)
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	sum, err := s.store.GetJobSummary(ctx, r.PathValue("id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	doc, err := s.store.JobDoc(ctx, sum.Job)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	runs, _ := s.store.ListRuns(ctx, sum.ID, 20)
	writeJSON(w, http.StatusOK, map[string]any{"job": sum, "spec": doc, "runs": runs})
}

func (s *Server) handleUpdateJob(w http.ResponseWriter, r *http.Request) {
	var in control.JobInput
	if err := decodeJSON(r, &in); err != nil {
		s.writeError(w, r, http.StatusBadRequest, plainError("请求体不是合法 JSON：%v", err))
		return
	}
	job, err := s.store.UpdateJob(r.Context(), r.PathValue("id"), in)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	_ = s.store.Audit(r.Context(), actorOf(r), control.ActionJobUpdate, map[string]any{"job_id": job.ID})
	sum, _ := s.store.GetJobSummary(r.Context(), job.ID)
	writeJSON(w, http.StatusOK, sum)
}

func (s *Server) handleDeleteJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := s.store.DeleteJob(r.Context(), id); err != nil {
		s.fail(w, r, err)
		return
	}
	_ = s.store.Audit(r.Context(), actorOf(r), control.ActionJobDelete, map[string]any{"job_id": id})
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "deleted": id})
}

func (s *Server) handleExportJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	job, err := s.store.GetJob(ctx, r.PathValue("id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	data, err := s.store.ExportYAML(ctx, job)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	_ = s.store.Audit(ctx, actorOf(r), control.ActionJobExport, map[string]any{"job_id": job.ID})
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="`+safeFilename(job.Name)+`.yaml"`)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data)
}

func (s *Server) handleExportAll(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	jobs, err := s.store.ListJobs(ctx)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	var b strings.Builder
	b.WriteString("# 津梁全部任务导出（每份之间用 --- 分隔，可被 import 逐份读回）\n")
	for _, j := range jobs {
		data, err := s.store.ExportYAML(ctx, j.Job)
		if err != nil {
			s.fail(w, r, err)
			return
		}
		b.WriteString("---\n")
		b.Write(data)
	}
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="jinliang-export.yaml"`)
	_, _ = w.Write([]byte(b.String()))
}

func (s *Server) handlePreflight(w http.ResponseWriter, r *http.Request) {
	rep, err := s.store.PreflightJob(r.Context(), r.PathValue("id"))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

func (s *Server) handleRun(w http.ResponseWriter, r *http.Request) {
	run, err := s.store.Runner().Start(r.Context(), r.PathValue("id"), actorOf(r))
	if err != nil {
		if errors.Is(err, control.ErrRunInProgress) {
			s.writeError(w, r, http.StatusConflict, apiError{
				Class:   "user_action",
				Message: "这个任务正在跑",
				Why:     "同一个任务同时跑两份会互相覆盖（多实例靠目标端租约，本机靠这一层）",
				Next:    []string{"等它跑完（GET /api/jobs/{id} 看 runs）", "或先 POST /pause 停下来"},
				Risk:    "无",
			})
			return
		}
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"run":     run,
		"stub":    true,
		"message": "B0 骨架运行已开始：只推进阶段，不搬运数据。事件流：GET /api/jobs/{id}/events",
	})
}

func (s *Server) handlePause(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Runner().Pause(r.Context(), r.PathValue("id")); err != nil {
		s.runControlError(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "paused": true})
}

func (s *Server) handleResume(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Runner().Resume(r.Context(), r.PathValue("id")); err != nil {
		s.runControlError(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "paused": false})
}

func (s *Server) handleStop(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Runner().Stop(r.Context(), r.PathValue("id")); err != nil {
		s.runControlError(w, r, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "stopped": true})
}

func (s *Server) runControlError(w http.ResponseWriter, r *http.Request, err error) {
	if errors.Is(err, control.ErrNoActiveRun) {
		s.writeError(w, r, http.StatusConflict, apiError{
			Class:   "user_action",
			Message: "这个任务没有在跑",
			Why:     "暂停/恢复/停止只能作用在正在运行的实例上",
			Next:    []string{"先 POST /api/jobs/{id}/run 开始一次运行"},
			Risk:    "无",
		})
		return
	}
	s.fail(w, r, plainError("%v", err))
}

func (s *Server) handleImport(w http.ResponseWriter, r *http.Request) {
	raw, err := s.readYAMLBody(r)
	if err != nil {
		s.writeError(w, r, http.StatusBadRequest, plainError("%v", err))
		return
	}
	report, err := s.store.ImportYAMLDocs(r.Context(), raw, control.ImportOptions{Actor: actorOf(r)})
	// 有跳过时仍然把成功的部分报出来，但状态码不是 201：调用方必须知道「不是全都进了」。
	if err != nil && report.Total == 0 {
		s.fail(w, r, err)
		return
	}
	ids := make([]string, 0, len(report.Imported))
	for _, job := range report.Imported {
		ids = append(ids, job.ID)
	}
	_ = s.store.Audit(r.Context(), actorOf(r), control.ActionJobImport, map[string]any{
		"job_ids": ids, "imported": report.ImportedCount(), "skipped": len(report.Skipped),
	})
	status := http.StatusCreated
	if len(report.Skipped) > 0 {
		status = http.StatusMultiStatus // 207：部分成功
	}
	writeJSON(w, status, report)
}

// readYAMLBody accepts either a raw yaml body or {"yaml": "..."} JSON.
func (s *Server) readYAMLBody(r *http.Request) ([]byte, error) {
	ct := r.Header.Get("Content-Type")
	if strings.Contains(ct, "json") {
		var body struct {
			YAML string `json:"yaml"`
		}
		if err := decodeJSON(r, &body); err != nil {
			return nil, plainError("请求体不是合法 JSON：%v", err)
		}
		if strings.TrimSpace(body.YAML) == "" {
			return nil, plainError("yaml 字段是空的")
		}
		return []byte(body.YAML), nil
	}
	buf := make([]byte, 0, 8192)
	tmp := make([]byte, 4096)
	for {
		n, err := r.Body.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if err != nil {
			break
		}
		if len(buf) > 1<<20 {
			return nil, plainError("yaml 太大了（>1 MiB）")
		}
	}
	if len(buf) == 0 {
		return nil, plainError("请求体是空的：把 job.yaml 的内容放在 body 里")
	}
	return buf, nil
}

// ── 运行历史 / 审计 / 设置 / 体检 ───────────────────────────

func (s *Server) handleListRuns(w http.ResponseWriter, r *http.Request) {
	limit := intParam(r, "limit", 100)
	jobID := r.URL.Query().Get("job_id")
	runs, err := s.store.ListRuns(r.Context(), jobID, limit)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": runs, "count": len(runs)})
}

func (s *Server) handleAudit(w http.ResponseWriter, r *http.Request) {
	entries, err := s.store.ListAudit(r.Context(), intParam(r, "limit", 100))
	if err != nil {
		s.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"audit": entries, "count": len(entries)})
}

func (s *Server) handleGetSettings(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	settings, err := s.store.Settings(ctx)
	if err != nil {
		s.fail(w, r, err)
		return
	}
	instance, _ := s.store.InstanceID(ctx)
	configured, _ := s.store.AdminConfigured(ctx)
	writeJSON(w, http.StatusOK, map[string]any{
		"settings":         settings,
		"instance_id":      instance,
		"schema_version":   s.store.SchemaVersionText(ctx),
		"admin_configured": configured,
		"data_dir":         s.store.Dir(),
		"db_path":          s.store.Path(),
		"key_source":       string(s.store.Cipher().KeySource()),
		"key_path":         s.store.Cipher().KeyPath(),
		"listen":           s.listen,
	})
}

func (s *Server) handlePutSettings(w http.ResponseWriter, r *http.Request) {
	var body map[string]string
	if err := decodeJSON(r, &body); err != nil {
		s.writeError(w, r, http.StatusBadRequest, plainError("请求体不是合法 JSON：%v", err))
		return
	}
	allowed := map[string]bool{control.SettingDefaultBatch: true}
	for k, v := range body {
		if !allowed[k] {
			s.writeError(w, r, http.StatusBadRequest, plainError("不能改这个设置项：%s（可改：default_batch_rows）", k))
			return
		}
		if err := s.store.SetSetting(r.Context(), k, v); err != nil {
			s.fail(w, r, err)
			return
		}
	}
	_ = s.store.Audit(r.Context(), actorOf(r), "settings.update", body)
	s.handleGetSettings(w, r)
}

func (s *Server) handleDoctor(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	rep := s.doctor(ctx)
	writeJSON(w, http.StatusOK, rep)
}

// doctor collects the checks `jinliang doctor` and /diagnostics print.
func (s *Server) doctor(ctx context.Context) map[string]any {
	integrity, ierr := s.store.IntegrityCheck(ctx)
	stats, _ := s.store.DBStats(ctx)
	active, _ := s.store.ActiveRuns(ctx)
	instance, _ := s.store.InstanceID(ctx)
	configured, _ := s.store.AdminConfigured(ctx)

	out := map[string]any{
		"version":          s.version,
		"instance_id":      instance,
		"data_dir":         s.store.Dir(),
		"db_path":          s.store.Path(),
		"db_bytes":         stats["db_bytes"],
		"counts":           stats,
		"schema_version":   s.store.SchemaVersionText(ctx),
		"key_source":       string(s.store.Cipher().KeySource()),
		"key_path":         s.store.Cipher().KeyPath(),
		"listen":           s.listen,
		"admin_configured": configured,
		"active_runs":      len(active),
		"integrity":        integrity,
		"problems":         []string{},
	}
	var problems []string
	if ierr != nil || integrity != "ok" {
		problems = append(problems, "控制库完整性检查没通过："+integrity)
	}
	if s.listen != "" && !isLoopback(s.listen) {
		problems = append(problems, "监听地址不是本机（"+s.listen+"）：同网络的人可以访问控制台，请确认这是你要的")
	}
	if !configured {
		problems = append(problems, "还没有设置管理员口令：先打开首页走一遍引导")
	}
	out["problems"] = problems
	out["healthy"] = len(problems) == 0
	return out
}

// ── 小工具 ──────────────────────────────────────────────────

// fail maps control-layer errors onto HTTP status + four-part cards.
func (s *Server) fail(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, control.ErrNotFound):
		s.writeError(w, r, http.StatusNotFound, apiError{
			Class: "user_action", Message: "找不到这条记录",
			Why:  "可能刚被删掉，或者 id 打错了",
			Next: []string{"GET /api/jobs 或 /api/connections 看现有 id"},
			Risk: "无",
		})
	case errors.Is(err, control.ErrInUse):
		s.writeError(w, r, http.StatusConflict, apiError{
			Class: "user_action", Message: "还被别的东西引用着",
			Why:  err.Error(),
			Next: []string{"先处理引用它的任务"},
			Risk: "无",
		})
	case errors.Is(err, control.ErrConflict):
		s.writeError(w, r, http.StatusConflict, apiError{
			Class: "user_action", Message: "冲突：这可能是同一份同步",
			Why:  err.Error(),
			Next: []string{"改表范围或换连接，让指纹不同", "或者直接编辑已有任务"},
			Risk: "两个任务跑同一份数据会互相覆盖",
		})
	case errors.Is(err, control.ErrIdentityLocked):
		s.writeError(w, r, http.StatusConflict, apiError{
			Class:   "user_action",
			Message: "这个连接的坐标不能改：它已经是任务身份的一部分",
			Why: "任务的指纹＝源＋目标＋表范围（docs/IDENTITY.md §2），改坐标会让已有任务和指纹脱节，" +
				"同一份同步就变成两个任务、两条写入流打同一张目标表",
			Next: []string{
				"要换地址/库：新建一个连接，再把任务切过去（任务页可以改源与目标）",
				"只是换名字、换口令、加参数：这些不属于身份，随便改",
			},
			Risk: "无：这条只是拒绝改坐标，不会动现有连接与任务",
		})
	case errors.Is(err, control.ErrInvalid):
		s.writeError(w, r, http.StatusBadRequest, plainError("%s", strings.TrimPrefix(err.Error(), control.ErrInvalid.Error()+": ")))
	case errors.As(err, new(apiError)):
		var ae apiError
		errors.As(err, &ae)
		s.writeError(w, r, http.StatusBadRequest, ae)
	default:
		s.writeError(w, r, http.StatusInternalServerError, plainError("%v", err))
	}
}

func actorOf(r *http.Request) string {
	if r == nil {
		return control.ActorAPI
	}
	if strings.HasPrefix(r.URL.Path, "/api/") {
		return control.ActorAPI
	}
	return control.ActorUI
}

func intParam(r *http.Request, name string, def int) int {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

func safeFilename(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		case r > 127:
			b.WriteRune(r) // 中文名保留
		default:
			b.WriteByte('_')
		}
	}
	out := b.String()
	if out == "" {
		out = "job"
	}
	if len(out) > 64 {
		out = out[:64]
	}
	return out
}

func isLoopback(listen string) bool { return netcheck.IsLoopbackListen(listen) }
