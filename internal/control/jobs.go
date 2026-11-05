package control

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/state"
)

// Job is one sync task definition. The semantic half lives in SpecJSON so that
// yaml and the control database cannot drift apart (docs/CONTROL-PLANE.md §3).
type Job struct {
	ID        string `json:"id"`
	JobKey    string `json:"job_key"`
	Name      string `json:"name"`
	SourceID  string `json:"source_id"`
	TargetID  string `json:"target_id"`
	SpecJSON  string `json:"-"`
	Enabled   bool   `json:"enabled"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

// JobInput is what the wizard / API submits. Spec is a full job.yaml body: the
// UI fills it from the catalog, and `import` fills it from a file.
type JobInput struct {
	Name     string `json:"name"`
	SourceID string `json:"source_id"`
	TargetID string `json:"target_id"`
	Enabled  bool   `json:"enabled"`
	Spec     JobDoc `json:"spec"`
}

// JobSummary is a job plus the display names of its endpoints, which is what
// every list page actually needs (jobs source_id alone is useless to a human).
type JobSummary struct {
	Job
	SourceName string `json:"source_name"`
	SourceKind string `json:"source_kind"`
	TargetName string `json:"target_name"`
	TargetKind string `json:"target_kind"`
	LastRun    *Run   `json:"last_run,omitempty"`
}

// ConnPlaceholderPrefix is how a job.yaml export refers to a stored connection
// without carrying its password: env:JINLIANG_CONN_<连接id>.
//
// 这样导出的 yaml 天然满足 schema 的 `^(env:|file:)` 约束，又能被 import
// 原地认回来复用密文——不需要用户在导出文件里看到任何凭证。
const ConnPlaceholderPrefix = "env:JINLIANG_CONN_"

// ConnectionPlaceholder renders the export placeholder for a connection id.
func ConnectionPlaceholder(id string) string {
	return ConnPlaceholderPrefix + strings.ToUpper(strings.ReplaceAll(id, "-", "_"))
}

// ConnectionIDFromPlaceholder reverses ConnectionPlaceholder.
func ConnectionIDFromPlaceholder(dsn string) (string, bool) {
	dsn = strings.TrimSpace(dsn)
	if !strings.HasPrefix(dsn, ConnPlaceholderPrefix) {
		return "", false
	}
	rest := strings.TrimPrefix(dsn, ConnPlaceholderPrefix)
	rest = strings.ToLower(strings.ReplaceAll(rest, "_", "-"))
	if rest == "" {
		return "", false
	}
	return rest, true
}

// identityDSN is the password-free coordinates used to derive job_key. Format
// is stable on purpose: changing it changes every job's identity.
func identityDSN(c Connection) string {
	return fmt.Sprintf("%s://%s@%s:%d/%s", c.Kind, c.Username, c.Host, c.Port, c.Database)
}

// buildDoc materialises a stored job's spec into a complete, valid JobDoc
// (connection kind/dsn filled from the connections table).
func (s *Store) buildDoc(ctx context.Context, j Job) (JobDoc, error) {
	doc, err := unmarshalDoc(j.SpecJSON)
	if err != nil {
		return JobDoc{}, err
	}
	doc.Job = j.Name
	doc.Version = 1
	src, err := s.GetConnection(ctx, j.SourceID)
	if err != nil {
		return JobDoc{}, fmt.Errorf("任务的源连接 %s 不在了: %w", j.SourceID, err)
	}
	tgt, err := s.GetConnection(ctx, j.TargetID)
	if err != nil {
		return JobDoc{}, fmt.Errorf("任务的目标连接 %s 不在了: %w", j.TargetID, err)
	}
	doc.Source.Kind = src.Kind
	doc.Target.Kind = tgt.Kind
	doc.Source.DSN = ConnectionPlaceholder(src.ID)
	doc.Target.DSN = ConnectionPlaceholder(tgt.ID)
	return doc, nil
}

func unmarshalDoc(specJSON string) (JobDoc, error) {
	var doc JobDoc
	if err := json.Unmarshal([]byte(specJSON), &doc); err != nil {
		return JobDoc{}, fmt.Errorf("spec_json 解析失败（控制库可能被手工改过）: %w", err)
	}
	return doc, nil
}

// CreateJob validates a job, derives its job_key and stores it.
func (s *Store) CreateJob(ctx context.Context, in JobInput) (Job, error) {
	in.Name = strings.TrimSpace(in.Name)
	if in.Name == "" {
		return Job{}, invalid("任务名不能为空（只是展示名，可以重复）")
	}
	src, err := s.GetConnection(ctx, in.SourceID)
	if err != nil {
		return Job{}, fmt.Errorf("源连接不存在: %w", err)
	}
	tgt, err := s.GetConnection(ctx, in.TargetID)
	if err != nil {
		return Job{}, fmt.Errorf("目标连接不存在: %w", err)
	}
	doc := in.Spec
	doc.Version = 1
	doc.Job = in.Name
	doc.Source.Kind = src.Kind
	doc.Target.Kind = tgt.Kind
	doc.Source.DSN = ConnectionPlaceholder(src.ID)
	doc.Target.DSN = ConnectionPlaceholder(tgt.ID)
	doc.Instance = "" // instance 属于本机，不进 spec_json
	doc.ApplyDefaults()
	if err := doc.Validate(); err != nil {
		return Job{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	key := state.JobKey(identityDSN(src), identityDSN(tgt), doc.ScopeFingerprint())

	specJSON, err := json.Marshal(doc)
	if err != nil {
		return Job{}, err
	}
	id, err := NewID()
	if err != nil {
		return Job{}, err
	}
	now := s.nowText()
	enabled := 0
	if in.Enabled {
		enabled = 1
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO jobs (id, job_key, name, source_id, target_id, spec_json, enabled, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, key, in.Name, src.ID, tgt.ID, string(specJSON), enabled, now, now)
	if err != nil {
		if isUniqueViolation(err) {
			existing, _ := s.FindJobByKey(ctx, key)
			return Job{}, fmt.Errorf("%w: 这其实是同一个任务（指纹 %s%s）。%s",
				ErrConflict, key,
				nameSuffix(existing.Name),
				"同一个「源 + 目标 + 表范围」只能有一个任务，改一下表范围或换连接")
		}
		return Job{}, fmt.Errorf("保存任务失败: %w", err)
	}
	return s.GetJob(ctx, id)
}

func nameSuffix(name string) string {
	if name == "" {
		return ""
	}
	return "，已有任务叫「" + name + "」"
}

// UpdateJob replaces a job definition (keeping the row id and created_at).
func (s *Store) UpdateJob(ctx context.Context, id string, in JobInput) (Job, error) {
	cur, err := s.GetJob(ctx, id)
	if err != nil {
		return Job{}, err
	}
	if strings.TrimSpace(in.Name) == "" {
		in.Name = cur.Name
	}
	if in.SourceID == "" {
		in.SourceID = cur.SourceID
	}
	if in.TargetID == "" {
		in.TargetID = cur.TargetID
	}
	src, err := s.GetConnection(ctx, in.SourceID)
	if err != nil {
		return Job{}, fmt.Errorf("源连接不存在: %w", err)
	}
	tgt, err := s.GetConnection(ctx, in.TargetID)
	if err != nil {
		return Job{}, fmt.Errorf("目标连接不存在: %w", err)
	}
	doc := in.Spec
	doc.Version = 1
	doc.Job = strings.TrimSpace(in.Name)
	doc.Source.Kind = src.Kind
	doc.Target.Kind = tgt.Kind
	doc.Source.DSN = ConnectionPlaceholder(src.ID)
	doc.Target.DSN = ConnectionPlaceholder(tgt.ID)
	doc.Instance = ""
	doc.ApplyDefaults()
	if err := doc.Validate(); err != nil {
		return Job{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	key := state.JobKey(identityDSN(src), identityDSN(tgt), doc.ScopeFingerprint())
	specJSON, err := json.Marshal(doc)
	if err != nil {
		return Job{}, err
	}
	enabled := 0
	if in.Enabled {
		enabled = 1
	}
	if _, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET job_key=?, name=?, source_id=?, target_id=?, spec_json=?, enabled=?, updated_at=?
		 WHERE id=?`,
		key, doc.Job, src.ID, tgt.ID, string(specJSON), enabled, s.nowText(), id); err != nil {
		if isUniqueViolation(err) {
			return Job{}, fmt.Errorf("%w: 改完之后和另一个任务的指纹（%s）撞了，说明它们其实是同一份同步", ErrConflict, key)
		}
		return Job{}, err
	}
	return s.GetJob(ctx, id)
}

const jobCols = `id, job_key, name, source_id, target_id, spec_json, enabled, created_at, updated_at`

func scanJob(sc interface{ Scan(...any) error }) (Job, error) {
	var j Job
	var enabled int
	if err := sc.Scan(&j.ID, &j.JobKey, &j.Name, &j.SourceID, &j.TargetID, &j.SpecJSON,
		&enabled, &j.CreatedAt, &j.UpdatedAt); err != nil {
		return Job{}, err
	}
	j.Enabled = enabled != 0
	return j, nil
}

// GetJob loads one job by id.
func (s *Store) GetJob(ctx context.Context, id string) (Job, error) {
	j, err := scanJob(s.db.QueryRowContext(ctx, `SELECT `+jobCols+` FROM jobs WHERE id = ?`, id))
	if isNoRows(err) {
		return Job{}, ErrNotFound
	}
	return j, err
}

// FindJobByKey loads the job with this fingerprint, if any.
func (s *Store) FindJobByKey(ctx context.Context, key string) (Job, error) {
	j, err := scanJob(s.db.QueryRowContext(ctx, `SELECT `+jobCols+` FROM jobs WHERE job_key = ?`, key))
	if isNoRows(err) {
		return Job{}, ErrNotFound
	}
	return j, err
}

// ResolveJob accepts an id or a display name (the CLI takes either).
func (s *Store) ResolveJob(ctx context.Context, ref string) (Job, error) {
	if j, err := s.GetJob(ctx, ref); err == nil {
		return j, nil
	} else if !errorsIsNotFound(err) {
		return Job{}, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+jobCols+` FROM jobs WHERE name = ?`, ref)
	if err != nil {
		return Job{}, err
	}
	defer rows.Close()
	var found []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return Job{}, err
		}
		found = append(found, j)
	}
	switch len(found) {
	case 0:
		return Job{}, ErrNotFound
	case 1:
		return found[0], nil
	default:
		return Job{}, fmt.Errorf("%w: 有 %d 个任务都叫「%s」，请改用 id", ErrConflict, len(found), ref)
	}
}

// ListJobs returns jobs with endpoint names and their latest run.
func (s *Store) ListJobs(ctx context.Context) ([]JobSummary, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+jobCols+` FROM jobs`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []JobSummary
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, JobSummary{Job: j})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	names := map[string]Connection{}
	conns, err := s.ListConnections(ctx)
	if err != nil {
		return nil, err
	}
	for _, c := range conns {
		names[c.ID] = c
	}
	for i := range out {
		if c, ok := names[out[i].SourceID]; ok {
			out[i].SourceName, out[i].SourceKind = c.Name, c.Kind
		}
		if c, ok := names[out[i].TargetID]; ok {
			out[i].TargetName, out[i].TargetKind = c.Name, c.Kind
		}
		if run, err := s.LatestRun(ctx, out[i].ID); err == nil {
			out[i].LastRun = &run
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Name < out[b].Name })
	return out, nil
}

// GetJobSummary is ListJobs for one job.
func (s *Store) GetJobSummary(ctx context.Context, id string) (JobSummary, error) {
	j, err := s.GetJob(ctx, id)
	if err != nil {
		return JobSummary{}, err
	}
	sum := JobSummary{Job: j}
	if c, err := s.GetConnection(ctx, j.SourceID); err == nil {
		sum.SourceName, sum.SourceKind = c.Name, c.Kind
	}
	if c, err := s.GetConnection(ctx, j.TargetID); err == nil {
		sum.TargetName, sum.TargetKind = c.Name, c.Kind
	}
	if run, err := s.LatestRun(ctx, id); err == nil {
		sum.LastRun = &run
	}
	return sum, nil
}

// JobDoc returns the complete, validated document for a stored job.
func (s *Store) JobDoc(ctx context.Context, j Job) (JobDoc, error) { return s.buildDoc(ctx, j) }

// ExportYAML renders the job as importable job.yaml (no secrets inside).
func (s *Store) ExportYAML(ctx context.Context, j Job) ([]byte, error) {
	doc, err := s.buildDoc(ctx, j)
	if err != nil {
		return nil, err
	}
	return MarshalJobYAML(doc)
}

// DeleteJob removes a job together with its run history.
//
// CONTROL-PLANE §4 只列了 GET/POST/PUT，没有 DELETE；但「任务 CRUD」是 B0 的
// 明确要求，所以这里用 DELETE 同一路径，并在 PR 里把这条文档缺口报给 hugo。
func (s *Store) DeleteJob(ctx context.Context, id string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if _, err := tx.ExecContext(ctx, `DELETE FROM runs WHERE job_id = ?`, id); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `DELETE FROM jobs WHERE id = ?`, id)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return ErrNotFound
	}
	return tx.Commit()
}

// ImportOptions controls yaml import.
type ImportOptions struct {
	// EnvLookup resolves env:NAME placeholders (os.LookupEnv in production,
	// a map in tests). Nil means "use the process environment".
	EnvLookup func(string) (string, bool)
	// Actor is recorded in the audit trail.
	Actor string
	// DryRun validates and reports without writing.
	DryRun bool
}

// ImportYAML turns a job.yaml into connections + a job. Existing connections are
// reused (by placeholder id, then by coordinates) so repeated imports are
// idempotent — GitOps needs that.
func (s *Store) ImportYAML(ctx context.Context, data []byte, opts ImportOptions) (Job, error) {
	doc, err := ParseJobYAML(data)
	if err != nil {
		return Job{}, err
	}
	if err := doc.Validate(); err != nil {
		return Job{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	lookup := opts.EnvLookup
	if lookup == nil {
		lookup = os.LookupEnv
	}
	srcID, err := s.resolveImportEndpoint(ctx, doc.Source.Kind, doc.Source.DSN, doc.Job+"-source", lookup, opts.DryRun)
	if err != nil {
		return Job{}, err
	}
	tgtID, err := s.resolveImportEndpoint(ctx, doc.Target.Kind, doc.Target.DSN, doc.Job+"-target", lookup, opts.DryRun)
	if err != nil {
		return Job{}, err
	}
	doc.Instance = ""
	in := JobInput{Name: doc.Job, SourceID: srcID, TargetID: tgtID, Enabled: false, Spec: doc}
	// spec 里去掉 dsn/kind：它们由连接决定，CreateJob 会重新填。
	in.Spec.Source.DSN, in.Spec.Source.Kind = "", ""
	in.Spec.Target.DSN, in.Spec.Target.Kind = "", ""
	if opts.DryRun {
		return Job{Name: doc.Job}, nil
	}
	// 指纹相同的任务已存在 → 更新它，而不是报冲突（幂等导入）。
	// 指纹只在 CreateJob/UpdateJob 里按连接坐标算一次，这里不重复计算。
	if existing, err := s.findByEndpointsAndScope(ctx, srcID, tgtID, doc.ScopeFingerprint()); err == nil {
		return s.UpdateJob(ctx, existing.ID, in)
	}
	return s.CreateJob(ctx, in)
}

// resolveImportEndpoint maps a yaml dsn to a stored connection id, creating the
// connection from the environment when needed.
func (s *Store) resolveImportEndpoint(ctx context.Context, kind, dsnRef, defaultName string,
	lookup func(string) (string, bool), dryRun bool) (string, error) {

	if id, ok := ConnectionIDFromPlaceholder(dsnRef); ok {
		if _, err := s.GetConnection(ctx, id); err == nil {
			return id, nil
		}
		// 占位符指向的连接不在本机（换了机器导入）：当成普通 env 继续，
		// 占位符名字本身不是环境变量，所以下面会给出可执行的提示。
	}
	name, isEnv := splitEnvDSN(dsnRef)
	if !isEnv {
		return "", invalid("导入只接受 env:NAME 或 env:JINLIANG_CONN_<id> 形式的 dsn（收到 %q）", dsnRef)
	}
	raw, ok := lookup(name)
	if !ok || strings.TrimSpace(raw) == "" {
		return "", invalid("环境变量 %s 没有设置。要么设置它（内容是一条完整 DSN），要么先在本机建好连接再导入", name)
	}
	ep, err := dsn.Parse(raw)
	if err != nil {
		return "", invalid("环境变量 %s 的内容不是可解析的 DSN: %v", name, err)
	}
	if kind != "" && ep.Kind != "" && kind != ep.Kind {
		return "", invalid("yaml 说 source.kind=%s，但 %s 里的 DSN 是 %s", kind, name, ep.Kind)
	}
	// 同坐标的连接已存在 → 复用（用户可能刚在 UI 建过）
	if conns, err := s.ListConnections(ctx); err == nil {
		for _, c := range conns {
			if c.Kind == ep.Kind && c.Host == ep.Host && c.Port == ep.Port &&
				c.Database == ep.Database && c.Username == ep.Username {
				return c.ID, nil
			}
		}
	}
	if dryRun {
		return "", nil
	}
	c, err := s.CreateConnection(ctx, ConnectionInput{
		Name: defaultName, Kind: ep.Kind, Host: ep.Host, Port: ep.Port,
		Database: ep.Database, Username: ep.Username, Password: ep.Password, Params: ep.Params,
	})
	if err != nil {
		return "", err
	}
	return c.ID, nil
}

// findByEndpointsAndScope finds an existing job with the same endpoints and the
// same table selection (that is what job_key encodes).
func (s *Store) findByEndpointsAndScope(ctx context.Context, sourceID, targetID, scope string) (Job, error) {
	src, err := s.GetConnection(ctx, sourceID)
	if err != nil {
		return Job{}, err
	}
	tgt, err := s.GetConnection(ctx, targetID)
	if err != nil {
		return Job{}, err
	}
	key := state.JobKey(identityDSN(src), identityDSN(tgt), scope)
	return s.FindJobByKey(ctx, key)
}

func splitEnvDSN(dsn string) (string, bool) {
	dsn = strings.TrimSpace(dsn)
	if strings.HasPrefix(dsn, "env:") {
		return strings.TrimPrefix(dsn, "env:"), true
	}
	return "", false
}

func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed") || strings.Contains(msg, "constraint")
}

func errorsIsNotFound(err error) bool { return err != nil && err == ErrNotFound }
