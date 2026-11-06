package control

import (
	"context"
	"encoding/json"
	"errors"
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
	if key != cur.JobKey {
		// 改连接或改表范围 = 换身份。B1 起目标端的同步元数据（_jl_meta）是按
		// job_key 存的，静默重算会让旧身份下的状态变成孤儿：新 run 从头再来，
		// 旧书签没人回收，用户看到的是「数据重复」而不是「你改了配置」。
		// 有运行历史就硬挡；没有历史（刚建还没跑过）允许改，那是正常的编辑。
		var runs int
		if err := s.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM runs WHERE job_id = ?`, id).Scan(&runs); err != nil {
			return Job{}, err
		}
		if runs > 0 {
			return Job{}, fmt.Errorf("%w：这个任务已经有 %d 条运行历史，换连接或改表范围会换掉它的身份（指纹 %s → %s），目标库里的同步状态会变成孤儿。要换就新建一个任务，再停用这个",
				ErrIdentityLocked, runs, cur.JobKey, key)
		}
	}
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
		// 列表页的「最近一次运行」只认同步运行：verify 也写 runs，但它不是同步
		if run, err := s.LatestSyncRun(ctx, out[i].ID); err == nil {
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
	if run, err := s.LatestSyncRun(ctx, id); err == nil {
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
// **守卫：有未结束的 run 就拒删。** 没有这道守卫时，网页入口（`DELETE /api/jobs/{id}`）
// 会「删掉记录、但引擎的 goroutine 还在写、目标端租约还在续期」→ 同目标新建任务
// 一直撞 E1005，而用户没有任何办法让那个进程停下来。
//
// 这个判断放在**数据入口**而不是每个调用方：两个入口各写一遍迟早漏一个
// （今晚 `schemaOf`/`targetSchema` 就是这么分叉的）。CLI 的删除路径先 StopJob
// 再删，所以它能通过；网页在接上 StopJob 之前会拿到一句可执行的话。
//
// 归属：本守卫由 Core 代改，**后续 job 删除语义归 Panel**。
func (s *Store) DeleteJob(ctx context.Context, id string) error {
	active, err := s.ActiveRuns(ctx)
	if err != nil {
		return err
	}
	for _, run := range active {
		if run.JobID != id {
			continue
		}
		return fmt.Errorf("%w：这个任务还有一条没结束的运行（run %s，阶段 %s，心跳 %s）。"+
			"删任务会留下「记录没了、引擎还在写」的状态，所以先停掉它。"+
			"你能做什么：1) 跑一次 `jinliang delete --job %s`（会先停引擎再删）；"+
			"2) 或在网页上先点「停止」再删。"+
			"风险：没有（什么都没删）",
			ErrRunActive, run.ID, run.Phase, orDash(run.HeartbeatAt), id)
	}
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

// orDash 让空值在文案里显示成「（无）」而不是空白。
func orDash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "（无）"
	}
	return s
}

// ErrRunActive 表示任务还有没结束的运行，删除被拒（E1005 同一族：先停再删）。
var ErrRunActive = errors.New("任务还有没结束的运行")

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

// ImportSkip is one document that could not be imported. It is reported to the
// user instead of being swallowed: "import 3 of 5" must never look like success.
type ImportSkip struct {
	Index  int    `json:"index"` // 1-based, 在输入里的第几份
	Name   string `json:"name,omitempty"`
	Reason string `json:"reason"`
}

// ImportReport is what a multi-document import did.
type ImportReport struct {
	Total    int          `json:"total"`
	Imported []Job        `json:"imported"`
	Skipped  []ImportSkip `json:"skipped"`
	DryRun   bool         `json:"dry_run"`
}

// ImportedCount is the number of jobs written (or validated, for dry runs).
func (r ImportReport) ImportedCount() int { return len(r.Imported) }

// Summary is the one line the CLI prints and the UI shows.
func (r ImportReport) Summary() string {
	if r.DryRun {
		return fmt.Sprintf("校验通过 %d 份 / 跳过 %d 份（dry-run，没有写入）", r.ImportedCount(), len(r.Skipped))
	}
	return fmt.Sprintf("导入 %d 份 / 跳过 %d 份", r.ImportedCount(), len(r.Skipped))
}

// ImportYAML turns one job.yaml document into connections + a job. Existing
// connections are reused (by placeholder id, then by coordinates) so repeated
// imports are idempotent — GitOps needs that.
func (s *Store) ImportYAML(ctx context.Context, data []byte, opts ImportOptions) (Job, error) {
	doc, err := ParseJobYAML(data)
	if err != nil {
		return Job{}, err
	}
	return s.importDoc(ctx, doc, opts)
}

// ImportYAMLDocs imports every document in a `---` stream (what `export --all`
// writes). 单份失败不再让整批静默丢掉：成功的照写，失败的逐条回报，
// 由调用方决定退出码（CLI 有跳过就非零退出，HTTP 返回 207）。
func (s *Store) ImportYAMLDocs(ctx context.Context, data []byte, opts ImportOptions) (ImportReport, error) {
	docs, err := ParseJobYAMLDocs(data)
	if err != nil {
		return ImportReport{}, err
	}
	rep := ImportReport{Total: len(docs), DryRun: opts.DryRun}
	for i, doc := range docs {
		job, err := s.importDoc(ctx, doc, opts)
		if err != nil {
			rep.Skipped = append(rep.Skipped, ImportSkip{Index: i + 1, Name: doc.Job, Reason: err.Error()})
			continue
		}
		rep.Imported = append(rep.Imported, job)
	}
	if rep.ImportedCount() == 0 && len(rep.Skipped) > 0 {
		return rep, fmt.Errorf("%s；第一份失败原因：%s", rep.Summary(), rep.Skipped[0].Reason)
	}
	return rep, nil
}

// importDoc is the single-document body shared by ImportYAML / ImportYAMLDocs.
func (s *Store) importDoc(ctx context.Context, doc JobDoc, opts ImportOptions) (Job, error) {
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
