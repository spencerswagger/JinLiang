package control

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// JobDoc is the in-memory shape of a job.yaml document (schema/job.schema.json)
// and, simultaneously, of jobs.spec_json.
//
// 为什么共用一套结构：CONTROL-PLANE §3 说 spec_json「字段与 job.yaml schema 一致，
// 便于导入导出」。两套并行结构迟早漂移，所以只有这一套；区别只在于
// spec_json 里 source.kind/dsn 与 target.kind/dsn 留空——它们来自 connections 表，
// 导出时再填。JSON tag 与 YAML tag 指向同一批名字，导出物天然过 schema。
type JobDoc struct {
	Version       int               `yaml:"version" json:"version"`
	Job           string            `yaml:"job" json:"job"`
	Instance      string            `yaml:"instance,omitempty" json:"instance,omitempty"`
	Source        SourceDoc         `yaml:"source" json:"source"`
	Target        TargetDoc         `yaml:"target" json:"target"`
	Mode          ModeDoc           `yaml:"mode" json:"mode"`
	Content       *ContentDoc       `yaml:"content,omitempty" json:"content,omitempty"`
	Conflict      *ConflictDoc      `yaml:"conflict,omitempty" json:"conflict,omitempty"`
	Reliability   *ReliabilityDoc   `yaml:"reliability,omitempty" json:"reliability,omitempty"`
	Guard         *GuardDoc         `yaml:"guard,omitempty" json:"guard,omitempty"`
	Observability *ObservabilityDoc `yaml:"observability,omitempty" json:"observability,omitempty"`
}

// SourceDoc is the source half of a job.
type SourceDoc struct {
	Kind     string       `yaml:"kind" json:"kind"`
	DSN      string       `yaml:"dsn" json:"dsn"`
	Scope    *ScopeDoc    `yaml:"scope,omitempty" json:"scope,omitempty"`
	Snapshot *SnapshotDoc `yaml:"snapshot,omitempty" json:"snapshot,omitempty"`
}

// ScopeDoc selects which tables on the source participate.
type ScopeDoc struct {
	Schema  string   `yaml:"schema,omitempty" json:"schema,omitempty"`
	Include []string `yaml:"include,omitempty" json:"include,omitempty"`
	Exclude []string `yaml:"exclude,omitempty" json:"exclude,omitempty"`
}

// SnapshotDoc tunes the initial bulk copy.
type SnapshotDoc struct {
	ChunkRows  *int `yaml:"chunk_rows,omitempty" json:"chunk_rows,omitempty"`
	Conns      *int `yaml:"conns,omitempty" json:"conns,omitempty"`
	MaxReadQPS *int `yaml:"max_read_qps,omitempty" json:"max_read_qps,omitempty"`
}

// TargetDoc is the target half of a job.
type TargetDoc struct {
	Kind          string `yaml:"kind" json:"kind"`
	DSN           string `yaml:"dsn" json:"dsn"`
	Schema        string `yaml:"schema,omitempty" json:"schema,omitempty"`
	OnExists      string `yaml:"on_exists,omitempty" json:"on_exists,omitempty"`
	CreateIndexes string `yaml:"create_indexes,omitempty" json:"create_indexes,omitempty"`
}

// ModeDoc is how often and how deeply the job runs.
type ModeDoc struct {
	Frequency         string `yaml:"frequency" json:"frequency"`
	Interval          string `yaml:"interval,omitempty" json:"interval,omitempty"`
	IncrementalColumn string `yaml:"incremental_column,omitempty" json:"incremental_column,omitempty"`
	Lookback          string `yaml:"lookback,omitempty" json:"lookback,omitempty"`
	MaxOpsPerSec      *int   `yaml:"max_ops_per_sec,omitempty" json:"max_ops_per_sec,omitempty"`
}

// ContentDoc is per-table column/row selection.
type ContentDoc struct {
	Tables       map[string]TableDoc `yaml:"tables,omitempty" json:"tables,omitempty"`
	SchemaFollow string              `yaml:"schema_follow,omitempty" json:"schema_follow,omitempty"`
}

// TableDoc is one table entry under content.tables.
type TableDoc struct {
	Columns   *ColumnsDoc      `yaml:"columns,omitempty" json:"columns,omitempty"`
	Where     string           `yaml:"where,omitempty" json:"where,omitempty"`
	Transform []map[string]any `yaml:"transform,omitempty" json:"transform,omitempty"`
}

// ColumnsDoc selects or retypes columns.
type ColumnsDoc struct {
	Include []string          `yaml:"include,omitempty" json:"include,omitempty"`
	Exclude []string          `yaml:"exclude,omitempty" json:"exclude,omitempty"`
	Map     map[string]string `yaml:"map,omitempty" json:"map,omitempty"`
}

// ConflictDoc decides what happens when both sides changed a row.
type ConflictDoc struct {
	Policy   string `yaml:"policy,omitempty" json:"policy,omitempty"`
	LogTable string `yaml:"log_table,omitempty" json:"log_table,omitempty"`
}

// ReliabilityDoc covers batching, retry and dead letters.
type ReliabilityDoc struct {
	LeaseTTL   string         `yaml:"lease_ttl,omitempty" json:"lease_ttl,omitempty"`
	BatchRows  *int           `yaml:"batch_rows,omitempty" json:"batch_rows,omitempty"`
	BatchDelay string         `yaml:"batch_delay,omitempty" json:"batch_delay,omitempty"`
	Retry      *RetryDoc      `yaml:"retry,omitempty" json:"retry,omitempty"`
	DeadLetter *DeadLetterDoc `yaml:"dead_letter,omitempty" json:"dead_letter,omitempty"`
	TxOversize string         `yaml:"tx_oversize,omitempty" json:"tx_oversize,omitempty"`
}

// RetryDoc is the backoff ladder.
type RetryDoc struct {
	Max     *int     `yaml:"max,omitempty" json:"max,omitempty"`
	Backoff []string `yaml:"backoff,omitempty" json:"backoff,omitempty"`
}

// DeadLetterDoc bounds how many bad rows we keep.
type DeadLetterDoc struct {
	MaxRows      *int     `yaml:"max_rows,omitempty" json:"max_rows,omitempty"`
	OnErrorRatio *float64 `yaml:"on_error_ratio,omitempty" json:"on_error_ratio,omitempty"`
	StoreRow     string   `yaml:"store_row,omitempty" json:"store_row,omitempty"`
}

// GuardDoc protects the source from an over-eager sync.
type GuardDoc struct {
	SlotWarnLag     string `yaml:"slot_warn_lag,omitempty" json:"slot_warn_lag,omitempty"`
	SlotWarnSeconds string `yaml:"slot_warn_seconds,omitempty" json:"slot_warn_seconds,omitempty"`
	OnExceed        string `yaml:"on_exceed,omitempty" json:"on_exceed,omitempty"`
}

// ObservabilityDoc is metrics/logging knobs.
type ObservabilityDoc struct {
	MetricsAddr string `yaml:"metrics_addr,omitempty" json:"metrics_addr,omitempty"`
	LogLevel    string `yaml:"log_level,omitempty" json:"log_level,omitempty"`
}

var (
	reDSN      = regexp.MustCompile(`^(env:|file:)`)
	reDuration = regexp.MustCompile(`^[0-9]+(ms|s|m|h)$`)
	reSize     = regexp.MustCompile(`^[0-9]+(KB|MB|GB|TB)$`)
	reCron     = regexp.MustCompile(`^cron:.*$`)
)

// Defaults applied when a field is absent. Visible and overridable on purpose
// (docs/SOUL.md: 所有默认值都要可见、可改、可恢复默认).
const (
	DefaultBatchRows    = 500
	DefaultChunkRows    = 100000
	DefaultOnExists     = "error"
	DefaultCreateIdx    = "defer"
	DefaultConflictPol  = "manual"
	DefaultSchemaFollow = "whitelist"
)

// ApplyDefaults fills the optional fields the UI likes to show as placeholders.
// It never overrides an explicit value.
func (d *JobDoc) ApplyDefaults() {
	if d.Version == 0 {
		d.Version = 1
	}
	if d.Target.OnExists == "" {
		d.Target.OnExists = DefaultOnExists
	}
	if d.Target.CreateIndexes == "" {
		d.Target.CreateIndexes = DefaultCreateIdx
	}
	if d.Content != nil && d.Content.SchemaFollow == "" {
		d.Content.SchemaFollow = DefaultSchemaFollow
	}
	if d.Conflict != nil && d.Conflict.Policy == "" {
		d.Conflict.Policy = DefaultConflictPol
	}
	if d.Reliability != nil && d.Reliability.BatchRows == nil {
		n := DefaultBatchRows
		d.Reliability.BatchRows = &n
	}
	if d.Source.Snapshot != nil && d.Source.Snapshot.ChunkRows == nil {
		n := DefaultChunkRows
		d.Source.Snapshot.ChunkRows = &n
	}
}

// Validate reproduces schema/job.schema.json in Go. The Python validator stays
// authoritative in CI; this one exists so the UI can refuse bad input before it
// ever reaches a file (and so `import` does not need a JSON-Schema library).
func (d *JobDoc) Validate() error {
	var errs []string
	add := func(format string, args ...any) { errs = append(errs, fmt.Sprintf(format, args...)) }

	if d.Version != 1 {
		add("version 必须是 1（收到 %d）", d.Version)
	}
	if strings.TrimSpace(d.Job) == "" {
		add("job 名字不能为空")
	} else if len(d.Job) > 64 {
		add("job 名字最多 64 个字符")
	}
	if len(d.Instance) > 32 {
		add("instance 最多 32 个字符")
	}

	// source
	switch d.Source.Kind {
	case "postgres", "mysql":
	default:
		add("source.kind 必须是 postgres 或 mysql（收到 %q）", d.Source.Kind)
	}
	checkDSN := func(where, dsn string, required bool) {
		if dsn == "" {
			if required {
				add("%s.dsn 不能为空（只接受 env:NAME 或 file:/path）", where)
			}
			return
		}
		if !reDSN.MatchString(dsn) {
			add("%s.dsn 必须以 env: 或 file: 开头（禁止明文密码，docs/SECURITY.md §1）", where)
		}
	}
	checkDSN("source", d.Source.DSN, true)
	checkDSN("target", d.Target.DSN, true)
	if d.Target.Kind != "postgres" && d.Target.Kind != "mysql" {
		add("target.kind 必须是 postgres 或 mysql（收到 %q）", d.Target.Kind)
	}
	if d.Source.Snapshot != nil {
		if v := d.Source.Snapshot.ChunkRows; v != nil && *v < 1000 {
			add("source.snapshot.chunk_rows 至少 1000（收到 %d）", *v)
		}
		if v := d.Source.Snapshot.Conns; v != nil && (*v < 1 || *v > 16) {
			add("source.snapshot.conns 必须在 1–16 之间（收到 %d）", *v)
		}
		if v := d.Source.Snapshot.MaxReadQPS; v != nil && *v < 1 {
			add("source.snapshot.max_read_qps 至少 1")
		}
	}
	oneOf := func(where, val string, allowed ...string) {
		if val == "" {
			return
		}
		for _, a := range allowed {
			if val == a {
				return
			}
		}
		add("%s 只能是 %s（收到 %q）", where, strings.Join(allowed, " / "), val)
	}
	oneOf("target.on_exists", d.Target.OnExists, "error", "append", "recreate")
	oneOf("target.create_indexes", d.Target.CreateIndexes, "defer", "now", "none")

	// mode
	oneOf("mode.frequency", d.Mode.Frequency, "once", "continuous+initial", "continuous", "interval", "full_interval")
	switch d.Mode.Frequency {
	case "":
		add("mode.frequency 不能为空")
	case "interval", "full_interval":
		if d.Mode.Interval == "" {
			add("mode.frequency=%s 时必须给 mode.interval（例如 5m / 1h / cron:0 */2 * * *）", d.Mode.Frequency)
		}
	}
	if d.Mode.Frequency == "interval" && d.Mode.IncrementalColumn == "" {
		add("mode.frequency=interval 时必须给 mode.incremental_column（水位列，例如 updated_at）")
	}
	if d.Mode.Interval != "" && !reDuration.MatchString(d.Mode.Interval) && !reCron.MatchString(d.Mode.Interval) {
		add("mode.interval 格式不对：%q（要 5m / 1h 这类，或 cron:...）", d.Mode.Interval)
	}
	checkDuration := func(where, v string) {
		if v != "" && !reDuration.MatchString(v) {
			add("%s 格式不对：%q（要 100ms / 5s / 2m / 1h）", where, v)
		}
	}
	checkDuration("mode.lookback", d.Mode.Lookback)

	// content
	if d.Content != nil {
		oneOf("content.schema_follow", d.Content.SchemaFollow, "off", "whitelist")
		for name, t := range d.Content.Tables {
			if strings.TrimSpace(name) == "" {
				add("content.tables 里有空表名")
			}
			if t.Where != "" && (strings.Contains(strings.ToLower(t.Where), "select ") || strings.Contains(t.Where, ";")) {
				add("content.tables.%s.where 只能写列比较 + 常量，不支持子查询或多语句", name)
			}
			if t.Columns != nil && len(t.Columns.Include) > 0 && len(t.Columns.Exclude) > 0 {
				add("content.tables.%s.columns 不能同时给 include 和 exclude", name)
			}
		}
	}

	// conflict
	if d.Conflict != nil {
		oneOf("conflict.policy", d.Conflict.Policy, "manual", "source_wins", "target_wins", "newest")
	}
	// reliability
	if r := d.Reliability; r != nil {
		checkDuration("reliability.lease_ttl", r.LeaseTTL)
		checkDuration("reliability.batch_delay", r.BatchDelay)
		if r.BatchRows != nil && (*r.BatchRows < 1 || *r.BatchRows > 50000) {
			add("reliability.batch_rows 必须在 1–50000 之间（收到 %d）", *r.BatchRows)
		}
		if r.Retry != nil {
			if r.Retry.Max != nil && *r.Retry.Max < 0 {
				add("reliability.retry.max 不能为负")
			}
			for _, b := range r.Retry.Backoff {
				checkDuration("reliability.retry.backoff[]", b)
			}
		}
		if dl := r.DeadLetter; dl != nil {
			if dl.MaxRows != nil && *dl.MaxRows < 0 {
				add("reliability.dead_letter.max_rows 不能为负")
			}
			if dl.OnErrorRatio != nil && (*dl.OnErrorRatio < 0 || *dl.OnErrorRatio > 1) {
				add("reliability.dead_letter.on_error_ratio 必须在 0–1 之间")
			}
			oneOf("reliability.dead_letter.store_row", dl.StoreRow, "full", "none")
		}
		oneOf("reliability.tx_oversize", r.TxOversize, "spill", "fail")
	}
	// guard
	if g := d.Guard; g != nil {
		if g.SlotWarnLag != "" && !reSize.MatchString(g.SlotWarnLag) {
			add("guard.slot_warn_lag（同步进度落后多少时告警）格式不对：%q（要 5GB 这类）", g.SlotWarnLag)
		}
		checkDuration("guard.slot_warn_seconds（落后多久时告警）", g.SlotWarnSeconds)
		oneOf("guard.on_exceed", g.OnExceed, "pause", "invalidate", "advance")
	}
	// observability
	if o := d.Observability; o != nil {
		oneOf("observability.log_level", o.LogLevel, "debug", "info", "warn", "error")
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "；"))
	}
	return nil
}

// ParseJobYAML decodes one job.yaml document (strict: unknown fields are an
// error, matching the "no hidden switches" rule in docs/SPEC.md).
func ParseJobYAML(data []byte) (JobDoc, error) {
	var doc JobDoc
	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	dec.KnownFields(true)
	if err := dec.Decode(&doc); err != nil && err.Error() != "EOF" {
		return JobDoc{}, fmt.Errorf("yaml 解析失败: %w", err)
	}
	return doc, nil
}

// MarshalJobYAML renders a document the way `jinliang export` writes it.
func MarshalJobYAML(doc JobDoc) ([]byte, error) {
	var b strings.Builder
	b.WriteString("# 津梁任务定义（由控制库导出，可再次 import 回来）\n")
	b.WriteString("# 密码不在这个文件里：dsn 写的是占位符 env:JINLIANG_CONN_<连接id>，\n")
	b.WriteString("# 导入时若本机控制库已有该连接，会直接复用已加密的凭证。\n")
	enc := yaml.NewEncoder(&b)
	enc.SetIndent(2)
	if err := enc.Encode(doc); err != nil {
		return nil, err
	}
	if err := enc.Close(); err != nil {
		return nil, err
	}
	return []byte(b.String()), nil
}

// ScopeFingerprint canonicalises 「选了哪些表」 for state.JobKey. Different table
// selections must yield different job_keys (docs/IDENTITY.md §2).
func (d *JobDoc) ScopeFingerprint() string {
	var b strings.Builder
	if sc := d.Source.Scope; sc != nil {
		b.WriteString("schema=" + strings.ToLower(strings.TrimSpace(sc.Schema)) + ";")
		b.WriteString("include=" + joinSorted(sc.Include) + ";")
		b.WriteString("exclude=" + joinSorted(sc.Exclude) + ";")
	}
	if d.Content != nil {
		names := make([]string, 0, len(d.Content.Tables))
		for n := range d.Content.Tables {
			names = append(names, n)
		}
		sortStrings(names)
		b.WriteString("tables=" + strings.Join(names, ","))
	}
	return b.String()
}

func joinSorted(in []string) string {
	cp := make([]string, len(in))
	for i, s := range in {
		cp[i] = strings.ToLower(strings.TrimSpace(s))
	}
	sortStrings(cp)
	return strings.Join(cp, ",")
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
