// Package source abstracts change producers: initial snapshot, incremental
// change stream, position advancement, plus the capability probing and
// compensation declarations every connector must implement (DESIGN.md §4A).
package source

import (
	"context"
	"time"
)

// Capability names a database feature the sync engine depends on.
// Connectors probe for capabilities at runtime; they never branch on version
// numbers (MySQL 8.4 removed SHOW MASTER STATUS, PG 10 lacks a slot disk cap).
type Capability string

// Capabilities the engine can consume. A connector reports the ones it has.
const (
	CapSnapshot       Capability = "snapshot"        // 全量读取
	CapSnapshotExport Capability = "snapshot_export" // PG: EXPORT_SNAPSHOT
	CapCDC            Capability = "cdc"             // 增量变更流
	CapTruncate       Capability = "truncate_replication"
	CapRowFilter      Capability = "row_filter"      // PG 15+
	CapColumnList     Capability = "column_list"     // PG 15+
	CapSlotDiskLimit  Capability = "slot_disk_limit" // PG 13+ max_slot_wal_keep_size
	CapSlotAdvance    Capability = "slot_advance"    // PG 11+ pg_replication_slot_advance()
	CapStreamingTx    Capability = "streaming_tx"    // PG 14+ protocol v2
	CapTwoPhase       Capability = "two_phase"       // PG 15+ TWO_PHASE
	CapBinaryCopy     Capability = "binary_initial_copy"
	CapGTID           Capability = "gtid"
	CapBackupLock     Capability = "backup_lock"        // MySQL 8.0+ LOCK INSTANCE FOR BACKUP
	CapBinlogCompress Capability = "binlog_compression" // MySQL 8.0.20+
)

// Level says what the engine does when a capability is missing.
// The rule: compensate first, refuse only when compensation would break
// correctness (DESIGN.md §4A).
type Level string

const (
	LevelAuto    Level = "auto"    // 我们自动补偿，用户无感
	LevelCostly  Level = "costly"  // 有代价的替代，需用户确认
	LevelBlocked Level = "blocked" // 物理上做不到（缺权限/缺变更流）；不是"因风险拒绝"，并给替代
)

// Compensation describes the fallback for one missing capability.
type Compensation struct {
	Level     Level  `json:"level"`
	Note      string `json:"note"`       // 人话说明，直接进预检报告
	ConfigKey string `json:"config_key"` // 对应 job.yaml 的开关；空表示无需配置
}

// CapabilityState is the probe result for one capability.
type CapabilityState struct {
	Supported    bool         `json:"supported"`
	SinceVersion string       `json:"since_version,omitempty"` // 首次出现的版本，仅用于文档
	Compensation Compensation `json:"compensation"`
}

// CapSet is the whole probe result.
type CapSet map[Capability]CapabilityState

// TableRef names a table in a specific schema.
type TableRef struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
}

// Column is one catalog column.
type Column struct {
	Name     string  `json:"name"`
	Type     string  `json:"type"`
	Nullable bool    `json:"nullable"`
	Default  *string `json:"default,omitempty"`
	Comment  string  `json:"comment,omitempty"`
}

// Table is one catalog table.
type Table struct {
	TableRef
	Columns         []Column `json:"columns"`
	PrimaryKey      []string `json:"primary_key"`
	HasStablePK     bool     `json:"has_stable_pk"` // 无主键：增量 upsert 与双向都不成立
	RowsEstimate    int64    `json:"rows_estimate"`
	Partitioned     bool     `json:"partitioned"`
	Engine          string   `json:"engine,omitempty"` // MySQL: InnoDB / MyISAM
	ReplicaIdentity string   `json:"replica_identity,omitempty"`
}

// Catalog is what the user picks tables from.
type Catalog struct {
	Tables []Table    `json:"tables"`
	Views  []TableRef `json:"views"`
}

// Scope selects tables by schema plus include/exclude patterns.
type Scope struct {
	Schema  string   `json:"schema"`
	Include []string `json:"include,omitempty"`
	Exclude []string `json:"exclude,omitempty"`
}

// ValueKind tags a decoded value without interface boxing on the hot path.
type ValueKind uint8

// Decoded value kinds.
const (
	KindNull ValueKind = iota
	KindInt
	KindUint
	KindFloat
	KindString
	KindBytes
	KindTime
	KindBool
	KindJSON
	// KindRaw carries a driver-native value (pgx already decoded it). The
	// snapshot path uses it so a value never round-trips through text: numeric,
	// uuid, interval and arrays keep their exact representation.
	KindRaw
)

// Value is one decoded cell. Decoders fill exactly one payload field.
type Value struct {
	Kind ValueKind
	I    int64
	U    uint64
	F    float64
	S    string
	B    []byte
	T    time.Time
	V    any // KindRaw payload
}

// Any converts a decoded cell back into the Go value a driver takes as a
// parameter. Sinks use it so they never have to switch on ValueKind themselves.
func (v Value) Any() any {
	switch v.Kind {
	case KindNull:
		return nil
	case KindInt:
		return v.I
	case KindUint:
		return v.U
	case KindFloat:
		return v.F
	case KindString:
		return v.S
	case KindBytes:
		return v.B
	case KindTime:
		return v.T
	case KindBool:
		return v.I != 0
	case KindJSON:
		return v.S
	case KindRaw:
		return v.V
	}
	return nil
}

// Row is one table row in column order (column names come from the catalog).
type Row struct {
	Table  TableRef
	Values []Value
}

// RowHandler consumes snapshot rows; returning an error aborts the snapshot.
type RowHandler func(ctx context.Context, row Row) error

// Position is an opaque, comparable checkpoint. Concrete types live in each
// connector package (PGPosition, MySQLPosition) and serialize to one string
// that is written to the target database inside the same transaction as data.
type Position interface {
	String() string
}

// EventKind classifies one change event.
type EventKind uint8

// Change event kinds.
const (
	EvBegin EventKind = iota + 1
	EvInsert
	EvUpdate
	EvDelete
	EvTruncate
	EvDDL
	EvCommit
	EvHeartbeat
)

// Event is one unit of change. Rows are only valid until the handler returns.
type Event struct {
	Kind     EventKind
	Table    TableRef
	New      Row
	Old      Row
	TxID     string    // 用于把同一事务的事件串起来
	CommitTS time.Time // 源端提交时间；拿不到时为零值（PG track_commit_timestamp=off）
	DDL      string    // EvDDL 时的原始语句
	// Position is set on EvCommit: the checkpoint this transaction makes
	// durable once its data lands (docs/STATE-SCHEMA.md §2).
	Position string
}

// EventHandler consumes the stream; returning an error stops the stream.
type EventHandler func(ctx context.Context, ev Event) error

// CheckStatus is the three-way preflight verdict.
type CheckStatus string

// Preflight verdicts.
const (
	StatusOK          CheckStatus = "ok"          // 已满足
	StatusAutofixable CheckStatus = "autofixable" // 我可代做（可逆、不动数据、不重启）
	StatusWarn        CheckStatus = "warn"        // 有风险但能跑：只提示，用户确认即可继续
	StatusBlocked     CheckStatus = "blocked"     // 物理上做不到（缺变更流/缺权限/副本），不是"不让你做"
)

// Check is one preflight item, written so a non-DBA can act on it.
type Check struct {
	ID       string      `json:"id"`    // PG-04 / MY-02 / NET-01
	Title    string      `json:"title"` // 一句话说人话
	Status   CheckStatus `json:"status"`
	Detail   string      `json:"detail"`         // 当前真实值
	SQL      string      `json:"sql,omitempty"`  // 可直接复制执行（blocked 时为给用户的那条）
	Undo     string      `json:"undo,omitempty"` // 回滚语句
	Who      string      `json:"who,omitempty"`  // you / dba / cloud-console
	DocURL   string      `json:"doc_url,omitempty"`
	Affected []string    `json:"affected,omitempty"`
	// Code links this check to the four-part error card in docs/ERRORS.md, so
	// the UI can render 「发生了什么/为什么/能做什么/风险」 without a lookup table.
	Code string `json:"code,omitempty"`
}

// Report is the whole preflight result.
type Report struct {
	Checks   []Check `json:"checks"`
	CanStart bool    `json:"can_start"` // blocked=物理做不到时 false；warn 不影响启动，只需确认
}

// Requirements declares which capabilities a job needs, so preflight can look
// only at what matters.
type Requirements struct {
	Needs         []Capability `json:"needs"`
	Mode          string       `json:"mode"` // once / continuous / interval …
	SnapshotConns int          `json:"snapshot_conns"`
}

// Action is one side effect the tool performs on the user's database. Every
// action must be reversible and is listed in "系统为你做了什么".
type Action struct {
	ID      string `json:"id"`
	What    string `json:"what"` // 人话
	SQL     string `json:"sql,omitempty"`
	Undo    string `json:"undo,omitempty"`
	Applied bool   `json:"applied"`
	Reason  string `json:"reason,omitempty"` // 为什么做这一步
}

// SnapshotOpt controls a full load.
type SnapshotOpt struct {
	ChunkRows  int64 `json:"chunk_rows"` // 每块行数，默认 100000
	Conns      int   `json:"conns"`      // 并发块数，默认 4
	MaxReadQPS int   `json:"max_read_qps"`
}

// Endpoint is everything both sides share.
type Endpoint interface {
	Kind() string
	Version(ctx context.Context) (string, error)
	Probe(ctx context.Context) (CapSet, error)
	Catalog(ctx context.Context, scope Scope) (Catalog, error)
}

// Source is the read side.
type Source interface {
	Endpoint
	Preflight(ctx context.Context, req Requirements) (Report, error)
	// Prepare performs the autofixable actions; each carries its own undo.
	Prepare(ctx context.Context, actions []Action) ([]Action, error)
	// Snapshot reads the existing rows and returns the stream start position.
	Snapshot(ctx context.Context, opt SnapshotOpt, onRow RowHandler) (Position, error)
	// Stream emits changes from pos until ctx is cancelled.
	Stream(ctx context.Context, from Position, onEvent EventHandler) error
	// Advance moves the source-side checkpoint forward (slot advance / purge).
	Advance(ctx context.Context, to Position) error
	// Cleanup removes objects this job created (slot, publication, temp snapshot).
	Cleanup(ctx context.Context) ([]Action, error)
}
