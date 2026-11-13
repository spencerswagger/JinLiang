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
//
// Type 是**完整类型文本**，含长度/精度/unsigned（`varchar(255)`、`numeric(12, 2)`、
// `bigint unsigned`、`datetime(6)`）。`catalog.normalizeType` 就按这个形状解析，
// 别在这里塞 charset 之类的修饰——那是下面两个字段的事。
type Column struct {
	Name     string  `json:"name"`
	Type     string  `json:"type"`
	Nullable bool    `json:"nullable"`
	Default  *string `json:"default,omitempty"`
	Comment  string  `json:"comment,omitempty"`
	// Charset / Collation 是源端的字符集与排序规则（可空：非字符类型或拿不到时留空）。
	// MySQL 的 binlog_row_metadata=FULL 与 PG 的 relation message 都给了这两项，
	// 不落进契约就只能丢掉或硬塞进 Type——那会让跨字符集的 upsert 判重与排序悄悄不一致。
	Charset   string `json:"charset,omitempty"`
	Collation string `json:"collation,omitempty"`
	// TypeOID 是源端**类型的身份**（PG: pg_type.oid；MySQL 源目前恒为 0 = 不知道）。
	//
	// 为什么要它：类型文本会在别名与 typmod 上骗人——`int8` / `bigint`、`numeric(12,2)` /
	// `numeric` 在文本上不同但**可能是同一个类型**，反之「改成了 text」与「放宽 varchar 长度」
	// 在文本上都叫变化、语义却一个是「暂停该表」一个是「追平即可」。比 OID 才是比身份。
	// 0 表示这个连接器拿不到身份（那就退回文本比较，并在文档里写清代价）。
	TypeOID int64 `json:"type_oid,omitempty"`
}

// Table is one catalog table.
type Table struct {
	TableRef
	Columns     []Column `json:"columns"`
	PrimaryKey  []string `json:"primary_key"`
	HasStablePK bool     `json:"has_stable_pk"` // 无主键：增量 upsert 与双向都不成立
	// UniqueKeys 是**目录侧**发现的唯一索引列集合（不含主键、不含部分/表达式索引、
	// 不含前缀索引、只含全列索引）。它只用于「核对用户声明的 unique_key 是否真实存在」
	//（TARGET-02），**不用来自动发现行身份**——身份只认源端主键或用户声明
	//（docs/DESIGN-NO-PRIMARY-KEY.md R1/§3.4）。
	UniqueKeys      [][]string `json:"unique_keys,omitempty"`
	RowsEstimate    int64      `json:"rows_estimate"`
	Partitioned     bool       `json:"partitioned"`
	Engine          string     `json:"engine,omitempty"` // MySQL: InnoDB / MyISAM
	ReplicaIdentity string     `json:"replica_identity,omitempty"`
}

// Catalog is what the user picks tables from.
type Catalog struct {
	Tables []Table    `json:"tables"`
	Views  []TableRef `json:"views"`
	// Engines 是**服务器自报的引擎事务能力**（`information_schema.ENGINES` 的
	// `TRANSACTIONS` 列）：键 = 大写引擎名，值 = `TRANSACTIONS='YES'`。
	//
	// 这是 SPEC MY-08 的判据输入，属于 **Catalog 而不是 CapSet**：CapSet 是服务器级
	// 能力键，而「这张表的引擎有没有事务」是**逐表**的，塞进 CapSet 会逼出一个按引擎名
	// 的键空间。
	//
	// 键不存在 = 服务器没说（老版本没有这列 / 读失败 / 插件引擎没装）——按「没有事务」
	// 处理（`preflight.JudgeEngines`）：**读不到能力 ≠ 有事务**。空 map 不是错误。
	Engines map[string]bool `json:"engines,omitempty"`
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
}

// KindJSON 的交付契约（2026-11-05 裁决；MySQL 的 json_binary 就是第一个用户）：
//
//   - **必须以文本形态交付**：Value.S 承载 UTF-8 的 JSON 文本（对象、数组、标量都按 JSON 文本写），
//     Value.B 不使用。sink 只负责按目标方言落列（PG 的 json/jsonb 按文本写入），不做第二遍解码。
//   - 源端负责解自己线缆上的二进制格式（例如 MySQL binlog 的 json_binary）：
//     这是连接器的私有格式，不能指望下游认识。类型映射见 docs/TYPE-MAPPING.md §5。
//   - **不得用 KindBytes 交付 JSON**：KindBytes 是二进制列（BYTEA/BLOB）的通道，
//     sink 会原样写进去，结果是目标上一列"看起来像 JSON 的乱码"，而全链路都没有报错。
//   - 解不出来就是数据或协议错误：返回 error 终止本批，**不降级为文本、不写半截**。
//     源库的 json 列本身保证合法，解不出只可能是数据损坏或格式理解错；此时把碎片当文本写进目标，
//     等于用"看起来成功"掩盖数据损坏——这比失败更难查。
//   - 目标端的单值上限与方言差异（PG jsonb 拒绝 \u0000、MySQL 数字按 double 失真）归 sink 判定，
//     超限一律明确失败并给下一步，不截断、不拆分。

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
//
// 契约（2026-11-05 裁决，为三轨并行定最小公共面）：
//   - String() 是**唯一**落到目标库的形态，格式固定为 "<kind>:v1:<payload>"：
//     kind 是产生它的连接类型（postgres / mysql），v1 是位点格式版本，
//     payload 由连接器自己定义（PG 的 "0/16B6C50"、MySQL 的 "mysql-bin.000003:1234"
//     或 GTID 集合）。位点是写进目标库、要跨版本存活的，格式一变旧库就读不懂；
//     **加版本前缀这件事现在零成本，以后是迁移**。解析入口见 SplitPosition。
//   - Kind() **只表达端点类型**（`postgres` / `mysql`），取值与 Endpoint.Kind() 同值；
//     **不返回位点变体**。位点变体（例如 MySQL 的 `file_pos` 与 `gtid` 两套坐标）
//     由连接器自己的坐标表达——MySQL 用 `Coordinate()` 返回 `mysql_file_pos` / `mysql_gtid`
//     并写进 payload（`;coord=…`），不进 Kind()。
//     为什么：Kind() 的消费方是引擎做「这个位点属于哪个源」的对账；让它返回细分值，
//     等于逼引擎知道每种源的内部变体，这层抽象就白做了。
//   - 前缀里的 kind 也只写 `postgres` / `mysql`，变体永远在 payload 里（见上一条）。
//     引擎拿它做对账：kind 与当前源不匹配时给 E1010（user_action：换回对应的源，或重新开始同步），
//     **不要用 E3001**——那是「变更流水接不上，建议 --resync 重搬全量」，
//     拿它报「这串位点是 MySQL 的、你连的是 PG」会把用户引去重搬一遍全量。
//   - 位点只在**事务边界**上前进（见 sink.Sink.Apply 的不变量与 ValidateBatch），
//     因此两次提交之间的比较只需要可判等，不需要全序。
type Position interface {
	String() string
	// Kind 返回产生这个位点的连接类型（postgres / mysql）。
	Kind() string
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
	// Relation 是这条事件的行镜像所依据的列定义（2026-11-05 裁决）。
	//   - 非 nil：New/Old 的 Values 按这份 Table.Columns 的顺序对齐。PG 的 Relation
	//     消息、MySQL 开了 binlog_row_metadata=FULL 时都能直接给出，两者不需要互相迁就。
	//   - nil：按 EnsureSchema 时那份 catalog 对齐（连接器没拿到更细的元数据）。
	// 源端 DDL 之后，连接器必须先发一条**带新 Relation** 的事件（EvDDL 本身或其紧随的事件），
	// 再发按新列定义的数据事件——否则引擎无从判断"这一行有哪几列"，只能按旧 catalog 猜错。
	// 实现约定：同一版定义请复用同一个指针（指针相等即同一版），不要每条事件都新建。
	Relation *Table
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
	// ParsePosition 把目标库里存着的位点串还原成连接器自己的类型（2026-11-05 裁决）。
	//
	// 为什么放在 Source 上而不是让引擎自己解析：位点串的格式归连接器所有，引擎不该
	// import 具体连接器包（否则 engine 与 pgsource/mysqlsource 互相依赖）。
	// 解析失败按「位点与本连接类型不匹配或已损坏」处理，返回错误让上层给 E3001，
	// 不要静默回落到全量重搬。
	ParsePosition(raw string) (Position, error)
	// Advance moves the source-side checkpoint forward (slot advance / purge).
	Advance(ctx context.Context, to Position) error
	// Cleanup removes objects this job created (slot, publication, temp snapshot).
	Cleanup(ctx context.Context) ([]Action, error)
}
