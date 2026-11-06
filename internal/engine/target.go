package engine

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/spencerswagger/jinliang/internal/catalog"
	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/mysqlsink"
	"github.com/spencerswagger/jinliang/internal/pgsink"
	"github.com/spencerswagger/jinliang/internal/pgsource"
	"github.com/spencerswagger/jinliang/internal/sink"
	"github.com/spencerswagger/jinliang/internal/source"
	"github.com/spencerswagger/jinliang/internal/state"
)

// 目标端抽象：**在构造阶段按连接类型确定**，跑起来之后引擎只认接口。
//
// 为什么不写 `if target.Kind == "mysql"`：那样的分支会长在所有用到目标端的
// 地方（全量、增量、校验、清理各一次），把 B2/B3 的边界焊死在引擎里。
// 这里只有一个工厂（newTargetSink），之后全是接口调用——写路径只有两条：
// PG 快照直连 pgsink.BulkApply（COPY + 块记账），其它目标端走契约的 sink.Apply。
type targetSink interface {
	sink.Sink
	// Close 两种 sink 的签名不同（pgsink.Close() / mysqlsink.Close() error），
	// 这里只要求「能关」：错误在 teardown 里忽略（关连接失败不该盖住原错误）。
	Close() error
}

// stateCapable 是「目标端状态表能力」：租约、位点、块记账、对象登记。
//
// 这些**不在 sink.Sink 契约里**（契约归 Ark，只描述数据写入），所以在引擎侧定义成
// 可选能力接口——全量路径只要求 sink.Sink，增量路径才要求它。
//
// 关键：必须有编译期断言（下面 var _ 那两行）。否则签名差一个参数，
// 运行时会静默 `ok == false` 而退回「这个目标端不支持增量」——功能看着在、其实从没被走到。
// 故意**不**列 Position / Watermark / SetWatermark 之类：
// 它们不是这条路径要用的能力，漏列只会变成编译错误（安全），
// 而多补一条断言等于把「编译期保证」换成「运行时检查」——那是退步。
// 换句话说：这个接口的成员应该由「谁在用」决定，不由「谁有」决定。
type stateCapable interface {
	AcquireLease(ctx context.Context, ttl time.Duration) (bool, string, error)
	RenewLease(ctx context.Context, ttl time.Duration) (bool, error)
	ReleaseLease(ctx context.Context) error
	EnsureMeta(ctx context.Context, jobHash, phase, position, snapshotID string) error
	SetPhase(ctx context.Context, phase string) error
	State(ctx context.Context) (phase, position string, err error)
	DoneChunks(ctx context.Context) (map[string]map[int]bool, error)
	ResetChunks(ctx context.Context) error
	DidCreateTable(ctx context.Context, ref source.TableRef) (bool, error)
	TruncateTable(ctx context.Context, ref source.TableRef) error
	RecordObject(ctx context.Context, name, kind string) error
	MarkObjectDropped(ctx context.Context, name string) error
	Count(ctx context.Context, ref source.TableRef) (int64, error)
}

// 编译期保证：两个目标端都满足状态能力接口（签名不一致会直接编译失败）。
var (
	_ stateCapable = (*pgsink.Sink)(nil)
	_ stateCapable = (*mysqlsink.Sink)(nil)
)

// newTargetSink 在开跑前把目标端定下来；不支持的组合在这里就报清楚（四段式）。
func newTargetSink(ctx context.Context, r *runner) (targetSink, error) {
	switch r.tgtEP.Kind {
	case dsn.KindPostgres:
		s, err := r.openSink(ctx)
		if err != nil {
			return nil, err
		}
		return s, nil
	case dsn.KindMySQL:
		s, err := mysqlsink.Open(ctx, mysqlsink.Options{
			DSN:        r.tgtEP,
			JobKey:     r.job.JobKey,
			Name:       r.job.Name,
			InstanceID: r.instID,
			SourceKind: catalogEngine(r.srcEP.Kind),
			OnExists:   mysqlsink.OnExists(r.doc.Target.OnExists),
		})
		if err != nil {
			return nil, fmt.Errorf("连不上 MySQL 目标端（%s）: %w", r.tgtEP.Redacted(), err)
		}
		return s, nil
	}
	return nil, fmt.Errorf("目标端类型 %q 还不支持：现在能做 PostgreSQL 与 MySQL 两种目标端。"+
		"你现在能做什么：把这条连接的 kind 改成 postgres/mysql，或换一个支持的目标库。风险：没有（什么都没动）",
		r.tgtEP.Kind)
}

// closeTarget 是 teardown 的统一出口（关连接失败不盖住原错误）。
func closeTarget(s targetSink) {
	if s != nil {
		_ = s.Close()
	}
}

// reachedDone 表示「通用路径已经自己收尾到 done」（drain 成功），调用方不必再 fail。
type reachedDone struct{}

func (*reachedDone) Error() string { return "本轮已完成" }

// prepareGeneric 给非 PG 目标端做准备：只构造目标端与源端连接。
// 不做租约、不写 _jl_meta —— 那些是目标端状态表的能力，MySQL 侧还没实现（下次做）。
func (r *runner) prepareGeneric(ctx context.Context) error {
	gs, err := newTargetSink(ctx, r)
	if err != nil {
		return err
	}
	r.gsink = gs
	if r.tgtEP.Kind == dsn.KindMySQL {
		r.note(ctx, "目标端是 MySQL：本版只支持全量（不建同步书签、不写位点、不记块进度）。"+
			"崩溃/中断后重跑会**从头重搬**；幂等靠目标表有主键/唯一键（没有唯一键会在写入前直接报错，不会插重复），"+
			"所以恢复时间按「整表重搬」估，而不是 PG 那边的「从断点续搬」")
	}
	src, err := r.openSource(ctx)
	if err != nil {
		return err
	}
	r.src = src
	return nil
}

// ── 非 PostgreSQL 目标端：本版只做「全量」 ──────────────────────

// executeGenericFullCopy 跑一次全量到非 PG 目标端（MySQL）。
//
// 本次范围只到全量：
//   - **虽然目标端（MySQL）已经有状态表能力（#50），本版这条路径仍然不 acquire 租约、
//     不写位点、不记块进度**——「能力有了」和「这条路径用了」是两件事，
//     别看到 `stateCapable` 就以为这里在用它；
//   - 增量（Apply 的流式路径 / interval 的水位）明确报「本版仅支持全量」，不静默降级。
//
// 写入走契约里的 `sink.Apply`：把每一页行包成 EvInsert 事件（带 Relation=这一版列定义），
// 目标端按自己的方言 upsert。这样不需要 mysqlsink 提供任何新方法，也不需要引擎知道 MySQL 语法。
func (r *runner) executeGenericFullCopy(ctx context.Context, res *Result) error {
	if err := r.phaseTo(ctx, state.Prepare); err != nil {
		return err
	}
	// 只把**选中的表**交给目标端（传整个 catalog 会去建任务范围外的表），
	// 并且把 schema 换成目标端的：PG catalog 里每张表的 Schema 是 "public"，
	// 直接拿去 MySQL 就成了 `public`.`t`（既没权限也不存在）。
	actions, err := r.gsink.EnsureSchema(ctx, r.targetCatalog())
	if err != nil {
		return fmt.Errorf("在 %s 目标端建表失败: %w", r.tgtEP.Kind, err)
	}
	for _, a := range actions {
		r.emit(ctx, control.Event{Type: "note", Message: a.What})
	}
	if err := r.incrementalGate(); err != nil {
		return err
	}

	if err := r.phaseTo(ctx, state.Snapshot); err != nil {
		return err
	}
	chunkRows := r.chunkRows()
	_, err = r.src.SnapshotChunks(ctx, source.SnapshotOpt{ChunkRows: chunkRows, Conns: 1},
		func(c context.Context, ch pgsource.Chunk) error {
			rows := ch.Rows
			// 一页可能十万行：按小批喂给 Apply，避免一条超大 INSERT 撞 max_allowed_packet。
			const perBatch = 500
			ref := r.targetRef(ch.Table)
			rel := r.relationOf(ch.Table)
			for i := 0; i < len(rows); i += perBatch {
				end := i + perBatch
				if end > len(rows) {
					end = len(rows)
				}
				evs := make([]source.Event, 0, end-i)
				for _, row := range rows[i:end] {
					vals, verr := toValues(row)
					if verr != nil {
						return fmt.Errorf("转换 %s 的一行失败（宁可停下也不写错数据）: %w", ch.Table.Table, verr)
					}
					evs = append(evs, source.Event{
						Kind:     source.EvInsert,
						Table:    ref,
						Relation: rel,
						New:      source.Row{Table: ref, Values: vals},
					})
				}
				// 位点留空：非 PG 目标端本次不写位点（状态表未实现），不是「忘了传」。
				if err := r.gsink.Apply(c, evs, nil); err != nil {
					return err
				}
			}
			r.rowsCopied += int64(len(rows))
			r.progress(c, fmt.Sprintf("全量：%s 第 %d 块，%d 行", ch.Table.Table, ch.No, len(rows)))
			return nil
		})
	if err != nil {
		return err
	}
	r.emit(ctx, control.Event{Type: "note",
		Message: fmt.Sprintf("全量完成：%d 行（目标端 %s，本版未写位点/租约，增量下一版）",
			r.rowsCopied, r.tgtEP.Kind)})
	res.Note = fmt.Sprintf("目标端 %s：本次只搬全量（增量下一版）", r.tgtEP.Kind)
	if err := r.drain(ctx, ExitOK, "全量完成（"+r.tgtEP.Kind+" 目标端）"); err != nil {
		return err
	}
	return &reachedDone{}
}

// targetCatalog 是交给目标端的表定义：保留选中的表，schema 换成目标端自己的。
//
// 为什么必须换：源端 catalog 里每张表都带 `Schema="public"`；MySQL 目标端把它当
// schema（库）名去建表，于是变成 `public`.`t` —— 用户看到的是「CREATE 权限被拒」，
// 实际是库名不对。目标端 schema 为空时 mysqlsink 会回落到连接里的库。
func (r *runner) targetCatalog() source.Catalog {
	targetSchema := strings.TrimSpace(r.doc.Target.Schema)
	out := source.Catalog{}
	for _, t := range r.selectedTables() {
		cp := t
		cp.Schema = targetSchema // 空 = 用连接里的库
		out.Tables = append(out.Tables, cp)
	}
	return out
}

// targetRef 把源端表引用换成目标端引用（schema 换成目标端的）。
// 事件里的 Table 会被目标端直接拼进 SQL，必须是目标端的库名。
func (r *runner) targetRef(ref source.TableRef) source.TableRef {
	return source.TableRef{Schema: strings.TrimSpace(r.doc.Target.Schema), Table: ref.Table}
}

// incrementalGate 决定「这个目标端能不能跑增量」。
//
// 两件事分开说，因为它们对用户意味着完全不同的下一步：
//   - 目标端**没有**状态能力（租约/位点/块记账）→ 这是接线没到，只能跑全量；
//   - 目标端有状态能力、但引擎的非 PG 增量路径还没接 → 这是本版范围没到。
//
// 无论哪种都**显式拒绝**，不静默降级成「只搬一次」。
func (r *runner) incrementalGate() error {
	mode := r.mode()
	if mode == "once" || mode == "full_interval" {
		return nil
	}
	if r.tgtEP.Kind == dsn.KindPostgres {
		return nil // PG 目标端的增量路径是完整实现的
	}
	_, hasState := r.gsink.(stateCapable)
	if !hasState {
		return &notImplementedError{fmt.Sprintf(
			"目标端（%s）还没有租约/位点能力，只能跑全量：没有位点就没法知道「上次搬到哪」，"+
				"跑增量会重复或漏数据。你现在能做什么：1) 把 mode.frequency 改成 once 先搬存量；"+
				"2) 需要周期性重跑就用 full_interval；3) 或者换 PostgreSQL 目标端跑真增量。"+
				"风险：没有（本次没有写入任何数据）", r.tgtEP.Kind)}
	}
	// 注意：**不要建议用户改 interval**——非 PG 目标端在 Run 里提前返回、走
	// executeGenericFullCopy，而它第一步就是本函数，interval 会撞回这同一句拒绝
	//（engine.go 的 interval 分派对 MySQL 不可达）。给的建议必须是走得通的。
	return &notImplementedError{fmt.Sprintf(
		"目标端（%s）的状态表已经就绪，但本版引擎的非 PG 增量路径还没接上：目前只搬全量。"+
			"你现在能做什么：1) 把 mode.frequency 改成 once 先搬存量；"+
			"2) 需要周期性重跑就用 full_interval（每轮整表重搬，明确接受这个代价）。"+
			"风险：没有（本次没有写入任何数据）", r.tgtEP.Kind)}
}

// relationOf 给出这张表这一版的列定义（引擎据此告诉目标端「值按什么列序」）。
func (r *runner) relationOf(ref source.TableRef) *source.Table {
	for i := range r.cat.Tables {
		if r.cat.Tables[i].Schema == ref.Schema && r.cat.Tables[i].Table == ref.Table {
			return &r.cat.Tables[i]
		}
	}
	return nil
}

// toValues 把驱动原生值（pgx 已解好的 any）转成契约里的 source.Value。
// 目标端只按 Kind 取值，所以这里不需要什么聪明处理；认不出的类型按字符串兜底
// （mysqlsink 的 driverValue 会按 KindString 传，最差是让数据库自己转）。
func toValues(row []any) ([]source.Value, error) {
	out := make([]source.Value, len(row))
	for i, v := range row {
		sv, err := toValue(v)
		if err != nil {
			return nil, fmt.Errorf("第 %d 列：%w", i+1, err)
		}
		out[i] = sv
	}
	return out, nil
}

func toValue(v any) (source.Value, error) {
	switch x := v.(type) {
	case nil:
		return source.Value{Kind: source.KindNull}, nil
	case int16:
		return source.Value{Kind: source.KindInt, I: int64(x)}, nil
	case int32:
		return source.Value{Kind: source.KindInt, I: int64(x)}, nil
	case int64:
		return source.Value{Kind: source.KindInt, I: x}, nil
	case float32:
		return source.Value{Kind: source.KindFloat, F: float64(x)}, nil
	case float64:
		return source.Value{Kind: source.KindFloat, F: x}, nil
	case bool:
		b := int64(0)
		if x {
			b = 1
		}
		return source.Value{Kind: source.KindBool, I: b}, nil
	case string:
		return source.Value{Kind: source.KindString, S: x}, nil
	case []byte:
		return source.Value{Kind: source.KindBytes, B: x}, nil
	case time.Time:
		return source.Value{Kind: source.KindTime, T: x}, nil
	// 注意：**`[]any` 不在这份白名单里** —— 复合类型可能被 pgx 解成 `[]any`，
	// 无条件 marshal 会写出 `["a",1]` 这种丢了字段名的位置数组（正是今天修掉的形状换个入口）。
	// 它落到下面的反射分支：元素必须实现 json.Marshaler，否则响亮失败。
	case []int16, []int32, []int64, []float32, []float64, []string, []bool, []time.Time:
		// pgx 把 SQL 数组解成原生切片，**不实现 driver.Valuer**——落到 fmt.Sprint 会写出
		// "[1 2 3]"，而 typemap 把数组映成 MySQL 的 json 列，于是错误要等到插第一行时才现形
		// （"Invalid JSON text"，离真实原因很远）。这里显式 json.Marshal，落成合法 JSON。
		if b, err := json.Marshal(x); err == nil {
			return source.Value{Kind: source.KindJSON, S: string(b)}, nil
		}
		return source.Value{Kind: source.KindString, S: fmt.Sprint(x)}, nil
	case driver.Valuer:
		// pgx 对 numeric/uuid 等返回自己的类型（pgtype.*），它们实现了 driver.Valuer；
		// **不能 fmt.Sprint** —— 那会把 numeric 变成 "{1 -2 false finite true}" 这种结构体打印，
		// 目标端报「Incorrect decimal value」，离真相很远。
		dv, err := x.Value()
		if err != nil {
			return source.Value{}, fmt.Errorf("%T 取值失败（driver.Valuer）: %w", v, err)
		}
		return toValue(dv)
	default:
		// 其它元素类型的数组（pgx 对 interval[]/自定义类型数组也给原生切片）：
		// **只有元素实现了 json.Marshaler 才敢 marshal**。
		// 反例（Owl 实测）：`[]pgtype.Interval` 没有 MarshalJSON，json.Marshal 会写出
		// `[{"Microseconds":100,"Days":1,"Months":0,"Valid":true}]` —— 合法 JSON，内容是
		// 结构体字段。它会被当合法 JSON 写进 json 列，**全程不报错，md5 也发现不了**
		// （因为它一致地写错了）。所以这里宁可响亮失败，也不静默写错。
		rv := reflect.ValueOf(v)
		if k := rv.Kind(); k == reflect.Slice || k == reflect.Array {
			if rv.Len() == 0 {
				return source.Value{Kind: source.KindJSON, S: "[]"}, nil
			}
			elem := rv.Type().Elem()
			if !elem.Implements(reflect.TypeOf((*json.Marshaler)(nil)).Elem()) {
				return source.Value{}, fmt.Errorf(
					"%s 是数组，但元素类型 %s 没有 JSON 表示（没实现 json.Marshaler）："+
						"直接序列化会写出结构体字段（看起来是合法 JSON，实际写错数据）。"+
						"要么把该列排除出同步范围，要么等我们为它加专门映射", elem, elem)
			}
			b, err := json.Marshal(v)
			if err != nil {
				return source.Value{}, fmt.Errorf("%s 数组序列化失败: %w", elem, err)
			}
			return source.Value{Kind: source.KindJSON, S: string(b)}, nil
		}
		return source.Value{}, fmt.Errorf(
			"不认识的 %T 值：没有 JSON 表示也没有可用的转换，宁可不写也不写错。"+
				"请把这一列排除出同步范围，或把它反馈给我们补映射", v)
	}
}

// catalogEngine 把连接类型映射到类型字典的引擎名（类型映射用）。
func catalogEngine(kind string) catalog.Engine {
	if kind == dsn.KindMySQL {
		return catalog.MySQL
	}
	return catalog.Postgres
}
