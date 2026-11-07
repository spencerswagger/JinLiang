package engine

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
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
	// Position 读回已落库的位点（续跑与 ack 前校验都要它）。
	// 引擎一直在调 `r.sink.Position`，但之前只是靠具体类型——写进接口才有编译期保证。
	Position(ctx context.Context) (string, error)
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
	// 状态表能力（租约/位点/块/对象登记）：MySQL 侧 #50 已具备，PG 的 pgsink 也满足。
	// 全量路径不需要它；增量路径必须有，缺了在 incrementalGate 里明确拒绝。
	if st, ok := gs.(stateCapable); ok {
		r.state = st
	}
	// 增量支持取决于「有没有状态表能力」而不是目标端类型：MySQL 侧 #50 已具备，
	// 所以首次搬完全量后会续增量；once / full_interval 仍然只搬一次。
	if r.state == nil {
		r.note(ctx, "目标端没有租约/位点能力：本版只能搬一次全量（崩溃后重跑会从头重搬）")
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
	// 状态表就绪 → 允许增量。非 PG 目标端的增量走 executeGenericIncremental：
	// 租约 + 位点都在目标端状态表里，写路径只走契约的 sink.Apply。
	return nil
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

// executeGenericIncremental 跑「非 PG 目标端」的增量：全量 + 持续流。
//
// 与 PG 路径的差别只有一处：**目标端能力不一样**。写路径仍然只走契约的
// `sink.Apply`（目标端自己决定 upsert 方言），租约/位点在目标端状态表里。
//
// 下单顺序（与 PG 路径同一套安全规则）：
//  1. 先拿租约（拿不到就停，不写任何数据）；
//  2. **先取起点位点、再做全量**：全量期间的变更会在后面被重放，靠 upsert 幂等吸收；
//     反过来（先搬后取位点）会漏掉整个搬期间的变化；
//  3. 全量按页喂 `Apply`（每 500 行一批，避免一条超大 INSERT 撞 max_allowed_packet）；
//  4. 流式：batcher 每批 `Apply(数据 + 位点)`，成功后**先过 ackGuard 再 ack**；
//  5. 收尾：flush → 写 done 阶段 → 释放租约。
func (r *runner) executeGenericIncremental(ctx context.Context, res *Result) error {
	if r.state == nil {
		return &notImplementedError{"目标端没有租约/位点能力，只能跑全量"}
	}
	if err := r.phaseTo(ctx, state.Prepare); err != nil {
		return err
	}
	ok, why, err := r.state.AcquireLease(ctx, r.leaseTTL)
	if err != nil {
		return fmt.Errorf("拿租约失败: %w", err)
	}
	if !ok {
		return &notImplementedError{fmt.Sprintf(
			"另一个进程正在跑这个任务（%s）：等它结束，或在那边先停掉。"+
				"你现在能做什么：1) 等一会儿重试；2) 确认那个进程是否还活着（`jinliang status`）；"+
				"3) 确实要接管就等租约过期（30 秒）再跑。风险：强抢会让两个进程同时写同一张目标表", why)}
	}
	defer func() { _ = r.state.ReleaseLease(context.WithoutCancel(ctx)) }()

	if err := r.state.EnsureMeta(ctx, hashOf(r.doc), string(state.Prepare), "", ""); err != nil {
		return fmt.Errorf("写入任务状态行失败: %w", err)
	}

	// 续跑：目标端已有位点就从那儿接；没有就是首次，要搬全量。
	raw, err := r.state.Position(ctx)
	if err != nil {
		return fmt.Errorf("读目标端位点失败: %w", err)
	}
	fresh := raw == ""

	// **只有首次才建表/按 on_exists 处理**：续跑时再走一遍 `recreate` 会把目标表
	// 清空（我 e2e 第一次就是在这里把 20000 行清成 0 的）。续跑只要求表在。
	if fresh {
		actions, err := r.gsink.EnsureSchema(ctx, r.targetCatalog())
		if err != nil {
			return fmt.Errorf("在 %s 目标端建表失败: %w", r.tgtEP.Kind, err)
		}
		for _, a := range actions {
			r.emit(ctx, control.Event{Type: "note", Message: a.What})
		}
	}

	// **源端也要准备**：PG 的逻辑复制需要书签（slot）+ 发布（publication）。
	// 非 PG 目标端的增量同样是 PG 源在供流，所以这一步与 PG 路径一样不能省
	//（省了会拿到「replication slot ... does not exist」）。
	prep, err := r.src.Prepare(ctx, pgsource.PrepareOptions{Snapshot: fresh})
	if err != nil {
		return fmt.Errorf("准备源端对象失败: %w", err)
	}
	for _, a := range prep.Actions {
		r.emit(ctx, control.Event{Type: "note", Message: a.What})
	}
	r.snapshotName = prep.SnapshotName
	r.consistentPoint = prep.ConsistentPoint
	r.reusedSlot = !fresh
	r.src.SetSnapshotName(prep.SnapshotName)

	from := prep.ConsistentPoint
	if !fresh {
		if from, err = r.resumePosition(ctx); err != nil {
			return err
		}
		// 续跑前确认目标表还在：`_jl_meta.position` 说「搬过」，但表被删了/换库了，
		// 继续流会把变更写进不存在的表——宁可停下并说清，也不静默空转。
		for _, ref := range r.tables {
			tref := r.targetRef(ref)
			if _, cerr := r.state.Count(ctx, tref); cerr != nil {
				return fmt.Errorf("目标表 %s 不存在但位点还在（%s）：状态表与目标表不一致。"+
					"你能做的：① 用 `jinliang run --job %s --resync` 重新初始化；"+
					"② 或确认目标库是不是被切换/删过。风险：直接继续流会把变更写进不存在的表，看起来“在跑”实际什么都没写",
					qualifiedTarget(tref), from.String(), r.job.Name)
			}
		}
	}
	r.mu.Lock()
	r.lastPos = from
	r.mu.Unlock()

	// 首次跑（目标端没有位点）才搬全量；有位置说明上次已经搬过，直接续增量。
	if fresh {
		if err := r.phaseTo(ctx, state.Snapshot); err != nil {
			return err
		}
		if err := r.copyAllGeneric(ctx); err != nil {
			return err
		}
		r.src.EndSnapshot()
		// 全量结束把**起点位点**写进目标端：不写的话下次跑会认为是首次、
		// 又整表重搬一遍（我 e2e 第一次就是这么重复搬了 20003 行）。
		// 用空批 Apply 写位点，与 batcher.AdvanceTo 是同一个机制，不需要新方法。
		if err := r.gsink.Apply(ctx, nil, from); err != nil {
			return fmt.Errorf("写入全量起点位点失败: %w", err)
		}
		// 状态机不允许 snapshot → streaming 直跳：与 PG 路径一样先到 catchup（追赶）。
		if err := r.phaseTo(ctx, state.Catchup); err != nil {
			return err
		}
	}
	if r.phase == state.Snapshot || r.phase == state.Catchup {
		if err := r.phaseTo(ctx, state.Streaming); err != nil {
			return err
		}
	}

	b := newBatcher(r.gsink, r.src, r.doc, r.batchRows(), r.batchDelay(), from, func(rows int64, pos pgsource.Position) {
		r.rowsStream += rows
		r.mu.Lock()
		r.lastPos = pos
		r.mu.Unlock()
	})
	b.onFlush = func(applied int64, pos pgsource.Position) {
		r.progress(ctx, fmt.Sprintf("增量：已应用 %d 行，位点 %s", applied, pos.String()))
	}
	// **staged_phases 才需要 ack 前校验**：MySQL 目标端含 DDL 的批次有三个提交点，
	// 「Apply 返回 nil」不等于「位点已落盘」——读回确认，才允许向 PG 源 ack（清 WAL）。
	if d, ok := r.gsink.(interface{ PositionDurability() string }); ok && d.PositionDurability() == "staged_phases" {
		b.ackGuard = func(c context.Context, pos pgsource.Position) error {
			got, err := r.state.Position(c)
			if err != nil {
				return fmt.Errorf("ack 前读回位点失败: %w", err)
			}
			if got != pos.String() {
				return fmt.Errorf("拒绝向源端确认位点 %s：目标端实际是 %q——位点还没落盘就开始 ack，"+
					"源端会清掉还能重放的流水。风险：继续跑会永久缺数据；已停下，什么都没丢",
					pos.String(), got)
			}
			return nil
		}
	}

	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	go b.flushLoop(streamCtx)

	var stopHit, idleHit atomic.Bool
	if r.opt.StopAfter > 0 {
		go func() {
			select {
			case <-streamCtx.Done():
			case <-time.After(r.opt.StopAfter):
				stopHit.Store(true)
				cancelStream()
			}
		}()
	}
	if r.opt.IdleAfter > 0 {
		go func() {
			t := time.NewTicker(300 * time.Millisecond)
			defer t.Stop()
			idleSince := time.Now()
			for {
				select {
				case <-streamCtx.Done():
					return
				case <-t.C:
					rows, inTx := b.Pending()
					if rows == 0 && !inTx {
						if time.Since(idleSince) > r.opt.IdleAfter {
							// 追平后收尾是**成功完成**（不是用户按停），历史页别记成 stopped。
							idleHit.Store(true)
							stopHit.Store(true)
							cancelStream()
							return
						}
					} else {
						idleSince = time.Now()
					}
				}
			}
		}()
	}

	streamErr := r.src.Stream(streamCtx, from, func(c context.Context, ev source.Event) error {
		if stopHit.Load() {
			return &stoppedError{msg: fmt.Sprintf("到达本次运行的时间上限（%s），已收尾", r.opt.StopAfter)}
		}
		// **事件里的表名是源端 schema（public.xxx），必须换成目标端的**：
		// 不换的话 mysqlsink 会去查 `public`.`my_orders`（不存在），报「没有唯一键」——
		// 这条错误看起来像目标表问题，实际是引擎没做 schema 映射。
		ev.Table = r.targetRef(ev.Table)
		if ev.Relation != nil {
			rel := *ev.Relation
			rel.TableRef = ev.Table
			ev.Relation = &rel
		}
		// 时间类列：PG 发的是带时区后缀的文本，MySQL 的 datetime 不接受，先转成 time.Time。
		normalizeTemporal(&ev)
		return b.Add(c, ev)
	})
	cancelStream()
	if err := b.Flush(context.Background()); err != nil {
		return err
	}
	if streamErr != nil {
		var stopped *stoppedError
		code := ExitStopped
		if idleHit.Load() {
			code = ExitOK // 追平后收尾 = 正常完成
		}
		if errors.As(streamErr, &stopped) {
			return r.finishGeneric(context.Background(), code)
		}
		if errors.Is(streamErr, context.Canceled) || isUserStop(ctx, streamErr) {
			return r.finishGeneric(context.Background(), code)
		}
		return streamErr
	}
	return r.finishGeneric(context.Background(), ExitOK)
}

// finishGeneric 收尾：状态机只允许 streaming → draining → done，不能直跳 done。
//
// 注意必须走 `r.drain`：它除了改阶段，还会 `Store.FinishRun` 写 ended_at +
// finish_reason。少了这一步，run 行会永远「未结束」——下一次跑会被自己的残留
// 挡成 409（我第一次跑 e2e 就是这么撞上的）。drain 里 `r.sink` 是 nil 时安全跳过
// `SetPhase`，MySQL 目标端的阶段由 `phaseTo` 通过 `r.state` 写。
func (r *runner) finishGeneric(ctx context.Context, code int) error {
	msg := "已完成"
	if code == ExitStopped {
		msg = "已手动停止"
	}
	if err := r.drain(ctx, code, msg); err != nil {
		return err
	}
	return nil
}

// resumePosition 读目标端已落库的位点；没有就取「此刻」作为起点（先取位点再做全量）。
func (r *runner) resumePosition(ctx context.Context) (pgsource.Position, error) {
	raw, err := r.state.Position(ctx)
	if err != nil {
		return pgsource.Position{}, fmt.Errorf("读目标端位点失败: %w", err)
	}
	if raw == "" {
		return r.src.CurrentLSN(ctx)
	}
	parsed, err := r.src.ParsePosition(raw)
	if err != nil {
		return pgsource.Position{}, err
	}
	if err := assertPositionKind(parsed, r.src.Kind()); err != nil {
		return pgsource.Position{}, err
	}
	p, ok := parsed.(pgsource.Position)
	if !ok {
		return pgsource.Position{}, fmt.Errorf("%w：这个源给出的位点类型是 %T", source.ErrPositionMalformed, parsed)
	}
	return p, nil
}

// copyAllGeneric 把选中的表按页搬到非 PG 目标端（不写位点：全量期间的变更靠后续重放吸收）。
func (r *runner) copyAllGeneric(ctx context.Context) error {
	chunkRows := r.chunkRows()
	_, err := r.src.SnapshotChunks(ctx, source.SnapshotOpt{ChunkRows: chunkRows, Conns: 1},
		func(c context.Context, ch pgsource.Chunk) error {
			const perBatch = 500
			ref := r.targetRef(ch.Table)
			rel := r.relationOf(ch.Table)
			for i := 0; i < len(ch.Rows); i += perBatch {
				end := i + perBatch
				if end > len(ch.Rows) {
					end = len(ch.Rows)
				}
				evs := make([]source.Event, 0, end-i)
				for _, row := range ch.Rows[i:end] {
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
				if err := r.gsink.Apply(c, evs, nil); err != nil {
					return err
				}
			}
			r.rowsCopied += int64(len(ch.Rows))
			r.progress(c, fmt.Sprintf("全量：%s 第 %d 块，%d 行", ch.Table.Table, ch.No, len(ch.Rows)))
			return nil
		})
	return err
}

// qualifiedTarget 只用于错误信息里显示表名。
func qualifiedTarget(ref source.TableRef) string {
	if ref.Schema == "" {
		return ref.Table
	}
	return ref.Schema + "." + ref.Table
}

// normalizeTemporal 把源端「时间类列的文本值」转成 time.Time。
//
// 为什么需要：PG 的逻辑复制（pgoutput）把 timestamptz 发成文本
// `2026-11-07 03:17:34.117788+00`，而 MySQL 的 datetime 列不接受带时区后缀的字面量
// （Error 1292）。全量走的是驱动值（已经是 time.Time），所以只有增量这条路径会撞上——
// 转成 time.Time 后由驱动按列类型格式化，两端才对得上。
func normalizeTemporal(ev *source.Event) {
	if ev.Relation == nil || len(ev.New.Values) == 0 {
		return
	}
	for i, col := range ev.Relation.Columns {
		if i >= len(ev.New.Values) {
			break
		}
		if !isTemporalType(col.Type) {
			continue
		}
		v := ev.New.Values[i]
		if v.Kind != source.KindString || v.S == "" {
			continue
		}
		if t, ok := parsePGTime(v.S); ok {
			ev.New.Values[i] = source.Value{Kind: source.KindTime, T: t}
		}
	}
}

func isTemporalType(t string) bool {
	lt := strings.ToLower(t)
	return strings.HasPrefix(lt, "timestamp") || lt == "date" || strings.HasPrefix(lt, "time ")
}

// parsePGTime 接受 pgoutput 的时间文本；解析不出来就返回 false（保持原值，由目标端报错）。
func parsePGTime(s string) (time.Time, bool) {
	layouts := []string{
		"2006-01-02 15:04:05.999999-07",
		"2006-01-02 15:04:05.999999-07:00",
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
		time.RFC3339Nano,
	}
	for _, l := range layouts {
		if t, err := time.Parse(l, s); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}
