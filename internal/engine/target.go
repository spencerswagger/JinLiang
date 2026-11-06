package engine

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"
	"time"

	"github.com/spencerswagger/jinliang/internal/catalog"
	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/mysqlsink"
	"github.com/spencerswagger/jinliang/internal/pgsource"
	"github.com/spencerswagger/jinliang/internal/sink"
	"github.com/spencerswagger/jinliang/internal/source"
	"github.com/spencerswagger/jinliang/internal/state"
)

// 目标端抽象：**在构造阶段按连接类型确定**，跑起来之后引擎只认接口。
//
// 为什么不写 `if target.Kind == "mysql"`：那样的分支会长在所有用到目标端的
// 地方（全量、增量、校验、清理各一次），把 B2/B3 的边界焊死在引擎里。
// 这里只有一个工厂（newTargetSink），之后全是接口调用 + 能力探测（bulkWriter）。
type targetSink interface {
	sink.Sink
	// Close 两种 sink 的签名不同（pgsink.Close() / mysqlsink.Close() error），
	// 这里只要求「能关」：错误在 teardown 里忽略（关连接失败不该盖住原错误）。
	Close() error
}

// bulkWriter 是可选的「整块写入」快路径（PG 用 COPY；MySQL 走契约的 Apply）。
// 用类型断言做**能力探测**，不按连接类型分支——新目标端实现了就自动用上。
type bulkWriter interface {
	BulkAppend(ctx context.Context, ref source.TableRef, chunkNo int, rows [][]any, pos string) error
}

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
		r.note(ctx, "目标端是 MySQL：本版只支持全量（不建同步书签、不写位点），增量下一版")
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
//   - 目标端状态表（_jl_lease / _jl_meta 的位点与阶段）还没实现，所以**不租约、不记位点**；
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
	if r.mode() != "once" && r.mode() != "full_interval" {
		return &notImplementedError{fmt.Sprintf(
			"目标端是 %s 时本版只支持全量（mode.frequency 用 once 或 full_interval）："+
				"位点/租约状态表与增量写入还没接上。你现在能做什么："+
				"1) 把 mode.frequency 改成 once 先搬存量；2) 或等下一版增量。"+
				"风险：没有（本次没有写入任何数据）", r.tgtEP.Kind)}
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
					evs = append(evs, source.Event{
						Kind:     source.EvInsert,
						Table:    ref,
						Relation: rel,
						New:      source.Row{Table: ref, Values: toValues(row)},
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
func toValues(row []any) []source.Value {
	out := make([]source.Value, len(row))
	for i, v := range row {
		out[i] = toValue(v)
	}
	return out
}

func toValue(v any) source.Value {
	switch x := v.(type) {
	case nil:
		return source.Value{Kind: source.KindNull}
	case int16:
		return source.Value{Kind: source.KindInt, I: int64(x)}
	case int32:
		return source.Value{Kind: source.KindInt, I: int64(x)}
	case int64:
		return source.Value{Kind: source.KindInt, I: x}
	case float32:
		return source.Value{Kind: source.KindFloat, F: float64(x)}
	case float64:
		return source.Value{Kind: source.KindFloat, F: x}
	case bool:
		b := int64(0)
		if x {
			b = 1
		}
		return source.Value{Kind: source.KindBool, I: b}
	case string:
		return source.Value{Kind: source.KindString, S: x}
	case []byte:
		return source.Value{Kind: source.KindBytes, B: x}
	case time.Time:
		return source.Value{Kind: source.KindTime, T: x}
	case driver.Valuer:
		// pgx 对 numeric/uuid/array/interval 等返回自己的类型（pgtype.*），
		// 它们都实现了 driver.Valuer；**不能 fmt.Sprint** —— 那会把 numeric 变成
		// "{1 -2 false finite true}" 这种结构体打印，目标端报「Incorrect decimal value」。
		if dv, err := x.Value(); err == nil {
			return toValue(dv)
		}
		return source.Value{Kind: source.KindString, S: fmt.Sprint(x)}
	default:
		return source.Value{Kind: source.KindString, S: fmt.Sprint(x)}
	}
}

// bulkAppend 走「整块写入」快路径；目标端没实现就退回契约的 Apply（一批 500 行）。
//
// 这是**能力探测**，不是类型分支：pgsink 实现了 bulkWriter 就用 COPY，
// MySQL 目标端没实现就走 Apply，行为正确性由契约保证、速度差一截而已。
func (r *runner) bulkAppend(ctx context.Context, ref source.TableRef, chunkNo int,
	rows [][]any, pos string) error {
	if bw, ok := r.gsink.(bulkWriter); ok {
		return bw.BulkAppend(ctx, ref, chunkNo, rows, pos)
	}
	const perBatch = 500
	for i := 0; i < len(rows); i += perBatch {
		end := i + perBatch
		if end > len(rows) {
			end = len(rows)
		}
		evs := make([]source.Event, 0, end-i)
		tref := r.targetRef(ref)
		rel := r.relationOf(ref)
		for _, row := range rows[i:end] {
			evs = append(evs, source.Event{
				Kind: source.EvInsert, Table: tref,
				Relation: rel,
				New:      source.Row{Table: tref, Values: toValues(row)},
			})
		}
		if pos == "" {
			if err := r.gsink.Apply(ctx, evs, nil); err != nil {
				return err
			}
			continue
		}
		p, err := pgsource.ParsePosition(pos)
		if err != nil {
			return err
		}
		if err := r.gsink.Apply(ctx, evs, p); err != nil {
			return err
		}
	}
	return nil
}

// catalogEngine 把连接类型映射到类型字典的引擎名（类型映射用）。
func catalogEngine(kind string) catalog.Engine {
	if kind == dsn.KindMySQL {
		return catalog.MySQL
	}
	return catalog.Postgres
}
