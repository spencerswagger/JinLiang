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
	// ClearProgress 清掉权威位点/水位/快照 ID（`--resync` 的唯一清点写法）。
	ClearProgress(ctx context.Context) error
	DoneChunks(ctx context.Context) (map[string]map[int]bool, error)
	// 这里故意**没有**「清全部块标记」这种能力：清标记与推位点必须同一个事务
	// （pgsink.StartFreshCopy），单独暴露一个「只清标记」的方法等于把必改 1 的窗口
	// 重新交回给调用方。
	DidCreateTable(ctx context.Context, ref source.TableRef) (bool, error)
	// 清空目标表不走单独的能力：清数据与清块标记必须在同一个事务里
	// （pgsink.ResetTableChunks），否则崩在中间会让「空的表 + 已搬完的块标记」同时存在。
	RecordObject(ctx context.Context, name, kind string) error
	MarkObjectDropped(ctx context.Context, name string) error
	Count(ctx context.Context, ref source.TableRef) (int64, error)
	// Position 读回已落库的位点（续跑与 ack 前校验都要它）。
	Position(ctx context.Context) (string, error)
	// TruncateTable 清空一张目标表（重搬前清掉本任务写过的行；所有权由 DidCreateTable 判）。
	TruncateTable(ctx context.Context, ref source.TableRef) error
}

// chunkChronicker 是「块级记账」的可选能力：PG 走 BulkApply（数据 + 块标记 + 位点同事务），
// 非 PG 目标端没有那条快路径，只能在每块写完之后单独记一笔。
//
// 为什么做成可选接口而不是塞进 sink.Sink：契约（sink.Sink）只描述数据写入，
// 块记账是目标端**运行时状态**的一部分；塞进去等于让每个目标端都必须实现一份空壳。
// 没有这个能力的目标端照旧能搬（只是搬完不记账、崩了整轮重来），不是静默降级——
// 引擎在泛化路径里已经用「有没有位点」把增量卡死，这里只影响续跑粒度。
type chunkChronicker interface {
	MarkChunkDone(ctx context.Context, ref source.TableRef, no int, from, to string, rows int64) error
	MarkTableDone(ctx context.Context, ref source.TableRef) error
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
// 租约/位点/阶段由 executeGeneric 通过目标端的**状态表能力**做（不再分两条路）。
func (r *runner) prepareGeneric(ctx context.Context) error {
	gs, err := newTargetSink(ctx, r)
	if err != nil {
		return err
	}
	r.gsink = gs
	// 目标端实现了暂停存储（六个方法）就绑上：MySQL 目标端从此也能「只停一张表」。
	// 没实现则为 nil，写路径原样透传、遇结构不一致按失败处理（不许静默跳过）。
	if reg, ok := gs.(pauseRegistry); ok {
		r.pauseReg = reg
		if err := reg.LoadPausedTables(ctx); err != nil {
			return err
		}
		if err := r.loadPausedSet(ctx); err != nil {
			return err
		}
	}
	src, err := r.openSource(ctx)
	if err != nil {
		return err
	}
	r.src = src
	return nil
}

// isIncrementalMode 判断这个频率档位是否需要持续流。
//
// once / full_interval 只搬（每轮整表重搬）；continuous* 才需要租约 + 位点 + 持续流。
func isIncrementalMode(freq string) bool {
	switch freq {
	case "continuous", "continuous+initial":
		return true
	}
	return false
}

// ── 非 PostgreSQL 目标端：一个入口，一套状态机 ──────────────────

// executeGeneric 是非 PG 目标端的**唯一**入口：全量、增量、周期重搬都从这里走。
//
// 为什么只有一个入口：两个入口（只全量 / 增量）会长成两套状态机——各自拿租约、
// 各自写阶段、各自收尾，迟早分叉（「只全量」那条就曾经不拿租约、不写位点，
// 于是崩溃后只能整轮重搬）。现在按模式分派只发生在**这一层**：
//
//	once            → 首次搬全量；已有位点就什么都不搬
//	full_interval   → 每轮整轮重搬（先清本任务建的表，再搬）
//	continuous*     → 首次搬全量，之后持续流；位点与数据同批落库，ack 前过守卫
//	interval        → 明确拒绝（水位轮询只有 PG 目标端实现，见 genericIntervalUnsupported）
//
// 安全顺序与 PG 路径同一套：先拿租约 → 先取起点位点 → 再搬全量 → 搬完把起点位点
// 写进目标端（否则下次会当成首次又重搬一遍）→ 持续流 → 收尾。
func (r *runner) executeGeneric(ctx context.Context, res *Result) error {
	st, err := r.genericState()
	if err != nil {
		return err
	}
	// interval（水位轮询）靠 `_jl_meta.watermark` + 按水位读页，这一版只有 PG 目标端
	// 实现。**不许静默退化成「每轮整表重搬」**——那是把「定时增量」悄悄换成另一种语义。
	if r.mode() == "interval" {
		return r.genericIntervalUnsupported()
	}
	// `--verify` 在非 PG 目标端这一版没有落地：静默无效比报错更糟（用户以为校验过了）。
	// 跨引擎的行级校验走 `jinliang verify`（逐行/分块比对），这里只把话说清楚。
	if r.opt.Verify {
		r.note(ctx, fmt.Sprintf("--verify 在 %s 目标端这一版不生效（跨引擎的行级校验走 `jinliang verify`）："+
			"本次没有做内容校验", r.tgtEP.Kind))
	}
	// **先读「上一轮留下的阶段」**：它是「上一轮崩在哪个阶段」的唯一可重算事实。
	// 表还不存在（首次跑）时读不到，按空处理。
	priorPhase, _, perr := st.State(ctx)
	if perr != nil {
		priorPhase = ""
	}

	// ① 先拿租约：拿不到就停，一个字节都不写。冲突是 E1005（有码，历史页能看见）。
	ok, why, err := st.AcquireLease(ctx, r.leaseTTL)
	if err != nil {
		return fmt.Errorf("取任务租约失败: %w", err)
	}
	if !ok {
		return fmt.Errorf("%w：%s", ErrHeldByOther, why)
	}
	defer func() { _ = st.ReleaseLease(context.WithoutCancel(ctx)) }()
	if why != "" {
		r.note(ctx, why)
	}
	r.leaseOK = true

	if err := st.EnsureMeta(ctx, hashOf(r.doc), string(state.Prepare), "", ""); err != nil {
		return fmt.Errorf("写入任务状态行失败: %w", err)
	}
	// 暂停登记能力（契约 §1.7 最后一条 / §4.5 Data 清单最后一项）：**在跑之前**就把
	// 「这个目标端能不能记暂停」问清楚。不致命——这条任务可能整轮都用不到暂停；
	// 但必须说出来，别等半夜遇到结构不一致时才报一句「补列失败」。
	if pr, ok := r.gsink.(interface{ PauseRegistryReady(context.Context) error }); ok {
		if perr := pr.PauseRegistryReady(ctx); perr != nil {
			r.note(ctx, "这台 MySQL 目标端暂时不能登记按表暂停（遇到结构不一致会整 run 失败）："+perr.Error())
		}
	}
	rawPos, err := st.Position(ctx)
	if err != nil {
		return fmt.Errorf("读目标端位点失败: %w", err)
	}
	fresh := rawPos == ""
	// once 已经有位点：这一轮没有要搬的，直接收尾（不重搬、不流）。
	// full_interval 相反：**每轮都整轮重搬**，这是模式的语义。
	needCopy := fresh || r.mode() == "full_interval"

	// ② 源端准备（PG 源要建书签+发布；MySQL 源要开一致性快照）。必须在取位点之前：
	// 全量的读视图与增量起点是同一瞬间定下来的。
	prep, err := r.src.Prepare(ctx, needCopy)
	if err != nil {
		return fmt.Errorf("准备源端对象失败: %w", err)
	}
	for _, a := range prep.Actions {
		r.emit(ctx, control.Event{Type: "note", Message: a.What})
	}
	defer r.src.EndSnapshot()

	from := prep.ConsistentPoint
	if !fresh {
		from, err = r.storedPosition(ctx)
		if err != nil {
			return err
		}
	}
	if from == nil && needCopy {
		return fmt.Errorf("拿不到源端的一致性位点：没有位点就没法知道「全量期间的变更从哪儿重放」，" +
			"继续搬会让这段时间的增删改永远补不回来。你能做什么：确认源端读位点的权限" +
			"（PG 需要 REPLICATION，MySQL 需要 REPLICATION CLIENT）后重跑；风险：没有（还没写数据）")
	}
	r.mu.Lock()
	r.lastPos = from
	r.mu.Unlock()

	// ③ 建表：**只要这一轮要搬就对齐一次**（首次，或 full_interval 的每一轮）。
	//
	// 为什么 full_interval 也要：源端加了列之后重搬，目标表没有那一列会直接
	// 「unknown column」失败——而 full_interval 的语义就是「每轮等于源端当前状态」。
	// 续跑（不搬）**不重建**：`recreate` 在续跑时会把目标表清空（e2e 里当场把两万行清成 0）。
	if needCopy {
		actions, err := r.gsink.EnsureSchema(ctx, r.targetCatalog())
		if err != nil {
			return fmt.Errorf("在 %s 目标端建表失败: %w", r.tgtEP.Kind, err)
		}
		for _, a := range actions {
			r.emit(ctx, control.Event{Type: "note", Message: a.What})
		}
	}
	if needCopy {
		// 清表判定必须排在**任何阶段写入之前**：phaseTo(Prepare) 会把 `_jl_meta.phase`
		// 改写成 'prepare'，而它正是这一步要读的事实。写早了，保护就变成 one-shot：
		// 第一次重跑硬停（phase 已被这轮改成 prepare）→ 第二次重跑读回 prepare →
		// 走「跳过 + note」、退出码 0，源端停机期删掉的行留在用户表里。
		if err := r.cleanBeforeGenericCopy(ctx, st, fresh, priorPhase); err != nil {
			return err
		}
	}
	if err := r.phaseTo(ctx, state.Prepare); err != nil {
		return err
	}
	if needCopy {
		if err := r.phaseTo(ctx, state.Snapshot); err != nil {
			return err
		}
		if err := r.copyGeneric(ctx); err != nil {
			return err
		}
		r.src.EndSnapshot()
		// 全量结束把**起点位点**写进目标端：不写的话下次会当成首次又整表重搬一遍。
		// 用空批 Apply 写位点，与 batcher.AdvanceTo 是同一个机制。
		if err := r.gsink.Apply(ctx, nil, from); err != nil {
			return fmt.Errorf("写入全量起点位点失败: %w", err)
		}
		if err := r.phaseTo(ctx, state.Catchup); err != nil {
			return err
		}
	}

	// `--snapshot-only` 与 PG 路径同一语义：搬完存量就收尾，**不进增量**。
	// 少了这个判断，打在 continuous+initial 的 MySQL 目标任务上会一路流下去——
	// 拿它做一次性搬迁会挂住，而 --help 承诺的是「只搬存量」。
	if r.opt.SnapshotOnly || !isIncrementalMode(r.mode()) {
		note := fmt.Sprintf("目标端 %s：本轮搬完全量（模式 %s）", r.tgtEP.Kind, r.mode())
		if r.opt.SnapshotOnly {
			note = fmt.Sprintf("目标端 %s：--snapshot-only，只搬了存量（不进增量）", r.tgtEP.Kind)
		}
		res.Note = note
		if err := r.drain(ctx, ExitOK, "全量完成（"+r.tgtEP.Kind+" 目标端）"); err != nil {
			return err
		}
		return &reachedDone{}
	}

	// ④ 持续流：写路径只走契约的 sink.Apply；位点与数据同批落库。
	if err := r.phaseTo(ctx, state.Streaming); err != nil {
		return err
	}
	return r.streamGeneric(ctx, st, from)
}

// genericState 取目标端的状态表能力（租约/位点/对象登记）。
//
// fail-closed：没有状态能力就没有位点，跑增量会重复或漏数据——明确拒绝，
// 不静默降级成「只搬一次」（用户以为在持续同步，实际只搬了一轮）。
func (r *runner) genericState() (stateCapable, error) {
	st, ok := r.gsink.(stateCapable)
	if !ok {
		return nil, &notImplementedError{fmt.Sprintf(
			"目标端（%s）没有租约/位点能力，只能跑一次全量：没有位点就没法知道「上次搬到哪」。"+
				"你现在能做什么：1) 把 mode.frequency 改成 once 先搬存量；"+
				"2) 需要周期性重跑就用 full_interval（每轮整表重搬，明确接受这个代价）；"+
				"3) 或者换 PostgreSQL 目标端。风险：没有（本次没有写入任何数据）", r.tgtEP.Kind)}
	}
	return st, nil
}

// genericIntervalUnsupported 是「这种目标端这一版不支持 interval」的四段式说明。
func (r *runner) genericIntervalUnsupported() error {
	return &notImplementedError{fmt.Sprintf(
		"目标端是 %s，这一版还不支持 interval（按水位列轮询增量）。"+
			"发生了什么：interval 靠目标端 `_jl_meta.watermark` 记每张表的水位并按水位读页，"+
			"这套落库路径目前只有 PG 目标端实现（MySQL 目标端的 ApplyRowsRange 还没有）。"+
			"为什么不能凑合：把 interval 悄悄当成「每轮整表重搬」会让「只补新增/更新」变成"+
			"另一种语义，用户按文档估的窗口与代价全都不成立。"+
			"你现在能做什么：1) 把 mode.frequency 改成 continuous+initial（走位点增量，删除也能同步）；"+
			"2) 或改成 once / full_interval（每轮整表重搬，明确接受这个代价）；3) 或换 PG 目标端。"+
			"风险：改成 once / full_interval 后每轮都会重搬全量，大表要算好窗口；没有数据被写入", r.tgtEP.Kind)}
}

// storedPosition 读目标端已落库的位点并按源端类型解析。
func (r *runner) storedPosition(ctx context.Context) (source.Position, error) {
	raw, err := r.gsink.(stateCapable).Position(ctx)
	if err != nil {
		return nil, fmt.Errorf("读目标端位点失败: %w", err)
	}
	if raw == "" {
		return nil, nil
	}
	parsed, err := r.src.ParsePosition(raw)
	if err != nil {
		return nil, err
	}
	if err := assertPositionKind(parsed, r.src.Kind()); err != nil {
		return nil, err
	}
	return parsed, nil
}

// cleanBeforeGenericCopy 在「从头搬」之前清掉本任务写过的行。
//
// 为什么必须清：目标端只有 upsert、没有 delete（全量重搬不带删除信息）。不清的话，
// 源端上次搬完之后删掉的行会永远留在目标端——「搬完了、退出码 0、界面全绿」，而数据是脏的。
//
// 但**首次搬进一张用户自己的表（append）不能清、也不该停**：那种表里的行全是用户的，
// 我们一行都没写过（main 上这条路径一直是能跑的）。判据用「可重算的事实」，不用「猜」：
//
//	Count == 0                                  → 空表，没什么可清
//	_jl_object 里有登记（ours）                  → 清（行都是我们写的）
//	不是我们的表 + fresh + phase == "prepare"    → **跳过 + note**：上一轮连读阶段都没进过，
//	                                             表里的行与我们无关；append 的既定语义就是
//	                                             「源端删过的行会残留」
//	不是我们的表 + fresh + phase == "snapshot"   → 硬停：上一轮搬了一半，表里可能混着
//	                                             我们写的行和用户的行走，分不清就不动
//	不是我们的表 + !fresh                        → 硬停：同上（重搬会动到用户的行走）
//
// 这正是 #74 在 PG 路径上踩过的那一课：判据必须是可重算的事实（phase / 对象登记 / 行数），
// 不能是「这一轮是不是第一次」这种需要人回忆的东西。
//
// **不变量（顺序敏感，改顺序前先读这段）**：`Count == 0 → continue`（下面那句
// `if n == 0 { continue }`，紧跟在 Count 之后）必须排在所有权与 phase 判定**之前**。
// 有它在，「清表成功之后、phaseTo(prepare) 之前崩」就是安全的：
// 表已经空了，下一轮第一件事就是 Count==0 跳过，不会因为 phase 还停在 'snapshot' 而
// 永久硬停（那会把「防止静默跳过」换成「卡死不可恢复」）。会一直硬停的只有
// 「用户表 + 有行 + phase=='snapshot'」——那正是要停下来问人的那一格，且有三条出路
// （改 on_exists / 换空的 target.schema）。**不再有「手动删掉我们写的行」这条路**：
// 「凡从头搬就清表」之后它失效了（标记删了也会因 copiedBefore 整批点名）。
// 用例：TestCleanBeforeGenericCopySkipsEmptyTable（不连库，`go test ./...` 里就能发现
// 有人把顺序改回去了）与 TestMySQLTargetEmptyTableWithSnapshotPhaseStillCopies（真库）。
func (r *runner) cleanBeforeGenericCopy(ctx context.Context, st stateCapable, fresh bool, phase string) error {
	for _, ref := range r.tables {
		tref := r.targetRef(ref)
		n, err := st.Count(ctx, tref)
		if err != nil {
			return fmt.Errorf("数目标表 %s 的行数失败（判断不了要不要清）：%w", qualifiedTarget(tref), err)
		}
		if n == 0 {
			continue // 空表：没有脏行，也没什么可清
		}
		ours, err := st.DidCreateTable(ctx, tref)
		if err != nil {
			return fmt.Errorf("查目标表 %s 是不是本任务建的失败: %w", qualifiedTarget(tref), err)
		}
		if !ours {
			// 上一轮连读阶段都没进过（phase=prepare 或空）：表里的行与我们无关。
			// append 语义下**不清、也不停**——首次搬进用户表本来就能跑。
			if fresh && (phase == "" || phase == string(state.Prepare)) {
				r.note(ctx, qualifiedTarget(tref)+" 里已有 "+fmt.Sprintf("%d", n)+
					" 行且不是本任务建的：本次只做 upsert，不清表（append 语义：源端删过的行会留在目标端）")
				continue
			}
			return &reusedTableRecopyError{
				table:  qualifiedTarget(tref),
				jobKey: r.job.JobKey,
				schema: tref.Schema,
				why:    recopyReused,
			}
		}
		if err := st.TruncateTable(ctx, tref); err != nil {
			return fmt.Errorf("清空目标表 %s 准备重搬失败: %w", qualifiedTarget(tref), err)
		}
		r.note(ctx, "清空了 "+qualifiedTarget(tref)+" 里上次搬进去的 "+fmt.Sprintf("%d 行", n)+
			"（重搬全量，避免留下源端已删除的行）；从这一刻到这张表搬完之前，目标端是部分数据")
	}
	return nil
}

// copyGeneric 把选中的表按页搬到非 PG 目标端（不写位点：全量期间的变更靠后续重放吸收）。
func (r *runner) copyGeneric(ctx context.Context) error {
	chunkRows := r.chunkRows()
	_, err := r.src.SnapshotChunksFrom(ctx, source.SnapshotOpt{ChunkRows: chunkRows, Conns: 1}, nil,
		func(c context.Context, chRef source.TableRef, chNo int, rows [][]any, from, to string) error {
			// 一页可能十万行：按小批喂给 Apply，避免一条超大 INSERT 撞 max_allowed_packet。
			const perBatch = 500
			ref := r.targetRef(chRef)
			rel := r.targetRelation(chRef)
			for i := 0; i < len(rows); i += perBatch {
				end := i + perBatch
				if end > len(rows) {
					end = len(rows)
				}
				evs := make([]source.Event, 0, end-i)
				for _, row := range rows[i:end] {
					vals, verr := toValues(row)
					if verr != nil {
						return fmt.Errorf("转换 %s 的一行失败（宁可停下也不写错数据）: %w", chRef.Table, verr)
					}
					evs = append(evs, source.Event{
						Kind:     source.EvInsert,
						Table:    ref,
						Relation: rel,
						New:      source.Row{Table: ref, Values: vals},
					})
				}
				// 位点留空：全量期间**不推位点**，位点只在全量结束（空批）与增量批次里前进。
				if err := r.gsink.Apply(c, evs, nil); err != nil {
					return err
				}
			}
			// 块记账：**数据先落、标记后落**（MySQL 目标端这一版的两步）。
			// 崩在两者之间只会「数据在、标记没写」——下一轮重搬一遍靠 upsert 收敛，
			// 不丢数据、只多搬一次。反过来（先记标记）崩了就是「标记说搬完、数据缺着」。
			// 记不下来要**响亮失败**：没有标记的续跑会让「这张表欠一次重搬」无从表达
			// （契约 §1.6 步 2 的 MarkTableRecopyPending 清的就是这些标记）。
			if cc, ok := r.gsink.(chunkChronicker); ok {
				if err := cc.MarkChunkDone(c, ref, chNo, from, to, int64(len(rows))); err != nil {
					return err
				}
			}
			r.rowsCopied += int64(len(rows))
			r.progress(c, fmt.Sprintf("全量：%s 第 %d 块，%d 行", chRef.Table, chNo, len(rows)))
			return nil
		})
	if err != nil {
		return err
	}
	// 给每张搬过的表盖完成标记（块号 0）。放在所有块之后：崩在盖章之前不会丢数据，
	// 下一轮从游标往后读只会拿到 0 行，然后重新盖章。
	if cc, ok := r.gsink.(chunkChronicker); ok {
		for _, t := range r.selectedTables() {
			// 这一轮没搬的表**不许盖章**：等 Core 把暂停集合接进通用路径之后，
			// 被暂停、存量没搬的表也会走到这里——盖上 done=1 就是「pause 解除了、数据还缺着」
			// 的永久缺行（契约 §4.6 红线 3/5）。现在这条路径没有 skipped，先把这个前提钉住。
			if r.skippedTables[t.Table] || r.plan.Skip[t.Table] {
				continue
			}
			ref := r.targetRef(source.TableRef{Schema: t.Schema, Table: t.Table})
			if err := cc.MarkTableDone(ctx, ref); err != nil {
				return err
			}
		}
	}
	r.emit(ctx, control.Event{Type: "note",
		Message: fmt.Sprintf("全量完成：%d 行（目标端 %s）", r.rowsCopied, r.tgtEP.Kind)})
	return nil
}

// targetRelation 是「目标端那一版列定义」：列不变，表名换成目标端的。
// 目标端按它决定列序与列名（事件里的 Table 会被直接拼进 SQL）。
func (r *runner) targetRelation(ref source.TableRef) *source.Table {
	src := r.relationOf(ref)
	if src == nil {
		return nil
	}
	cp := *src
	cp.TableRef = r.targetRef(ref)
	return &cp
}

// streamGeneric 是「非 PG 目标端」的持续流：与 PG 路径同一套 batcher 规则。
func (r *runner) streamGeneric(ctx context.Context, st stateCapable, from source.Position) error {
	// 按表暂停的过滤包在**映射之后**（paused_tables 的 key 是目标端裸表名）。
	// 暂停存储没绑上时 pauseSink 原样透传——MySQL 目标端从 PR #95 起就有这套存储了。
	mapped := newRefMappedSink(r.gsink, r.targetRef)
	b := newBatcher(newPauseSink(mapped, r), r.src, r.doc, r.batchRows(), r.batchDelay(), from,
		func(rows int64, pos source.Position) {
			r.rowsStream += rows
			r.mu.Lock()
			r.lastPos = pos
			r.mu.Unlock()
		})
	b.onFlush = func(applied int64, pos source.Position) {
		r.progress(ctx, fmt.Sprintf("增量：已应用 %d 行，位点 %s", applied, posString(pos)))
	}
	// **ack 前的位点落盘校验**：装不上就是接线缺口，响亮失败（不降级成「没有守卫」）。
	if err := r.installAckGuard(b); err != nil {
		return err
	}

	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	// 后台提交失败要立刻打断流（否则错误被吞，任务停在「实时同步中」）。
	b.onError = func(error) { cancelStream() }
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
	var lastEvent atomic.Int64
	lastEvent.Store(time.Now().UnixNano())
	go func() {
		t := time.NewTicker(300 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-streamCtx.Done():
				return
			case <-t.C:
				if r.opt.IdleAfter <= 0 {
					continue
				}
				rows, inTx := b.Pending()
				if rows == 0 && !inTx && time.Since(time.Unix(0, lastEvent.Load())) > r.opt.IdleAfter {
					// 追平后收尾是**成功完成**（不是用户按停），历史页别记成 stopped。
					idleHit.Store(true)
					stopHit.Store(true)
					cancelStream()
					return
				}
			}
		}
	}()

	streamErr := r.src.Stream(streamCtx, from, func(c context.Context, ev source.Event) error {
		lastEvent.Store(time.Now().UnixNano())
		if stopHit.Load() {
			return &stoppedError{msg: fmt.Sprintf("到达本次运行的时间上限（%s），已收尾", r.opt.StopAfter)}
		}
		// 时间类列：PG 发的是带时区后缀的文本，MySQL 的 datetime 不接受（Error 1292）。
		// 全量走驱动值（已是 time.Time），所以只有增量会撞。
		normalizeTemporal(&ev)
		return b.Add(c, ev)
	})
	cancelStream()
	// 先取根因再 Flush（见 engine.go 同一处的理由）：Flush 失败时别让重试错误盖住根因。
	cause := b.Err()
	if err := b.Flush(context.Background()); err != nil {
		if cause != nil {
			return cause
		}
		return err
	}
	// 后台提交失败过就是失败：报真原因，不报「流被打断」。
	if ferr := b.Err(); ferr != nil {
		return ferr
	}
	code := ExitOK
	if idleHit.Load() {
		r.note(context.Background(), fmt.Sprintf("已经追平且安静了 %s：按 --stop-when-idle 正常收尾", r.opt.IdleAfter))
	}
	if streamErr != nil {
		var stopped *stoppedError
		switch {
		case errors.As(streamErr, &stopped):
			code = ExitStopped
			if idleHit.Load() {
				code = ExitOK
			}
		case errors.Is(streamErr, context.Canceled) || isUserStop(ctx, streamErr):
			code = ExitStopped
			if idleHit.Load() {
				code = ExitOK
			}
		case (stopHit.Load() || idleHit.Load()) && isLocalClose(streamErr):
			// 我们自己要求收尾（按停 / 到达 --stop-after / 已追平）后 cancel ctx 打断阻塞读，
			// 有的连接器会把这个中断冒成一个网络错误（MySQL 源的
			// `use of closed network connection`）。这里的判据是**两个条件同时成立**：
			//   ① 我们先要停的；② 错误本身长得像「本地主动关闭」（`isLocalClose`）。
			// 只判 ① 会把「停止请求之后才到的真实错误」（目标库断连 / 权限被回收 / 解码失败）
			// 记成「已停止」、退出码 0 —— 用户以为一切正常，实际数据停在半路。
			// 后台提交失败（`b.Err()`）在调用点更早就已返回，不会走到这里。
			code = ExitStopped
			if idleHit.Load() {
				code = ExitOK
			}
		default:
			return streamErr
		}
	}
	msg := "增量到运行结束（continuous）"
	if code == ExitOK && idleHit.Load() {
		msg = "已追平并收尾（--stop-when-idle）"
	}
	if err := r.drain(context.Background(), code, msg); err != nil {
		return err
	}
	return &reachedDone{}
}

// installAckGuard 给 batcher 装「ack 前位点必须已落盘」的守卫。
//
// fail-closed：目标端必须**声明**自己的位点持久化语义；声明缺失、或声明了本引擎
// 不认识的取值，一律报错并说清期望值与实际值。**绝不**降级成「没有守卫继续跑」——
// 那样「位点还没落盘就 ack」会悄悄发生，源端清掉 WAL，目标端一回滚就永久缺数据。
func (r *runner) installAckGuard(b *batcher) error {
	d, ok := r.gsink.(positionDurability)
	if !ok {
		return fmt.Errorf("目标端（%s）没有声明位点持久化语义（期望实现 PositionDurability() string，实际类型 %T）："+
			"没有它就没法判断「Apply 返回」是否等于「位点已落盘」，而 staged_phases 的目标端"+
			"在 DDL/TRUNCATE 批次里位点是分三段写的。你能做什么：这是接线缺口，报给维护者；"+
			"风险：没有（还没开始流，什么都没写）", r.tgtEP.Kind, r.gsink)
	}
	switch d.PositionDurability() {
	case "same_tx":
		// 数据与位点同一个事务：Apply 返回即落盘，不需要额外校验。
		return nil
	case "staged_phases":
		b.ackGuard = func(c context.Context, pos source.Position) error {
			st, err := d.PositionDurable(c, posString(pos))
			if err != nil {
				return fmt.Errorf("ack 前校验位点失败（目标端读不回来）：%w。"+
					"风险：没有（这一批已落库，位点未 ack，重启会重放这一批，幂等）", err)
			}
			switch st {
			case mysqlsink.PositionDurable:
				return nil
			case mysqlsink.PositionBehind:
				return fmt.Errorf("拒绝向源端确认位点 %s：目标端位点还停在更早的位置（这一批没落盘）。"+
					"风险：现在就 ack 会让源端清掉还能重放的流水，目标端一回滚就永久缺数据；"+
					"已停下，数据没丢", posString(pos))
			case mysqlsink.PositionDifferent:
				return fmt.Errorf("目标端位点是别的值（被回退过，或被另一个实例改过），不是本批的 %s："+
					"立刻停下，别覆盖别人的进度。你能做什么：`jinliang status` 看清当前位点与阶段，"+
					"确认是不是别的实例在写；风险：继续跑会把位点写乱，重放范围说不清", posString(pos))
			default:
				return fmt.Errorf("目标端返回了不认识的位点状态 %v（期望 durable/behind/different）", st)
			}
		}
		return nil
	default:
		return fmt.Errorf("目标端声明的位点持久化语义 %q 不认识（期望 same_tx / staged_phases）："+
			"不知道语义就没法判断能不能 ack。你能做什么：这是接线缺口，报给维护者；风险：没有（还没开始流）",
			d.PositionDurability())
	}
}

// positionDurability 是「位点怎么落盘的」这一能力的接口（目标端按可选接口实现）。
type positionDurability interface {
	PositionDurability() string
	PositionDurable(ctx context.Context, want string) (mysqlsink.PositionState, error)
}

// normalizeTemporal 把源端「时间类列的文本值」转成 time.Time。
//
// 为什么需要：PG 的逻辑复制（pgoutput）把 timestamptz 发成文本
// `2026-11-07 03:17:34.117788+00`，而 MySQL 的 datetime 列不接受带时区后缀的字面量
// （Error 1292）。全量走的是驱动值（已经是 time.Time），所以只有增量这条路径会撞上。
func normalizeTemporal(ev *source.Event) {
	if ev.Relation == nil {
		return
	}
	// New 与 Old 都要规范：DELETE 只带 Old（时间主键的删除靠它定位），
	// 只处理 New 会让「时间主键的删除」继续撞 1292。
	normalizeRow(ev.Relation, &ev.New)
	normalizeRow(ev.Relation, &ev.Old)
}

func normalizeRow(rel *source.Table, row *source.Row) {
	if len(row.Values) == 0 {
		return
	}
	for i, col := range rel.Columns {
		if i >= len(row.Values) {
			break
		}
		if !isTemporalType(col.Type) {
			continue
		}
		v := row.Values[i]
		if v.Kind != source.KindString || v.S == "" {
			continue
		}
		if t, ok := parsePGTime(v.S); ok {
			row.Values[i] = source.Value{Kind: source.KindTime, T: t}
		}
	}
}

// isTemporalType 判断这一列要不要做「文本 → time.Time」规范化。
//
// 口径：`timestamp` / `timestamptz` / `date` 是真正需要的（pgoutput 发的是带时区后缀的
// 文本，MySQL 的 datetime 不接受）。`time` / `time(n)` 也一并放进来只是**统一**：
// `parsePGTime` 没有裸 time 的 layout，所以对 time 列实际不会转换，MySQL 本来就接受
// 那个字面量——这里不宣称「不转就撞 1292」，那是不准确的说法。
func isTemporalType(t string) bool {
	lt := strings.ToLower(strings.TrimSpace(t))
	return strings.HasPrefix(lt, "time") || strings.HasPrefix(lt, "date")
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

// qualifiedTarget 只用于错误信息里显示表名。
func qualifiedTarget(ref source.TableRef) string {
	if ref.Schema == "" {
		return ref.Table
	}
	return ref.Schema + "." + ref.Table
}

// targetCatalog 是交给目标端的表定义：保留选中的表，schema 换成目标端自己的。
//
// 为什么必须换：源端 catalog 里每张表都带 `Schema="public"`；MySQL 目标端把它当
// schema（库）名去建表，于是变成 `public`.`t` —— 用户看到的是「CREATE 权限被拒」，
// 实际是库名不对。目标端 schema 为空时 mysqlsink 会回落到连接里的库。
func (r *runner) targetCatalog() source.Catalog {
	targetSchema := r.targetSchema()
	out := source.Catalog{}
	for _, t := range r.selectedTables() {
		cp := t
		cp.Schema = targetSchema
		out.Tables = append(out.Tables, cp)
	}
	return out
}

// targetRef 把源端表引用换成目标端引用（schema 换成目标端的）。
// 事件里的 Table 会被目标端直接拼进 SQL，必须是目标端的库名。
func (r *runner) targetRef(ref source.TableRef) source.TableRef {
	schema := strings.TrimSpace(r.doc.Target.Schema)
	if schema == "" {
		// 空 = 用目标端的默认落点。PG 是 public、MySQL 是连接里的库——这个判断
		// 只在 schemaOf 里有一份，这里委托它，不另写一套回落逻辑。
		schema = r.targetSchema()
	}
	return source.TableRef{Schema: schema, Table: ref.Table}
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
