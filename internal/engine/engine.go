// Package engine is the sync engine itself: preflight → prepare → snapshot →
// catchup → streaming → draining, with every phase change written to the control
// database and published on the SSE bus (docs/CONTROL-PLANE.md §4).
//
// 它只做编排，不碰 SQL：读端在 pgsource，写端在 pgsink，配置来自控制库
// （jobs.spec_json + connections，密码只在内存里出现一次）。
package engine

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spencerswagger/jinliang/internal/connect"
	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/core"
	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/mysqlsink"
	"github.com/spencerswagger/jinliang/internal/pgsink"
	"github.com/spencerswagger/jinliang/internal/pgsource"
	"github.com/spencerswagger/jinliang/internal/pgwire"
	"github.com/spencerswagger/jinliang/internal/preflight"
	"github.com/spencerswagger/jinliang/internal/source"
	"github.com/spencerswagger/jinliang/internal/state"
)

// Exit codes are the contract spencer asked for (0 完成 / 1 失败 / 130 手动停止),
// so a container or CI can tell "finished" from "someone pressed Ctrl-C".
const (
	ExitOK      = 0
	ExitFailed  = 1
	ExitStopped = 130
)

// Options is one engine invocation.
type Options struct {
	Store *control.Store
	JobID string
	Actor string

	// BatchRows / BatchDelay override reliability.batch_rows / batch_delay.
	BatchRows  int
	BatchDelay time.Duration
	// SnapshotOnly forces "copy the data and stop" (used by --once and tests).
	SnapshotOnly bool
	// StopAfter stops streaming after this long (smoke tests; 0 = never).
	StopAfter time.Duration
	// IdleAfter finishes the run once the stream has been quiet this long AND
	// the source-side backlog is below LagThreshold. It turns "continuous" into
	// a one-shot incremental sync, which is what a scripted/CI run wants.
	IdleAfter time.Duration
	// LagThreshold is how much source WAL may still be unreplayed before an
	// idle finish counts as "caught up" (default 8 MB).
	LagThreshold int64
	// Verify compares source and target fingerprints when the run finishes.
	Verify bool
	// Resync 是**显式整轮重搬**：重建书签/位点，先清本任务建的表再整表重搬。
	// 语义与边界见 resync.go 顶部；它只作用于「这一轮」，不能配 --watch 循环跑。
	Resync bool
	// LeaseTTL defaults to 30s.
	LeaseTTL time.Duration
	// HeartbeatEvery 覆盖心跳间隔（测试用；默认 control.DefaultHeartbeatInterval/2）。
	HeartbeatEvery time.Duration
}

// Result is what a caller reports to a human.
type Result struct {
	RunID      string
	Phase      state.Phase
	ExitCode   int
	RowsCopied int64
	RowsStream int64
	Position   string
	Elapsed    time.Duration
	Mismatches []string
	Note       string
	// Resync 非空表示这一轮做了整轮重搬（里面有旧位点与清掉的表，见 ResyncReport）。
	Resync *ResyncReport
}

// ErrHeldByOther means another instance holds the job's lease (E1005).
var ErrHeldByOther = errors.New("另一个实例正在跑这个任务")

// Run executes one job end to end. It returns an error only for outcomes a
// human must look at; "stopped by user" is a Result with ExitStopped, not an
// error, because nothing went wrong.
func Run(ctx context.Context, opt Options) (Result, error) {
	if opt.Store == nil {
		return Result{}, errors.New("内部错误：没有控制库")
	}
	res := Result{ExitCode: ExitOK}
	// migrateErr 记「状态表迁移没成功」：后面若撞上缺列，把它一起包进主错误，
	// 别让用户先读到一条与真实原因无关的列错误。
	var migrateErr error
	started := time.Now()

	job, err := opt.Store.GetJob(ctx, opt.JobID)
	if err != nil {
		return res, fmt.Errorf("找不到任务: %w", err)
	}
	doc, err := opt.Store.JobDoc(ctx, job)
	if err != nil {
		return res, err
	}
	srcConn, err := opt.Store.GetConnection(ctx, job.SourceID)
	if err != nil {
		return res, err
	}
	tgtConn, err := opt.Store.GetConnection(ctx, job.TargetID)
	if err != nil {
		return res, err
	}
	srcSecret, err := opt.Store.Secret(ctx, job.SourceID)
	if err != nil {
		return res, fmt.Errorf("解不开源连接的口令（key 文件换过？）: %w", err)
	}
	tgtSecret, err := opt.Store.Secret(ctx, job.TargetID)
	if err != nil {
		return res, fmt.Errorf("解不开目标连接的口令（key 文件换过？）: %w", err)
	}
	// 源端支持 PG（逻辑复制槽）与 MySQL（binlog/GTID）两种读实现，其它类型在构造阶段报清楚。
	if srcConn.Kind != dsn.KindPostgres && srcConn.Kind != dsn.KindMySQL {
		return res, fmt.Errorf("源端类型 %q 还没有读实现（现在支持 postgres / mysql 源）。"+
			"你现在能做什么：把源连接的 kind 改成 postgres 或 mysql。"+
			"风险：没有（什么都没动）", srcConn.Kind)
	}
	if tgtConn.Kind != dsn.KindPostgres && tgtConn.Kind != dsn.KindMySQL {
		return res, fmt.Errorf("目标端类型 %q 还不支持（现在支持 postgres / mysql）。"+
			"你现在能做什么：把目标连接的 kind 改成 postgres 或 mysql。"+
			"风险：没有（什么都没动）", tgtConn.Kind)
	}
	srcEP := endpointOf(srcConn, srcSecret)
	tgtEP := endpointOf(tgtConn, tgtSecret)

	// **状态表迁移要早于任何读 `_jl_meta` 的动作**（StartRun 的冲突回收会读租约/状态）。
	// 否则旧 schema 的库会在 prepare 之前就因为「没有 paused_tables 列」报一条与真实原因
	// 无关的错（租约/预检/表不存在才是真因），用户还会以为要手工改表。幂等，一条 DDL。
	// **尽力而为，绝不在这里中止**：目标库连不上/schema 还没建的时候，真原因会由后面
	// 的预检或 prepare 报出来（并且要落一条 run 行）——这里中止会让「必失败任务」连
	// run 行都没有，用户反而看不到真原因。
	// 只对 **PG 目标端**跑（MySQL 目标端没有暂停记录这类状态列）。
	// **必须有超时**：目标端黑洞时无超时的 connect 会卡到 OS TCP 超时（~2min），而这时
	// 还没有 run 行；随后的 preflight 再连一次 → 总等待翻倍且第一次失败不留痕。
	// 同理，那条 `ADD COLUMN IF NOT EXISTS` 会拿一次 ACCESS EXCLUSIVE，撞上 `_jl_meta`
	// 长事务会一直等——10s 上限同时解掉这两件事。真原因交给 preflight 报。
	if tgtEP.Kind == dsn.KindPostgres {
		mctx, cancelMigrate := context.WithTimeout(ctx, 10*time.Second)
		err := pgsink.EnsureStateTables(mctx, tgtEP, schemaOf(doc, tgtConn))
		cancelMigrate()
		if err != nil {
			migrateErr = err
			emitEarlyNote(&res, "先把目标端状态表升到当前版本时出错（真原因稍后由预检报出）："+err.Error())
		}
	}
	run, err := opt.Store.StartRun(ctx, job.ID, state.Preflight, control.RunKindSync)
	if err != nil && errors.Is(err, control.ErrConflict) {
		// 控制面有「一个任务只允许一条活跃 run」的约束（#5）。进程被 kill -9 之后
		// 那条 run 会永久占位，直接报错等于「崩过一次就再也跑不了」。
		// 我们只在**能证明它已经死了**的时候替它收尾：目标端租约写着 pid 已不在/已过期。
		if reclaimed, rerr := reclaimAbandonedRun(ctx, opt.Store, job, tgtEP, schemaOf(doc, tgtConn)); rerr != nil {
			return res, rerr
		} else if reclaimed {
			run, err = opt.Store.StartRun(ctx, job.ID, state.Preflight, control.RunKindSync)
		}
	}
	if err != nil {
		return res, err
	}
	res.RunID = run.ID

	// 登记到本进程的登记表：删任务时 StopJob 才能 cancel 到这个 run 并等它退出
	//（退出路径自己会 ReleaseLease —— 租约是退出时才真正放掉的）。
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	_, unregister := registerRun(job.ID, runCancel)
	defer unregister()
	ctx = runCtx

	r := &runner{
		opt:    opt,
		job:    job,
		doc:    doc,
		srcEP:  srcEP,
		tgtEP:  tgtEP,
		run:    run,
		instID: instanceID(ctx, opt.Store),
		leaseTTL: func() time.Duration {
			if opt.LeaseTTL > 0 {
				return opt.LeaseTTL
			}
			return 30 * time.Second
		}(),
	}
	err = r.execute(ctx, &res)
	if err != nil && migrateErr != nil && strings.Contains(err.Error(), "paused_tables") {
		// 迁移没成功、后面又撞上缺列：把迁移那条一起包进来，别让用户先读到无关的列错误。
		err = fmt.Errorf("%w（另外：状态表迁移也没成功：%v）", err, migrateErr)
	}
	res.Elapsed = time.Since(started)
	if err != nil {
		res.ExitCode = ExitFailed
	}
	return res, err
}

// reclaimAbandonedRun closes a run row left behind by a killed process, but only
// when the target-side lease proves that nobody is running it any more.
//
// 为什么需要：控制面的「一任务一活跃 run」是库层唯一索引（#5），进程被 kill -9
// 之后那条行没人收尾，下次 `run --job` 直接被拒。控制面自己的收尾是 `serve` 启动时
// 按心跳过期做的（90 秒），headless 场景等不起。这里用**目标端租约**作证据：
// 租约过期、或 label 里的 pid 已经不在 → 那一轮确实死了，收尾成 interrupted。
func reclaimAbandonedRun(ctx context.Context, store *control.Store, job control.Job,
	ep dsn.Endpoint, schema string) (bool, error) {

	active, err := store.ActiveRuns(ctx)
	if err != nil {
		return false, nil // 查不到就当没这事，让原来那条冲突错误冒上去
	}
	for _, run := range active {
		if run.JobID != job.ID {
			continue
		}
		// 两条证据链，任一成立就算「上一次真死了」：
		//   1. 目标端租约没人续期（pid 已不在 / expires_at 已过）；
		//   2. 控制面心跳停了超过 StaleAfter（3×心跳间隔）。
		// 只看第 1 条会漏掉「读不到租约表 / 目标端连不上」的情况——那时用户会陷入
		// 「崩过一次就永远 E1005 等 serve 来救」，而 serve 可能根本没在跑。
		why := ""
		abandoned, lerr := targetLeaseAbandoned(ctx, ep, schema, job.JobKey)
		switch {
		case lerr == nil && abandoned:
			why = "目标端租约已无人续期"
		case heartbeatStale(run, time.Now()):
			why = "控制面心跳已停超过 " + control.StaleAfter(control.DefaultHeartbeatInterval).String()
		default:
			return false, nil
		}
		_, err = store.FinishRun(ctx, run.ID, state.Failed,
			"上一次运行随进程退出中断（kill -9 或断电；判据："+why+"）；进度以目标端 _jl_meta 为准，重启会从断点续跑",
			ExitFailed, control.FinishInterrupted)
		if err != nil {
			return false, fmt.Errorf("收尾上一次被中断的运行失败: %w", err)
		}
		return true, nil
	}
	return false, nil
}

// targetLeaseAbandoned 判断「目标端那条租约背后还有没有活着的进程」。
//
// 按目标端类型分派：PG 的租约表在 schema 里、MySQL 的在库里；两边的 label 都是
// `任务名#pid#token`，所以「pid 不在 / 已过期」的判据一致。这里只做分派，
// 判断逻辑不复制（各自实现 Abandoned）。
func targetLeaseAbandoned(ctx context.Context, ep dsn.Endpoint, schema, jobKey string) (bool, error) {
	switch ep.Kind {
	case dsn.KindMySQL:
		h, err := mysqlsink.ReadLease(ctx, ep, jobKey)
		if err != nil {
			return false, err
		}
		return h.Abandoned(time.Now(), mysqlsink.ProcessAlive), nil
	default:
		h, err := pgsink.ReadLease(ctx, ep, schema, jobKey)
		if err != nil {
			return false, err
		}
		return h.Abandoned(time.Now(), pgsink.ProcessAlive), nil
	}
}

// heartbeatStale 判断一条活跃 run 的心跳是否已经停了超过 StaleAfter。
// 心跳时间解析不出来时按「不可证明」处理（返回 false），不让它变成误杀的理由。
func heartbeatStale(run control.Run, now time.Time) bool {
	if run.HeartbeatAt == "" {
		return false
	}
	hb, err := time.Parse(time.RFC3339, run.HeartbeatAt)
	if err != nil {
		return false
	}
	return now.Sub(hb) > control.StaleAfter(control.DefaultHeartbeatInterval)
}

// schemaOf is targetSchema as a free function (Run() needs it before the runner exists).
func schemaOf(doc control.JobDoc, tgt control.Connection) string {
	if s := strings.TrimSpace(doc.Target.Schema); s != "" {
		return s
	}
	// 回落值必须与目标端**真实落点**一致：
	// PG 的默认 schema 是 public（数据库名不是 schema）；MySQL 的库就是 schema。
	// 之前统一回落 tgt.Database，导致默认配置的 PG 目标变成
	// 「源端 public ≠ 目标端 jinliang」→ DDL 闸门把每一条都拒掉，文案还很像真的。
	if tgt.Kind == dsn.KindMySQL {
		return tgt.Database
	}
	return "public"
}

func endpointOf(c control.Connection, secret string) dsn.Endpoint {
	return dsn.Endpoint{
		Kind:     c.Kind,
		Host:     c.Host,
		Port:     c.Port,
		Database: c.Database,
		Username: c.Username,
		Password: secret,
		Params:   c.Params(),
	}
}

func instanceID(ctx context.Context, st *control.Store) string {
	if id, err := st.InstanceID(ctx); err == nil {
		return id
	}
	return "unknown"
}

// runner carries the per-run mutable state.
type runner struct {
	opt    Options
	job    control.Job
	doc    control.JobDoc
	srcEP  dsn.Endpoint
	tgtEP  dsn.Endpoint
	run    control.Run
	instID string

	leaseTTL time.Duration

	src  runSource
	sink *pgsink.Sink
	// gsink 是「按接口」的目标端：PG 用 pgsink（COPY 快路径），MySQL 用 mysqlsink
	// （走契约的 Apply）。构造阶段确定，之后引擎只用接口。
	gsink  targetSink
	cat    source.Catalog
	tables []source.TableRef
	// emitHook 只在单测里用（数事件条数），生产路径为 nil。
	emitHook func(control.Event)

	phase   state.Phase
	leaseOK bool
	// 丢掉租约必须让整个 run 停下来：光 emit 一句「本进程停止写入」而写路径照跑，
	// 等于两个进程同时写同一张目标表。
	leaseLost atomic.Bool
	leaseWhy  atomic.Value // string
	cancelRun context.CancelFunc
	leaseTick time.Duration

	snapshotName    string
	consistentPoint source.Position
	reusedSlot      bool
	needSnapshot    bool
	priorPhase      string
	// plan 是这次全量「接着搬 / 从头搬 / 不用搬」的结论（见 resume.go）。
	plan chunkPlan
	// resyncReport 记录这一轮 --resync 实际清了什么（Result 与 CLI 都读它）。
	resyncReport *ResyncReport
	// snapshotDone 只用于 `--resync` 的收尾断言：说了整轮重搬就必须真的搬过。
	snapshotDone bool
	// clearedTables 是**真的清空过**的表（cleanBeforeRecopy 每清成一张追加一条）。
	clearedTables []string

	// progressByTable 是本轮读到的块进度（暂停记录收口要用「存量完成没」这个事实，
	// 而它只能从 _jl_chunk 来，不能从 paused_tables 来）。
	progressByTable map[string]pgsink.ChunkProgress

	rowsCopied int64
	rowsStream int64
	exitCode   int

	mu       sync.Mutex
	lastPos  source.Position
	lastLagB int64
}

func (r *runner) execute(parent context.Context, res *Result) error {
	ctx, cancel := context.WithCancel(parent)
	r.cancelRun = cancel
	defer cancel()

	// ① 心跳：`runs.heartbeat_at` 不刷新的话，空闲 90 秒就会被 doctor 判成幽灵、
	// 重启时把健康任务标成 interrupted（TouchRun 之前一个调用方都没有）。
	// 间隔取 control.DefaultHeartbeatInterval 的一半，保证总有至少两个心跳落在
	// StaleAfter(=3×) 窗口里 —— 口径与 control 包一致，不另写一套。
	hbCtx, stopHB := context.WithCancel(ctx)
	defer stopHB()
	go r.heartbeatLoop(hbCtx, r.heartbeatEvery(), func(c context.Context) error {
		return r.opt.Store.TouchRun(c, r.run.ID)
	})

	defer func() {
		res.Phase = r.phase
		res.RowsCopied = r.rowsCopied
		res.RowsStream = r.rowsStream
		res.Position = posString(r.lastPos)
		res.ExitCode = r.exitCode
		res.Resync = r.resyncReport
	}()

	if err := r.checkResync(); err != nil {
		// 先判再动：--resync 用在不支持的目标端/模式上时，一个字节都不该被改。
		return r.fail(ctx, err)
	}
	if err := r.preflight(ctx); err != nil {
		return r.fail(ctx, err)
	}
	// interval（水位轮询）这一版只有 PG 源实现：在拿租约、建表之前就说清楚，
	// 不留「跑到一半才发现这条路没做」的现场。
	if r.mode() == "interval" && r.srcEP.Kind != dsn.KindPostgres {
		return r.fail(ctx, r.intervalUnsupported())
	}
	// 目标端不是 PG：走「只做全量」的通用路径（用契约里的 sink 接口，不按类型分支）。
	if r.tgtEP.Kind != dsn.KindPostgres {
		if err := r.prepareGeneric(ctx); err != nil {
			return r.fail(ctx, err)
		}
		defer r.teardown()
		if err := r.executeGeneric(ctx, res); err != nil {
			if r.leaseLost.Load() {
				return r.fail(ctx, r.leaseLostError())
			}
			if isUserStop(ctx, err) {
				return r.fail(ctx, err)
			}
			if errors.As(err, new(*reachedDone)) {
				return nil // 通用路径自己收尾到 done
			}
			return r.fail(ctx, err)
		}
		return nil
	}

	// teardown 要在 prepare **之前**注册：prepare 已经抢了租约、开了连接，它中途失败
	// （比如目标列类型对不上）时若直接返回，租约就没人放 —— 同一台机器上紧接着再跑一次
	// 会撞「另一个实例正在跑这个任务」（E1005），而「改完类型重跑」正是错误文案让用户做的
	// 下一步。teardown 对 nil 的 sink/src 安全，提前注册没有副作用。
	defer r.teardown()
	if err := r.prepare(ctx); err != nil {
		return r.fail(ctx, err)
	}

	if r.needSnapshot {
		if err := r.snapshot(ctx); err != nil {
			if r.leaseLost.Load() {
				return r.fail(ctx, r.leaseLostError())
			}
			if errors.Is(err, context.Canceled) {
				return r.drain(ctx, ExitStopped, "收到停止信号：全量阶段已停在块边界")
			}
			return r.fail(ctx, err)
		}
	}
	// 「说了整轮重搬却没搬」是一种比失败更糟的成功：退出码 0、用户以为已重建，
	// 数据其实一行都没重搬。判据留在收尾这里，任何一处漏改（needSnapshot 判据、
	// applyResync、Clean 名单）都会让用例直接红，而不是悄悄放过去。
	if r.opt.Resync && r.plan.Kind == copyFresh && !r.snapshotDone {
		return r.fail(ctx, errResyncWithoutSnapshot)
	}

	if r.opt.SnapshotOnly || r.mode() == "once" {
		if r.opt.Verify {
			if diffs, err := r.verify(ctx); err != nil {
				r.note(ctx, "校验没跑完："+err.Error())
			} else {
				res.Mismatches = diffs
			}
		}
		if err := r.drain(ctx, ExitOK, "全量完成"); err != nil {
			return err
		}
		emitEarlyNote(res, "只搬了存量数据（mode.frequency="+r.mode()+"）")
		return nil
	}
	if r.mode() == "interval" {
		if err := r.phaseForInterval(ctx); err != nil {
			return r.fail(ctx, err)
		}
		if err := r.runIntervalRound(ctx); err != nil {
			if r.leaseLost.Load() {
				return r.fail(ctx, r.leaseLostError())
			}
			if isUserStop(ctx, err) {
				return r.fail(ctx, err)
			}
			return r.fail(ctx, err)
		}
		if err := r.drain(ctx, ExitOK, "定时增量本轮完成（水位列 "+r.doc.Mode.IncrementalColumn+"）"); err != nil {
			return err
		}
		// 说清这条路的取舍（Ark 口径 2026-11-10）：水位增量只补新增/更新，
		// **不同步删除**。文案里不许暗示我们追 delete，否则用户会以为已经一致。
		emitEarlyNote(res, "interval 模式：只跑了水位增量（整表重搬仅发生在第一次或没有水位列的表）；"+
			"水位增量只补新增与更新，**不同步删除**——源端删掉的行会留在目标端，"+
			"需要连删除一起同步请用 continuous+initial，或定期用 once 整轮重搬")
		return nil
	}
	if r.mode() == "full_interval" {
		if err := r.drain(ctx, ExitOK, "全量周期重跑本轮完成"); err != nil {
			return err
		}
		emitEarlyNote(res, "full_interval 模式：本轮整表重跑；下一轮由调度器（--watch）触发")
		return nil
	}

	if err := r.stream(ctx, res); err != nil {
		if r.leaseLost.Load() {
			return r.fail(ctx, r.leaseLostError())
		}
		var stopped *stoppedError
		if errors.As(err, &stopped) {
			code := ExitStopped
			if stopped.ok {
				code = ExitOK
			}
			return r.drain(ctx, code, stopped.msg)
		}
		if errors.Is(err, context.Canceled) {
			return r.drain(ctx, ExitStopped, "收到停止信号：已把手上这个事务提交完")
		}
		return r.fail(ctx, err)
	}
	if r.leaseLost.Load() {
		return r.fail(ctx, r.leaseLostError())
	}
	if err := r.drain(ctx, ExitOK, "增量追平"); err != nil {
		return err
	}
	if r.opt.Verify {
		if diffs, err := r.verify(ctx); err != nil {
			r.note(ctx, "校验没跑完："+err.Error())
		} else {
			res.Mismatches = diffs
		}
	}
	return nil
}

// humanLag formats a WAL byte distance the way a human reads it.
func humanLag(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	units := []string{"KB", "MB", "GB", "TB"}
	v := float64(n)
	i := -1
	for v >= unit && i < len(units)-1 {
		v /= unit
		i++
	}
	return fmt.Sprintf("%.1f %s", v, units[i])
}

// knownSourceKinds 是位点串里允许出现的端点类型。source.SplitPosition 只管分段与
// 版本，**不校验 kind 取值域**（`foo:v1:x` 它能过），所以对账这一步必须在引擎里补。
var knownSourceKinds = map[string]bool{
	dsn.KindPostgres: true,
	dsn.KindMySQL:    true,
}

// positionKindError is E1010: the checkpoint names a source this job is not.
type positionKindError struct {
	got  string
	want string
}

func (e *positionKindError) Error() string {
	if !knownSourceKinds[e.got] {
		return fmt.Sprintf("目标库里的同步进度标记写着来源类型 %q，不是本工具认得的类型（只有 postgres / mysql）", e.got)
	}
	return fmt.Sprintf("目标库里的同步进度标记是 %s 写的，而这次连的是 %s：换回对应的源，或重新开始同步", e.got, e.want)
}

// assertPositionKind 校验位点的来源类型：必须是已知类型，且与本任务的源一致。
func assertPositionKind(p source.Position, want string) error {
	if p == nil {
		return nil
	}
	got := p.Kind()
	if !knownSourceKinds[got] || got != want {
		return &positionKindError{got: got, want: want}
	}
	return nil
}

// caughtUpAt decides "we have replayed everything the source has announced".
// Using the server's WAL end (from keepalives) instead of "no events recently"
// matters on a big backlog: replaying 2 GB of WAL emits no data events, and a
// time-based guess would declare victory and silently leave lag behind.
func (r *runner) caughtUpAt(applied, startupTarget, serverEnd source.Position) bool {
	target := startupTarget
	if r.src != nil && !r.src.IsZero(serverEnd) && r.src.Before(target, serverEnd) {
		target = serverEnd
	}
	if target == nil || (r.src != nil && r.src.IsZero(target)) {
		return false
	}
	if r.src == nil {
		return false
	}
	return !r.src.Before(applied, target)
}

// posString 把位点安全地转成落库的文本（nil 接口直接 .String() 会 panic）。
func posString(p source.Position) string {
	if p == nil {
		return ""
	}
	return p.String()
}

// recopyAction decides whether a half-copied table may be truncated before the
// copy is redone.
//
// 只有「这张表是本任务建的」才允许清：append 模式复用用户表时，表里本来就有
// 用户的行，清掉等于删别人数据。这种表我们无从分辨哪些行是本次写的（设计上
// 不往用户表里塞标记列），所以**宁可停下来说清楚**，让用户选：
//   - 想让我们重建 → target.on_exists: recreate（明确会清空）；
//   - 想保留原表 → 手动清掉本次写入的行后重跑；
//   - 或换一个空的 target.schema。
//
// 返回 nil 表示可以清；返回错误表示必须停下（原因随 recopyReason 走）。
func (r *runner) recopyAction(ctx context.Context, ref source.TableRef, why recopyReason) error {
	// 查的是**目标端**的表（对象登记按目标 schema 记），报错里说源端表名。
	ours, err := r.sink.DidCreateTable(ctx, r.targetRef(ref))
	if err != nil {
		return fmt.Errorf("查不到目标表 %s 是谁建的（读 _jl_object 失败）: %w", ref.Table, err)
	}
	if ours {
		return nil
	}
	return &reusedTableRecopyError{table: ref.Table, jobKey: r.job.JobKey, schema: ref.Schema, why: why}
}

// recopyReason 说清「为什么要清这张表」——文案必须跟着原因走。
// 只写「上次搬到一半就断了」是错的：还有一种是「上次搬完了，但位点追不回来」。
type recopyReason int

const (
	recopyHalfCopy recopyReason = iota // 上次全量搬到一半就断了
	recopyGap                          // 上次搬完了，但书签追不回位点，那段变更补不回来
	recopyNoPK                         // 无主键表，只能整表重来
	recopyOnce                         // once / full_interval：每轮整轮重搬，必须清空重建
	recopyNoPos                        // 搬过，但目标端没留下位点（一块都没搬完）
	recopyResync                       // --resync：用户显式要求整轮重搬
	// recopyReused：目标端没有可用位点（要重搬），而表里已经有**用户自己的行**。
	// 与「once / full_interval 每轮重搬」是两种不同的话：前者是状态缺失，后者是模式语义。
	recopyReused
)

func (w recopyReason) sentence() string {
	switch w {
	case recopyGap:
		return "源端书签已经追不回目标端的位点（书签不在了，或 WAL 已被丢掉），" +
			"这段时间源端删掉的行只会留在目标端，所以必须整轮重搬"
	case recopyNoPK:
		return "这张表没有稳定的主键，没法按块续跑，只能整表重来"
	case recopyOnce:
		return "这个任务是 once / full_interval 模式，每一轮都整轮重搬（目标端结果要等于源端当前状态）"
	case recopyNoPos:
		return "之前搬过但目标端没留下一致性位点（很可能一块都没搬完就断了）"
	case recopyResync:
		return "你显式要求了 --resync（整轮重搬）：这一轮先从零重建，目标端结果要等于源端当前状态"
	case recopyReused:
		return "目标端没有可用的同步位点，这一轮要从头搬"
	default:
		return "上次全量搬到一半就断了"
	}
}

// reusedTableRecopyError stops a recopy that would have to delete rows this job
// did not write. It is E1002-ish (目标表已存在/不覆盖) but the situation is
// specific, so it carries its own sentence.
//
// 为什么必须停：不清表跑完算「成功」是最坏的结果 —— 源端删掉的行留在目标端，
// 界面上一切正常，用户以为已经一致。所以这里把选择权交回去，并给三条都做得到的路。
type reusedTableRecopyError struct {
	table string
	// jobKey / schema 现在 **不再出现在文案里**（第 3 条「手动删掉我们写的行」删掉后没人用它们），
	// 但保留字段：调用点与将来若要写进 note/审计时还要用，且删字段会让 diff 噪音掩盖行为变化。
	jobKey string
	schema string
	why    recopyReason
}

func (e *reusedTableRecopyError) Error() string {
	return fmt.Sprintf("%s，而目标表 %s 是你自己的表（不是本任务建的）：要重搬就得先清空它，"+
		"那会连你原有的行一起删掉，所以我停在这里，不替你决定。\n"+
		// 只留两条**真能走通**的路。曾经有第 3 条「手动删掉本任务写进去的行后重跑」——
		// 在「凡从头搬就清表」（#74）之后它已经失效：标记删了也会因 `copiedBefore` 整批点名，
		// 用户照做必然再撞同一句拒绝。把做不到的动作写进错误，比不给建议更糟。
		"你现在能做什么（二选一）：1) 想让我们重建：把 target.on_exists 改成 recreate（会清空这张表）；"+
		"2) 想保留原表：换一个空的 target.schema 重新初始化（旧表会留在原 schema，不再被同步）。\n"+
		"想先看清差多少：jinliang verify 会逐条报「目标端多出一行（源端没有这个主键 …）」——"+
		"这一版还没有按块补 delete 的自动修正，所以补齐要靠上面两条之一。\n"+
		"风险：选 1 会清空这张表；放着不管，源端已删除的行会一直留在目标端。",
		e.why.sentence(), e.table)
}

// initialCopyDone reports whether a previous run finished the initial copy.
func initialCopyDone(phase, position string) bool {
	if position == "" {
		return false
	}
	switch phase {
	case string(state.Catchup), string(state.Streaming), string(state.Draining),
		string(state.Done), string(state.Paused):
		return true
	}
	return false
}

func (r *runner) mode() string {
	if strings.TrimSpace(r.doc.Mode.Frequency) == "" {
		return "continuous+initial"
	}
	return r.doc.Mode.Frequency
}

// ── 阶段：预检 ──────────────────────────────────────────────

func (r *runner) preflight(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	srcConn, err := connect.Open(r.srcEP)
	if err != nil {
		return err
	}
	defer srcConn.Close()
	tgtConn, err := connect.Open(r.tgtEP)
	if err != nil {
		return err
	}
	defer tgtConn.Close()

	in := preflight.Input{
		JobName:      r.job.Name,
		SourceKind:   r.srcEP.Kind,
		TargetKind:   r.tgtEP.Kind,
		Mode:         r.mode(),
		OnExists:     r.doc.Target.OnExists,
		BatchRows:    r.batchRows(),
		SnapshotConn: r.snapshotConns(),
		Lookback:     r.doc.Mode.Lookback,
		// 水位列必须交给预检：interval 模式靠它判断「哪些是新数据」，
		// 漏传会让预检永远报「缺水位列」（这条曾经真的漏过）。
		Incremental: r.doc.Mode.IncrementalColumn,
	}
	if v, err := srcConn.Version(ctx); err != nil {
		in.SourceErr = err.Error()
	} else {
		in.SourceVer = v
	}
	if v, err := tgtConn.Version(ctx); err != nil {
		in.TargetErr = err.Error()
	} else {
		in.TargetVer = v
	}
	if in.SourceErr == "" {
		if caps, err := srcConn.Probe(ctx); err == nil {
			in.SourceCaps = caps
		}
		if cat, err := srcConn.Catalog(ctx, r.scope()); err == nil {
			in.SourceCat = &cat
			r.cat = cat
		}
	}
	if in.TargetErr == "" {
		if cat, err := tgtConn.Catalog(ctx, source.Scope{Schema: r.targetSchema()}); err == nil {
			in.TargetCat = &cat
		}
	}

	tables, err := r.selectTables()
	if err != nil {
		return err
	}
	r.tables = tables
	// 类型映射的判定**只有一处**：`preflight` 的 TYPE-01（数组 → blocked）/ TYPE-02（其它有损 → warn），
	// 由 report.CanStart 决定能不能开跑。引擎不再自己另写一份判断——两份判断迟早分叉。

	in.Tables = tableNames(tables)
	for _, t := range tables {
		for _, c := range r.cat.Tables {
			if c.Table == t.Table {
				if m, ok := pgsink.TypeMapLoss(source.Column{Type: columnType(c, "")}); ok && m.Loss > 0 {
					_ = m // 同构迁移无损；这里留着接口，跨库时才有意义
				}
			}
		}
	}

	report := preflight.Run(in)
	for _, c := range report.Checks {
		if c.Status == source.StatusOK {
			continue
		}
		r.emit(ctx, control.Event{
			Type: "note", Message: checkLine(c), Code: c.Code,
		})
	}
	if !report.CanStart {
		return &preflightBlocked{checks: report.Checks}
	}
	if len(tables) == 0 {
		return errors.New("这次没有选中任何表（source.scope / content.tables 都是空的）")
	}
	return nil
}

func checkLine(c source.Check) string {
	return fmt.Sprintf("%s %s：%s", c.ID, c.Title, c.Detail)
}

// preflightBlocked carries the blocked checks so fail() can print the first one.
type preflightBlocked struct{ checks []source.Check }

func (e *preflightBlocked) Error() string {
	for _, c := range e.checks {
		if c.Status == source.StatusBlocked {
			return c.Title + "：" + c.Detail
		}
	}
	return "预检没通过"
}

func (e *preflightBlocked) Code() core.Code {
	for _, c := range e.checks {
		if c.Status == source.StatusBlocked && c.Code != "" {
			return core.Code(c.Code)
		}
	}
	return core.CodeWalLevel
}

// ── 阶段：准备 ──────────────────────────────────────────────

func (r *runner) prepare(ctx context.Context) error {
	if err := r.phaseTo(ctx, state.Prepare); err != nil {
		return err
	}
	sink, err := r.openSink(ctx)
	if err != nil {
		return err
	}
	r.sink = sink

	ok, why, err := sink.AcquireLease(ctx, r.leaseTTL)
	if err != nil {
		return fmt.Errorf("取任务租约失败: %w", err)
	}
	if !ok {
		return fmt.Errorf("%w：%s", ErrHeldByOther, why)
	}
	if why != "" {
		r.note(ctx, why)
	}
	r.leaseOK = true
	go r.renewLease(ctx)

	if err := sink.EnsureMeta(ctx, hashOf(r.doc), "", "", ""); err != nil {
		return fmt.Errorf("写入任务状态行失败: %w", err)
	}
	// 暂停记录读在 **EnsureMeta 之后、EnsureSchema 之前**（契约 §1.2）：
	//   - 之后：状态行得先存在，否则读不到那一列；
	//   - 之前：写路径的暂停快检必须在**第一次写入前**就绪——EnsureSchema 本身可能写行，
	//     而 cleanBeforeRecopy 之后才读就已经晚了（那时表可能已经被清过一轮）。
	// 只读一次：写路径每批都要判定，不能每批查库；这一列只在本轮 prepare 与
	// 「某表搬成」两处变化。**顺序还有一条硬的：EnsureSchema 必须早于 cleanBeforeRecopy**
	//（否则会「先清空、后报结构不对」）。三条一起看，这段位置就是唯一正确的。
	if err := sink.LoadPausedTables(ctx); err != nil {
		return err
	}
	if sink.PausedDirty() {
		r.note(ctx, "目标端的暂停表记录不是合法 JSON：本轮按「没有暂停」继续（不影响同步）")
	}
	// 建表要用**目标端**的表定义（schema 换成目标端的）：源端 catalog 里的
	// schema 是源库名（MySQL 源就是库名），直接拿去建表会变成「目标端没有这个 schema」。
	// **顺序有要求：EnsureSchema 必须早于 snapshot() 里的 cleanBeforeRecopy。**
	// 清表这一步要清的表得先存在：EnsureSchema 负责建表并把「这张表是本任务建的」
	// 记进 _jl_object，cleanBeforeRecopy 正靠它决定能不能 TRUNCATE；反过来就会
	// 撞上「relation does not exist」，或者把用户的表当成自己的清掉。
	// 断言在 TestCleanRunsAfterEnsureSchema（真库：目标表不存在 + 需要清表的那一走法）。
	actions, err := sink.EnsureSchema(ctx, r.targetCatalog())
	if err != nil {
		return fmt.Errorf("准备目标表失败: %w", err)
	}
	for _, a := range actions {
		r.emit(ctx, control.Event{Type: "note", Message: a.What})
	}
	// prepare 阶段降级成提示的判定（用户表多出的列不算漂移）——必须发出来，不许无声。
	// **位置很关键：必须在 EnsureSchema 之后**——`prepareNotes` 是在它调用的
	// `reconcileColumns` 里 append 的；放在之前排空，拿到的永远是空列表（实测过：
	// 用户表那格没有 note，而用例断言不出来的话就是静默）。
	// 这里 `EnsureSchema` 已返回并释放了 `s.mu`，`PrepareNotes()` 再加锁不会重入。
	for _, n := range sink.PrepareNotes() {
		r.note(ctx, n)
	}
	src, err := r.openSource(ctx)
	if err != nil {
		return err
	}
	r.src = src
	// **只有 interval 在这里收口**：它既不清表、也不走 Apply，且 `prepare` 早退后
	// 再也走不到下面那一次调用；interval 没有「存量」概念，`copyDoneFor` 恒真是设计如此
	//（契约 §1.2 例外段）。
	//
	// 别的模式**必须**等 `decideCopyPlan` 之后再收口：`progressByTable` 是那一步才赋值，
	// 提前调会让 `chunkProgress()` 是空 map → `copyDoneFor` 走 `!ok → true` → 每张表都被判
	// 「已搬完」→ 暂停记录被无条件清掉（半截拷贝的表显示「已恢复」而数据是半空的）。
	if r.mode() == "interval" {
		if err := r.reconcilePaused(ctx); err != nil {
			return err
		}
	}
	// interval 模式不做逻辑复制（没有书签、没有发布），只要一条普通读连接；
	// 少建一个 slot 就等于少一份磁盘占用与一项权限要求。
	if r.mode() == "interval" {
		return nil
	}
	// 全量要不要做、要怎么做，看目标端的**块进度**（_jl_chunk），不靠阶段猜：
	// 快照期间每块都会写位点，所以「有位点」不等于「存量搬完了」；而「跑到过 catchup」
	// 又说不清「上次是被打断的还是搬完的」。块标记（含表完成标记）才回答得了。
	phaseInTarget, posInTarget, err := sink.State(ctx)
	if err != nil {
		return err
	}
	r.priorPhase = phaseInTarget
	plan, err := r.decideCopyPlan(ctx, src, phaseInTarget, posInTarget)
	if err != nil {
		return err
	}
	if r.opt.Resync {
		// --resync 改的是**判据**（不看上次搬到哪，直接整轮重来），不是另写一套搬运。
		if err := r.applyResync(&plan, phaseInTarget, posInTarget); err != nil {
			return err
		}
	}
	r.plan = plan
	r.needSnapshot = plan.Kind != copySkip
	if r.opt.Resync {
		// **先问「这几张表能不能清」，再动任何东西**。清位点、重建书签、清表是三步
		// 破坏性动作；把「不是本任务建的表」这种需要用户决定的失败留到最后一步，
		// 就等于先把位点和书签毁了再停下：位点没了、旧书签没了、表一行没清，用户
		// 「再跑一次」会被判成 copySkip 直接进流，把 (旧位点, 新起点] 永久跳过且
		// 不报错——他明明要的是整轮重搬，代码却替他做了「接受空洞」的决定。
		if err := r.precheckResyncTables(ctx, plan); err != nil {
			return err
		}
		// 清位点必须在 src.Prepare 之前、更要在清表之前：
		//   - 在 EnsureMeta 之前：它的 position 是 COALESCE 语义，晚清会被写回的旧位点盖掉；
		//   - 在清表之前：清表发生在 snapshot()。反过来（先清表、后清位点）一旦崩在中间，
		//     库里就是「位点说有进度、表却是空的」→ 下次按位点续跑 → 数据永久缺失且没人报错。
		if err := r.sink.ClearProgress(ctx); err != nil {
			return fmt.Errorf("清位点失败，已停下（目标表一行都没动）: %w", err)
		}
		// 审计写的是**计划**（planned_tables）：真正清完哪些表另有一条
		// job.resync.cleared，别把计划当已完成报告。
		if err := r.auditResync(ctx, plan.CleanNames); err != nil {
			return err
		}
		// continuous 没有存量：--resync 的动作全在「换一条书签」上。
		// 不做这一步的话，Prepare 会**复用**旧书签，位点根本不会动——审计写了
		// 「已清空位点」，实际却原样写回，正是「跑了但没重建」。
		if r.mode() == "continuous" {
			rb, ok := r.src.(slotRebuilder)
			if !ok {
				return fmt.Errorf("内部错误：%s 源没有重建书签的能力，但 checkResync 放行了", r.srcEP.Kind)
			}
			if err := rb.RebuildSlot(ctx); err != nil {
				return err
			}
		}
	}
	// 续跑时**不重建书签、不导出视图**：书签得留着才能从原来的位点重放变更。
	prep, err := src.Prepare(ctx, r.needSnapshot && plan.Kind != copyResume)
	if err != nil {
		return fmt.Errorf("准备源端对象失败: %w", err)
	}
	// 位点这一轮要不要动、什么时候动，是三件不同的事：
	//   - 续跑：不动（靠增量从原位点重放来收敛）；
	//   - 从头搬：**等块标记清干净了再动**，两者必须同一个事务（snapshot() 里的
	//     StartFreshCopy）——先动位点、后清标记，中间崩一次就会以「续跑」的名义丢一段变更；
	//   - 不搬：不动（位点只在数据事务里前进）。
	writePos := ""
	switch plan.Kind {
	case copyResume:
		r.consistentPoint = parsedCP(src, posInTarget)
		r.snapshotName = ""
		r.reusedSlot = false
		writePos = posInTarget // 原样写回：不挪
		r.note(ctx, plan.Why)
	default:
		if plan.Why != "" {
			// 「为什么重搬」必须说出来（docs/SNAPSHOT.md §4）：不然用户只看到又搬了一遍。
			r.note(ctx, plan.Why)
		}
		r.reusedSlot = !r.needSnapshot
		r.snapshotName = prep.SnapshotName
		r.consistentPoint = prep.ConsistentPoint
		if plan.Kind == copySkip {
			// 沿用旧行为：书签已确认的位置就是消费到的位置。
			// MySQL 源没有「书签」这种东西（没有槽位），Prepare 不会给位点——
			// 这时位点就是目标库里存着的那个，**原样写回**，不挪。
			if src.IsZero(r.consistentPoint) {
				r.consistentPoint = parsedCP(src, posInTarget)
				writePos = posInTarget
			} else {
				writePos = posString(r.consistentPoint)
			}
		}
	}
	for _, a := range prep.Actions {
		r.emit(ctx, control.Event{Type: "note", Message: a.What})
		_ = r.sink.RecordObject(ctx, objectOf(a), objectKind(a))
	}
	if err := r.sink.EnsureMeta(ctx, hashOf(r.doc), string(state.Prepare),
		writePos, prep.SnapshotName); err != nil {
		return err
	}
	// 收口必须在 `decideCopyPlan` **之后**：`progressByTable` 是那一步才赋值，
	// 提前调会让 `chunkProgress()` 是空 map、`copyDoneFor` 走 `!ok → true` → 每张表都被
	// 判成「已搬完」→ 暂停记录被无条件清掉（半截拷贝的表会显示「已恢复」而数据是半空的）。
	return r.reconcilePaused(ctx)
}

// reconcilePaused 收紧本轮的暂停记录。
//
// 跨轮存活当且仅当：job_key 未变（它就是这个任务）、job_hash 未变、该表仍在本轮表集合里。
// 任一条不成立 → 清掉并说明；清不掉的（目标端不支持）→ 明确失败，不假装写成功。
//
// 解除挂在「EnsureSchema 通过 + 存量搬完」，不是只 EnsureSchema 通过：
// 结构对齐了但存量还没搬完，这张表仍然是半截数据，不能当作恢复。
func (r *runner) reconcilePaused(ctx context.Context) error {
	rec, err := r.sink.PausedTables(ctx)
	if err != nil {
		return fmt.Errorf("读暂停表失败: %w", err)
	}
	if len(rec) == 0 {
		return nil
	}
	stored, err := r.sink.StoredJobHash(ctx)
	if err != nil {
		return fmt.Errorf("读任务指纹失败: %w", err)
	}
	sameJob := stored == "" || stored == hashOf(r.doc)
	inScope := map[string]bool{}
	for _, t := range r.tables {
		inScope[t.Table] = true
	}
	var dropped []string
	for _, key := range pgsink.PausedKeys(rec) {
		if sameJob && inScope[key] {
			continue
		}
		if err := r.sink.ClearPausedTable(ctx, source.TableRef{Schema: r.targetSchema(), Table: key}); err != nil {
			return fmt.Errorf("清掉不再适用的暂停记录 %s 失败: %w", key, err)
		}
		dropped = append(dropped, key)
	}
	if len(dropped) > 0 {
		why := "已不在本轮的表集合里"
		if !sameJob {
			why = "任务定义变了（job_hash 不同）"
		}
		r.note(ctx, fmt.Sprintf("清掉了 %d 条暂停记录（%s）：%s",
			len(dropped), why, strings.Join(dropped, "、")))
	}

	// 结构已对齐 + 存量已完成 → 解除。存量没搬完的保留，等它搬成那一刻再解除
	//（snapshot() 里 MarkTableDone 之后 ClearPausedTable）。
	for _, key := range pgsink.PausedKeys(rec) {
		if !sameJob || !inScope[key] {
			continue
		}
		if !r.copyDoneFor(key) {
			continue
		}
		if err := r.sink.ClearPausedTable(ctx, source.TableRef{Schema: r.targetSchema(), Table: key}); err != nil {
			return fmt.Errorf("解除 %s 的暂停失败: %w", key, err)
		}
		r.note(ctx, "表 "+key+" 的结构已对齐且存量已完成：解除暂停，接下来会从原位点继续重放")
	}
	return nil
}

// copyDoneFor 表示这张表的存量已经搬完（有完成标记）。查的是目标端的块进度，
// 不看 paused_tables 本身 —— 判定输入必须能只靠「块进度 + 位点」重建。
func (r *runner) copyDoneFor(table string) bool {
	pr, ok := r.chunkProgress()[table]
	if !ok {
		// 一条块标记都没有：这张表本来就没有「存量要搬」（比如 continuous 只接增量，
		// 或者上次停在第一块之前）。把它当成「存量已完成」，否则暂停永远解不开。
		return true
	}
	return pr.Done
}

// chunkProgress 是本轮 prepare 时读到的那份块进度（decideCopyPlan 里装好的）。
func (r *runner) chunkProgress() map[string]pgsink.ChunkProgress {
	if r.progressByTable == nil {
		return map[string]pgsink.ChunkProgress{}
	}
	return r.progressByTable
}

func (r *runner) renewLease(ctx context.Context) {
	interval := r.leaseTTL / 3
	if r.leaseTick > 0 {
		interval = r.leaseTick
	}
	r.watchLease(ctx, interval, func(c context.Context) (bool, error) {
		return r.sink.RenewLease(c, r.leaseTTL)
	})
}

// watchLease keeps the lease alive; the moment it is lost it cancels the whole
// run. 只有「真的停下来」才算数：emit 一句话然后继续写是最坏的结果。
func (r *runner) watchLease(ctx context.Context, interval time.Duration, renew func(context.Context) (bool, error)) {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			ok, err := renew(context.Background())
			if err != nil {
				// 一次网络抖动不算丢；连续两次续不上就当丢了（TTL 还有余量）。
				failures++
				if failures < 2 {
					continue
				}
				r.loseLease(fmt.Sprintf("续租连续失败：%v", err))
				return
			}
			failures = 0
			if !ok {
				r.loseLease("租约被别的实例接管了")
				return
			}
		}
	}
}

// loseLease marks the run as lost and stops every writer.
func (r *runner) loseLease(why string) {
	r.leaseWhy.Store(why)
	r.leaseLost.Store(true)
	r.emit(context.Background(), control.Event{
		Type: "error", Code: string(core.CodeJobRunningElsewhere),
		Message: "租约丢了（" + why + "）：本进程立刻停止写入，这一轮按失败收尾",
	})
	if r.cancelRun != nil {
		r.cancelRun()
	}
}

// leaseLostError is what the run ends with after losing the lease.
type leaseLostError struct{ why string }

func (e *leaseLostError) Error() string {
	return "租约丢了（" + e.why + "）：另一个实例接管了这个任务，本进程已停止写入。" +
		"这一轮记成失败，重启会从目标端 _jl_meta 的位点接着跑"
}

func (r *runner) leaseLostError() error {
	why, _ := r.leaseWhy.Load().(string)
	return &leaseLostError{why: why}
}

func (r *runner) teardown() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if r.sink != nil {
		if r.leaseOK {
			_ = r.sink.ReleaseLease(ctx)
		}
		_ = r.sink.Close()
	}
	closeTarget(r.gsink)
	if r.src != nil {
		r.src.Close()
	}
}

// ── 阶段：全量 ──────────────────────────────────────────────

func (r *runner) snapshot(ctx context.Context) error {
	if err := r.phaseTo(ctx, state.Snapshot); err != nil {
		return err
	}
	if r.plan.Kind == copyResume {
		// 续跑：**不清表、不清块标记**。已提交的块就是已完成的工作，重读它们既慢又
		// 会把已经收敛的数据再写一遍。唯一要重来的是不能块级续跑的表（无主键）。
		for _, ref := range r.plan.Recopy {
			if err := r.recopyAction(ctx, ref, recopyNoPK); err != nil {
				return err
			}
			if err := r.sink.ResetTableChunks(ctx, r.targetRef(ref)); err != nil {
				return fmt.Errorf("清空 %s 准备整表重来时失败: %w", ref.Table, err)
			}
			r.note(ctx, ref.Table+" 没有稳定的主键，只能整表重来（不清其它表）")
		}
	} else {
		if err := r.cleanBeforeRecopy(ctx); err != nil {
			if r.opt.Resync {
				// 清到一半失败：把**已经清掉的**表落进审计（job.resync.partial），
				// 让「清了哪几张」在审计里自洽，而不是只活在易失的事件流里。
				r.resyncReport.Tables = r.clearedTables
				if aerr := r.auditResyncPartial(ctx, err); aerr != nil {
					return fmt.Errorf("%w（另：写 job.resync.partial 审计失败：%v）", err, aerr)
				}
			}
			return err
		}
		if r.opt.Resync {
			// 报告与「实清」审计都在这里定稿：清完才填，不拿计划冒充结果。
			r.resyncReport.Tables = r.clearedTables
			if err := r.auditResyncCleared(ctx); err != nil {
				return err
			}
		}
		// 开始一次新的搬：**清块标记 + 推位点，同一个事务**（pgsink.StartFreshCopy）。
		// 分两次写、还先推位点的话，中间崩一次会留下「新位点 + 上一轮的块标记」，
		// 下次被判成续跑：旧游标接着搬、增量从新位点重放 → (旧位点,新位点] 的
		// 增/改/删永远不重放，还以退出码 0 收尾。
		if err := r.sink.StartFreshCopy(ctx, r.consistentPoint.String()); err != nil {
			return fmt.Errorf("开始新一轮全量（清块进度 + 定位点）失败: %w", err)
		}
	}
	// 续跑时没有导出的视图（视图随上个进程一起没了），靠新的 REPEATABLE READ；
	// 从头搬时必须有一个导出的视图，否则「读到的数据」和「增量起点」之间会漏一段。
	if r.src.RequiresExportedSnapshot() && r.snapshotName == "" && r.plan.Kind != copyResume {
		return errors.New("拿不到一致性视图（书签已存在但导出失败）：请用 --resync 重新初始化")
	}
	opt := source.SnapshotOpt{ChunkRows: r.chunkRows(), Conns: r.snapshotConns()}
	if opt.Conns > 1 {
		r.note(ctx, fmt.Sprintf("全量按单连接顺序读（并发读落在 B2）；源端只被一个读连接占用，不会压住业务库"))
	}

	start := time.Now()
	var chunks, tables int
	_, err := r.src.SnapshotChunksFrom(ctx, opt, r.plan.From,
		func(c context.Context, ref source.TableRef, no int, rows [][]any, from, to string) error {
			tref := r.targetRef(ref)
			pos := posString(r.consistentPoint)
			if err := r.sink.BulkApply(c, r.sink.Table(tref), no, rows, pos, from, to); err != nil {
				return err
			}
			r.rowsCopied += int64(len(rows))
			chunks++
			r.progress(c, fmt.Sprintf("全量：%s 第 %d 块，%d 行", ref.Table, no, len(rows)))
			return nil
		})
	if err != nil {
		return err
	}
	r.src.EndSnapshot()
	// 给「这次搬过的每张表」盖完成标记。放在所有块之后做，崩在盖章之前也不会丢数据：
	// 下次续跑从上次的游标往后读只会拿到 0 行，然后重新盖章。
	for _, t := range r.selectedTables() {
		if r.plan.Skip[t.Table] {
			continue
		}
		ref := r.targetRef(source.TableRef{Schema: t.Schema, Table: t.Table})
		// 搬成 → 解除这张表的暂停（**只删这一个 key**，别的表记录留着）。
		// 契约：「搬成清该表，搬败留记录」。
		if err := r.sink.ClearPausedTable(ctx, ref); err != nil {
			return fmt.Errorf("解除 %s 的暂停失败: %w", t.Table, err)
		}
		if err := r.sink.MarkTableDone(ctx, r.targetRef(source.TableRef{Schema: t.Schema, Table: t.Table})); err != nil {
			return fmt.Errorf("记录 %s 的搬完标记失败: %w", t.Table, err)
		}
		tables++
	}
	// 先把「存量已搬完」写进 _jl_meta，再离开 snapshot 阶段：崩在这之后不会
	// 重搬全量；崩在这之前会重扫一遍（upsert 幂等），方向是安全的。
	if err := r.sink.SetPhase(ctx, string(state.Catchup)); err != nil {
		return err
	}
	elapsed := time.Since(start)
	rate := float64(r.rowsCopied) / elapsed.Seconds()
	r.emit(ctx, control.Event{Type: "note",
		Message: fmt.Sprintf("全量完成：%d 行，%d 块，%d 张表，用时 %s（%.0f 行/秒）",
			r.rowsCopied, chunks, tables, elapsed.Truncate(time.Millisecond), rate)})
	r.snapshotDone = true
	if r.opt.Resync {
		// 搬成才解除按表暂停：清表时那些记录是**故意**留着的（结构可能还没对齐）。
		if err := r.clearPauseRecords(ctx); err != nil {
			return err
		}
	}
	return nil
}

// cleanBeforeRecopy empties the rows this job wrote before, before copying again.
//
// Ark 口径（2026-11-10）：**凡是从头搬就清**（不是「位点坏了才清」）。BulkApply 只有
// upsert、没有 delete，不清空就永远补不上源端删掉的行；绑在位点状态上还会让同一个模式
// 因位点不同而对 delete 表现不同。
//
// 清哪些表由 plan.CleanNames 决定（只清我们写过的表），顺序就是 selectedTables() 的顺序：
// 崩在中间时「清过哪几张」要能对上，用例也才稳定。
//
// 清不动就停下要一个显式决定（用户自己的表）：不能静默降级成「不清表跑完算成功」——
// 那等于把删除追不回来的事实藏起来。
func (r *runner) cleanBeforeRecopy(ctx context.Context) error {
	if r.plan.Clean == cleanNone || len(r.plan.CleanNames) == 0 {
		return nil
	}
	for _, name := range r.plan.CleanNames {
		ref, ok := r.tableByName(name)
		if !ok {
			continue
		}
		if err := r.recopyAction(ctx, ref, r.plan.CleanWhy); err != nil {
			// 非 --resync 路径，这里是**唯一**的判定点（原样报给用户）。
			// --resync 已经在前置里过了同一道判定；能走到这里还失败，只可能是
			// 「预检与真清之间所有权变了」（`_jl_object` 被外部改过）——属内部错误面，
			// 原样上报即可，不另写一套文案（换文案会让人以为是新的用户可修问题）。
			return err
		}
		// 清数据和清标记**必须同一个事务**：中间崩在「数据清了、标记还在」那种
		// 状态，重启会把空的表当成「这几块已经搬完」——静默丢行。
		if err := r.sink.ResetTableChunks(ctx, r.targetRef(ref)); err != nil {
			return fmt.Errorf("清空 %s 准备重搬失败: %w", name, err)
		}
		// **清成一张记一张**：清到一半崩/失败时，审计里也要能说出「已经清掉哪几张」，
		// 而不是只在事件流里翻（进程一重启就没了）。
		r.clearedTables = append(r.clearedTables, name)
		// 说清维护窗口的形状：清完到搬完之间，目标端是**部分数据**，不是「旧但完整」。
		r.note(ctx, "清空了 "+name+" 里上次搬进去的行（重搬全量，避免留下源端已删除的行）；"+
			"从这一刻到这张表搬完之前，目标端是部分数据，不是旧但完整的快照")
	}
	return nil
}

// ── 阶段：追赶 + 增量 ────────────────────────────────────────

func (r *runner) stream(ctx context.Context, res *Result) error {
	// 从快照过来才是「追赶」（Prepare→Catchup 不是合法迁移）；续跑直接进增量。
	if r.phase == state.Snapshot || r.phase == state.Catchup {
		if err := r.phaseTo(ctx, state.Catchup); err != nil {
			return err
		}
	} else if err := r.phaseTo(ctx, state.Streaming); err != nil {
		return err
	}
	from := r.consistentPoint
	if r.reusedSlot {
		if raw, err := r.sink.Position(ctx); err == nil && raw != "" {
			// 位点串的格式归连接器所有：引擎不自己解析（契约 2026-11-05）。
			parsed, err := r.src.ParsePosition(raw)
			if err != nil {
				return err
			}
			if err := assertPositionKind(parsed, r.src.Kind()); err != nil {
				return err
			}
			from = parsed
		}
	}
	target, err := r.src.Now(ctx)
	if err != nil {
		return err
	}

	r.mu.Lock()
	r.lastPos = from
	r.mu.Unlock()

	// 事件里的表名是源端的，目标端只认自己的 schema：这里统一换一次（恒等映射时无副作用）。
	b := newBatcher(newRefMappedSink(r.sink, r.targetRef), r.src, r.doc, r.batchRows(), r.batchDelay(), from, func(rows int64, pos source.Position) {
		r.rowsStream += rows
		r.mu.Lock()
		r.lastPos = pos
		r.mu.Unlock()
	})
	b.onFlush = func(applied int64, pos source.Position) {
		r.progress(ctx, fmt.Sprintf("增量：已应用 %d 行，位点 %s", applied, posString(pos)))
		if !r.src.Before(pos, target) && r.phase == state.Catchup {
			if err := r.phaseTo(context.Background(), state.Streaming); err == nil {
				r.note(context.Background(), "已追平初始化期间的变更，转入常态增量")
			}
		}
	}

	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
	// 后台提交失败要立刻打断流：否则错误被吞在 goroutine 里，任务卡在「实时同步中」。
	b.onError = func(error) { cancelStream() }
	go b.flushLoop(streamCtx)

	var stopHit, idleDone atomic.Bool
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
	lagEvery := 3 * time.Second
	lastLag := time.Now()
	lastAdvance := time.Now()
	// 没有变更时 pgoutput 只发心跳：靠「一段时间没有数据事件且没有待提交批」
	// 判定已追平，而不是等一条永远不来的 Commit。
	go func() {
		t := time.NewTicker(300 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-streamCtx.Done():
				return
			case <-t.C:
				rows, inTx := b.Pending()
				_, pos := b.Applied()
				idle := time.Since(time.Unix(0, lastEvent.Load()))
				quiet := rows == 0 && !inTx

				if quiet && r.caughtUpAt(pos, target, r.src.ServerEnd()) && r.phase == state.Catchup {
					if err := r.phaseTo(context.Background(), state.Streaming); err == nil {
						r.note(context.Background(), "已追平初始化期间的变更，转入常态增量")
					}
				}
				// 源库安静时把位点推到「两次心跳都没动」的那个位置：不推的话书签
				// 会把 WAL 一直攒着（同步一张表、源库在写别的表时尤其明显）。
				if quiet {
					if p, ok := r.src.SafeAckPoint(); ok && time.Since(lastAdvance) > time.Second {
						if err := b.AdvanceTo(context.Background(), p); err == nil {
							lastAdvance = time.Now()
							_, pos = b.Applied()
						}
					}
				}

				var lag int64 = -1
				if quiet && time.Since(lastLag) > lagEvery {
					lastLag = time.Now()
					if n, err := r.src.Backlog(context.Background(), pos); err == nil {
						lag = n
						if n > 0 {
							r.note(context.Background(), fmt.Sprintf("落后 %s（位点 %s）", humanLag(n), posString(pos)))
						}
					}
				}
				// --stop-when-idle：安静够久 + 源端积压已经很小才收尾。
				// 只看安静是不够的：重放 2.5 GB 变更流的时候一个数据事件都没有，
				// 那种「安静」恰恰说明还在追赶。位点是每批提交的，提前收尾也不会丢——
				// 没收到的东西仍在书签里，下次接着流。
				if r.opt.IdleAfter > 0 && quiet && idle > r.opt.IdleAfter && lag >= 0 && lag <= r.lagThreshold() {
					idleDone.Store(true)
					stopHit.Store(true)
					cancelStream()
					return
				}
			}
		}
	}()

	streamErr := r.src.Stream(streamCtx, from, func(c context.Context, ev source.Event) error {
		select {
		case <-c.Done():
			return c.Err()
		default:
		}
		lastEvent.Store(time.Now().UnixNano())
		if err := b.Add(c, ev); err != nil {
			return err
		}
		if stopHit.Load() {
			return &stoppedError{msg: fmt.Sprintf("到达本次运行的时间上限（%s），已收尾", r.opt.StopAfter)}
		}
		return nil
	})
	cancelStream()
	// 先取后台提交的根因再 Flush：Flush 失败时用户看到的应该是「目标表没有唯一键」，
	// 而不是重试途中的 `conn closed`（Owl II P2）。顺序反过来就会盖住根因。
	cause := b.Err()
	if err := b.Flush(context.Background()); err != nil {
		if cause != nil {
			return cause
		}
		return err
	}
	// 后台提交失败过：它是真正的原因，而 streamErr 只是被它 cancel 掉之后的
	// 「流被打断」。先报真原因，别让用户看到一句「上下文取消」。
	if ferr := b.Err(); ferr != nil {
		return ferr
	}
	if streamErr != nil {
		var stopped *stoppedError
		if errors.As(streamErr, &stopped) {
			return stopped
		}
		if idleDone.Load() {
			return &stoppedError{ok: true, msg: fmt.Sprintf("已经追平且安静了 %s：按 --stop-when-idle 正常收尾", r.opt.IdleAfter)}
		}
		if stopHit.Load() {
			return &stoppedError{msg: fmt.Sprintf("到达本次运行的时间上限（%s），已收尾", r.opt.StopAfter)}
		}
		if ctx.Err() != nil {
			return &stoppedError{msg: "收到停止信号：已把手上这个事务提交完"}
		}
		return streamErr
	}
	// 追加而不是赋值：迁移失败（列已存在那类良性失败）留的 early note 不能被盖掉。
	emitEarlyNote(res, "增量到运行结束（continuous）")
	return nil
}

// ── 收尾 ────────────────────────────────────────────────────

// drain ends the run: current transaction already committed by the batcher,
// position is durable, phase goes Draining → Done.
func (r *runner) drain(ctx context.Context, exit int, msg string) error {
	r.exitCode = exit
	dctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if r.phase != state.Draining && !state.CanTransition(r.phase, state.Draining) {
		// 有些阶段不能直接进 Draining（例如 once 发现「没有可搬的存量」时还停在
		// Prepare）。按状态机允许的路径补一步空转，而不是硬写一个非法终态。
		if state.CanTransition(r.phase, state.Streaming) {
			if err := r.phaseTo(dctx, state.Streaming); err != nil {
				return err
			}
		}
	}
	if r.phase != state.Draining && state.CanTransition(r.phase, state.Draining) {
		if err := r.phaseTo(dctx, state.Draining); err != nil {
			return err
		}
	}
	if r.src != nil && !r.src.IsZero(r.lastPos) {
		if err := r.src.Advance(dctx, r.lastPos); err != nil {
			r.note(dctx, "把书签往前推时出错（不影响已提交的数据）："+err.Error())
		}
	}
	// finish_reason 说清「为什么结束」：正常跑完 completed、用户按停 stopped。
	// 阶段都落 done——「停止」不是失败，别在历史页把它显示成失败。
	reason := control.FinishCompleted
	if exit == ExitStopped {
		reason = control.FinishStopped
	}
	if _, err := r.opt.Store.FinishRun(dctx, r.run.ID, state.Done, "", exit, reason); err != nil {
		return err
	}
	if st := r.stateSink(); st != nil {
		_ = st.SetPhase(dctx, string(state.Done))
	}
	r.phase = state.Done
	r.emit(dctx, control.Event{Type: "done", Phase: state.Done, Message: msg})
	return nil
}

// emitEarlyNote 在 runner 还不存在时也能把一句话带到结果里（CLI 会打印 Note）。
func emitEarlyNote(res *Result, msg string) {
	if res.Note != "" {
		res.Note += "；"
	}
	res.Note += msg
}

// pauseHead 拼 last_error 的前缀。**表名可能为空**（错误被别处包装过、或 typed error
// 没填字段）：宁可只写原因码，也不要在界面上留下「（表 ，原因码 x）」这种残句。
func pauseHead(base string, pe pgsink.PausableError) string {
	if tbl := pe.Table(); tbl != "" {
		return fmt.Sprintf("%s（表 %s，原因码 %s）", base, tbl, pe.Reason())
	}
	return fmt.Sprintf("%s（原因码 %s）", base, pe.Reason())
}

// registerPause 把「这张表要暂停」记进目标端的 paused_tables。
//
// 目标端不支持（MySQL 没有那条状态列）时**明确失败**：绝不能「UPDATE 影响 0 行当成功」，
// 那会让用户以为暂停登记生效了，而下一轮又从头踩一遍。
func (r *runner) registerPause(ctx context.Context, err error) error {
	var pe pgsink.PausableError
	if !errors.As(err, &pe) || pe.Table() == "" {
		return nil
	}
	if r.tgtEP.Kind != dsn.KindPostgres {
		return &pauseUnsupportedError{kind: r.tgtEP.Kind, table: pe.Table(), inner: err}
	}
	if r.sink == nil {
		return nil // 还没开目标端：原始错误照常上抛
	}
	ref := r.targetRef(source.TableRef{Schema: r.targetSchema(), Table: pe.Table()})
	detail := err.Error()
	if len(detail) > 500 {
		detail = detail[:500] + "…"
	}
	if perr := r.sink.PauseTable(ctx, ref, pe.Reason(), detail); perr != nil {
		return fmt.Errorf("%w（另外：登记这张表的暂停状态也失败了：%v）", err, perr)
	}
	r.note(ctx, "已登记暂停：表 "+pe.Table()+"（原因码 "+pe.Reason()+"），位点没有推进")
	return nil
}

// pauseUnsupportedError：目标端不支持按表暂停（本版只有 PG 目标端有那条状态列）。
type pauseUnsupportedError struct {
	kind  string
	table string
	inner error
}

func (e *pauseUnsupportedError) Error() string {
	return "表 " + e.table + " 的结构和源端对不上，需要按表暂停；而当前目标端（" + e.kind + "）" +
		"还没有暂停记录这一列，本版无法登记。\n" +
		"你现在能做什么：1) 把目标端的表结构手工对齐后重跑（位点没推进，不会重搬历史）；" +
		"2) 或者把这张表移出同步范围。\n" +
		"风险：不对齐就继续写会让数据与列错位；而我们不会「假装登记成功」。原始原因：" + e.inner.Error()
}

func (e *pauseUnsupportedError) Unwrap() error { return e.inner }

// classifyError 决定这条错误该挂哪个码。**纯函数**，好测、也好评审。
//
// 纪律（2026-11-06 裁决）：兜底**不再落 E4002**。E4002 在用户眼里是「津梁自己的 bug，
// 请提 issue」——默认落它等于把所有未分类错误都归成我们的锅，方向错得最彻底。
// 未分类就如实说「未识别」，并给出排查方向；已识别但码表还没编号的（如 42501/3F000）
// 先给文字方向，等 Ark 的 E1012/E1013 落地再挂码。
func classifyError(err error) (code core.Code, classified bool) {
	if err == nil {
		return "", false
	}
	var pb *preflightBlocked
	var foreign *pgsource.ErrPositionForeignSource
	var kindErr *positionKindError
	var tooBig *txTooLargeError
	var lost *leaseLostError
	switch {
	case errors.As(err, &pb):
		return pb.Code(), true
	case errors.Is(err, ErrHeldByOther), errors.As(err, &lost):
		return core.CodeJobRunningElsewhere, true
	case errors.Is(err, pgsink.ErrTargetTableExists), errors.As(err, new(*reusedTableRecopyError)):
		return core.CodeTargetTableExists, true
	case errors.As(err, &foreign), errors.As(err, &kindErr):
		return core.CodePositionKindMismatch, true
	case errors.As(err, &tooBig):
		return core.CodeTxTooLarge, true
	case errors.As(err, new(*pauseUnsupportedError)):
		// 目标端没有「按表暂停」这条状态列：方向/能力不支持，不是这条同步本身错了。
		return core.CodeUnsupportedDirection, true
	case errors.Is(err, pgsink.ErrCollationMismatch):
		// 排序规则不一致 = 同一个文本列在两端比较/判重行为不同，属于「结构对不上」。
		// 挂 E1015（不另开码）：用户要做的动作和类型不一致那条完全一样——对齐结构后重跑；
		// 另开一个码只会让用户多查一次「这个码和那个码有什么不同」。
		return core.CodeSchemaMismatch, true
	case errors.As(err, new(pgsink.PausableError)):
		// 「这张表的结构对不上」：prepare 补列时类型不一致，或跑起来后源端改类型/改名/删列。
		// 目标写成**接口指针**（不是 *TypeChangedError）：将来第三个实现也能匹配上。
		// 具体是哪一种、哪张表哪一列由 fail() 拼进 last_error——**码表说类别，运行时说具体**。
		return core.CodeSchemaMismatch, true
	case errors.As(err, new(*TableNotFoundError)):
		// 我们自己判定的配置错误：源端清单里没有这张表（选错表 / 改名 / 不在这个 schema 下）。
		// 文案以码表 E1013 为准；**不造伪 SQLSTATE**——那是协议语义，塞假值会带偏所有
		// 按 SQLSTATE 分类的地方。
		return core.CodeObjectMissing, true
	case pgsource.IsNoPrivilege(err):
		// 42501：源端 SELECT 或目标端 DDL/DML 被拒——用户要做的事都是「去要授权」。
		return core.CodeNoPrivilege, true
	case pgwire.SQLState(err) == "53400":
		// all replication slots are in use：权限够、资源满（E1016）。
		// 位置：排在 42501（权限不足）之后——那是**另一个码、另一类动作**；
		// 53400 与 3F000/42P01（对象不存在）互斥（一个错误只带一个 SQLSTATE），
		// 放在这里只是就近，不构成「必须在这之后」的约束。
		// 用户要做的事只有三件（见码表 E1016 的 Next）；last_error 原样保留 SQLSTATE 与消息。
		return core.CodeSourceResourceLimit, true
	case pgsource.IsObjectMissing(err):
		// 3F000/42P01：schema 或表不存在。next 里按 SQLSTATE 分流（缺 schema 建 schema、
		// 缺表别去建 schema），文案以码表为准，不走 hint()。
		return core.CodeObjectMissing, true
	case pgsource.IsWalLevelTooLow(err):
		// 运行期撞上「源库没开逻辑变更流」：用户要做的事与预检时一模一样，复用 E1001。
		return core.CodeWalLevel, true
	case pgsource.IsPositionGap(err):
		return core.CodePositionGap, true
	}
	return "", false
}

// heartbeatLoop 周期刷新本条 run 的存活心跳；只在 run 未结束时可写（TouchRun 自己带条件）。
func (r *runner) heartbeatLoop(ctx context.Context, interval time.Duration, touch func(context.Context) error) {
	// 间隔的默认值只在 heartbeatEvery() 里算一份；这里拿到的一定是有效值。
	if interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	failing := false // 只在状态转换点发 note：15 秒一条会把 200 条环形缓冲铺满并挤掉真正的 progress/error
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			err := touch(ctx)
			if ctx.Err() != nil {
				return
			}
			switch {
			case err != nil && !failing:
				failing = true
				r.emit(ctx, control.Event{Type: "note",
					Message: "刷新运行心跳失败（状态页可能把这一轮判成「已中断」）：" + err.Error()})
			case err == nil && failing:
				failing = false
				r.emit(ctx, control.Event{Type: "note", Message: "运行心跳已恢复"})
			}
		}
	}
}

// heartbeatEvery 是心跳间隔：control 的 DefaultHeartbeatInterval 的一半。
func (r *runner) heartbeatEvery() time.Duration {
	if r.opt.HeartbeatEvery > 0 {
		return r.opt.HeartbeatEvery
	}
	return control.DefaultHeartbeatInterval / 2
}

// isUserStop reports「这次失败其实是用户按了停止」。
//
// **要区分的是「谁 cancel 的」，不是「超时还是取消」**：`errors.Is(Canceled)` 与
// `errors.Is(DeadlineExceeded)` 互不匹配，但 `ctx.Err()` 那一半不可靠——带 deadline
// 的 ctx 若先被 parent cancel，返回的也是 Canceled。所以这里只认「ctx 被父级取消
// 且不是我们自己 cancel 的」：内部 cancel（`loseLease`）在 fail() 里已被提前拦掉，
// 调用方保证不会走到这里。
func isUserStop(ctx context.Context, err error) bool {
	if errors.Is(err, context.Canceled) {
		return true
	}
	return ctx != nil && errors.Is(ctx.Err(), context.Canceled)
}

// failLostLease 是「租约被接管/续不上」的收尾：interrupted + E1005，**不是**用户停止。
// 之前它会被误判成 stopped（因为 loseLease 自己 cancel 了 runCtx），历史页上看不出真原因。
func (r *runner) failLostLease(ctx context.Context, err error) error {
	dctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	why, _ := r.leaseWhy.Load().(string)
	msg := "租约丢了（" + why + "）：另一个实例接管了这个任务，本进程已停止写入"
	ev := control.ErrorEvent(r.job.ID, r.run.ID, core.CodeJobRunningElsewhere, msg)
	ev.Phase = r.phase
	r.opt.Store.Bus().Publish(ev)
	if r.phase != state.Failed {
		if _, ferr := r.opt.Store.FinishRun(dctx, r.run.ID, state.Failed, msg, ExitFailed,
			control.FinishInterrupted); ferr != nil {
			return fmt.Errorf("%w（收尾也失败：%v）", err, ferr)
		}
		r.phase = state.Failed
	}
	return err
}

// stopEarly 在「还没动数据」的阶段按停止收尾：finish_reason=stopped + exit_code=130。
// FinishRun 对 FinishStopped 允许跳过阶段表（#5 的约定：停止不是失败，阶段落 done），
// 所以 preflight/prepare 也能直接收成 done。
func (r *runner) stopEarly(cause error) error {
	dctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := r.opt.Store.FinishRun(dctx, r.run.ID, state.Done, "", ExitStopped,
		control.FinishStopped); err != nil {
		return fmt.Errorf("按停止收尾失败: %w（原始原因: %v）", err, cause)
	}
	r.exitCode = ExitStopped
	r.phase = state.Done
	msg := "已手动停止（在动数据之前）：没有写入任何数据，下次 run 从头开始"
	if r.sink != nil {
		_ = r.sink.SetPhase(dctx, string(state.Done))
	}
	r.emit(dctx, control.Event{Type: "done", Phase: state.Done, Message: msg})
	return nil
}

// hint 给「已识别类别、但码表还没有编号」的错误一句可执行的方向。
// 只写方向，不发明码；等 Ark 的 E1012/E1013 落地后由 classifyError 挂上。
func hint(err error) (string, []string) {
	// 注意：42501 / 3F000 / 42P01 已经在 classifyError 里挂了 E1012 / E1013，
	// 这里**不再接管它们**——否则同一类错误会有两套 why/next，迟早对不上。
	// 这个函数只服务「已知类别、但码表还没编号」的错误。
	_ = err
	return "未识别的数据库错误：这条错误还没分类，我们不想给你一个错误的处理方向。",
		[]string{"把这次的日志（含 SQLSTATE 与原始报错）发给维护者，我们补进码表",
			"如果是权限或对象不存在，上面那句提示可能已经指向原因"}
}

func (r *runner) fail(ctx context.Context, err error) error {
	// 丢租约优先于「用户停止」：loseLease 自己也会 cancel(runCtx)，所以丢租约之后
	// 那个 ctx 带着 Canceled 进来——先判停止会把它记成「用户点的」。
	//
	// 承重的是 `leaseLost` 这个 flag：`loseLease()` 先置 flag 再 cancel，而所有
	// `fail(ctx, leaseLostError())` 的调用点都在 `if r.leaseLost.Load()` 里面，
	// 所以两个条件今天同真同假。`errors.As` 那半是**兜底**：将来若有人把
	// `leaseLostError` 包一层再传进来（不再经过那三处检查），这里仍能认出来。
	if r.leaseLost.Load() || errors.As(err, new(*leaseLostError)) {
		return r.failLostLease(ctx, err)
	}
	// 「这张表结构对不上」这一类失败要**登记暂停**（给用户看、给下一轮 prepare 用）。
	// 登记是独立一条 UPDATE（pgsink.PauseTable），绝不与 position 同条：位点在数据
	// 事务里本就没推进（这一批整个回滚了），暂停登记不能再顺手把它带走。
	if perr := r.registerPause(ctx, err); perr != nil {
		return perr
	}
	if isUserStop(ctx, err) {
		return r.stopEarly(err)
	}
	dctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	code, classified := classifyError(err)
	var ev control.Event
	lastError := err.Error()
	var np *notImplementedError
	switch {
	case errors.As(err, &np):
		// 没有错误码可挂：这不是「你做错了什么」，是我们还没做。
		ev = control.Event{JobID: r.job.ID, RunID: r.run.ID, Type: "error", Message: err.Error(), Phase: r.phase}
	case classified:
		ev = control.ErrorEvent(r.job.ID, r.run.ID, code, err.Error())
		ev.Phase = r.phase
		// 有码也要把方向写进 last_error：历史页在 code 为空时只有这条字符串。
		// 不写的话，「该怎么处理」只活在事件流里（进程一重启就没了）。
		if spec, ok := core.Lookup(code); ok {
			// 「按表暂停」这一族要额外把**表名与原因码**写进去，而且放在**开头**：
			// 用户拿到这条错误，第一件要问的是「哪张表」，其次才是「怎么办」。
			// 码表说类别，运行时说具体——这条边界由 Ark 给 E1015 时定下。
			head := spec.Message
			var pe pgsink.PausableError
			if errors.As(err, &pe) {
				head = pauseHead(head, pe)
			}
			lastError = fmt.Sprintf("[%s] %s：%s 下一步：%s",
				code, head, err.Error(), strings.Join(spec.Next, "；"))
		} else {
			lastError = fmt.Sprintf("[%s] %s", code, err.Error())
		}
	default:
		// 未分类：如实说，不给假码（尤其不落 E4002「我们的 bug」）。
		why, next := hint(err)
		ev = control.Event{
			JobID: r.job.ID, RunID: r.run.ID, Type: "error", Phase: r.phase,
			Message: why + " 原始错误：" + err.Error(),
			Next:    next,
		}
		// 排查方向必须落进 runs.last_error：事件是易失的（进程一重启就没了），
		// 而用户回头找原因看的是历史页。诊断信息只在事件流里活一次，等于没做。
		lastError = why + " 原始错误：" + err.Error() + " 下一步：" + strings.Join(next, "；")
	}
	r.opt.Store.Bus().Publish(ev)

	reason := control.FinishFailed
	var lost *leaseLostError
	if errors.As(err, &lost) {
		reason = control.FinishInterrupted // 租约被接管 = 被中断，不是「这条同步本身错了」
	}
	if r.phase != state.Failed {
		// 把分类结果写进 runs.code：列表页只显示「徽标 + 一句摘要」，全文留给详情页。
		// classified=false 时 code 为空——不编造码（未分类就该显示「未分类」）。
		codeStr := ""
		if classified {
			codeStr = string(code)
		}
		if _, ferr := r.opt.Store.FinishRunWithCode(dctx, r.run.ID, state.Failed, lastError, ExitFailed, reason, codeStr); ferr != nil {
			return fmt.Errorf("%w（收尾也失败：%v）", err, ferr)
		}
		r.phase = state.Failed
	}
	return err
}

// stoppedError ends the stream early. ok means "this is a normal finish"
// (caught up + idle), not a human pressing Ctrl-C.
// notImplementedError marks a segment B1 deliberately did not build. It is not
// a user-action error: there is nothing to fix on their side, they just need a
// mode that exists.
type notImplementedError struct{ msg string }

func (e *notImplementedError) Error() string { return e.msg }

type stoppedError struct {
	msg string
	ok  bool
}

func (e *stoppedError) Error() string { return e.msg }

// stateSink 返回「能写阶段/位点的那个目标端」：PG 目标端是 pgsink（r.sink），
// 非 PG 目标端是 gsink 的状态表能力（mysqlsink）。两条路径共用同一套阶段语义。
func (r *runner) stateSink() stateCapable {
	if r.sink != nil {
		return r.sink
	}
	if st, ok := r.gsink.(stateCapable); ok {
		return st
	}
	return nil
}

func (r *runner) phaseTo(ctx context.Context, to state.Phase) error {
	if r.phase == to {
		return nil
	}
	if r.phase != "" && !state.CanTransition(r.phase, to) {
		return fmt.Errorf("内部错误：非法阶段迁移 %s → %s", r.phase, to)
	}
	if _, err := r.opt.Store.UpdateRunPhase(ctx, r.run.ID, to); err != nil {
		return err
	}
	r.phase = to
	// 阶段必须真的落到目标端：PG 目标端写 pgsink（r.sink），非 PG 目标端写它的
	// 状态表能力。少了后半句，「上一轮崩在哪个阶段」这条唯一可重算的事实永远是
	// 'prepare'——引擎就分不清「这张用户表我没碰过」和「我搬到一半崩了」。
	if st := r.stateSink(); st != nil {
		_ = st.SetPhase(ctx, string(to))
	}
	r.emit(ctx, control.Event{Type: "phase", Phase: to, Message: control.PhaseLabel(to)})
	return nil
}

// progress writes rows/lag into the control database and publishes it, so both
// the CLI and the SSE page show the same numbers.
func (r *runner) progress(ctx context.Context, msg string) {
	if r.opt.Store == nil {
		return
	}
	rows := r.rowsCopied + r.rowsStream
	var lagB *int64
	if r.src != nil {
		if n, err := r.src.Backlog(ctx, r.lastPos); err == nil {
			lagB = &n
			r.mu.Lock()
			r.lastLagB = n
			r.mu.Unlock()
		}
	}
	_ = r.opt.Store.SetRunProgress(ctx, r.run.ID, rows, lagB, nil)
	r.emit(ctx, control.Event{Type: "progress", Phase: r.phase, Message: msg})
}

func (r *runner) note(ctx context.Context, msg string) {
	r.emit(ctx, control.Event{Type: "note", Phase: r.phase, Message: msg})
}

func (r *runner) emit(ctx context.Context, ev control.Event) {
	if r.emitHook != nil { // 单测要数「发了几条」时挂这里（不在测的路径上留后门）
		r.emitHook(ev)
	}
	if r.opt.Store == nil { // 单测里没有控制库：事件流不是被测对象
		return
	}
	ev.JobID = r.job.ID
	ev.RunID = r.run.ID
	if ev.Phase == "" {
		ev.Phase = r.phase
	}
	r.opt.Store.Bus().Publish(ev)
}

// ── 配置取值（都在这里，别处不许再猜默认值） ────────────────

func (r *runner) batchRows() int {
	if r.opt.BatchRows > 0 {
		return r.opt.BatchRows
	}
	if r.doc.Reliability != nil && r.doc.Reliability.BatchRows != nil {
		return *r.doc.Reliability.BatchRows
	}
	return control.DefaultBatchRows
}

func (r *runner) batchDelay() time.Duration {
	if r.opt.BatchDelay > 0 {
		return r.opt.BatchDelay
	}
	if r.doc.Reliability != nil {
		if d, err := time.ParseDuration(r.doc.Reliability.BatchDelay); err == nil && d > 0 {
			return d
		}
	}
	return time.Millisecond
}

func (r *runner) chunkRows() int64 {
	if r.doc.Source.Snapshot != nil && r.doc.Source.Snapshot.ChunkRows != nil {
		return int64(*r.doc.Source.Snapshot.ChunkRows)
	}
	return control.DefaultChunkRows
}

// lagThreshold is how much unreplayed WAL still counts as "caught up".
func (r *runner) lagThreshold() int64 {
	if r.opt.LagThreshold > 0 {
		return r.opt.LagThreshold
	}
	if r.doc.Guard != nil && r.doc.Guard.SlotWarnLag != "" {
		if n, ok := parseSize(r.doc.Guard.SlotWarnLag); ok && n > 0 {
			return n
		}
	}
	return 8 << 20
}

// parseSize turns "5GB" into bytes.
func parseSize(s string) (int64, bool) {
	s = strings.ToUpper(strings.TrimSpace(s))
	mult := int64(1)
	switch {
	case strings.HasSuffix(s, "TB"):
		mult, s = 1<<40, strings.TrimSuffix(s, "TB")
	case strings.HasSuffix(s, "GB"):
		mult, s = 1<<30, strings.TrimSuffix(s, "GB")
	case strings.HasSuffix(s, "MB"):
		mult, s = 1<<20, strings.TrimSuffix(s, "MB")
	case strings.HasSuffix(s, "KB"):
		mult, s = 1<<10, strings.TrimSuffix(s, "KB")
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	var n int64
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
		n = n*10 + int64(s[i]-'0')
	}
	return n * mult, true
}

func (r *runner) snapshotConns() int {
	if r.doc.Source.Snapshot != nil && r.doc.Source.Snapshot.Conns != nil {
		return *r.doc.Source.Snapshot.Conns
	}
	return 1
}

// targetSchema 是「目标端真实落点 schema」的**唯一取值入口**。
//
// 它必须与自由函数 schemaOf 同值（Run() 在 runner 存在之前也要算这个值），
// 所以这里直接委托——**不另写一份回落逻辑**。
//
// 之前这里回落 r.tgtEP.Database（数据库名），而 PG 的默认 schema 是 public：
// 默认配置的任务被算成「源端 public ≠ 目标端 jinliang」，DDL 闸门把每一条都拒了，
// 文案还很像真的。同一个判断两处实现，就一定会分叉（今晚这是第四次）。
func (r *runner) targetSchema() string {
	return schemaOf(r.doc, control.Connection{Kind: r.tgtEP.Kind, Database: r.tgtEP.Database})
}

func (r *runner) publication() string {
	name, err := state.ObjectName("publication", r.job.JobKey, 0)
	if err != nil {
		return "jl_" + r.job.JobKey
	}
	return name
}

func (r *runner) slot() string {
	name, err := state.ObjectName("slot", r.job.JobKey, 1)
	if err != nil {
		return "jl_" + r.job.JobKey + "_1"
	}
	return name
}

func scopeOf(doc control.JobDoc) source.Scope {
	sc := source.Scope{}
	if doc.Source.Scope != nil {
		sc.Schema = doc.Source.Scope.Schema
		sc.Include = doc.Source.Scope.Include
		sc.Exclude = doc.Source.Scope.Exclude
	}
	return sc
}

// scope 是「源端选表范围」的**唯一取值入口**：它必须知道源的种类。
//
// 默认 schema 不是常量：PG 的默认 schema 是 public，MySQL 的「schema」就是库名
// （连接里那个）。一律回落 public 会让 MySQL 源读到 0 张表，而报出来的话是
// 「没读到源端的表清单（权限不足或 schema 写错）」——把人引向权限，实际是名字错了。
func (r *runner) scope() source.Scope {
	sc := scopeOf(r.doc)
	if sc.Schema == "" {
		if r.srcEP.Kind == dsn.KindMySQL {
			sc.Schema = r.srcEP.Database
		} else {
			sc.Schema = "public"
		}
	}
	return sc
}

// TableNotFoundError 表示用户选中的表在源端清单里不存在。
//
// 单独成类型（而不是 fmt.Errorf）是为了让 classifyError 用 errors.As 挂码：
// 这条失败从来不是「未识别的驱动错误」，而是**我们判定出来的配置错误**。
type TableNotFoundError struct {
	Table     string
	Schema    string
	Available []string // 最接近的几个表名（拼写错误时最有用的线索）
}

// Error 只陈述**事实**（表名 / schema / 相近名）。
//
// 动作与风险**交给码表**：`lastError` 的落盘格式是
// `[E1013] <码表 Message>：<err.Error()> 下一步：<码表 Next>`，
// 如果这里也写一遍「你能做什么…风险…」，用户在历史页会读到两段措辞不同的动作。
// **同一事实只留一处表达**——这条今晚已经踩过好几次。
func (e *TableNotFoundError) Error() string {
	msg := fmt.Sprintf("源端找不到表 %s（schema %s）：可能表名写错、被改名，或不在这个 schema 下",
		e.Table, orDash(e.Schema))
	// MySQL 源这条会误判：元数据可见，但**账号对该表没有 SELECT** 时，清单里也可能看不到它。
	// 一句话把「其实有表、只是没权限」这条路指出来，别让人一直改表名。
	msg += "。若你确认这张表存在，检查账号对它是否有 SELECT 权限（MySQL 源的元数据会按权限过滤）"
	if len(e.Available) > 0 {
		// 只列**源端表名**（不列列名、不列数据），信息暴露边界写在这里。
		msg += "。源端相近的表名：" + strings.Join(e.Available, "、")
	}
	return msg
}

// nearestNames 给出与目标名最接近的几个表名，拼错时最有用。
//
// 排序按「像不像」而不是「catalog 里先出现谁」：
//  1. 前缀命中优先于包含命中（`ord` → `orders` 比 `work_order_log` 更像）；
//  2. 同档内按「长度差 + 编辑距离」升序。
//
// **短名（<3）不做 contains 匹配**：表名是 `t` 时，双向前缀/包含会把「恰好含字母 t 的前 5 张表」
// 都列出来——那不是相近而是噪音，而这行提示一旦名不副实，用户就再也不信它了。
//
// 只是提示，**不做模糊匹配的自动替换**：选错表比报错更危险。
func nearestNames(want string, tables []source.Table) []string {
	lw := strings.ToLower(strings.TrimSpace(want))
	if lw == "" {
		return nil
	}
	type cand struct {
		name  string
		rank  int
		score int
	}
	// 允许的编辑距离：短名严格（1），长名放宽到 1/3 长度——同时候选必须**真的像**，
	// 宁可少列（用户还能自己看表清单），也不要列出噪音让人不再信这行提示。
	maxDist := len([]rune(lw)) / 3
	if maxDist < 1 {
		maxDist = 1
	}
	var cands []cand
	for _, t := range tables {
		lt := strings.ToLower(t.Table)
		d := editDistance(lt, lw)
		score := abs(len(lt)-len(lw)) + d
		switch {
		case strings.HasPrefix(lt, lw) || strings.HasPrefix(lw, lt):
			cands = append(cands, cand{name: t.Table, rank: 0, score: score})
		case len(lw) >= 3 && len(lt) >= 3 && (strings.Contains(lt, lw) || strings.Contains(lw, lt)):
			cands = append(cands, cand{name: t.Table, rank: 1, score: score})
		case d <= maxDist:
			cands = append(cands, cand{name: t.Table, rank: 2, score: score})
		}
	}
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].rank != cands[j].rank {
			return cands[i].rank < cands[j].rank
		}
		if cands[i].score != cands[j].score {
			return cands[i].score < cands[j].score
		}
		return cands[i].name < cands[j].name
	})
	out := make([]string, 0, 5)
	for _, c := range cands {
		out = append(out, c.name)
		if len(out) == 5 {
			break
		}
	}
	return out
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}

// editDistance 是 Levenshtein 距离（表名都很短，O(n²) 足够）。
func editDistance(a, b string) int {
	ra, rb := []rune(a), []rune(b)
	prev := make([]int, len(rb)+1)
	cur := make([]int, len(rb)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(ra); i++ {
		cur[0] = i
		for j := 1; j <= len(rb); j++ {
			cost := 1
			if ra[i-1] == rb[j-1] {
				cost = 0
			}
			cur[j] = min3(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
		}
		prev, cur = cur, prev
	}
	return prev[len(rb)]
}

func min3(a, b, c int) int {
	m := a
	if b < m {
		m = b
	}
	if c < m {
		m = c
	}
	return m
}

func orDash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "（默认）"
	}
	return s
}

// selectTables resolves content.tables (explicit) or scope include/exclude
// (patterns) against the source catalog.
func (r *runner) selectTables() ([]source.TableRef, error) {
	if r.cat.Tables == nil {
		return nil, errors.New("没读到源端的表清单（权限不足或 schema 写错）")
	}
	byName := map[string]source.Table{}
	for _, t := range r.cat.Tables {
		byName[t.Table] = t
	}
	var out []source.TableRef
	if r.doc.Content != nil && len(r.doc.Content.Tables) > 0 {
		for name := range r.doc.Content.Tables {
			t, ok := byName[name]
			if !ok {
				// 这里是**我们自己判定出来**的失败（表清单里就是没有它），原因非常明确：
				// 选错表 / 表改名 / 不在这个 schema 下。所以给它一个 typed error，让
				// classifyError 挂上 E1013——否则用户看到的是「未分类」，而这条恰恰是
				// 最常见的配置错误之一。
				//
				// 注意：**不造伪 SQLSTATE**。SQLSTATE 是协议的语义，塞假值会污染
				// pgwire.SQLState 的判据，将来任何按 SQLSTATE 分类的地方都被带偏。
				return nil, &TableNotFoundError{Table: name, Schema: scopeOf(r.doc).Schema,
					Available: nearestNames(name, r.cat.Tables)}
			}
			out = append(out, t.TableRef)
		}
		sortRefs(out)
		return out, nil
	}
	sc := r.scope()
	for _, t := range r.cat.Tables {
		if included(t.Table, sc.Include) && !excluded(t.Table, sc.Exclude) {
			out = append(out, t.TableRef)
		}
	}
	sortRefs(out)
	return out, nil
}

func (r *runner) tableByName(name string) (source.TableRef, bool) {
	for _, t := range r.tables {
		if t.Table == name {
			return t, true
		}
	}
	return source.TableRef{}, false
}

func (r *runner) selectedTables() []source.Table {
	sel := map[string]bool{}
	for _, t := range r.tables {
		sel[t.Schema+"."+t.Table] = true
	}
	var out []source.Table
	for _, t := range r.cat.Tables {
		if sel[t.Schema+"."+t.Table] {
			out = append(out, t)
		}
	}
	return out
}

func included(name string, patterns []string) bool {
	if len(patterns) == 0 {
		return true
	}
	for _, p := range patterns {
		if ok, _ := path.Match(p, name); ok {
			return true
		}
	}
	return false
}

func excluded(name string, patterns []string) bool {
	for _, p := range patterns {
		if ok, _ := path.Match(p, name); ok {
			return true
		}
	}
	return false
}

func sortRefs(rs []source.TableRef) {
	for i := 1; i < len(rs); i++ {
		for j := i; j > 0 && rs[j].Table < rs[j-1].Table; j-- {
			rs[j], rs[j-1] = rs[j-1], rs[j]
		}
	}
}

func tableNames(rs []source.TableRef) []string {
	out := make([]string, len(rs))
	for i, r := range rs {
		out[i] = r.Table
	}
	return out
}

func hashOf(doc control.JobDoc) string {
	h := fnv32(doc.ScopeFingerprint() + "|" + doc.Mode.Frequency + "|" + doc.Target.OnExists)
	return fmt.Sprintf("%08x", h)
}

func fnv32(s string) uint32 {
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

func columnType(t source.Table, name string) string {
	for _, c := range t.Columns {
		if c.Name == name {
			return c.Type
		}
	}
	return ""
}

func objectOf(a source.Action) string {
	if s, ok := strings.CutPrefix(a.ID, "slot:"); ok {
		return s
	}
	if s, ok := strings.CutPrefix(a.ID, "publication:"); ok {
		return s
	}
	return a.ID
}

func objectKind(a source.Action) string {
	switch {
	case strings.HasPrefix(a.ID, "slot:"):
		return "slot"
	case strings.HasPrefix(a.ID, "publication:"):
		return "publication"
	}
	return "other"
}
