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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spencerswagger/jinliang/internal/connect"
	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/core"
	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/pgsink"
	"github.com/spencerswagger/jinliang/internal/pgsource"
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
	// LeaseTTL defaults to 30s.
	LeaseTTL time.Duration
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
	// 源端目前只有 PG 的读实现；目标端允许 PG 与 MySQL，其它类型在构造阶段报清楚。
	if srcConn.Kind != dsn.KindPostgres {
		return res, fmt.Errorf("源端类型 %q 还没有读实现（现在支持 postgres 源）。"+
			"你现在能做什么：把源连接换成 PostgreSQL；MySQL 源在下个版本。"+
			"风险：没有（什么都没动）", srcConn.Kind)
	}
	if tgtConn.Kind != dsn.KindPostgres && tgtConn.Kind != dsn.KindMySQL {
		return res, fmt.Errorf("目标端类型 %q 还不支持（现在支持 postgres / mysql）。"+
			"你现在能做什么：把目标连接的 kind 改成 postgres 或 mysql。"+
			"风险：没有（什么都没动）", tgtConn.Kind)
	}
	srcEP := endpointOf(srcConn, srcSecret)
	tgtEP := endpointOf(tgtConn, tgtSecret)

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
		holder, err := pgsink.ReadLease(ctx, ep, schema, job.JobKey)
		if err != nil || !holder.Abandoned(time.Now(), pgsink.ProcessAlive) {
			return false, nil
		}
		_, err = store.FinishRun(ctx, run.ID, state.Failed,
			"上一次运行随进程退出中断（kill -9 或断电）；进度以目标端 _jl_meta 为准，重启会从断点续跑",
			ExitFailed, control.FinishInterrupted)
		if err != nil {
			return false, fmt.Errorf("收尾上一次被中断的运行失败: %w", err)
		}
		return true, nil
	}
	return false, nil
}

// schemaOf is targetSchema as a free function (Run() needs it before the runner exists).
func schemaOf(doc control.JobDoc, tgt control.Connection) string {
	if s := strings.TrimSpace(doc.Target.Schema); s != "" {
		return s
	}
	return tgt.Database
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

	src  *pgsource.Source
	sink *pgsink.Sink
	// gsink 是「按接口」的目标端：PG 用 pgsink（COPY 快路径），MySQL 用 mysqlsink
	// （走契约的 Apply）。构造阶段确定，之后引擎只用接口。
	gsink  targetSink
	cat    source.Catalog
	tables []source.TableRef

	phase   state.Phase
	leaseOK bool
	// 丢掉租约必须让整个 run 停下来：光 emit 一句「本进程停止写入」而写路径照跑，
	// 等于两个进程同时写同一张目标表。
	leaseLost atomic.Bool
	leaseWhy  atomic.Value // string
	cancelRun context.CancelFunc
	leaseTick time.Duration

	snapshotName    string
	consistentPoint pgsource.Position
	reusedSlot      bool
	needSnapshot    bool
	priorPhase      string

	rowsCopied int64
	rowsStream int64
	exitCode   int

	mu       sync.Mutex
	lastPos  pgsource.Position
	lastLagB int64
}

func (r *runner) execute(parent context.Context, res *Result) error {
	ctx, cancel := context.WithCancel(parent)
	r.cancelRun = cancel
	defer cancel()

	defer func() {
		res.Phase = r.phase
		res.RowsCopied = r.rowsCopied
		res.RowsStream = r.rowsStream
		res.Position = r.lastPos.String()
		res.ExitCode = r.exitCode
	}()

	if err := r.preflight(ctx); err != nil {
		return r.fail(ctx, err)
	}
	// 目标端不是 PG：走「只做全量」的通用路径（用契约里的 sink 接口，不按类型分支）。
	if r.tgtEP.Kind != dsn.KindPostgres {
		if err := r.prepareGeneric(ctx); err != nil {
			return r.fail(ctx, err)
		}
		defer r.teardown()
		if err := r.executeGenericFullCopy(ctx, res); err != nil {
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

	if err := r.prepare(ctx); err != nil {
		return r.fail(ctx, err)
	}
	defer r.teardown()

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
		res.Note = "只搬了存量数据（mode.frequency=" + r.mode() + "）"
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
		res.Note = "interval 模式：只跑了水位增量（整表重搬仅发生在第一次或没有水位列的表）"
		return nil
	}
	if r.mode() == "full_interval" {
		if err := r.drain(ctx, ExitOK, "全量周期重跑本轮完成"); err != nil {
			return err
		}
		res.Note = "full_interval 模式：本轮整表重跑；下一轮由调度器（--watch）触发"
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
func caughtUpAt(applied, startupTarget, serverWALEnd pgsource.Position) bool {
	target := startupTarget
	if serverWALEnd.Compare(target) > 0 {
		target = serverWALEnd
	}
	if target.IsZero() {
		return false
	}
	return applied.Compare(target) >= 0
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
// 返回 (true, nil) 表示可以清；(false, err) 表示必须停下。
func (r *runner) recopyAction(ctx context.Context, ref source.TableRef) (bool, error) {
	ours, err := r.sink.DidCreateTable(ctx, ref)
	if err != nil {
		return false, fmt.Errorf("查不到目标表 %s 是谁建的（读 _jl_object 失败）: %w", ref.Table, err)
	}
	if ours {
		return true, nil
	}
	return false, &reusedTableRecopyError{table: ref.Table, jobKey: r.job.JobKey, schema: ref.Schema}
}

// reusedTableRecopyError stops a recopy that would have to delete rows this job
// did not write. It is E1002-ish (目标表已存在/不覆盖) but the situation is
// specific, so it carries its own sentence.
type reusedTableRecopyError struct {
	table  string
	jobKey string
	schema string
}

func (e *reusedTableRecopyError) Error() string {
	return fmt.Sprintf("上次全量搬到一半就断了，而目标表 %s 是你自己的表（不是本任务建的）："+
		"重搬会先清空它，那会连你原有的行一起删掉，所以我停在这里。\n"+
		"你现在能做什么：1) 想让我们重建：把 target.on_exists 改成 recreate（会清空这张表）；"+
		"2) 想保留原表：手动删掉本任务写进去的行后重跑（进度表 %s._jl_chunk 里有 job_key=%s 与块范围）；"+
		"3) 换一个空的 target.schema。\n"+
		"风险：选 1 会清空这张表。", e.table, e.schema, e.jobKey)
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
		if cat, err := srcConn.Catalog(ctx, scopeOf(r.doc)); err == nil {
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
	actions, err := sink.EnsureSchema(ctx, r.cat)
	if err != nil {
		return fmt.Errorf("准备目标表失败: %w", err)
	}
	for _, a := range actions {
		r.emit(ctx, control.Event{Type: "note", Message: a.What})
	}

	src, err := r.openSource(ctx)
	if err != nil {
		return err
	}
	r.src = src
	// interval 模式不做逻辑复制（没有书签、没有发布），只要一条普通读连接；
	// 少建一个 slot 就等于少一份磁盘占用与一项权限要求。
	if r.mode() == "interval" {
		return nil
	}
	// 全量要不要做，看 _jl_meta 的阶段而不是位点：快照期间每块都会写位点，
	// 所以「有位点」不等于「存量搬完了」。只有到过 catchup/streaming 才算完。
	phaseInTarget, posInTarget, err := sink.State(ctx)
	if err != nil {
		return err
	}
	r.priorPhase = phaseInTarget
	// once = 「搬一次」：再跑一次就应该再搬一次（upsert 幂等），不能因为上次跑完过
	// 就什么都不做然后假装成功。只有 continuous+initial 才按「上次到哪了」续跑。
	// once / full_interval = 「每次都要重搬」；continuous+initial 才按上次到哪了续跑。
	alwaysCopy := r.mode() == "once" || r.mode() == "full_interval"
	r.needSnapshot = r.mode() != "continuous" && r.mode() != "interval" &&
		(alwaysCopy || !initialCopyDone(phaseInTarget, posInTarget))
	prep, err := src.Prepare(ctx, pgsource.PrepareOptions{Snapshot: r.needSnapshot})
	if err != nil {
		return fmt.Errorf("准备源端对象失败: %w", err)
	}
	r.reusedSlot = !r.needSnapshot
	src.SetSnapshotName(prep.SnapshotName)
	r.snapshotName = prep.SnapshotName
	r.consistentPoint = prep.ConsistentPoint
	for _, a := range prep.Actions {
		r.emit(ctx, control.Event{Type: "note", Message: a.What})
		_ = r.sink.RecordObject(ctx, objectOf(a), objectKind(a))
	}
	if err := r.sink.EnsureMeta(ctx, hashOf(r.doc), string(state.Prepare),
		r.consistentPoint.String(), prep.SnapshotName); err != nil {
		return err
	}
	return nil
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
	// 上一次全量没搬完就断了：目标端留着「上次搬到一半的行」。新的全量用的是新
	// 视图，那些行既不全也过期（源端期间可能删过行），必须先清掉，否则搬完会多出
	// 源端已经没有的行 —— 那就是「重」。
	//
	// 但「清」只能清**本任务自己建的表**：append 模式会复用用户已有的表，那张表
	// 里的行不全是我们的，清了就是删用户数据（Owl 审出来的 P0）。
	if r.priorPhase == string(state.Snapshot) {
		done, err := r.sink.DoneChunks(ctx)
		if err != nil {
			return err
		}
		for name := range done {
			ref, ok := r.tableByName(name)
			if !ok {
				continue
			}
			truncate, err := r.recopyAction(ctx, ref)
			if err != nil {
				return err
			}
			if !truncate {
				continue
			}
			if err := r.sink.TruncateTable(ctx, ref); err != nil {
				return fmt.Errorf("清空上次没搬完的 %s 失败: %w", name, err)
			}
			r.note(ctx, "清空了上次没搬完的 "+name+"（重搬全量，避免留下源端已删除的行）")
		}
	}
	// 新的一次全量用的是一个新导出视图，旧的块进度标记没有意义。
	if err := r.sink.ResetChunks(ctx); err != nil {
		return err
	}
	if r.snapshotName == "" {
		return errors.New("拿不到一致性视图（书签已存在但导出失败）：请用 --resync 重新初始化")
	}
	opt := source.SnapshotOpt{ChunkRows: r.chunkRows(), Conns: r.snapshotConns()}
	if opt.Conns > 1 {
		r.note(ctx, fmt.Sprintf("全量按单连接顺序读（并发读落在 B2）；源端只被一个读连接占用，不会压住业务库"))
	}

	start := time.Now()
	var tables int
	_, err := r.src.SnapshotChunks(ctx, opt, func(c context.Context, ch pgsource.Chunk) error {
		pos := r.consistentPoint.String()
		if err := r.sink.BulkApply(c, r.sink.Table(ch.Table), ch.No, ch.Rows, pos, ch.From, ch.To); err != nil {
			return err
		}
		r.rowsCopied += int64(len(ch.Rows))
		tables++
		r.progress(c, fmt.Sprintf("全量：%s 第 %d 块，%d 行", ch.Table.Table, ch.No, len(ch.Rows)))
		return nil
	})
	if err != nil {
		return err
	}
	r.src.EndSnapshot()
	// 先把「存量已搬完」写进 _jl_meta，再离开 snapshot 阶段：崩在这之后不会
	// 重搬全量；崩在这之前会重扫一遍（upsert 幂等），方向是安全的。
	if err := r.sink.SetPhase(ctx, string(state.Catchup)); err != nil {
		return err
	}
	elapsed := time.Since(start)
	rate := float64(r.rowsCopied) / elapsed.Seconds()
	r.emit(ctx, control.Event{Type: "note",
		Message: fmt.Sprintf("全量完成：%d 行，%d 块，用时 %s（%.0f 行/秒）",
			r.rowsCopied, tables, elapsed.Truncate(time.Millisecond), rate)})
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
			p, ok := parsed.(pgsource.Position)
			if !ok {
				return fmt.Errorf("%w：这个源给出的位点类型是 %T，引擎不认识", source.ErrPositionMalformed, parsed)
			}
			from = p
		}
	}
	target, err := r.src.CurrentLSN(ctx)
	if err != nil {
		return err
	}

	r.mu.Lock()
	r.lastPos = from
	r.mu.Unlock()

	b := newBatcher(r.sink, r.src, r.doc, r.batchRows(), r.batchDelay(), from, func(rows int64, pos pgsource.Position) {
		r.rowsStream += rows
		r.mu.Lock()
		r.lastPos = pos
		r.mu.Unlock()
	})
	b.onFlush = func(applied int64, pos pgsource.Position) {
		r.progress(ctx, fmt.Sprintf("增量：已应用 %d 行，位点 %s", applied, pos.String()))
		if pos.Compare(target) >= 0 && r.phase == state.Catchup {
			if err := r.phaseTo(context.Background(), state.Streaming); err == nil {
				r.note(context.Background(), "已追平初始化期间的变更，转入常态增量")
			}
		}
	}

	streamCtx, cancelStream := context.WithCancel(ctx)
	defer cancelStream()
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

				if quiet && caughtUpAt(pos, target, r.src.ServerWALEnd()) && r.phase == state.Catchup {
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
					if n, err := r.src.SlotLagBytes(context.Background()); err == nil {
						lag = n
						if n > 0 {
							r.note(context.Background(), fmt.Sprintf("落后 %s（位点 %s）", humanLag(n), pos.String()))
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
	if err := b.Flush(context.Background()); err != nil {
		return err
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
	res.Note = "增量到运行结束（continuous）"
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
	if r.src != nil && !r.lastPos.IsZero() {
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
	if r.sink != nil {
		_ = r.sink.SetPhase(dctx, string(state.Done))
	}
	r.phase = state.Done
	r.emit(dctx, control.Event{Type: "done", Phase: state.Done, Message: msg})
	return nil
}

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
	case pgsource.IsNoPrivilege(err):
		// 42501：源端 SELECT 或目标端 DDL/DML 被拒——用户要做的事都是「去要授权」。
		return core.CodeNoPrivilege, true
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
		// 有码也要把方向写进 last_error：run 行**不存 code**，历史页只有这条字符串。
		// 不写的话，「该怎么处理」只活在事件流里（进程一重启就没了）。
		if spec, ok := core.Lookup(code); ok {
			lastError = fmt.Sprintf("[%s] %s：%s 下一步：%s",
				code, spec.Message, err.Error(), strings.Join(spec.Next, "；"))
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
		if _, ferr := r.opt.Store.FinishRun(dctx, r.run.ID, state.Failed, lastError, ExitFailed, reason); ferr != nil {
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
	if r.sink != nil {
		_ = r.sink.SetPhase(ctx, string(to))
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
		if n, err := r.src.SlotLagBytes(ctx); err == nil {
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

func (r *runner) targetSchema() string {
	if s := strings.TrimSpace(r.doc.Target.Schema); s != "" {
		return s
	}
	return r.tgtEP.Database
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
	if sc.Schema == "" {
		sc.Schema = "public"
	}
	return sc
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
				return nil, fmt.Errorf("源端找不到表 %s（可能改名了，或不在 %s 下）", name, scopeOf(r.doc).Schema)
			}
			out = append(out, t.TableRef)
		}
		sortRefs(out)
		return out, nil
	}
	sc := scopeOf(r.doc)
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
