package httpapi

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/core"
	"github.com/spencerswagger/jinliang/internal/jobrun"
	"github.com/spencerswagger/jinliang/internal/state"
)

// 控制面这一侧只需要管三件事：谁在跑、怎么停、停完怎么收尾。
// 阶段推进、事务、位点全部在 internal/engine，这里不重复实现。

// runRegistry 记住「本进程里正在跑的任务」对应的 cancel 函数。
//
// 为什么需要它：engine.Run 是同步阻塞的，WebUI 的「开始」不能把 HTTP 请求挂住，
// 所以起 goroutine 跑；而「停止」只能通过取消 ctx 实现（引擎没有 pause）。
// 同一任务同时只允许一条活跃 run，这一层在控制库里由 idx_runs_one_active 兜住，
// 这里再挡一次是为了给用户一个立刻可读的 409，而不是等引擎报错。
type runRegistry struct {
	mu sync.Mutex
	by map[string]*runningJob
}

type runningJob struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// fallbackNext 是「没有错误码」时的下一步：不编造原因，只给能自己动手的三条。
// 为什么需要它：只有 ErrConflict 那条会填 Event.Next，而最常见的失败恰好没有码
// （权限、schema 写错、连不上）——审计里留个 null 的 next 等于把问题丢回给用户。
var fallbackNext = []string{
	// 第 1 条不能写「去看最近一次运行」：这条 audit 恰恰多是「还没开跑」的场景，
	// 没有 run 行，按提示去看会什么都没看到。原始报错就在摘要里。
	"原始报错就在上面的摘要里；若引擎已经开跑，再去看那条 run 的 last_error（任务页「最近一次运行」）",
	"按报错里的关键词自查最常见三处：连接能否连通、源端表/schema 名是否写对、账号有没有 SELECT 与建表权限",
	// 第 3 条不能只说「发给维护者」：自托管场景里用户往往就是维护者，那等于没说。
	"还解决不了：记下错误摘要（含 SQLSTATE）与任务 id——自建部署发给你们的 DBA/运维，用上游项目就提 issue",
}

func newRunRegistry() *runRegistry {
	return &runRegistry{by: map[string]*runningJob{}}
}

// claim 占位；已被占用时返回 false。
func (r *runRegistry) claim(jobID string, cancel context.CancelFunc) (*runningJob, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, busy := r.by[jobID]; busy {
		return nil, false
	}
	rj := &runningJob{cancel: cancel, done: make(chan struct{})}
	r.by[jobID] = rj
	return rj, true
}

// release 清位并关闭 done。
func (r *runRegistry) release(jobID string) {
	r.mu.Lock()
	rj, ok := r.by[jobID]
	delete(r.by, jobID)
	r.mu.Unlock()
	if ok {
		close(rj.done)
	}
}

// stop 取消一条正在跑的 run；不在跑返回 false。
func (r *runRegistry) stop(jobID string) bool {
	r.mu.Lock()
	rj, ok := r.by[jobID]
	r.mu.Unlock()
	if !ok {
		return false
	}
	rj.cancel()
	return true
}

// active 报告是否在跑。
func (r *runRegistry) active(jobID string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.by[jobID]
	return ok
}

// activeSyncRun 从控制库读「这个任务当前有没有未结束的同步 run」。
//
// 用控制库而不是内存：进程重启后内存登记是空的，但刚被 kill 的 run 行还在
// （ended_at IS NULL，靠心跳过期由 serve 启动时收尾），此时再起一条会被
// idx_runs_one_active 挡住。先查库能把这种情况如实告诉用户。
// runHeartbeatFresh 用与控制面收尾同一口径（StaleAfter = 3×心跳）判断那条 run 是否还活着。
func (s *Server) runHeartbeatFresh(r *control.Run) bool {
	if r == nil || r.HeartbeatAt == "" {
		return false
	}
	at, err := time.Parse(time.RFC3339, r.HeartbeatAt)
	if err != nil {
		return false
	}
	return time.Since(at) < control.StaleAfter(s.store.HeartbeatInterval())
}

func (s *Server) activeSyncRun(ctx context.Context, jobID string) (*control.Run, error) {
	runs, err := s.store.ListRuns(ctx, jobID, 5)
	if err != nil {
		return nil, err
	}
	for i := range runs {
		r := runs[i]
		if r.EndedAt == "" && r.Kind == control.RunKindSync {
			return &r, nil
		}
	}
	return nil, nil
}

// startJob 起一条真执行（engine.Run），立刻返回。
//
// 失败不是「弹一句运行失败」：引擎会把四段式写进 run 行的 last_error，并通过
// SSE 发一条 error 事件（带 code / next），前端照它渲染。
// 两种「这个任务起不来」的原因必须分开说：它们的下一步完全不同。
var (
	// errThisProcessBusy：本进程里就有一条在跑（内存登记）。
	errThisProcessBusy = fmt.Errorf("%w：这台机器上已有另一个 jinliang 进程在跑这个任务", control.ErrRunInProgress)
	// errStaleRunInControlDB：心跳过期，说明是上个进程被强杀留下的尸体。
	errStaleRunInControlDB = fmt.Errorf("%w：控制库里还有一条上次没结束的运行", control.ErrRunInProgress)
)

// otherInstanceRunningError 表示「另一个实例正在跑这个任务」，并带上**是哪一台**。
// 用户被建议「去那台机器上 stop」，就必须能在界面上看到实例标识；把它放进结构体
// 而不是拼进字符串，是为了让上层用 errors.As 取值——正则解析自己的错误文本太脆。
type otherInstanceRunningError struct {
	RunID         string
	OwnerInstance string
	HeartbeatAt   string
}

func (e *otherInstanceRunningError) Error() string {
	return fmt.Sprintf("%v：另一个实例正在跑这个任务（run %s，实例 %s，最后心跳 %s）",
		control.ErrRunInProgress, e.RunID, e.OwnerInstance, e.HeartbeatAt)
}

// Unwrap 让 errors.Is(err, control.ErrRunInProgress) 仍然成立。
func (e *otherInstanceRunningError) Unwrap() error { return control.ErrRunInProgress }

// asOtherInstance 取出「另一个实例在跑」这个错误里的实例信息。
// 用 errors.As 而不是 errors.Is：这个错误带字段，哨兵指针比较永远不相等。
func asOtherInstance(err error) *otherInstanceRunningError {
	var oi *otherInstanceRunningError
	if errors.As(err, &oi) {
		return oi
	}
	return nil
}

// 注意：这里**不再**判断「连接类型组合能不能跑」。
//
// 判断只该有一处——引擎的构造阶段（newTargetSink 按连接类型选 pgsink/mysqlsink，
// 不认识的类型当场四段式）。HTTP 入口再判一次，两处迟早分叉：今晚已经因为
// 「同一个判断散在多处」出过好几次（IsLoopbackListen vs IsLoopbackRequest、文档阈值
// 与代码常量、E1013 的分类器与码表）。能力边界前移曾经是必要的（那时引擎会先 202
// 然后什么都不发生），但那个洞现在由「引擎返回错误 → 补一条 error 事件 + audit」
// 兜住了，所以入口不必再自己认一遍类型。
func (s *Server) startJob(ctx context.Context, job control.Job, actor string) error {
	if s.runs.active(job.ID) {
		// 本进程里就有一条在跑：这是最常见的情况，文案不能说「控制库里有一条」——
		// 那里可能什么都没有，按那句话去查会白忙。
		return errThisProcessBusy
	}
	if run, err := s.activeSyncRun(ctx, job.ID); err != nil {
		return err
	} else if run != nil {
		// 控制库里有一条没结束的 run。**心跳是否新鲜决定它在干什么**：
		// 新鲜 = 另一个实例正在写（绝不能建议重启 serve——那会把别人的同步打断）；
		// 过期 = 上个进程被强杀留下的尸体，这时才谈得上「等收尾 / 重启」。
		if s.runHeartbeatFresh(run) {
			// 用结构化错误带上实例标识：上层要让用户「去那台机器上 stop」，
			// 就必须能把那台机器显示出来——不能靠正则解析自己的错误字符串。
			return &otherInstanceRunningError{
				RunID: run.ID, OwnerInstance: run.OwnerInstance, HeartbeatAt: run.HeartbeatAt,
			}
		}
		return fmt.Errorf("%w（run %s，owner_instance=%s，最后心跳 %s）",
			errStaleRunInControlDB, run.ID, run.OwnerInstance, run.HeartbeatAt)
	}

	// 故意用 context.Background：run 的生命周期比 HTTP 请求长，请求返回后还要继续跑。
	runCtx, cancel := context.WithCancel(context.Background())
	if _, ok := s.runs.claim(job.ID, cancel); !ok {
		cancel()
		return control.ErrRunInProgress
	}
	_ = s.store.Audit(ctx, actor, control.ActionJobRun, map[string]any{"job_id": job.ID, "engine": "b1"})

	// 只有一个 goroutine：跑引擎，跑完（或取消后）释放占位。
	//
	// 之前这里写成两个 goroutine（一个 <-done 后 release、一个跑引擎）——那是死锁：
	// done 只有 release 才关，而 release 又排在 <-done 之后，于是永远不释放，
	// 同一个任务第二次 run 会永远 409。P0，已由 TestStartJobReleasesAfterFinish 钉住。
	run := s.runFn
	go func() {
		defer func() {
			cancel()
			s.runs.release(job.ID)
		}()
		_, err := run(runCtx, s.store, job, jobrun.Options{Actor: actor})
		if err == nil || errors.Is(err, context.Canceled) {
			return
		}
		// 引擎在「还没建出 run 行」之前就失败时（典型：另有一个进程持有活跃 run），
		// 它自己不会发事件——HTTP 已经回了 202，客户端会以为「跑了但什么都没发生」。
		// 这里补一条 error 事件，让事件流上一定看得见原因。
		s.logf("job %s 未能开始: %v", job.ID, err)
		// 这一帧不属于任何 run（引擎还没建出 run 行），所以 RunID 留空、
		// Phase 只是「失败发生在开跑前」的标注——UI 对 error 不做阶段推进。
		ev := control.Event{
			JobID:   job.ID,
			Type:    "error",
			Phase:   state.Preflight,
			Message: err.Error(),
		}
		if errors.Is(err, control.ErrConflict) {
			ev.Code = string(core.CodeJobRunningElsewhere)
			if spec, ok := core.Lookup(core.CodeJobRunningElsewhere); ok {
				ev.Next = spec.Next
			}
		}
		// 事件也要带「现在做什么」：只有 ErrConflict 会填 Next，而构造期拒绝
		// （例如目标端类型不支持、增量未实现）同样需要下一步——否则这一帧又是
		// 「有发生了什么，没有现在做什么」。
		if len(ev.Next) == 0 {
			ev.Next = fallbackNext
		}
		// 事件只活在环形缓冲里，进程重启就没了。用户回头问「它说已接受，为什么
		// 什么都没发生」时，唯一能查的地方是控制库——所以这里必须落一条 audit。
		// 「现在能做什么」必须每条都有。只有 ErrConflict 那条会填 ev.Next，
		// 而最常见的恰恰是没码的那些（权限、schema 写错、连不上）——只写
		// 「发生了什么」而留个 null 的 next，等于把问题原样丢回给用户。
		next := ev.Next
		if len(next) == 0 {
			next = fallbackNext
		}
		detail := map[string]any{
			"job_id":  job.ID,
			"actor":   actor,
			"code":    ev.Code,
			"message": ev.Message,
			"next":    next,
			"note":    "HTTP 已回 202，但引擎返回了错误；这里留一条不随进程重启消失的痕迹",
		}
		// 引擎若已经建出 run 行（失败发生在开跑之后），把那条 run 也指出来，
		// 免得读者以为「没有 run 行」。
		if last, lerr := s.store.LatestRun(context.Background(), job.ID); lerr == nil {
			detail["run_id"] = last.ID
			detail["run_phase"] = string(last.Phase)
			detail["run_finish_reason"] = last.FinishReason
			if last.LastError != "" {
				detail["run_last_error"] = last.LastError
			}
		}
		// 审计行的「谁」用真实发起者：同一次 startJob 里 job.run 记的就是它，
		// 写成 runner 会导致「按 actor 过滤时看得到发起、看不到被拒绝」。
		if aerr := s.store.Audit(context.Background(), actor, control.ActionRunRejected, detail); aerr != nil {
			s.logf("job %s 的拒绝痕迹写入失败（audit）: %v", job.ID, aerr)
		}
		s.store.Bus().Publish(ev)
	}()
	return nil
}

// stopJob 取消正在跑的 run。引擎会按 finish_reason=interrupted/stopped 收尾，
// 位点已提交，重跑会从断点继续。
func (s *Server) stopJob(jobID string) bool {
	return s.runs.stop(jobID)
}
