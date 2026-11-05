package httpapi

import (
	"context"
	"errors"
	"fmt"
	"sync"

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
// errEngineUnsupported 是「引擎只做了 PG→PG」这条能力边界的四段式。
// 放在 HTTP 入口同步判，是为了不在返回 202 之后什么也不发生——那正是我们一直
// 在打的静默失败。（引擎里也有同样一条检查，见 internal/engine/engine.go:114。）
func (s *Server) checkEngineSupported(ctx context.Context, job control.Job) error {
	src, err := s.store.GetConnection(ctx, job.SourceID)
	if err != nil {
		return err
	}
	tgt, err := s.store.GetConnection(ctx, job.TargetID)
	if err != nil {
		return err
	}
	if src.Kind == "postgres" && tgt.Kind == "postgres" {
		return nil
	}
	return fmt.Errorf("引擎暂时只支持 PostgreSQL → PostgreSQL（当前 %s → %s）：MySQL 目标端在 B2；"+
		"现在可以先建一个 PostgreSQL 目标连接来验证链路", src.Kind, tgt.Kind)
}

func (s *Server) startJob(ctx context.Context, job control.Job, actor string) error {
	if err := s.checkEngineSupported(ctx, job); err != nil {
		return err
	}
	if s.runs.active(job.ID) {
		return control.ErrRunInProgress
	}
	if run, err := s.activeSyncRun(ctx, job.ID); err != nil {
		return err
	} else if run != nil {
		// 控制库里还有一条没结束的 run：可能是上一个进程被 kill 留下的。
		return fmt.Errorf("%w：控制库里还有一条没结束的运行（%s），serve 启动时会按心跳过期替它收尾；也可以先删除控制库里的那条 run",
			control.ErrRunInProgress, run.ID)
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
		// 事件只活在环形缓冲里，进程重启就没了。用户回头问「它说已接受，为什么
		// 什么都没发生」时，唯一能查的地方是控制库——所以这里必须落一条 audit。
		detail := map[string]any{
			"job_id":  job.ID,
			"actor":   actor,
			"code":    ev.Code,
			"message": ev.Message,
			"next":    ev.Next,
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
		if aerr := s.store.Audit(context.Background(), control.ActorStub, control.ActionRunRejected, detail); aerr != nil {
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
