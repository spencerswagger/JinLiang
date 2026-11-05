package control

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/spencerswagger/jinliang/internal/state"
)

// Runner is the B0 skeleton executor.
//
// 它**故意**不搬运任何数据。目标只有一个：让「点 run → 看到阶段推进 → 看到
// 事件流」这条链路是真的通的，而不是靠假时间戳糊弄。所以每个事件都带
// stub=true，页面上也写着「骨架运行，没有数据被移动」。
//
// B1 会把 loop 换成真正的引擎；pause/resume/stop 的控制语义保持不动。
type Runner struct {
	store *Store
	bus   *Bus

	mu     sync.Mutex
	active map[string]*runControl
}

type runControl struct {
	runID  string
	jobID  string
	cancel context.CancelFunc
	done   chan struct{}
	wake   chan struct{}

	mu     sync.Mutex
	paused bool
	stop   bool
}

func (c *runControl) notify() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *runControl) requestPause() (already bool) {
	c.mu.Lock()
	already = c.paused
	c.paused = true
	c.mu.Unlock()
	c.notify()
	return already
}

func (c *runControl) requestResume() (wasPaused bool) {
	c.mu.Lock()
	wasPaused = c.paused
	c.paused = false
	c.mu.Unlock()
	c.notify()
	return wasPaused
}

func (c *runControl) requestStop() {
	c.mu.Lock()
	c.stop = true
	c.mu.Unlock()
	c.notify()
}

func (c *runControl) status() (paused, stopped bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.paused, c.stop
}

func newRunner(s *Store) *Runner {
	return &Runner{store: s, bus: s.bus, active: map[string]*runControl{}}
}

// Runner exposes the store's runner.
func (s *Store) Runner() *Runner { return s.runner }

// Bus exposes the event bus (SSE and the CLI both listen here).
func (s *Store) Bus() *Bus { return s.bus }

var (
	// ErrRunInProgress means this job already has an active run.
	ErrRunInProgress = errors.New("这个任务正在跑")
	// ErrNoActiveRun means pause/resume/stop had nothing to act on.
	ErrNoActiveRun = errors.New("这个任务没有在跑")
)

// Start begins a stub run and returns immediately.
//
// 同一个任务不会并行开两次：引擎在目标端用租约挡这个，控制面在本地先挡一次
// （两个进程写同一份数据的故事见 docs/IDENTITY.md）。
func (r *Runner) Start(ctx context.Context, jobID, actor string) (Run, error) {
	job, err := r.store.GetJob(ctx, jobID)
	if err != nil {
		return Run{}, err
	}
	r.mu.Lock()
	if _, busy := r.active[jobID]; busy {
		r.mu.Unlock()
		return Run{}, fmt.Errorf("%w：先暂停，或者等它跑完", ErrRunInProgress)
	}
	run, err := r.store.StartRun(ctx, jobID, state.Preflight)
	if err != nil {
		r.mu.Unlock()
		return Run{}, err
	}
	if err := r.store.SetRunStub(ctx, run.ID, true); err != nil {
		r.mu.Unlock()
		return Run{}, err
	}
	run.Stub = true
	runCtx, cancel := context.WithCancel(context.Background())
	ctl := &runControl{
		runID: run.ID, jobID: jobID, cancel: cancel,
		done: make(chan struct{}), wake: make(chan struct{}, 1),
	}
	r.active[jobID] = ctl
	r.mu.Unlock()

	_ = r.store.Audit(ctx, actor, ActionJobRun, map[string]any{"job_id": jobID, "run_id": run.ID, "stub": true})
	r.bus.Publish(Event{
		JobID: jobID, RunID: run.ID, Type: "phase", Phase: state.Preflight,
		Message: "骨架运行（B0 桩）：只推进阶段，不搬运任何数据",
		Stub:    true,
	})
	_ = runCtx
	go r.loop(job, run, ctl)
	return run, nil
}

// phaseStep is one stop on the skeleton's route.
type phaseStep struct {
	phase state.Phase
	note  string
	hold  time.Duration
}

var stubSteps = []phaseStep{
	{state.Prepare, "（桩）这一步会做的：在目标端准备表与同步元数据 —— B1 才真做", 350 * time.Millisecond},
	{state.Snapshot, "（桩）这一步会做的：把历史数据整块搬过去 —— B1 才真做", 450 * time.Millisecond},
	{state.Catchup, "（桩）这一步会做的：把初始化期间发生的变更补上 —— B1 才真做", 350 * time.Millisecond},
	{state.Streaming, "（桩）这一步会做的：转入常态增量，秒级追上 —— B1 才真做", 350 * time.Millisecond},
	{state.Draining, "（桩）这一步会做的：提交最后一批并释放占用", 250 * time.Millisecond},
}

// loop walks the phases. Each step is short on purpose: the acceptance test has
// to see every transition within a couple of seconds.
func (r *Runner) loop(job Job, run Run, ctl *runControl) {
	defer close(ctl.done)
	defer ctl.cancel()
	defer func() {
		r.mu.Lock()
		delete(r.active, ctl.jobID)
		r.mu.Unlock()
	}()

	ctx := context.Background()
	cur := state.Preflight
	for _, step := range stubSteps {
		if !r.gate(job, run, &cur, ctl) {
			r.finishStopped(job, run, cur)
			return
		}
		if !r.sleep(ctl, step.hold) {
			r.finishStopped(job, run, cur)
			return
		}
		if _, err := r.store.UpdateRunPhase(ctx, run.ID, step.phase); err != nil {
			r.fail(job, run, "阶段推进失败："+err.Error())
			return
		}
		cur = step.phase
		r.bus.Publish(Event{
			JobID: job.ID, RunID: run.ID, Type: "phase", Phase: step.phase,
			Message: step.note, Stub: true,
		})
	}
	if !r.gate(job, run, &cur, ctl) {
		r.finishStopped(job, run, cur)
		return
	}
	if !r.sleep(ctl, 200*time.Millisecond) {
		r.finishStopped(job, run, cur)
		return
	}
	final, err := r.store.FinishRun(ctx, run.ID, state.Done, "", 0)
	if err != nil {
		r.fail(job, run, "收尾失败："+err.Error())
		return
	}
	r.bus.Publish(Event{
		JobID: job.ID, RunID: run.ID, Type: "done", Phase: final.Phase,
		Message: "骨架运行结束：没有数据被移动（B0 桩）。B1 接上引擎后这里才是真的同步",
		Stub:    true,
	})
	_ = r.store.Audit(ctx, ActorStub, ActionRunStub, map[string]any{"job_id": job.ID, "run_id": run.ID})
}

// gate blocks while the run is paused. It returns false when the run must stop.
//
// 暂停期间阶段会真的写成 Paused（而不是假装没发生），因为用户看的就是这个字段；
// 恢复时回到暂停前的阶段，且只在 state.CanTransition 允许时切换。
func (r *Runner) gate(job Job, run Run, cur *state.Phase, ctl *runControl) bool {
	paused, stopped := ctl.status()
	if stopped {
		return false
	}
	if !paused {
		return true
	}
	before := *cur
	if state.CanTransition(before, state.Paused) {
		if _, err := r.store.UpdateRunPhase(context.Background(), run.ID, state.Paused); err == nil {
			*cur = state.Paused
			r.bus.Publish(Event{JobID: job.ID, RunID: run.ID, Type: "note", Phase: state.Paused,
				Message: "已暂停：登台的地方已经提交，恢复会从断点继续", Stub: true})
		}
	}
	for {
		p, stop := ctl.status()
		if stop {
			return false
		}
		if !p {
			break
		}
		select {
		case <-ctl.wake:
		case <-time.After(150 * time.Millisecond):
		}
	}
	if *cur == state.Paused && state.CanTransition(state.Paused, before) {
		if _, err := r.store.UpdateRunPhase(context.Background(), run.ID, before); err == nil {
			*cur = before
			r.bus.Publish(Event{JobID: job.ID, RunID: run.ID, Type: "phase", Phase: before,
				Message: "已恢复：继续之前的阶段", Stub: true})
		}
	}
	return true
}

// sleep waits in small slices so pause/stop react quickly.
func (r *Runner) sleep(ctl *runControl, d time.Duration) bool {
	const slice = 50 * time.Millisecond
	waited := time.Duration(0)
	for waited < d {
		if _, stopped := ctl.status(); stopped {
			return false
		}
		if paused, _ := ctl.status(); paused {
			return true // 交给 gate 处理
		}
		select {
		case <-ctl.wake:
		case <-time.After(slice):
		}
		waited += slice
	}
	return true
}

func (r *Runner) fail(job Job, run Run, msg string) {
	ev := ErrorEvent(job.ID, run.ID, "E4002", msg)
	ev.Stub = true
	r.bus.Publish(ev)
	_, _ = r.store.FinishRun(context.Background(), run.ID, state.Failed, msg, 1)
}

func (r *Runner) finishStopped(job Job, run Run, cur state.Phase) {
	ctx := context.Background()
	if state.CanTransition(cur, state.Draining) && cur != state.Draining {
		if _, err := r.store.UpdateRunPhase(ctx, run.ID, state.Draining); err != nil {
			r.fail(job, run, "停止时收尾失败："+err.Error())
			return
		}
	}
	final, err := r.store.FinishRun(ctx, run.ID, state.Done, "", 0)
	if err != nil {
		r.fail(job, run, "停止时收尾失败："+err.Error())
		return
	}
	r.bus.Publish(Event{JobID: job.ID, RunID: run.ID, Type: "done", Phase: final.Phase,
		Message: "已手动停止（骨架运行，没有任何数据被移动）", Stub: true})
}

// Pause asks the active run to stop at a safe point.
func (r *Runner) Pause(ctx context.Context, jobID string) error {
	ctl := ctlFor(r, jobID)
	if ctl == nil {
		return ErrNoActiveRun
	}
	ctl.requestPause()
	_ = r.store.Audit(ctx, ActorAPI, "job.pause", map[string]any{"job_id": jobID, "run_id": ctl.runID})
	return nil
}

// Resume continues a paused run.
func (r *Runner) Resume(ctx context.Context, jobID string) error {
	ctl := ctlFor(r, jobID)
	if ctl == nil {
		return ErrNoActiveRun
	}
	if !ctl.requestResume() {
		return fmt.Errorf("这个任务没有暂停")
	}
	_ = r.store.Audit(ctx, ActorAPI, "job.resume", map[string]any{"job_id": jobID, "run_id": ctl.runID})
	return nil
}

// Stop ends the active run (it drains to Done and says it was stopped).
func (r *Runner) Stop(ctx context.Context, jobID string) error {
	ctl := ctlFor(r, jobID)
	if ctl == nil {
		return ErrNoActiveRun
	}
	ctl.requestStop()
	_ = r.store.Audit(ctx, ActorAPI, "job.stop", map[string]any{"job_id": jobID, "run_id": ctl.runID})
	return nil
}

// Active reports the active run for a job.
func (r *Runner) Active(jobID string) (Run, bool) {
	ctl := ctlFor(r, jobID)
	if ctl == nil {
		return Run{}, false
	}
	run, err := r.store.GetRun(context.Background(), ctl.runID)
	if err != nil {
		return Run{}, false
	}
	return run, true
}

// Wait blocks until the job's active run finishes (used by `run --job`).
func (r *Runner) Wait(ctx context.Context, jobID string) error {
	ctl := ctlFor(r, jobID)
	if ctl == nil {
		return nil
	}
	select {
	case <-ctl.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func ctlFor(r *Runner, jobID string) *runControl {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.active[jobID]
}
