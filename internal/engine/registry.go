package engine

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/spencerswagger/jinliang/internal/control"
)

// 本进程内「正在跑的 run」登记表。
//
// 为什么需要它：删任务时如果只删控制库那一行，引擎的 goroutine 还活着、
// 目标端租约还在续期——同目标新建任务直接撞 E1005，而那条错误的「下一步」
// 用户没法执行（真正在写的进程根本没被通知）。所以删除必须先 cancel 引擎的
// context、等它退出（退出路径会自己 ReleaseLease），等不到就留痕。
//
// 边界：这只覆盖**本进程**。跨进程（另一个 serve/CLI 在跑）靠目标端租约过期 + 抢占，
// 两条路都不静默：等不到就写 audit 说清「谁还持有、为什么没停」。
type runHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
}

var (
	runningMu sync.Mutex
	running   = map[string]*runHandle{}
)

// registerRun 登记一个正在跑的 run，返回注销函数（defer 调用）。
func registerRun(jobID string, cancel context.CancelFunc) (*runHandle, func()) {
	h := &runHandle{cancel: cancel, done: make(chan struct{})}
	runningMu.Lock()
	running[jobID] = h
	runningMu.Unlock()
	return h, func() {
		runningMu.Lock()
		if cur, ok := running[jobID]; ok && cur == h {
			delete(running, jobID)
		}
		runningMu.Unlock()
		close(h.done)
	}
}

// CancelRun 让**本进程内**正在跑的那个 run 停下来。返回 false 表示这个任务
// 在本进程里没有活跃的 run（可能在另一个进程里跑，或已经结束）。
func CancelRun(jobID string) bool {
	runningMu.Lock()
	h := running[jobID]
	runningMu.Unlock()
	if h == nil {
		return false
	}
	h.cancel()
	return true
}

// RunningInThisProcess 报告本进程是否正在跑这个任务。
func RunningInThisProcess(jobID string) bool {
	runningMu.Lock()
	defer runningMu.Unlock()
	_, ok := running[jobID]
	return ok
}

// StopJob 是「删任务」用的停引擎入口：
//  1. cancel 本进程的 run（如果有）；
//  2. 等 run 退出（最多 wait）——退出路径会 ReleaseLease，所以租约这时才真正放掉；
//  3. 等不到就写一条 audit，写明原因与当前持有者，**不静默略过**。
//
// 返回值 stopped 表示「本进程的 run 确实退出了」；canceled 表示「发过 cancel」。
func StopJob(ctx context.Context, store *control.Store, jobID, jobKey string, wait time.Duration) (canceled, stopped bool, err error) {
	if wait <= 0 {
		wait = 30 * time.Second
	}
	runningMu.Lock()
	h := running[jobID]
	runningMu.Unlock()
	if h == nil {
		// 本进程没有这个任务在跑：可能是别的进程，也可能本来就没跑。
		// 不写 audit（删除路径自己会说明），返回 false 让调用方判断要不要提示。
		return false, false, nil
	}
	h.cancel()
	select {
	case <-h.done:
		return true, true, nil
	case <-time.After(wait):
		// 卡住了：留痕。用户必须能看到「为什么没停干净」，否则下一次新建任务
		// 撞 E1005 时无从下手。
		reason := fmt.Sprintf("删除任务时引擎在 %s 内没有退出（已发 cancel）；"+
			"目标端租约会在 TTL 后过期并可被抢占，期间新建同名任务会撞 E1005", wait)
		if store != nil {
			_ = store.Audit(ctx, control.ActorSystem, "job.run.stop_timeout", map[string]any{
				"job_id": jobID, "job_key": jobKey, "waited": wait.String(), "reason": reason,
			})
		}
		return true, false, fmt.Errorf("%s", reason)
	case <-ctx.Done():
		return true, false, ctx.Err()
	}
}

// WaitRunExit 只等、不 cancel（给「已经发过 stop」的路径用）。
func WaitRunExit(jobID string, wait time.Duration) bool {
	runningMu.Lock()
	h := running[jobID]
	runningMu.Unlock()
	if h == nil {
		return true
	}
	select {
	case <-h.done:
		return true
	case <-time.After(wait):
		return false
	}
}
