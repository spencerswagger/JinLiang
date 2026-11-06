// Package jobrun 是「跑一个任务」的唯一入口。
//
// 为什么要有这一层：CLI 的 `jinlong run --job` 与 WebUI 的「开始」必须走**同一条**
// 执行路径——否则两边会各自传一套参数、各自处理退出码，慢慢地行为就不一样了。
// 这里只做一件事：把任务的配置与调用方的选项拼成 engine.Options，然后调用引擎。
// 它不含任何执行逻辑（没有重试、没有阶段推进），那些都在 internal/engine。
package jobrun

import (
	"context"
	"time"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/engine"
)

// Options 是调用方（CLI / WebUI）能影响本次执行的全部参数。
// 其余参数（批次、并发、分块）由任务的 spec 决定，不从 UI 传——避免界面里出现
// 「同一件事两个开关」。
type Options struct {
	// Actor 记进审计与 run 行：cli / ui / api。
	Actor string
	// SnapshotOnly 只搬数据不追变更（CLI 的 --snapshot-only，UI 不用）。
	SnapshotOnly bool
	// StopAfter / IdleAfter / LagThreshold 供脚本与冒烟测试用（CLI 专用）。
	StopAfter    time.Duration
	IdleAfter    time.Duration
	LagThreshold int64
	// Verify 让引擎在搬完之后跑一致性校验（CLI 的 --verify）。
	Verify bool
	// BatchRows 覆盖任务配置里的 reliability.batch_rows（0 = 用任务配置）。
	BatchRows int
}

// Run 执行一个任务直到它停下（同步阻塞）。调用方负责在需要时用 ctx 取消。
//
// 返回值是引擎的结果（阶段、退出码 0/1/130、行数、位点）；错误是「没能开始」或
// 「中途不可恢复」，此时 run 行的 finish_reason / last_error 里已经有四段式信息。
func Run(ctx context.Context, store *control.Store, job control.Job, opt Options) (engine.Result, error) {
	return engine.Run(ctx, engine.Options{
		Store:        store,
		JobID:        job.ID,
		Actor:        opt.Actor,
		SnapshotOnly: opt.SnapshotOnly,
		StopAfter:    opt.StopAfter,
		IdleAfter:    opt.IdleAfter,
		LagThreshold: opt.LagThreshold,
		Verify:       opt.Verify,
		BatchRows:    opt.BatchRows,
	})
}
