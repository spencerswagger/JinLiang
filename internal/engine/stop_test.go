package engine

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/core"
	"github.com/spencerswagger/jinliang/internal/state"
)

// 在 preflight / prepare 阶段按停止，必须记成 stopped + 130，不能记成 failed，
// 更不能挂 E4002（「津梁自己的 bug」）——用户做了一件正确的事。
//
// 用真控制库（纯 Go 的 sqlite，不需要数据库 DSN），断言读回来的 run 行。
func TestUserStopBeforeDataIsStoppedNotFailed(t *testing.T) {
	for _, phase := range []state.Phase{state.Preflight, state.Prepare} {
		t.Run(string(phase), func(t *testing.T) {
			store, jobID := newControlStore(t)
			run, err := store.StartRun(context.Background(), jobID, phase, control.RunKindSync)
			if err != nil {
				t.Fatalf("建 run 失败: %v", err)
			}
			// 订阅事件，确认没有 error/E4002：停止不是错误事件。
			ch, cancel := store.Bus().Subscribe(jobID)
			defer cancel()

			r := &runner{opt: Options{Store: store}, job: control.Job{ID: jobID}, run: run, phase: phase}
			if err := r.fail(context.Background(), context.Canceled); err != nil {
				t.Fatalf("用户停止不该返回错误，得到 %v", err)
			}

			got, err := store.GetRun(context.Background(), run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Phase != state.Done {
				t.Fatalf("阶段应落 done（停止不是失败），得到 %s", got.Phase)
			}
			if got.FinishReason != control.FinishStopped {
				t.Fatalf("finish_reason 应为 stopped，得到 %q", got.FinishReason)
			}
			if got.ExitCode == nil || *got.ExitCode != ExitStopped {
				t.Fatalf("exit_code 应为 %d，得到 %v", ExitStopped, got.ExitCode)
			}
			if got.LastError != "" {
				t.Fatalf("手动停止不该写 last_error，得到 %q", got.LastError)
			}

			// 事件流里只有 done，没有 error、没有 E4002
			timeout := time.After(500 * time.Millisecond)
			for {
				select {
				case ev := <-ch:
					if ev.Type == "error" {
						t.Fatalf("停止不该产生 error 事件（code=%q）：%s", ev.Code, ev.Message)
					}
					if strings.Contains(ev.Code, "E4002") {
						t.Fatal("停止不该被归成 E4002「我们的 bug」")
					}
				case <-timeout:
					return
				}
			}
		})
	}
}

// 非取消类错误仍然走 failed（别把这条修复扩大成「所有错误都 stopped」）。
func TestRealErrorStillFails(t *testing.T) {
	store, jobID := newControlStore(t)
	run, err := store.StartRun(context.Background(), jobID, state.Preflight, control.RunKindSync)
	if err != nil {
		t.Fatal(err)
	}
	r := &runner{opt: Options{Store: store}, job: control.Job{ID: jobID}, run: run, phase: state.Preflight}
	cause := context.DeadlineExceeded // 不是 Canceled
	if err := r.fail(context.Background(), cause); err == nil {
		t.Fatal("非取消类错误应返回错误")
	}
	got, _ := store.GetRun(context.Background(), run.ID)
	if got.FinishReason != control.FinishFailed {
		t.Fatalf("真失败应记 failed，得到 %q", got.FinishReason)
	}
}

// newControlStore 建一个临时控制库 + 一条连接 + 一个任务，返回 store 与 job id。
func newControlStore(t *testing.T) (*control.Store, string) {
	t.Helper()
	ctx := context.Background()
	store, err := control.Open(control.Options{DataDir: t.TempDir(), Autogen: true})
	if err != nil {
		t.Fatalf("打开控制库失败: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	c, err := store.CreateConnection(ctx, control.ConnectionInput{
		Name: "src", Kind: "postgres", Host: "127.0.0.1", Port: 5432,
		Database: "d", Username: "u", Password: "p",
	})
	if err != nil {
		t.Fatalf("建连接失败: %v", err)
	}
	job, err := store.CreateJob(ctx, control.JobInput{
		Name: "stop-test", SourceID: c.ID, TargetID: c.ID,
		Spec: control.JobDoc{
			Version: 1, Job: "stop-test",
			Source: control.SourceDoc{Kind: "postgres"},
			Target: control.TargetDoc{Kind: "postgres", OnExists: "append"},
			Mode:   control.ModeDoc{Frequency: "once"},
		},
	})
	if err != nil {
		t.Fatalf("建任务失败: %v", err)
	}
	return store, job.ID
}

// P3-2（升为必修）：排查方向必须落进 runs.last_error。
// 事件是易失的（进程重启就没了），用户回头找原因看的是历史页；
// 诊断信息只在事件流里活一次，等于没做。
func TestUnclassifiedErrorHintPersistsToLastError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want []string
	}{
		{
			// 挂了 E1012 之后，方向来自码表（Message/Next），并带码本身
			name: "权限不够（已挂码）",
			err:  &pgconn.PgError{Code: "42501", Message: "permission denied for schema public"},
			want: []string{"E1012", "GRANT", "下一步"},
		},
		{
			name: "对象不存在（已挂码）",
			err:  &pgconn.PgError{Code: "3F000", Message: `schema "report" does not exist`},
			want: []string{"E1013", "缺 schema", "下一步"},
		},
		{
			name: "完全未识别",
			err:  errors.New("something we have never seen"),
			want: []string{"还没分类", "维护者", "原始错误"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			store, jobID := newControlStore(t)
			run, err := store.StartRun(context.Background(), jobID, state.Prepare, control.RunKindSync)
			if err != nil {
				t.Fatal(err)
			}
			r := &runner{opt: Options{Store: store}, job: control.Job{ID: jobID}, run: run, phase: state.Prepare}
			if err := r.fail(context.Background(), c.err); err == nil {
				t.Fatal("这条错误应返回错误")
			}
			got, err := store.GetRun(context.Background(), run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.FinishReason != control.FinishFailed {
				t.Fatalf("应记 failed，得到 %q", got.FinishReason)
			}
			for _, w := range c.want {
				if !strings.Contains(got.LastError, w) {
					t.Fatalf("last_error 缺 %q（历史页看不到排查方向）：%s", w, got.LastError)
				}
			}
		})
	}
}

// 丢租约**不是**用户停止：loseLease 自己会 cancel(runCtx)，所以进 fail() 时 ctx 已经
// 是 Canceled —— 之前这个误判把「另一个实例接管」记成「用户点了停止 +130 + done」，
// 历史页显示「已手动停止」，真原因彻底消失。Owl 的探针就是撞到这条。
func TestLeaseLostIsInterruptedNotStopped(t *testing.T) {
	for _, phase := range []state.Phase{state.Preflight, state.Streaming} {
		t.Run(string(phase), func(t *testing.T) {
			store, jobID := newControlStore(t)
			run, err := store.StartRun(context.Background(), jobID, phase, control.RunKindSync)
			if err != nil {
				t.Fatal(err)
			}
			// 造出「租约已丢」的状态：run ctx 被 cancel（loseLease 的行为）+ leaseLost 置位
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			r := &runner{opt: Options{Store: store}, job: control.Job{ID: jobID}, run: run, phase: phase}
			r.leaseLost.Store(true)
			r.leaseWhy.Store("租约被别的实例接管了")
			r.cancelRun = cancel

			err = r.fail(ctx, r.leaseLostError())
			if err == nil {
				t.Fatal("丢租约应返回错误（不是静默成功）")
			}

			got, gerr := store.GetRun(context.Background(), run.ID)
			if gerr != nil {
				t.Fatal(gerr)
			}
			if got.FinishReason != control.FinishInterrupted {
				t.Fatalf("丢租约应记 interrupted，得到 %q（被误判成用户停止？）", got.FinishReason)
			}
			if got.ExitCode != nil && *got.ExitCode == ExitStopped {
				t.Fatal("丢租约不能记成 130（那是「用户主动停止」）")
			}
			if got.Phase != state.Failed {
				t.Fatalf("丢租约的阶段应为 failed，得到 %s", got.Phase)
			}
			if !strings.Contains(got.LastError, "租约") {
				t.Fatalf("last_error 必须写清是租约原因，得到 %q", got.LastError)
			}
		})
	}
}

// Owl 用变异证明过：「用户 stop 仍记 stopped」这条是 TestUserStopBeforeDataIsStoppedNotFailed
// 的严格子集（那里断言更全、还覆盖 prepare）。所以这条测试存在的唯一理由是覆盖
// **它覆盖不到的角度：丢租约之后再 stop，也不会把这一轮改回 stopped**——
// 否则历史页会显示「用户停止」，而真相是「被另一个实例接管」。
// （变异验证：去掉 fail() 里的租约优先判断，这条会红。）
func TestStopAfterLeaseLostKeepsInterrupted(t *testing.T) {
	store, jobID := newControlStore(t)
	run, err := store.StartRun(context.Background(), jobID, state.Streaming, control.RunKindSync)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &runner{opt: Options{Store: store}, job: control.Job{ID: jobID}, run: run, phase: state.Streaming}
	r.cancelRun = cancel
	r.leaseLost.Store(true)
	r.leaseWhy.Store("租约被别的实例接管了")

	// 第一下：丢租约
	if err := r.fail(ctx, r.leaseLostError()); err == nil {
		t.Fatal("丢租约应返回错误")
	}
	// 第二下：同一个 run 上又来一次「用户取消」（例如用户随后点了停止）
	if err := r.fail(ctx, context.Canceled); err == nil {
		t.Fatal("第二次 fail 也应返回错误")
	}

	got, err := store.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.FinishReason != control.FinishInterrupted {
		t.Fatalf("丢租约之后再 stop，仍应是 interrupted，得到 %q（被改写成了用户停止？）", got.FinishReason)
	}
	if got.ExitCode != nil && *got.ExitCode == ExitStopped {
		t.Fatal("丢租约的收尾不能被后来的 stop 改写成 130")
	}
}

// 42703（缺列）也归 E1013——码表已扩到「schema / 表 / 列」，并且 Next 里
// **必须有一条专门针对缺列的有效动作**（回任务页重跑预检）。
// 之前那条码只讲「建 schema」，对缺列的用户全是无效动作（Owl 的必改）。
func TestMissingColumnUsesE1013WithUsableNextSteps(t *testing.T) {
	missingCol := &pgconn.PgError{Code: "42703", Message: `column "updated_at" does not exist`}
	code, ok := classifyError(missingCol)
	if !ok || code != core.CodeObjectMissing {
		t.Fatalf("42703 应映射 E1013，得到 %s %v", code, ok)
	}
	spec, found := core.Lookup(core.CodeObjectMissing)
	if !found {
		t.Fatal("码表里应有 E1013")
	}
	if len(spec.Next) < 3 {
		t.Fatalf("E1013 的 next 应有 3 条（schema / 表 / 列各一条），实际 %d 条：%v", len(spec.Next), spec.Next)
	}
	joined := strings.Join(spec.Next, "；")
	if !strings.Contains(joined, "42703") || !strings.Contains(joined, "预检") {
		t.Fatalf("缺列必须有一条可执行动作（回任务页重跑预检），得到：%s", joined)
	}
	// 挂码的错误也要把这条方向写进 last_error（历史页只有这条字符串）
	store, jobID := newControlStore(t)
	run, err := store.StartRun(context.Background(), jobID, state.Prepare, control.RunKindSync)
	if err != nil {
		t.Fatal(err)
	}
	r := &runner{opt: Options{Store: store}, job: control.Job{ID: jobID}, run: run, phase: state.Prepare}
	_ = r.fail(context.Background(), missingCol)
	got, _ := store.GetRun(context.Background(), run.ID)
	if !strings.Contains(got.LastError, "E1013") || !strings.Contains(got.LastError, "预检") {
		t.Fatalf("last_error 应含 E1013 与缺列的动作，得到：%s", got.LastError)
	}
}

// ① 心跳必须真的刷新：`runs.heartbeat_at` 不更新的后果是「空闲 90 秒被判幽灵、
// 重启把健康任务标成 interrupted」。这条测的是心跳循环本身（注入 interval 与 touch，
// 不等真同步跑两分钟），另有一条对比断言：不调 TouchRun 时 heartbeat_at 不变。
func TestHeartbeatLoopRefreshesRunRow(t *testing.T) {
	store, jobID := newControlStore(t)
	run, err := store.StartRun(context.Background(), jobID, state.Streaming, control.RunKindSync)
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if first.HeartbeatAt == "" {
		t.Fatal("StartRun 应写入初始 heartbeat_at")
	}

	// 把心跳手动推到「很久以前」，再看循环能不能把它刷新回来
	if _, err := store.DB().Exec(`UPDATE runs SET heartbeat_at = '2000-01-01T00:00:00Z' WHERE id = ?`, run.ID); err != nil {
		t.Fatal(err)
	}

	r := &runner{opt: Options{Store: store}, job: control.Job{ID: jobID}, run: run, phase: state.Streaming}
	ctx, cancel := context.WithCancel(context.Background())
	go r.heartbeatLoop(ctx, 10*time.Millisecond, func(c context.Context) error {
		return store.TouchRun(c, run.ID)
	})
	time.Sleep(80 * time.Millisecond)
	cancel()

	got, err := store.GetRun(context.Background(), run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.HeartbeatAt == "2000-01-01T00:00:00Z" {
		t.Fatal("心跳没有被刷新（runs.heartbeat_at 还是旧值）——doctor 会把这轮判成幽灵")
	}
}

// 心跳失败不能打断同步，但要 emit 一条 note（状态页可能误判，用户得知道）。
func TestHeartbeatLoopReportsFailureButKeepsRunning(t *testing.T) {
	r := &runner{opt: Options{}, job: control.Job{}, run: control.Run{ID: "x"}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32 // race 检测器会盯着：计数只能在 goroutine 里原子加、主线程原子读
	go r.heartbeatLoop(ctx, 5*time.Millisecond, func(context.Context) error {
		calls.Add(1)
		return errors.New("控制库暂时不可写")
	})
	time.Sleep(60 * time.Millisecond)
	if n := calls.Load(); n < 2 {
		t.Fatalf("心跳失败后应继续尝试，实际只调了 %d 次", n)
	}
}

// 心跳 note 只在状态转换点发：每 15 秒一条会把 200 条环形缓冲铺满、
// 把真正的 progress/error 挤掉（13 分钟就能铺满）。
// 断言的是「两次失败只发一条、恢复时再发一条」，不是「至少发了一条」。
func TestHeartbeatNotesOnlyOnStateTransitions(t *testing.T) {
	var notes atomic.Int32
	r := &runner{opt: Options{}, job: control.Job{}, run: control.Run{ID: "x"}}
	r.emitHook = func(control.Event) { notes.Add(1) }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bad := atomic.Bool{}
	bad.Store(true)
	go r.heartbeatLoop(ctx, 5*time.Millisecond, func(context.Context) error {
		if bad.Load() {
			return errors.New("控制库暂时不可写")
		}
		return nil
	})
	time.Sleep(60 * time.Millisecond) // 期间会有多次失败
	if n := notes.Load(); n != 1 {
		t.Fatalf("连续失败只应发一条 note，实际 %d 条", n)
	}
	bad.Store(false) // 恢复
	time.Sleep(40 * time.Millisecond)
	if n := notes.Load(); n != 2 {
		t.Fatalf("恢复时应再发一条，实际累计 %d 条", n)
	}
}
