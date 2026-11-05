package engine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/control"
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
