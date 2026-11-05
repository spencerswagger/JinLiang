package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/state"
)

// P1-d 回归：一个任务同时只能有一条活跃 run（ended_at IS NULL）。
// 进程被 kill -9 后原来那行永远不会自己结束，于是历史页永远显示「在跑」，
// 还能再开第二个。partial unique index 把这条关在库层面。
func TestOneActiveRunPerJob(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	src, tgt := seedConnections(t, s)
	job, err := s.CreateJob(ctx, sampleJobInput(src, tgt))
	if err != nil {
		t.Fatal(err)
	}

	first, err := s.StartRun(ctx, job.ID, "preflight", RunKindSync)
	if err != nil {
		t.Fatalf("第一条 run 应该能建: %v", err)
	}
	if first.OwnerInstance == "" {
		t.Fatal("run 必须记下 owner_instance（谁在跑）")
	}
	if first.HeartbeatAt == "" {
		t.Fatal("run 必须有初始心跳")
	}

	if _, err := s.StartRun(ctx, job.ID, "preflight", RunKindSync); !errors.Is(err, ErrConflict) {
		t.Fatalf("同一任务第二条活跃 run 必须被拒（ErrConflict），实际 %v", err)
	}

	// 结束之后可以再开一条：索引只约束「未结束」的行。
	walkToDraining(t, s, first.ID)
	if _, err := s.FinishRun(ctx, first.ID, "done", "", 0, FinishCompleted); err != nil {
		t.Fatalf("finish: %v", err)
	}
	if _, err := s.StartRun(ctx, job.ID, "preflight", RunKindSync); err != nil {
		t.Fatalf("上一轮结束后应该能再开: %v", err)
	}
}

// walkToDraining 走完合法的阶段路径（不能从 preflight 直接跳到 done）。
func walkToDraining(t *testing.T, s *Store, runID string) {
	t.Helper()
	for _, ph := range []state.Phase{state.Prepare, state.Snapshot, state.Catchup, state.Streaming, state.Draining} {
		if _, err := s.UpdateRunPhase(context.Background(), runID, ph); err != nil {
			t.Fatalf("advance to %s: %v", ph, err)
		}
	}
}

// P1-e 回归：手动停止不能写成「顺利完成」。B0 以前写 phase=done + exit_code=0，
// 历史页无法把它和真正跑完区分开。
func TestFinishRunRecordsWhyItEnded(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	src, tgt := seedConnections(t, s)
	job, err := s.CreateJob(ctx, sampleJobInput(src, tgt))
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.StartRun(ctx, job.ID, "preflight", RunKindSync)
	if err != nil {
		t.Fatal(err)
	}
	walkToDraining(t, s, run.ID)
	final, err := s.FinishRun(ctx, run.ID, "done", "", 0, FinishStopped)
	if err != nil {
		t.Fatal(err)
	}
	if final.FinishReason != FinishStopped {
		t.Fatalf("手动停止应记 finish_reason=stopped，实际 %q", final.FinishReason)
	}

	// 心跳应该被刷新到收尾时刻
	if final.HeartbeatAt == "" {
		t.Fatal("收尾时也要刷新心跳（否则重启会被误判为 interrupted）")
	}
}

// 进程被 kill -9：心跳停在旧时间，重启后的 serve 必须把它标 interrupted，
// 并且不能误伤「刚刚还在心跳」的 run。
func TestRecoverStaleRuns(t *testing.T) {
	clock := time.Now().UTC()
	s, err := Open(Options{
		DataDir: t.TempDir(), Autogen: true,
		Now: func() time.Time { return clock },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	src, tgt := seedConnections(t, s)
	job, err := s.CreateJob(ctx, sampleJobInput(src, tgt))
	if err != nil {
		t.Fatal(err)
	}
	stale, err := s.StartRun(ctx, job.ID, "preflight", RunKindSync)
	if err != nil {
		t.Fatal(err)
	}

	// 三分钟过去，进程没有心跳（被杀）。
	clock = clock.Add(3 * time.Minute)

	n, err := s.RecoverStaleRuns(ctx, 2*time.Minute)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if n != 1 {
		t.Fatalf("应该标记 1 条，实际 %d", n)
	}
	got, err := s.GetRun(ctx, stale.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.EndedAt == "" || got.FinishReason != FinishInterrupted {
		t.Fatalf("残留 run 必须是 interrupted 且已结束: %+v", got)
	}
	if !got.Phase.Terminal() {
		// 收尾必须落到可表达的终态，否则历史页会永久显示「增量中/已暂停」。
		t.Fatalf("被判定中断的 run 的 phase 应该是终态，实际 %s", got.Phase)
	}

	// 恢复之后同一个任务能重新开跑：不会再被唯一索引挡住。
	if _, err := s.StartRun(ctx, job.ID, "preflight", RunKindSync); err != nil {
		t.Fatalf("恢复后应该能重新开跑: %v", err)
	}

	// 刚刚心跳过的 run 不能被误杀。
	clock = clock.Add(time.Minute)
	n, err = s.RecoverStaleRuns(ctx, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("心跳还在的 run 被误标了 %d 条", n)
	}
}

// A2 回归：心跳必须独立于阶段推进——暂停 10 分钟的活 run 不能在下一次
// RecoverStaleRuns 里被判死（那会静默放开活跃槽，还能起第二个 run）。
// 心跳把「正在跑」与「进程已死」区分开：只要心跳新鲜，RecoverStaleRuns 就不许
// 判死它（否则活跃槽会被静默放开，还能再起第二个 run）。
//
// B0 的桩 runner 已从生产路径移除（UI 改走真引擎），所以这里直接用控制库的
// StartRun/TouchRun 模拟「一条活着的 run」，只测心跳与判死这一层。
func TestHeartbeatKeepsLiveRunFromBeingReaped(t *testing.T) {
	s, err := Open(Options{DataDir: t.TempDir(), Autogen: true, Heartbeat: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	src, tgt := seedConnections(t, s)
	job, err := s.CreateJob(ctx, sampleJobInput(src, tgt))
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.StartRun(ctx, job.ID, state.Preflight, RunKindSync)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.TouchRun(ctx, run.ID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	if err := s.TouchRun(ctx, run.ID); err != nil {
		t.Fatal(err)
	}

	got, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.EndedAt != "" {
		t.Fatalf("活着的 run 不该结束: %+v", got)
	}
	if got.HeartbeatAt == run.HeartbeatAt {
		t.Fatalf("心跳没有更新（Started=%s HeartbeatAt=%s）", run.HeartbeatAt, got.HeartbeatAt)
	}

	// 阈值 3×1s：刚心跳过，绝不能被判死。
	n, err := s.RecoverStaleRuns(ctx, StaleAfter(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("心跳新鲜的活 run 被误判为中断（%d 条），活跃槽会被错误放开", n)
	}

	// 手动停止的语义：finish_reason=stopped，且不能伪装成 completed。
	final, err := s.FinishRun(ctx, run.ID, state.Done, "", 130, FinishStopped)
	if err != nil {
		t.Fatal(err)
	}
	if final.FinishReason != FinishStopped {
		t.Fatalf("手动停止的 finish_reason = %q，期望 stopped", final.FinishReason)
	}
}

func TestUpdateJobCannotChangeIdentityWithHistory(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	src, tgt := seedConnections(t, s)
	job, err := s.CreateJob(ctx, sampleJobInput(src, tgt))
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.StartRun(ctx, job.ID, "preflight", RunKindSync)
	if err != nil {
		t.Fatal(err)
	}
	walkToDraining(t, s, run.ID)
	if _, err := s.FinishRun(ctx, run.ID, "done", "", 0, FinishCompleted); err != nil {
		t.Fatal(err)
	}

	// 换一个源连接（坐标不同 → job_key 不同）
	other, err := s.CreateConnection(ctx, ConnectionInput{
		Name: "另一个源", Kind: "postgres", Host: "10.0.0.9", Port: 5432,
		Database: "other", Username: "u", Password: "pw",
	})
	if err != nil {
		t.Fatal(err)
	}
	in := sampleJobInput(other, tgt)
	in.Name = job.Name
	_, err = s.UpdateJob(ctx, job.ID, in)
	if !errors.Is(err, ErrIdentityLocked) {
		t.Fatalf("有运行历史时换连接必须被拒（ErrIdentityLocked），实际 %v", err)
	}

	// 只改名字 / 只改内容不算换身份，必须放行。
	rename := sampleJobInput(src, tgt)
	rename.Name = "改个名字"
	if _, err := s.UpdateJob(ctx, job.ID, rename); err != nil {
		t.Fatalf("只改名不该被拦：%v", err)
	}
}
