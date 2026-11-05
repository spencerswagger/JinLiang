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

	first, err := s.StartRun(ctx, job.ID, "preflight")
	if err != nil {
		t.Fatalf("第一条 run 应该能建: %v", err)
	}
	if first.OwnerInstance == "" {
		t.Fatal("run 必须记下 owner_instance（谁在跑）")
	}
	if first.HeartbeatAt == "" {
		t.Fatal("run 必须有初始心跳")
	}

	if _, err := s.StartRun(ctx, job.ID, "preflight"); !errors.Is(err, ErrConflict) {
		t.Fatalf("同一任务第二条活跃 run 必须被拒（ErrConflict），实际 %v", err)
	}

	// 结束之后可以再开一条：索引只约束「未结束」的行。
	walkToDraining(t, s, first.ID)
	if _, err := s.FinishRun(ctx, first.ID, "done", "", 0, FinishCompleted); err != nil {
		t.Fatalf("finish: %v", err)
	}
	if _, err := s.StartRun(ctx, job.ID, "preflight"); err != nil {
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
	run, err := s.StartRun(ctx, job.ID, "preflight")
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
	stale, err := s.StartRun(ctx, job.ID, "preflight")
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
	if got.Phase.Terminal() {
		t.Fatalf("phase 保持原样（不伪造阶段），实际 %s", got.Phase)
	}

	// 恢复之后同一个任务能重新开跑：不会再被唯一索引挡住。
	if _, err := s.StartRun(ctx, job.ID, "preflight"); err != nil {
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
