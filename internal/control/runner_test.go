package control

import (
	"context"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/state"
)

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("超时等待：%s", what)
}

func TestRunnerStubWalksEveryPhase(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	src, tgt := seedConnections(t, s)
	job, err := s.CreateJob(ctx, sampleJobInput(src, tgt))
	if err != nil {
		t.Fatal(err)
	}

	ch, cancel := s.Bus().Subscribe(job.ID)
	defer cancel()

	run, err := s.Runner().Start(ctx, job.ID, ActorCLI)
	if err != nil {
		t.Fatal(err)
	}
	if !run.Stub {
		t.Fatal("B0 的运行必须标记为桩")
	}

	seen := map[state.Phase]bool{}
	deadline := time.After(12 * time.Second)
	for done := false; !done; {
		select {
		case ev := <-ch:
			if ev.Phase != "" {
				seen[ev.Phase] = true
			}
			if ev.Type == "done" {
				done = true
			}
		case <-deadline:
			t.Fatalf("没等到 done，已看到阶段 %v", seen)
		}
	}
	for _, p := range []state.Phase{state.Preflight, state.Prepare, state.Snapshot, state.Catchup, state.Streaming, state.Draining, state.Done} {
		if !seen[p] {
			t.Errorf("缺少阶段 %s", p)
		}
	}

	final, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Phase != state.Done || final.EndedAt == "" {
		t.Fatalf("final run = %+v", final)
	}
	if final.RowsDone != 0 {
		t.Fatalf("桩运行不能报告搬了数据（rows_done=%d）", final.RowsDone)
	}
	if !final.Stub {
		t.Fatal("run 记录里必须保留 stub 标记")
	}
}

func TestRunnerRefusesDoubleStart(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	src, tgt := seedConnections(t, s)
	job, err := s.CreateJob(ctx, sampleJobInput(src, tgt))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Runner().Start(ctx, job.ID, ActorCLI); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Runner().Start(ctx, job.ID, ActorCLI); err == nil {
		t.Fatal("同一个任务不能并行跑两份")
	}
	if err := s.Runner().Stop(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Runner().Wait(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerPauseResume(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	src, tgt := seedConnections(t, s)
	job, err := s.CreateJob(ctx, sampleJobInput(src, tgt))
	if err != nil {
		t.Fatal(err)
	}
	run, err := s.Runner().Start(ctx, job.ID, ActorCLI)
	if err != nil {
		t.Fatal(err)
	}
	// 只在「能进入 paused」的阶段暂停：stage 表里 paused 只能从 snapshot/catchup/
	// streaming 进；如果等到 draining 才暂停，那一刻已经没有可暂停的东西了，
	// 测试会在 -race（更慢）下随机超时。
	waitFor(t, "运行记录进入 snapshot/catchup/streaming", func() bool {
		cur, err := s.GetRun(ctx, run.ID)
		if err != nil {
			return false
		}
		switch cur.Phase {
		case state.Snapshot, state.Catchup, state.Streaming:
			return true
		}
		return false
	})
	if err := s.Runner().Pause(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "阶段变成 paused", func() bool {
		cur, err := s.GetRun(ctx, run.ID)
		return err == nil && cur.Phase == state.Paused
	})
	if err := s.Runner().Resume(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "恢复后离开 paused", func() bool {
		cur, err := s.GetRun(ctx, run.ID)
		return err == nil && cur.Phase != state.Paused
	})
	if err := s.Runner().Stop(ctx, job.ID); err != nil && err != ErrNoActiveRun {
		t.Fatal(err)
	}
	if err := s.Runner().Wait(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerPauseWithoutRunFails(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	src, tgt := seedConnections(t, s)
	job, err := s.CreateJob(ctx, sampleJobInput(src, tgt))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Runner().Pause(ctx, job.ID); err != ErrNoActiveRun {
		t.Fatalf("pause without a run = %v, want ErrNoActiveRun", err)
	}
}

func TestEventsReplayFromRing(t *testing.T) {
	s := newTestStore(t)
	bus := s.Bus()
	for i := 0; i < 3; i++ {
		bus.Publish(Event{JobID: "j1", Type: "note", Message: "x"})
	}
	all := bus.Since("j1", 0)
	if len(all) != 3 {
		t.Fatalf("ring = %d events", len(all))
	}
	tail := bus.Since("j1", all[0].Seq)
	if len(tail) != 2 {
		t.Fatalf("replay after seq = %d events, want 2", len(tail))
	}
	if all[0].PhaseLabel != "" && all[0].PhaseLabel != PhaseLabel(all[0].Phase) {
		t.Fatalf("phase label not filled: %q", all[0].PhaseLabel)
	}
}
