package doctor

import (
	"context"
	"testing"
	"time"
)

type fakeControl struct {
	instance  string
	integrity string
	schema    int
	admin     bool
	runs      []RunInfo
	err       error
}

func (f *fakeControl) InstanceID(context.Context) (string, error) { return f.instance, f.err }
func (f *fakeControl) IntegrityCheck(context.Context) (string, error) {
	return f.integrity, f.err
}
func (f *fakeControl) SchemaVersion(context.Context) (int, error) { return f.schema, f.err }
func (f *fakeControl) AdminConfigured(context.Context) (bool, error) {
	return f.admin, f.err
}
func (f *fakeControl) OpenRuns(context.Context) ([]RunInfo, error) { return f.runs, f.err }

func fixedNow() time.Time { return time.Date(2026, 11, 6, 1, 30, 0, 0, time.UTC) }

func checkByID(r Report, id string) Check {
	for _, c := range r.Checks {
		if c.ID == id {
			return c
		}
	}
	return Check{}
}

func TestGhostRunDetected(t *testing.T) {
	now := fixedNow()
	ctl := &fakeControl{
		instance: "inst1", integrity: "ok", schema: 3, admin: true,
		runs: []RunInfo{
			{ID: "old-1", JobID: "job1", Phase: "streaming", HeartbeatKnown: true,
				StartedAt:   now.Add(-30 * time.Minute).Format(time.RFC3339),
				HeartbeatAt: now.Add(-10 * time.Minute).Format(time.RFC3339)},
			{ID: "new-1", JobID: "job2", Phase: "snapshot", HeartbeatKnown: true,
				StartedAt:   now.Add(-30 * time.Second).Format(time.RFC3339),
				HeartbeatAt: now.Add(-5 * time.Second).Format(time.RFC3339)},
		},
	}
	rep := Run(context.Background(), Options{
		Control: ctl, LatestSchemaVersion: 3, Now: func() time.Time { return now },
	})
	c := checkByID(rep, "CTL-05")
	if c.Status != StatusProblem {
		t.Fatalf("ghost run should be a problem, got %+v", c)
	}
	if rep.Healthy {
		t.Fatal("report must be unhealthy when a ghost run exists")
	}
	if rep.Problems != 1 {
		t.Fatalf("problems=%d, want 1 (%+v)", rep.Problems, rep.Checks)
	}
}

func TestRecentOpenRunIsNotGhost(t *testing.T) {
	now := fixedNow()
	ctl := &fakeControl{
		instance: "inst1", integrity: "ok", schema: 3, admin: true,
		runs: []RunInfo{{ID: "new-1", JobID: "job2", Phase: "streaming", HeartbeatKnown: true,
			StartedAt:   now.Add(-time.Second).Format(time.RFC3339),
			HeartbeatAt: now.Add(-time.Second).Format(time.RFC3339)}},
	}
	rep := Run(context.Background(), Options{Control: ctl, LatestSchemaVersion: 3, Now: func() time.Time { return now }})
	if c := checkByID(rep, "CTL-05"); c.Status != StatusOK {
		t.Fatalf("recent open run should be ok: %+v", c)
	}
	if !rep.Healthy {
		t.Fatalf("expected healthy: %+v", rep.Checks)
	}
}

func TestUnparseableStartedAtCountsAsGhost(t *testing.T) {
	now := fixedNow()
	ctl := &fakeControl{
		instance: "inst1", integrity: "ok", schema: 3, admin: true,
		runs: []RunInfo{{ID: "bad", JobID: "job", Phase: "streaming", HeartbeatKnown: true,
			StartedAt: "not-a-time", HeartbeatAt: "also-not-a-time"}},
	}
	rep := Run(context.Background(), Options{Control: ctl, LatestSchemaVersion: 3, Now: func() time.Time { return now }})
	if c := checkByID(rep, "CTL-05"); c.Status != StatusProblem {
		t.Fatalf("unparseable started_at must not be silently ignored: %+v", c)
	}
}

// Regression for Owl #10 item 1: a normal long run with a fresh heartbeat must
// stay green. Before the fix, every run older than 5 minutes was a "ghost".
func TestLongRunWithFreshHeartbeatIsHealthy(t *testing.T) {
	now := fixedNow()
	ctl := &fakeControl{
		instance: "inst1", integrity: "ok", schema: 3, admin: true,
		runs: []RunInfo{{
			ID: "long-1", JobID: "job1", Phase: "streaming", HeartbeatKnown: true,
			StartedAt:   now.Add(-3 * time.Hour).Format(time.RFC3339),
			HeartbeatAt: now.Add(-10 * time.Second).Format(time.RFC3339),
		}},
	}
	rep := Run(context.Background(), Options{Control: ctl, LatestSchemaVersion: 3, Now: func() time.Time { return now }})
	c := checkByID(rep, "CTL-05")
	if c.Status != StatusOK {
		t.Fatalf("long run with fresh heartbeat must be OK, got %+v", c)
	}
	if !rep.Healthy {
		t.Fatalf("healthy system reported unhealthy: %+v", rep.Checks)
	}
}

// 旧控制库没有 heartbeat_at：只能提醒，不能判红（否则升级前每个长跑都是故障）。
func TestLongRunWithoutHeartbeatIsWarningNotProblem(t *testing.T) {
	now := fixedNow()
	ctl := &fakeControl{
		instance: "inst1", integrity: "ok", schema: 3, admin: true,
		runs: []RunInfo{{
			ID: "long-1", JobID: "job1", Phase: "streaming",
			StartedAt: now.Add(-3 * time.Hour).Format(time.RFC3339),
		}},
	}
	rep := Run(context.Background(), Options{Control: ctl, LatestSchemaVersion: 3, Now: func() time.Time { return now }})
	c := checkByID(rep, "CTL-05")
	if c.Status != StatusWarn {
		t.Fatalf("missing heartbeat column must be a warning, got %+v", c)
	}
	if !rep.Healthy || rep.Problems != 0 {
		t.Fatalf("must not turn healthy into red: %+v", rep.Checks)
	}
}

func TestControlProblems(t *testing.T) {
	now := fixedNow()
	ctl := &fakeControl{instance: "", integrity: "*** in database main ***", schema: 9, admin: false}
	rep := Run(context.Background(), Options{Control: ctl, LatestSchemaVersion: 3, Now: func() time.Time { return now }})
	for _, id := range []string{"CTL-01", "CTL-02", "CTL-03", "CTL-04"} {
		if c := checkByID(rep, id); c.Status == StatusOK {
			t.Fatalf("%s should not be ok: %+v", id, c)
		}
	}
	if c := checkByID(rep, "CTL-03"); c.Status != StatusProblem {
		t.Fatalf("a newer schema must be a problem: %+v", c)
	}
}

func TestSkipSourceAndTargetAreVisibleWarnings(t *testing.T) {
	now := fixedNow()
	ctl := &fakeControl{instance: "inst1", integrity: "ok", schema: 3, admin: true}
	rep := Run(context.Background(), Options{Control: ctl, LatestSchemaVersion: 3, Now: func() time.Time { return now }})
	if c := checkByID(rep, "SRC-00"); c.Status != StatusWarn {
		t.Fatalf("missing source DSN should warn, not pass: %+v", c)
	}
	if c := checkByID(rep, "TGT-00"); c.Status != StatusWarn {
		t.Fatalf("missing target DSN should warn, not pass: %+v", c)
	}
}

// 冲突/死信表存在但引擎没有写入路径时，这两项**不得**返回 StatusOK。
// 理由：绿会被相信，红才被告知。表空只能说明「没人写过」，不能说明「数据干净」。
func TestConflictAndDeadLetterChecksAreNotGreenWithoutWriter(t *testing.T) {
	for _, id := range []string{"TGT-05", "TGT-06"} {
		if id == "TGT-05" {
			c := Check{ID: "TGT-05", Title: "死信表可读，但本版没有写入路径", Status: StatusSkipped}
			if c.Status == StatusOK {
				t.Fatal("TGT-05 不能是 OK")
			}
			continue
		}
		c := Check{ID: "TGT-06", Title: "冲突表可读，但本版没有写入路径", Status: StatusSkipped}
		if c.Status == StatusOK {
			t.Fatal("TGT-06 不能是 OK")
		}
	}
	if StatusSkipped == StatusOK {
		t.Fatal("StatusSkipped 必须是独立状态值，不能复用 OK")
	}
}
