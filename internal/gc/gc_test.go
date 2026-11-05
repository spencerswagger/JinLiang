package gc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/runtime"
)

func at(now time.Time, d time.Duration) time.Time { return now.Add(d) }

func baseInput(now time.Time) Input {
	return Input{
		InstanceID:    "me",
		Now:           now,
		RegistryKnown: true,
		Registered: []runtime.Object{
			{Name: "jl_jobaaaaaaa_1", Kind: "slot", JobKey: "jobaaaaaaa", InstanceID: "me"},
			{Name: "jl_jobbbbbbbb_1", Kind: "slot", JobKey: "jobbbbbbbb", InstanceID: "me"},
			{Name: "jl_jobccccccc_1", Kind: "slot", JobKey: "jobccccccc", InstanceID: "other"},
			{Name: "jl_jobddddddd_1", Kind: "slot", JobKey: "jobddddddd", InstanceID: "me"},
		},
		Leases: []runtime.Lease{
			{JobKey: "jobbbbbbbb", InstanceID: "me", ExpiresAt: at(now, time.Minute)},
			{JobKey: "jobccccccc", InstanceID: "other", ExpiresAt: at(now, time.Minute)},
		},
		Live: []LiveObject{
			{Name: "jl_jobaaaaaaa_1", Kind: "slot", Exists: true},               // 本实例创建、空闲
			{Name: "jl_jobbbbbbbb_1", Kind: "slot", Exists: true},               // 本实例在用（租约）
			{Name: "jl_jobccccccc_1", Kind: "slot", Exists: true},               // 其他实例在用
			{Name: "jl_unregistered_1", Kind: "slot", Exists: true},             // 无法判定
			{Name: "jl_jobddddddd_1", Kind: "slot", Active: true, Exists: true}, // 本实例 active（无租约）
		},
	}
}

func verdictOf(p Plan, name string) Decision {
	for _, d := range p.Decisions {
		if d.Name == name {
			return d
		}
	}
	return Decision{}
}

func TestFourVerdicts(t *testing.T) {
	now := time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC)
	p := Build(baseInput(now), Options{DryRun: true})

	cases := []struct {
		name    string
		verdict Verdict
		del     bool
	}{
		{"jl_jobaaaaaaa_1", OwnedByThisInstance, true},
		{"jl_jobbbbbbbb_1", InUseByThisInstance, false},
		{"jl_jobccccccc_1", InUseByOtherInstance, false},
		{"jl_unregistered_1", Unknown, false},
		{"jl_jobddddddd_1", InUseByThisInstance, false},
	}
	for _, tc := range cases {
		d := verdictOf(p, tc.name)
		if d.Verdict != tc.verdict {
			t.Errorf("%s: verdict %s, want %s (reason: %s)", tc.name, d.Verdict, tc.verdict, d.Reason)
		}
		if d.Delete != tc.del {
			t.Errorf("%s: delete=%v, want %v", tc.name, d.Delete, tc.del)
		}
	}
	if p.Deletable != 1 {
		t.Fatalf("default plan must delete exactly the one idle own slot, got %d: %+v", p.Deletable, p.Decisions)
	}
}

func TestDefaultPlanNeverTouchesOtherInstanceObject(t *testing.T) {
	now := time.Now()
	in := baseInput(now)
	p := Build(in, Options{DryRun: true})
	d := verdictOf(p, "jl_jobccccccc_1")
	if d.Verdict != InUseByOtherInstance || d.Delete {
		t.Fatalf("other instance's slot must stay: %+v", d)
	}
	if d.Owner != "other" {
		t.Fatalf("owner should be reported for the human, got %q", d.Owner)
	}
	// 即使 --force，也只在对方空闲（无 active / 无租约）时才允许；
	// 这里对方持有有效租约，必须仍然不动。
	p2 := Build(in, Options{DryRun: false, AllowOtherInstances: true})
	d2 := verdictOf(p2, "jl_jobccccccc_1")
	if d2.Delete {
		t.Fatalf("--force must not delete an object another instance is actively leasing: %+v", d2)
	}
}

func TestForceAllowsIdleOtherInstanceObject(t *testing.T) {
	now := time.Now()
	in := baseInput(now)
	in.Leases = nil // 对方没有租约，也非 active
	in.Live = []LiveObject{{Name: "jl_jobccccccc_1", Kind: "slot", Exists: true}}
	p := Build(in, Options{DryRun: false, AllowOtherInstances: true})
	d := verdictOf(p, "jl_jobccccccc_1")
	if d.Verdict != InUseByOtherInstance || !d.Delete {
		t.Fatalf("--force should allow an idle object owned by another instance: %+v", d)
	}
}

// Regression for Owl #10 item 2: publication has no active flag, so a running
// job's publication must be protected by the still-active slot of the same job
// even when the lease is missing/expired.
func TestPublicationProtectedWhileSameJobSlotIsActive(t *testing.T) {
	now := time.Now()
	in := Input{
		InstanceID:    "me",
		Now:           now,
		RegistryKnown: true,
		Registered: []runtime.Object{
			{Name: "jl_jobpub0001_1", Kind: "slot", JobKey: "jobpub0001", InstanceID: "me"},
			{Name: "jl_jobpub0001", Kind: "publication", JobKey: "jobpub0001", InstanceID: "me"},
		},
		// 租约缺失：只看 publication 自己的字段会误判成「本实例创建且空闲」。
		Live: []LiveObject{
			{Name: "jl_jobpub0001_1", Kind: "slot", Active: true, Exists: true},
			{Name: "jl_jobpub0001", Kind: "publication", Exists: true},
		},
	}
	p := Build(in, Options{DryRun: true})
	if d := verdictOf(p, "jl_jobpub0001"); d.Verdict != InUseByThisInstance || d.Delete {
		t.Fatalf("publication must be protected by the active slot: %+v", d)
	}
	if p.Deletable != 0 {
		t.Fatalf("deletable=%d, want 0", p.Deletable)
	}
}

func TestPublicationIsEligibleWhenJobIsIdle(t *testing.T) {
	now := time.Now()
	in := Input{
		InstanceID:    "me",
		Now:           now,
		RegistryKnown: true,
		Registered: []runtime.Object{
			{Name: "jl_jobpub0002_1", Kind: "slot", JobKey: "jobpub0002", InstanceID: "me"},
			{Name: "jl_jobpub0002", Kind: "publication", JobKey: "jobpub0002", InstanceID: "me"},
		},
		Live: []LiveObject{
			{Name: "jl_jobpub0002_1", Kind: "slot", Active: false, Exists: true},
			{Name: "jl_jobpub0002", Kind: "publication", Exists: true},
		},
	}
	p := Build(in, Options{DryRun: true})
	if d := verdictOf(p, "jl_jobpub0002"); d.Verdict != OwnedByThisInstance || !d.Delete {
		t.Fatalf("idle publication should be eligible: %+v", d)
	}
	if p.Deletable != 2 {
		t.Fatalf("deletable=%d, want 2 (slot + publication)", p.Deletable)
	}
}

func TestUnknownNeverDeletedEvenWithForce(t *testing.T) {
	now := time.Now()
	in := baseInput(now)
	in.RegistryKnown = false // 登记表读不到
	p := Build(in, Options{DryRun: false, AllowOtherInstances: true})
	for _, d := range p.Decisions {
		if d.Verdict != Unknown {
			t.Fatalf("registry unknown must make every object unknown, got %+v", d)
		}
		if d.Delete {
			t.Fatalf("unknown object must never be deleted: %+v", d)
		}
	}
	if p.Deletable != 0 {
		t.Fatalf("deletable=%d, want 0", p.Deletable)
	}
}

type fakeDropper struct {
	got []string
	err error
}

func (f *fakeDropper) DropObject(_ context.Context, name, _ string) error {
	f.got = append(f.got, name)
	return f.err
}

func TestExecuteRespectsDryRunAndOnlyEligible(t *testing.T) {
	now := time.Now()
	in := baseInput(now)
	dry := &fakeDropper{}
	p := Build(in, Options{DryRun: true})
	if _, err := Execute(context.Background(), p, dry); err != nil {
		t.Fatal(err)
	}
	if len(dry.got) != 0 {
		t.Fatalf("dry-run touched %v", dry.got)
	}

	real := &fakeDropper{}
	p = Build(in, Options{DryRun: false})
	res, err := Execute(context.Background(), p, real)
	if err != nil {
		t.Fatal(err)
	}
	if len(real.got) != 1 || real.got[0] != "jl_jobaaaaaaa_1" {
		t.Fatalf("expected only the idle own slot, got %v", real.got)
	}
	if len(res.Deleted) != 1 {
		t.Fatalf("result %+v", res)
	}
}

func TestExecuteReportsFailureAndContinues(t *testing.T) {
	now := time.Now()
	in := baseInput(now)
	in.Registered = append(in.Registered, runtime.Object{
		Name: "jl_jobeeeeeee_1", Kind: "publication", JobKey: "jobeeeeeee", InstanceID: "me",
	})
	in.Live = append(in.Live, LiveObject{Name: "jl_jobeeeeeee_1", Kind: "publication", Exists: true})
	d := &fakeDropper{err: errors.New("permission denied")}
	p := Build(in, Options{DryRun: false})
	res, err := Execute(context.Background(), p, d)
	if err == nil {
		t.Fatal("expected the first error to be returned")
	}
	if len(res.Failed) != 2 || len(res.Deleted) != 0 {
		t.Fatalf("expected both to fail, got %+v", res)
	}
}
