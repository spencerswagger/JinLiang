//go:build integration

package gc_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/gc"
	"github.com/spencerswagger/jinliang/internal/runtime"
)

func openDSN(t *testing.T, env string) *runtime.DB {
	t.Helper()
	raw := os.Getenv(env)
	if raw == "" {
		t.Fatalf("%s 未设置：集成测试需要真实数据库；缺 DSN 必须失败，不允许跳过", env)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db, err := runtime.Open(ctx, raw)
	if err != nil {
		t.Fatalf("%s 连不上：%v", env, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func mustExec(t *testing.T, db *runtime.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(context.Background(), q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

func hasSlot(t *testing.T, db *runtime.DB, name string) bool {
	t.Helper()
	slots, err := db.Slots(context.Background(), "jl_")
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range slots {
		if s.Name == name {
			return true
		}
	}
	return false
}

func find(plan gc.Plan, name string) gc.Decision {
	for _, d := range plan.Decisions {
		if d.Name == name {
			return d
		}
	}
	return gc.Decision{}
}

func TestGCIntegrationOtherInstanceSlotIsNeverTouched(t *testing.T) {
	src := openDSN(t, "JINLIANG_TEST_PG_SRC_DSN")
	tgt := openDSN(t, "JINLIANG_TEST_PG_DSN")
	ctx := context.Background()

	mustExec(t, tgt, `CREATE TABLE IF NOT EXISTS _jl_object (
		object_name text PRIMARY KEY, object_kind text NOT NULL, job_key text NOT NULL,
		instance_id text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), dropped_at timestamptz)`)
	mustExec(t, tgt, `CREATE TABLE IF NOT EXISTS _jl_lease (
		job_key text PRIMARY KEY, instance_id text NOT NULL, label text, expires_at timestamptz NOT NULL)`)

	// 造三个真实 slot：别人的（在用）、我的（空闲）、未登记的（无法判定）。
	for _, s := range []string{"jl_gctest_1", "jl_gctest_2", "jl_gctest_3"} {
		_, _ = src.Exec(ctx, `SELECT pg_drop_replication_slot($1)`, s) // 上次失败留下的
		mustExec(t, src, `SELECT pg_create_physical_replication_slot($1)`, s)
	}
	t.Cleanup(func() {
		for _, s := range []string{"jl_gctest_1", "jl_gctest_2", "jl_gctest_3"} {
			_, _ = src.Exec(context.Background(), `SELECT pg_drop_replication_slot($1)`, s)
		}
		_, _ = tgt.Exec(context.Background(), `DELETE FROM _jl_object WHERE object_name LIKE 'jl_gctest%'`)
		_, _ = tgt.Exec(context.Background(), `DELETE FROM _jl_lease WHERE job_key LIKE 'gctest%'`)
	})

	mustExec(t, tgt, `INSERT INTO _jl_object (object_name, object_kind, job_key, instance_id)
		VALUES ('jl_gctest_1','slot','gctestaaaa','other-instance'),
		       ('jl_gctest_2','slot','gctestbbbb','me')
		ON CONFLICT (object_name) DO UPDATE SET instance_id = EXCLUDED.instance_id, dropped_at = NULL`)
	// 对方实例持有有效租约 = 「其他实例在用」。
	mustExec(t, tgt, `INSERT INTO _jl_lease (job_key, instance_id, expires_at)
		VALUES ('gctestaaaa','other-instance', now() + interval '5 minutes')
		ON CONFLICT (job_key) DO UPDATE SET expires_at = EXCLUDED.expires_at`)

	plan, err := gc.Load(ctx, src, tgt, "me", gc.Options{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if d := find(plan, "jl_gctest_1"); d.Verdict != gc.InUseByOtherInstance || d.Delete {
		t.Fatalf("other instance's slot must be protected: %+v", d)
	}
	if d := find(plan, "jl_gctest_2"); d.Verdict != gc.OwnedByThisInstance || !d.Delete {
		t.Fatalf("my idle slot should be eligible: %+v", d)
	}
	if d := find(plan, "jl_gctest_3"); d.Verdict != gc.Unknown || d.Delete {
		t.Fatalf("unregistered slot must be unknown and untouched: %+v", d)
	}

	// dry-run 不删任何东西。
	if _, err := gc.Execute(ctx, plan, src); err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"jl_gctest_1", "jl_gctest_2", "jl_gctest_3"} {
		if !hasSlot(t, src, s) {
			t.Fatalf("dry-run deleted %s", s)
		}
	}

	// 真跑：只应删掉「我的空闲 slot」，另一个实例的与未知的都必须还在。
	plan.DryRun = false
	res, err := gc.Execute(ctx, plan, src)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Deleted) != 1 || res.Deleted[0] != "jl_gctest_2" {
		t.Fatalf("deleted %v, want only jl_gctest_2", res.Deleted)
	}
	if hasSlot(t, src, "jl_gctest_2") {
		t.Fatal("jl_gctest_2 should be gone")
	}
	if !hasSlot(t, src, "jl_gctest_1") {
		t.Fatal("gc deleted another instance's slot")
	}
	if !hasSlot(t, src, "jl_gctest_3") {
		t.Fatal("gc deleted an unregistered (unknown) slot")
	}

	// --force 也不能碰仍然被其他实例租用的对象。
	forced, err := gc.Load(ctx, src, tgt, "me", gc.Options{DryRun: false, AllowOtherInstances: true})
	if err != nil {
		t.Fatal(err)
	}
	if d := find(forced, "jl_gctest_1"); d.Delete {
		t.Fatalf("--force must not delete a leased other-instance slot: %+v", d)
	}
}
