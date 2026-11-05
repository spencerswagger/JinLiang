package control

import (
	"context"
	"testing"
	"time"
)

// 迁移 v2 必须能从真实的 v1 控制库升上来：三列 + partial unique index，
// 并把升级前遗留的活跃 run 收尾（否则唯一索引建不起来）。
func TestMigrationV2UpgradesExistingV1Database(t *testing.T) {
	dir := t.TempDir()
	all := migrations
	migrations = all[:1] // 只跑到 v1，模拟老二进制建出来的库
	v1, err := Open(Options{DataDir: dir, Autogen: true})
	if err != nil {
		migrations = all
		t.Fatal(err)
	}
	ctx := context.Background()
	src, tgt := seedConnections(t, v1)
	job, err := v1.CreateJob(ctx, sampleJobInput(src, tgt))
	if err != nil {
		migrations = all
		t.Fatal(err)
	}
	// v1 的 runs 表没有心跳列，所以这里直接按 v1 的列插：
	//   - 一条「十分钟前开始、再也没动过」的遗留 run（该被收尾）
	//   - 一条「刚刚开始」的 open run（不能碰：它可能是另一个活进程的）
	const (
		legacyRunID = "legacy-run-from-v1"
		freshRunID  = "fresh-run-from-v1"
	)
	old := v1.Now().Add(-10 * time.Minute).UTC().Format(time.RFC3339)
	if _, err := v1.DB().ExecContext(ctx,
		`INSERT INTO runs (id, job_id, phase, started_at, rows_done, stub) VALUES (?, ?, 'preflight', ?, 0, 0)`,
		legacyRunID, job.ID, old); err != nil {
		migrations = all
		t.Fatal(err)
	}
	freshJob, err := v1.CreateJob(ctx, func() JobInput {
		in := sampleJobInput(src, tgt)
		in.Name = "fresh-job"
		in.Spec.Source.Scope.Include = []string{"invoices"}
		in.Spec.Content = &ContentDoc{Tables: map[string]TableDoc{"invoices": {}}}
		return in
	}())
	if err != nil {
		migrations = all
		t.Fatal(err)
	}
	if _, err := v1.DB().ExecContext(ctx,
		`INSERT INTO runs (id, job_id, phase, started_at, rows_done, stub) VALUES (?, ?, 'preflight', ?, 0, 0)`,
		freshRunID, freshJob.ID, v1.nowText()); err != nil {
		migrations = all
		t.Fatal(err)
	}
	if err := v1.Close(); err != nil {
		migrations = all
		t.Fatal(err)
	}
	migrations = all

	// 新版二进制打开同一个库：迁移应自动 apply。
	v2, err := Open(Options{DataDir: dir, Autogen: true})
	if err != nil {
		t.Fatalf("v1 → v2 迁移失败: %v", err)
	}
	defer v2.Close()

	ver, err := v2.SchemaVersion(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ver != LatestSchemaVersion() || ver < 2 {
		t.Fatalf("schema_version = %d，期望 %d", ver, LatestSchemaVersion())
	}

	for _, col := range []string{"heartbeat_at", "owner_instance", "finish_reason"} {
		if !columnExists(t, v2, "runs", col) {
			t.Fatalf("迁移后 runs 缺少列 %s", col)
		}
	}

	// 心跳过期的遗留 run：必须被收尾（否则「一条活跃 run」的唯一索引建不起来）。
	got, err := v2.GetRun(ctx, legacyRunID)
	if err != nil {
		t.Fatal(err)
	}
	if got.EndedAt == "" {
		t.Fatalf("十分钟没心跳的 run 应该被标成已结束（interrupted）: %+v", got)
	}
	if got.FinishReason != FinishInterrupted {
		t.Fatalf("遗留 run 的 finish_reason = %q，期望 interrupted", got.FinishReason)
	}
	if !got.Phase.Terminal() {
		t.Fatalf("收尾后 phase 必须是终态（历史页才能显示对），实际 %s", got.Phase)
	}

	// 心跳新鲜的 open run：迁移**不能**碰它。一个只跑 200ms 的 export 也会打开
	// 同一个控制库，它无权把正在服务的 serve 手里的活 run 标成中断。
	fresh, err := v2.GetRun(ctx, freshRunID)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.EndedAt != "" {
		t.Fatalf("刚开 1 秒的 run 被迁移误杀了: %+v", fresh)
	}

	// 迁移写了一条审计，说明收尾了几条。
	var audits int
	if err := v2.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM audit WHERE action = 'migrate.recover_runs'`).Scan(&audits); err != nil {
		t.Fatal(err)
	}
	if audits != 1 {
		t.Fatalf("迁移收尾应写 1 条审计，实际 %d", audits)
	}

	// 迁移后仍能正常开新 run（唯一索引没有把业务挡住）。
	if _, err := v2.StartRun(ctx, freshJob.ID, "preflight"); err == nil {
		// freshJob 还有一条 open run，唯一索引应该挡住第二次。
		t.Fatal("同一任务第二条活跃 run 应被唯一索引拒绝")
	}
	if _, err := v2.StartRun(ctx, job.ID, "preflight"); err != nil {
		t.Fatalf("收尾过的任务应该能重新开跑: %v", err)
	}
}

func columnExists(t *testing.T, s *Store, table, column string) bool {
	t.Helper()
	rows, err := s.DB().QueryContext(context.Background(), `SELECT name FROM pragma_table_info(?)`, table)
	if err != nil {
		t.Fatalf("pragma_table_info(%s): %v", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		if name == column {
			return true
		}
	}
	return false
}
