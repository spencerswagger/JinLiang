package control

import (
	"context"
	"database/sql"
	"testing"

	"github.com/spencerswagger/jinliang/internal/state"
)

// ① 旧库升级：stub=1 的历史行必须回填成 kind='stub'，且 stub 列保留一版。
//
// 造旧库的方式：先按当前版本开库，再手工把 kind 列删掉、schema_version 拨回 1，
// 写入一条 stub=1 的「历史运行」，关库重开让迁移跑一遍。
func TestMigrateRunsKindBackfillsOldStubRows(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	s1, err := Open(Options{DataDir: dir, Autogen: true})
	if err != nil {
		t.Fatal(err)
	}
	src, tgt := seedConnections(t, s1)
	job, err := s1.CreateJob(ctx, sampleJobInput(src, tgt))
	if err != nil {
		t.Fatal(err)
	}
	// 退回「v1 结构」：把 v2（心跳/归属/结束原因）与 v3（kind）加的列都删掉，
	// 这样重开时两条迁移都会真跑一遍——只删 kind 的话 v2 会因为列已存在而报
	// "duplicate column name"，测的就不是升级了。
	// 注意：**每条迁移新增的列都要在这里删掉**，否则重开时那条迁移会因为
	// "duplicate column name" 失败，测的就不是升级了（v6 的 paused_tables 就踩过）。
	for _, col := range []string{"kind", "heartbeat_at", "owner_instance", "finish_reason", "code", "paused_tables"} {
		if _, err := s1.DB().ExecContext(ctx, `ALTER TABLE runs DROP COLUMN `+col); err != nil {
			t.Fatalf("模拟 v1 旧库失败（删 %s；SQLite 需要 3.35+ 的 DROP COLUMN）: %v", col, err)
		}
	}
	if err := s1.SetSetting(ctx, SettingSchemaVersion, "1"); err != nil {
		t.Fatal(err)
	}
	// 两条历史行：一条桩（stub=1）、一条真跑（stub=0）。
	// 都要写 ended_at：#5 的 idx_runs_one_active 是「一个任务只能有一条未结束的 run」，
	// 历史行当然是已结束的，否则插第二条就会被唯一索引挡住。
	if _, err := s1.DB().ExecContext(ctx,
		`INSERT INTO runs (id, job_id, phase, started_at, ended_at, rows_done, stub) VALUES (?, ?, 'done', ?, ?, 0, 1)`,
		"legacy-stub", job.ID, s1.nowText(), s1.nowText()); err != nil {
		t.Fatal(err)
	}
	if _, err := s1.DB().ExecContext(ctx,
		`INSERT INTO runs (id, job_id, phase, started_at, ended_at, rows_done, stub) VALUES (?, ?, 'done', ?, ?, 10, 0)`,
		"legacy-real", job.ID, s1.nowText(), s1.nowText()); err != nil {
		t.Fatal(err)
	}
	if err := s1.Close(); err != nil {
		t.Fatal(err)
	}

	// 重新打开 → 迁移 2 应该跑一遍
	s2, err := Open(Options{DataDir: dir, Autogen: true})
	if err != nil {
		t.Fatalf("旧库升级失败: %v", err)
	}
	defer s2.Close()
	if v, err := s2.SchemaVersion(ctx); err != nil || v != LatestSchemaVersion() {
		t.Fatalf("schema_version = %d err=%v", v, err)
	}
	for id, want := range map[string]string{"legacy-stub": RunKindStub, "legacy-real": RunKindSync} {
		run, err := s2.GetRun(ctx, id)
		if err != nil {
			t.Fatalf("读回 %s: %v", id, err)
		}
		if run.Kind != want {
			t.Errorf("%s 的 kind = %q，期望 %q（回填没生效）", id, run.Kind, want)
		}
	}
	// stub 列必须还在（B1 收口后才删）
	var hasStub bool
	rows, err := s2.DB().QueryContext(ctx, `PRAGMA table_info(runs)`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var (
			cid, notnull, pk int
			name, ctype      string
			dflt             sql.NullString
		)
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if name == "stub" {
			hasStub = true
		}
	}
	rows.Close()
	if !hasStub {
		t.Fatal("stub 列被删掉了：裁决要求保留一版做兼容")
	}

	// 幂等：再开一次不报错、值不变
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
	s3, err := Open(Options{DataDir: dir, Autogen: true})
	if err != nil {
		t.Fatalf("重跑迁移应该幂等，却报错: %v", err)
	}
	defer s3.Close()
	run, err := s3.GetRun(ctx, "legacy-stub")
	if err != nil || run.Kind != RunKindStub {
		t.Fatalf("重开后 legacy-stub = %+v err=%v", run, err)
	}
}

// ② kind='stub' 的历史行必须能写、能读（B0 桩已从生产路径移除，但读侧兼容一版）。
func TestStubRunKindStillReadable(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	src, tgt := seedConnections(t, s)
	job, err := s.CreateJob(ctx, sampleJobInput(src, tgt))
	if err != nil {
		t.Fatal(err)
	}
	// 直接按桩的语义写一行：kind=stub，兼容列 stub=1
	run, err := s.StartRun(ctx, job.ID, state.Preflight, RunKindStub)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != RunKindStub {
		t.Fatalf("桩行的 kind = %q，期望 %q", got.Kind, RunKindStub)
	}
	if !got.Stub {
		t.Fatal("兼容列 stub 也应仍为 1")
	}
	if _, err := s.FinishRun(ctx, run.ID, state.Failed, "桩路径已下线", 1, FinishInterrupted); err != nil {
		t.Fatal(err)
	}
}

// ③ 正常同步运行写 kind='sync'；未知种类直接拒绝（不靠列默认值蒙混）。
func TestSyncRunKindAndUnknownRejected(t *testing.T) {
	s := newTestStore(t)
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
	got, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != RunKindSync {
		t.Fatalf("同步运行的 kind = %q，期望 %q", got.Kind, RunKindSync)
	}
	if got.Stub {
		t.Fatal("同步运行的兼容列 stub 必须是 0")
	}
	if _, err := s.StartRun(ctx, job.ID, state.Preflight, "whatever"); err == nil {
		t.Fatal("未知 kind 必须被拒绝")
	}
}

// ④ 列表页与任务摘要的「最近一次运行」只认 kind='sync'：
// 即使 verify 行更晚，也不能把它当成同步运行展示。
func TestLatestRunIgnoresVerifyRows(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	src, tgt := seedConnections(t, s)
	job, err := s.CreateJob(ctx, sampleJobInput(src, tgt))
	if err != nil {
		t.Fatal(err)
	}

	syncRun, err := s.StartRun(ctx, job.ID, state.Preflight, RunKindSync)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.FinishRun(ctx, syncRun.ID, state.Failed, "样例失败", 1, FinishFailed); err != nil {
		t.Fatal(err)
	}

	// verify 运行更晚（也写成一行 run），但它不是同步。
	// 这里不收尾：preflight → done 不是合法迁移，而本用例只关心「最近一次同步运行」的取值。
	verifyRun, err := s.StartRun(ctx, job.ID, state.Preflight, RunKindVerify)
	if err != nil {
		t.Fatal(err)
	}
	if verifyRun.Kind != RunKindVerify {
		t.Fatalf("verify 运行的 kind = %q", verifyRun.Kind)
	}
	// 让 verify 行在时间上严格更晚：started_at 是 RFC3339（秒精度），同一秒内
	// 两条记录谁「最新」只能靠 id 兜底，而 uuid7 在同毫秒内的尾部是随机的——
	// 靠它排序会偶发失败（CI 已经抓到过一次）。这里显式把时间拉开，断言才稳。
	if _, err := s.DB().ExecContext(ctx,
		`UPDATE runs SET started_at = ? WHERE id = ?`, "2099-01-01T00:00:00Z", verifyRun.ID); err != nil {
		t.Fatal(err)
	}

	latestAny, err := s.LatestRun(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if latestAny.Kind != RunKindVerify {
		t.Fatalf("LatestRun 应取任意种类里最新的（verify），实际 %q", latestAny.Kind)
	}

	latestSync, err := s.LatestSyncRun(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if latestSync.ID != syncRun.ID {
		t.Fatalf("LatestSyncRun 误取了 %s（kind=%s），期望同步运行 %s",
			latestSync.ID, latestSync.Kind, syncRun.ID)
	}

	// 任务摘要是列表页的数据来源，同样只能看到同步运行
	sum, err := s.GetJobSummary(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if sum.LastRun == nil {
		t.Fatal("任务摘要应该带最近一次同步运行")
	}
	if sum.LastRun.Kind != RunKindSync || sum.LastRun.ID != syncRun.ID {
		t.Fatalf("任务摘要的最近运行 = %s（kind=%s），期望同步运行 %s",
			sum.LastRun.ID, sum.LastRun.Kind, syncRun.ID)
	}

	// 列表页同理
	jobs, err := s.ListJobs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].LastRun == nil || jobs[0].LastRun.Kind != RunKindSync {
		t.Fatalf("任务列表的最近运行不是同步运行：%+v", jobs)
	}
}
