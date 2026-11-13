//go:build integration

package engine_test

// S-NP3（docs/DESIGN-NO-PRIMARY-KEY.md §2.4 / §5 S2 验收）：
// 无主键表**收到一次 UPDATE** 之后，只停这张表——
//   - `paused_tables.reason = no_pk_write`（稳定机器码，不是 type_changed）；
//   - `skipped_rows ≥ 1`（跳过窗口有账，可枚举可补偿）；
//   - **其它表照常写入**（这就是「只停该表」）；
//   - 位点照常前进、退出码 `ExitNeedsAction=3`（cite: #109 的收尾口径）；
//   - 修好身份（两端都加主键）后重跑：**整表重搬**补齐，暂停解除、两端一致。
//
// 前置：PG 源的无主键表必须先 `REPLICA IDENTITY FULL`（§3.5 / PG-06）——
// 不设的话用例在源端解码就断了，测的是 PostgreSQL 而不是我们。这一步是 setup，不是被测行为。
//
// 变异（任一处拿掉即红）：
//   - pgsink 不在写入前判「没有身份」→ UPDATE 会被当成 INSERT 写进去（目标端多一行）；
//   - `NoPKWriteError.Reason()` 写死 type_changed → 第 1 条断言红；
//   - `pauseSink` 不按表过滤 → 其它表那一行也进不来（整批失败/整轮失败）。

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/engine"
	"github.com/spencerswagger/jinliang/internal/pgsink"
	"github.com/spencerswagger/jinliang/internal/state"
)

func TestNoPKUpdatePausesOnlyThatTableAndRecovers(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	srcDB := mustConn(t, srcDSN)
	dstDB := mustConn(t, dstDSN)
	ctx := context.Background()

	ta, tb := "nopk_events", "nopk_orders"
	exec(t, srcDB, "DROP TABLE IF EXISTS "+ta)
	exec(t, srcDB, "CREATE TABLE "+ta+" (id bigint, payload text, v bigint)") // 无主键
	exec(t, srcDB, "INSERT INTO "+ta+" SELECT g,'r'||g,g FROM generate_series(1,300) g")
	// PG-06 前置：没有它，源端在第一条 UPDATE 上就断开整条流（用例会变成测 PG）。
	exec(t, srcDB, "ALTER TABLE "+ta+" REPLICA IDENTITY FULL")
	exec(t, srcDB, "DROP TABLE IF EXISTS "+tb)
	exec(t, srcDB, "CREATE TABLE "+tb+" (id bigint PRIMARY KEY, payload text)")
	exec(t, srcDB, "INSERT INTO "+tb+" SELECT g,'b'||g FROM generate_series(1,200) g")
	for _, tbl := range []string{ta, tb} {
		exec(t, dstDB, "DROP TABLE IF EXISTS "+tbl)
	}
	exec(t, dstDB, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")

	store, jobID := continuousJobBoth(t, srcDSN, dstDSN, "nopk", ta, tb)

	// ① 首搬：insert-only 把无主键表的存量搬完（不需要行身份）。
	if res, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, SnapshotOnly: true}); err != nil {
		t.Fatalf("首搬失败: %v", err)
	} else if res.ExitCode != 0 {
		t.Fatalf("首搬没有暂停表，应退 0，实际 %d", res.ExitCode)
	}
	var copied int
	if err := dstDB.QueryRow(ctx, "SELECT count(*) FROM "+ta).Scan(&copied); err != nil {
		t.Fatal(err)
	}
	if copied != 300 {
		t.Fatalf("无主键表首搬应 300 行（insert-only），实际 %d", copied)
	}

	job, err := store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	var posBefore string
	if err := dstDB.QueryRow(ctx, "SELECT COALESCE(position,'') FROM _jl_meta WHERE job_key=$1",
		job.JobKey).Scan(&posBefore); err != nil {
		t.Fatal(err)
	}

	// ② 源端改无主键表的一行（这一批会触发 no_pk_write），同时给别的表插一行。
	exec(t, srcDB, "UPDATE "+ta+" SET payload='changed', v=v+1000 WHERE id=42")
	exec(t, srcDB, "INSERT INTO "+tb+" (id,payload) VALUES (1001,'after-nopk')")

	res, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, IdleAfter: 500 * time.Millisecond})
	if err != nil {
		t.Fatalf("无主键表的改/删只该停它自己，不该打死整轮：%v", err)
	}

	// ③ 退出码 / 收尾口径：有暂停表 → done + finish_reason=paused_tables + 退 3。
	run, gerr := store.GetRun(ctx, res.RunID)
	if gerr != nil {
		t.Fatal(gerr)
	}
	if run.Phase != state.Done {
		t.Fatalf("表级暂停不是失败，阶段应是 done，得到 %q（last_error=%s）", run.Phase, run.LastError)
	}
	if run.FinishReason != control.FinishPausedTables {
		t.Fatalf("finish_reason 应为 %q，得到 %q", control.FinishPausedTables, run.FinishReason)
	}
	if res.ExitCode != engine.ExitNeedsAction {
		t.Fatalf("有暂停表必须退 %d（ExitNeedsAction），实际 %d", engine.ExitNeedsAction, res.ExitCode)
	}

	// ④ 暂停记录：原因为 no_pk_write、有跳过记账、位点仍前进。
	paused, err := readPaused(t, dstDB, job.JobKey)
	if err != nil {
		t.Fatal(err)
	}
	rec, ok := paused[ta]
	if !ok || len(paused) != 1 {
		t.Fatalf("应只暂停 %s，实际 %v", ta, paused)
	}
	if rec.Reason != pgsink.PauseReasonNoPKWrite {
		t.Fatalf("原因码应是 %q，得到 %q（Detail=%s）", pgsink.PauseReasonNoPKWrite, rec.Reason, rec.Detail)
	}
	if rec.SkippedRows < 1 {
		t.Fatalf("跳过窗口必须记账（skipped_rows ≥ 1），得到 %+v", rec)
	}
	var posAfter string
	if err := dstDB.QueryRow(ctx, "SELECT COALESCE(position,'') FROM _jl_meta WHERE job_key=$1",
		job.JobKey).Scan(&posAfter); err != nil {
		t.Fatal(err)
	}
	if posAfter == posBefore {
		t.Fatalf("跳过这张表不等于停下同步：位点必须前进（%q）", posBefore)
	}

	// ⑤ 只停该表：无主键表被跳过的更新没写进目标端（仍是旧值），其它表那一行必须到。
	var payload string
	var v int64
	if err := dstDB.QueryRow(ctx, "SELECT payload, v FROM "+ta+" WHERE id=42").Scan(&payload, &v); err != nil {
		t.Fatal(err)
	}
	if payload == "changed" {
		t.Fatal("没有身份的表，改动写不进去；写进去了说明 UPDATE 被当成 INSERT/upsert 硬写（错）")
	}
	var n int
	if err := dstDB.QueryRow(ctx, "SELECT count(*) FROM "+ta).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 300 {
		t.Fatalf("跳过该表的更新不该多出一行（无身份的表做 UPDATE 若被降级成 INSERT 会变 301），实际 %d", n)
	}
	if err := dstDB.QueryRow(ctx, "SELECT count(*) FROM "+tb+" WHERE id=1001").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("其它表必须照常推进（这就是「只停该表」）：nopk_orders 的 1001 行=%d，期望 1", n)
	}

	// ⑥ 修身份（两端都加主键）后重跑：整表重搬补齐、暂停解除、两端一致。
	exec(t, srcDB, "ALTER TABLE "+ta+" ADD PRIMARY KEY (id)")
	exec(t, dstDB, "ALTER TABLE "+ta+" ADD PRIMARY KEY (id)")
	res2, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, IdleAfter: 700 * time.Millisecond})
	if err != nil {
		t.Fatalf("补上主键后重跑应成功: %v", err)
	}
	if res2.RowsCopied < 1 {
		t.Fatalf("暂停表必须整表重搬（RowsCopied=%d）：只从旧位点重放补不回跳过的那次更新", res2.RowsCopied)
	}
	paused2, err := readPaused(t, dstDB, job.JobKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(paused2) != 0 {
		t.Fatalf("重搬搬成 → 必须解除暂停，仍有：%v", paused2)
	}
	for _, tbl := range []string{ta, tb} {
		if fp := tableFingerprint(t, dstDB, tbl); fp != tableFingerprint(t, srcDB, tbl) {
			t.Fatalf("%s 恢复后应与源端一致：目标 %s / 源 %s", tbl, fp, tableFingerprint(t, srcDB, tbl))
		}
	}
	if err := dstDB.QueryRow(ctx, "SELECT count(*) FROM "+ta+" WHERE id=42 AND v=1042").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal("重搬后应看到源端的新值（v=1042）——这正是「跳过窗口由整表重搬补齐」")
	}
	var lastErr string
	if err := dstDB.QueryRow(ctx, "SELECT COALESCE(last_error,'') FROM _jl_meta WHERE job_key=$1",
		job.JobKey).Scan(&lastErr); err != nil && !strings.Contains(err.Error(), "no rows") {
		t.Logf("（读 last_error 跳过：%v）", err)
	}
}
