//go:build integration

package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/engine"
	"github.com/spencerswagger/jinliang/internal/pgsink"
)

// PG 源无主键表 → MySQL 目标端：全量 insert-only 搬完、行数一致，追加的 INSERT 也照写。
//
// 这是 S1（docs/DESIGN-NO-PRIMARY-KEY.md §5）在真库上的验收：这条路径以前会死在
// `ErrNoUniqueKey`（全量也走 Apply），现在按「源端无身份 → 只 INSERT」照跑。
// 改/删要触发按表暂停（S2），这里**不碰**——收到 UPDATE 时本片仍是明确失败，不静默降级。
func TestMySQLTargetInsertOnlyNoPKSource(t *testing.T) {
	srcDSN, dstDSN := mysqlTargetDSNs(t)
	src := mustConn(t, srcDSN)
	dst := mustMySQLTarget(t, dstDSN)
	table := "engine_my_nopk"

	exec(t, src, "DROP TABLE IF EXISTS "+table)
	exec(t, src, "CREATE TABLE "+table+" (payload text, amount numeric(12,2))") // 无主键
	exec(t, src, "INSERT INTO "+table+" SELECT 'row-'||g, (g%100)::numeric/100 FROM generate_series(1,20) g")
	t.Cleanup(func() { exec(t, src, "DROP TABLE IF EXISTS "+table) })

	_, _ = dst.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+table+"`")
	dropMySQLState(t, dst)

	store, jobID := newMySQLTargetJob(t, srcDSN, dstDSN, "my-target-nopk", table, "continuous+initial")

	res, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, StopAfter: 3 * time.Second})
	if err != nil {
		t.Fatalf("无主键表全量失败（应 insert-only 照跑）: %v", err)
	}
	if res.RowsCopied != 20 {
		t.Fatalf("应复制 20 行，实际 %d（阶段 %s）", res.RowsCopied, res.Phase)
	}
	if n := myCount(t, dst, table); n != 20 {
		t.Fatalf("目标端应有 20 行，实际 %d", n)
	}

	// 追加一行（无改/删）：insert-only 路径的增量照写。
	exec(t, src, "INSERT INTO "+table+" VALUES ('new-row', 1.23)")
	if _, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, StopAfter: 8 * time.Second}); err != nil {
		t.Fatalf("无主键表增量（纯 INSERT）失败: %v", err)
	}
	if n := myCount(t, dst, table); n != 21 {
		t.Fatalf("追加后目标端应有 21 行，实际 %d", n)
	}
}

// PG 源无主键表 + 用户声明 unique_key → MySQL 目标端：全量、增量改、增量删都走得通。
//
// 这是 S3（docs/DESIGN-NO-PRIMARY-KEY.md §4.2）在真库上的验收：声明生效后
// upsert 用声明列（INSERT … ON DUPLICATE KEY UPDATE）、DELETE 也用声明列定位；
// 目标端的那条唯一索引由用户建好，我们只**核对**它（R1：身份只认源端声明）。
func TestMySQLTargetDeclaredUniqueKeyNoPKSource(t *testing.T) {
	srcDSN, dstDSN := mysqlTargetDSNs(t)
	src := mustConn(t, srcDSN)
	dst := mustMySQLTarget(t, dstDSN)
	table := "engine_my_decl"

	exec(t, src, "DROP TABLE IF EXISTS "+table)
	exec(t, src, "CREATE TABLE "+table+" (tenant_id int NOT NULL, event_no varchar(64) NOT NULL, payload text)")
	// 无主键表的 UPDATE/DELETE 走逻辑复制必须 FULL（默认只带主键，没主键就没定位信息）。
	exec(t, src, "ALTER TABLE "+table+" REPLICA IDENTITY FULL")
	exec(t, src, "INSERT INTO "+table+" SELECT 1, 'e'||g, 'row-'||g FROM generate_series(1,10) g")
	t.Cleanup(func() { exec(t, src, "DROP TABLE IF EXISTS "+table) })

	// 目标表由用户建好并带唯一索引（我们只核对，不代建索引）。
	_, _ = dst.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+table+"`")
	dropMySQLState(t, dst)
	if _, err := dst.ExecContext(context.Background(),
		"CREATE TABLE `"+table+"` (tenant_id int NOT NULL, event_no varchar(64) NOT NULL, payload text, UNIQUE KEY uk_event (tenant_id,event_no))"); err != nil {
		t.Fatalf("建目标表: %v", err)
	}

	store, jobID := newMySQLTargetJobWith(t, srcDSN, dstDSN, "my-target-decl", table, "continuous+initial",
		func(doc *control.JobDoc) {
			doc.Content.Tables[table] = control.TableDoc{UniqueKey: []string{"tenant_id", "event_no"}}
		})

	res, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, StopAfter: 3 * time.Second})
	if err != nil {
		t.Fatalf("声明 unique_key 后全量失败: %v", err)
	}
	if res.RowsCopied != 10 {
		t.Fatalf("应复制 10 行，实际 %d（阶段 %s）", res.RowsCopied, res.Phase)
	}
	if n := myCount(t, dst, table); n != 10 {
		t.Fatalf("目标端应有 10 行，实际 %d", n)
	}

	// 改一行（源端无主键，但声明给了身份）：目标端必须更新那一行，不是插一版。
	exec(t, src, "UPDATE "+table+" SET payload='changed' WHERE tenant_id=1 AND event_no='e3'")
	if _, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, StopAfter: 8 * time.Second}); err != nil {
		t.Fatalf("声明身份后的增量改失败: %v", err)
	}
	if n := myCount(t, dst, table); n != 10 {
		t.Fatalf("改行后目标端仍应 10 行（upsert 不新增），实际 %d", n)
	}
	var payload string
	if err := dst.QueryRowContext(context.Background(),
		"SELECT payload FROM `"+table+"` WHERE tenant_id=1 AND event_no='e3'").Scan(&payload); err != nil {
		t.Fatalf("读改后的行: %v", err)
	}
	if payload != "changed" {
		t.Fatalf("目标端未按声明列更新，payload=%q", payload)
	}

	// 删一行：声明列定位删除。
	exec(t, src, "DELETE FROM "+table+" WHERE tenant_id=1 AND event_no='e5'")
	if _, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, StopAfter: 8 * time.Second}); err != nil {
		t.Fatalf("声明身份后的增量删失败: %v", err)
	}
	if n := myCount(t, dst, table); n != 9 {
		t.Fatalf("删除后目标端应 9 行，实际 %d", n)
	}
}

// MySQL 目标端 + 无主键表收到 UPDATE：按表暂停（no_pk_write / E1017），其它表照写、
// 位点照常前进——与 PG 目标端那条语义完全一致（S2 口径，S3 起两个目标端共用同一份错误形状）。
//
// 变异：mysqlsink 把改行退化成整批失败（S1 的旧行为）→ 退出码不是 3，本用例红。
func TestMySQLTargetNoPKUpdatePausesOnlyThatTable(t *testing.T) {
	srcDSN, dstDSN := mysqlTargetDSNs(t)
	src := mustConn(t, srcDSN)
	dst := mustMySQLTarget(t, dstDSN)
	ta, tb := "engine_my_nopk_a", "engine_my_nopk_b"
	ctx := context.Background()

	exec(t, src, "DROP TABLE IF EXISTS "+ta)
	exec(t, src, "CREATE TABLE "+ta+" (payload text, amount numeric(12,2))")
	exec(t, src, "ALTER TABLE "+ta+" REPLICA IDENTITY FULL")
	exec(t, src, "INSERT INTO "+ta+" SELECT 'r'||g, (g%100)::numeric/100 FROM generate_series(1,50) g")
	exec(t, src, "DROP TABLE IF EXISTS "+tb)
	exec(t, src, "CREATE TABLE "+tb+" (id bigint PRIMARY KEY, payload text)")
	exec(t, src, "INSERT INTO "+tb+" SELECT g,'b'||g FROM generate_series(1,20) g")
	t.Cleanup(func() {
		exec(t, src, "DROP TABLE IF EXISTS "+ta)
		exec(t, src, "DROP TABLE IF EXISTS "+tb)
	})
	for _, tbl := range []string{ta, tb} {
		_, _ = dst.ExecContext(ctx, "DROP TABLE IF EXISTS `"+tbl+"`")
	}
	dropMySQLState(t, dst)

	store, jobID := newMySQLTargetJobMulti(t, srcDSN, dstDSN, "my-nopk-pause", "continuous+initial", ta, tb)

	// ① 首搬：insert-only 把 50 行搬完。
	if res, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, SnapshotOnly: true}); err != nil {
		t.Fatalf("首搬失败: %v", err)
	} else if res.ExitCode != 0 {
		t.Fatalf("首搬没有暂停表，应退 0，实际 %d", res.ExitCode)
	}
	if n := myCount(t, dst, ta); n != 50 {
		t.Fatalf("无主键表首搬应 50 行，实际 %d", n)
	}

	// ② 改无主键表一行 + 给另一张表插一行。
	exec(t, src, "UPDATE "+ta+" SET payload='changed' WHERE amount=(SELECT amount FROM "+ta+" LIMIT 1)")
	exec(t, src, "INSERT INTO "+tb+" (id,payload) VALUES (1001,'after-nopk')")

	res, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, IdleAfter: 500 * time.Millisecond})
	if err != nil {
		t.Fatalf("无主键表的改只该停它自己，不该打死整轮: %v", err)
	}
	if res.ExitCode != engine.ExitNeedsAction {
		t.Fatalf("有暂停表必须退 %d，实际 %d", engine.ExitNeedsAction, res.ExitCode)
	}
	job, err := store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	rec := myPausedRecord(t, dst, ta)
	if rec == nil {
		t.Fatal("无主键表必须被登记为暂停")
	}
	if rec.Reason != pgsink.PauseReasonNoPKWrite {
		t.Fatalf("原因码应是 %q，得到 %q（Detail=%s）", pgsink.PauseReasonNoPKWrite, rec.Reason, rec.Detail)
	}
	if rec.SkippedRows < 1 {
		t.Fatalf("跳过窗口要记账（skipped_rows ≥ 1），得到 %+v", rec)
	}
	var posAfter string
	if err := dst.QueryRowContext(ctx, "SELECT COALESCE(position,'') FROM `_jl_meta` WHERE job_key=?",
		job.JobKey).Scan(&posAfter); err != nil {
		t.Fatal(err)
	}
	if posAfter == "" {
		t.Fatal("跳过这张表不等于停下同步：位点必须前进")
	}
	// ③ 只停该表：另一张表的新行必须到。
	if n := myCountWhere(t, dst, tb, "id=1001"); n != 1 {
		t.Fatalf("其它表照写：应看到 id=1001，实际 %d 行", n)
	}
}
