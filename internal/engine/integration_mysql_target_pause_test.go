//go:build integration

package engine_test

// MySQL 目标端的「只停一张表 + 恢复追平」——补上验收腿的最后一块。
//
// 与 PG 目标端那条（`integration_paused_test.go:TestPauseTableRegistersKeyKeepsPositionAndRecovers`）
// **同口径**，便于横向比：
//   ① 源端改 A 的列类型 + 给 A/B 各插一行；
//   ② 这一轮**不许失败**：A 的那行不写、B 的那行照写、位点前进、`skipped_rows` ≥ 1；
//   ③ 目标端对齐 A 的列类型（并在暂停窗口里让源端删掉 50 行——只 upsert 不清表会留幻行）；
//   ④ 重跑：A **清空 + 整表重搬**、暂停解除、两端指纹一致、行数 = 251。
//
// 跨引擎的差异都收在辅助函数里：MySQL 的列类型文本（`decimal(12,2)`）、
// 状态表在目标库（`_jl_meta.paused_tables` 是 JSON 文本）。

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/engine"
	"github.com/spencerswagger/jinliang/internal/pgsink"
)

// newMySQLTargetJobMulti 建一条「PG 源 → MySQL 目标、多张表」的任务。
func newMySQLTargetJobMulti(t *testing.T, srcDSN, dstDSN, jobName string, freq string, tables ...string) (*control.Store, string) {
	t.Helper()
	dir := t.TempDir()
	store, err := control.Open(control.Options{DataDir: dir, Autogen: true})
	if err != nil {
		t.Fatalf("打开控制库失败: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	ctx := context.Background()

	sc, err := store.CreateConnection(ctx, control.ConnectionInput{
		Name: "src", Kind: "postgres", Host: "127.0.0.1", Port: portOf(t, srcDSN),
		Database: dbOf(t, srcDSN), Username: userOf(t, srcDSN), Password: passOf(t, srcDSN),
		Params: map[string]string{"sslmode": "disable"},
	})
	if err != nil {
		t.Fatalf("建 PG 源连接失败: %v", err)
	}
	my := myConfigOf(t, dstDSN)
	tc, err := store.CreateConnection(ctx, control.ConnectionInput{
		Name: "dst", Kind: "mysql", Host: my.host, Port: my.port,
		Database: my.db, Username: my.user, Password: my.pass,
	})
	if err != nil {
		t.Fatalf("建 MySQL 目标连接失败: %v", err)
	}
	content := map[string]control.TableDoc{}
	for _, tbl := range tables {
		content[tbl] = control.TableDoc{}
	}
	job, err := store.CreateJob(ctx, control.JobInput{
		Name: jobName, SourceID: sc.ID, TargetID: tc.ID,
		Spec: control.JobDoc{
			Version: 1, Job: jobName,
			Source:  control.SourceDoc{Kind: "postgres", Scope: &control.ScopeDoc{Schema: "public"}},
			Target:  control.TargetDoc{Kind: "mysql", OnExists: "append"},
			Mode:    control.ModeDoc{Frequency: freq},
			Content: &control.ContentDoc{Tables: content},
		},
	})
	if err != nil {
		t.Fatalf("建任务失败: %v", err)
	}
	dropJobSlot(t, srcDSN, job.JobKey)
	return store, job.ID
}

// myTableFingerprint 是 (id, payload, v) 形状的跨引擎指纹：行数 + 按主键排序逐行 md5 再 md5。
func myTableFingerprint(t *testing.T, db *sql.DB, table string) string {
	t.Helper()
	var n int
	var fp string
	// GROUP_CONCAT 默认只留 1024 字节（25 行 md5）——不调大，指纹会静默截断，两边永远对不上。
	if _, err := db.ExecContext(context.Background(), "SET SESSION group_concat_max_len=1073741824"); err != nil {
		t.Fatalf("调大 group_concat_max_len 失败: %v", err)
	}
	q := "SELECT COUNT(*), COALESCE(MD5(GROUP_CONCAT(h ORDER BY id SEPARATOR '')), '-') FROM (" +
		"SELECT id, MD5(CONCAT_WS('|', id, payload, v)) h FROM `" + table + "`) x"
	if err := db.QueryRowContext(context.Background(), q).Scan(&n, &fp); err != nil {
		t.Fatalf("算 %s 的指纹失败: %v", table, err)
	}
	return fmt.Sprintf("%d:%s", n, fp)
}

// pgTableFingerprint 与上面同口径（PG 侧）。
func pgTableFingerprint(t *testing.T, table string) string {
	t.Helper()
	var n int
	var fp string
	srcDB := mustConn(t, srcEnv(t))
	q := `SELECT count(*), COALESCE(md5(string_agg(h, '' ORDER BY id)), '-') FROM (
		SELECT id, md5(concat_ws('|', id, payload, v)) h FROM ` + table + `) x`
	if err := srcDB.QueryRow(context.Background(), q).Scan(&n, &fp); err != nil {
		t.Fatalf("算源端 %s 的指纹失败: %v", table, err)
	}
	return fmt.Sprintf("%d:%s", n, fp)
}

func srcEnv(t *testing.T) string {
	t.Helper()
	src, _ := mysqlTargetDSNs(t)
	return src
}

func TestMySQLTargetPauseOnlyThatTableAndRecovers(t *testing.T) {
	srcDSN, dstDSN := mysqlTargetDSNs(t)
	srcDB := mustConn(t, srcDSN)
	myDB := mustMySQLTarget(t, dstDSN)
	ctx := context.Background()

	ta, tb := "myp_a", "myp_b"
	for _, tbl := range []string{ta, tb} {
		exec(t, srcDB, "DROP TABLE IF EXISTS "+tbl)
		exec(t, srcDB, "CREATE TABLE "+tbl+" (id bigint PRIMARY KEY, payload text, v bigint)")
		exec(t, srcDB, "INSERT INTO "+tbl+" SELECT g, 'r'||g, g FROM generate_series(1,300) g")
		_, _ = myDB.ExecContext(ctx, "DROP TABLE IF EXISTS `"+tbl+"`")
	}
	dropMySQLState(t, myDB)

	store, jobID := newMySQLTargetJobMulti(t, srcDSN, dstDSN, "myp-pause", "continuous+initial", ta, tb)
	// ① 首搬
	if _, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, SnapshotOnly: true}); err != nil {
		t.Fatalf("首搬失败: %v", err)
	}
	if got := myCount(t, myDB, ta); got != 300 {
		t.Fatalf("首搬后 %s 应有 300 行，实际 %d", ta, got)
	}

	// ② 源端改 A 的列类型 + 两张表各插一行
	exec(t, srcDB, "ALTER TABLE "+ta+" ALTER COLUMN v TYPE numeric(12,2)")
	exec(t, srcDB, "INSERT INTO "+ta+" (id,payload,v) VALUES (301,'after-ddl',1.25)")
	exec(t, srcDB, "INSERT INTO "+tb+" (id,payload,v) VALUES (301,'b-after',7)")

	posBefore := myPausedPosition(t, myDB)

	// ③ 这一轮**不许失败**：只停 A，B 照写，位点前进
	if _, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, IdleAfter: 700 * time.Millisecond}); err != nil {
		t.Fatalf("一张表结构对不上，不该打死整轮（MySQL 目标端）：%v", err)
	}
	if n := myCountWhere(t, myDB, ta, "id=301"); n != 0 {
		t.Fatalf("暂停表的行不许写进目标端，实际 A.id=301 有 %d 行", n)
	}
	if n := myCountWhere(t, myDB, tb, "id=301"); n != 1 {
		t.Fatalf("其它表必须照常推进（这就是「只停该表」）：B.id=301 有 %d 行，期望 1", n)
	}
	posAfter := myPausedPosition(t, myDB)
	if posAfter == posBefore {
		t.Fatalf("跳过了 A 之后位点必须前进（否则下一轮会重读同一段、永远卡住）：%q", posAfter)
	}
	rec := myPausedRecord(t, myDB, ta)
	if rec == nil {
		t.Fatal("A 必须被登记为暂停（MySQL 目标端的 paused_tables 列）")
	}
	if rec.Reason != pgsink.PauseReasonTypeChanged {
		t.Fatalf("原因码应为 %q，得到 %q（detail=%s）", pgsink.PauseReasonTypeChanged, rec.Reason, rec.Detail)
	}
	if rec.SkippedRows < 1 {
		t.Fatalf("跳过窗口必须记账（skipped_rows ≥ 1），得到 %+v", *rec)
	}
	if rec.SkippedFrom == "" {
		t.Fatalf("跳过窗口要记起点（skipped_from），得到 %+v", *rec)
	}

	// ④ 对齐目标端列类型；同时让源端删掉 50 行（只 upsert 不清表 = 幻行）
	if _, err := myDB.ExecContext(ctx, "ALTER TABLE `"+ta+"` MODIFY v DECIMAL(12,2)"); err != nil {
		t.Fatalf("对齐目标端列类型失败: %v", err)
	}
	exec(t, srcDB, "DELETE FROM "+ta+" WHERE id BETWEEN 1 AND 50")

	t0 := time.Now()
	res, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, IdleAfter: 900 * time.Millisecond})
	if err != nil {
		t.Fatalf("对齐结构后重跑应成功: %v", err)
	}
	caughtUp := time.Since(t0)
	if rec := myPausedRecord(t, myDB, ta); rec != nil {
		t.Fatalf("搬成之后必须解除暂停，仍有 %+v", *rec)
	}
	if n := myCount(t, myDB, ta); n != 251 {
		t.Fatalf("重搬后应剩 251 行（300+1 新行−50 删除），实际 %d：清表没做干净就是幻行", n)
	}
	for _, tbl := range []string{ta, tb} {
		if got, want := myTableFingerprint(t, myDB, tbl), pgTableFingerprint(t, tbl); got != want {
			t.Fatalf("%s 恢复后两端指纹应一致：MySQL %s / PG %s", tbl, got, want)
		}
	}
	t.Logf("MySQL 目标端按表暂停：位点 %q → %q（前进）；A.id=301 未写、B.id=301 已写；"+
		"跳过记账 %d 行；对齐后整表重搬并解除，两端指纹一致，重搬追平 %s（全量 %d 行）",
		posBefore, posAfter, rec.SkippedRows, caughtUp.Round(time.Millisecond), res.RowsCopied)
}

// myCountWhere 数 MySQL 目标表里满足条件的行（条件由测试写死，不做参数化）。
func myCountWhere(t *testing.T, db *sql.DB, table, where string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM `"+table+"` WHERE "+where).Scan(&n); err != nil {
		t.Fatalf("数 %s（%s）失败: %v", table, where, err)
	}
	return n
}

// myPausedPosition 读 MySQL 目标端状态表里的权威位点。
func myPausedPosition(t *testing.T, db *sql.DB) string {
	t.Helper()
	var pos string
	if err := db.QueryRowContext(context.Background(),
		"SELECT COALESCE(position,'') FROM `_jl_meta` LIMIT 1").Scan(&pos); err != nil {
		t.Fatalf("读位点失败: %v", err)
	}
	return pos
}

// myPausedRecord 读 MySQL 目标端 `_jl_meta.paused_tables` 里某张表的记录（没有就 nil）。
func myPausedRecord(t *testing.T, db *sql.DB, table string) *pgsink.PausedTable {
	t.Helper()
	var raw string
	if err := db.QueryRowContext(context.Background(),
		"SELECT COALESCE(paused_tables,'') FROM `_jl_meta` LIMIT 1").Scan(&raw); err != nil {
		t.Fatalf("读 paused_tables 失败: %v", err)
	}
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var m map[string]pgsink.PausedTable
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		t.Fatalf("paused_tables 不是合法 JSON（%q）：%v", raw, err)
	}
	if one, ok := m[table]; ok {
		return &one
	}
	return nil
}
