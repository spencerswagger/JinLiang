//go:build integration

package engine_test

// DDL 改类型 → 只把**那张表**记成暂停、位点不动、对齐后从原位点恢复追平。
// 这条用例跑的是**真 MySQL 源**（binlog 的 TABLE_MAP 带新列类型），不是造事件 ——
// 改类型这条路上最容易骗过自己的就是「造出来的事件长得对」。
//
// 判据（每条都对着契约 notes/contract-paused-tables.md §1/§3 写）：
//  1. paused_tables 里**只有改类型的那张表**，reason 是稳定码 type_changed，
//     detail 里同时有**源端前后类型**（bigint → varchar(64)）与目标端映射；
//  2. run 以 E1015 结束，位点在暂停前后**逐字节相等**（暂停不是位点推进的副作用）；
//  3. 同批次里另一张表的那一行也**没有**落库（位点只有一个 → 整条同步停下；
//     这是当前契约，不是疏漏，见用例末尾的断言与注释）；
//  4. 对齐目标端列类型后重跑：暂停记录消失、位点前进、两张表都与源端一致。

import (
	"context"
	"os"
	execpkg "os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/spencerswagger/jinliang/internal/pgsink"
)

func TestMySQLSourceTypeChangePausesOnlyThatTableAndRecovers(t *testing.T) {
	srcDSN, dstDSN := mysqlDSNs(t)
	my := mustMySQL(t, srcDSN)
	dst := mustConn(t, dstDSN)
	ctx := context.Background()

	const changed = "engine_my_type_change" // 改类型的那张
	const other = "engine_my_bystander"     // 旁证：同一批次里另一张表
	for _, tbl := range []string{changed, other} {
		myExec(t, my, "DROP TABLE IF EXISTS "+tbl)
		exec(t, dst, "DROP TABLE IF EXISTS "+tbl)
	}
	myExec(t, my, "CREATE TABLE "+changed+" (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NULL) ENGINE=InnoDB")
	myExec(t, my, "CREATE TABLE "+other+" (id BIGINT NOT NULL PRIMARY KEY, payload VARCHAR(64) NOT NULL) ENGINE=InnoDB")
	myExec(t, my, "INSERT INTO "+changed+" (id,v) VALUES (1,10),(2,20),(3,30)")
	myExec(t, my, "INSERT INTO "+other+" (id,payload) VALUES (1,'r1'),(2,'r2'),(3,'r3')")
	exec(t, dst, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")

	bin := buildBinary(t)
	dataDir := filepath.Join(t.TempDir(), "data")
	jobName := "mysql-type-change"
	jobYAML := filepath.Join(t.TempDir(), "job.yaml")
	yaml := "version: 1\njob: " + jobName + "\n" +
		"source:\n  kind: mysql\n  dsn: env:JINLIANG_TEST_MYSQL_SRC_DSN\n" +
		"  scope:\n    schema: " + mySchemaOf(t0DSN("JINLIANG_TEST_MYSQL_SRC_DSN")) + "\n" +
		"  snapshot:\n    chunk_rows: 1000\n" +
		"target:\n  kind: postgres\n  dsn: env:JINLIANG_TEST_PG_DSN\n  schema: public\n  on_exists: append\n" +
		"content:\n  tables:\n    " + changed + ": {}\n    " + other + ": {}\n" +
		"mode:\n  frequency: continuous+initial\n"
	if err := os.WriteFile(jobYAML, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := execpkg.Command(bin, "import", "--file", jobYAML, "--data-dir", dataDir).CombinedOutput(); err != nil {
		t.Fatalf("import 失败: %v\n%s", err, out)
	}
	run := func(extra ...string) (string, int) {
		args := append([]string{"run", "--job", jobName, "--data-dir", dataDir}, extra...)
		cmd := execpkg.Command(bin, args...)
		cmd.Env = os.Environ()
		out, err := cmd.CombinedOutput()
		code := 0
		if ee, ok := err.(*execpkg.ExitError); ok {
			code = ee.ExitCode()
		} else if err != nil {
			t.Fatalf("跑 run 失败: %v\n%s", err, out)
		}
		return string(out), code
	}

	// ── 第 1 轮：搬完存量，拿到权威位点 ──────────────────────────────
	if out, code := run("--stop-when-idle", "3s"); code != 0 {
		t.Fatalf("首轮全量失败（退出码 %d）：\n%s", code, out)
	}
	if got := pgCount(t, dst, changed); got != 3 {
		t.Fatalf("全量后 %s 应有 3 行，实际 %d", changed, got)
	}
	posFull := myPosition(t, dst)
	mustMySQLPos(t, "全量后", posFull)

	// ── 第 2 轮：流着的时候源端改列类型 ──────────────────────────────
	cmd := execpkg.Command(bin, "run", "--job", jobName, "--data-dir", dataDir)
	cmd.Env = os.Environ()
	logFile, err := os.Create(filepath.Join(t.TempDir(), "run-ddl.log"))
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动增量轮失败: %v", err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()

	// 先让流真的跑起来（写一行、等它到目标端），再动 DDL。
	//
	// **位点基线要在这里取**：上面那行流过去之后「数据 + 位点」一起提交，位点本来就该前进；
	// 拿全量后的位点当基线，会把这次正常前进误判成「暂停期间位点被推进」。
	myExec(t, my, "INSERT INTO "+other+" (id,payload) VALUES (10,'r10')")
	waitRows(t, dst, other, 4, 45*time.Second)
	posBefore := myPosition(t, dst)
	mustMySQLPos(t, "改类型前", posBefore)

	myExec(t, my, "ALTER TABLE "+changed+" MODIFY COLUMN v VARCHAR(64) NULL")
	myExec(t, my, "INSERT INTO "+changed+" (id,v) VALUES (11,'abc')")
	myExec(t, my, "INSERT INTO "+other+" (id,payload) VALUES (11,'r11')")

	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		_ = cmd.Process.Kill()
		body, _ := os.ReadFile(logFile.Name())
		t.Fatalf("源端改列类型之后任务没有停下来（60s 仍在跑）——这一批会静默写错或一直重试：\n%s", body)
	}
	_ = logFile.Close()
	body, _ := os.ReadFile(logFile.Name())
	out := string(body)

	// 判据②：run 以「结构对不上」结束（E1015 那张四段式卡片），不是「未识别的数据库错误」。
	// 断言落在卡片文案与「已登记暂停」这两句上：码表里的编号不一定印在 stdout，
	// 但「表 + 列 + 前后类型 + 位点没推进」这四件事必须都在。
	for _, want := range []string{"与源端的列类型不一致", "已登记暂停", "type_changed", "位点没有推进"} {
		if !strings.Contains(out, want) {
			t.Fatalf("改类型后的输出里应有 %q（E1015 卡片的一部分），实际：\n%s", want, out)
		}
	}
	// 判据④的前半：**暂停这一批没有把位点带走**。
	//
	// 这里不能直接断言「位点逐字节不变」：源端改类型的那条 ALTER 在 binlog 里自成一笔事务，
	// 它被 MySQL 源按「本版不搬 DDL」过滤掉之后，引擎仍然要把事务边界往上走（空批次只推进位点）——
	// 否则位点会永久卡在这条 DDL 上、每轮都从它开始重放。所以「位点不动」的准确含义是：
	// **出问题的那一批一条都没落库，位点也没越过它**。可观测的判据是下面两条：
	//   ① 位点只前进不回退（没有任何回退的余地）；
	//   ② 那张表里待重放的那一行（id=11）不在目标端 —— 位点若越过它，恢复后就再也补不回来。
	posAfterFail := myPosition(t, dst)
	mustMySQLPos(t, "暂停后", posAfterFail)
	if myPosLess(posAfterFail, posBefore) {
		t.Fatalf("位点回退了：%s → %s", posBefore, posAfterFail)
	}
	if pgHasRow(t, dst, changed, "id = 11") {
		t.Fatalf("%s 里出现了 id=11：改类型那一批本该整批拒绝（位点没越过它，重放时才补得上）", changed)
	}

	// 判据①：paused_tables 里只有改类型的那张表，且记录可读。
	jobKey := myJobKey(t, dst)
	paused, err := readPaused(t, dst, jobKey)
	if err != nil {
		t.Fatalf("读暂停记录失败: %v", err)
	}
	rec, ok := paused[changed]
	if !ok {
		t.Fatalf("paused_tables 里没有 %s：%v。改类型必须**留痕**，不能只失败不记录", changed, paused)
	}
	if _, bad := paused[other]; bad {
		t.Fatalf("paused_tables 里出现了没改类型的表 %s：%v。"+
			"暂停记录的 key 必须是**出问题的那张表**，不能把同批次别的表一起记上", other, paused)
	}
	if rec.Reason != pgsink.PauseReasonTypeChanged {
		t.Fatalf("暂停原因码应为 type_changed，实际 %q（detail=%s）。原因码是机器判等用的稳定值", rec.Reason, rec.Detail)
	}
	for _, want := range []string{"bigint", "varchar(64)", "v"} { // 64 是**字符**：binlog 给的是字节 256
		if !strings.Contains(rec.Detail, want) {
			t.Fatalf("detail 里应有 %q（用户要知道哪一列、从什么类型到什么类型），实际 %q", want, rec.Detail)
		}
	}
	if !strings.Contains(rec.Detail, "bigint → varchar(64)") {
		t.Fatalf("detail 应写成「源端 bigint → varchar(64)」这种前后对照，实际 %q", rec.Detail)
	}

	// 判据③（当前契约的诚实断言）：另一张表的那一行也没落库。
	if got := pgCount(t, dst, other); got != 4 {
		t.Fatalf("%s 现在应有 4 行（id=11 那行不该进），实际 %d。"+
			"位点只有一个 → 整条同步会停下，这是契约 notes/contract-paused-tables.md §4.1 写明的边界；"+
			"若将来改成「其它表继续跑」，这条断言要跟着改成 5", other, got)
	}

	// ── 第 3 轮：对齐目标端列类型 → 重跑，从原位点恢复并追平 ─────────
	exec(t, dst, "ALTER TABLE "+changed+" ALTER COLUMN v TYPE text USING v::text")
	out2, code := run("--stop-when-idle", "5s")
	if code != 0 {
		t.Fatalf("对齐类型后重跑失败（退出码 %d）：\n%s", code, out2)
	}
	if strings.Contains(out2, "全量完成") {
		t.Fatalf("对齐后重跑又搬了一遍全量（应该从原位点重放）：\n%s", out2)
	}
	if paused, err := readPaused(t, dst, jobKey); err != nil {
		t.Fatalf("读暂停记录失败: %v", err)
	} else if len(paused) != 0 {
		t.Fatalf("对齐后暂停记录应已解除，实际还有：%v", paused)
	}
	posFixed := myPosition(t, dst)
	mustMySQLPos(t, "恢复后", posFixed)
	if !myPosLess(posBefore, posFixed) {
		t.Fatalf("恢复后位点没有前进：%s → %s", posBefore, posFixed)
	}

	// 位点没越过那一批的**正面证据**：恢复后 id=11 被重放补上了。
	if !pgHasRow(t, dst, changed, "id = 11 AND v = 'abc'") {
		t.Fatalf("%s 恢复后仍没有 id=11/'abc'：说明位点已经越过了改类型那一批，这段变更永久丢失", changed)
	}

	// 判据④的后半：两张表都与源端一致（改类型的列按值语义比对）。
	for _, tc := range []struct{ tbl, srcQ, dstQ string }{
		{changed,
			"SELECT CONCAT(COUNT(*), ':', COALESCE(GROUP_CONCAT(CONCAT(id,'|',COALESCE(v,'∅')) ORDER BY id),'')) FROM " + changed,
			"SELECT COUNT(*)::text || ':' || COALESCE(string_agg(id::text||'|'||COALESCE(v,'∅'), ',' ORDER BY id), '') FROM " + changed},
		{other,
			"SELECT CONCAT(COUNT(*), ':', COALESCE(GROUP_CONCAT(CONCAT(id,'|',payload) ORDER BY id),'')) FROM " + other,
			"SELECT COUNT(*)::text || ':' || COALESCE(string_agg(id::text||'|'||payload, ',' ORDER BY id), '') FROM " + other},
	} {
		var srcFP, dstFP string
		if err := my.QueryRow(tc.srcQ).Scan(&srcFP); err != nil {
			t.Fatalf("读源端 %s 指纹失败: %v", tc.tbl, err)
		}
		if err := dst.QueryRow(ctx, tc.dstQ).Scan(&dstFP); err != nil {
			t.Fatalf("读目标端 %s 指纹失败: %v", tc.tbl, err)
		}
		if srcFP != dstFP {
			t.Fatalf("%s 恢复后两端不一致：源 %s / 目标 %s", tc.tbl, srcFP, dstFP)
		}
	}
	t.Logf("改类型暂停：%s 被记成 %s（%s），位点 %s 未动；对齐后重放追平，位点 %s",
		changed, rec.Reason, rec.Detail, posBefore, posFixed)
}

// myJobKey 读本任务在目标端的 job_key（_jl_meta 只有这一行 MySQL 位点）。
func myJobKey(t *testing.T, conn *pgx.Conn) string {
	t.Helper()
	var k string
	if err := conn.QueryRow(context.Background(),
		"SELECT job_key FROM _jl_meta WHERE COALESCE(position,'') LIKE 'mysql:v1:%'").Scan(&k); err != nil {
		t.Fatalf("读 job_key 失败: %v", err)
	}
	return k
}

// waitRows 等目标端某张表到 n 行（流真的跑起来了才动 DDL）。
func waitRows(t *testing.T, conn *pgx.Conn, table string, n int, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if pgCount(t, conn, table) >= n {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("等 %s 到 %d 行超时（实际 %d）：增量流没有跑起来", table, n, pgCount(t, conn, table))
}

// pgHasRow 报告目标表里有没有满足条件的那一行。
func pgHasRow(t *testing.T, conn *pgx.Conn, table, where string) bool {
	t.Helper()
	var n int
	if err := conn.QueryRow(context.Background(),
		"SELECT count(*) FROM "+table+" WHERE "+where).Scan(&n); err != nil {
		t.Fatalf("查 %s 的行失败: %v", table, err)
	}
	return n > 0
}
