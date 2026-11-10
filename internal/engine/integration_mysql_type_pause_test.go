//go:build integration

package engine_test

// DDL 改类型 → **只停这一张表**：其它表照写、位点照常前进；结构对齐后整表重搬补齐。
// 这条用例跑的是**真 MySQL 源**（binlog 的 TABLE_MAP 带新列类型），不是造事件 ——
// 改类型这条路上最容易骗过自己的就是「造出来的事件长得对」。
//
// 判据（对着契约 docs/CONTRACT-PAUSE-AND-POSITION.md §1.5/§1.6，**与正文断言逐条对应**；
// 正文里用「判据（…）」标注，顺序按断言的先后，不按这里的编号）：
//  1. paused_tables 里**只有改类型的那张表**，reason 是稳定码 type_changed，
//     detail 里同时有**源端前后类型**（bigint → varchar(64)）与目标端映射；
//  2. **其它表照跑**：同一时段另一张表（旁证）的那一行必须落库（5 行；旧契约下会停在 4 行）；
//  3. **位点照常前进**（`posBefore < posAfterPause`），被暂停那张表的行**不写**（跳过是事实）——
//     欠账由恢复时的整表重搬还（不是靠把位点卡住）；
//  4. run **不失败**，`--stop-when-idle` 正常收尾（退出码 0）；
//  5. 对齐目标端列类型后重跑：暂停记录清空、**整表重搬**把跳过的那一行补回来、
//     位点不后退、两张表与源端逐行一致。
//
// **别把旧口径写回来**：#90 之前的行为是「整 run 失败、位点不推进、旁证表的行也停」，
// 那三条已被裁决 A 作废（契约见 docs/CONTRACT-PAUSE-AND-POSITION.md §1.5/§1.6）。
// 注释与正文断言必须描述同一件事——以正文和契约文档为准。

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
	// mixed 专门压「字符类列的渲染」：CHAR / TEXT / VARBINARY / VARCHAR(utf8mb4)。
	// binlog 给的长度是**字节**、TEXT 与 BLOB 共用类型字节 —— 渲染错一个字，这一张表
	// 就会在第一条 Relation 上被判成「改类型」并停下来（Owl II 在 #94 评审里实测过 char→char(40)、text→blob）。
	const mixed = "engine_my_chartypes"
	for _, tbl := range []string{changed, other, mixed} {
		myExec(t, my, "DROP TABLE IF EXISTS "+tbl)
		exec(t, dst, "DROP TABLE IF EXISTS "+tbl)
	}
	myExec(t, my, "CREATE TABLE "+changed+" (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NULL) ENGINE=InnoDB")
	myExec(t, my, "CREATE TABLE "+other+" (id BIGINT NOT NULL PRIMARY KEY, payload VARCHAR(64) NOT NULL) ENGINE=InnoDB")
	myExec(t, my, "INSERT INTO "+changed+" (id,v) VALUES (1,10),(2,20),(3,30)")
	myExec(t, my, "INSERT INTO "+other+" (id,payload) VALUES (1,'r1'),(2,'r2'),(3,'r3')")
	myExec(t, my, "CREATE TABLE "+mixed+` (
		id BIGINT NOT NULL PRIMARY KEY,
		c CHAR(10) NOT NULL,
		t TEXT NULL,
		vb VARBINARY(32) NULL,
		vc VARCHAR(64) NOT NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	myExec(t, my, "INSERT INTO "+mixed+" (id,c,t,vb,vc) VALUES (1,'中文十','文本',UNHEX('0001'),REPEAT('é',20))")
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
		"content:\n  tables:\n    " + changed + ": {}\n    " + other + ": {}\n    " + mixed + ": {}\n" +
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
	// CHAR/TEXT/VARBINARY 三列的全量必须**原样过去**（长度按字节渲染错了这里就露馅）。
	if got := pgCount(t, dst, mixed); got != 1 {
		t.Fatalf("全量后 %s 应有 1 行，实际 %d", mixed, got)
	}
	// CHAR 在两端都会按定义**补空格**到定长（MySQL 取回时剥、PG 不剥），比较时去掉尾空格。
	var c, t2, vb, vc string
	if err := dst.QueryRow(ctx, "SELECT btrim(c), t, encode(vb,'hex'), vc FROM "+mixed+" WHERE id=1").
		Scan(&c, &t2, &vb, &vc); err != nil {
		t.Fatalf("读 %s 失败: %v", mixed, err)
	}
	if c != "中文十" || t2 != "文本" || vc != strings.Repeat("é", 20) || vb != "0001" {
		t.Fatalf("字符类列搬家后不一致：c=%q t=%q vb=%s vc=%q", c, t2, vb, vc)
	}
	posFull := myPosition(t, dst)
	mustMySQLPos(t, "全量后", posFull)

	// ── 第 2 轮：流着的时候源端改列类型 ──────────────────────────────
	//
	// 裁决 A（docs/CONTRACT-PAUSE-AND-POSITION.md，#90/#93）之后的行为：
	//   - 只停**这张表**：它的事件被引擎丢掉，其它表照写；
	//   - 位点**照常前进**（跳过的那段记在 paused_tables 里，靠恢复时整表重搬补齐）；
	//   - run 不失败，安静下来按 --stop-when-idle 收尾。
	cmd := execpkg.Command(bin, "run", "--job", jobName, "--data-dir", dataDir, "--stop-when-idle", "6s")
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

	jobKey := myJobKey(t, dst)

	// 先让流真的跑起来（写一行、等它到目标端），再动 DDL。
	myExec(t, my, "INSERT INTO "+other+" (id,payload) VALUES (10,'r10')")
	if !waitRowsQuiet(t, dst, other, 4, 45*time.Second) {
		body, _ := os.ReadFile(logFile.Name())
		t.Fatalf("增量流没把 id=10 搬过来（%s 仍是 %d 行）。这一轮的输出：\n%s",
			other, pgCount(t, dst, other), body)
	}
	posBefore := myPosition(t, dst)
	mustMySQLPos(t, "改类型前", posBefore)

	myExec(t, my, "ALTER TABLE "+changed+" MODIFY COLUMN v VARCHAR(64) NULL")
	myExec(t, my, "INSERT INTO "+changed+" (id,v) VALUES (11,'abc')")
	myExec(t, my, "INSERT INTO "+other+" (id,payload) VALUES (11,'r11')")

	// 等「这张表被记成暂停」（裁决 A 下 run 不退出，判定靠库里的记录）。
	rec, ok := waitPaused(t, dst, jobKey, changed, 60*time.Second)
	if !ok {
		body, _ := os.ReadFile(logFile.Name())
		t.Fatalf("源端改列类型后 60s 内没有把 %s 记成暂停。这一轮的输出：\n%s", changed, body)
	}

	done := make(chan struct{})
	code := 0
	go func() {
		err := cmd.Wait()
		if ee, ok := err.(*execpkg.ExitError); ok {
			code = ee.ExitCode()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(90 * time.Second):
		_ = cmd.Process.Kill()
		body, _ := os.ReadFile(logFile.Name())
		t.Fatalf("改类型之后任务没有安静下来（90s 仍在跑）：\n%s", body)
	}
	_ = logFile.Close()
	body, _ := os.ReadFile(logFile.Name())
	out := string(body)
	// 判据（run 不失败）：裁决 A 下「只停该表」不该把整条同步判死。
	if code != 0 {
		t.Fatalf("裁决 A 下「只停该表」不该让 run 失败，实际退出码 %d：\n%s", code, out)
	}

	// 判据（只有那张表进 paused）：paused_tables 里只有改类型的那张表，且记录可读。
	if _, bad := readPausedMap(t, dst, jobKey)[other]; bad {
		t.Fatalf("paused_tables 里出现了没改类型的表 %s：%v", other, readPausedMap(t, dst, jobKey))
	}
	if rec.Reason != pgsink.PauseReasonTypeChanged {
		t.Fatalf("暂停原因码应为 type_changed，实际 %q（detail=%s）。原因码是机器判等用的稳定值", rec.Reason, rec.Detail)
	}
	for _, want := range []string{"bigint", "v"} {
		if !strings.Contains(rec.Detail, want) {
			t.Fatalf("detail 里应有 %q（用户要知道哪一列），实际 %q", want, rec.Detail)
		}
	}
	// 前后对照必须写清楚。长度单位随元数据级别可能差一档（FULL 带列字符集 → varchar(64)；
	// 拿不到列字符集的版本只能按字节报 → varchar(256)），两种都要给出「从什么到什么」。
	if !strings.Contains(rec.Detail, "bigint → varchar(64)") &&
		!strings.Contains(rec.Detail, "bigint → varchar(256)") {
		t.Fatalf("detail 应写成「源端 bigint → varchar(N)」这种前后对照，实际 %q", rec.Detail)
	}

	// 判据（其它表照跑）：旁证表那一行必须落库（旧契约下它停在 4 行）。
	if got := pgCount(t, dst, other); got != 5 {
		t.Fatalf("%s 应有 5 行（裁决 A：一张表坏了别的表继续跑；旧契约下是 4 行），实际 %d：\n%s",
			other, got, out)
	}
	// 判据（位点照常前进）：跳过的那段靠恢复时整表重搬补齐，不靠卡住位点。
	posAfterPause := myPosition(t, dst)
	mustMySQLPos(t, "暂停后", posAfterPause)
	if !myPosLess(posBefore, posAfterPause) {
		t.Fatalf("裁决 A 下位点必须前进：%s → %s（旧契约才要求暂停期间位点不动）", posBefore, posAfterPause)
	}
	// 被暂停那张表的行**没有**进目标端（跳过是事实，不能假装写了）。
	if pgHasRow(t, dst, changed, "id = 11") {
		t.Fatalf("%s 里出现了 id=11：暂停期间这张表的行应当被跳过（欠账交给恢复时的整表重搬）", changed)
	}

	// ── 第 3 轮：对齐目标端列类型 → 重跑：解除暂停 + 整表重搬补齐 ────
	exec(t, dst, "ALTER TABLE "+changed+" ALTER COLUMN v TYPE text USING v::text")
	out2, code2 := run("--stop-when-idle", "8s")
	if code2 != 0 {
		t.Fatalf("对齐类型后重跑失败（退出码 %d）：\n%s", code2, out2)
	}
	if paused, err := readPaused(t, dst, jobKey); err != nil {
		t.Fatalf("读暂停记录失败: %v", err)
	} else if len(paused) != 0 {
		t.Fatalf("对齐后暂停记录应已解除，实际还有：%v", paused)
	}
	posFixed := myPosition(t, dst)
	mustMySQLPos(t, "恢复后", posFixed)
	// 恢复这一轮**不该**要求位点再前进：整表重搬是补数据，不是推位点；
	// 位点只能不动或前进，退后就是重放，重放会让 md5 对不上。
	if myPosLess(posFixed, posAfterPause) {
		t.Fatalf("恢复后位点退后了：%s → %s", posAfterPause, posFixed)
	}
	// 跳过的那一行靠**整表重搬**补回来（不是靠位点重放）。
	if !pgHasRow(t, dst, changed, "id = 11 AND v = 'abc'") {
		t.Fatalf("%s 恢复后仍没有 id=11/'abc'：整表重搬没把它补回来，这段变更永久丢失：\n%s", changed, out2)
	}

	// 判据（恢复后一致）：整表重搬补齐之后，两张表都与源端逐行一致。
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
	t.Logf("改类型暂停（裁决 A）：%s 记成 %s（%s）；期间位点 %s → %s、旁证表照写到 5 行；"+
		"对齐后整表重搬补齐，位点 %s",
		changed, rec.Reason, rec.Detail, posBefore, posAfterPause, posFixed)
}

// readPausedMap 读暂停记录（不存在就返回空 map）。
func readPausedMap(t *testing.T, conn *pgx.Conn, jobKey string) map[string]pgsink.PausedTable {
	t.Helper()
	m, err := readPaused(t, conn, jobKey)
	if err != nil {
		t.Fatalf("读暂停记录失败: %v", err)
	}
	return m
}

// waitPaused 等某张表被记成暂停（裁决 A 下 run 不会因此退出，只能看库里的记录）。
func waitPaused(t *testing.T, conn *pgx.Conn, jobKey, table string,
	d time.Duration) (pgsink.PausedTable, bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if m, err := readPaused(t, conn, jobKey); err == nil {
			if rec, ok := m[table]; ok {
				return rec, true
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return pgsink.PausedTable{}, false
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

// waitRowsQuiet 等目标端某张表到 n 行，只回报结果（调用方自己决定打印什么）。
func waitRowsQuiet(t *testing.T, conn *pgx.Conn, table string, n int, d time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if pgCount(t, conn, table) >= n {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
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
