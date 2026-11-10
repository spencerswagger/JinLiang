//go:build integration

package engine_test

// MySQL 源「增量跑到一半被 kill -9」的续跑验收 —— 口径与昨晚 PG 源那条（resume_kill_test.go）
// 完全一致，只是把源端换成 binlog：
//
//   - **重搬行数必须为 0**：重启后不许重跑全量（早期版本会退化成整轮重搬）；
//   - 目标端行数与内容 md5 必须与源端逐行一致（不是「差不多」）；
//   - 中断时落库的位点不许回退：增量从更早的位点重放会重复，从更晚的位点起会漏一段，
//     两者都会让 md5 对不上，但只有位点断言能把「碰巧对上」和「语义对」分开。
//
// 为什么要用子进程而不是进程内 cancel：进程内 cancel 会走 teardown（放租约、收尾、
// 把位点落盘），而 kill -9 什么都来不及做 —— 用户遇到的是后者。
//
// 缺 DSN 直接失败（不跳过）：跳过＝静默通过，这个仓库不接受。

import (
	"context"
	"database/sql"
	"os"
	execpkg "os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// 存量行数：够大才搬得出「全量 → 增量」两段，又不至于让 PR 门禁变慢。
const myIncrRows = 20_000

// 增量在流上追加多少行（够把中断点落在「流已经跑了一段」的窗口里）。
const myIncrExtra = 4_000

func TestMySQLSourceIncrementalResumeAfterSIGKILL(t *testing.T) {
	srcDSN, dstDSN := mysqlDSNs(t)
	my := mustMySQL(t, srcDSN)
	dst := mustConn(t, dstDSN)
	table := "engine_mysql_incr_kill"

	// ── 造数：源端存量 + 干净的目标端与状态表 ─────────────────────────
	myExec(t, my, "DROP TABLE IF EXISTS "+table)
	myExec(t, my, "CREATE TABLE "+table+` (
		id BIGINT NOT NULL PRIMARY KEY,
		payload VARCHAR(64) NOT NULL,
		amount DECIMAL(12,2) NULL,
		created_at DATETIME(6) NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	// 造数**不用 WITH RECURSIVE**：MySQL 5.7 没有 CTE（也没有 cte_max_recursion_depth
	// 这个变量），而 5.7 在矩阵里要真跑。VALUES 直接拼，5.7/8.0/8.4 都认。
	const step = 5000
	for lo := 1; lo <= myIncrRows; lo += step {
		hi := lo + step - 1
		myExec(t, my, "INSERT INTO "+table+" (id,payload,amount,created_at) VALUES "+mySeedValues(lo, hi))
	}
	defer func() {
		myExec(t, my, "DROP TABLE IF EXISTS "+table)
	}()
	exec(t, dst, "DROP TABLE IF EXISTS "+table)
	exec(t, dst, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")

	bin := buildBinary(t)
	dataDir := filepath.Join(t.TempDir(), "data")
	jobName := "mysql-incr-kill"
	jobYAML := filepath.Join(t.TempDir(), "job.yaml")
	if err := os.WriteFile(jobYAML, []byte(myYAMLFor(jobName, table)), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := execpkg.Command(bin, "import", "--file", jobYAML, "--data-dir", dataDir).
		CombinedOutput(); err != nil {
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

	// ── 第 1 轮：全量搬完（这一步不是要证明的东西，但它必须干净地完成）──
	if out, code := run("--stop-when-idle", "3s"); code != 0 {
		t.Fatalf("首轮全量失败（退出码 %d）：\n%s", code, out)
	}
	if got := pgCount(t, dst, table); got != myIncrRows {
		t.Fatalf("全量后目标端应有 %d 行，实际 %d", myIncrRows, got)
	}
	posFull := myPosition(t, dst)
	t.Logf("全量完成：%d 行，位点 %s", myIncrRows, posFull)

	// ── 第 2 轮：增量跑到一半 kill -9 ────────────────────────────────
	cmd := execpkg.Command(bin, "run", "--job", jobName, "--data-dir", dataDir)
	cmd.Env = os.Environ()
	logPath := filepath.Join(t.TempDir(), "run-incr.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动增量轮失败: %v", err)
	}

	// 一边流一边写：每批 200 行一个事务（引擎按事务提交位点）。
	var wrote int64
	stopWriter := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := 0; i < myIncrExtra/200; i++ {
			select {
			case <-stopWriter:
				return
			default:
			}
			lo := myIncrRows + i*200 + 1
			hi := lo + 199
			_, err := my.Exec("INSERT INTO " + table + ` (id,payload,amount,created_at)
				VALUES ` + myValues(lo, hi))
			if err != nil {
				return
			}
			atomic.AddInt64(&wrote, 200)
			time.Sleep(15 * time.Millisecond)
		}
	}()

	// 等「流已经搬了一段、还没追平」的窗口再杀：搬 0 行时杀证明不了位点续跑。
	deadline := time.Now().Add(60 * time.Second)
	var seen int
	for time.Now().Before(deadline) {
		seen = pgCount(t, dst, table)
		if seen >= myIncrRows+1200 && seen < myIncrRows+myIncrExtra {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if seen < myIncrRows+1200 {
		close(stopWriter)
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("增量阶段没有搬够行（目标端 %d/%d）：构造不出「跑到一半」的中断场景",
			seen, myIncrRows+myIncrExtra)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill -9 失败: %v", err)
	}
	_ = cmd.Wait()
	close(stopWriter)
	<-writerDone
	_ = logFile.Close()

	posKill := myPosition(t, dst)
	rowsKill := pgCount(t, dst, table)

	// 停机期间源端继续写：这一段只可能由「重启后从落库位点往前重放」补上，
	// 全量重搬补不了（全量的视图比它早）、跳过更补不了。这一段也让
	// 「杀了之后目标端确实落后」成为确定性事实，不靠掐时间。
	const downtime = 500
	myExec(t, my, "INSERT INTO "+table+" (id,payload,amount,created_at) VALUES "+
		myDownValues(myIncrRows+myIncrExtra+1, myIncrRows+myIncrExtra+downtime))
	myExec(t, my, "UPDATE "+table+" SET payload='touched' WHERE id BETWEEN 1 AND 100")
	myExec(t, my, "DELETE FROM "+table+" WHERE id BETWEEN 200 AND 249")

	srcKill := mySourceCount(t, my, table)
	t.Logf("kill -9：目标端 %d 行（源端此刻 %d 行 / 本轮写入 %d 行），中断时位点 %s；停机期间源端又写了 %d 行",
		rowsKill, srcKill, atomic.LoadInt64(&wrote), posKill, downtime)
	// 断言「kill 那一刻目标端确实落后」必须用**上界**比，不能用停机写完之后才数的
	// srcKill：它必然 ≥ kill 时刻的源端行数 ≥ rowsKill，那个比较恒假（Owl II P3）。
	// kill 时刻源端最多 myIncrRows+myIncrExtra 行（停机那 500 行是之后才写的），
	// 目标端到不了这个上界才说明流还在半路上——这条是可能红的。
	if rowsKill >= myIncrRows+myIncrExtra {
		t.Fatalf("目标端 %d 行已经追平源端上界 %d 行：这一轮没测到续跑",
			rowsKill, myIncrRows+myIncrExtra)
	}

	// ── 第 3 轮：重启，从落库的位点接着流 ────────────────────────────
	started := time.Now()
	out, code := run("--stop-when-idle", "5s")
	elapsed := time.Since(started)
	if code != 0 {
		t.Fatalf("重启续流失败（退出码 %d）：\n%s", code, out)
	}

	// 硬指标①：重搬行数是 0（不是「少搬」，是根本没搬）。
	if copied := rowsCopiedRe(t, out); copied != 0 {
		t.Fatalf("重启后重搬了 %d 行（应为 0）：这是整轮重搬，不是位点续跑\n%s", copied, out)
	}
	if strings.Contains(out, "全量完成") {
		t.Fatalf("重启后又跑了一遍全量：\n%s", out)
	}

	// 硬指标②：行数与内容一致（两端各自算指纹，跨引擎可比）。
	srcFP, srcN := myFingerprint(t, my, table)
	dstFP, dstN := pgFingerprint(t, dst, table)
	if srcN != dstN {
		t.Fatalf("追平后行数不一致：源 %d / 目标 %d", srcN, dstN)
	}
	if srcFP != dstFP {
		t.Fatalf("追平后内容不一致：源 %s / 目标 %s", srcFP, dstFP)
	}

	// 硬指标③：位点没有回退（本次中断点 → 追平点，只能往前）。
	//
	// 比较之前先确认三个位点**都解析得出 binlog 偏移**：解析不出时 myPosLess 恒为 false，
	// 这两条断言就成了空转（今晚实测踩到过——从别的任务的行里读到了 postgres 位点）。
	posAfter := myPosition(t, dst)
	for _, c := range []struct{ name, pos string }{
		{"全量后", posFull}, {"中断时", posKill}, {"追平后", posAfter},
	} {
		mustMySQLPos(t, c.name, c.pos)
	}
	if myPosLess(posAfter, posKill) {
		t.Fatalf("位点回退了：中断时 %s → 追平后 %s（增量会重复一整段）", posKill, posAfter)
	}
	if myPosLess(posAfter, posFull) {
		t.Fatalf("位点回到了全量之前的点：%s < %s", posAfter, posFull)
	}

	t.Logf("续跑：中断时 %d 行 → 重搬 0 行、用时 %s → 追平后 %d 行，md5 %s，位点 %s → %s",
		rowsKill, elapsed.Truncate(time.Millisecond), dstN, dstFP, posKill, posAfter)
}

// myYAMLFor 是「MySQL 源 → PG 目标」的任务定义。
func myYAMLFor(name, table string) string {
	return "version: 1\njob: " + name + "\n" +
		"source:\n  kind: mysql\n  dsn: env:JINLIANG_TEST_MYSQL_SRC_DSN\n" +
		"  scope:\n    schema: " + mySchemaOf(t0DSN("JINLIANG_TEST_MYSQL_SRC_DSN")) + "\n" +
		"  snapshot:\n    chunk_rows: 5000\n" +
		"target:\n  kind: postgres\n  dsn: env:JINLIANG_TEST_PG_DSN\n  schema: public\n  on_exists: append\n" +
		"content:\n  tables:\n    " + table + ": {}\n" +
		"mode:\n  frequency: continuous+initial\n"
}

// t0DSN 让 yaml 里的库名跟着 DSN 走（不写死 jinliang_src）。
func t0DSN(env string) string {
	v := os.Getenv(env)
	s := strings.TrimPrefix(v, "mysql://")
	if i := strings.IndexByte(s, '/'); i >= 0 {
		s = s[i+1:]
	}
	if i := strings.IndexByte(s, '?'); i >= 0 {
		s = s[:i]
	}
	return s
}

func mySchemaOf(db string) string {
	if db == "" {
		return "jinliang_src"
	}
	return db
}

// mySeedValues 拼存量行（id, 'row-id', 金额, 时间）。
//
// 为什么不在 SQL 里用 WITH RECURSIVE 生成：MySQL 5.7 不支持 CTE。矩阵要在 5.7 上跑这条
// 用例，造数就不能依赖 8.0 的语法（#89 合入时只在 8.0 的 job 跑过，5.7 腿一开就红）。
func mySeedValues(lo, hi int) string {
	var b strings.Builder
	for i := lo; i <= hi; i++ {
		if i > lo {
			b.WriteByte(',')
		}
		n := strconv.Itoa(i)
		b.WriteString("(" + n + ",'row-" + n + "',(" + n + " % 10000) / 100," +
			"TIMESTAMPADD(SECOND,-" + n + ",'2026-01-01 00:00:00'))")
	}
	return b.String()
}

// myDownValues 拼「停机期间源端写入」的行。
func myDownValues(lo, hi int) string {
	var b strings.Builder
	for i := lo; i <= hi; i++ {
		if i > lo {
			b.WriteByte(',')
		}
		n := strconv.Itoa(i)
		b.WriteString("(" + n + ",'down-" + n + "',2.5,NOW(6))")
	}
	return b.String()
}

// myValues 拼一批 VALUES（(lo,'row-lo',…), …）。
func myValues(lo, hi int) string {
	var b strings.Builder
	for i := lo; i <= hi; i++ {
		if i > lo {
			b.WriteByte(',')
		}
		b.WriteString("(" + strconv.Itoa(i) + ",'live-" + strconv.Itoa(i) + "',1.5,NOW(6))")
	}
	return b.String()
}

func mySourceCount(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT count(*) FROM " + table).Scan(&n); err != nil {
		t.Fatalf("数源端 %s 失败: %v", table, err)
	}
	return n
}

// myFingerprint / pgFingerprint 是跨引擎可比的指纹：行数 + 按主键排序逐行 md5 再 md5。
// 时间列两边都格式化到微秒（MySQL 的 %f 与 PG 的 US 都是 6 位）。
func myFingerprint(t *testing.T, db *sql.DB, table string) (string, int) {
	t.Helper()
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, "SET SESSION group_concat_max_len=1073741824"); err != nil {
		t.Fatal(err)
	}
	var fp string
	var n int
	if err := conn.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("数源端行失败: %v", err)
	}
	err = conn.QueryRowContext(ctx, `SELECT COALESCE(MD5(GROUP_CONCAT(h ORDER BY id SEPARATOR '')), '-')
		FROM (SELECT id, MD5(CONCAT_WS('|', id, payload, amount,
		      DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s.%f'))) h FROM `+table+`) x`).Scan(&fp)
	if err != nil {
		t.Fatalf("算源端指纹失败: %v", err)
	}
	return strconv.Itoa(n) + ":" + fp, n
}

func pgFingerprint(t *testing.T, conn *pgx.Conn, table string) (string, int) {
	t.Helper()
	var fp string
	var n int
	if err := conn.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("数目标端行失败: %v", err)
	}
	err := conn.QueryRow(context.Background(), `SELECT COALESCE(md5(string_agg(h, '' ORDER BY id)), '-')
		FROM (SELECT id, md5(concat_ws('|', id, payload, amount,
		      to_char(created_at, 'YYYY-MM-DD HH24:MI:SS.US'))) h FROM `+table+`) x`).Scan(&fp)
	if err != nil {
		t.Fatalf("算目标端指纹失败: %v", err)
	}
	return strconv.Itoa(n) + ":" + fp, n
}

// myPosition 读目标端落库的**源端位点**（_jl_meta.position 的唯一权威）。
//
// 两个坑，这行 SQL 都是为了绕开它们：
//  1. **不许 `LIMIT 1` 裸读**：同一套库上跑过别的任务时，_jl_meta 里会有别的行，
//     `LIMIT 1` 读到谁看运气。读到 PG 源的位点（`postgres:v1:0/…`）时，
//     myPosLess 解析不出 binlog 偏移 → 比较恒为 false → **位点断言变成空转**，
//     用例照样绿（今晚实测撞到过：全量后读到的就是别的任务的 postgres 位点）。
//  2. 所以：只认 `mysql:v1:` 前缀，且必须**正好一行**。0 行 = 没写位点（真失败），
//     多行 = 库没隔离干净（用例本身不可信）—— 两种都要当场红，不静默。
func myPosition(t *testing.T, conn *pgx.Conn) string {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		"SELECT COALESCE(position,'') FROM _jl_meta WHERE COALESCE(position,'') LIKE 'mysql:v1:%'")
	if err != nil {
		t.Fatalf("读位点失败: %v", err)
	}
	defer rows.Close()
	var pos string
	n := 0
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			t.Fatalf("读位点失败: %v", err)
		}
		pos, n = p, n+1
	}
	if n != 1 {
		var all string
		_ = conn.QueryRow(context.Background(),
			"SELECT COALESCE(string_agg(job_key||'→'||COALESCE(position,'∅'), ' | '),'') FROM _jl_meta").Scan(&all)
		t.Fatalf("目标端 _jl_meta 里应有且只有 1 行 MySQL 位点，实际 %d 行；库里的行：%s。"+
			"多行多半是这套库没隔离干净（别的任务的行还在）——位点断言会读错行、变成空转", n, all)
	}
	return pos
}

var (
	myPosRe = regexp.MustCompile(`pos=(\d+)`)
	// 「复制 N 行」是 CLI 的最后一行结果（与 PG 源那条用例同一个解析口径）。
	myCopiedRe = regexp.MustCompile(`复制 (\d+) 行`)
)

// mustMySQLPos 断言这个位点确实是 MySQL 的（带 file=/pos=），否则位点比较没有意义。
func mustMySQLPos(t *testing.T, label, pos string) {
	t.Helper()
	if !strings.HasPrefix(pos, "mysql:v1:") || myPosRe.FindStringSubmatch(pos) == nil {
		t.Fatalf("%s 的位点不是 MySQL 位点：%q。"+
			"位点比较按 binlog 偏移进行，拿别的位点比会恒为 false（断言空转）", label, pos)
	}
}

// myPosLess 比较两个 MySQL 位点的 binlog 偏移（同文件内可比；文件名变大也算前进）。
func myPosLess(a, b string) bool {
	fa, pa := mySplitPos(a)
	fb, pb := mySplitPos(b)
	if fa != fb {
		return fa < fb
	}
	return pa < pb
}

func mySplitPos(p string) (int64, int64) {
	var file, pos int64
	if m := regexp.MustCompile(`file=[^;]+\.(\d+)`).FindStringSubmatch(p); m != nil {
		file, _ = strconv.ParseInt(m[1], 10, 64)
	}
	if m := myPosRe.FindStringSubmatch(p); m != nil {
		pos, _ = strconv.ParseInt(m[1], 10, 64)
	}
	return file, pos
}

func rowsCopiedRe(t *testing.T, out string) int {
	t.Helper()
	m := myCopiedRe.FindStringSubmatch(out)
	if m == nil {
		t.Fatalf("输出里找不到「复制 N 行」：\n%s", out)
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		t.Fatal(err)
	}
	return n
}
