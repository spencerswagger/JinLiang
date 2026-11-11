//go:build integration

package engine_test

// MySQL **目标端**全量中途 kill -9 → 按块续跑（不是整轮重搬）。
//
// 口径与 PG 目标端那条（`resume_kill_test.go`）一致：
//   - 重启后的**重搬行数 ≤ 1 块**（`chunk_rows`），不是「少搬」而是「没重跑全量」；
//   - 最终行数与内容指纹与源端逐行一致；
//   - 位点不回退。
//
// 这一格是 Owl II 在 #92 门禁里点名的 killcopy：原来 `copyGeneric` 给
// `SnapshotChunksFrom` 传 `nil`、`needCopy` 只看「有没有位点」，于是 MySQL 目标端
// 中断一次只能整轮重搬（实测重搬 4000 行）。现在与 PG 路径共用 `planCopy`。
//
// 用子进程而不是进程内 cancel：kill -9 什么都来不及做（放租约、收尾、落位点都跳过了），
// 那才是用户遇到的情况。

import (
	"context"
	"fmt"
	"os"
	execpkg "os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

const myTargetCopyRows = 20_000
const myTargetChunkRows = 2000

func TestMySQLTargetCopyResumesFromChunkBoundary(t *testing.T) {
	srcDSN, dstDSN := mysqlTargetDSNs(t)
	srcDB := mustConn(t, srcDSN)
	myDB := mustMySQLTarget(t, dstDSN)
	ctx := context.Background()
	table := "engine_myt_copy_resume"

	for _, stmt := range []string{
		"DROP TABLE IF EXISTS " + table,
		"CREATE TABLE " + table + " (id bigint PRIMARY KEY, payload text, v bigint)",
		fmt.Sprintf("INSERT INTO %s SELECT g, 'r'||g, g FROM generate_series(1,%d) g", table, myTargetCopyRows),
	} {
		exec(t, srcDB, stmt)
	}
	_, _ = myDB.ExecContext(ctx, "DROP TABLE IF EXISTS `"+table+"`")
	dropMySQLState(t, myDB)
	t.Cleanup(func() {
		_, _ = myDB.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+table+"`")
	})

	bin := buildBinary(t)
	dataDir := filepath.Join(t.TempDir(), "data")
	jobName := "myt-copy-resume"
	jobYAML := filepath.Join(t.TempDir(), "job.yaml")
	if err := os.WriteFile(jobYAML, []byte(myTargetYAML(jobName, table)), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := execpkg.Command(bin, "import", "--file", jobYAML, "--data-dir", dataDir).
		CombinedOutput(); err != nil {
		t.Fatalf("import 失败: %v\n%s", err, out)
	}

	// ── 第 1 轮：跑到目标端有几千行时 kill -9 ───────────────────────
	cmd := execpkg.Command(bin, "run", "--job", jobName, "--data-dir", dataDir, "--snapshot-only")
	cmd.Env = os.Environ()
	logPath := filepath.Join(t.TempDir(), "copy.log")
	logFile, err := os.Create(logPath)
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = logFile, logFile
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动全量失败: %v", err)
	}
	deadline := time.Now().Add(90 * time.Second)
	var seen int
	for time.Now().Before(deadline) {
		var n int
		if err := myDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM `"+table+"`").Scan(&n); err == nil {
			seen = n
		}
		// 落在「搬了几块、还没搬完」的窗口里杀
		if seen >= 3*myTargetChunkRows && seen < myTargetCopyRows {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if seen < 3*myTargetChunkRows {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		_ = logFile.Close()
		t.Fatalf("全量没跑到「搬了几块」的窗口（目标端 %d 行）：构造不出中断场景", seen)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill -9 失败: %v", err)
	}
	_ = cmd.Wait()
	_ = logFile.Close()
	rowsKilled := myCount(t, myDB, table)

	// 块边界：中断时已提交到第几块、右边界在哪
	var lastNo int
	var lastTo string
	if err := myDB.QueryRowContext(ctx,
		"SELECT chunk_no, COALESCE(range_to,'') FROM `_jl_chunk` WHERE table_name = ? AND chunk_no > 0 ORDER BY chunk_no DESC LIMIT 1",
		table).Scan(&lastNo, &lastTo); err != nil {
		t.Fatalf("读中断时的块边界失败: %v", err)
	}

	// ── 第 2 轮：接着搬（不是整轮重搬） ─────────────────────────────
	out, code := runBin(t, bin, "run", "--job", jobName, "--data-dir", dataDir, "--snapshot-only")
	if code != 0 {
		t.Fatalf("续跑退出码 %d：\n%s", code, out)
	}
	// 判据（与门禁 ``b1_mysql_to_pg.sh`` 的 killcopy 同口径）：
	//   「重做」(中断前已写 + 重启又搬) 不能超过总行数 + 1 块 —— 超了 = 整轮重搬；
	//   也不能小于总行数 —— 少了 = 有段被跳过（不是续跑，是漏）。
	// 注意 `复制 N 行` 是**本轮**搬的行数：续跑本来就要搬剩下的那一大截（本例约 13000），
	// 所以「copied ≤ 1 块」这种断言在实现完全正确时也会红（Owl II 必改 4）。
	copied := rowsCopiedRe(t, out)
	if redone := rowsKilled + copied; redone > myTargetCopyRows+myTargetChunkRows {
		t.Fatalf("重做 %d 行（中断前 %d + 重启 %d）超过总行数 %d + 1 块 %d：这是整轮重搬\n%s",
			redone, rowsKilled, copied, myTargetCopyRows, myTargetChunkRows, out)
	} else if redone < myTargetCopyRows {
		t.Fatalf("重做 %d 行（中断前 %d + 重启 %d）少于总行数 %d：有段被跳过了（不是续跑，是漏）\n%s",
			redone, rowsKilled, copied, myTargetCopyRows, out)
	}
	if got := myCount(t, myDB, table); got != myTargetCopyRows {
		t.Fatalf("续跑后应有 %d 行，实际 %d", myTargetCopyRows, got)
	}
	if got, want := myTableFingerprint(t, myDB, table), pgTableFingerprint(t, table); got != want {
		t.Fatalf("两端指纹应一致：MySQL %s / PG %s", got, want)
	}
	t.Logf("MySQL 目标端中断续跑：kill 时目标端 %d 行（已提交到第 %d 块，右边界 %q），"+
		"重启搬了 %d 行 → 重做共 %d 行（总 %d，允许 ≤ 总+1 块 %d），最终 %d 行、两端指纹一致",
		rowsKilled, lastNo, lastTo, copied, rowsKilled+copied, myTargetCopyRows,
		myTargetCopyRows+myTargetChunkRows, myTargetCopyRows)
}

// runBin 跑一次 CLI 并把 (输出, 退出码) 交回来。
func runBin(t *testing.T, bin string, args ...string) (string, int) {
	t.Helper()
	cmd := execpkg.Command(bin, args...)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	code := 0
	if ee, ok := err.(*execpkg.ExitError); ok {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatalf("跑 %v 失败: %v\n%s", args, err, out)
	}
	return string(out), code
}

// myTargetYAML 是「PG 源 → MySQL 目标」的一次性全量任务（块小，便于观察续跑粒度）。
func myTargetYAML(name, table string) string {
	my := strings.TrimPrefix(os.Getenv("JINLIANG_TEST_MYSQL_DSN"), "mysql://")
	db := my
	if i := strings.IndexByte(db, '/'); i >= 0 {
		db = db[i+1:]
	}
	if i := strings.IndexByte(db, '?'); i >= 0 {
		db = db[:i]
	}
	return "version: 1\njob: " + name + "\n" +
		"source:\n  kind: postgres\n  dsn: env:JINLIANG_TEST_PG_SRC_DSN\n  scope:\n    schema: public\n" +
		"  snapshot:\n    chunk_rows: " + strconv.Itoa(myTargetChunkRows) + "\n" +
		"target:\n  kind: mysql\n  dsn: env:JINLIANG_TEST_MYSQL_DSN\n  on_exists: append\n" +
		"content:\n  tables:\n    " + table + ": {}\n" +
		"mode:\n  frequency: once\n"
}
