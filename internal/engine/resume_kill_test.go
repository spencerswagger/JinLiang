//go:build integration

package engine_test

// 块级续跑的真库验收（docs/SNAPSHOT.md §4）。
//
// 为什么用**子进程**而不是进程内 cancel：进程内 cancel 是「优雅停止」，会走 teardown
// （放租约、收尾）；kill -9 是「什么都没来得及做」——那才是用户遇到的形状。
// 两种都要能续，所以两种都测，用的是同一个断言集。
//
// 缺 DSN 直接失败（不跳过）：静默通过是这个仓库最不能犯的错。

import (
	"bytes"
	"context"
	"errors"
	"os"
	execpkg "os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const resumeTotal = 200_000

// 断言集：中断后重启**只搬剩下的**，且结果与一次性全量逐表一致。
func TestChunkResumeAfterSIGKILL(t *testing.T) {
	f := newResumeFixture(t, "resume_kill")
	posBefore := f.killDuringCopy(t)
	f.assertResume(t, posBefore)
}

func TestChunkResumeAfterGracefulCancel(t *testing.T) {
	f := newResumeFixture(t, "resume_cancel")
	posBefore := f.cancelDuringCopy(t)
	f.assertResume(t, posBefore)
}

// 这条盯的是**续跑读的是更晚的视图**这件事是否安全：
// 中断期间源端被改过（已搬区间里的行被删掉又插回、被更新；未搬区间里的行被删掉），
// 续跑搬完 + 增量追平之后，目标端必须与源端逐行一致。
//
// 为什么它能成立（也是设计前提）：续跑不许把位点往前挪，增量仍然从**中断前那个位点**
// 重放，于是「已搬区间里的变更」会被重放一遍盖掉/删掉；「未搬区间里的变更」由续跑
// 的新视图读到，重复的那部分靠 upsert 收敛。位点一旦被挪到续跑时刻，已搬区间里
// 新增的行就再也没人搬——这条用例就是拦这个的。
func TestChunkResumeReconcilesChangesDuringDowntime(t *testing.T) {
	f := newResumeFixture(t, "resume_gap")
	f.killDuringCopy(t)

	ctx := context.Background()
	// 已搬区间（中断时至少搬了一半）：删掉再插回 + 改内容。这些行续跑不会重读。
	exec(t, f.srcDB, "DELETE FROM "+f.table+" WHERE id BETWEEN 100 AND 199")
	exec(t, f.srcDB, "INSERT INTO "+f.table+" SELECT g, 'reborn-'||g FROM generate_series(100,199) g")
	exec(t, f.srcDB, "UPDATE "+f.table+" SET payload='upd-'||id WHERE id BETWEEN 5000 AND 5199")
	// 未搬区间：删掉一批（续跑读的是后来的视图，本来就不会读到）。
	exec(t, f.srcDB, "DELETE FROM "+f.table+" WHERE id BETWEEN "+strconv.Itoa(resumeTotal-500)+
		" AND "+strconv.Itoa(resumeTotal-401))
	// 表尾新增一批（续跑会读到，增量也会再给一次 → 只能收敛成一行，不能重复）。
	exec(t, f.srcDB, "INSERT INTO "+f.table+" SELECT g, 'tail-'||g FROM generate_series("+
		strconv.Itoa(resumeTotal+1)+","+strconv.Itoa(resumeTotal+500)+") g")

	// 续跑 + 追平（不带 --snapshot-only）：搬到表尾之后从原位点重放中断期间的变更。
	out, code := f.run(t, "--stop-when-idle", "3s")
	if code != 0 {
		t.Fatalf("续跑+追平失败（退出码 %d）：\n%s", code, out)
	}

	var srcN, dstN int
	if err := f.srcDB.QueryRow(ctx, "SELECT count(*) FROM "+f.table).Scan(&srcN); err != nil {
		t.Fatal(err)
	}
	if err := f.dstDB.QueryRow(ctx, "SELECT count(*) FROM "+f.table).Scan(&dstN); err != nil {
		t.Fatal(err)
	}
	if srcN != dstN {
		t.Fatalf("追平后行数不一致：源 %d / 目标 %d（多出来的是重复行）", srcN, dstN)
	}
	if s, d := f.md5(t, f.srcDB, f.table), f.md5(t, f.dstDB, f.table); s != d {
		t.Fatalf("追平后内容不一致：源 %s / 目标 %s；差异 %s\n%s", s, d, f.diff(t), out)
	}
	t.Logf("中断期间源端改动（删了再插回 100 行 / 改 200 行 / 删 100 行 / 表尾新增 500 行）"+
		"在续跑+追平后两端一致：%d 行，md5 %s", dstN, f.md5(t, f.dstDB, f.table))
}

type resumeFixture struct {
	bin      string
	dataDir  string
	jobName  string
	table    string
	srcDB    *pgx.Conn
	dstDB    *pgx.Conn
	dstDSN   string
	baseline string // 一次性全量的内容 md5（基准）
	// interrupted 是中断那一刻的事实（块号/行数/位点）。
	interrupted interruption
}

// newResumeFixture 造数据，并先跑一次**不中断**的全量当基准（逐表 md5 的对照）。
func newResumeFixture(t *testing.T, name string) *resumeFixture {
	t.Helper()
	srcDSN, dstDSN := dsns(t)
	f := &resumeFixture{
		bin:     buildBinary(t),
		dataDir: filepath.Join(t.TempDir(), "data"),
		jobName: strings.ReplaceAll(name, "_", "-"),
		table:   name,
		srcDB:   mustConn(t, srcDSN),
		dstDB:   mustConn(t, dstDSN),
		dstDSN:  dstDSN,
	}

	ctx := context.Background()
	exec(t, f.srcDB, "DROP TABLE IF EXISTS "+f.table)
	exec(t, f.srcDB, "CREATE TABLE "+f.table+" (id bigint PRIMARY KEY, payload text)")
	exec(t, f.srcDB, "INSERT INTO "+f.table+" SELECT g, 'r'||g FROM generate_series(1,"+
		strconv.Itoa(resumeTotal)+") g")
	f.resetTarget(t)
	f.importJob(t)

	// 基准：同一条源数据、同一个目标表，一次性搬完。
	out, code := f.run(t, "--snapshot-only")
	if code != 0 {
		t.Fatalf("基准全量失败（退出码 %d）：\n%s", code, out)
	}
	if got := copiedRows(t, out); got != resumeTotal {
		t.Fatalf("基准全量应搬 %d 行，实际 %d", resumeTotal, got)
	}
	var n int
	if err := f.dstDB.QueryRow(ctx, "SELECT count(*) FROM "+f.table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != resumeTotal {
		t.Fatalf("基准全量后目标端应有 %d 行，实际 %d", resumeTotal, n)
	}
	f.baseline = f.md5(t, f.dstDB, f.table)
	if src := f.md5(t, f.srcDB, f.table); src != f.baseline {
		t.Fatalf("基准全量本身就不一致：源 %s / 目标 %s", src, f.baseline)
	}

	// 回到「从没跑过」：清目标表、清状态表；书签由下一次 Prepare 自己重建。
	f.resetTarget(t)
	t.Logf("基准：%d 行，md5 %s", resumeTotal, f.baseline)
	return f
}

func (f *resumeFixture) resetTarget(t *testing.T) {
	t.Helper()
	exec(t, f.dstDB, "DROP TABLE IF EXISTS "+f.table)
	exec(t, f.dstDB, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")
}

// importJob 把任务写进控制库（一条 yaml 一份任务）。
func (f *resumeFixture) importJob(t *testing.T) {
	t.Helper()
	jobYAML := filepath.Join(filepath.Dir(f.dataDir), "job.yaml")
	if err := os.WriteFile(jobYAML, []byte(yamlFor(f.jobName, f.table)), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, err := execpkg.Command(f.bin, "import", "--file", jobYAML, "--data-dir", f.dataDir).
		CombinedOutput(); err != nil {
		t.Fatalf("import 失败: %v\n%s", err, out)
	}
}

// startCopy 起一个只搬存量的 run，并在「已经落了一部分、但还没搬完」时返回。
func (f *resumeFixture) startCopy(t *testing.T) *execpkg.Cmd {
	t.Helper()
	cmd := execpkg.Command(f.bin, "run", "--job", f.jobName, "--data-dir", f.dataDir, "--snapshot-only")
	cmd.Env = os.Environ()
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动 run 失败: %v", err)
	}
	f.waitPartial(t)
	return cmd
}

// waitPartial 等到「目标端已经有行、但还没搬完」，这个窗口就是中断点。
func (f *resumeFixture) waitPartial(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(90 * time.Second)
	var n int
	for time.Now().Before(deadline) {
		// 等到「搬了一半以上、还没搬完」再动手：杀在第 1 块上虽然也算中断，
		// 但证明不了「已提交的块不重搬」——那才是这个用例要盯的东西。
		if err := f.dstDB.QueryRow(ctx, "SELECT count(*) FROM "+f.table).Scan(&n); err == nil &&
			n >= resumeTotal/2 && n < resumeTotal {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("等不到「搬到一半」的窗口（目标端 %d/%d 行）：没法构造中断场景", n, resumeTotal)
}

// killDuringCopy 用 SIGKILL 中断（进程没有机会 teardown），返回中断时的位点。
func (f *resumeFixture) killDuringCopy(t *testing.T) string {
	t.Helper()
	cmd := f.startCopy(t)
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill -9 失败: %v", err)
	}
	_ = cmd.Wait() // 被强杀的进程必然是 error，收尸就行
	return f.snapshotOfInterruption(t)
}

// cancelDuringCopy 用 SIGINT 中断（正常取消：会走收尾、放租约），返回中断时的位点。
func (f *resumeFixture) cancelDuringCopy(t *testing.T) string {
	t.Helper()
	cmd := f.startCopy(t)
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatalf("发 SIGINT 失败: %v", err)
	}
	err := cmd.Wait()
	var ee *execpkg.ExitError
	if err == nil || !errors.As(err, &ee) || ee.ExitCode() != 130 {
		t.Fatalf("正常取消应以 130 收尾（0 完成 / 1 失败 / 130 手动停止），得到: %v", err)
	}
	return f.snapshotOfInterruption(t)
}

// interruption 是「中断那一刻目标端的事实」：搬到哪一块、多少行、位点是什么。
type interruption struct {
	position string
	rows     int
	chunks   int
	maxChunk int
}

func (f *resumeFixture) snapshotOfInterruption(t *testing.T) string {
	t.Helper()
	in := f.readState(t)
	if in.rows == 0 {
		t.Fatal("中断时目标端一行都没落：构造不出「未完成」场景")
	}
	if in.rows >= resumeTotal {
		t.Fatalf("中断时已经搬完（%d 行）：把数据放大或块调小", in.rows)
	}
	if in.chunks == 0 || in.maxChunk == 0 {
		t.Fatalf("中断时没有块记录（chunks=%d maxNo=%d）：续跑就无从判定", in.chunks, in.maxChunk)
	}
	t.Logf("中断点：目标端 %d/%d 行，已提交 %d 块（最大块号 %d），位点 %s",
		in.rows, resumeTotal, in.chunks, in.maxChunk, in.position)
	f.interrupted = in
	return in.position
}

func (f *resumeFixture) readState(t *testing.T) interruption {
	t.Helper()
	ctx := context.Background()
	var in interruption
	if err := f.dstDB.QueryRow(ctx,
		"SELECT COALESCE(position,'') FROM _jl_meta LIMIT 1").Scan(&in.position); err != nil {
		t.Fatalf("读 _jl_meta 失败: %v", err)
	}
	var jobKey string
	if err := f.dstDB.QueryRow(ctx, "SELECT job_key FROM _jl_meta LIMIT 1").Scan(&jobKey); err != nil {
		t.Fatal(err)
	}
	if err := f.dstDB.QueryRow(ctx,
		`SELECT COALESCE(count(*),0), COALESCE(max(chunk_no),0) FROM _jl_chunk
		 WHERE job_key=$1 AND chunk_no > 0 AND done`, jobKey).Scan(&in.chunks, &in.maxChunk); err != nil {
		t.Fatal(err)
	}
	if err := f.dstDB.QueryRow(ctx, "SELECT count(*) FROM "+f.table).Scan(&in.rows); err != nil {
		t.Fatal(err)
	}
	return in
}

// assertResume 重启一次，断言「只搬剩下的」且结果与一次性全量逐表一致。
func (f *resumeFixture) assertResume(t *testing.T, posBefore string) {
	t.Helper()
	before := f.interrupted

	out, code := f.run(t, "--snapshot-only")
	if code != 0 {
		t.Fatalf("续跑失败（退出码 %d）：\n%s", code, out)
	}
	copied := copiedRows(t, out)
	elapsed := elapsedOf(t, out)

	if copied >= resumeTotal {
		t.Fatalf("重启后搬了 %d 行（总 %d）：这是整轮重搬，不是块级续跑\n%s", copied, resumeTotal, out)
	}
	// 中断时正在写的那一块会整个回滚（块 = 一个事务），所以最少要重搬它的行数；
	// 上限按「没搬完的部分 + 一块」卡，超过就说明跳过的块又搬了一遍。
	remaining := resumeTotal - before.rows
	maxChunkRows := 1000 // yamlFor 里的 snapshot.chunk_rows
	if copied < remaining || copied > remaining+maxChunkRows {
		t.Fatalf("续跑搬了 %d 行，应该是未完成部分 %d 行（±1 块 %d）\n%s",
			copied, remaining, maxChunkRows, out)
	}

	after := f.readState(t)
	if after.position != posBefore {
		t.Fatalf("续跑把位点挪了：中断时 %s，续跑后 %s（增量会因此漏掉一段变更）",
			posBefore, after.position)
	}
	if after.maxChunk > 0 && before.maxChunk > 0 && after.maxChunk <= before.maxChunk {
		t.Fatalf("续跑的块号没有接着往上排：中断时最大 %d，续跑后最大 %d", before.maxChunk, after.maxChunk)
	}

	var n int
	if err := f.dstDB.QueryRow(context.Background(), "SELECT count(*) FROM "+f.table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != resumeTotal {
		t.Fatalf("续跑后应有 %d 行，得到 %d", resumeTotal, n)
	}
	// 行数相同还不够：主键重复也会让 count 变多，所以必须比内容。
	if got, src := f.md5(t, f.dstDB, f.table), f.md5(t, f.srcDB, f.table); got != src {
		t.Fatalf("续跑后两端内容不一致：源 %s / 目标 %s", src, got)
	}
	if got := f.md5(t, f.dstDB, f.table); got != f.baseline {
		t.Fatalf("续跑结果与一次性全量不一致：基准 %s / 续跑 %s", f.baseline, got)
	}

	t.Logf("续跑：中断时 %d 行 / %d 块（最大块号 %d）→ 续跑搬 %d 行、用时 %s；"+
		"最终 %d 行，md5 %s == 基准，位点未挪（%s）",
		before.rows, before.chunks, before.maxChunk, copied, elapsed, n, f.md5(t, f.dstDB, f.table), after.position)
}

// 书签被清掉（max_slot_wal_keep_size 把它的 WAL 丢干净、或被人 drop）→ 重启必须**整轮重搬**。
//
// 这条盯的是「新位点 + 上一轮的块标记」那个中间态：从头搬的时候，位点必须在
// **块标记清掉的同一个事务里**才前进。为了稳定地停在窗口中间，这里不靠 sleep 掐时间：
// 测试自己开一个读事务压住目标表的 ACCESS SHARE 锁，TRUNCATE 会在锁上等——等到
// 「TRUNCATE 正在等锁」这一刻再 kill -9，窗口就是确定命中的。
//
// 改之前（位点在 prepare 就推了、标记要到 snapshot 才清）这里的位点会变成新视图的位点，
// 于是断言 `位点没被提前推` 立刻红；接着的重启会按「续跑」从旧游标搬，已搬区间里
// 停机期间改过的行永远不会被重放 —— 内容 md5 那条也跟着红。
func TestSlotLossForcesFullRecopyNotResume(t *testing.T) {
	f := newResumeFixture(t, "resume_slotlost")
	f.killDuringCopy(t)
	before := f.interrupted

	ctx := context.Background()
	jobKey := f.jobKey(t)

	// ① 模拟书签被清：max_slot_wal_keep_size 超额时 PG 会把书签的 WAL 丢掉（wal_status=lost），
	//    这里直接 drop——对引擎来说等价：从任何旧位点都重放不了。
	exec(t, f.srcDB, "SELECT pg_drop_replication_slot('"+slotNameFor(jobKey)+"')")

	// ② 停机期间改「已搬区间」里的行：整轮重搬会读到新值；错判成续跑就再也读不到。
	exec(t, f.srcDB, "UPDATE "+f.table+" SET payload='post-slot-loss-'||id WHERE id <= 1000")
	// 表尾新增：不管怎么搬都该在。
	exec(t, f.srcDB, "INSERT INTO "+f.table+" SELECT g, 'tail-'||g FROM generate_series("+
		strconv.Itoa(resumeTotal+1)+","+strconv.Itoa(resumeTotal+200)+") g")

	// ③ 压住目标表的锁，让第二轮全量卡在 TRUNCATE（也就是「标记还没清、位点已经推了」的窗口）。
	// 必须用**另一条连接**持有这个事务：f.dstDB 还要用来查 pg_stat_activity 和进度。
	lockConn, err := pgx.Connect(ctx, f.dstDSN)
	if err != nil {
		t.Fatalf("开锁会话失败: %v", err)
	}
	defer lockConn.Close(context.Background())
	lockTx, err := lockConn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var held int
	if err := lockTx.QueryRow(ctx, "SELECT count(*) FROM "+f.table).Scan(&held); err != nil {
		t.Fatal(err)
	}

	var runOut bytes.Buffer
	cmd := execpkg.Command(f.bin, "run", "--job", f.jobName, "--data-dir", f.dataDir, "--snapshot-only")
	cmd.Env = os.Environ()
	cmd.Stdout, cmd.Stderr = &runOut, &runOut
	if err := cmd.Start(); err != nil {
		t.Fatalf("启动第二轮全量失败: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	f.waitTruncateBlocked(t, done, &runOut)
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill -9 失败: %v", err)
	}
	_ = cmd.Wait()
	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	// ④ 位点不许被提前推：此刻块标记还在（这一轮一块都没搬），位点必须仍是中断前那个。
	//    这就是「清标记与推位点同事务」的可观测判据。
	afterCrash := f.readState(t)
	if afterCrash.position != before.position {
		t.Fatalf("从头搬的窗口里位点被提前推了：中断前 %s，崩后 %s（块标记还在，下次会被判成续跑）",
			before.position, afterCrash.position)
	}

	// ⑤ 再跑一次：必须整轮重搬（不是续跑），且内容与源端一致（含停机期间的修改）。
	out, code := f.run(t, "--snapshot-only")
	if code != 0 {
		t.Fatalf("整轮重搬失败（退出码 %d）：\n%s", code, out)
	}
	if copied := copiedRows(t, out); copied != resumeTotal+200 {
		t.Fatalf("书签被清后应整轮重搬 %d 行（源端当前行数），实际搬了 %d 行\n%s",
			resumeTotal+200, copied, out)
	}
	var srcN, dstN int
	if err := f.srcDB.QueryRow(ctx, "SELECT count(*) FROM "+f.table).Scan(&srcN); err != nil {
		t.Fatal(err)
	}
	if err := f.dstDB.QueryRow(ctx, "SELECT count(*) FROM "+f.table).Scan(&dstN); err != nil {
		t.Fatal(err)
	}
	if srcN != dstN {
		t.Fatalf("整轮重搬后行数不一致：源 %d / 目标 %d", srcN, dstN)
	}
	if s, d := f.md5(t, f.srcDB, f.table), f.md5(t, f.dstDB, f.table); s != d {
		t.Fatalf("整轮重搬后内容不一致：源 %s / 目标 %s；差异 %s", s, d, f.diff(t))
	}
	t.Logf("书签被清 → 整轮重搬：崩溃窗口内位点保持 %s；重搬 %d 行，%d 行，md5 %s == 源端",
		afterCrash.position, resumeTotal+200, dstN, f.md5(t, f.dstDB, f.table))
}

// 书签被 max_slot_wal_keep_size 打成 wal_status='lost'（书签**还在**、WAL 没了）时，
// 重跑一次必须能自愈：整轮重搬成功，且书签不堆积。
//
// 这和「书签被 drop」是两个不同状态，判据也不同：
//   - 「存在」决定建/删书签（lost 的书签还占着名字，不先 drop 就报 already exists，
//     于是每次 run 都失败、重跑也治不好）；
//   - 「能重放」才决定能不能续跑。
//
// 真 lost 是造出来的，不是假设出来的：把 max_slot_wal_keep_size 调到最小 → reload →
// 灌 WAL → CHECKPOINT → **轮询**到 wal_status 真的变成 lost（轮不到就失败，不靠 sleep）。
func TestLostSlotSelfHealsByFullRecopy(t *testing.T) {
	f := newResumeFixture(t, "resume_lost")
	f.killDuringCopy(t)

	ctx := context.Background()
	slot := slotNameFor(f.jobKey(t))

	f.requireSuperuser(t)
	restore := f.setMaxSlotWALKeepSize(t, "64kB")
	defer restore()

	// 灌 WAL 并让检查点把超额的 WAL 回收掉 —— 书签的 restart 点被甩在后面就成了 lost。
	for round := 0; round < 40; round++ {
		exec(t, f.srcDB, "INSERT INTO "+f.table+" SELECT g, repeat('w',2000) FROM generate_series("+
			strconv.Itoa(resumeTotal+1000+round*200)+","+strconv.Itoa(resumeTotal+1199+round*200)+") g")
		exec(t, f.srcDB, "SELECT pg_switch_wal()")
		exec(t, f.srcDB, "CHECKPOINT")
		if f.walStatus(t, slot) == "lost" {
			break
		}
	}
	if st := f.walStatus(t, slot); st != "lost" {
		t.Fatalf("造不出 wal_status=lost（当前 %q）：这个用例要的是「书签在、WAL 没了」", st)
	}
	t.Logf("书签 %s 已变成 wal_status=lost（书签还在，WAL 被丢掉）", slot)
	// 先把集群参数恢复回去：后面还要跑一次正常全量，别让它继续把 WAL 丢掉。
	restore()

	// 重跑一次：必须能自愈（drop lost 书签 → 重建 → 整轮重搬），而不是每次报 already exists。
	out, code := f.run(t, "--snapshot-only")
	if code != 0 {
		t.Fatalf("lost 书签后重跑必须自愈，实际退出码 %d：\n%s", code, out)
	}

	var srcN, dstN int
	if err := f.srcDB.QueryRow(ctx, "SELECT count(*) FROM "+f.table).Scan(&srcN); err != nil {
		t.Fatal(err)
	}
	if err := f.dstDB.QueryRow(ctx, "SELECT count(*) FROM "+f.table).Scan(&dstN); err != nil {
		t.Fatal(err)
	}
	if copied := copiedRows(t, out); copied != srcN {
		t.Fatalf("lost 之后应整轮重搬 %d 行（源端当前行数），实际 %d 行\n%s", srcN, copied, out)
	}
	if srcN != dstN {
		t.Fatalf("整轮重搬后行数不一致：源 %d / 目标 %d", srcN, dstN)
	}
	if s, d := f.md5(t, f.srcDB, f.table), f.md5(t, f.dstDB, f.table); s != d {
		t.Fatalf("整轮重搬后内容不一致：源 %s / 目标 %s；差异 %s", s, d, f.diff(t))
	}

	// 不许留孤儿：这个任务名下只能有 1 个书签。
	var slots int
	if err := f.srcDB.QueryRow(ctx,
		"SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE $1", "jl_"+f.jobKey(t)+"%").
		Scan(&slots); err != nil {
		t.Fatal(err)
	}
	if slots != 1 {
		var names []string
		rows, _ := f.srcDB.Query(ctx, "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE $1",
			"jl_"+f.jobKey(t)+"%")
		if rows != nil {
			defer rows.Close()
			for rows.Next() {
				var n string
				_ = rows.Scan(&n)
				names = append(names, n)
			}
		}
		t.Fatalf("重跑后这个任务名下应剩 1 个书签，实际 %d 个：%v", slots, names)
	}
	t.Logf("lost → 整轮重搬：%d 行（源端 %d 行），md5 %s == 源端；书签数 %d",
		dstN, srcN, f.md5(t, f.dstDB, f.table), slots)
}

// requireSuperuser：造 lost 要改 max_slot_wal_keep_size（superuser 才能改）。
// 改不了就直说失败，别静默跳过——跳过等于这条用例从来没跑过。
func (f *resumeFixture) requireSuperuser(t *testing.T) {
	t.Helper()
	var super bool
	if err := f.srcDB.QueryRow(context.Background(),
		"SELECT rolsuper FROM pg_roles WHERE rolname = current_user").Scan(&super); err != nil {
		t.Fatalf("查不到当前账号是不是 superuser: %v", err)
	}
	if !super {
		t.Fatalf("这个用例要制造 wal_status=lost，需要 superuser 改 max_slot_wal_keep_size；" +
			"当前账号不是 superuser —— 换一个能改的账号跑，别把它改成 skip")
	}
}

// setMaxSlotWALKeepSize 改集群参数并 reload，返回恢复函数（cleanup 里必须调）。
func (f *resumeFixture) setMaxSlotWALKeepSize(t *testing.T, v string) func() {
	t.Helper()
	ctx := context.Background()
	exec(t, f.srcDB, "ALTER SYSTEM SET max_slot_wal_keep_size='"+v+"'")
	exec(t, f.srcDB, "SELECT pg_reload_conf()")
	// 先把参数归到已知状态（-1 = 不限），再设成很小的值 —— 这样断言不依赖上一次跑完留了什么。
	showParam := func() string {
		var cur string
		if err := f.srcDB.QueryRow(ctx, "SHOW max_slot_wal_keep_size").Scan(&cur); err != nil {
			return ""
		}
		return cur
	}
	_, _ = f.srcDB.Exec(ctx, "ALTER SYSTEM RESET max_slot_wal_keep_size")
	_, _ = f.srcDB.Exec(ctx, "SELECT pg_reload_conf()")
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && showParam() != "-1" {
		time.Sleep(50 * time.Millisecond)
	}
	// 这个参数的单位是 MB：写 64kB 会被取整成 0（= 一点 WAL 都不留，正好用来造 lost）。
	exec(t, f.srcDB, "ALTER SYSTEM SET max_slot_wal_keep_size='"+v+"'")
	exec(t, f.srcDB, "SELECT pg_reload_conf()")
	deadline = time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && showParam() == "-1" {
		time.Sleep(50 * time.Millisecond)
	}
	if cur := showParam(); cur == "-1" || cur == "" {
		t.Fatalf("max_slot_wal_keep_size 没生效（设了 %s，实际 %q）", v, cur)
	} else {
		t.Logf("max_slot_wal_keep_size = %s（PG 按 MB 取整，0 表示一点 WAL 都不留）", cur)
	}
	return func() {
		// 恢复成默认值并 reload：这个参数是集群级的，留着会污染后面的用例。
		_, _ = f.srcDB.Exec(context.Background(), "ALTER SYSTEM RESET max_slot_wal_keep_size")
		_, _ = f.srcDB.Exec(context.Background(), "SELECT pg_reload_conf()")
	}
}

// walStatus 读一个书签的 wal_status（书签不存在时返回空串）。
func (f *resumeFixture) walStatus(t *testing.T, slot string) string {
	t.Helper()
	var st string
	err := f.srcDB.QueryRow(context.Background(),
		"SELECT COALESCE(wal_status,'') FROM pg_replication_slots WHERE slot_name=$1", slot).Scan(&st)
	if err != nil {
		return ""
	}
	return st
}

// waitTruncateBlocked 等到「有 TRUNCATE 卡在锁上」——那就是新一轮全量的窗口。
// 进程提前结束就直接报错（否则「等不到」会被误读成「窗口没构造出来」）。
func (f *resumeFixture) waitTruncateBlocked(t *testing.T, done <-chan error, out *bytes.Buffer) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			t.Fatalf("第二轮全量在 TRUNCATE 之前就结束了（%v）：\n%s", err, out.String())
		default:
		}
		var n int
		err := f.dstDB.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
			WHERE datname = current_database() AND state = 'active'
			  AND wait_event_type = 'Lock' AND query ILIKE '%truncate%'`).Scan(&n)
		if err == nil && n > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("60 秒内没有看到 TRUNCATE 卡在锁上：\n%s\n--- pg_stat_activity ---\n%s",
		out.String(), f.sessions(t))
}

// jobKey 读目标端状态行里的 job_key（书签名是 jl_<job_key>_1）。
func (f *resumeFixture) jobKey(t *testing.T) string {
	t.Helper()
	var k string
	if err := f.dstDB.QueryRow(context.Background(),
		"SELECT job_key FROM _jl_meta LIMIT 1").Scan(&k); err != nil {
		t.Fatalf("读 job_key 失败: %v", err)
	}
	return k
}

func slotNameFor(jobKey string) string { return "jl_" + jobKey + "_1" }

// sessions 列出目标库里所有会话在干什么（卡住时用来定位到底等在哪一句）。
func (f *resumeFixture) sessions(t *testing.T) string {
	t.Helper()
	rows, err := f.dstDB.Query(context.Background(), `SELECT state, COALESCE(wait_event_type,'')||'/'||COALESCE(wait_event,''),
		left(replace(query, E'\n',' '), 70) FROM pg_stat_activity WHERE datname = current_database()`)
	if err != nil {
		return "读 pg_stat_activity 失败: " + err.Error()
	}
	defer rows.Close()
	out := ""
	for rows.Next() {
		var st, ev, q string
		if err := rows.Scan(&st, &ev, &q); err != nil {
			return "扫描失败: " + err.Error()
		}
		out += st + " " + ev + " | " + q + "\n"
	}
	return out
}

// interrupted 让 assertResume 知道中断点；只在这条用例的生命周期里读。
func (f *resumeFixture) run(t *testing.T, extra ...string) (string, int) {
	t.Helper()
	args := append([]string{"run", "--job", f.jobName, "--data-dir", f.dataDir}, extra...)
	cmd := execpkg.Command(f.bin, args...)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	code := 0
	var ee *execpkg.ExitError
	if errors.As(err, &ee) {
		code = ee.ExitCode()
	} else if err != nil {
		t.Fatalf("执行失败: %v\n%s", err, out)
	}
	return string(out), code
}

var (
	copiedRe  = regexp.MustCompile(`复制 (\d+) 行`)
	elapsedRe = regexp.MustCompile(`用时 ([0-9.]+m?s)`)
)

func copiedRows(t *testing.T, out string) int {
	t.Helper()
	m := copiedRe.FindStringSubmatch(out)
	if m == nil {
		t.Fatalf("输出里找不到「复制 N 行」：\n%s", out)
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func elapsedOf(t *testing.T, out string) string {
	t.Helper()
	m := elapsedRe.FindStringSubmatch(out)
	if m == nil {
		t.Fatalf("输出里找不到「用时」：\n%s", out)
	}
	return m[1]
}

// md5 算一张表的内容哈希：两端同一条 SQL，逐行按主键排序后拼接。
// 用它而不是 count：主键重复、内容错位都藏不住。
func (f *resumeFixture) md5(t *testing.T, conn *pgx.Conn, table string) string {
	t.Helper()
	var v string
	q := "SELECT md5(string_agg(id::text||'|'||payload, E'\\n' ORDER BY id)) FROM " + table
	if err := conn.QueryRow(context.Background(), q).Scan(&v); err != nil {
		t.Fatalf("md5 失败: %v", err)
	}
	return v
}

// diff 说清差在哪：少行、多行、内容不同各有多少条（只采样前几个 id，便于定位）。
func (f *resumeFixture) diff(t *testing.T) string {
	t.Helper()
	srcRows := f.rowMap(t, f.srcDB)
	dstRows := f.rowMap(t, f.dstDB)
	var idsMissing, idsExtra, idsChanged []int64
	for id, v := range srcRows {
		if dv, ok := dstRows[id]; !ok {
			idsMissing = append(idsMissing, id)
		} else if dv != v {
			idsChanged = append(idsChanged, id)
		}
	}
	for id := range dstRows {
		if _, ok := srcRows[id]; !ok {
			idsExtra = append(idsExtra, id)
		}
	}
	sample := func(xs []int64) string {
		const n = 8
		out := ""
		for i, v := range xs {
			if i >= n {
				out += "…"
				break
			}
			out += strconv.FormatInt(v, 10) + " "
		}
		return out
	}
	return "少 " + strconv.Itoa(len(idsMissing)) + " 行 [" + sample(idsMissing) + "]；多 " +
		strconv.Itoa(len(idsExtra)) + " 行 [" + sample(idsExtra) + "]；内容不同 " +
		strconv.Itoa(len(idsChanged)) + " 行 [" + sample(idsChanged) + "]"
}

func (f *resumeFixture) rowMap(t *testing.T, conn *pgx.Conn) map[int64]string {
	t.Helper()
	rows, err := conn.Query(context.Background(), "SELECT id, payload FROM "+f.table)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	out := map[int64]string{}
	for rows.Next() {
		var id int64
		var p *string
		if err := rows.Scan(&id, &p); err != nil {
			t.Fatal(err)
		}
		if p == nil {
			out[id] = "<null>"
		} else {
			out[id] = *p
		}
	}
	return out
}

func buildBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "jinliang")
	build := execpkg.Command("go", "build", "-o", bin, "./cmd/jinliang")
	build.Dir = repoRoot(t)
	build.Env = os.Environ()
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("编译二进制失败: %v\n%s", err, out)
	}
	return bin
}

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Dir(filepath.Dir(wd)) // internal/engine → repo root
}

func yamlFor(name, table string) string {
	return "version: 1\njob: " + name + "\n" +
		"source:\n  kind: postgres\n  dsn: env:JINLIANG_TEST_PG_SRC_DSN\n  scope:\n    schema: public\n" +
		"  snapshot:\n    chunk_rows: 1000\n" +
		"target:\n  kind: postgres\n  dsn: env:JINLIANG_TEST_PG_DSN\n  schema: public\n  on_exists: append\n" +
		"content:\n  tables:\n    " + table + ": {}\n" +
		// continuous+initial：全量 + 追平 + 常态增量。搬存量的用例都带 --snapshot-only，
		// 只有「中断期间的改动能不能追平」那条会真的进增量。
		"mode:\n  frequency: continuous+initial\n"
}
