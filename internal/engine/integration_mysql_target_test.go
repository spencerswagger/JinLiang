//go:build integration

package engine_test

// PG 源 → MySQL 目标端的真库集成测试（CI 的 b1-engine job 带 -tags=integration 跑）。
//
// 缺 DSN 直接失败（不跳过）：静默通过是这个仓库最不能犯的错。
//   JINLIANG_TEST_PG_SRC_DSN（源）/ JINLIANG_TEST_MYSQL_DSN（目标）

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/core"
	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/engine"
	"github.com/spencerswagger/jinliang/internal/mysqlsink"
)

func mysqlTargetDSNs(t *testing.T) (src, dst string) {
	t.Helper()
	src = os.Getenv("JINLIANG_TEST_PG_SRC_DSN")
	dst = os.Getenv("JINLIANG_TEST_MYSQL_DSN")
	if src == "" || dst == "" {
		t.Fatalf("缺 DSN：需要 JINLIANG_TEST_PG_SRC_DSN 与 JINLIANG_TEST_MYSQL_DSN（缺了就是失败，不跳过）")
	}
	return src, dst
}

func mustMySQLTarget(t *testing.T, raw string) *sql.DB {
	t.Helper()
	c := myConfigOf(t, raw)
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&loc=UTC&charset=utf8mb4",
		c.user, c.pass, c.host, c.port, c.db))
	if err != nil {
		t.Fatalf("连不上 MySQL 目标: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("MySQL 目标 ping 失败: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// myEndpoint 把测试里的 mysql:// DSN 拆成 dsn.Endpoint（mysqlsink.Open 要端点）。
func myEndpoint(c myConn) dsn.Endpoint {
	return dsn.Endpoint{Kind: dsn.KindMySQL, Host: c.host, Port: c.port,
		Database: c.db, Username: c.user, Password: c.pass}
}

// newMySQLTargetJob 建一个「PG 源 → MySQL 目标」的任务（目标库就是连接里的库）。
func newMySQLTargetJob(t *testing.T, srcDSN, dstDSN, jobName, table, freq string) (*control.Store, string) {
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
	job, err := store.CreateJob(ctx, control.JobInput{
		Name: jobName, SourceID: sc.ID, TargetID: tc.ID,
		Spec: control.JobDoc{
			Version: 1, Job: jobName,
			Source: control.SourceDoc{Kind: "postgres", Scope: &control.ScopeDoc{Schema: "public"}},
			Target: control.TargetDoc{Kind: "mysql", OnExists: "append"},
			Mode: control.ModeDoc{Frequency: freq, Interval: "1h",
				// interval 模式要求水位列：给它一个，才能走到「目标端不支持」那道拒绝。
				IncrementalColumn: incCol(freq)},
			Content: &control.ContentDoc{Tables: map[string]control.TableDoc{table: {}}},
		},
	})
	if err != nil {
		t.Fatalf("建任务失败: %v", err)
	}
	// 清掉本任务在源端留下的书签与发布：PG 的 max_replication_slots 很小（默认 10），
	// 一个用例漏一个书签，整套跑第二遍就会「all replication slots are in use」——
	// 那不是被测代码的问题，是测试自己不收尾。
	dropJobSlot(t, srcDSN, job.JobKey)
	return store, job.ID
}

// dropJobSlot 在用例结束时清掉 jl_<jobKey>_1 书签与 jl_<jobKey> 发布。
func dropJobSlot(t *testing.T, srcDSN, jobKey string) {
	t.Helper()
	t.Cleanup(func() {
		conn := mustConn(t, srcDSN)
		ctx := context.Background()
		_, _ = conn.Exec(ctx,
			"SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE slot_name LIKE $1",
			"jl_"+jobKey+"%")
		_, _ = conn.Exec(ctx, "DROP PUBLICATION IF EXISTS "+`"jl_`+jobKey+`"`)
	})
}

// incCol：interval 模式必须有水位列，其它模式留空。
func incCol(freq string) string {
	if freq == "interval" {
		return "updated_at"
	}
	return ""
}

func myCount(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM `"+table+"`").Scan(&n); err != nil {
		t.Fatalf("数 %s 失败: %v", table, err)
	}
	return n
}

func dropMySQLState(t *testing.T, db *sql.DB) {
	t.Helper()
	for _, tbl := range []string{"_jl_meta", "_jl_chunk", "_jl_lease", "_jl_object", "_jl_instance"} {
		_, _ = db.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+tbl+"`")
	}
}

// 全量 + 增量（改/删/增）追平：PG 源 → MySQL 目标端走的是「非 PG 目标端」这条唯一路径。
func TestMySQLTargetFullCopyAndIncremental(t *testing.T) {
	srcDSN, dstDSN := mysqlTargetDSNs(t)
	src := mustConn(t, srcDSN)
	dst := mustMySQLTarget(t, dstDSN)
	table := "engine_my_target"

	exec(t, src, "DROP TABLE IF EXISTS "+table)
	exec(t, src, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, payload text, amount numeric(12,2))")
	exec(t, src, "INSERT INTO "+table+" SELECT g, 'row-'||g, (g%100)::numeric/100 FROM generate_series(1,20) g")
	t.Cleanup(func() { exec(t, src, "DROP TABLE IF EXISTS "+table) })

	_, _ = dst.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+table+"`")
	dropMySQLState(t, dst)

	store, jobID := newMySQLTargetJob(t, srcDSN, dstDSN, "my-target", table, "continuous+initial")

	res, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, StopAfter: 3 * time.Second})
	if err != nil {
		t.Fatalf("全量失败: %v", err)
	}
	if res.RowsCopied != 20 {
		t.Fatalf("应复制 20 行，实际 %d（阶段 %s）", res.RowsCopied, res.Phase)
	}
	if n := myCount(t, dst, table); n != 20 {
		t.Fatalf("目标端应有 20 行，实际 %d", n)
	}

	// 增量：改一笔、删一笔、加一笔
	exec(t, src, "UPDATE "+table+" SET payload='updated' WHERE id=2")
	exec(t, src, "DELETE FROM "+table+" WHERE id=3")
	exec(t, src, "INSERT INTO "+table+" VALUES (21,'new-row',1.23)")
	if _, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, StopAfter: 8 * time.Second}); err != nil {
		t.Fatalf("增量轮失败: %v", err)
	}
	if n := myCount(t, dst, table); n != 20 {
		t.Fatalf("增量后应有 20 行（21 - 删 1），实际 %d", n)
	}
	var payload string
	if err := dst.QueryRowContext(context.Background(),
		"SELECT payload FROM `"+table+"` WHERE id=2").Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if payload != "updated" {
		t.Fatalf("id=2 应更新成 updated，实际 %q", payload)
	}
	var gone int
	if err := dst.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM `"+table+"` WHERE id=3").Scan(&gone); err != nil {
		t.Fatal(err)
	}
	if gone != 0 {
		t.Fatal("源端删掉的 id=3 必须从目标端消失")
	}
}

// 重搬前必须清表：full_interval 每轮整轮重搬，源端删掉的行不能留在目标端。
//
// 这条同时是「删掉 clean-before-recopy 必须红」的那一格：不清的话目标端会保留
// 上一轮搬进去、源端已经删掉的行（upsert 不会删）。
func TestMySQLTargetRecopyCleansStaleRows(t *testing.T) {
	srcDSN, dstDSN := mysqlTargetDSNs(t)
	src := mustConn(t, srcDSN)
	dst := mustMySQLTarget(t, dstDSN)
	table := "engine_my_recopy"

	exec(t, src, "DROP TABLE IF EXISTS "+table)
	exec(t, src, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, payload text)")
	exec(t, src, "INSERT INTO "+table+" SELECT g, 'row-'||g FROM generate_series(1,30) g")
	t.Cleanup(func() { exec(t, src, "DROP TABLE IF EXISTS "+table) })

	_, _ = dst.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+table+"`")
	dropMySQLState(t, dst)

	store, jobID := newMySQLTargetJob(t, srcDSN, dstDSN, "my-recop", table, "full_interval")

	if _, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID}); err != nil {
		t.Fatalf("第一轮失败: %v", err)
	}
	if n := myCount(t, dst, table); n != 30 {
		t.Fatalf("第一轮后应有 30 行，实际 %d", n)
	}

	// 源端删 10 行：第二轮整轮重搬，目标端必须只剩 20 行
	exec(t, src, "DELETE FROM "+table+" WHERE id <= 10")
	if _, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID}); err != nil {
		t.Fatalf("第二轮失败: %v", err)
	}
	if n := myCount(t, dst, table); n != 20 {
		t.Fatalf("重搬后目标端应剩 20 行（源端删了 10 行），实际 %d —— 重搬前没清表，"+
			"upsert 不会删，源端已删的行留在目标端", n)
	}
}

// 租约冲突必须是 E1005（有码），不是「未分类」：历史页要能直接看出「另一个实例在跑」。
func TestMySQLTargetLeaseConflictIsE1005(t *testing.T) {
	srcDSN, dstDSN := mysqlTargetDSNs(t)
	src := mustConn(t, srcDSN)
	dst := mustMySQLTarget(t, dstDSN)
	table := "engine_my_lease"

	exec(t, src, "DROP TABLE IF EXISTS "+table)
	exec(t, src, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, payload text)")
	exec(t, src, "INSERT INTO "+table+" VALUES (1,'a')")
	t.Cleanup(func() { exec(t, src, "DROP TABLE IF EXISTS "+table) })
	_, _ = dst.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+table+"`")
	dropMySQLState(t, dst)

	store, jobID := newMySQLTargetJob(t, srcDSN, dstDSN, "my-lease", table, "continuous+initial")
	job, err := store.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatal(err)
	}

	// 用另一个句柄占住同一把租约（同一个 job_key）——模拟「另一个实例正在跑」。
	my := myConfigOf(t, dstDSN)
	holder, err := mysqlsink.Open(context.Background(), mysqlsink.Options{
		DSN:        myEndpoint(my),
		JobKey:     job.JobKey,
		Name:       "another-instance",
		InstanceID: "another-instance",
		OnExists:   mysqlsink.OnExists("append"),
	})
	if err != nil {
		t.Fatalf("打开占位句柄失败: %v", err)
	}
	defer holder.Close()
	if ok, why, err := holder.AcquireLease(context.Background(), time.Minute); err != nil || !ok {
		t.Fatalf("占位句柄应能拿到租约: ok=%v why=%s err=%v", ok, why, err)
	}

	_, err = engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID})
	if err == nil {
		t.Fatal("租约被占时这一轮必须失败")
	}
	if !strings.Contains(err.Error(), "另一个实例") {
		t.Fatalf("错误里要说清是租约冲突，得到: %v", err)
	}

	run, err := store.LatestRun(context.Background(), jobID)
	if err != nil {
		t.Fatalf("读最近一轮失败: %v", err)
	}
	if run.Code != string(core.CodeJobRunningElsewhere) {
		t.Fatalf("runs.code 应是 %s（租约冲突），实际 %q（last_error=%q）",
			core.CodeJobRunningElsewhere, run.Code, run.LastError)
	}
}

// 必改 1（回归）：首次搬进**用户自己的表**（append）+ 表里已有用户行，必须能跑。
//
// 旧实现「首次也清表」→ append 下 EnsureSchema 不建表、不登记所有权 → Count>0 且
// DidCreateTable=false → 直接 reusedTableRecopyError 硬停，文案还让用户「手动删掉
// 本任务写进去的行」——首次搬根本没有我们的行。这条用例就是拦它的。
func TestMySQLTargetFirstCopyIntoUserTableKeepsRows(t *testing.T) {
	srcDSN, dstDSN := mysqlTargetDSNs(t)
	src := mustConn(t, srcDSN)
	dst := mustMySQLTarget(t, dstDSN)
	table := "engine_my_user_table"

	exec(t, src, "DROP TABLE IF EXISTS "+table)
	exec(t, src, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, payload text)")
	exec(t, src, "INSERT INTO "+table+" SELECT g, 'src-'||g FROM generate_series(1,10) g")
	t.Cleanup(func() { exec(t, src, "DROP TABLE IF EXISTS "+table) })

	// 目标表是**用户自己的**：先建好、预置 3 行，不经过本任务的任何建表动作。
	ctx := context.Background()
	_, _ = dst.ExecContext(ctx, "DROP TABLE IF EXISTS `"+table+"`")
	dropMySQLState(t, dst)
	if _, err := dst.ExecContext(ctx, "CREATE TABLE `"+table+"` (id BIGINT PRIMARY KEY, payload TEXT)"); err != nil {
		t.Fatalf("建用户表失败: %v", err)
	}
	if _, err := dst.ExecContext(ctx,
		"INSERT INTO `"+table+"` VALUES (900001,'user-1'),(900002,'user-2'),(900003,'user-3')"); err != nil {
		t.Fatalf("预置用户行失败: %v", err)
	}
	t.Cleanup(func() { _, _ = dst.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+table+"`") })

	store, jobID := newMySQLTargetJob(t, srcDSN, dstDSN, "my-user-table", table, "once")
	if _, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID}); err != nil {
		t.Fatalf("首次搬进用户表不该失败（append 语义）：%v", err)
	}

	var userRows int
	if err := dst.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM `"+table+"` WHERE id BETWEEN 900001 AND 900003").Scan(&userRows); err != nil {
		t.Fatal(err)
	}
	if userRows != 3 {
		t.Fatalf("用户预置的 3 行必须还在，实际 %d 行", userRows)
	}
	if n := myCount(t, dst, table); n != 13 {
		t.Fatalf("目标端应有 13 行（源端 10 + 用户 3），实际 %d", n)
	}
}

// 必改 2（回归）：`--snapshot-only` 打在 continuous+initial 上必须**搬完就退出**，
// 不能一路进持续流（`--help` 承诺「只搬存量、不进增量」）。
func TestMySQLTargetSnapshotOnlyDoesNotStream(t *testing.T) {
	srcDSN, dstDSN := mysqlTargetDSNs(t)
	src := mustConn(t, srcDSN)
	dst := mustMySQLTarget(t, dstDSN)
	table := "engine_my_snapshotonly"

	exec(t, src, "DROP TABLE IF EXISTS "+table)
	exec(t, src, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, payload text)")
	exec(t, src, "INSERT INTO "+table+" SELECT g, 'row-'||g FROM generate_series(1,10) g")
	t.Cleanup(func() { exec(t, src, "DROP TABLE IF EXISTS "+table) })
	_, _ = dst.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+table+"`")
	dropMySQLState(t, dst)

	store, jobID := newMySQLTargetJob(t, srcDSN, dstDSN, "my-snapshot-only", table, "continuous+initial")

	started := time.Now()
	res, err := engine.Run(context.Background(), engine.Options{
		Store: store, JobID: jobID, SnapshotOnly: true,
	})
	elapsed := time.Since(started)
	if err != nil {
		t.Fatalf("--snapshot-only 失败: %v", err)
	}
	if elapsed > 20*time.Second {
		t.Fatalf("--snapshot-only 跑了 %s：进了持续流（应搬完就退出）", elapsed.Round(time.Millisecond))
	}
	if res.RowsCopied != 10 {
		t.Fatalf("应复制 10 行，实际 %d", res.RowsCopied)
	}
	if res.RowsStream != 0 {
		t.Fatalf("--snapshot-only 不该走增量，实际应用了 %d 行", res.RowsStream)
	}
	if n := myCount(t, dst, table); n != 10 {
		t.Fatalf("目标端应有 10 行，实际 %d", n)
	}
	// 位点：这条路径**应该**前进到复制起点（不是「位点没动」）。没有位点的话，
	// 下一轮会被当成首次又整表重搬一遍。
	job, err := store.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatal(err)
	}
	var pos string
	if err := dst.QueryRowContext(context.Background(),
		"SELECT COALESCE(position,'') FROM `_jl_meta` WHERE job_key = ?", job.JobKey).Scan(&pos); err != nil {
		t.Fatalf("读位点失败: %v", err)
	}
	if pos == "" {
		t.Fatal("--snapshot-only 之后位点应已前进到复制起点，实际为空（下一轮会当首次重搬）")
	}
}

// interval 在 MySQL 目标端必须**在拿租约、建表之前**就停（不只是文案对）：
// 跑完之后 `_jl_lease` 里没有这个任务的租约、业务表也不存在。
func TestMySQLTargetIntervalStopsBeforeLeaseAndSchema(t *testing.T) {
	srcDSN, dstDSN := mysqlTargetDSNs(t)
	src := mustConn(t, srcDSN)
	dst := mustMySQLTarget(t, dstDSN)
	table := "engine_my_interval"

	exec(t, src, "DROP TABLE IF EXISTS "+table)
	exec(t, src, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, payload text, updated_at timestamptz)")
	exec(t, src, "INSERT INTO "+table+" VALUES (1,'a',now())")
	t.Cleanup(func() { exec(t, src, "DROP TABLE IF EXISTS "+table) })
	_, _ = dst.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+table+"`")
	dropMySQLState(t, dst)

	store, jobID := newMySQLTargetJob(t, srcDSN, dstDSN, "my-interval", table, "interval")
	job, err := store.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatal(err)
	}

	_, err = engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID})
	if err == nil {
		t.Fatal("interval 在 MySQL 目标端必须拒绝")
	}
	if !strings.Contains(err.Error(), "不支持 interval") {
		t.Fatalf("要给出四段式说明，得到: %v", err)
	}
	// 停在拿租约之前：状态表里没有这个任务的租约（表可能还没建，查不到也算没有）。
	var leases int
	if err := dst.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM `_jl_lease` WHERE job_key = ?", job.JobKey).Scan(&leases); err == nil && leases != 0 {
		t.Fatalf("interval 拒绝前不该拿租约，实际留下 %d 行租约", leases)
	}
	// 也不该建业务表。
	var tables int
	if err := dst.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
		table).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if tables != 0 {
		t.Fatal("interval 拒绝前不该建业务表")
	}
	// 也不能留下对象登记（排除「先建后删」这条可能：登记还在说明我们动过表）。
	var objects int
	if err := dst.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM `_jl_object` WHERE object_name LIKE ? AND job_key = ?",
		"%"+table+"%", job.JobKey).Scan(&objects); err == nil && objects != 0 {
		t.Fatalf("interval 拒绝前不该登记任何对象，实际 %d 行", objects)
	}
}

// full_interval 第 2 轮起也要对齐结构：源端 ADD COLUMN 之后重搬不能「unknown column」。
func TestMySQLTargetFullIntervalFollowsAddedColumn(t *testing.T) {
	srcDSN, dstDSN := mysqlTargetDSNs(t)
	src := mustConn(t, srcDSN)
	dst := mustMySQLTarget(t, dstDSN)
	table := "engine_my_addcol"

	exec(t, src, "DROP TABLE IF EXISTS "+table)
	exec(t, src, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, payload text)")
	exec(t, src, "INSERT INTO "+table+" SELECT g, 'row-'||g FROM generate_series(1,10) g")
	t.Cleanup(func() { exec(t, src, "DROP TABLE IF EXISTS "+table) })
	_, _ = dst.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+table+"`")
	dropMySQLState(t, dst)

	store, jobID := newMySQLTargetJob(t, srcDSN, dstDSN, "my-addcol", table, "full_interval")
	if _, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID}); err != nil {
		t.Fatalf("第一轮失败: %v", err)
	}

	// 源端加列 + 补值，第二轮整轮重搬必须跟上
	exec(t, src, "ALTER TABLE "+table+" ADD COLUMN extra text")
	exec(t, src, "UPDATE "+table+" SET extra='e'||id")
	if _, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID}); err != nil {
		t.Fatalf("源端加列后重搬失败（目标表没跟上结构）：%v", err)
	}
	var got string
	if err := dst.QueryRowContext(context.Background(),
		"SELECT extra FROM `"+table+"` WHERE id=5").Scan(&got); err != nil {
		t.Fatalf("读新列失败: %v", err)
	}
	if got != "e5" {
		t.Fatalf("新列的值应是 e5，实际 %q", got)
	}
}

// 必改 2：`phase == "snapshot" ⇒ 硬停` 必须**真的可达**。
//
// 构造「上一轮首搬崩在半路」：目标表是用户自己的（append）、里面有 3 行用户数据，
// 另外还有一行是我们上一轮搬进去的；`_jl_meta` 的 phase 停在 `snapshot`、位点为空。
// 这时重跑必须停下来问人——表里已经混着两种行，清不清都得用户决定。
//
// 旧实现的保护是假的：generic 路径从不写 `_jl_meta.phase`（r.sink 为 nil），
// 而 `EnsureMeta` 每次开头把 phase 重置成 'prepare' → 这条规则永远走不到，
// 结果是「跳过清表 + 退出码 0」，源端停机期删掉的行留在用户表里。
func TestMySQLTargetCrashMidCopyOnUserTableStops(t *testing.T) {
	srcDSN, dstDSN := mysqlTargetDSNs(t)
	src := mustConn(t, srcDSN)
	dst := mustMySQLTarget(t, dstDSN)
	table := "engine_my_crash_user"

	exec(t, src, "DROP TABLE IF EXISTS "+table)
	exec(t, src, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, payload text)")
	exec(t, src, "INSERT INTO "+table+" SELECT g, 'src-'||g FROM generate_series(1,10) g")
	t.Cleanup(func() { exec(t, src, "DROP TABLE IF EXISTS "+table) })

	ctx := context.Background()
	_, _ = dst.ExecContext(ctx, "DROP TABLE IF EXISTS `"+table+"`")
	dropMySQLState(t, dst)
	if _, err := dst.ExecContext(ctx, "CREATE TABLE `"+table+"` (id BIGINT PRIMARY KEY, payload TEXT)"); err != nil {
		t.Fatalf("建用户表失败: %v", err)
	}
	// 用户自己的 3 行 + 我们上一轮搬进去的 1 行（混在一起，分不清）
	if _, err := dst.ExecContext(ctx,
		"INSERT INTO `"+table+"` VALUES (900001,'user-1'),(900002,'user-2'),(900003,'user-3'),(1,'src-1')"); err != nil {
		t.Fatalf("预置行失败: %v", err)
	}
	t.Cleanup(func() { _, _ = dst.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+table+"`") })

	store, jobID := newMySQLTargetJob(t, srcDSN, dstDSN, "my-crash-user", table, "once")
	job, err := store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	// 造「上一轮崩在快照中途」：phase=snapshot、位点为空（位点只在全量之后写）。
	my := myConfigOf(t, dstDSN)
	seed, err := mysqlsink.Open(ctx, mysqlsink.Options{
		DSN: myEndpoint(my), JobKey: job.JobKey, Name: job.Name,
		InstanceID: "crashed-instance", OnExists: mysqlsink.OnExists("append"),
	})
	if err != nil {
		t.Fatalf("打开种子句柄失败: %v", err)
	}
	if err := seed.EnsureMeta(ctx, "seed", "prepare", "", ""); err != nil {
		t.Fatalf("种 _jl_meta 失败: %v", err)
	}
	if err := seed.SetPhase(ctx, "snapshot"); err != nil {
		t.Fatalf("种阶段失败: %v", err)
	}
	_ = seed.Close()

	_, err = engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID})
	if err == nil {
		t.Fatal("首搬崩在半路的用户表：重跑必须停下来问人（不清也不静默 upsert）")
	}
	if !strings.Contains(err.Error(), "是你自己的表") {
		t.Fatalf("要给出 reusedTableRecopyError 那种「你自己的表」说明，得到: %v", err)
	}
	// 一行都不能少：用户的 3 行 + 我们的 1 行都还在（没有清表）
	if n := myCount(t, dst, table); n != 4 {
		t.Fatalf("硬停时不该动数据：应有 4 行，实际 %d", n)
	}
}

// 阶段要真的落进 `_jl_meta`：跑完之后是 done（不是永远停在 prepare）。
// 这条同时是上一条的前提——写不进去，「崩在哪个阶段」就无从判断。
func TestMySQLTargetPhaseLandsInMeta(t *testing.T) {
	srcDSN, dstDSN := mysqlTargetDSNs(t)
	src := mustConn(t, srcDSN)
	dst := mustMySQLTarget(t, dstDSN)
	table := "engine_my_phase"

	exec(t, src, "DROP TABLE IF EXISTS "+table)
	exec(t, src, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, payload text)")
	exec(t, src, "INSERT INTO "+table+" SELECT g, 'row-'||g FROM generate_series(1,5) g")
	t.Cleanup(func() { exec(t, src, "DROP TABLE IF EXISTS "+table) })
	_, _ = dst.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+table+"`")
	dropMySQLState(t, dst)

	store, jobID := newMySQLTargetJob(t, srcDSN, dstDSN, "my-phase", table, "once")
	job, err := store.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID}); err != nil {
		t.Fatalf("全量失败: %v", err)
	}
	var phase string
	if err := dst.QueryRowContext(context.Background(),
		"SELECT phase FROM `_jl_meta` WHERE job_key = ?", job.JobKey).Scan(&phase); err != nil {
		t.Fatalf("读阶段失败: %v", err)
	}
	if phase != "done" {
		t.Fatalf("跑完之后 _jl_meta.phase 应是 done，实际 %q（阶段没落到目标端）", phase)
	}
}

// `Old` 也要做时间规范化：DELETE 只带 `Old`，时间主键的删除靠它定位。
// 不规范化的话 pgoutput 发的是带时区后缀的文本，MySQL 按主键比较会报 1292 或删 0 行。
func TestMySQLTargetDeleteByTemporalKey(t *testing.T) {
	srcDSN, dstDSN := mysqlTargetDSNs(t)
	src := mustConn(t, srcDSN)
	dst := mustMySQLTarget(t, dstDSN)
	table := "engine_my_ts_pk"

	exec(t, src, "DROP TABLE IF EXISTS "+table)
	// 时间主键用 **timestamptz**：pgoutput 对它会发带时区后缀的文本
	// （`…+00`），MySQL 的 datetime 不接受——这正是 Old 必须规范化的场景。
	// （`timestamp` 不带后缀，MySQL 本来就能解析，测不出这条。）
	exec(t, src, "CREATE TABLE "+table+" (at timestamptz PRIMARY KEY, payload text)")
	exec(t, src, "INSERT INTO "+table+` VALUES
		('2026-01-01 10:00:00.000001','a'),
		('2026-01-01 10:00:00.000002','b'),
		('2026-01-01 10:00:00.000003','c')`)
	t.Cleanup(func() { exec(t, src, "DROP TABLE IF EXISTS "+table) })
	_, _ = dst.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+table+"`")
	dropMySQLState(t, dst)

	store, jobID := newMySQLTargetJob(t, srcDSN, dstDSN, "my-ts-pk", table, "continuous+initial")
	if _, err := engine.Run(context.Background(), engine.Options{
		Store: store, JobID: jobID, StopAfter: 3 * time.Second,
	}); err != nil {
		t.Fatalf("全量失败: %v", err)
	}
	if n := myCount(t, dst, table); n != 3 {
		t.Fatalf("目标端应有 3 行，实际 %d", n)
	}

	// 删掉中间那一行：这条 DELETE 事件只带 Old（时间主键）
	exec(t, src, "DELETE FROM "+table+" WHERE payload='b'")
	if _, err := engine.Run(context.Background(), engine.Options{
		Store: store, JobID: jobID, StopAfter: 8 * time.Second,
	}); err != nil {
		t.Fatalf("增量轮失败: %v", err)
	}
	if n := myCount(t, dst, table); n != 2 {
		t.Fatalf("时间主键的删除必须生效（Old 也要规范化）：应有 2 行，实际 %d", n)
	}
	var gone int
	if err := dst.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM `"+table+"` WHERE payload='b'").Scan(&gone); err != nil {
		t.Fatal(err)
	}
	if gone != 0 {
		t.Fatal("被删的那一行还在目标端")
	}
}
