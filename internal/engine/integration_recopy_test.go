//go:build integration

package engine_test

// 真库集成测试：只有带 -tags=integration 才编译/运行（CI 的 b1-engine job 会带这个 tag）。
//
// 缺 DSN 时**直接失败**而不是跳过：跳过＝静默通过，这个仓库不接受。
// 变量名以 scripts/e2e_headless.sh 与 docs/BUILD.md 为准：
//   JINLIANG_TEST_PG_SRC_DSN（源）/ JINLIANG_TEST_PG_DSN（目标）

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/core"
	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/engine"
	"github.com/spencerswagger/jinliang/internal/pgsink"
	"github.com/spencerswagger/jinliang/internal/source"
	"github.com/spencerswagger/jinliang/internal/state"
)

func dsns(t *testing.T) (src, dst string) {
	t.Helper()
	src = os.Getenv("JINLIANG_TEST_PG_SRC_DSN")
	dst = os.Getenv("JINLIANG_TEST_PG_DSN")
	if src == "" || dst == "" {
		t.Fatalf("缺 DSN：需要 JINLIANG_TEST_PG_SRC_DSN 与 JINLIANG_TEST_PG_DSN（缺了就是失败，不跳过）")
	}
	if src == dst {
		t.Fatalf("源库与目标库不能是同一个（会同步到自己身上）")
	}
	return src, dst
}

func mustConn(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("连不上 %s: %v", dsn, err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

func exec(t *testing.T, conn *pgx.Conn, sql string, args ...any) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("执行失败 %q: %v", sql, err)
	}
}

// newJob 在临时控制库里建连接与任务，返回 job id。
func newJob(t *testing.T, src, dst, jobName, table string) (*control.Store, string) {
	t.Helper()
	dir := t.TempDir()
	store, err := control.Open(control.Options{DataDir: dir, Autogen: true})
	if err != nil {
		t.Fatalf("打开控制库失败: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	ctx := context.Background()

	sc, err := store.CreateConnection(ctx, control.ConnectionInput{
		Name: "src", Kind: "postgres", Host: "127.0.0.1", Port: portOf(t, src),
		Database: dbOf(t, src), Username: userOf(t, src), Password: passOf(t, src),
		Params: map[string]string{"sslmode": "disable"},
	})
	if err != nil {
		t.Fatalf("建源连接失败: %v", err)
	}
	tc, err := store.CreateConnection(ctx, control.ConnectionInput{
		Name: "dst", Kind: "postgres", Host: "127.0.0.1", Port: portOf(t, dst),
		Database: dbOf(t, dst), Username: userOf(t, dst), Password: passOf(t, dst),
		Params: map[string]string{"sslmode": "disable"},
	})
	if err != nil {
		t.Fatalf("建目标连接失败: %v", err)
	}
	job, err := store.CreateJob(ctx, control.JobInput{
		Name: jobName, SourceID: sc.ID, TargetID: tc.ID,
		Spec: control.JobDoc{
			Version: 1, Job: jobName,
			Source:  control.SourceDoc{Kind: "postgres", Scope: &control.ScopeDoc{Schema: "public"}},
			Target:  control.TargetDoc{Kind: "postgres", Schema: "public", OnExists: "append"},
			Mode:    control.ModeDoc{Frequency: "once"},
			Content: &control.ContentDoc{Tables: map[string]control.TableDoc{table: {}}},
		},
	})
	if err != nil {
		t.Fatalf("建任务失败: %v", err)
	}
	trackJobSlots(t, src, store, job.ID)
	return store, job.ID
}

func portOf(t *testing.T, dsn string) int    { return int(cfgOf(t, dsn).Port) }
func dbOf(t *testing.T, dsn string) string   { return cfgOf(t, dsn).Database }
func userOf(t *testing.T, dsn string) string { return cfgOf(t, dsn).User }
func passOf(t *testing.T, dsn string) string { return cfgOf(t, dsn).Password }

func cfgOf(t *testing.T, dsn string) *pgx.ConnConfig {
	t.Helper()
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("DSN 解析失败: %v", err)
	}
	return cfg
}

// trackJobSlots 在用例收尾时回收这条任务建的逻辑书签。
//
// 为什么必须回收：逻辑书签是**实例级**资源（max_replication_slots，PG 默认 10），
// 而一轮 engine 集成用例会建 6~10 个书签。只要有一条用例不回收，同一轮里靠后的用例
// 就会撞 "all replication slots are in use (SQLSTATE 53400)"，下一轮更是从第一条就开始红。
// 这就是「整包跑红、单条单跑绿」的真正来源——不是用例之间顺序依赖，是资源没还。
func trackJobSlots(t *testing.T, srcDSN string, store *control.Store, jobID string) {
	t.Helper()
	conn := mustConn(t, srcDSN)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		job, err := store.GetJob(ctx, jobID)
		if err != nil {
			return // 用例自己把任务删了：没有书签要清
		}
		dropSlotsLike(ctx, conn, "jl_"+job.JobKey+"%")
	})
}

// dropSlotsLike 删掉名字匹配的书签；占着书签的读者先踢掉（测试收尾时它一定是废弃进程）。
// 尽力而为：清不掉只记日志，不用清理失败改写用例结论。
func dropSlotsLike(ctx context.Context, conn *pgx.Conn, pattern string) {
	rows, err := conn.Query(ctx,
		`SELECT slot_name, active_pid FROM pg_replication_slots WHERE slot_name LIKE $1`, pattern)
	if err != nil {
		return
	}
	type slot struct {
		name string
		pid  *int
	}
	var slots []slot
	for rows.Next() {
		var s slot
		if rows.Scan(&s.name, &s.pid) == nil {
			slots = append(slots, s)
		}
	}
	rows.Close()
	for _, s := range slots {
		if s.pid != nil {
			if _, err := conn.Exec(ctx, `SELECT pg_terminate_backend($1)`, *s.pid); err == nil {
				time.Sleep(200 * time.Millisecond) // 等后端真的退干净
			}
		}
		_, _ = conn.Exec(ctx, `SELECT pg_drop_replication_slot($1)`, s.name)
	}
}

// 这是 Owl 要的那条可证伪用例：目标端**预置 3 行**（用户自己的表，append 模式复用它）
// → 上次全量搬到一半（phase=snapshot、有块标记）→ 重启重搬 → **那 3 行必须还在**。
//
// 为什么「预置行」是这条用例不可省的构造（Owl 的两条证据，写在这里防止以后被简化掉）：
//  1. CI 里 `scripts/b1_pg_e2e.sh` 的 kill/append 用例，目标库是**这一轮自己建的空表**，
//     同一段代码换成「用户预置 3 行」就是数据丢失 —— 空表跑绿证明不了这条安全。
//  2. 默认 `on_exists: error` 时根本进不到 snapshot()：崩后表已存在，EnsureSchema 先返回
//     ErrTargetTableExists；`recreate` 在 prepare 阶段已经 DROP、清空是冗余。
//     也就是说「崩在快照中间 → 清空半截表」**只对 append 成立**，而 append 恰恰是唯一
//     不该清的模式。唯一可达的 TRUNCATE 路径正好落在错误的分支上。
//
// 所以判据必须是「这张表是不是本任务建的」（_jl_object 的 table:<schema>.<table>），
// 不能用「有没有块标记」——BulkApply 对 append 复用的表也会无条件写块标记。
// 修之前这条会红：旧代码只看「有块标记」就 TRUNCATE，把用户的行一起清掉。
func TestRecopyDoesNotWipeReusedTable(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	srcDB := mustConn(t, srcDSN)
	dstDB := mustConn(t, dstDSN)
	table := "recopy_reused"

	exec(t, srcDB, "DROP TABLE IF EXISTS "+table)
	exec(t, srcDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, payload text)")
	exec(t, srcDB, "INSERT INTO "+table+" SELECT g, 'src-'||g FROM generate_series(1,200) g")

	exec(t, dstDB, "DROP TABLE IF EXISTS "+table)
	exec(t, dstDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, payload text)")
	exec(t, dstDB, "INSERT INTO "+table+" VALUES (1,'user-1'),(2,'user-2'),(3,'user-3')")
	exec(t, dstDB, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")

	store, jobID := newJob(t, srcDSN, dstDSN, "recopy-reused", table)
	ensureStateTables(t, store, dstDSN, jobID)
	// 造「上次全量搬到一半就崩了」的状态：meta 停在 snapshot，块 1 已落库。
	seedCrashedSnapshot(t, store, dstDB, jobID, table, 10)

	_, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID})
	if err == nil {
		t.Fatal("目标表是用户自己的表时，重搬全量必须停下来，不能默默清空")
	}
	if !strings.Contains(err.Error(), "是你自己的表") {
		t.Fatalf("错误里要说清「表不是本任务建的」，得到: %v", err)
	}

	var n int
	if err := dstDB.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		rows := dump(t, dstDB, table)
		t.Fatalf("用户原有的 3 行必须还在，实际 %d 行：%v", n, rows)
	}
}

// 反过来：表是本任务建的（`_jl_object` 里有 table: 记录）→ 重搬时允许清空并重来。
func TestRecopyTruncatesTableWeCreated(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	srcDB := mustConn(t, srcDSN)
	dstDB := mustConn(t, dstDSN)
	table := "recopy_ours"

	exec(t, srcDB, "DROP TABLE IF EXISTS "+table)
	exec(t, srcDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, payload text)")
	exec(t, srcDB, "INSERT INTO "+table+" SELECT g, 'src-'||g FROM generate_series(1,50) g")

	exec(t, dstDB, "DROP TABLE IF EXISTS "+table)
	exec(t, dstDB, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")
	exec(t, dstDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, payload text)")
	exec(t, dstDB, "INSERT INTO "+table+" VALUES (9999,'half-copied-by-us')")

	store, jobID := newJob(t, srcDSN, dstDSN, "recopy-ours", table)
	ensureStateTables(t, store, dstDSN, jobID)
	seedCrashedSnapshot(t, store, dstDB, jobID, table, 7)
	markTableCreatedByJob(t, store, dstDB, jobID, table)

	if _, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID}); err != nil {
		t.Fatalf("本任务建的表应该允许重搬，得到: %v", err)
	}
	var n int
	if err := dstDB.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 50 {
		t.Fatalf("重搬后应是源端的 50 行（半截行被清掉），实际 %d", n)
	}
	var leftover int
	if err := dstDB.QueryRow(context.Background(), "SELECT count(*) FROM "+table+" WHERE id=9999").Scan(&leftover); err != nil {
		t.Fatal(err)
	}
	if leftover != 0 {
		t.Fatal("上次写了一半的行应被清掉")
	}
}

// ensureStateTables 先把 `_jl_*` 建出来（正常是 pgsink.Open 干的，这里要在造状态前就跑一次）。
func ensureStateTables(t *testing.T, store *control.Store, dstDSN, jobID string) {
	t.Helper()
	ctx := context.Background()
	job, err := store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	cfg := cfgOf(t, dstDSN)
	ep := dsn.Endpoint{Kind: "postgres", Host: cfg.Host, Port: int(cfg.Port),
		Database: cfg.Database, Username: cfg.User, Password: cfg.Password,
		Params: map[string]string{"sslmode": "disable"}}
	sink, err := pgsink.Open(ctx, pgsink.Options{
		Target: ep, JobKey: job.JobKey, JobName: job.Name, InstanceID: "test", Schema: "public",
		OnExists: "append",
	})
	if err != nil {
		t.Fatalf("建状态表失败: %v", err)
	}
	sink.Close()
}

// seedCrashedSnapshot 直接造出「全量中途被 kill -9」之后的目标端状态。
func seedCrashedSnapshot(t *testing.T, store *control.Store, dst *pgx.Conn, jobID, table string, rows int) {
	t.Helper()
	job, err := store.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatal(err)
	}
	exec(t, dst, `INSERT INTO _jl_meta (job_key,name,instance_id,phase,position)
		VALUES ($1,$2,'test','snapshot','postgres:v1:0/10')
		ON CONFLICT (job_key) DO UPDATE SET phase='snapshot', position='postgres:v1:0/10'`,
		job.JobKey, job.Name)
	exec(t, dst, `INSERT INTO _jl_chunk (job_key,table_name,chunk_no,done,rows)
		VALUES ($1,$2,1,true,$3) ON CONFLICT DO NOTHING`, job.JobKey, table, rows)
}

// markTableCreatedByJob 让 `_jl_object` 认为这张表是本任务建的（模拟上次运行 create 过）。
func markTableCreatedByJob(t *testing.T, store *control.Store, dst *pgx.Conn, jobID, table string) {
	t.Helper()
	job, err := store.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatal(err)
	}
	exec(t, dst, `INSERT INTO _jl_object (object_name, object_kind, job_key, instance_id)
		VALUES ($1,'table',$2,'test') ON CONFLICT (object_name) DO NOTHING`,
		"table:public."+table, job.JobKey)
}

func dump(t *testing.T, conn *pgx.Conn, table string) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), "SELECT id::text||'='||payload FROM "+table+" ORDER BY id")
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err == nil {
			out = append(out, s)
		}
	}
	return out
}

// Owl 要的那条：两个持有**同一个 instance_id** 的句柄，第二个在租约未过期时抢不到。
// 没有这条，同一台机器上两次 `run --job X` 会同时写同一张目标表。
func TestLeaseNotStolenBySameInstance(t *testing.T) {
	_, dstDSN := dsns(t)
	dst := mustConn(t, dstDSN)
	exec(t, dst, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")

	cfg := cfgOf(t, dstDSN)
	ep := dsn.Endpoint{Kind: "postgres", Host: cfg.Host, Port: int(cfg.Port),
		Database: cfg.Database, Username: cfg.User, Password: cfg.Password,
		Params: map[string]string{"sslmode": "disable"}}
	opts := pgsink.Options{Target: ep, JobKey: "leasejob01", JobName: "lease-job",
		InstanceID: "same-instance", Schema: "public"}

	ctx := context.Background()
	first, err := pgsink.Open(ctx, opts)
	if err != nil {
		t.Fatalf("打开第一个句柄失败: %v", err)
	}
	defer first.Close()
	ok, why, err := first.AcquireLease(ctx, time.Minute)
	if err != nil || !ok {
		t.Fatalf("第一个句柄应拿到租约: ok=%v err=%v why=%s", ok, err, why)
	}

	second, err := pgsink.Open(ctx, opts)
	if err != nil {
		t.Fatalf("打开第二个句柄失败: %v", err)
	}
	defer second.Close()
	ok, why, err = second.AcquireLease(ctx, time.Minute)
	if err != nil {
		t.Fatalf("第二个句柄取租约报错: %v", err)
	}
	if ok {
		t.Fatal("同一个 instance_id 的第二个句柄不该抢到未过期的租约（会双写）")
	}
	if !strings.Contains(why, "还有一个进程") {
		t.Fatalf("拒绝时要说清是谁在跑，得到: %s", why)
	}

	// 第一个仍然有效：续租成功，第二个续租失败（它没有租约）
	if ok, err := first.RenewLease(ctx, time.Minute); err != nil || !ok {
		t.Fatalf("第一个句柄应能续租: ok=%v err=%v", ok, err)
	}
	if ok, _ := second.RenewLease(ctx, time.Minute); ok {
		t.Fatal("没拿到租约的句柄不该续租成功")
	}
}

// B5：interval 模式只搬「水位之后」的行，且第一次先整表搬完并把水位对齐到表尾。
// 断言的是行数变化（不是「跑通了」）：第二轮必须只搬 4 行，不是 1000 行。
func TestIntervalCopiesOnlyRowsAfterWatermark(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	srcDB := mustConn(t, srcDSN)
	dstDB := mustConn(t, dstDSN)
	table := "wm_it"
	ctx := context.Background()

	exec(t, srcDB, "DROP TABLE IF EXISTS "+table)
	exec(t, srcDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, payload text, updated_at timestamptz)")
	// 时间戳**逐行递增**（跨 2 小时），否则「同一时刻的行全在水位窗口内」，
	// 回看窗口会合理地重读整表，断言「只搬 N 行」就没有意义了。
	exec(t, srcDB, "INSERT INTO "+table+
		" SELECT g, 'row-'||g, now() - interval '2 hours' + (g||' seconds')::interval FROM generate_series(1,1000) g")
	exec(t, dstDB, "DROP TABLE IF EXISTS "+table)
	exec(t, dstDB, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")

	dir := t.TempDir()
	store, err := control.Open(control.Options{DataDir: dir, Autogen: true})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	sc, err := store.CreateConnection(ctx, control.ConnectionInput{
		Name: "src", Kind: "postgres", Host: "127.0.0.1", Port: portOf(t, srcDSN),
		Database: dbOf(t, srcDSN), Username: userOf(t, srcDSN), Password: passOf(t, srcDSN),
		Params: map[string]string{"sslmode": "disable"},
	})
	if err != nil {
		t.Fatal(err)
	}
	tc, err := store.CreateConnection(ctx, control.ConnectionInput{
		Name: "dst", Kind: "postgres", Host: "127.0.0.1", Port: portOf(t, dstDSN),
		Database: dbOf(t, dstDSN), Username: userOf(t, dstDSN), Password: passOf(t, dstDSN),
		Params: map[string]string{"sslmode": "disable"},
	})
	if err != nil {
		t.Fatal(err)
	}
	job, err := store.CreateJob(ctx, control.JobInput{
		Name: "wm-it", SourceID: sc.ID, TargetID: tc.ID,
		Spec: control.JobDoc{
			Version: 1, Job: "wm-it",
			Source: control.SourceDoc{Kind: "postgres", Scope: &control.ScopeDoc{Schema: "public"}},
			Target: control.TargetDoc{Kind: "postgres", Schema: "public", OnExists: "append"},
			Mode: control.ModeDoc{Frequency: "interval", Interval: "1s",
				IncrementalColumn: "updated_at", Lookback: "1s"},
			Content: &control.ContentDoc{Tables: map[string]control.TableDoc{table: {}}},
		},
	})
	if err != nil {
		t.Fatalf("建任务失败: %v", err)
	}

	// 第一轮：整表 1000 行 + 水位对齐表尾
	if _, err := engine.Run(ctx, engine.Options{Store: store, JobID: job.ID}); err != nil {
		t.Fatalf("第一轮失败: %v", err)
	}
	var n int
	if err := dstDB.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1000 {
		t.Fatalf("第一轮应整表搬 1000 行，得到 %d", n)
	}

	// 第二轮前：3 条新行 + 1 条老行被更新（水位之后的变更）
	exec(t, srcDB, "INSERT INTO "+table+" VALUES (1001,'new-1',now()),(1002,'new-2',now()),(1003,'new-3',now())")
	exec(t, srcDB, "UPDATE "+table+" SET payload='touched', updated_at=now() WHERE id=5")

	res, err := engine.Run(ctx, engine.Options{Store: store, JobID: job.ID})
	if err != nil {
		t.Fatalf("第二轮失败: %v", err)
	}
	// 回看窗口（1m）会重读水位附近的一小段，这是「宁重不漏」的设计代价；
	// 断言的是「没重扫整表」+「新增/更新都到了」，而不是精确 4 行。
	if res.RowsStream < 4 {
		t.Fatalf("第二轮应至少搬 4 行（3 新增 + 1 更新），实际 %d", res.RowsStream)
	}
	// 上界收到 20：回看窗口正常时合法重读就是这 4 行（测试数据确定性很强），
	// 留 200 会连「回退窗口被误设成 10 分钟」这种形态都发现不了。
	if res.RowsStream > 20 {
		t.Fatalf("第二轮搬了 %d 行（合法应只有 4 行左右），说明回退窗口设错了或水位没生效", res.RowsStream)
	}
	var touched int
	if err := dstDB.QueryRow(ctx, "SELECT count(*) FROM "+table+" WHERE payload='touched'").Scan(&touched); err != nil {
		t.Fatal(err)
	}
	if touched != 1 {
		t.Fatalf("更新应被搬过去，得到 %d 行", touched)
	}
}

// Owl 要的那条：**lookback 必须真的生效**。
//
// 场景模拟「长事务 / 复制延迟」：一行在事务里很早就写好了（updated_at 很早），
// 但它在本轮水位推进之后才可见。若回退窗口没生效（曾经 cursor 没清空 PK，
// lookback 一次都用不上），这条行会永久读不到——漏读是重跑也补不回来的错误。
func TestIntervalLookbackCatchesLateCommittedRows(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	srcDB := mustConn(t, srcDSN)
	dstDB := mustConn(t, dstDSN)
	table := "wm_late"
	ctx := context.Background()

	exec(t, srcDB, "DROP TABLE IF EXISTS "+table)
	exec(t, srcDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, payload text, updated_at timestamptz)")
	exec(t, srcDB, "INSERT INTO "+table+
		" SELECT g, 'row-'||g, now() - interval '2 hours' FROM generate_series(1,10) g")
	exec(t, dstDB, "DROP TABLE IF EXISTS "+table)
	exec(t, dstDB, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")

	store, jobID := intervalJob(t, srcDSN, dstDSN, "wm-late", table, "1h")
	if _, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID}); err != nil {
		t.Fatalf("第一轮失败: %v", err)
	}
	var wm string
	if err := dstDB.QueryRow(ctx, "SELECT COALESCE(watermark,'') FROM _jl_meta").Scan(&wm); err != nil {
		t.Fatal(err)
	}
	if wm == "" {
		t.Fatal("第一轮之后应有水位")
	}

	// 模拟长事务：这一行的 updated_at **比水位更早**（到达时水位已越过它），但此刻才可见。
	// 只有「水位 − lookback」的回退窗口能读到它；没有回退就永久漏读。
	exec(t, srcDB, "INSERT INTO "+table+
		" VALUES (999,'late-commit', now() - interval '2 hours' - interval '30 seconds')")

	res, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID})
	if err != nil {
		t.Fatalf("第二轮失败: %v", err)
	}
	if res.RowsStream == 0 {
		t.Fatal("回退窗口内的晚提交行没被读到——lookback 没生效（正是 Owl 指出的漏读 bug）")
	}
	var got int
	if err := dstDB.QueryRow(ctx, "SELECT count(*) FROM "+table+" WHERE id=999").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Fatal("晚提交的那一行必须出现在目标端")
	}
}

// intervalJob 建一条 interval 任务（lookback 可指定），返回 store 与 job id。
func intervalJob(t *testing.T, srcDSN, dstDSN, name, table, lookback string) (*control.Store, string) {
	t.Helper()
	ctx := context.Background()
	store, err := control.Open(control.Options{DataDir: t.TempDir(), Autogen: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	mk := func(nm string, port int, db, user, pass string) string {
		c, err := store.CreateConnection(ctx, control.ConnectionInput{
			Name: nm, Kind: "postgres", Host: "127.0.0.1", Port: port,
			Database: db, Username: user, Password: pass,
			Params: map[string]string{"sslmode": "disable"},
		})
		if err != nil {
			t.Fatal(err)
		}
		return c.ID
	}
	srcID := mk("src", portOf(t, srcDSN), dbOf(t, srcDSN), userOf(t, srcDSN), passOf(t, srcDSN))
	dstID := mk("dst", portOf(t, dstDSN), dbOf(t, dstDSN), userOf(t, dstDSN), passOf(t, dstDSN))
	job, err := store.CreateJob(ctx, control.JobInput{
		Name: name, SourceID: srcID, TargetID: dstID,
		Spec: control.JobDoc{
			Version: 1, Job: name,
			Source: control.SourceDoc{Kind: "postgres", Scope: &control.ScopeDoc{Schema: "public"}},
			Target: control.TargetDoc{Kind: "postgres", Schema: "public", OnExists: "append"},
			Mode: control.ModeDoc{Frequency: "interval", Interval: "1s",
				IncrementalColumn: "updated_at", Lookback: lookback},
			Content: &control.ContentDoc{Tables: map[string]control.TableDoc{table: {}}},
		},
	})
	if err != nil {
		t.Fatalf("建任务失败: %v", err)
	}
	trackJobSlots(t, srcDSN, store, job.ID)
	return store, job.ID
}

// Owl 的坑：PG 默认 `autovacuum_analyze_threshold = 50` 行，**几行的小维表永远不会被自动
// analyze**（reltuples 长期 -1）。若把「统计信息未知」一律拦死，这种表会让整条 interval
// 任务起不来——用户得先去 ANALYZE 一张 3 行的表。物理大小 < 8 MiB 时必须放行。
func TestTinyTableWithoutStatsIsAllowed(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	srcDB := mustConn(t, srcDSN)
	dstDB := mustConn(t, dstDSN)
	// 唯一表名：新建表一定是 reltuples=-1（从没 ANALYZE 过），前提可靠；
	// 也避免上一次运行留下的表被别人 analyze 过。
	table := fmt.Sprintf("wm_tiny_%d", time.Now().UnixNano()%1e9)
	ctx := context.Background()

	exec(t, srcDB, "DROP TABLE IF EXISTS "+table)
	exec(t, srcDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, name text)")
	exec(t, srcDB, "INSERT INTO "+table+" VALUES (1,'a'),(2,'b'),(3,'c')")
	exec(t, dstDB, "DROP TABLE IF EXISTS "+table)
	exec(t, dstDB, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")

	// 关键前提：新表且只有 3 行 → reltuples = -1（不会被自动 analyze）
	var est int64
	if err := srcDB.QueryRow(ctx,
		`SELECT c.reltuples::bigint FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
		 WHERE n.nspname='public' AND c.relname=$1`, table).Scan(&est); err != nil {
		t.Fatal(err)
	}
	if est >= 0 {
		// 不 Skip：一条会静默跳过的守护用例等于没有（今晚已经栽过「没跑到却报绿」）。
		t.Fatalf("构造不出「统计信息未知」前提：新建表 %s 的 reltuples=%d。"+
			"要么这台机器的 autovacuum 提前 analyze 了它，要么表名复用了旧表——请查清后再跑", table, est)
	}

	store, jobID := intervalJob(t, srcDSN, dstDSN, "wm-tiny", table, "1m")
	if _, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID}); err != nil {
		t.Fatalf("3 行的小表不该被拦（统计信息未知但物理很小）：%v", err)
	}
	var n int
	if err := dstDB.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("小表应整表搬过去，得到 %d 行", n)
	}
}

// DDL 第①步：源端的 DDL 必须真的落到目标端（之前 `pgsink.Apply` 直接跳过 EvDDL，
// 而引擎里也没有处理 DDL 的代码 → DDL 从来没搬过）。
// 同时钉住幂等：同一条 DDL 再来一次（崩溃重放）不许失败。
func TestDDLIsAppliedToTargetIdempotently(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	srcDB := mustConn(t, srcDSN)
	dstDB := mustConn(t, dstDSN)
	table := "ddl_follow"
	ctx := context.Background()

	exec(t, srcDB, "DROP TABLE IF EXISTS "+table)
	exec(t, srcDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, a text)")
	exec(t, dstDB, "DROP TABLE IF EXISTS "+table)
	exec(t, dstDB, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")

	ep, err := dsn.Parse(dstDSN)
	if err != nil {
		t.Fatal(err)
	}
	rel := source.Table{
		TableRef:   source.TableRef{Schema: "public", Table: table},
		Columns:    []source.Column{{Name: "id", Type: "bigint"}, {Name: "a", Type: "text"}},
		PrimaryKey: []string{"id"},
	}
	s, err := pgsink.Open(ctx, pgsink.Options{
		Target: ep, JobKey: "ddlfollow", JobName: "ddl-follow", InstanceID: "it-ddl",
		Schema: "public", OnExists: "append", Catalog: source.Catalog{Tables: []source.Table{rel}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.EnsureSchema(ctx, source.Catalog{Tables: []source.Table{rel}}); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	ref := source.TableRef{Schema: "public", Table: table}
	ddl := []source.Event{
		{Kind: source.EvDDL, Table: ref, Relation: &rel,
			DDL: "ALTER TABLE public." + table + " ADD COLUMN b text"},
		{Kind: source.EvDDL, Table: ref, Relation: &rel,
			DDL: "CREATE INDEX " + table + "_a_idx ON public." + table + " (a)"},
		{Kind: source.EvCommit},
	}
	if err := s.Apply(ctx, ddl, nil); err != nil {
		t.Fatalf("DDL 应被搬到目标端：%v", err)
	}

	// 1) 列 b 真的在
	var hasCol bool
	if err := dstDB.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_schema='public' AND table_name=$1 AND column_name='b')`, table).Scan(&hasCol); err != nil {
		t.Fatal(err)
	}
	if !hasCol {
		t.Fatal("源端加了列 b，目标端没有——DDL 没跟随")
	}
	// 2) 索引真的在（以前这种情况是**完全静默**）
	var hasIdx bool
	if err := dstDB.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname=$1)`,
		table+"_a_idx").Scan(&hasIdx); err != nil {
		t.Fatal(err)
	}
	if !hasIdx {
		t.Fatal("源端建了索引，目标端没有——建索引仍然被静默忽略")
	}
	// 3) 幂等：同一条 DDL 再来一次（崩溃重放）不许失败
	if err := s.Apply(ctx, ddl, nil); err != nil {
		t.Fatalf("重放同一条 DDL 应幂等跳过（42701/42P07），得到：%v", err)
	}
}

// 整条 P1 的**验收核心**：删除一个正在跑的任务 → 立刻重建同名任务 → 能跑起来（不是 E1005）。
// 这条不测「函数被调了」，测的是用户实际做的那串操作在真库上的结果。
func TestDeleteRunningJobThenRecreateRuns(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	srcDB := mustConn(t, srcDSN)
	dstDB := mustConn(t, dstDSN)
	table := "del_recreate"
	ctx := context.Background()

	exec(t, srcDB, "DROP TABLE IF EXISTS "+table)
	exec(t, srcDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, payload text)")
	exec(t, srcDB, "INSERT INTO "+table+" SELECT g,'r'||g FROM generate_series(1,500) g")
	exec(t, dstDB, "DROP TABLE IF EXISTS "+table)
	exec(t, dstDB, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")

	// 必须是**真的在跑**的任务：`interval` 会很快跑完（搬完就 done），
	// 那样测不到「跑着删掉」。用 continuous+initial，它会停在 stream 阶段等变更。
	store, jobID := continuousJob(t, srcDSN, dstDSN, "del-recreate", table)

	// 起一个「一直在跑」的同步（continuous+initial），然后删任务
	runCtx, stopRun := context.WithCancel(ctx)
	doneRun := make(chan struct{})
	go func() {
		defer close(doneRun)
		res, rerr := engine.Run(runCtx, engine.Options{Store: store, JobID: jobID})
		if rerr != nil {
			t.Logf("引擎提前结束：%v（阶段 %s，退出码 %d）", rerr, res.Phase, res.ExitCode)
		}
	}()
	// 等它真的开始写（目标表出现）
	waitFor(t, 10*time.Second, func() bool {
		var n int
		_ = dstDB.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n)
		return n > 0
	})

	job, err := store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	// ① 先确认租约**此刻真的被持有**（否则这条用例又是「跑得太快什么都没测到」）
	var held int
	if err := dstDB.QueryRow(ctx, "SELECT count(*) FROM _jl_lease WHERE job_key=$1", job.JobKey).Scan(&held); err != nil {
		t.Fatal(err)
	}
	if held == 0 {
		t.Fatalf("此刻目标端没有租约：说明任务没在跑，这条用例测不到「跑着删掉」")
	}

	// ② 跨进程那条路：删掉目标端租约行 → 那个正在跑的引擎续租失败 → 自己停下
	h, alive, lerr := pgsink.ForceReleaseLease(ctx, mustEndpoint(t, dstDSN), "public", job.JobKey)
	if lerr != nil {
		t.Fatalf("清目标端租约失败：%v", lerr)
	}
	if h == "" {
		t.Fatal("应删掉一条真实存在的租约（持有者描述不能为空）")
	}
	t.Logf("清掉的租约持有者=%q（进程还活着=%v）", h, alive)

	// ③ 引擎必须**自己**发现租约没了并退出（这是 #30 的语义，不是我们强杀）
	select {
	case <-doneRun:
	case <-time.After(25 * time.Second):
		t.Fatal("租约被删掉后引擎没有自己退出（`loseLease` 没生效？）")
	}
	stopRun()

	// ④ 等 run 行收尾 → 删任务（守卫会拦住未结束的 run）
	if err := settleForTest(ctx, store, job.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteJob(ctx, job.ID); err != nil {
		t.Fatalf("删除任务失败：%v", err)
	}

	// 目标端租约必须是空的（否则同名任务新建后会撞 E1005）
	var leases int
	if err := dstDB.QueryRow(ctx, "SELECT count(*) FROM _jl_lease").Scan(&leases); err != nil {
		t.Fatal(err)
	}
	if leases != 0 {
		t.Fatalf("删除后目标端还有 %d 条租约——同名任务会撞 E1005", leases)
	}

	// 立刻重建同名任务并跑：必须成功，且数据到齐
	store2, jobID2 := intervalJob(t, srcDSN, dstDSN, "del-recreate2", table, "1m")
	res, err := engine.Run(ctx, engine.Options{Store: store2, JobID: jobID2, SnapshotOnly: true})
	if err != nil {
		t.Fatalf("重建同名任务后应该能跑起来（而不是 E1005）：%v", err)
	}
	if res.ExitCode != 0 {
		t.Fatalf("重建后的 run 应成功，退出码 %d", res.ExitCode)
	}
	var n int
	if err := dstDB.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 500 {
		t.Fatalf("目标端应有 500 行，得到 %d", n)
	}
}

// waitFor 轮询直到条件成立或超时（集成用例等状态用）。
func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("等了 %s 条件仍未成立", d)
}

// mustEndpoint 把 DSN 解析成 endpoint（测试用）。
func mustEndpoint(t *testing.T, raw string) dsn.Endpoint {
	t.Helper()
	ep, err := dsn.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return ep
}

// settleForTest 等这个任务的 run 收尾；等不到就替它标 interrupted（与 CLI 同一套语义）。
func settleForTest(ctx context.Context, store *control.Store, jobID string) error {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		active, err := store.ActiveRuns(ctx)
		if err != nil {
			return err
		}
		var mine []control.Run
		for _, r := range active {
			if r.JobID == jobID {
				mine = append(mine, r)
			}
		}
		if len(mine) == 0 {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	active, _ := store.ActiveRuns(ctx)
	for _, r := range active {
		if r.JobID == jobID {
			_, _ = store.FinishRun(ctx, r.ID, state.Failed, "测试：删除任务时强制收尾", 1, control.FinishInterrupted)
		}
	}
	return nil
}

// continuousJob 建一条 continuous+initial 的任务（会一直跑着，用来测「跑着删掉」）。
func continuousJob(t *testing.T, srcDSN, dstDSN, name, table string) (*control.Store, string) {
	t.Helper()
	ctx := context.Background()
	store, err := control.Open(control.Options{DataDir: t.TempDir(), Autogen: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	mk := func(nm string, port int, db, user, pass string) string {
		c, err := store.CreateConnection(ctx, control.ConnectionInput{
			Name: nm, Kind: "postgres", Host: "127.0.0.1", Port: port,
			Database: db, Username: user, Password: pass,
			Params: map[string]string{"sslmode": "disable"},
		})
		if err != nil {
			t.Fatal(err)
		}
		return c.ID
	}
	srcID := mk("src", portOf(t, srcDSN), dbOf(t, srcDSN), userOf(t, srcDSN), passOf(t, srcDSN))
	dstID := mk("dst", portOf(t, dstDSN), dbOf(t, dstDSN), userOf(t, dstDSN), passOf(t, dstDSN))
	job, err := store.CreateJob(ctx, control.JobInput{
		Name: name, SourceID: srcID, TargetID: dstID,
		Spec: control.JobDoc{
			Version: 1, Job: name,
			Source:  control.SourceDoc{Kind: "postgres", Scope: &control.ScopeDoc{Schema: "public"}},
			Target:  control.TargetDoc{Kind: "postgres", Schema: "public", OnExists: "append"},
			Mode:    control.ModeDoc{Frequency: "continuous+initial"},
			Content: &control.ContentDoc{Tables: map[string]control.TableDoc{table: {}}},
		},
	})
	if err != nil {
		t.Fatalf("建任务失败: %v", err)
	}
	trackJobSlots(t, srcDSN, store, job.ID)
	return store, job.ID
}

// DDL 第②步：源端加列之后，**带新 Relation 的行必须能写进去**（先对齐列再写行）。
// 一张表的 DDL 不该把整个任务打死——之前这种行会让整批失败、任务 failed。
func TestDDLAddColumnThenRowsSucceed(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	srcDB := mustConn(t, srcDSN)
	dstDB := mustConn(t, dstDSN)
	table := "ddl_align"
	ctx := context.Background()

	exec(t, srcDB, "DROP TABLE IF EXISTS "+table)
	exec(t, srcDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, a text)")
	exec(t, srcDB, "INSERT INTO "+table+" VALUES (1,'x')")
	exec(t, dstDB, "DROP TABLE IF EXISTS "+table)
	exec(t, dstDB, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")

	ref := source.TableRef{Schema: "public", Table: table}
	oldRel := source.Table{
		TableRef:   ref,
		Columns:    []source.Column{{Name: "id", Type: "bigint"}, {Name: "a", Type: "text"}},
		PrimaryKey: []string{"id"},
	}
	s, err := pgsink.Open(ctx, pgsink.Options{
		Target: mustEndpoint(t, dstDSN), JobKey: "ddlalign", JobName: "ddl-align",
		InstanceID: "it-ddl-align", Schema: "public", OnExists: "append",
		Catalog: source.Catalog{Tables: []source.Table{oldRel}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.EnsureSchema(ctx, source.Catalog{Tables: []source.Table{oldRel}}); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// 先把旧结构的行写进去（模拟「全量已经搬过」），否则下面「旧行还在」没有基数
	if err := s.Apply(ctx, []source.Event{
		{Kind: source.EvInsert, Table: ref, Relation: &oldRel,
			New: source.Row{Table: ref, Values: []source.Value{
				{Kind: source.KindString, S: "1"}, {Kind: source.KindString, S: "x"}}}},
		{Kind: source.EvCommit},
	}, nil); err != nil {
		t.Fatalf("先写旧行失败：%v", err)
	}

	// 源端加列；随后的行带着新的 Relation（3 列）
	exec(t, srcDB, "ALTER TABLE "+table+" ADD COLUMN b text")
	newRel := source.Table{
		TableRef: ref,
		Columns: []source.Column{
			{Name: "id", Type: "bigint"}, {Name: "a", Type: "text"}, {Name: "b", Type: "text"},
		},
		PrimaryKey: []string{"id"},
	}
	evs := []source.Event{
		{Kind: source.EvDDL, Table: ref, Relation: &newRel,
			DDL: "ALTER TABLE public." + table + " ADD COLUMN b text"},
		{Kind: source.EvInsert, Table: ref, Relation: &newRel,
			// 行值与生产路径一致：逻辑解码出来的是**文本**，占位符是 `$n::text::<type>`。
			New: source.Row{Table: ref, Values: []source.Value{
				{Kind: source.KindString, S: "2"},
				{Kind: source.KindString, S: "y"},
				{Kind: source.KindString, S: "new"},
			}}},
		{Kind: source.EvCommit},
	}
	if err := s.Apply(ctx, evs, nil); err != nil {
		t.Fatalf("源端加列之后的行必须能写进去（先对齐列再写行）：%v", err)
	}

	var got string
	if err := dstDB.QueryRow(ctx, "SELECT COALESCE(b,'（空）') FROM "+table+" WHERE id=2").Scan(&got); err != nil {
		t.Fatalf("目标端没有列 b：%v", err)
	}
	if got != "new" {
		t.Fatalf("新列的值应搬过去，得到 %q", got)
	}
	// 旧行还在（只加列、不动数据）
	var n int
	if err := dstDB.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("应有 2 行（旧 1 + 新 1），得到 %d", n)
	}
}

// DDL 必改：**列数没变但类型变了**不许走「一切正常」那条路。
// 早退只比列数时，改类型会让缓存留着旧类型、写行仍用旧口径——静默写错，比原来整批失败更坏。
func TestDDLTypeChangeIsRefusedNotSilent(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	srcDB := mustConn(t, srcDSN)
	dstDB := mustConn(t, dstDSN)
	table := "ddl_typechange"
	ctx := context.Background()

	exec(t, srcDB, "DROP TABLE IF EXISTS "+table)
	exec(t, srcDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, v bigint)")
	exec(t, dstDB, "DROP TABLE IF EXISTS "+table)
	exec(t, dstDB, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")

	ref := source.TableRef{Schema: "public", Table: table}
	oldRel := source.Table{TableRef: ref,
		Columns:    []source.Column{{Name: "id", Type: "bigint"}, {Name: "v", Type: "bigint"}},
		PrimaryKey: []string{"id"}}
	s, err := pgsink.Open(ctx, pgsink.Options{
		Target: mustEndpoint(t, dstDSN), JobKey: "ddltype", JobName: "ddl-type",
		InstanceID: "it-ddl-type", Schema: "public", OnExists: "append",
		Catalog: source.Catalog{Tables: []source.Table{oldRel}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.EnsureSchema(ctx, source.Catalog{Tables: []source.Table{oldRel}}); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	// 列数不变、类型变了（bigint → text）
	newRel := source.Table{TableRef: ref,
		Columns:    []source.Column{{Name: "id", Type: "bigint"}, {Name: "v", Type: "text"}},
		PrimaryKey: []string{"id"}}
	err = s.Apply(ctx, []source.Event{
		{Kind: source.EvInsert, Table: ref, Relation: &newRel,
			New: source.Row{Table: ref, Values: []source.Value{
				{Kind: source.KindString, S: "1"}, {Kind: source.KindString, S: "abc"}}}},
		{Kind: source.EvCommit},
	}, nil)
	if err == nil {
		t.Fatal("列类型变了必须响亮拒绝（不能安静按旧类型写）")
	}
	var te *pgsink.TypeChangedError
	if !errors.As(err, &te) {
		t.Fatalf("应返回 TypeChangedError（第③步要按类型降级成「只暂停该表」），得到 %T: %v", err, err)
	}
	// ① 方向必须无歧义。这条路（streaming）两端都是**源端**类型：旧 → 新。
	if !strings.Contains(err.Error(), "源端原来是 bigint") || !strings.Contains(err.Error(), "现在是 text") {
		t.Fatalf("streaming 路要标清「源端原来/现在」各是什么：%v", err)
	}
	// ② typed error 只带事实：动作与风险交给码表（否则同一条错误两段动作）
	for _, unwanted := range []string{"你能做什么", "风险"} {
		if strings.Contains(err.Error(), unwanted) {
			t.Fatalf("动作/风险应由码表给，typed error 里不该有 %q：%v", unwanted, err)
		}
	}
}

// 改名同样不是「列数没变就没事」：必须带列名报出来，而不是按名找不到下标时丢一个模糊错。
func TestDDLRenameIsReported(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	srcDB := mustConn(t, srcDSN)
	dstDB := mustConn(t, dstDSN)
	table := "ddl_rename"
	ctx := context.Background()

	exec(t, srcDB, "DROP TABLE IF EXISTS "+table)
	exec(t, srcDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, a text)")
	exec(t, dstDB, "DROP TABLE IF EXISTS "+table)
	exec(t, dstDB, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")

	ref := source.TableRef{Schema: "public", Table: table}
	oldRel := source.Table{TableRef: ref,
		Columns:    []source.Column{{Name: "id", Type: "bigint"}, {Name: "a", Type: "text"}},
		PrimaryKey: []string{"id"}}
	s, err := pgsink.Open(ctx, pgsink.Options{
		Target: mustEndpoint(t, dstDSN), JobKey: "ddlrename", JobName: "ddl-rename",
		InstanceID: "it-ddl-rename", Schema: "public", OnExists: "append",
		Catalog: source.Catalog{Tables: []source.Table{oldRel}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.EnsureSchema(ctx, source.Catalog{Tables: []source.Table{oldRel}}); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	newRel := source.Table{TableRef: ref,
		Columns:    []source.Column{{Name: "id", Type: "bigint"}, {Name: "b", Type: "text"}},
		PrimaryKey: []string{"id"}}
	err = s.Apply(ctx, []source.Event{
		{Kind: source.EvInsert, Table: ref, Relation: &newRel,
			New: source.Row{Table: ref, Values: []source.Value{
				{Kind: source.KindString, S: "1"}, {Kind: source.KindString, S: "x"}}}},
		{Kind: source.EvCommit},
	}, nil)
	if err == nil {
		t.Fatal("列改名必须报错")
	}
	// 改名只输出「原→现」配对，不再重复两条单边说法
	if !strings.Contains(err.Error(), "原 a → 现 b") {
		t.Fatalf("错误里要有「原 a → 现 b」这种配对：%v", err)
	}
	for _, dup := range []string{"原 a 已不存在", "新增 b"} {
		if strings.Contains(err.Error(), dup) {
			t.Fatalf("有配对时不该再重复单边说法 %q：%v", dup, err)
		}
	}
	var sd *pgsink.SchemaDriftError
	if !errors.As(err, &sd) {
		t.Fatalf("应返回 SchemaDriftError（第③步要按类型降级成只暂停该表），得到 %T", err)
	}
	if sd.Reason() != pgsink.PauseReasonSchemaDrift {
		t.Fatalf("原因码应为 %q，得到 %q", pgsink.PauseReasonSchemaDrift, sd.Reason())
	}
}

// 同一件事的两个时机必须给同一个原因码：ensureSchema（prepare 时）发现目标列类型与源端
// 不一致，和跑起来时发现类型变了，都是「这张表的类型对不上」。
// 以前前者是裸哨兵 ErrTypeMismatch → 整任务死，后者是 TypeChangedError → 暂停该表，
// 用户会当成两个 bug。
func TestEnsureSchemaTypeMismatchIsPausable(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	srcDB := mustConn(t, srcDSN)
	dstDB := mustConn(t, dstDSN)
	table := "type_mismatch_prep"
	ctx := context.Background()

	exec(t, srcDB, "DROP TABLE IF EXISTS "+table)
	exec(t, srcDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, v bigint)")
	// 目标端故意建一张「列名同、类型不同」的表
	exec(t, dstDB, "DROP TABLE IF EXISTS "+table)
	exec(t, dstDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, v text)")
	exec(t, dstDB, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")

	ref := source.TableRef{Schema: "public", Table: table}
	rel := source.Table{TableRef: ref,
		Columns:    []source.Column{{Name: "id", Type: "bigint"}, {Name: "v", Type: "bigint"}},
		PrimaryKey: []string{"id"}}
	s, err := pgsink.Open(ctx, pgsink.Options{
		Target: mustEndpoint(t, dstDSN), JobKey: "typemismatchprep", JobName: "tm",
		InstanceID: "it-tm", Schema: "public", OnExists: "append",
		Catalog: source.Catalog{Tables: []source.Table{rel}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// 按表暂停落地后：`EnsureSchema` **不再在第一张不一致的表上返回错误**——它把不一致收进
	// `TakePendingPauses()`，由引擎统一登记暂停、其它表照常（契约 §6 硬口径 2）。
	// 所以这里断言的是「收得到那张表 + 原因码对」，而不是「返回一个 error」。
	if _, err := s.EnsureSchema(ctx, source.Catalog{Tables: []source.Table{rel}}); err != nil {
		t.Fatalf("结构对不上不该让 EnsureSchema 整轮失败（其它表还要跑）: %v", err)
	}
	pending := s.TakePendingPauses()
	if len(pending) != 1 {
		t.Fatalf("应收到 1 张待暂停的表，得到 %d：%v", len(pending), pending)
	}
	pe := pending[0]
	// 再取一次必须为空（交出去就清空，防止同一张表被登记两遍）
	if again := s.TakePendingPauses(); len(again) != 0 {
		t.Fatalf("TakePendingPauses 必须交出并清空，第二次还拿到 %v", again)
	}
	if pe.Table() != table {
		t.Fatalf("要能直接拿到表名，得到 %q", pe.Table())
	}
	if pe.Reason() != pgsink.PauseReasonTypeChanged {
		t.Fatalf("原因码应与「跑起来时发现类型变了」一致（%q），得到 %q",
			pgsink.PauseReasonTypeChanged, pe.Reason())
	}
}

// prepare 期目标列类型与源端不一致 → **只暂停这张表**，run 正常收尾（契约 §1.2/§1.5）。
//
// 这条用例翻过面：原先是「整轮失败 + runs.code=E1015」。按表暂停落地后，prepare 期的不一致
// 由 `EnsureSchema` 收进 `TakePendingPauses` 交给引擎登记，其它表照跑、位点照前进。
// 现在钉的是：run 不失败、记录里点名这张表与原因码、**这张表一行都不写**（存量也不搬）。
//
// 这张目标表是测试自己建的（不在 `_jl_object` 里）→ `resume=manual`：引擎不许自动清空它。
func TestSchemaMismatchPausesOnlyThatTable(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	srcDB := mustConn(t, srcDSN)
	dstDB := mustConn(t, dstDSN)
	table := "type_change_run"
	ctx := context.Background()

	exec(t, srcDB, "DROP TABLE IF EXISTS "+table)
	exec(t, srcDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, v bigint)")
	// 目标端故意建「列名同、类型不同」的表：EnsureSchema 会撞上类型不一致
	exec(t, dstDB, "DROP TABLE IF EXISTS "+table)
	exec(t, dstDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, v text)")
	exec(t, dstDB, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")

	store, jobID := onceJobSingleTable(t, srcDSN, dstDSN, "typechange", table)
	if _, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, SnapshotOnly: true}); err != nil {
		t.Fatalf("一张表结构对不上不该打死整轮（其它表要继续跑）: %v", err)
	}
	job, err := store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	var raw string
	if err := dstDB.QueryRow(ctx, "SELECT COALESCE(paused_tables,'') FROM _jl_meta WHERE job_key=$1",
		job.JobKey).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var rec map[string]pgsink.PausedTable
	if err := json.Unmarshal([]byte(raw), &rec); err != nil {
		t.Fatalf("paused_tables 不是合法 JSON（%q）：%v", raw, err)
	}
	one, ok := rec[table]
	if !ok {
		t.Fatalf("这张表必须被登记为暂停，得到 %q", raw)
	}
	if one.Reason != pgsink.PauseReasonTypeChanged {
		t.Fatalf("原因码应为 %q，得到 %q", pgsink.PauseReasonTypeChanged, one.Reason)
	}
	if pgsink.NormalizedResume(one.Resume) != pgsink.ResumeManual {
		t.Fatalf("测试自己建的目标表不在 _jl_object 里 → 必须按 manual 处置（不许自动清空），得到 %q", one.Resume)
	}
	// 存量也不搬：这张表一行都不该出现
	var n int
	if err := dstDB.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("暂停表连存量都不搬（契约 §6 硬口径 2），实际写了 %d 行", n)
	}
}

// onceJobSingleTable 建一条 mode=once、只选一张表的任务（本文件的用例自用）。
func onceJobSingleTable(t *testing.T, srcDSN, dstDSN, name, table string) (*control.Store, string) {
	t.Helper()
	ctx := context.Background()
	store, err := control.Open(control.Options{DataDir: t.TempDir(), Autogen: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	mk := func(nm string, port int, db, user, pass string) string {
		c, err := store.CreateConnection(ctx, control.ConnectionInput{
			Name: nm, Kind: "postgres", Host: "127.0.0.1", Port: port,
			Database: db, Username: user, Password: pass,
			Params: map[string]string{"sslmode": "disable"},
		})
		if err != nil {
			t.Fatal(err)
		}
		return c.ID
	}
	srcID := mk("src", portOf(t, srcDSN), dbOf(t, srcDSN), userOf(t, srcDSN), passOf(t, srcDSN))
	dstID := mk("dst", portOf(t, dstDSN), dbOf(t, dstDSN), userOf(t, dstDSN), passOf(t, dstDSN))
	job, err := store.CreateJob(ctx, control.JobInput{
		Name: name, SourceID: srcID, TargetID: dstID,
		Spec: control.JobDoc{
			Version: 1, Job: name,
			Source:  control.SourceDoc{Kind: "postgres", Scope: &control.ScopeDoc{Schema: "public"}},
			Target:  control.TargetDoc{Kind: "postgres", Schema: "public", OnExists: "append"},
			Mode:    control.ModeDoc{Frequency: "once"},
			Content: &control.ContentDoc{Tables: map[string]control.TableDoc{table: {}}},
		},
	})
	if err != nil {
		t.Fatalf("建任务失败: %v", err)
	}
	trackJobSlots(t, srcDSN, store, job.ID)
	return store, job.ID
}

// EnsureSchema 必须早于 cleanBeforeRecopy：清表要清的表得先存在。
//
// 构造：目标端**表不存在**，但状态行说「搬过、有位点」（老状态）→ 这一轮判成
// 「从头搬 + 要清表」。顺序要是反了，清表这一步会撞 "relation does not exist"；
// 顺序对了就是「先建表 → 再清（空表）→ 再搬」。
func TestCleanRunsAfterEnsureSchema(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	srcDB := mustConn(t, srcDSN)
	dstDB := mustConn(t, dstDSN)
	table := "ensure_before_clean"
	ctx := context.Background()

	exec(t, srcDB, "DROP TABLE IF EXISTS "+table)
	exec(t, srcDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, payload text)")
	exec(t, srcDB, "INSERT INTO "+table+" SELECT g,'r'||g FROM generate_series(1,200) g")

	exec(t, dstDB, "DROP TABLE IF EXISTS "+table)
	exec(t, dstDB, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")

	store, jobID := onceJobSingleTable(t, srcDSN, dstDSN, "ensure-clean", table)
	ensureStateTables(t, store, dstDSN, jobID)
	job, err := store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	// 老状态：有位点、阶段说搬过，但块账一条都没有、目标表也不在。
	exec(t, dstDB, `INSERT INTO _jl_meta (job_key,name,instance_id,phase,position)
		VALUES ($1,$2,'test','streaming','postgres:v1:0/10')
		ON CONFLICT (job_key) DO UPDATE SET phase='streaming', position='postgres:v1:0/10'`,
		job.JobKey, job.Name)

	// 前提不许含混：此刻目标表确实不存在、块账确实为空（否则这条用例测不到顺序）。
	var tables0, markers0 int
	if err := dstDB.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables
		WHERE table_schema='public' AND table_name=$1`, table).Scan(&tables0); err != nil {
		t.Fatal(err)
	}
	if err := dstDB.QueryRow(ctx, `SELECT count(*) FROM _jl_chunk`).Scan(&markers0); err != nil {
		t.Fatal(err)
	}
	if tables0 != 0 || markers0 != 0 {
		t.Fatalf("前提不成立：目标表存在=%d、块账条数=%d（都应为 0）", tables0, markers0)
	}

	// 订阅事件：这一轮**必须真的清了表**，否则这条用例根本没走到那个顺序上。
	ch, stopSub := store.Bus().Subscribe(jobID)
	defer stopSub()
	notes := make(chan string, 512)
	go func() {
		for ev := range ch {
			if ev.Message != "" {
				notes <- ev.Message
			}
		}
	}()

	if _, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, SnapshotOnly: true}); err != nil {
		t.Fatalf("目标表不存在但需要清表时，必须先 EnsureSchema 建表再清：%v", err)
	}
	close(notes)
	cleaned := false
	for msg := range notes {
		if strings.Contains(msg, "清空了") {
			cleaned = true
		}
	}
	if !cleaned {
		t.Fatal("这一轮应当清表（老状态 + 有位点 → 从头搬先清），事件里没有「清空了」")
	}
	var n int
	if err := dstDB.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 200 {
		t.Fatalf("应搬 200 行，得到 %d", n)
	}
	t.Logf("先建表再清表：目标表从「不存在」到 200 行，指纹 %s", tableFingerprint(t, dstDB, table))
}

// tableFingerprint 是这套用例通用的内容指纹（行数 + 内容哈希），两端同一条 SQL。
func tableFingerprint(t *testing.T, conn *pgx.Conn, table string) string {
	t.Helper()
	var fp string
	q := "SELECT count(*)::text || ':' || COALESCE(sum(hashtextextended(t::text,0)),0)::text FROM " + table + " t"
	if err := conn.QueryRow(context.Background(), q).Scan(&fp); err != nil {
		t.Fatalf("算 %s 的指纹失败: %v", table, err)
	}
	return fp
}

// countRowsIn 数一张表的行数（按名拼 SQL，仅测试用表名）。
func countRowsIn(t *testing.T, conn *pgx.Conn, table string) int {
	t.Helper()
	var n int
	if err := conn.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("数 %s 的行数失败: %v", table, err)
	}
	return n
}

// columnType 读目标端列的 data_type（改类型前后各查一次，用来证明两端一致）。
func columnType(t *testing.T, conn *pgx.Conn, table, col string) string {
	t.Helper()
	var typ string
	if err := conn.QueryRow(context.Background(),
		`SELECT data_type FROM information_schema.columns
		 WHERE table_schema='public' AND table_name=$1 AND column_name=$2`,
		table, col).Scan(&typ); err != nil {
		t.Fatalf("读 %s.%s 的类型失败: %v", table, col, err)
	}
	return typ
}

// DDL 改类型这条路的端到端证据（A 切片：契约与引擎路径，不含按表暂停）。
//
// 走的是「错误文案给的那条路」：源端改类型 → 本版**明确拒绝**（E1015，点名表与原因码）
// → 用户把目标端列类型改成与源端一致 → 重跑 → 两端类型一致、数据指纹一致 →
// 之后的增/改/删照样追平。三件事各自有判据，缺一条这条用例就该红。
func TestTypeChangeAlignThenIncrementalWorks(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	srcDB := mustConn(t, srcDSN)
	dstDB := mustConn(t, dstDSN)
	table := "type_align_inc"
	ctx := context.Background()

	exec(t, srcDB, "DROP TABLE IF EXISTS "+table)
	exec(t, srcDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, payload text, v bigint)")
	exec(t, srcDB, "INSERT INTO "+table+" SELECT g,'r'||g,g FROM generate_series(1,1000) g")
	exec(t, dstDB, "DROP TABLE IF EXISTS "+table)
	exec(t, dstDB, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")

	store, jobID := continuousJob(t, srcDSN, dstDSN, "type-align-inc", table)

	// ① 首搬：两端列类型一致（改类型**前**的那一次）
	if _, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, SnapshotOnly: true}); err != nil {
		t.Fatalf("首搬失败: %v", err)
	}
	srcType0, dstType0 := columnType(t, srcDB, table, "v"), columnType(t, dstDB, table, "v")
	if srcType0 != dstType0 {
		t.Fatalf("首搬后两端列类型应一致：源 %s / 目标 %s", srcType0, dstType0)
	}
	srcFP0 := tableFingerprint(t, srcDB, table)
	if dstFP0 := tableFingerprint(t, dstDB, table); dstFP0 != srcFP0 {
		t.Fatalf("首搬后指纹不一致：源 %s / 目标 %s", srcFP0, dstFP0)
	}

	// ② 源端改类型（bigint → numeric(12,2)）
	exec(t, srcDB, "ALTER TABLE "+table+" ALTER COLUMN v TYPE numeric(12,2)")

	// ③ 本版不跟随改类型：**只暂停这张表**（不是整轮失败），run 正常收尾
	if _, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, SnapshotOnly: true}); err != nil {
		t.Fatalf("一张表改了列类型不该打死整轮: %v", err)
	}
	job, jerr := store.GetJob(ctx, jobID)
	if jerr != nil {
		t.Fatal(jerr)
	}
	var rawPaused string
	if err := dstDB.QueryRow(ctx, "SELECT COALESCE(paused_tables,'') FROM _jl_meta WHERE job_key=$1",
		job.JobKey).Scan(&rawPaused); err != nil {
		t.Fatal(err)
	}
	var recPaused map[string]pgsink.PausedTable
	if err := json.Unmarshal([]byte(rawPaused), &recPaused); err != nil {
		t.Fatalf("paused_tables 不是合法 JSON（%q）：%v", rawPaused, err)
	}
	one, ok := recPaused[table]
	if !ok {
		t.Fatalf("改类型的表必须被登记暂停，得到 %q", rawPaused)
	}
	if one.Reason != pgsink.PauseReasonTypeChanged {
		t.Fatalf("原因码应为 %q，得到 %q", pgsink.PauseReasonTypeChanged, one.Reason)
	}
	// 这张目标表是首搬时引擎自己建的（在 _jl_object 里）→ recopy：对齐后必须整表重搬
	if pgsink.NormalizedResume(one.Resume) != pgsink.ResumeRecopy {
		t.Fatalf("本任务建的表应走 recopy（对齐后整表重搬），得到 %q", one.Resume)
	}
	// 暂停期间表还是旧结构、旧数据：一行新数据都不该写进去
	if n := countRowsIn(t, dstDB, table); n != 1000 {
		t.Fatalf("暂停期间不许写这张表：期望 1000 行（首搬的），实际 %d 行", n)
	}

	// ④ 按文案给的下一步：把目标端列类型改成与源端一致，再跑一次
	exec(t, dstDB, "ALTER TABLE "+table+" ALTER COLUMN v TYPE numeric(12,2)")
	if _, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, SnapshotOnly: true}); err != nil {
		t.Fatalf("两边类型对齐后重跑应成功: %v", err)
	}
	srcType1, dstType1 := columnType(t, srcDB, table, "v"), columnType(t, dstDB, table, "v")
	if srcType1 != dstType1 {
		t.Fatalf("改类型后两端列类型应一致：源 %s / 目标 %s", srcType1, dstType1)
	}
	srcFP1 := tableFingerprint(t, srcDB, table)
	if dstFP1 := tableFingerprint(t, dstDB, table); dstFP1 != srcFP1 {
		t.Fatalf("改类型后指纹不一致：源 %s / 目标 %s", srcFP1, dstFP1)
	}

	// ⑤ 改类型后增量仍可用：一次增/改/删，量「源端提交 → 目标端可见」的时延
	exec(t, srcDB, "INSERT INTO "+table+" (id,payload,v) VALUES (1001,'ins',1.50)")
	exec(t, srcDB, "UPDATE "+table+" SET payload='upd', v=9.99 WHERE id=1")
	exec(t, srcDB, "DELETE FROM "+table+" WHERE id=2")
	committed := time.Now()

	seen := make(chan time.Duration, 1)
	go func() {
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			var n int
			var p1 string
			var has1001, has2 int
			err := dstDB.QueryRow(ctx, `SELECT count(*),
				COALESCE((SELECT payload FROM `+table+` WHERE id=1),''),
				(SELECT count(*) FROM `+table+` WHERE id=1001),
				(SELECT count(*) FROM `+table+` WHERE id=2) FROM `+table).Scan(&n, &p1, &has1001, &has2)
			if err == nil && n == 1000 && p1 == "upd" && has1001 == 1 && has2 == 0 {
				seen <- time.Since(committed)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
		seen <- -1
	}()

	if _, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, IdleAfter: 800 * time.Millisecond}); err != nil {
		t.Fatalf("改类型后的增量应能跑（追平即收尾）: %v", err)
	}
	latency := <-seen
	if latency < 0 {
		t.Fatal("30 秒内没追平：改类型之后增量不可用")
	}
	if fp := tableFingerprint(t, dstDB, table); fp != tableFingerprint(t, srcDB, table) {
		t.Fatalf("追平后指纹不一致：目标 %s", fp)
	}
	t.Logf("改类型前 %s/%s → 改类型后 %s/%s；指纹 %s → %s；暂停原因码=%s（resume=%s）；"+
		"对齐后整表重搬并解除；改类型后增/改/删追平时延 %.0fms",
		srcType0, dstType0, srcType1, dstType1, srcFP0, srcFP1, one.Reason, one.Resume,
		float64(latency.Microseconds())/1000)
}

// 端到端：选了一张不存在的表 → `runs.code` 必须是 E1013（不是空、不是「未分类」）。
// 这条是 `runs.code` 那半件事的关键：列有了、写入点有了，如果最常见的配置错误
// 产生不出码，等于只做了一半。
func TestMissingTableRunGetsE1013(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	srcDB := mustConn(t, srcDSN)
	dstDB := mustConn(t, dstDSN)
	ctx := context.Background()
	exec(t, srcDB, "DROP TABLE IF EXISTS ghost_table_xyz")

	store, jobID := tableJob(t, srcDSN, dstDSN, "ghost-job", "ghost_table_xyz")
	// 「位点没动」要可证伪、不靠读代码：跑前跑后 position 都为空（或没有状态行）。
	job0, jerr := store.GetJob(ctx, jobID)
	if jerr != nil {
		t.Fatal(jerr)
	}
	posBefore := metaPosition(t, dstDB, job0.JobKey)
	if posBefore != "" {
		t.Fatalf("还没开始搬就不该有位点，得到 %q", posBefore)
	}
	res, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, SnapshotOnly: true})
	if err == nil {
		t.Fatal("选不存在的表必须失败")
	}
	if res.ExitCode == 0 {
		t.Fatal("失败时退出码不能是 0")
	}
	if posAfter := metaPosition(t, dstDB, job0.JobKey); posAfter != posBefore {
		t.Fatalf("表在源端清单里找不到：位点不该动（%q → %q）", posBefore, posAfter)
	}
	run, gerr := store.GetRun(ctx, res.RunID)
	if gerr != nil {
		t.Fatal(gerr)
	}
	if run.Code != string(core.CodeObjectMissing) {
		t.Fatalf("runs.code 应为 %s（我们自己判定的配置错误），得到 %q；last_error=%s",
			core.CodeObjectMissing, run.Code, run.LastError)
	}
	if !strings.Contains(run.LastError, "ghost_table_xyz") {
		t.Fatalf("last_error 要点名那张表：%s", run.LastError)
	}
}

// tableJob 建一条 mode=once、只选一张表（可能不存在）的任务。
func tableJob(t *testing.T, srcDSN, dstDSN, name, table string) (*control.Store, string) {
	t.Helper()
	ctx := context.Background()
	store, err := control.Open(control.Options{DataDir: t.TempDir(), Autogen: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	mk := func(nm string, port int, db, user, pass string) string {
		c, err := store.CreateConnection(ctx, control.ConnectionInput{
			Name: nm, Kind: "postgres", Host: "127.0.0.1", Port: port,
			Database: db, Username: user, Password: pass,
			Params: map[string]string{"sslmode": "disable"},
		})
		if err != nil {
			t.Fatal(err)
		}
		return c.ID
	}
	srcID := mk("src", portOf(t, srcDSN), dbOf(t, srcDSN), userOf(t, srcDSN), passOf(t, srcDSN))
	dstID := mk("dst", portOf(t, dstDSN), dbOf(t, dstDSN), userOf(t, dstDSN), passOf(t, dstDSN))
	job, err := store.CreateJob(ctx, control.JobInput{
		Name: name, SourceID: srcID, TargetID: dstID,
		Spec: control.JobDoc{
			Version: 1, Job: name,
			Source:  control.SourceDoc{Kind: "postgres", Scope: &control.ScopeDoc{Schema: "public"}},
			Target:  control.TargetDoc{Kind: "postgres", Schema: "public", OnExists: "append"},
			Mode:    control.ModeDoc{Frequency: "once"},
			Content: &control.ContentDoc{Tables: map[string]control.TableDoc{table: {}}},
		},
	})
	if err != nil {
		t.Fatalf("建任务失败: %v", err)
	}
	return store, job.ID
}

// metaPosition 读目标端 _jl_meta.position（没有状态行时返回空串）。
func metaPosition(t *testing.T, conn *pgx.Conn, jobKey string) string {
	t.Helper()
	var pos string
	if err := conn.QueryRow(context.Background(),
		`SELECT COALESCE(position,'') FROM _jl_meta WHERE job_key=$1`, jobKey).Scan(&pos); err != nil {
		return "" // 没有状态行：也算「位点为空」
	}
	return pos
}
