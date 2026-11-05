//go:build integration

package engine_test

// 真库集成测试：只有带 -tags=integration 才编译/运行（CI 的 b1-engine job 会带这个 tag）。
//
// 缺 DSN 时**直接失败**而不是跳过：跳过＝静默通过，这个仓库不接受。
// 变量名以 scripts/e2e_headless.sh 与 docs/BUILD.md 为准：
//   JINLIANG_TEST_PG_SRC_DSN（源）/ JINLIANG_TEST_PG_DSN（目标）

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/engine"
	"github.com/spencerswagger/jinliang/internal/pgsink"
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

// 这是 Owl 要的那条可证伪用例：
// 目标端预置 3 行（用户自己的表，append 模式复用它）→ 上次全量搬到一半（phase=snapshot、有块标记）
// → 重启重搬 → **那 3 行必须还在**。
//
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
