//go:build integration

package engine_test

// MySQL 源 → PostgreSQL 目标端的真库集成测试（CI 的 b1-engine job 带 -tags=integration 跑）。
//
// 缺 DSN 时**直接失败**而不是跳过：跳过＝静默通过，这个仓库不接受。
//   JINLIANG_TEST_MYSQL_SRC_DSN（MySQL 源）/ JINLIANG_TEST_PG_DSN（PG 目标）
//
// 这条用例是「MySQL 源接进引擎主路径」在 CI 里的证据：真连 binlog、真写 PG、
// 真按块记账、真追平删除。只跑单测证明不了 binlog 解码、类型映射、事件表名映射这几段是对的。

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/engine"
)

func mysqlDSNs(t *testing.T) (src, dst string) {
	t.Helper()
	src = os.Getenv("JINLIANG_TEST_MYSQL_SRC_DSN")
	dst = os.Getenv("JINLIANG_TEST_PG_DSN")
	if src == "" || dst == "" {
		t.Fatalf("缺 DSN：需要 JINLIANG_TEST_MYSQL_SRC_DSN 与 JINLIANG_TEST_PG_DSN（缺了就是失败，不跳过）")
	}
	return src, dst
}

func mustMySQL(t *testing.T, raw string) *sql.DB {
	t.Helper()
	c := myConfigOf(t, raw)
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&loc=UTC&charset=utf8mb4",
		c.user, c.pass, c.host, c.port, c.db))
	if err != nil {
		t.Fatalf("连不上 MySQL 源: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("MySQL 源 ping 失败: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func myExec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), q, args...); err != nil {
		t.Fatalf("MySQL 执行失败 %q: %v", q, err)
	}
}

type myConn struct {
	host, db, user, pass string
	port                 int
}

// myConfigOf 解析 mysql://user:pass@host:port/db（集成测试里的 DSN 就是这个形状）。
func myConfigOf(t *testing.T, raw string) myConn {
	t.Helper()
	s := strings.TrimPrefix(raw, "mysql://")
	at := strings.LastIndex(s, "@")
	if at < 0 {
		t.Fatalf("MySQL DSN 里没有 @（%q）", raw)
	}
	creds, rest := s[:at], s[at+1:]
	hostPort, dbPart := rest, ""
	if i := strings.IndexByte(rest, '/'); i >= 0 {
		hostPort, dbPart = rest[:i], rest[i+1:]
	}
	if i := strings.IndexByte(dbPart, '?'); i >= 0 {
		dbPart = dbPart[:i]
	}
	host, port := hostPort, 3306
	if i := strings.LastIndex(hostPort, ":"); i >= 0 {
		host = hostPort[:i]
		fmt.Sscanf(hostPort[i+1:], "%d", &port)
	}
	user, pass := creds, ""
	if i := strings.IndexByte(creds, ':'); i >= 0 {
		user, pass = creds[:i], creds[i+1:]
	}
	return myConn{host: host, port: port, db: dbPart, user: user, pass: pass}
}

// newMySQLJob 在临时控制库里建「MySQL 源 → PG 目标」的任务。
func newMySQLJob(t *testing.T, srcDSN, dstDSN, jobName, table, freq string) (*control.Store, string) {
	t.Helper()
	dir := t.TempDir()
	store, err := control.Open(control.Options{DataDir: dir, Autogen: true})
	if err != nil {
		t.Fatalf("打开控制库失败: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	ctx := context.Background()

	my := myConfigOf(t, srcDSN)
	sc, err := store.CreateConnection(ctx, control.ConnectionInput{
		Name: "src", Kind: "mysql", Host: my.host, Port: my.port,
		Database: my.db, Username: my.user, Password: my.pass,
	})
	if err != nil {
		t.Fatalf("建 MySQL 源连接失败: %v", err)
	}
	tc, err := store.CreateConnection(ctx, control.ConnectionInput{
		Name: "dst", Kind: "postgres", Host: "127.0.0.1", Port: portOf(t, dstDSN),
		Database: dbOf(t, dstDSN), Username: userOf(t, dstDSN), Password: passOf(t, dstDSN),
		Params: map[string]string{"sslmode": "disable"},
	})
	if err != nil {
		t.Fatalf("建 PG 目标连接失败: %v", err)
	}
	job, err := store.CreateJob(ctx, control.JobInput{
		Name: jobName, SourceID: sc.ID, TargetID: tc.ID,
		Spec: control.JobDoc{
			Version: 1, Job: jobName,
			Source:  control.SourceDoc{Kind: "mysql", Scope: &control.ScopeDoc{Schema: my.db}},
			Target:  control.TargetDoc{Kind: "postgres", Schema: "public", OnExists: "append"},
			Mode:    control.ModeDoc{Frequency: freq},
			Content: &control.ContentDoc{Tables: map[string]control.TableDoc{table: {}}},
		},
	})
	if err != nil {
		t.Fatalf("建任务失败: %v", err)
	}
	return store, job.ID
}

func pgCount(t *testing.T, conn *pgx.Conn, table string) int {
	t.Helper()
	var n int
	if err := conn.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("数 %s 失败: %v", table, err)
	}
	return n
}

// 主用例：全量按块搬（块记账与 PG 源共用）+ 增量（增/改/删）追平 + 重跑不重搬。
func TestMySQLSourceFullCopyAndIncremental(t *testing.T) {
	srcDSN, dstDSN := mysqlDSNs(t)
	my := mustMySQL(t, srcDSN)
	dst := mustConn(t, dstDSN)
	table := "engine_mysql_orders"

	myExec(t, my, "DROP TABLE IF EXISTS "+table)
	myExec(t, my, "CREATE TABLE "+table+` (
		id BIGINT NOT NULL PRIMARY KEY,
		payload VARCHAR(64) NOT NULL,
		amount DECIMAL(12,2) NULL,
		created_at DATETIME(6) NULL
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`)
	myExec(t, my, "INSERT INTO "+table+` (id,payload,amount,created_at) VALUES
		(1,'a',1.10,'2026-01-01 00:00:01.000001'),
		(2,'b',2.20,'2026-01-01 00:00:02.000002'),
		(3,'c',3.30,'2026-01-01 00:00:03.000003')`)

	exec(t, dst, "DROP TABLE IF EXISTS "+table)
	exec(t, dst, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")

	store, jobID := newMySQLJob(t, srcDSN, dstDSN, "mysql-engine", table, "continuous+initial")
	ensureStateTables(t, store, dstDSN, jobID)

	res, err := engine.Run(context.Background(), engine.Options{
		Store: store, JobID: jobID, StopAfter: 3 * time.Second,
	})
	if err != nil {
		t.Fatalf("MySQL 源全量失败: %v", err)
	}
	if res.RowsCopied != 3 {
		t.Fatalf("应复制 3 行，实际 %d（阶段 %s）", res.RowsCopied, res.Phase)
	}
	// 块记账必须真的写进去了：MySQL 源与 PG 源共用同一套 `_jl_chunk` 语义。
	var chunks int
	if err := dst.QueryRow(context.Background(),
		"SELECT count(*) FROM _jl_chunk WHERE table_name=$1 AND done", table).Scan(&chunks); err != nil {
		t.Fatalf("读块进度失败: %v", err)
	}
	if chunks == 0 {
		t.Fatal("全量完成后 _jl_chunk 里应有这张表的完成标记（块记账与 PG 源共用）")
	}
	if got := pgCount(t, dst, table); got != 3 {
		t.Fatalf("目标端应有 3 行，实际 %d", got)
	}

	// 增量：改一笔、删一笔、加一笔，跑到追平为止。
	myExec(t, my, "UPDATE "+table+" SET payload='b-updated', amount=9.99 WHERE id=2")
	myExec(t, my, "DELETE FROM "+table+" WHERE id=3")
	myExec(t, my, "INSERT INTO "+table+" (id,payload,amount,created_at) VALUES (4,'d',4.40,'2026-01-01 00:00:04.000004')")

	res2, err := engine.Run(context.Background(), engine.Options{
		Store: store, JobID: jobID, StopAfter: 8 * time.Second,
	})
	if err != nil {
		t.Fatalf("增量轮失败: %v", err)
	}
	if res2.RowsCopied != 0 {
		t.Fatalf("续跑不该再搬全量，实际复制了 %d 行", res2.RowsCopied)
	}
	if got := pgCount(t, dst, table); got != 3 {
		t.Fatalf("增量后目标端应有 3 行（4 行 - 删掉 1 行），实际 %d", got)
	}
	var payload string
	if err := dst.QueryRow(context.Background(),
		"SELECT payload FROM "+table+" WHERE id=2").Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if payload != "b-updated" {
		t.Fatalf("id=2 应被更新成 b-updated，实际 %q", payload)
	}
	var gone int
	if err := dst.QueryRow(context.Background(),
		"SELECT count(*) FROM "+table+" WHERE id=3").Scan(&gone); err != nil {
		t.Fatal(err)
	}
	if gone != 0 {
		t.Fatal("源端删掉的 id=3 必须从目标端消失（MySQL 源的删除事件必须带删除前的值）")
	}
	var added int
	if err := dst.QueryRow(context.Background(),
		"SELECT count(*) FROM "+table+" WHERE id=4").Scan(&added); err != nil {
		t.Fatal(err)
	}
	if added != 1 {
		t.Fatal("源端新增的 id=4 必须出现在目标端")
	}
}
