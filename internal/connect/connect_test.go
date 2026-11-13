package connect

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/source"
)

// testEndpoint reads a DSN from the environment (never hard-coded: the four
// test DSNs are injected by CI and by the dev sandbox).
func testEndpoint(t *testing.T, env string) (dsn.Endpoint, bool) {
	t.Helper()
	raw := os.Getenv(env)
	if raw == "" {
		return dsn.Endpoint{}, false
	}
	ep, err := dsn.Parse(raw)
	if err != nil {
		t.Fatalf("%s 解析失败: %v", env, err)
	}
	// 测试库没有配证书链，显式降级；生产默认仍是 verify-full。
	if ep.Kind == dsn.KindPostgres {
		if _, ok := ep.Params["sslmode"]; !ok {
			ep.Params["sslmode"] = "disable"
		}
	}
	return ep, true
}

func TestPostgresProbeAndCatalog(t *testing.T) {
	ep, ok := testEndpoint(t, "JINLIANG_TEST_PG_DSN")
	if !ok {
		t.Skip("JINLIANG_TEST_PG_DSN 未设置")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	res := Test(ctx, ep, 10*time.Second)
	if !res.OK {
		t.Fatalf("测连失败: %s (%s) %s", res.Message, res.Step, res.RawError)
	}
	if res.ServerVersion == "" {
		t.Fatal("测连必须返回服务端版本")
	}
	if len(res.Capabilities) == 0 {
		t.Fatal("测连必须给出能力探测结果")
	}

	conn, err := Open(ep)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	cat, err := conn.Catalog(ctx, source.Scope{Schema: "public"})
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	for _, tbl := range cat.Tables {
		if len(tbl.Columns) == 0 {
			t.Fatalf("表 %s 没有列信息", tbl.Table)
		}
		if tbl.Schema != "public" {
			t.Fatalf("表 %s 的 schema = %q", tbl.Table, tbl.Schema)
		}
	}
}

func TestMySQLProbeAndCatalog(t *testing.T) {
	ep, ok := testEndpoint(t, "JINLIANG_TEST_MYSQL_DSN")
	if !ok {
		t.Skip("JINLIANG_TEST_MYSQL_DSN 未设置")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	res := Test(ctx, ep, 10*time.Second)
	if !res.OK {
		t.Fatalf("测连失败: %s (%s) %s", res.Message, res.Step, res.RawError)
	}
	if res.ServerVersion == "" {
		t.Fatal("测连必须返回服务端版本")
	}
	conn, err := Open(ep)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Catalog(ctx, source.Scope{Schema: ep.Database}); err != nil {
		t.Fatalf("catalog: %v", err)
	}
}

func TestUnreachableEndpointGivesHumanError(t *testing.T) {
	ep := dsn.Endpoint{
		Kind: "postgres", Host: "127.0.0.1", Port: 1,
		Database: "nope", Username: "nobody", Password: "secret",
		Params: map[string]string{"sslmode": "disable"},
	}
	res := Test(context.Background(), ep, 3*time.Second)
	if res.OK {
		t.Fatal("port 1 must not connect")
	}
	if res.Message == "" {
		t.Fatal("失败必须给出人话原因")
	}
	if containsAny(res.Message, "secret") {
		t.Fatalf("错误信息泄露了口令: %s", res.Message)
	}
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if len(sub) > 0 && len(s) >= len(sub) {
			for i := 0; i+len(sub) <= len(s); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
		}
	}
	return false
}

// UniqueKeys 只收「完整列、非主键唯一索引」：部分索引、表达式索引、前缀索引都不算
// （docs/DESIGN-NO-PRIMARY-KEY.md §3.4 判据 ①②④；列可空性归预检判）。
func TestPostgresCatalogCollectsUniqueIndexes(t *testing.T) {
	ep, ok := testEndpoint(t, "JINLIANG_TEST_PG_DSN")
	if !ok {
		t.Skip("JINLIANG_TEST_PG_DSN 未设置")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	db, err := sql.Open("pgx", ep.PostgresURL())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const table = "jl_uk_probe"
	drop := "DROP TABLE IF EXISTS public." + table
	if _, err := db.ExecContext(ctx, drop); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(ctx, drop) //nolint:errcheck
	stmts := []string{
		"CREATE TABLE public." + table + " (a int NOT NULL, b text NOT NULL, c text, note text)",
		"CREATE UNIQUE INDEX jl_uk_ab ON public." + table + " (a, b)",
		"CREATE UNIQUE INDEX jl_uk_c ON public." + table + " (c)",                                 // 可空列：照收，由预检判
		"CREATE UNIQUE INDEX jl_uk_partial ON public." + table + " (note) WHERE note IS NOT NULL", // 部分索引：不收
		"CREATE UNIQUE INDEX jl_uk_expr ON public." + table + " (lower(note))",                    // 表达式索引：不收
	}
	for _, q := range stmts {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	conn, err := Open(ep)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	cat, err := conn.Catalog(ctx, source.Scope{Schema: "public"})
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	var got [][]string
	for _, tbl := range cat.Tables {
		if tbl.Table == table {
			got = tbl.UniqueKeys
		}
	}
	want := map[string]bool{"a,b": true, "c": true}
	if len(got) != len(want) {
		t.Fatalf("应收到 2 组唯一键（a,b 与 c），实际 %v", got)
	}
	for _, key := range got {
		joined := strings.Join(key, ",")
		if !want[joined] {
			t.Fatalf("多收了不该收的唯一索引 %v（部分/表达式索引必须滤掉）", key)
		}
		delete(want, joined)
	}
	if len(want) != 0 {
		t.Fatalf("漏了唯一索引 %v", want)
	}
}

func TestMySQLCatalogCollectsUniqueIndexes(t *testing.T) {
	ep, ok := testEndpoint(t, "JINLIANG_TEST_MYSQL_DSN")
	if !ok {
		t.Skip("JINLIANG_TEST_MYSQL_DSN 未设置")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	db, err := sql.Open("mysql", ep.MySQLConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const table = "jl_uk_probe"
	drop := "DROP TABLE IF EXISTS `" + table + "`"
	if _, err := db.ExecContext(ctx, drop); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(ctx, drop) //nolint:errcheck
	stmts := []string{
		"CREATE TABLE `" + table + "` (a int NOT NULL, b varchar(16) NOT NULL, c varchar(64), d text," +
			" UNIQUE KEY uk_ab (a,b), UNIQUE KEY uk_c (c), UNIQUE KEY uk_prefix (c(8)), UNIQUE KEY uk_text (d(10)))",
	}
	for _, q := range stmts {
		if _, err := db.ExecContext(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	conn, err := Open(ep)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	cat, err := conn.Catalog(ctx, source.Scope{Schema: ep.Database})
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	var got [][]string
	for _, tbl := range cat.Tables {
		if tbl.Table == table {
			got = tbl.UniqueKeys
		}
	}
	want := map[string]bool{"a,b": true, "c": true}
	if len(got) != len(want) {
		t.Fatalf("应收到 2 组唯一键（a,b 与 c），实际 %v", got)
	}
	for _, key := range got {
		joined := strings.Join(key, ",")
		if !want[joined] {
			t.Fatalf("多收了不该收的唯一索引 %v（前缀索引必须滤掉）", key)
		}
		delete(want, joined)
	}
	if len(want) != 0 {
		t.Fatalf("漏了唯一索引 %v", want)
	}
}
