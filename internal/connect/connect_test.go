package connect

import (
	"context"
	"os"
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
