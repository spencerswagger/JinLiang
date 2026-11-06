package control

import (
	"testing"

	"github.com/spencerswagger/jinliang/internal/dsn"
)

// 没显式选 schema 时的默认值：MySQL 必须落回 DSN 里那个库。
// 之前的实现在 MySQL 上取 Schemas()[0]，会把别的库的表列出来（e2e 里选中的是
// 字母序第一个库，导致「catalog 没有返回任何表」）。
func TestDefaultCatalogSchema(t *testing.T) {
	cases := []struct {
		name    string
		ep      dsn.Endpoint
		schemas []string
		want    string
	}{
		{
			name:    "mysql 用 DSN 里的库，不看别的库",
			ep:      dsn.Endpoint{Kind: dsn.KindMySQL, Database: "jinliang_test"},
			schemas: []string{"jinliang_src", "jinliang_test"},
			want:    "jinliang_test",
		},
		{
			name:    "mysql 没写库名时退回第一个可见库",
			ep:      dsn.Endpoint{Kind: dsn.KindMySQL},
			schemas: []string{"a", "b"},
			want:    "a",
		},
		{
			name:    "postgres 优先 public",
			ep:      dsn.Endpoint{Kind: dsn.KindPostgres, Database: "jinliang_test"},
			schemas: []string{"public", "report"},
			want:    "public",
		},
		{
			// 回归：`ORDER BY nspname` 会把 jl_* / app 排到 public 前面，
			// 于是向导默认列出一个空 schema，用户看到「这个库没有表」。
			name:    "postgres 即使 public 不在第一个也选它",
			ep:      dsn.Endpoint{Kind: dsn.KindPostgres, Database: "jinliang_test"},
			schemas: []string{"jl_e2e_out", "public"},
			want:    "public",
		},
		{
			name:    "postgres 没有 public 时退回第一个",
			ep:      dsn.Endpoint{Kind: dsn.KindPostgres},
			schemas: []string{"app", "report"},
			want:    "app",
		},
		{
			name: "什么都没有时兜底 public",
			ep:   dsn.Endpoint{Kind: dsn.KindPostgres},
			want: "public",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := defaultCatalogSchema(c.ep, c.schemas); got != c.want {
				t.Fatalf("got %q, want %q", got, c.want)
			}
		})
	}
}
