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
			name:    "postgres 取搜索路径第一个 schema，不是数据库名",
			ep:      dsn.Endpoint{Kind: dsn.KindPostgres, Database: "jinliang_test"},
			schemas: []string{"public", "report"},
			want:    "public",
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
