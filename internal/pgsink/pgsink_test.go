package pgsink

import (
	"context"
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/source"
)

func testCatalog() source.Catalog {
	return source.Catalog{Tables: []source.Table{
		{
			TableRef: source.TableRef{Schema: "public", Table: "orders"},
			Columns: []source.Column{
				{Name: "id", Type: "bigint"},
				{Name: "payload", Type: "text"},
				{Name: "amount", Type: "numeric(12,2)"},
			},
			PrimaryKey:  []string{"id"},
			HasStablePK: true,
		},
		{
			TableRef: source.TableRef{Schema: "public", Table: "kv"},
			Columns: []source.Column{
				{Name: "k", Type: "text"},
				{Name: "v", Type: "text"},
			},
			PrimaryKey:  []string{"k"},
			HasStablePK: true,
		},
		{
			TableRef: source.TableRef{Schema: "public", Table: "no_pk"},
			Columns: []source.Column{
				{Name: "a", Type: "integer"},
			},
			HasStablePK: false,
		},
	}}
}

func TestBuildTablesIndexesAndPK(t *testing.T) {
	tables, order := buildTables(testCatalog())
	if len(order) != 3 {
		t.Fatalf("应索引 3 张表，得到 %d", len(order))
	}
	ti := tables["public.orders"]
	if ti == nil || !ti.hasPK || len(ti.pkIdx) != 1 || ti.pkIdx[0] != 0 {
		t.Fatalf("orders 的主键解析不对: %+v", ti)
	}
	if ti.colIndex["amount"] != 2 {
		t.Fatalf("列序应与 catalog 一致: %+v", ti.colIndex)
	}
	if tables["public.no_pk"].hasPK {
		t.Fatal("无主键表不该被标成有主键")
	}
}

// 主键列不在列清单里（权限不足读不到）时按无主键处理，不要发一个必然失败的
// ON CONFLICT。
func TestBuildTablesFallsBackWhenPKColumnMissing(t *testing.T) {
	cat := source.Catalog{Tables: []source.Table{{
		TableRef:   source.TableRef{Schema: "public", Table: "t"},
		Columns:    []source.Column{{Name: "a", Type: "integer"}},
		PrimaryKey: []string{"missing"},
	}}}
	tables, _ := buildTables(cat)
	if tables["public.t"].hasPK {
		t.Fatal("主键列读不到时必须降级为无主键")
	}
}

func TestConflictClause(t *testing.T) {
	tables, _ := buildTables(testCatalog())
	if got := conflictClause(tables["public.orders"]); !strings.Contains(got, `ON CONFLICT ("id") DO UPDATE SET`) ||
		!strings.Contains(got, `"payload" = EXCLUDED."payload"`) {
		t.Fatalf("orders 的 upsert 尾句不对: %s", got)
	}
	if got := conflictClause(tables["public.no_pk"]); got != "" {
		t.Fatalf("无主键表不该有 ON CONFLICT，得到 %q", got)
	}
	allPK := &tableInfo{cols: []string{"a", "b"}, pk: []string{"a", "b"}, hasPK: true}
	if got := conflictClause(allPK); !strings.Contains(got, "DO NOTHING") {
		t.Fatalf("整行都是主键时应为 DO NOTHING，得到 %q", got)
	}
}

// 增量路径必须把参数显式转成列类型：pgoutput 给的是文本，
// bytea / 数组 / json 只有经列类型的输入函数才能正确落地。
func TestCastedHoldersUseColumnTypes(t *testing.T) {
	tables, _ := buildTables(testCatalog())
	got := castedHolders(tables["public.orders"])
	want := `$1::text::bigint, $2::text::text, $3::text::numeric(12,2)`
	if got != want {
		t.Fatalf("参数占位符不对:\n 得到 %s\n 期望 %s", got, want)
	}
}

func TestQuoteListQuotesEveryName(t *testing.T) {
	if got := quoteList([]string{"id", `we"ird`}); got != `"id", "we""ird"` {
		t.Fatalf("标识符转义不对: %s", got)
	}
}

func TestCompatibleTypeAcceptsAliases(t *testing.T) {
	cases := [][2]string{
		{"integer", "int4"},
		{"character varying(50)", "varchar(50)"},
		{"timestamp with time zone", "timestamptz"},
		{"boolean", "bool"},
		{"numeric(12,2)", "decimal(12,2)"},
	}
	for _, c := range cases {
		if !compatibleType(c[0], c[1]) {
			t.Fatalf("%s 与 %s 应该视为同一类型", c[0], c[1])
		}
	}
	if compatibleType("bigint", "text") {
		t.Fatal("bigint 与 text 不该视为兼容")
	}
}

func TestCreateTableSQL(t *testing.T) {
	tables, _ := buildTables(testCatalog())
	got := (&Sink{}).createTableSQL(tables["public.orders"])
	want := `CREATE TABLE "public"."orders" ("id" bigint, "payload" text, "amount" numeric(12,2), PRIMARY KEY ("id"))`
	if got != want {
		t.Fatalf("建表语句不对:\n 得到 %s\n 期望 %s", got, want)
	}
}

// 跨库搬 text 列必须带上排序规则：否则唯一索引判重与 ORDER BY 会静默走偏。
func TestCreateTableCarriesCollation(t *testing.T) {
	cat := source.Catalog{Tables: []source.Table{{
		TableRef: source.TableRef{Schema: "public", Table: "t"},
		Columns: []source.Column{
			{Name: "id", Type: "bigint"},
			{Name: "name", Type: "text", Collation: "C"},
			{Name: "note", Type: "text"},
		},
		PrimaryKey:  []string{"id"},
		HasStablePK: true,
	}}}
	tables, _ := buildTables(cat)
	got := (&Sink{}).createTableSQL(tables["public.t"])
	want := `CREATE TABLE "public"."t" ("id" bigint, "name" text COLLATE "C", "note" text, PRIMARY KEY ("id"))`
	if got != want {
		t.Fatalf("建表语句应带排序规则:\n 得到 %s\n 期望 %s", got, want)
	}
}

// ValidateBatch 必须跑在**碰数据库之前**：非法批次整批不落，不能先写一半。
func TestApplyRejectsInvalidBatchBeforeTouchingTarget(t *testing.T) {
	s := &Sink{} // 没有连接：只要碰到 conn 就会 panic，能证明校验在前
	err := s.Apply(context.Background(), []source.Event{{Kind: source.EvBegin}}, nil)
	if err == nil {
		t.Fatal("没有 EvCommit 收尾的批次必须被拒绝")
	}
	if !strings.Contains(err.Error(), "事务边界") {
		t.Fatalf("错误应说明是事务边界问题，得到 %v", err)
	}
}
