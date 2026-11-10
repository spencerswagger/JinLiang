package pgsink

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

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
	tables, order := buildTables(testCatalog(), "")
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
	tables, _ := buildTables(cat, "")
	if tables["public.t"].hasPK {
		t.Fatal("主键列读不到时必须降级为无主键")
	}
}

func TestConflictClause(t *testing.T) {
	tables, _ := buildTables(testCatalog(), "")
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
	tables, _ := buildTables(testCatalog(), "")
	got := castedHoldersFor(tables["public.orders"], 1)
	want := `($1::text::bigint, $2::text::text, $3::text::numeric(12,2))`
	if got != want {
		t.Fatalf("参数占位符不对:\n 得到 %s\n 期望 %s", got, want)
	}
}

// 多行占位必须**行优先**递增，且与参数追加顺序一一对应：错一位不会报错，
// 只会把值写进别的列。
func TestCastedHoldersForMultipleRows(t *testing.T) {
	tables, _ := buildTables(testCatalog(), "")
	ti := tables["public.kv"] // 两列：k, v
	got := castedHoldersFor(ti, 3)
	want := `($1::text::text, $2::text::text), ($3::text::text, $4::text::text), ($5::text::text, $6::text::text)`
	if got != want {
		t.Fatalf("三行两列的占位不对:\n 得到 %s\n 期望 %s", got, want)
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
	tables, _ := buildTables(testCatalog(), "")
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
	tables, _ := buildTables(cat, "")
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

// 每批行数必须按「参数总数 ≤ 65535」钳制：否则 10 列宽表上 100000 行/批 = 100 万参数，
// 每批必败、位点永不推进、任务永久卡死，而报错跟数据毫无关系。
func TestEffectiveBatchRowsClampedByParamLimit(t *testing.T) {
	cases := []struct {
		requested, cols, want int
	}{
		{0, 3, DefaultApplyBatchRows},        // 未配置 → 默认
		{500, 3, 500},                        // 正常范围不动
		{100000, 10, maxBindParams / 10},     // 10 列 → 6553
		{100000, 200, maxBindParams / 200},   // 宽表 → 327
		{100000, 1600, maxBindParams / 1600}, // PG 单表上限 1600 列（Open 已拦，这里验算术）
		{100000, 0, 100000},                  // 列数未知时不钳制（调用方会另行报错）
	}
	for _, c := range cases {
		got := effectiveBatchRows(c.requested, c.cols)
		if got != c.want {
			t.Fatalf("requested=%d cols=%d → %d，期望 %d", c.requested, c.cols, got, c.want)
		}
		if c.cols > 0 && got*c.cols > maxBindParams {
			t.Fatalf("钳制后参数总数 %d 仍超过 %d", got*c.cols, maxBindParams)
		}
	}
}

// 钳制必须留下日志：否则用户只看到「每批都失败」，看不出是自己把批大小配大了。
func TestBatchLimitClampIsLogged(t *testing.T) {
	// once 是包级的（interval 每轮新建 Sink 也不重复打），测试里重置一次才能观察输出。
	prev := batchLimitLogOnce
	batchLimitLogOnce = &sync.Once{}
	t.Cleanup(func() { batchLimitLogOnce = prev })

	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	s := &Sink{opt: Options{ApplyBatchRows: 100000}}
	limit := effectiveBatchRows(s.opt.ApplyBatchRows, 10)
	s.noteBatchLimit(&tableInfo{cols: make([]string, 10)}, limit)

	got := buf.String()
	if !strings.Contains(got, "6553") {
		t.Fatalf("日志里应有生效行数 6553，实际: %q", got)
	}
	t.Logf("钳制日志: %s", strings.TrimSpace(got))
	if !strings.Contains(got, "65535") || !strings.Contains(got, "配置问题") {
		t.Fatalf("日志要说清是参数上限与配置问题: %q", got)
	}
}

// 列太多是真实会撞的边界（PG 单表上限 1600）：在建连之前拦，并给出 exclude 的做法。
func TestColumnBudgetRejectedBeforeConnecting(t *testing.T) {
	cols := make([]source.Column, maxPGColumnsPerTable+1)
	for i := range cols {
		cols[i] = source.Column{Name: fmt.Sprintf("c%d", i), Type: "text"}
	}
	cat := source.Catalog{Tables: []source.Table{{
		TableRef: source.TableRef{Schema: "public", Table: "wide"},
		Columns:  cols,
	}}}
	err := checkColumnBudget(cat)
	if err == nil {
		t.Fatal("1601 列必须被拦住")
	}
	for _, want := range []string{"1600", "exclude", "风险"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("错误里应有 %q: %v", want, err)
		}
	}
	// 正好 1600 列放行。
	if err := checkColumnBudget(source.Catalog{Tables: []source.Table{{
		TableRef: source.TableRef{Table: "wide"}, Columns: cols[:maxPGColumnsPerTable],
	}}}); err != nil {
		t.Fatalf("1600 列不该被拦: %v", err)
	}
}

// 约束名为空时不能渲染成「SQLSTATE 23514（）：」那种空括号。
func TestSQLErrorWithoutConstraintName(t *testing.T) {
	e := &pgconn.PgError{Code: "23505", Message: "duplicate key"}
	got := describeSQLError(e)
	if strings.Contains(got, "（）") {
		t.Fatalf("空约束名不该渲染空括号: %s", got)
	}
	if !strings.Contains(got, "23505") || !strings.Contains(got, "duplicate key") {
		t.Fatalf("应保留码与消息: %s", got)
	}
}

// compatibleType 的「无参数」分支：**无界按最大处理**。
// 之前写反过一次：目标 numeric(12,2) / 源端无界 numeric 被判兼容 → PG 静默四舍五入、
// 退出 0、数据已变形；反过来目标 timestamp(默认 6) / 源端 timestamp(6) 又判成假不兼容。
func TestCompatibleTypeUnboundedArgs(t *testing.T) {
	cases := []struct {
		have, want string
		ok         bool
		why        string
	}{
		{"numeric", "numeric(12,3)", true, "目标无界装得下源端带参"},
		{"numeric(12,2)", "numeric", false, "目标有界、源端无界 → 可能收窄 → 不兼容"},
		{"timestamp", "timestamp(6)", true, "目标 timestamp(默认 6 位) 装得下 6 位"},
		{"timestamp(0)", "timestamp(6)", false, "6 位进 0 位 → 截断微秒"},
		{"varchar", "varchar(60)", true, "目标无长度上限"},
		{"varchar(20)", "varchar(60)", false, "收窄"},
		{"varchar(60)", "varchar(20)", true, "放宽放行"},
	}
	for _, c := range cases {
		if got := compatibleType(c.have, c.want); got != c.ok {
			t.Errorf("compatibleType(%q, %q) = %v，应为 %v（%s）", c.have, c.want, got, c.ok, c.why)
		}
	}
}
