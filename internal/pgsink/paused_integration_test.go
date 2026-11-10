//go:build integration

package pgsink

// 暂停记录的两条硬约束，必须能用真库证伪：
//  1. 只删一个 key —— 删 a 之后 b 的记录**仍在**（防「拿内存表集合重建整列」）；
//  2. 写路径**不做**暂停判定：sink 收到什么写什么（判定权在引擎，契约 §1.5）。

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/spencerswagger/jinliang/internal/source"
)

func TestPausedTableOnlyClearsOneKey(t *testing.T) {
	ctx := context.Background()
	table := "paused_clear"
	// 上一次跑崩可能留下同名表/状态行：先清干净（用独立连接，先于 sink 打开）。
	dropPausedFixtures(t, table)
	s := openPausedSink(t, table, "pausedclear")
	if _, err := s.EnsureSchema(ctx, pausedCatalog(table)); err != nil {
		t.Fatalf("建目标表失败: %v", err)
	}
	// 状态行要先存在（生产路径里 prepare 的 EnsureMeta 干这件事）。
	if err := s.EnsureMeta(ctx, "h", "", "", ""); err != nil {
		t.Fatalf("建状态行失败: %v", err)
	}
	a := source.TableRef{Schema: "public", Table: "a_orders"}
	b := source.TableRef{Schema: "public", Table: "b_orders"}

	if err := s.PauseTable(ctx, a, PausedTable{Reason: PauseReasonTypeChanged, Detail: "列 v：目标端当前是 text，源端是 bigint"}); err != nil {
		t.Fatal(err)
	}
	atA := time.Now()
	time.Sleep(1100 * time.Millisecond) // RFC3339 是秒级：至少要跨一秒才能看出「没被刷新」
	if err := s.PauseTable(ctx, b, PausedTable{Reason: PauseReasonSchemaDrift, Detail: "第 2 列：原 v → 现 w"}); err != nil {
		t.Fatal(err)
	}
	// 再登记一次 a：reason/detail 更新，但 at 必须还是第一次那个
	if err := s.PauseTable(ctx, a, PausedTable{Reason: PauseReasonTypeChanged, Detail: "列 v：目标端当前是 text，源端是 numeric(12,2)"}); err != nil {
		t.Fatal(err)
	}
	rec, err := s.PausedTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rec) != 2 {
		t.Fatalf("应有 2 条暂停记录，得到 %v", rec)
	}
	firstAt := rec[a.Table].At
	if firstAt == "" || time.Since(atA) > time.Minute {
		t.Fatalf("a 的 at 应是第一次登记的那个，得到 %q", firstAt)
	}

	// 只删 a：b 的记录必须原样留着（含 at 与 detail）
	if err := s.ClearPausedTable(ctx, a); err != nil {
		t.Fatal(err)
	}
	rec, err = s.PausedTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := rec[a.Table]; ok {
		t.Fatalf("a 的记录应已删除，仍有：%v", rec)
	}
	gotB, ok := rec[b.Table]
	if !ok {
		t.Fatalf("删 a 不该动 b —— 这是「整列覆盖」的判别点，得到 %v", rec)
	}
	if gotB.Reason != PauseReasonSchemaDrift || gotB.Detail != "第 2 列：原 v → 现 w" || gotB.At == "" {
		t.Fatalf("b 的记录被改动了：%+v", gotB)
	}
	// 库里存的就是这份形状（不靠内存）
	raw, err := s.PausedTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	blob, _ := json.Marshal(raw)
	if !json.Valid(blob) {
		t.Fatalf("paused_tables 不是合法 JSON：%s", blob)
	}
	t.Logf("只删一个 key：删 a 之后 b 记录仍在（reason=%s, at=%s）", gotB.Reason, gotB.At)
}

// pausedSchema 是这两个用例专用的 schema：`_jl_*` 状态表放在这里，
// 不会和并发跑的 engine 集成用例（public._jl_*）互相 DROP。
const pausedSchema = "paused_it"

// pausedInsertEvents 造一批写进专用 schema 的插入事件。
func pausedInsertEvents(table string, rows int) []source.Event {
	ref := source.TableRef{Schema: pausedSchema, Table: table}
	evs := []source.Event{{Kind: source.EvBegin, TxID: "paused-tx"}}
	for i := 1; i <= rows; i++ {
		evs = append(evs, source.Event{
			Kind: source.EvInsert, TxID: "paused-tx", Table: ref,
			New: source.Row{Table: ref, Values: []source.Value{
				{Kind: source.KindString, S: strconv.Itoa(i)},
				{Kind: source.KindString, S: "p" + strconv.Itoa(i)},
			}},
		})
	}
	return append(evs, source.Event{Kind: source.EvCommit, TxID: "paused-tx"})
}

func pausedCatalog(table string) source.Catalog {
	return source.Catalog{Tables: []source.Table{{
		TableRef: source.TableRef{Schema: pausedSchema, Table: table},
		Columns: []source.Column{
			{Name: "id", Type: "bigint"},
			{Name: "payload", Type: "text"},
		},
		PrimaryKey:  []string{"id"},
		HasStablePK: true,
	}}}
}

// openPausedSink 在专用 schema 里开一个 sink（先建 schema）。
func openPausedSink(t *testing.T, table, jobKey string) *Sink {
	t.Helper()
	conn := openRaw(t)
	if _, err := conn.Exec(context.Background(), "CREATE SCHEMA IF NOT EXISTS "+pausedSchema); err != nil {
		t.Fatalf("建专用 schema 失败: %v", err)
	}
	conn.Close(context.Background())
	return openSinkFor(t, pausedCatalog(table), func(o *Options) {
		o.JobKey = jobKey
		o.Schema = pausedSchema
	})
}

// dropPausedFixtures 清掉本用例可能留下的表（幂等；必须在 sink 打开之前）。
func dropPausedFixtures(t *testing.T, table string) {
	t.Helper()
	conn := openRaw(t)
	// **只清业务表**：`_jl_*` 是共享状态表，别的包（engine）的集成用例正在并发用它们，
	// 这里 DROP 会把人家跑到一半的状态抹掉。暂停记录按 job_key 隔离，不需要清表。
	for _, stmt := range []string{
		"DROP TABLE IF EXISTS " + table,
	} {
		if _, err := conn.Exec(context.Background(), stmt); err != nil {
			t.Fatalf("清场失败（%s）: %v", stmt, err)
		}
	}
	conn.Close(context.Background())
}

// openRaw 开一条裸连接（不建状态表），给「清场」用。
func openRaw(t *testing.T) *pgx.Conn {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("JINLIANG_TEST_PG_DSN"))
	if raw == "" {
		t.Fatal("缺少 JINLIANG_TEST_PG_DSN")
	}
	conn, err := pgx.Connect(context.Background(), raw)
	if err != nil {
		t.Fatalf("连不上目标库: %v", err)
	}
	return conn
}

// TestApplyRejectsLeftoverPausedRows：**sink 不做暂停判定，但绝不当成没看见**。
//
// 判定权在引擎（契约 §1.5 第 1 条）：引擎按表把暂停表的事件整条丢掉、其它表照写、
// 位点照常前进。所以走到 `Apply` 的批次里**不该**有暂停表的行——如果还有，那是引擎漏了
// （表名口径变了、新写路径没包 `pauseSink`、snapshot 漏了某张跳过表）。
//
// 这时必须立刻炸：带着新 Relation 往一张结构没对齐的表里写，`alignRelationTx` 还会顺手补列，
// 写进去就没人知道了。所以这条用例断言的是「**报错 + 目标端 0 行**」，不是「写进去」——
// 契约 §5-1 对这条用例的要求就是「引擎过滤后 sink 收到的批次里没有暂停表的行」。
func TestApplyRejectsLeftoverPausedRows(t *testing.T) {
	ctx := context.Background()
	table := "paused_apply"
	dropPausedFixtures(t, table)

	first := openPausedSink(t, table, "pausedapply")
	if _, err := first.EnsureSchema(ctx, pausedCatalog(table)); err != nil {
		t.Fatalf("建目标表失败: %v", err)
	}
	if err := first.EnsureMeta(ctx, "h", "", "", ""); err != nil {
		t.Fatalf("建状态行失败: %v", err)
	}
	ref := source.TableRef{Schema: pausedSchema, Table: table}
	if err := first.PauseTable(ctx, ref, PausedTable{
		Reason: PauseReasonTypeChanged,
		Detail: "列 v：目标端当前是 text，源端是 bigint",
		Resume: ResumeRecopy,
	}); err != nil {
		t.Fatal(err)
	}

	// 另一个实例从库里读回记录（跨进程可见），然后**故意**把这张表的行交给它。
	second := openPausedSink(t, table, "pausedapply")
	if err := second.LoadPausedTables(ctx); err != nil {
		t.Fatal(err)
	}
	err := second.Apply(ctx, pausedInsertEvents(table, 3), nil)
	if err == nil {
		t.Fatal("引擎漏过滤时必须炸：写进一张结构没对齐的表就是静默脏数据")
	}
	if !strings.Contains(err.Error(), "引擎没有过滤掉暂停表") {
		t.Fatalf("报错要指向真正的责任方（引擎没过滤），得到：%v", err)
	}
	var n int
	if err := first.conn.QueryRow(ctx, "SELECT count(*) FROM "+pausedSchema+"."+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("报错就是一行都不写，实际 %d 行", n)
	}
	t.Logf("最后一道防线：另一实例读回记录后收到暂停表的行 → 报「引擎没过滤」+ 目标端 0 行")
}

// 老库补列：`_jl_meta` 是 #76 之前的列集合（没有 paused_tables），Open 之后必须补上，
// 并且 PauseTable 真能写进去（不是「跑通没报错」）。
//
// 变异：把 `ADD COLUMN IF NOT EXISTS paused_tables` 那行注掉 → 这条必红。
func TestOldStateTableGetsPausedColumn(t *testing.T) {
	ctx := context.Background()
	const schema = "paused_old"
	table := "old_orders"
	conn := openRaw(t)
	for _, stmt := range []string{
		"CREATE SCHEMA IF NOT EXISTS " + schema,
		"DROP TABLE IF EXISTS " + schema + "." + table,
		"DROP TABLE IF EXISTS " + schema + "._jl_meta",
		// **旧列集合**：与 #76 之前完全一致（没有 paused_tables）
		`CREATE TABLE ` + schema + `._jl_meta (
			job_key        text PRIMARY KEY,
			name           text NOT NULL,
			instance_id    text NOT NULL,
			schema_version int  NOT NULL DEFAULT 1,
			job_hash       text NOT NULL DEFAULT '',
			schema_hash    text,
			phase          text NOT NULL,
			position       text,
			watermark      text,
			snapshot_id    text,
			started_at     timestamptz NOT NULL DEFAULT now(),
			updated_at     timestamptz NOT NULL DEFAULT now()
		)`,
	} {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("造老库失败（%s）: %v", stmt, err)
		}
	}
	conn.Close(ctx)

	// Open 会跑 ensureStateTables（生产里每条命令都经过它）
	s := openSinkFor(t, pausedCatalogIn(schema, table), func(o *Options) {
		o.JobKey = "pausedold"
		o.Schema = schema
	})
	var hasCol bool
	if err := s.conn.QueryRow(ctx, `SELECT count(*) > 0 FROM information_schema.columns
		WHERE table_schema=$1 AND table_name='_jl_meta' AND column_name='paused_tables'`, schema).Scan(&hasCol); err != nil {
		t.Fatal(err)
	}
	if !hasCol {
		t.Fatalf("老库的 %s._jl_meta 上没有补出 paused_tables 列（ADD COLUMN 没生效）", schema)
	}
	// 真能写：不是「列在就算好」
	if err := s.EnsureMeta(ctx, "h", "", "", ""); err != nil {
		t.Fatalf("老库状态行写不进去: %v", err)
	}
	ref := source.TableRef{Schema: schema, Table: table}
	if err := s.PauseTable(ctx, ref, PausedTable{Reason: PauseReasonTypeChanged, Detail: "v: 目标 text / 源 bigint"}); err != nil {
		t.Fatalf("老库上 PauseTable 必须能写: %v", err)
	}
	got, err := s.PausedTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got[table]; !ok {
		t.Fatalf("写进去的暂停记录读不回来：%v", got)
	}
	t.Logf("老库补列：%s._jl_meta 补出 paused_tables 且 PauseTable 可写（key=%s）", schema, table)
}

// pausedCatalogIn 是指定 schema 的表定义（与 pausedCatalog 同形状，schema 可换）。
func pausedCatalogIn(schema, table string) source.Catalog {
	c := pausedCatalog(table)
	for i := range c.Tables {
		c.Tables[i].Schema = schema
	}
	return c
}
