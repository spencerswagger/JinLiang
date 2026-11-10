//go:build integration

package pgsink

// 暂停记录的两条硬约束，必须能用真库证伪：
//  1. 只删一个 key —— 删 a 之后 b 的记录**仍在**（防「拿内存表集合重建整列」）；
//  2. 写路径遇到暂停表 → 整批拒绝、**目标端 0 行**（删掉这条判定就该红）。

import (
	"context"
	"encoding/json"
	"errors"
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

	if err := s.PauseTable(ctx, a, PauseReasonTypeChanged, "列 v：目标端当前是 text，源端是 bigint"); err != nil {
		t.Fatal(err)
	}
	atA := time.Now()
	time.Sleep(1100 * time.Millisecond) // RFC3339 是秒级：至少要跨一秒才能看出「没被刷新」
	if err := s.PauseTable(ctx, b, PauseReasonSchemaDrift, "第 2 列：原 v → 现 w"); err != nil {
		t.Fatal(err)
	}
	// 再登记一次 a：reason/detail 更新，但 at 必须还是第一次那个
	if err := s.PauseTable(ctx, a, PauseReasonTypeChanged, "列 v：目标端当前是 text，源端是 numeric(12,2)"); err != nil {
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

func TestApplyRefusesPausedTable(t *testing.T) {
	ctx := context.Background()
	table := "paused_apply"
	dropPausedFixtures(t, table)

	// 第一个实例：登记暂停（跨轮要能被**另一个进程/实例**读到，所以这里换实例验）
	first := openPausedSink(t, table, "pausedapply")
	if _, err := first.EnsureSchema(ctx, pausedCatalog(table)); err != nil {
		t.Fatalf("建目标表失败: %v", err)
	}
	if err := first.EnsureMeta(ctx, "h", "", "", ""); err != nil {
		t.Fatalf("建状态行失败: %v", err)
	}
	ref := source.TableRef{Schema: pausedSchema, Table: table}
	if err := first.PauseTable(ctx, ref, PauseReasonTypeChanged, "列 v：目标端当前是 text，源端是 bigint"); err != nil {
		t.Fatal(err)
	}

	// 第二个实例（模拟下一轮/另一个进程）：**必须靠 LoadPausedTables 从库里读回来**
	second := openPausedSink(t, table, "pausedapply")
	if err := second.LoadPausedTables(ctx); err != nil {
		t.Fatal(err)
	}
	evs := pausedInsertEvents(table, 3)
	if err := second.Apply(ctx, evs, nil); err == nil {
		t.Fatal("暂停表必须整批拒绝（跳过写 = 把这批变更永久丢掉）")
	} else {
		var pe PausableError
		if !errors.As(err, &pe) {
			t.Fatalf("应是 PausableError（要能按表识别），得到 %T：%v", err, err)
		}
		if pe.Table() != table {
			t.Fatalf("错误要带上是哪张表，得到 %q", pe.Table())
		}
	}
	var n int
	if err := first.conn.QueryRow(ctx, "SELECT count(*) FROM "+pausedSchema+"."+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("暂停表不许写入任何行，实际 %d 行", n)
	}
	t.Logf("暂停表写路径：另一个实例读回记录后整批拒绝、目标端 0 行")
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
	if err := s.PauseTable(ctx, ref, PauseReasonTypeChanged, "v: 目标 text / 源 bigint"); err != nil {
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
