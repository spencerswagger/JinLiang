//go:build integration

// 集成测试：需要真 PostgreSQL 目标库（JINLIANG_TEST_PG_DSN）。
// 缺 DSN 直接失败，不 skip——静默跳过会让「矩阵全绿」变成假象。
package pgsink

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/source"
)

func openSinkFor(t *testing.T, catalog source.Catalog, mutate func(*Options)) *Sink {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("JINLIANG_TEST_PG_DSN"))
	if raw == "" {
		t.Fatal("缺少 JINLIANG_TEST_PG_DSN：积分测试必须跑在真库上，没有 DSN 就是失败，不是跳过")
	}
	ep, err := dsn.Parse(raw)
	if err != nil {
		t.Fatalf("解析 DSN: %v", err)
	}
	opt := Options{
		Target:     ep,
		JobKey:     "itbatch001",
		JobName:    "it",
		InstanceID: "it-instance",
		Schema:     "public",
		OnExists:   "append",
		Catalog:    catalog,
	}
	if mutate != nil {
		mutate(&opt)
	}
	s, err := Open(context.Background(), opt)
	if err != nil {
		t.Fatalf("打开目标库: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func batchCatalog(table string) source.Catalog {
	return source.Catalog{Tables: []source.Table{{
		TableRef: source.TableRef{Schema: "public", Table: table},
		Columns: []source.Column{
			{Name: "id", Type: "bigint"},
			{Name: "payload", Type: "text"},
		},
		PrimaryKey:  []string{"id"},
		HasStablePK: true,
	}}}
}

func insertEvents(table string, rows int, pos string) []source.Event {
	var evs []source.Event
	evs = append(evs, source.Event{Kind: source.EvBegin, TxID: "tx1"})
	for i := 1; i <= rows; i++ {
		evs = append(evs, source.Event{
			Kind: source.EvInsert, TxID: "tx1",
			Table: source.TableRef{Schema: "public", Table: table},
			// 值按文本交付：pgsource 走 pgoutput 文本格式，所有列都是字符串；
			// SQL 里 `$n::text::<列类型>` 要求参数本身是 text（这也是生产路径的形态）。
			New:      source.Row{Table: source.TableRef{Schema: "public", Table: table}, Values: []source.Value{{Kind: source.KindString, S: fmt.Sprintf("%d", i)}, {Kind: source.KindString, S: fmt.Sprintf("p%d", i)}}},
			Relation: relationOf(table),
		})
	}
	evs = append(evs, source.Event{Kind: source.EvCommit, TxID: "tx1"})
	return evs
}

func relationOf(table string) *source.Table {
	t := batchCatalog(table).Tables[0]
	return &t
}

// testPosition 是最小 source.Position 实现（kind 与 PG 一致）。
type testPosition struct{ raw string }

func (p testPosition) String() string { return p.raw }
func (p testPosition) Kind() string   { return "postgres" }

// 批量路径与单行路径必须产出**完全相同**的行与位点：
// 一条用 ApplyBatchRows=500（多行 INSERT），一条用 1（逐行），数据相同 → 结果必须相同。
func TestIntegrationBatchMatchesSingleRowSemantics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const rows = 25
	batchTable, singleTable := "jl_batch_semantics", "jl_single_semantics"
	drop := func(s *Sink, table string) {
		_, _ = s.conn.Exec(ctx, "DROP TABLE IF EXISTS public."+table)
	}

	batched := openSinkFor(t, batchCatalog(batchTable), nil)
	drop(batched, batchTable)
	if _, err := batched.EnsureSchema(ctx, batchCatalog(batchTable)); err != nil {
		t.Fatalf("建表(批量): %v", err)
	}
	defer drop(batched, batchTable)
	if err := batched.Apply(ctx, insertEvents(batchTable, rows, "0/AA"), testPosition{"postgres:v1:0/AA"}); err != nil {
		t.Fatalf("批量写入: %v", err)
	}

	single := openSinkFor(t, batchCatalog(singleTable), func(o *Options) { o.ApplyBatchRows = 1 })
	drop(single, singleTable)
	if _, err := single.EnsureSchema(ctx, batchCatalog(singleTable)); err != nil {
		t.Fatalf("建表(单行): %v", err)
	}
	defer drop(single, singleTable)
	if err := single.Apply(ctx, insertEvents(singleTable, rows, "0/AA"), testPosition{"postgres:v1:0/AA"}); err != nil {
		t.Fatalf("单行写入: %v", err)
	}

	// 行内容逐字节比较（按 id 排序后拼串）。
	gotBatch, err := batched.Fingerprint(ctx, source.TableRef{Schema: "public", Table: batchTable})
	if err != nil {
		t.Fatalf("批量指纹: %v", err)
	}
	gotSingle, err := single.Fingerprint(ctx, source.TableRef{Schema: "public", Table: singleTable})
	if err != nil {
		t.Fatalf("单行指纹: %v", err)
	}
	if gotBatch != gotSingle {
		t.Fatalf("两条路径的结果不一致:\n 批量 %s\n 单行 %s", gotBatch, gotSingle)
	}
	nb, err := batched.Count(ctx, source.TableRef{Schema: "public", Table: batchTable})
	if err != nil {
		t.Fatal(err)
	}
	if nb != rows {
		t.Fatalf("批量路径应写 %d 行，实际 %d", rows, nb)
	}

	// 位点也必须一样：批量不能因为少一次往返就把位点写早/写晚。
	pb, err := batched.Position(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ps, err := single.Position(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pb != ps {
		t.Fatalf("两条路径落库的位点不同: 批量=%q 单行=%q", pb, ps)
	}
}

// 幂等：同一批（含 25 行）重放两次，行数与结果不变——批量路径也必须靠 upsert 吸收。
func TestIntegrationBatchReplayIsIdempotent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const rows = 25
	table := "jl_batch_replay"
	s := openSinkFor(t, batchCatalog(table), nil)
	_, _ = s.conn.Exec(ctx, "DROP TABLE IF EXISTS public."+table)
	if _, err := s.EnsureSchema(ctx, batchCatalog(table)); err != nil {
		t.Fatalf("建表: %v", err)
	}
	defer func() { _, _ = s.conn.Exec(ctx, "DROP TABLE IF EXISTS public."+table) }()

	first := insertEvents(table, rows, "0/BB")
	if err := s.Apply(ctx, first, testPosition{"postgres:v1:0/BB"}); err != nil {
		t.Fatalf("第一次写入: %v", err)
	}
	fp1, err := s.Fingerprint(ctx, source.TableRef{Schema: "public", Table: table})
	if err != nil {
		t.Fatal(err)
	}
	replay := insertEvents(table, rows, "0/BB")
	if err := s.Apply(ctx, replay, testPosition{"postgres:v1:0/BB"}); err != nil {
		t.Fatalf("重放失败（批量路径没有吸收重复）: %v", err)
	}
	n, err := s.Count(ctx, source.TableRef{Schema: "public", Table: table})
	if err != nil {
		t.Fatal(err)
	}
	if n != rows {
		t.Fatalf("重放后仍应 %d 行，实际 %d", rows, n)
	}
	fp2, err := s.Fingerprint(ctx, source.TableRef{Schema: "public", Table: table})
	if err != nil {
		t.Fatal(err)
	}
	if fp1 != fp2 {
		t.Fatal("重放后内容变了")
	}
}

// 批大小把一条语句切开时（25 行、批 10）结果必须仍与一次写完相同。
func TestIntegrationBatchSizeSplitDoesNotChangeResult(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const rows = 25
	table := "jl_batch_split"
	s := openSinkFor(t, batchCatalog(table), func(o *Options) { o.ApplyBatchRows = 10 })
	_, _ = s.conn.Exec(ctx, "DROP TABLE IF EXISTS public."+table)
	if _, err := s.EnsureSchema(ctx, batchCatalog(table)); err != nil {
		t.Fatalf("建表: %v", err)
	}
	defer func() { _, _ = s.conn.Exec(ctx, "DROP TABLE IF EXISTS public."+table) }()

	if err := s.Apply(ctx, insertEvents(table, rows, "0/CC"), testPosition{"postgres:v1:0/CC"}); err != nil {
		t.Fatalf("分批写入: %v", err)
	}
	n, err := s.Count(ctx, source.TableRef{Schema: "public", Table: table})
	if err != nil {
		t.Fatal(err)
	}
	if n != rows {
		t.Fatalf("批大小 10 切三片后应仍是 %d 行，实际 %d", rows, n)
	}
	if p, err := s.Position(ctx); err != nil || p != "postgres:v1:0/CC" {
		t.Fatalf("位点不对: %q err=%v", p, err)
	}
}

// 增量吞吐基线：一批 500 行，看「多行 upsert」vs「逐行 Exec」差多少。
// 只需在真库上跑：go test -tags=integration -bench BenchmarkApplyUpsert -benchtime=40x ./internal/pgsink/
func BenchmarkApplyUpsert500(b *testing.B) {
	for _, bs := range []int{DefaultApplyBatchRows, 1} {
		name := fmt.Sprintf("batch=%d", bs)
		b.Run(name, func(b *testing.B) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			table := fmt.Sprintf("jl_bench_%d", bs)
			cat := batchCatalog(table)
			s := openSinkBench(b, cat, bs)
			_, _ = s.conn.Exec(ctx, "DROP TABLE IF EXISTS public."+table)
			if _, err := s.EnsureSchema(ctx, cat); err != nil {
				b.Fatalf("建表: %v", err)
			}
			defer func() { _, _ = s.conn.Exec(context.Background(), "DROP TABLE IF EXISTS public."+table) }()

			const rowsPerOp = 500
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				evs := insertEventsFrom(table, i*rowsPerOp+1, rowsPerOp)
				if err := s.Apply(ctx, evs, testPosition{fmt.Sprintf("postgres:v1:0/%X", i)}); err != nil {
					b.Fatalf("写入: %v", err)
				}
			}
			b.StopTimer()
			elapsed := b.Elapsed().Seconds()
			total := float64(b.N * rowsPerOp)
			b.ReportMetric(total/elapsed, "rows/s")
		})
	}
}

func openSinkBench(b *testing.B, cat source.Catalog, batchRows int) *Sink {
	b.Helper()
	raw := strings.TrimSpace(os.Getenv("JINLIANG_TEST_PG_DSN"))
	if raw == "" {
		b.Fatal("缺少 JINLIANG_TEST_PG_DSN")
	}
	ep, err := dsn.Parse(raw)
	if err != nil {
		b.Fatalf("DSN: %v", err)
	}
	s, err := Open(context.Background(), Options{
		Target: ep, JobKey: "itbench001", JobName: "bench", InstanceID: "bench",
		Schema: "public", OnExists: "append", Catalog: cat, ApplyBatchRows: batchRows,
	})
	if err != nil {
		b.Fatalf("打开目标库: %v", err)
	}
	b.Cleanup(func() { s.Close() })
	return s
}

// insertEventsFrom 造从 first 开始的 n 行插入（主键连续且不重复）。
func insertEventsFrom(table string, first, n int) []source.Event {
	var evs []source.Event
	evs = append(evs, source.Event{Kind: source.EvBegin, TxID: "tx"})
	for i := 0; i < n; i++ {
		id := first + i
		evs = append(evs, source.Event{
			Kind: source.EvInsert, TxID: "tx",
			Table: source.TableRef{Schema: "public", Table: table},
			New: source.Row{Table: source.TableRef{Schema: "public", Table: table}, Values: []source.Value{
				{Kind: source.KindString, S: fmt.Sprintf("%d", id)},
				{Kind: source.KindString, S: fmt.Sprintf("payload-%d", id)},
			}},
			Relation: relationOf(table),
		})
	}
	evs = append(evs, source.Event{Kind: source.EvCommit, TxID: "tx"})
	return evs
}

// 同一事务里两次改同一个主键：单行路径是「后写覆盖」，批量路径必须一致
// （否则多行 INSERT 会撞 "cannot affect row a second time"，整批永久失败）。
func TestIntegrationBatchDuplicatePKLastWins(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	table := "jl_batch_dupkey"
	cat := batchCatalog(table)
	s := openSinkFor(t, cat, nil)
	_, _ = s.conn.Exec(ctx, "DROP TABLE IF EXISTS public."+table)
	if _, err := s.EnsureSchema(ctx, cat); err != nil {
		t.Fatalf("建表: %v", err)
	}
	defer func() { _, _ = s.conn.Exec(ctx, "DROP TABLE IF EXISTS public."+table) }()

	mk := func(v string) source.Event {
		return source.Event{Kind: source.EvInsert, TxID: "tx", Table: source.TableRef{Schema: "public", Table: table},
			New: source.Row{Table: source.TableRef{Schema: "public", Table: table}, Values: []source.Value{
				{Kind: source.KindString, S: "7"}, {Kind: source.KindString, S: v},
			}},
			Relation: relationOf(table)}
	}
	batch := []source.Event{
		{Kind: source.EvBegin, TxID: "tx"},
		mk("first"), mk("second"), mk("last"),
		{Kind: source.EvCommit, TxID: "tx"},
	}
	if err := s.Apply(ctx, batch, testPosition{"postgres:v1:0/DD"}); err != nil {
		t.Fatalf("同一主键在一批里出现多次不该失败: %v", err)
	}
	var payload string
	if err := s.conn.QueryRow(ctx, "SELECT payload FROM public."+table+" WHERE id=7").Scan(&payload); err != nil {
		t.Fatalf("读回: %v", err)
	}
	if payload != "last" {
		t.Fatalf("应保留最后一次写入（last），实际 %q", payload)
	}
	if n, err := s.Count(ctx, source.TableRef{Schema: "public", Table: table}); err != nil || n != 1 {
		t.Fatalf("应只有 1 行，实际 %d（err=%v）", n, err)
	}
}

// dedupeByPK 的纯逻辑单测（不需要数据库）。
func TestDedupeByPKKKeepsLast(t *testing.T) {
	tables, _ := buildTables(batchCatalog("t"), "")
	ti := tables["public.t"]
	mk := func(id, v string) source.Event {
		return source.Event{Kind: source.EvInsert, New: source.Row{Values: []source.Value{
			{Kind: source.KindString, S: id}, {Kind: source.KindString, S: v}}}}
	}
	got := dedupeByPK(ti, []source.Event{mk("1", "a"), mk("2", "b"), mk("1", "c")})
	if len(got) != 2 {
		t.Fatalf("应去重成 2 条，得到 %d", len(got))
	}
	if got[0].New.Values[0].S != "2" || got[1].New.Values[1].S != "c" {
		t.Fatalf("去重后应保留 (2,b) 与 (1,c) 且保持顺序: %#v", got)
	}
}

// 整批失败时必须能把坏行定位出来（主键 + SQLSTATE），否则毒丸行会让整批永久失败、
// 位点不动，而用户只看到「写入 t 失败（5 行）」——既没主键也没原因。
func TestIntegrationBatchFailureLocatesBadRow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	table := "jl_batch_poison"
	cat := batchCatalog(table)
	s := openSinkFor(t, cat, nil)
	_, _ = s.conn.Exec(ctx, "DROP TABLE IF EXISTS public."+table)
	// 手工建表并加一条 CHECK：id 必须为正。第 3 行故意给 -3。
	if _, err := s.conn.Exec(ctx, "CREATE TABLE public."+table+
		" (id bigint PRIMARY KEY, payload text, CONSTRAINT jl_positive CHECK (id > 0))"); err != nil {
		t.Fatalf("建表: %v", err)
	}
	defer func() { _, _ = s.conn.Exec(ctx, "DROP TABLE IF EXISTS public."+table) }()
	if _, err := s.EnsureSchema(ctx, cat); err != nil {
		t.Fatalf("EnsureSchema: %v", err)
	}

	mk := func(id string) source.Event {
		return source.Event{Kind: source.EvInsert, TxID: "tx", Table: source.TableRef{Schema: "public", Table: table},
			New: source.Row{Table: source.TableRef{Schema: "public", Table: table}, Values: []source.Value{
				{Kind: source.KindString, S: id}, {Kind: source.KindString, S: "x"},
			}},
			Relation: relationOf(table)}
	}
	batch := []source.Event{
		{Kind: source.EvBegin, TxID: "tx"},
		mk("1"), mk("2"), mk("-3"), mk("4"), mk("5"),
		{Kind: source.EvCommit, TxID: "tx"},
	}
	err := s.Apply(ctx, batch, testPosition{"postgres:v1:0/EE"})
	if err == nil {
		t.Fatal("违反 CHECK 的批次必须失败")
	}
	msg := err.Error()
	for _, want := range []string{"id=-3", "23514", "3/5", "content.tables"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("错误里应有 %q（主键/SQLSTATE/第几行/下一步），实际: %s", want, msg)
		}
	}
	// 整批回滚：一行都不该落库，位点不动。
	if n, err := s.Count(ctx, source.TableRef{Schema: "public", Table: table}); err != nil || n != 0 {
		t.Fatalf("整批回滚后应 0 行，实际 %d（err=%v）", n, err)
	}
	if p, _ := s.Position(ctx); strings.Contains(p, "0/EE") {
		t.Fatalf("失败时位点不能前进: %q", p)
	}
}

// 无主键表（源端没有身份，R1）：INSERT 照写；UPDATE 必须明确失败——不许静默插一版新的
// （设计 §1.5 已否）。按表暂停/整表重搬是 S2；在那之前这里整批失败、位点不动。
func TestIntegrationNoPKInsertOnlyRejectsUpdate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const table = "jl_nopk_insert_only"
	cat := source.Catalog{Tables: []source.Table{{
		TableRef: source.TableRef{Schema: "public", Table: table},
		Columns: []source.Column{
			{Name: "id", Type: "bigint"},
			{Name: "payload", Type: "text"},
		},
		// 无 PrimaryKey、HasStablePK=false：源端没有行身份
	}}}
	s := openSinkFor(t, cat, nil)
	drop := func() { _, _ = s.conn.Exec(ctx, "DROP TABLE IF EXISTS public."+table) }
	drop()
	defer drop()
	if _, err := s.EnsureSchema(ctx, cat); err != nil {
		t.Fatalf("建表: %v", err)
	}

	ref := source.TableRef{Schema: "public", Table: table}
	rel := &source.Table{TableRef: ref, Columns: cat.Tables[0].Columns}
	mk := func(kind source.EventKind, payload string) source.Event {
		// 值按文本交付（pgsource 走 pgoutput 文本格式，与 insertEvents 同形态）。
		return source.Event{Kind: kind, TxID: "tx", Table: ref,
			New: source.Row{Table: ref, Values: []source.Value{
				{Kind: source.KindString, S: "1"}, {Kind: source.KindString, S: payload},
			}},
			Old: source.Row{Table: ref, Values: []source.Value{
				{Kind: source.KindString, S: "1"}, {Kind: source.KindString, S: "old"},
			}},
			Relation: rel}
	}
	ins := []source.Event{{Kind: source.EvBegin, TxID: "tx"}, mk(source.EvInsert, "a"), {Kind: source.EvCommit, TxID: "tx"}}
	if err := s.Apply(ctx, ins, testPosition{"postgres:v1:0/10"}); err != nil {
		t.Fatalf("无主键表 insert-only 不应失败: %v", err)
	}
	if n, err := s.Count(ctx, ref); err != nil || n != 1 {
		t.Fatalf("应写 1 行，实际 %d（err=%v）", n, err)
	}

	upd := []source.Event{{Kind: source.EvBegin, TxID: "tx"}, mk(source.EvUpdate, "b"), {Kind: source.EvCommit, TxID: "tx"}}
	err := s.Apply(ctx, upd, testPosition{"postgres:v1:0/20"})
	if err == nil {
		t.Fatal("无主键表收到 UPDATE 必须失败，不能静默插一版新的")
	}
	if !strings.Contains(err.Error(), "没有主键") {
		t.Fatalf("错误应说清是无主键、无法定位改行: %v", err)
	}
	if n, _ := s.Count(ctx, ref); n != 1 {
		t.Fatalf("失败时不能多插行，实际 %d", n)
	}
	if p, _ := s.Position(ctx); strings.Contains(p, "0/20") {
		t.Fatalf("失败时位点不能前进: %q", p)
	}
}

// 声明 unique_key 的无主键表：ON CONFLICT 用声明列，重放幂等，删除能定位
// （docs/DESIGN-NO-PRIMARY-KEY.md §5 S3 验收）。
func TestIntegrationDeclaredUniqueKeyUpsertAndDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const table = "jl_b5_decl"
	cat := source.Catalog{Tables: []source.Table{{
		TableRef: source.TableRef{Schema: "public", Table: table},
		Columns: []source.Column{
			{Name: "tenant_id", Type: "bigint"},
			{Name: "event_no", Type: "text"},
			{Name: "payload", Type: "text", Nullable: true},
		},
		// 源端没有稳定主键（HasStablePK=false）：身份来自声明。
	}}}
	s := openSinkFor(t, cat, func(o *Options) {
		o.UniqueKeys = map[string][]string{table: {"tenant_id", "event_no"}}
	})
	drop := func() { _, _ = s.conn.Exec(ctx, "DROP TABLE IF EXISTS public."+table) }
	drop()
	// 目标表由用户建好并带唯一索引（声明要求「已有唯一索引」，我们不建）。
	if _, err := s.conn.Exec(ctx, "CREATE TABLE public."+table+
		" (tenant_id bigint NOT NULL, event_no text NOT NULL, payload text, UNIQUE (tenant_id, event_no))"); err != nil {
		t.Fatalf("建带唯一索引的无主键表: %v", err)
	}
	defer drop()
	if _, err := s.EnsureSchema(ctx, cat); err != nil {
		t.Fatalf("append 策略应沿用已有表: %v", err)
	}

	rel := &source.Table{TableRef: source.TableRef{Schema: "public", Table: table},
		Columns: cat.Tables[0].Columns}
	mk := func(payload string) []source.Event {
		return []source.Event{
			{Kind: source.EvBegin, TxID: "tx"},
			{Kind: source.EvInsert, TxID: "tx", Table: source.TableRef{Schema: "public", Table: table},
				Relation: rel,
				New: source.Row{Table: source.TableRef{Schema: "public", Table: table}, Values: []source.Value{
					{Kind: source.KindString, S: "1"},
					{Kind: source.KindString, S: "e1"},
					{Kind: source.KindString, S: payload}}}},
			{Kind: source.EvCommit, TxID: "tx"},
		}
	}
	if err := s.Apply(ctx, mk("x"), testPosition{"postgres:v1:0/B0"}); err != nil {
		t.Fatalf("声明身份后的首写失败: %v", err)
	}
	if err := s.Apply(ctx, mk("y"), testPosition{"postgres:v1:0/C0"}); err != nil {
		t.Fatalf("重放失败: %v", err)
	}
	n, err := s.Count(ctx, source.TableRef{Schema: "public", Table: table})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("声明行身份后重放应幂等（1 行），实际 %d", n)
	}
	var payload string
	if err := s.conn.QueryRow(ctx, "SELECT payload FROM public."+table).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if payload != "y" {
		t.Fatalf("重放应更新 payload，实际 %q", payload)
	}

	del := []source.Event{
		{Kind: source.EvBegin, TxID: "tx"},
		{Kind: source.EvDelete, TxID: "tx", Table: source.TableRef{Schema: "public", Table: table},
			Relation: rel,
			Old: source.Row{Table: source.TableRef{Schema: "public", Table: table}, Values: []source.Value{
				{Kind: source.KindString, S: "1"},
				{Kind: source.KindString, S: "e1"},
				{Kind: source.KindString, S: "y"}}}},
		{Kind: source.EvCommit, TxID: "tx"},
	}
	if err := s.Apply(ctx, del, testPosition{"postgres:v1:0/D0"}); err != nil {
		t.Fatalf("按声明列删除失败: %v", err)
	}
	if n, err = s.Count(ctx, source.TableRef{Schema: "public", Table: table}); err != nil || n != 0 {
		t.Fatalf("删除后应 0 行，实际 %d err=%v", n, err)
	}
}

// 声明列含 NULL：PG 的唯一索引允许 NULL 且 NULL 互不相等，这组列不构成唯一性 → 写前必须停。
func TestIntegrationDeclaredUniqueKeyNullableFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const table = "jl_b5_decl_null"
	cat := source.Catalog{Tables: []source.Table{{
		TableRef: source.TableRef{Schema: "public", Table: table},
		Columns: []source.Column{
			{Name: "tenant_id", Type: "bigint"},
			{Name: "event_no", Type: "text", Nullable: true},
		},
	}}}
	s := openSinkFor(t, cat, func(o *Options) {
		o.UniqueKeys = map[string][]string{table: {"tenant_id", "event_no"}}
	})
	drop := func() { _, _ = s.conn.Exec(ctx, "DROP TABLE IF EXISTS public."+table) }
	drop()
	if _, err := s.conn.Exec(ctx, "CREATE TABLE public."+table+
		" (tenant_id bigint NOT NULL, event_no text, UNIQUE (tenant_id, event_no))"); err != nil {
		t.Fatalf("建表: %v", err)
	}
	defer drop()
	if _, err := s.EnsureSchema(ctx, cat); err != nil {
		t.Fatalf("append 策略应沿用已有表: %v", err)
	}
	evs := []source.Event{
		{Kind: source.EvBegin, TxID: "tx"},
		{Kind: source.EvInsert, TxID: "tx", Table: source.TableRef{Schema: "public", Table: table},
			Relation: &cat.Tables[0],
			New: source.Row{Table: source.TableRef{Schema: "public", Table: table}, Values: []source.Value{
				{Kind: source.KindString, S: "1"}, {Kind: source.KindString, S: "e1"}}}},
		{Kind: source.EvCommit, TxID: "tx"},
	}
	err := s.Apply(ctx, evs, testPosition{"postgres:v1:0/E0"})
	if err == nil {
		t.Fatal("声明列可空时必须失败")
	}
	if !strings.Contains(err.Error(), "允许为空") {
		t.Fatalf("错误要点明含 NULL 的列不构成唯一性: %v", err)
	}
}

// 快照分页（BulkApply）也走 upsert，所以声明核对必须和 Apply 同口径：
// 声明与目标端索引对不上时，用户应该拿到四段式（可行动），而不是驱动的原始 42P10。
func TestIntegrationBulkApplyChecksDeclaredKey(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const table = "jl_b5_decl_bulk"
	cat := source.Catalog{Tables: []source.Table{{
		TableRef: source.TableRef{Schema: "public", Table: table},
		Columns: []source.Column{
			{Name: "tenant_id", Type: "bigint"},
			{Name: "event_no", Type: "text"},
			{Name: "payload", Type: "text", Nullable: true},
		},
	}}}
	// 声明只写 tenant_id，但目标表唯一索引是 (tenant_id,event_no)：核对不过。
	s := openSinkFor(t, cat, func(o *Options) {
		o.UniqueKeys = map[string][]string{table: {"tenant_id"}}
	})
	drop := func() { _, _ = s.conn.Exec(ctx, "DROP TABLE IF EXISTS public."+table) }
	drop()
	if _, err := s.conn.Exec(ctx, "CREATE TABLE public."+table+
		" (tenant_id bigint NOT NULL, event_no text NOT NULL, payload text, UNIQUE (tenant_id, event_no))"); err != nil {
		t.Fatalf("建表: %v", err)
	}
	defer drop()
	if _, err := s.EnsureSchema(ctx, cat); err != nil {
		t.Fatalf("append 策略应沿用已有表: %v", err)
	}
	ti := s.tables["public."+table]
	rows := [][]any{{int64(1), "e1", "x"}}
	err := s.BulkApply(ctx, ti, 0, rows, "", "", "")
	if err == nil {
		t.Fatal("声明与目标端唯一索引不一致时，快照分页必须停下")
	}
	if !strings.Contains(err.Error(), "声明了 unique_key") || !strings.Contains(err.Error(), "三种做法") {
		t.Fatalf("要出四段式（发生了什么/为什么/你能做什么/风险），得到：%v", err)
	}
	// 顺带：一行都不该写进去。
	if n, cErr := s.Count(ctx, source.TableRef{Schema: "public", Table: table}); cErr != nil || n != 0 {
		t.Fatalf("核对不过时不许写行，实际 %d err=%v", n, cErr)
	}
}
