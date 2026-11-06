//go:build integration

package pgsink

// 快照吞吐基线：量的是**产品路径** `BulkApply`（临时落地区 → COPY → INSERT …
// SELECT → 块记账 + 位点同事务），不是裸 CopyFrom。裸 COPY 只能代表目标端上限，
// 用它做门禁会让 BulkApply 慢 10 倍也照样 PASS —— Owl 在 #28 指出的正是这一点。
//
// 固定规模（默认 10 万行 / 每块 5000）与独立 schema，避免和其他集成测试抢表。

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/source"
)

const (
	bulkBenchSchema = "jl_bench_bulk"
	bulkBenchTable  = "orders"
)

func TestBulkApplyThroughput(t *testing.T) {
	raw := os.Getenv("JINLIANG_TEST_PG_DSN")
	if raw == "" {
		t.Fatalf("JINLIANG_TEST_PG_DSN 未设置：快照基线需要真实 PostgreSQL 目标库")
	}
	rowsTotal := envInt("JINLIANG_BULK_BENCH_ROWS", 100000)
	chunkRows := envInt("JINLIANG_BULK_BENCH_CHUNK", 5000)
	if rowsTotal <= 0 || chunkRows <= 0 || chunkRows > rowsTotal {
		t.Fatalf("BULK_BENCH_ROWS/CHUNK 不合法: %d/%d", rowsTotal, chunkRows)
	}
	ep, err := dsn.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	admin, err := pgx.Connect(ctx, ep.PostgresURL())
	if err != nil {
		t.Fatalf("连目标库失败: %v", err)
	}
	if _, err := admin.Exec(ctx, `DROP SCHEMA IF EXISTS `+bulkBenchSchema+` CASCADE`); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+bulkBenchSchema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+bulkBenchSchema+` CASCADE`)
		_ = admin.Close(context.Background())
	})

	ref := source.TableRef{Schema: bulkBenchSchema, Table: bulkBenchTable}
	cat := source.Catalog{Tables: []source.Table{{
		TableRef: ref,
		Columns: []source.Column{
			{Name: "id", Type: "bigint"},
			{Name: "name", Type: "text"},
			{Name: "amount", Type: "numeric(12,2)"},
		},
		PrimaryKey: []string{"id"}, HasStablePK: true,
	}}}
	sink, err := Open(ctx, Options{
		Target: ep, JobKey: "benchbulk", JobName: "benchbulk", InstanceID: "bench",
		Schema: bulkBenchSchema, OnExists: "recreate", Catalog: cat,
	})
	if err != nil {
		t.Fatalf("打开 Sink 失败: %v", err)
	}
	defer sink.Close()
	if _, err := sink.EnsureSchema(ctx, cat); err != nil {
		t.Fatalf("建表失败: %v", err)
	}
	ti := sink.tables[ref.Schema+"."+ref.Table]
	if ti == nil {
		t.Fatal("内部错误：bench 表不在 Sink 的 tableInfo 里")
	}

	chunks := rowsTotal / chunkRows
	var samples []time.Duration
	start := time.Now()
	for c := 0; c < chunks; c++ {
		rows := make([][]any, chunkRows)
		for j := 0; j < chunkRows; j++ {
			id := int64(c*chunkRows + j + 1)
			rows[j] = []any{id, fmt.Sprintf("row-%d", id), "12.34"}
		}
		pos := fmt.Sprintf("0/%X", c+1)
		t0 := time.Now()
		if err := sink.BulkApply(ctx, ti, c, rows, pos, "", ""); err != nil {
			t.Fatalf("第 %d 块 BulkApply 失败: %v", c, err)
		}
		samples = append(samples, time.Since(t0))
	}
	elapsed := time.Since(start)
	rowsPerSec := float64(chunks*chunkRows) / elapsed.Seconds()
	p50, p99 := percentiles(samples)

	rep := map[string]any{
		"metric":       "bulk_apply",
		"rows":         chunks * chunkRows,
		"chunk_rows":   chunkRows,
		"chunks":       chunks,
		"rows_per_sec": rowsPerSec,
		"p50_ms":       p50,
		"p99_ms":       p99,
		"elapsed_sec":  elapsed.Seconds(),
		"note":         "产品快照路径 pgsink.BulkApply：临时落地区 + COPY + INSERT…SELECT + 块记账 + 位点同事务",
	}
	if out := os.Getenv("JINLIANG_BULK_BENCH_OUT"); out != "" {
		b, _ := json.MarshalIndent(rep, "", "  ")
		if err := os.WriteFile(out, append(b, '\n'), 0o644); err != nil {
			t.Fatalf("写 %s 失败: %v", out, err)
		}
	}
	t.Logf("bulk_apply: %.0f rows/s, p50 %.2f ms, p99 %.2f ms (%d rows, chunk %d)",
		rowsPerSec, p50, p99, chunks*chunkRows, chunkRows)
}

func percentiles(ds []time.Duration) (p50, p99 float64) {
	if len(ds) == 0 {
		return 0, 0
	}
	sorted := append([]time.Duration(nil), ds...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
	rank := func(p float64) int {
		i := int(float64(len(sorted))*p+0.9999) - 1
		if i < 0 {
			i = 0
		}
		if i >= len(sorted) {
			i = len(sorted) - 1
		}
		return i
	}
	return ms(sorted[rank(0.50)]), ms(sorted[rank(0.99)])
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
