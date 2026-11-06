// Command benchapply measures the incremental apply hot path against a real
// PostgreSQL target.
//
// 为什么单独一个程序而不是 `go test -bench`：要同时拿到「吞吐」与「p99 延迟」
// 两个数，并且要能把固定数据规模、固定批量写进报告；`go test -bench` 的输出
// 还要再解析一层，脚本里不好用。
//
// 口径（写进报告，别只看一个数字）：
//   - 固定行数（默认 100000）与固定批量（默认 500）；
//   - 每个批次 = 一个目标事务（BEGIN/500×INSERT/COMMIT + 位点同事务），
//     与 pgsink.Apply 的真实路径一致；
//   - 吞吐 = 总行数 / 总耗时（不含建表与预热）；
//   - p99 = 批事务延迟的最近秩（样本数 = 批次数），不是单行延迟；
//   - 只测增量 apply，不测源端解码与网络：那两项需要两套库与固定数据语料。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/pgsink"
	"github.com/spencerswagger/jinliang/internal/source"
)

type report struct {
	Metric                   string  `json:"metric"`
	Rows                     int     `json:"rows"`
	BatchRows                int     `json:"batch_rows"`
	Batches                  int     `json:"batches"`
	WarmupBatches            int     `json:"warmup_batches"`
	RowsPerSec               float64 `json:"rows_per_sec"` // 增量 apply
	RawCopyCeilingRowsPerSec float64 `json:"raw_copy_ceiling_rows_per_sec"`
	P50MS                    float64 `json:"p50_ms"`
	P99MS                    float64 `json:"p99_ms"`
	MaxMS                    float64 `json:"max_ms"`
	ElapsedSec               float64 `json:"elapsed_sec"`
	SetupMS                  float64 `json:"setup_ms"`
	NumCPU                   int     `json:"num_cpu"`
	GoVersion                string  `json:"go_version"`
	CPUModel                 string  `json:"cpu_model,omitempty"`
	Note                     string  `json:"note"`
}

type benchPosition string

func (p benchPosition) String() string { return string(p) }
func (p benchPosition) Kind() string   { return dsn.KindPostgres }

func main() {
	rows := flag.Int("rows", 100000, "总行数（固定规模，默认 10 万）")
	batch := flag.Int("batch", 500, "每批行数（= 每事务行数）")
	warmup := flag.Int("warmup", 2, "预热批次数（不计入吞吐/延迟）")
	jsonOut := flag.String("json-out", "", "把 JSON 报告写到这个文件（空 = 只打印）")
	table := flag.String("table", "orders", "目标表名")
	schema := flag.String("schema", "jl_bench_apply", "专用 schema（避免与其他集成测试抢 public._jl_*）")
	flag.Parse()

	raw := strings.TrimSpace(os.Getenv("JINLIANG_TEST_PG_DSN"))
	if raw == "" {
		fmt.Fprintln(os.Stderr, "benchapply: JINLIANG_TEST_PG_DSN 未设置；吞吐/p99 需要真实 PostgreSQL 目标库")
		os.Exit(2)
	}
	if *rows <= 0 || *batch <= 0 || *batch > *rows {
		fmt.Fprintln(os.Stderr, "benchapply: --rows/--batch 不合法")
		os.Exit(2)
	}
	ep, err := dsn.Parse(raw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchapply: %v\n", err)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	admin, err := pgx.Connect(ctx, ep.PostgresURL())
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchapply: 连目标库失败: %v\n", err)
		os.Exit(2)
	}
	if _, err := admin.Exec(ctx, `DROP SCHEMA IF EXISTS "`+*schema+`" CASCADE`); err != nil {
		fmt.Fprintf(os.Stderr, "benchapply: 清理 schema 失败: %v\n", err)
		os.Exit(2)
	}
	if _, err := admin.Exec(ctx, `CREATE SCHEMA "`+*schema+`"`); err != nil {
		fmt.Fprintf(os.Stderr, "benchapply: 建 schema 失败: %v\n", err)
		os.Exit(2)
	}
	defer func() {
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA IF EXISTS "`+*schema+`" CASCADE`)
		_ = admin.Close(context.Background())
	}()

	ref := source.TableRef{Schema: *schema, Table: *table}
	cat := source.Catalog{Tables: []source.Table{{
		TableRef: ref,
		Columns: []source.Column{
			{Name: "id", Type: "bigint"},
			{Name: "name", Type: "text"},
			{Name: "amount", Type: "numeric(12,2)"},
		},
		PrimaryKey:  []string{"id"},
		HasStablePK: true,
	}}}

	setupStart := time.Now()
	sink, err := pgsink.Open(ctx, pgsink.Options{
		Target: ep, JobKey: "benchapply", JobName: "benchapply",
		InstanceID: "bench", Schema: *schema, OnExists: "recreate", Catalog: cat,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchapply: 打开目标库失败: %v\n", err)
		os.Exit(2)
	}
	defer sink.Close()
	if _, err := sink.EnsureSchema(ctx, cat); err != nil {
		fmt.Fprintf(os.Stderr, "benchapply: 建表失败: %v\n", err)
		os.Exit(2)
	}
	setupMS := float64(time.Since(setupStart).Microseconds()) / 1000

	// 快照吞吐：与 pgsink 的全量路径同为 COPY（docs/DESIGN.md §7A.1 的
	// 「快照吞吐 ≥20k 行/s」）。这里用 pgx.CopyFrom 直接量目标端 COPY 能力，
	// 不把源端 SELECT 时间算进来。
	snapshotRowsPerSec, err := measureSnapshot(ctx, ep, ref, *rows)
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchapply: 快照 COPY 失败: %v\n", err)
		os.Exit(2)
	}

	totalBatches := *rows / *batch
	if totalBatches == 0 {
		totalBatches = 1
	}
	var samples []time.Duration
	measuredRows := 0
	measureStart := time.Now()
	for b := 0; b < *warmup+totalBatches; b++ {
		evs := make([]source.Event, 0, *batch+2)
		evs = append(evs, source.Event{Kind: source.EvBegin, Table: ref})
		for j := 0; j < *batch; j++ {
			id := int64(b**batch + j + 1)
			evs = append(evs, source.Event{
				Kind: source.EvInsert, Table: ref,
				// pgsink 的占位符是 $n::text::<列类型>，参数必须以文本形态给
				// （引擎也是这样喂的）；用 KindString 才是真实热路径。
				New: source.Row{Table: ref, Values: []source.Value{
					{Kind: source.KindString, S: strconv.FormatInt(id, 10)},
					{Kind: source.KindString, S: fmt.Sprintf("row-%d", id)},
					{Kind: source.KindString, S: "12.34"},
				}},
			})
		}
		evs = append(evs, source.Event{Kind: source.EvCommit, Table: ref})

		start := time.Now()
		pos := benchPosition(fmt.Sprintf("0/%X", b+1))
		if err := sink.Apply(ctx, evs, pos); err != nil {
			fmt.Fprintf(os.Stderr, "benchapply: 第 %d 批失败: %v\n", b, err)
			os.Exit(2)
		}
		d := time.Since(start)
		if b >= *warmup {
			samples = append(samples, d)
			measuredRows += *batch
		}
	}
	elapsed := time.Since(measureStart)

	rep := report{
		Metric: "snapshot_copy+incremental_apply", Rows: measuredRows, BatchRows: *batch,
		Batches: len(samples), WarmupBatches: *warmup,
		RowsPerSec:               float64(measuredRows) / elapsed.Seconds(),
		RawCopyCeilingRowsPerSec: snapshotRowsPerSec,
		ElapsedSec:               elapsed.Seconds(), SetupMS: setupMS,
		NumCPU: runtime.NumCPU(), GoVersion: runtime.Version(),
		CPUModel: cpuModel(),
		Note: "快照=目标端 COPY；增量=单连接 apply（数据+位点同事务），均不含源端解码与网络；" +
			"p99 是增量批事务延迟的最近秩，不是单行延迟",
	}
	rep.P50MS, rep.P99MS, rep.MaxMS = percentiles(samples)

	if *jsonOut != "" {
		if err := writeJSON(*jsonOut, rep); err != nil {
			fmt.Fprintf(os.Stderr, "benchapply: 写 %s 失败: %v\n", *jsonOut, err)
			os.Exit(2)
		}
	}
	fmt.Printf("raw COPY ceiling (非门禁): %.0f rows/s (%d rows)\n", rep.RawCopyCeilingRowsPerSec, *rows)
	fmt.Printf("incremental apply: %.0f rows/s, p50 %.2f ms, p99 %.2f ms, max %.2f ms "+
		"(%d rows, batch %d, %d batches, setup %.0f ms)\n",
		rep.RowsPerSec, rep.P50MS, rep.P99MS, rep.MaxMS, rep.Rows, rep.BatchRows, rep.Batches, rep.SetupMS)
}

func percentiles(ds []time.Duration) (p50, p99, max float64) {
	if len(ds) == 0 {
		return 0, 0, 0
	}
	sorted := append([]time.Duration(nil), ds...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
	rank := func(p float64) int {
		idx := int(float64(len(sorted))*p+0.9999) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(sorted) {
			idx = len(sorted) - 1
		}
		return idx
	}
	return ms(sorted[rank(0.50)]), ms(sorted[rank(0.99)]), ms(sorted[len(sorted)-1])
}

func writeJSON(path string, rep report) error {
	b, err := json.MarshalIndent(rep, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(b, '\n'), 0o644)
}

func cpuModel() string {
	b, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(line, "model name") {
			if _, v, ok := strings.Cut(line, ":"); ok {
				return strings.TrimSpace(v)
			}
		}
	}
	return ""
}

// measureSnapshot loads rows through COPY, the same primitive pgsink uses for
// the initial snapshot.
func measureSnapshot(ctx context.Context, ep dsn.Endpoint, ref source.TableRef, rows int) (float64, error) {
	conn, err := pgx.Connect(ctx, ep.PostgresURL())
	if err != nil {
		return 0, err
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "TRUNCATE TABLE "+pgwireQualified(ref)); err != nil {
		return 0, err
	}
	batch := make([][]any, 0, 5000)
	start := time.Now()
	for i := 1; i <= rows; i++ {
		batch = append(batch, []any{int64(i), fmt.Sprintf("row-%d", i), "12.34"})
		if len(batch) == 5000 {
			if _, err := conn.CopyFrom(ctx, pgx.Identifier{ref.Schema, ref.Table},
				[]string{"id", "name", "amount"}, pgx.CopyFromRows(batch)); err != nil {
				return 0, err
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if _, err := conn.CopyFrom(ctx, pgx.Identifier{ref.Schema, ref.Table},
			[]string{"id", "name", "amount"}, pgx.CopyFromRows(batch)); err != nil {
			return 0, err
		}
	}
	return float64(rows) / time.Since(start).Seconds(), nil
}

// pgwireQualified avoids importing pgwire for one identifier join.
func pgwireQualified(ref source.TableRef) string {
	return `"` + ref.Schema + `"."` + ref.Table + `"`
}
