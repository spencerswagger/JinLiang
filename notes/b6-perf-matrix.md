# B6 · 性能基线 + 覆盖度 + 三门禁

分支 `feat/b6-perf-matrix`（基线 `8f87211`）。范围：`scripts/`、`.github/workflows/`、
README、`testmatrix.yaml`、集成测试夹具；未动 `internal/engine` / `internal/mysqlsink` /
`internal/httpapi` / `catalog/typemap.go` / `core/codes.go`。

## 1) 性能基线（可重复跑）

`scripts/bench.sh` 固定规模：10 万行、每批 500、2 批预热；快照每块 5000。五项实测：

```
指标                 实测         目标  结论
吞吐(快照)    98642 行/s      ≥ 20000  PASS
p99 延迟           81.77 ms     ≤ 100 ms  PASS
空转内存          15.6 MB      ≤ 80 MB  PASS
冷启                  27 ms     ≤ 200 ms  PASS
二进制            19.12 MB        < 25 MB  PASS
```

- 快照吞吐走**产品路径 `pgsink.BulkApply`**（临时落地区 → COPY → `INSERT…SELECT`
  → 块记账 + 位点同事务），不是裸 COPY；裸 COPY 只作为上限参考（~176k 行/s），
  不进门禁——否则 BulkApply 慢 10 倍也能 PASS（Owl #28 必改）；
- p99 是**增量批事务**（数据+位点同事务）延迟的最近秩，200 个样本，3 次取中位；
- 空转 = `ps` 的 **RSS**（不是 Go heap）；冷启 = exec → `/api/health` 返回 200；
  两者都是 `serve` + 内嵌 SQLite 的真实启动路径，3 次取中位；
- `bench/baseline.json` 入库；`scripts/bench_compare.py` 做 >15% 回归对比
  （CPU 型号不同时只查绝对阈值，跨机器比吞吐没有意义）。
- **已知差距**：增量 apply 约 10.5k 行/s（pgsink 每行一次 Exec，500 次/事务）。
  20k 目标是 DESIGN §7A.1 的**快照**吞吐；增量差距写进 README 与报告，不藏。

## 2) testmatrix：9 PG × 4 MySQL

- PG：9 个版本、81 组合；required = 9 自洽 + 8 相邻 + 2 极端 = 19；nightly = 81 全量；
- MySQL：4 个版本、16 组合；required = 4 + 3 = 7；nightly = 16；
- 异构：9×4×2 = 72，全部 nightly（PR 不跑）；
- `scripts/check_testmatrix.py` 在 PR 的 schema job 里校验版本数/组合数/场景声明：

```
testmatrix: postgres 9/9 (81), mysql 4/4 (16), cross 72 — 分层正确
```

## 3) 覆盖度报表进 README

`scripts/coverage.sh` 生成包级表并写回 README（标记 `coverage:start/end`）：

```
coverage: 总覆盖 44.1% ≥ 门槛 40%
```

口径写在 README：`engine/pgsource/pgsink/mysqlsource/runtime/remedy` 的真实覆盖靠
`-tags=integration`（nightly），单元数字偏低不代表没测。

## 4) GHA 三门禁（阈值明确）

| 门禁 | 文件 | 内容与阈值 |
|---|---|---|
| PR | `.github/workflows/ci.yml` | gofmt/vet/build/unit、testmatrix 校验、schema、control-plane e2e、race、集成（verify/gc/doctor/remedy，缺 DSN 红）、二进制 <25MB |
| nightly | `.github/workflows/nightly.yml` | PG 10–18 逐个真库 + MySQL 5.7/8.0/8.4/9.4 逐个真库、全量集成（含 engine/mysqlsource）、性能基线 `--enforce`（20k/p99≤100ms/≤80MB/≤200ms/<25MB） |
| release | `.github/workflows/release.yml` | 性能回归 >15% 红（`bench_compare.py`，CPU 不同则跳过回归只查绝对阈值）、二进制 <25MB、覆盖度 ≥40% |

## 清掉 `-p 1`

集成测试改成每包独立 schema：`jl_test_verify` / `jl_test_gc` / `jl_test_remedy`
（doctor 用 `jl_test_doctor` 只读），`go test -tags=integration ./internal/...`
可以并行跑：

```
ok internal/engine / mysqlsource / verify / gc / remedy / doctor …
```

CI 与 nightly 已去掉 `-p 1`。

## 验证命令

```
gofmt -l . ; go vet ./... ; go test ./...
go test -tags=integration ./internal/...
python scripts/check_testmatrix.py
scripts/coverage.sh --check --min 40
JINLIANG_TEST_PG_DSN=... scripts/bench.sh --enforce
```
