# CI 耗时压缩（PR #71）

## 改前 / 改后（同两个 job，GitHub Actions 实测秒数）

| job | 改前 | 改后 | 变化 |
|---|---:|---:|---:|
| control-plane | 329s | 232s | **-97s（-29.5%）** |
| b1-engine | 158s | 139s | **-19s（-12%）** |

### control-plane 明细

| 步骤 | 改前 | 改后 | 说明 |
|---|---:|---:|---|
| 控制面单元/接口测试 | 105s | 88s | 波动（同命令） |
| headless 全链路 | 96s | 83s | 波动（同命令） |
| 交叉编译三平台 | 71s | **0（移除）** | 移到 nightly `cross-compile` |
| Initialize containers | 27s | 28s | 不变 |
| 起 PostgreSQL | 11s | 15s | 波动 |
| setup-go | 11s | 10s | 开启 `cache: true`（模块缓存，收益被波动淹没） |

### b1-engine 明细

| 步骤 | 改前 | 改后 | 说明 |
|---|---:|---:|---|
| 1M/300k 端到端 | 52s | 45s | 波动 |
| 引擎单元测试 | 39s | 31s | 波动 |
| 起两个 PostgreSQL | 11s | 17s | 波动 |
| kill -9 重搬端到端 | 17s | 17s | 不变 |
| 真库集成测试 | 14s | 13s | 不变 |
| pgsink 吞吐 benchmark | 10s | **0（移除）** | 移到 nightly bench job |

## 做了什么（不丢覆盖）

1. **PR 不再跑三平台交叉编译**：`control-plane` 删除该步，nightly 新增 `cross-compile` job；
   PR 只保证 linux/amd64 能编译，平台覆盖由 nightly/release 守。
2. **PR 不再跑 pgsink `BenchmarkApplyUpsert500`**：`b1-engine` 删除该步，nightly `bench`
   job 增加同一阈值门禁（≥20k rows/s）；性能覆盖不丢。
3. 所有 `actions/setup-go@v5` 加 `cache: true`（go.sum 依赖缓存）；实测收益很小
   （模块下载本来就不慢），保留但不夸大。
4. 未动 e2e/集成用例内容；矩阵覆盖不变。

## 还能压的（下一步，不在本 PR）

- `control-plane` 单元/接口测试 88s 里 `internal/httpapi` 占大头；可按包拆成两个步骤并行。
- e2e 与 b1 e2e 都在 job 内重新 build；可上传 build artifact 复用（约几秒级，优先级低）。
- Go build cache 不是 setup-go 的模块缓存；可加 `actions/cache` 缓存 `~/.cache/go-build`。

## backlog（记下不做）

`internal/engine` 与 `internal/pgsink` 的集成用例共用目标端 `_jl_*` 表，并行跑会互踩；
二选一：`-p 1` 或按包独立 schema（与 verify/gc/remedy 同样的解法）。
