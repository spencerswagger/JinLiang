# CI 耗时压缩（#71 重放版，PR #79）

原 #71（分支 `chore/ci-duration`）基于 `43554b1` 之前的 main，与 main 冲突 1 文件（`ci.yml`）。
 #77 又把 `nightly.yml` 整体重写了（PG 改 `docker run` 带 `wal_level=logical`、四个 job 补库 +
 前提断言），所以这里**不是机械 rebase，而是把 #71 的同一份意图重放到新结构上**，并补上门禁加固。

前置条件：#77 上 nightly 连续三次 15/15 success（`42748322122` / `42746733746` / `42747168318`），
不存在「把门禁搬进一个长期红的 job」的问题。

## 改前 / 改后（GitHub Actions 实测秒数，来自原 #71 的两次对照跑）

| job | 改前 | 改后 | 变化 |
|---|---:|---:|---:|
| control-plane | 329s | 232s | **-97s（-29.5%）** |
| b1-engine | 158s | 139s | **-19s（-12%）** |

明细见原 #71 的对照（同命令、同 runner 类型）：

- `control-plane`：交叉编译三平台 **71s → 0（移除）**；
- `b1-engine`：pgsink `BenchmarkApplyUpsert500` **10s → 0（移除）**；
- 其余差异都在 ±5~10s 的跑动噪声内。

## 做了什么（不丢覆盖）

1. **PR 不再跑三平台交叉编译**：`control-plane` 删除该步，nightly 新增 `cross-compile` job
   （linux/amd64、linux/arm64、darwin/arm64，实测各 ~20s）。PR 只保证本机 linux/amd64 能编译，
   平台覆盖由 nightly + `release.yml` 守。
2. **PR 不再跑 pgsink 吞吐 benchmark**：`b1-engine` 删除该步，nightly `bench` job 增加同一道
   阈值门禁（≥20000 rows/s）。性能覆盖不丢。
3. **门禁加固成「不许静默通过」**（这是本 PR 相对原 #71 的新增）：
   - `set -o pipefail`：原写法是 `set -e` + `go test … | tee`，go test 自己失败会被 `tee` 吞掉；
   - `rate` 为空（没测到 `batch=500` 那一行）时**显式失败**，不做成 `r+0 = 0` 的静默通过；
   - 阈值收敛成一个 `THRESHOLD` 变量，判定和文案同源（改阈值不会漏改文案）。
4. **`cache: true`**：所有 `setup-go@v5` 显式写出来。**注意**：v5 的默认值本来就是 `true`，
   这条等于显式化，实测收益≈0（模块下载本来就不慢）——保留但不夸大，别把它算进省下来的 116s。
5. 未动 e2e/集成用例内容；矩阵覆盖、用例集不变。

## 验证（都在 Actions 上跑）

### 1）clean：门禁通过

- nightly `42750487707`（head `dd14141`）**success 15/15**，`03:35:57Z → 03:39:40Z`（3m43s）。
- `bench`：`批量路径吞吐: 227625 rows/s（阈值 20000）`；
- `cross-compile`：三个平台各 ~20s，绿。
- PR 的 `ci` `42750469552`（同 head）**7/7 success**：build / race / testmatrix / schema /
  control-plane / b1-engine / b4-mysql-source。

### 2）变异 A：阈值抬高 → 门禁必须红

`THRESHOLD=20000 → 500000`（只改这一个值，其余脚本逐字相同），nightly `42750502507`
（head `4ae1f74`）**failure**，红在 `bench`：

```
批量路径吞吐: 112833 rows/s（阈值 500000）
❌ 批量 upsert 吞吐 112833 低于阈值 500000 rows/s
##[error]Process completed with exit code 1.
```

### 3）变异 B：漏配置 → 必须响亮失败（不是 Skip）

把 `bench` job 里 PG 的 `-c wal_level=logical …` 去掉（模拟漏配置），nightly `42749328235`
（head `f1e8778`）**failure**，红在 `bench` 的前提断言：

```
PG: wal_level=replica
❌ PG 的 wal_level=replica（要 logical）；缺了它 B1 引擎建逻辑书签会报 SQLSTATE 55000
```

### 4）CI 上的吞吐方差（阈值为什么设 20000）

同一 benchmark 在 GitHub runner 上实测波动很大：`112833 / 124491 / 156284 / 219376 / 227625`
rows/s（1.7~2.0 倍差）。阈值 20000 相当于**最慢观测值的 5 倍余量**，是「崩塌才红」的峰值门禁，
不是性能基线（基线在 `scripts/bench.sh` 的 p99/内存/冷启那几项）。

## 还能压的（下一步，不在本 PR）

- `control-plane` 的单元/接口测试 88s（`internal/httpapi` 占大头）：可按包拆成两个步骤并行。
- e2e 与 b1 e2e 都在 job 内重新 build：可上传/下载 build artifact 复用（秒级收益，优先级低）。
- `setup-go` 的 `cache: true` 是模块缓存，不是 build cache；要压编译时间得加
  `actions/cache` 缓存 `~/.cache/go-build`。

## backlog（记下不做）

`internal/engine` 与 `internal/pgsink` 的集成用例共用目标端 `public._jl_*` 表，包级并行会互踩
（本机 `-p 2` 实测能撞出 `relation "public._jl_chunk" does not exist`）。main 的 `b1-engine`
已经用 `-p 1`；nightly 的 `integration` 是逐包串行调用，所以也没踩。要么统一 `-p 1`，
要么给 `internal/engine` 单独的 state schema——后者更彻底，但要改 pgsink 的 schema 选择。
