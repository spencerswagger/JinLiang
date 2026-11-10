# CI 耗时压缩（#71 重放版，PR #79）

原 #71（分支 `chore/ci-duration`）基于 `43554b1` 之前的 main，与 main 冲突 1 文件（`ci.yml`）；
`#77` 又把 `nightly.yml` 整体重写了（PG 改 `docker run` 带 `wal_level=logical`、四个 job 补库 +
前提断言）。所以这里**不是机械 rebase，而是把 #71 的同一份意图重放到新结构上**，并按 Owl II 复审意见修了两处。

前置条件：nightly 已经真绿（#77 上连续多次 15/15 success），不存在「把门禁搬进一个长期红的 job」。

## 一、平台覆盖：改成 ci.yml 的**独立并行 job**（Owl II 必改 1）

第一版把三平台交叉编译挪到 nightly，并在 notes 里写「平台覆盖由 nightly + release.yml 守」——**这句是假的**：
`release.yml` 里 `GOOS|GOARCH|goreleaser` 零命中，`size` job 只 `check_binary_size.sh dist/jinliang`（内部也是本机 `go build`）。
后果：某个 PR 把 arm64/darwin 构建搞坏（CGO、平台专属 syscall、`//go:build` 写错），PR 绿、合并绿、打 tag 也绿，
要等下一次 nightly 才红（最长滞后一天，且不阻断发版）。

现在选的做法：**`ci.yml` 里新增独立的 `cross-compile` job**（不是 control-plane 末尾那一步）——
PR wall-clock 基本不变（与 control-plane 并行），但「arm64/darwin 编不过」在 PR 阶段就红；
`nightly.yml` 里那个 `cross-compile` job 删掉（不重复跑）。
不选「加到 release.yml」：打 tag 时才验 = 承认发版不验证平台构建。不选「只改描述」：那是把缺口写进文档就当解决。

## 二、改前 / 改后（GitHub Actions 实测秒数，来自原 #71 的两次对照跑）

| job | 改前 | 改后 | 变化 |
|---|---:|---:|---:|
| control-plane | 329s | 232s | **−97s** |
| b1-engine | 158s | 139s | **−19s** |

**口径更正**：可归因的是 **71s（交叉编译移出 control-plane）+ 10s（benchmark 移出 b1-engine）= 81s**；
「116s」是上面两次对照跑里 `control-plane −97s` 与 `b1-engine −19s` 的合计，不是 71+10。
剩下的差异是跑动噪声（同命令 ±5~10s），别当收益。

`control-plane` 那 97s 里：交叉编译 71s 是直接原因；其余在噪声带内（单元/接口测试 105→88s，e2e 96→83s）。

## 三、做了什么

1. **PR 不再串行跑三平台交叉编译**：`control-plane` 删除该步；改由 `ci.yml` 的独立 `cross-compile` job 跑
   （`linux/amd64`、`linux/arm64`、`darwin/arm64`，`CGO_ENABLED=0`）。`nightly.yml` 不再重复。
2. **PR 不再跑 pgsink 吞吐 benchmark**：`b1-engine` 删除该步，nightly `bench` job 增加同一道阈值门禁
   （≥20000 rows/s）。性能覆盖不丢。
3. **门禁加固成「不许静默通过」**：
   - `set -o pipefail`：原写法是 `set -e` + `go test … | tee`，go test 自己失败会被 `tee` 吞掉；
   - **`-count=5` 取中位数**：单次 40 iterations 在 shared runner 上实测 **`14474`~`174350` rows/s，约 12 倍方差**
     （早前 3 个样本时看到的「1.7~2.0 倍」是低估——合并前那次 nightly 就出现过 `14474` 与 `174350` 同轮）。
     `20000` 只是**崩塌下限**，不是性能基线。3 个样本只能容忍 1 个慢样本（同轮两个 `14474` 这种就误红），
     所以取 5 个、容忍 2 个慢样本；多数样本都慢 → 中位数也慢 → 照样红。
     **这 12 倍是「同一次 run 内、样本之间」的间歇抖动**（证据：那一轮就是 `14474 / 99135 / 174350` 同轮出现；
     `94302ea` 那轮 5 个样本只差 1.11 倍）。所以 `-count=5` 吸收的是**轮内抖动**：
     整轮被限速（5 个全慢）时中位数照样低、照样红——那是期望行为，**不是**把 `-count` 降回去的理由。
     **中位数按实际样本数取**（`n>=2` 才算数，取下半中位数；输出格式一变被正则丢掉一两个时
     不会退化成「两个里取大的」这种偏松行为），`n<2` 直接失败；
     Go 在 ≥1e6 时打印 `1.2e+06 rows/s`（也可能是 `1.5e+05`），所以样本用**数值解析**（`printf '%.0f'`）而不是硬映射——
     硬当 1e6 会把 `1.5e+05` 高估成 1e6（偏松）；解析不出来就响亮失败，不静默通过；
   - 解析不再依赖 `awk`（用 `grep -oE` + `sort -n` + `sed -n 2p`），因此**能在本地用真库验证**；
   - 没测到 `batch=500` 那一行 → 显式失败（`|| true` 让脚本走到判空，日志里有「为什么」，不是静默退出）；
   - 阈值收敛成一个 `THRESHOLD` 变量，判定和文案同源。**阈值仍是 20000**（「崩塌才红」的峰值门禁，
     不是性能基线：p99/内存/冷启在 `scripts/bench.sh`）；有了中位数之后未来可以按实测收紧，本 PR 不抬也不收。
4. **`cache: true`**：所有 `setup-go@v5` 显式写出来。**注意**：v5 的默认值本来就是 `true`，
   这条等于显式化、收益≈0（模块下载本来就不慢）——别把它算进省下来的时间。
5. 未动 e2e/集成用例内容；矩阵覆盖、用例集不变。

## 四、验证

### 1）clean：门禁通过

- nightly `42757688693`（head `8fd54d5`，当时 nightly 还是 16 个 job）**success 16/16**，3m22s；
  `bench`：`批量路径吞吐: 206074 rows/s（阈值 20000）`；`cross-compile` 三平台绿。
- 更早一次：nightly `42751218269` success **16 个 job**（15 + `cross-compile`）——之前写成「15/15」是 #77 的数，更正。
- 本 PR 的 `ci` 在 head `8fd54d5`：**7/7 success**（`42757633170`）。
- 去掉 nightly 的 `cross-compile` 后 nightly 回到 15 个 job（pg-matrix 9 + mysql-matrix 4 + integration + bench），
  平台覆盖改由每个 PR 的 `ci / cross-compile` 守（同一轮会在下面重跑结果里体现）。

### 2）变异：阈值抬高 → 门禁必须红

`THRESHOLD=20000 → 500000`（只改这一个值），nightly `42750502507`（head `4ae1f74`）**failure**，红在 `bench`：

```
批量路径吞吐: 112833 rows/s（阈值 500000）
❌ 批量 upsert 吞吐 112833 低于阈值 500000 rows/s
```

### 3）变异：漏配置 → 必须响亮失败（不是 Skip）

把 `bench` job 里 PG 的 `-c wal_level=logical …` 去掉，nightly `42749328235`（head `f1e8778`）**failure**：

```
PG: wal_level=replica
❌ PG 的 wal_level=replica（要 logical）；缺了它 B1 引擎逻辑书签会报 SQLSTATE 55000
```

### 4）门禁脚本本地验证（用真库，因为不再依赖 awk）

沙盒 PG 17.11 + `BenchmarkApplyUpsert500 -count=3` 真实输出，把 `go test` 换成 `cat` 后跑同一段脚本：

| 输入 | 中位数 | 结果 |
|---|---:|---|
| 真实三次：`57647 / 58061 / 75462` | 58061 | rc=0（通过） |
| 三次都慢：`13000 / 14000 / 15000` | 14000 | rc=1，`❌ … 中位数 14000 低于阈值 20000` |
| 没有 `batch=500` 行 | — | rc=1，`❌ 没测到 batch=500 的吞吐数值 … 这不算通过` |

## 四点五、发版前提（写下来，别靠记性）

`release.yml` **不做**交叉编译（`GOOS|GOARCH|goreleaser` 零命中，`size` job 只校验本机二进制），
平台构建由 `ci.yml` 的 `cross-compile` job 守。所以发版有一条硬前提：

**现在这条前提已经机械化了**：`release.yml` 打 tag 时会跑 `scripts/cross-compile.sh`（约 80s），
编不过直接红；`ci.yml` 的 `cross-compile` job 调**同一个脚本**——平台列表只有一个真源，
不再靠「两份 YAML 逐字相同」这种注释级同步。`docs/BUILD.md` §2.1 也写了。

仍然保留的历史前提（现在是双保险）：**tag 应从「已经在 main 上跑过 `ci`」的 commit 打**——
这样被发出去的版本在合并时就已经过一次平台构建验证；从特性分支直接打 tag 也不会再静默发出
arm64/darwin 编不过的产物（release 会拦）。

同批记下的取舍（Owl II 建议 3，本轮不做）：
- `-count=5` 已做；`-benchtime=40x` 仍是 40 次迭代，若还想更稳可延长到 `-benchtime=1s`，
  稳了之后阈值可以从 20000 收到 50000 量级。**在没做这件事之前不许抬阈值**，也不许把 20000 当性能基线；
  也**不许靠降阈值**（比如降到 10000）来解决方差——那是削弱门禁。

## 五、还能压的（下一步，不在本 PR）

- `control-plane` 的单元/接口测试 88s（`internal/httpapi` 占大头）：可按包拆成两个步骤并行。
- e2e 与 b1 e2e 都在 job 内重新 build：可上传/下载 build artifact 复用（秒级收益，优先级低）。
- `setup-go` 的 `cache: true` 是模块缓存，不是 build cache；要压编译时间得加
  `actions/cache` 缓存 `~/.cache/go-build`。

## 六、backlog（记下不做）

`internal/engine` 与 `internal/pgsink` 的集成用例共用目标端 `public._jl_*` 表，包级并行会互踩
（本机 `-p 2` 实测能撞出 `relation "public._jl_chunk" does not exist`）。main 的 `b1-engine`
已经用 `-p 1`；nightly 的 `integration` 是逐包串行调用，所以也没踩。要么统一 `-p 1`，
要么给 `internal/engine` 单独的 state schema——后者更彻底，但要改 pgsink 的 schema 选择。
（这一条已单独立项：见 `notes/integration-isolation-2026-11-10.md`。）
