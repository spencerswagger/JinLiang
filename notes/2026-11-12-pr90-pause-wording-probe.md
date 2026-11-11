# #90 第二轮：预检/轮次提示口径探针（2026-11-12，分支 `contract/pause-and-position`）

Owl II 复检剩下的 3 条必改 + 1 条 P2，全在「预检卡片 / 轮次提示」的措辞上。下面是**实跑输出**，
不是推导。环境：本机 PG 17.11（源 `postgres://…/jinliang_src` → 目标 `…/jinliang_test`，wal_level=logical）；
MySQL 3306 本轮没起，所以 MySQL **源**只有预检层的探针（预检纯输入，不需要连库），没有真跑轮次。

## 1. 预检 PAUSE-01：3 mode × 2 源 × 2 目标（12 格，实跑 `preflight.Run`）

| 源 | 目标 | mode | 卡片 | 标题 |
|---|---|---|---|---|
| postgres | postgres | continuous+initial | **ok** | 目标端支持按表暂停 |
| postgres | postgres | full_interval | **ok** | 目标端支持按表暂停 |
| postgres | postgres | interval | **warn** | 定时增量不做按表暂停 |
| postgres | mysql | continuous+initial | **ok** | 目标端支持按表暂停 |
| postgres | mysql | full_interval | **ok** | 目标端支持按表暂停 |
| postgres | mysql | interval | **warn** | 定时增量在 MySQL 目标端没接 |
| mysql | postgres / mysql | continuous+initial、full_interval | **ok** | 目标端支持按表暂停 |
| mysql | postgres / mysql | interval | **warn** | 定时增量不做按表暂停 / 定时增量在 MySQL 目标端没接 |

原文（节选）：

```
[src=postgres tgt=postgres mode=continuous+initial] PAUSE-01 ok      目标端支持按表暂停
    结构对不上的表会被记录并只停它自己：其它表照常同步、位点照常前进，跳过的那段在结构对齐后由整表重搬补齐
[src=postgres tgt=postgres mode=interval          ] PAUSE-01 warn    定时增量不做按表暂停
    `frequency=interval` 这一版**没有**「只停这张表」这条通道（水位轮询按页读写，没有「跳过该表、位点照常前进」的位置）：表结构对不上时会照旧写这张表，多半直接报错。先手工把目标端结构对齐再跑；要做到「只停一张表 + 结构对齐后自动整表重搬补齐」，请改用 continuous+initial 或 full_interval
[src=postgres tgt=mysql mode=continuous+initial] PAUSE-01 ok      目标端支持按表暂停
    结构对不上的表会被记录并只停它自己（记在目标端 `_jl_meta.paused_tables` 那一列；老库缺列时开跑会先补，补不上会明确失败并说明原因）：其它表照常同步、位点照常前进，跳过的那段在结构对齐后由整表重搬补齐
[src=postgres tgt=mysql mode=interval          ] PAUSE-01 warn    定时增量在 MySQL 目标端没接
    `frequency=interval` 在 MySQL 目标端这一版**整体没接**（水位轮询只有 PG 目标端实现，开跑会被明确拒绝并给出替代模式），所以「只停这张表」也无从谈起。改成 continuous+initial / once / full_interval 之后按表暂停是生效的：只停这张表、其它表照常、位点照常前进，结构对齐后整表重搬补齐
```

MySQL 目标端那格从 `warn`+「引擎侧的『只停这张表』还没接」改成 `ok`+「已接」——因为 `#102` 把
`prepareGeneric` 的 `pauseReg` 绑定与恢复四步都合了（`target.go:142-149` / `pause_generic.go`）。

## 2. 轮次提示：三种 mode 真跑（PG → PG，两轮：先建表，再把目标列改成 text 制造漂移）

复现命令：`BIN=./dist/jinliang scripts/probe_pause_wording.sh`（提交在本 PR 里；每轮 `import` → `run` →
`ALTER TABLE t_drift ALTER COLUMN amount TYPE text` → 再 `run`；缺 DSN 以 78 退出，不会静默通过）。

### continuous+initial（rc=0）

```
[准备对象] 表 t_drift 的结构和源端对不上（type_changed）：本轮暂停这张表，其它表继续
[准备对象] 表 t_drift 的结构和源端对不上（type_changed）：暂停这张表，本轮不搬它的存量，其它表照常跑；结构对齐后重跑会整表重搬补齐
[准备对象] 表 t_drift 暂停中：本轮不搬它的存量（结构对齐后重跑会自动整表重搬）
paused_tables: {"t_drift":{"reason":"type_changed","at":"…","detail":"…列 amount：目标端当前是 text，源端是 numeric(12,2)","resume":"recopy"}}
```

### interval（rc=0，**只有 interval 专属那句**，没有「本轮暂停这张表，其它表继续」）

```
[note] PAUSE-01 定时增量不做按表暂停：`frequency=interval` 这一版**没有**「只停这张表」这条通道（…）：表结构对不上时会照旧写这张表，多半直接报错。先手工把目标端结构对齐再跑；要做到「只停一张表 + 结构对齐后自动整表重搬补齐」，请改用 continuous+initial 或 full_interval
[准备对象] 表 t_drift 的结构和源端对不上（type_changed）：interval 模式不做按表暂停，本轮会照旧写它（结构没对齐时通常直接失败）；先手工对齐目标端结构再重跑，或改用 continuous+initial
[实时同步中] 定时增量：本轮搬了 70 行（水位列 updated_at）
[已完成] 定时增量本轮完成（水位列 updated_at）
paused_tables: {"t_drift":{"reason":"type_changed","…","resume":"recopy"}}   ← 记录还在，但 interval 的写路径不读它
```

这格还实测到 §1.6 例外段写的那半句：**能隐式转换的类型会照旧写进去**（numeric → text 走 I/O 转换，
70 行全写进去了、rc=0）——不是「整轮失败」，所以文案不能一口咬定「会报错」，只能说「多半直接报错」。
修复前这里有两句自相矛盾的话（pgsink 的 Action「本轮暂停这张表，其它表继续」+ 新增的 interval note），
现在 sink 那条 Action 在 interval 下被抑制（`engine.go:972-980`），只剩一个声音。

### full_interval（rc=0）

```
[准备对象] 表 t_drift 的结构和源端对不上（type_changed）：本轮暂停这张表，其它表继续
[准备对象] 表 t_drift 的结构和源端对不上（type_changed）：暂停这张表，本轮不搬它的存量，其它表照常跑；结构对齐后重跑会整表重搬补齐
[准备对象] 表 t_drift 暂停中：本轮不搬它的存量（结构对齐后重跑会自动整表重搬）
[准备对象] 这一轮按模式整轮重搬（once / full_interval 每轮重建，目标端结果等于源端当前状态）
```

> 遗留观察（没改，超出这轮范围）：批式模式下 pgsink 的 Action「本轮暂停这张表，其它表继续」与引擎的
> note 是同一件事的两种说法（后者更全）。要合并只需把 `pause:` Action 的抑制条件从 `interval` 放宽到所有
> 模式，但那会改变 plan 预览里的动作清单，留给后续。

## 3. P2（`docs/ERRORS.md:25` E1014）——**前提要更正**

任务里说「`direction.go:36-42` 四个方向都在 supported 里」，实测**不是**：`supportedDirections` 三条
（pg→pg、pg→mysql、mysql→pg），`pendingDirections` 一条（mysql→mysql），`TestSupportedDirections` 明确断言
`{mysql, mysql} → false`。所以 E1014 的真问题是「**漏了 mysql→pg 已接通**」，不是「四个都接通」。

已按代码真值重写那一行（用 `core.Lookup` 打印的实文逐字抄），并加 `TestErrorsDocE1014MatchesDirectionMatrix`
读 `docs/ERRORS.md` 校验它包含 `DirectionsText()` 与 `PendingDirectionsText()+" 还没接通"`，以后改矩阵不改文档必红。

```
当前能跑的是：PostgreSQL → PostgreSQL（全量 + 增量）、PostgreSQL → MySQL（只全量）、
MySQL → PostgreSQL（全量 + 增量（continuous / once / full_interval；interval 暂不支持））；MySQL → MySQL 还没接通
```

**给主持人的一条交叉信号**：Gate 的 A 矩阵声称 MySQL→MySQL 实跑 20000=20000 通过，但 main 上
`SupportsDirection("mysql","mysql") == false`（`direction_test.go` 断言 false，`httpapi/runctl.go:200` 会拦）。
两者必有一个不成立：最可能是 `#110` 的 base `jinliang/merge-gate-59` 上带着自己的 direction 补丁，
那样合回 main 之后那格会被 E1014 直接拦住。这不是 #90 的范围，别让它带着走。

## 4. 自检（本机）

- `gofmt -l .` 空；`go vet ./...` 绿。
- `go test ./...`：除 `internal/connect`（`TestMySQLProbeAndCatalog`，本机 MySQL 3306 没起）之外全绿。
- `go test -tags integration ./internal/engine -run 'Pause|Reconcile|Recopy'`：19 个用例，PG 侧全绿
  （含 `TestReconcilePausedMarksRecopyAndKeepsRecord`、`TestPauseTableRegistersKeyKeepsPositionAndRecovers`、
  `TestForcedRecopyIsNotChunkResume` 等）；3 个 MySQL 目标的用例因 3306 没起而红（`dial tcp 127.0.0.1:3306:
  connect: connection refused`），由 CI 的 `b1-engine`/`b4-mysql-source` 覆盖。
- `scripts/validate_schema.py examples/*.yaml` 全 ok。
