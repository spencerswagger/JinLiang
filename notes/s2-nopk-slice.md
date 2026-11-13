# S2（Core）落地：改/删 → 按表暂停 + PG-06 源端前置（2026-11-12）

真源：`docs/DESIGN-NO-PRIMARY-KEY.md`（合入 `280ac67`），切片归属由 Ark 裁决：
**S1→Data、S2→Core、S3→Ark、S4→Gate**；S2 里 pgsink 那半归 Core，mysqlsink 那半归 Data
（两片都要动 `mysqlsink/apply.go`，两人并行必撞车）。

## 落了什么

| 位置 | 行为 |
|---|---|
| `internal/pgsink/paused.go` | `PauseReasonNoPKWrite = "no_pk_write"` + `NoPKWriteError{TableName, Detail}`（复用 `PausableError`，**不新增接口**；mysqlsink 直接 import 用） |
| `pgsink.Apply` | **写任何一行之前**扫事件：无主键表出现 `EvUpdate`/`EvDelete` → 整批返回 `NoPKWriteError`（§6 红线 6）；`INSERT`/`TRUNCATE` 不受影响（insert-only 照跑） |
| `pgsink.deleteTx` | 第二道防线：裸错误 → `NoPKWriteError`（按表暂停，而不是一句错误打死整轮） |
| `engine.classifyError` | `*pgsink.NoPKWriteError` → `E1017`，**排在泛化 `PausableError` 之前**（否则被 E1015「结构不一致」吸走，用户会去对齐一个本来就对的结构） |
| `preflight` `SCOPE-02` | 四档：`once`/`full_interval` ok、`continuous*`/空 warn、`interval` **blocked**（挂 E1017，空码会回落 E1001 把用户引去 wal_level）；PG 源的 continuous 卡片同时点出 RI 前置 |
| `preflight` `PG-06` | PG 源 + 增量 + 无主键表 `ReplicaIdentity ∉ {full,index}` → warn + 可执行 `ALTER TABLE … REPLICA IDENTITY FULL`；分区表**不代做**（D10）只给模板；三条路（DBA 代跑 / 只跑全量 / 移出表）+ WAL 代价 |

引擎侧**零改动**：`pauseSink`/`registerPause`/`resumeModeFor`/`ExitNeedsAction` 都按 `PausableError` 通用通道工作（Ark 裁决第 4 点）。只加了断言。

## 证据

- 单测（无库）：`pgsink` 3 条（改/删拦住、INSERT 照跑、deleteTx 仍可暂停）；`engine` 2 条（只停该表 + 跳过记账 + 原因码 no_pk_write；登记失败＝整批失败）；`preflight` 3 条（SCOPE-02 四档、PG-06 六种形态、PG 源卡片带 RI 提示）。
- 真库集成（`-tags=integration`，本机 PG 17）：`TestNoPKUpdatePausesOnlyThatTableAndRecovers` —— 首搬 300 行 → 源端 UPDATE → `reason=no_pk_write`、`skipped_rows≥1`、位点前进、退出码 3、其它表那行照写、目标端无主键表**不多出一行**（没把 UPDATE 降级成 INSERT）→ 两端加主键后重跑：`RowsCopied=300`、暂停解除、两端指纹一致、`v=1042` 可见。
- 自检：`gofmt -l .` 空 / `go vet ./...` / `go test ./...` / `validate_schema examples/*.yaml` 全绿。

## 悬着的一件事

`E1017` 的码表条目（`core.CodeNoIdentity` + `ERRORS.md`）由 Ark 落。接线现在用
`engine.codeNoIdentity = core.Code("E1017")` 与 `preflight` 里的同值字面量（各带注释）；
Ark 的常量到位后把这两处换成常量即可，语义一字不变。Ark 裁决：E1017 是**族码**
（「无身份导致做不到」= 运行时改/删 + 预检 interval 必然重复），不另开号。

## 不在本片（别顺手做）

- `mysqlsink` 的 insert-only SQL 与判定（S1，Data）；`unique_key` 全套（S3，Ark）；矩阵格（S4，Gate）。
- `docs/SPEC.md:91` PG-06 拆两句、`docs/CONNECTOR-SDK.md` `UniqueKeys`：§4.5 清单归 S3。
