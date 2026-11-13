# MyISAM 源端预检：明确降级（不是拒绝），且快照阶段不再假绿

夜班 2026-11-13，任务 1/2。分支 `feat/myisam-preflight-degrade`，基于 `origin/main` = `6f5e2d1`。

> 派工时说 main 是 `4e06b61`、CI 8/8。实际 `origin/main` 已经到 `6f5e2d1`：
> `#113`（`45ef9b0` MY-08 口径统一 + MyISAM 源端预检 warn）和 `#114`（`0143986`
> MyISAM 矩阵 smoke）早已在 main 上。所以本件不是从零实现，而是把「降级」这件事
> 补成**可判、可引用、不假绿**：之前预检只是 warn 一句话，收尾再也没人提它，
> 而文案里写着「这几张表会被标注」——那句话当时是空的。

## 结论（先看这个）

- 判定：**降级**（warn，照跑），**不是拒绝**。理由：MyISAM 读得到、搬得动，
  只是没有事务；`docs/DESIGN.md` 的「不拒绝，只提示」不允许因为风险阻断。
- 「假绿」的定义在这件里落成一条硬约束：**只要这一轮有 MyISAM 表，收尾就必须说
  它不是一致性切面**——事件流一次 + `Result.Note`（CLI 的「说明：…」）一次。
  「全量完成 + 退出码 0」不再单独出现。

## 判据（任务 2 的文档按这三个值逐条对齐）

`internal/preflight/engine.go`，**全仓唯一**判据：

```go
type EngineVerdict string
const (
    EngineTransactional    EngineVerdict = "transactional"      // 有事务 → ok，不发 MY-08
    EngineNonTransactional EngineVerdict = "non_transactional"  // MyISAM 家族 → 降级（warn 照跑）
    EngineNotJudged        EngineVerdict = "not_judged"         // 非 MySQL 源 / 没读到表清单 → 不判
)
func JudgeEngines(sourceKind string, cat *source.Catalog, selected []string) EngineJudgement
func EngineDegradeNote(j EngineJudgement) string   // 降级文案的唯一来源
```

- 消费者只有两个：`preflight.engineChecks`（MY-08 卡片）和 `engine.preflight`
  （收尾标注）。两处都只读 `JudgeEngines`，不再各写一份「Engine 里含 MYISAM」。
- 口径边界（刻意收窄，改它先改 `docs/SPEC.md` MY-08）：
  - 算无事务：`MyISAM` / `MRG_MyISAM`（引擎名含 MYISAM 的都算，覆盖变体）；
  - **暂不判**：`MEMORY` / `CSV` / `ARCHIVE`（同样没事务，但 SPEC 只写了 MyISAM，
    放宽要 Ark 拍板）。这是本件留下的**已知口子**，任务 2 定口径时一起收。
  - `not_judged` 不冒充 ok：SourceCat 没读到时 MY-08 不发，但判据不说「没问题」。

## 实测（真库，MySQL 8.0.28 / binlog ROW / MyISAM + InnoDB 各一张）

前置：

```
CREATE TABLE jl_myisam_demo (id INT PRIMARY KEY, note VARCHAR(32)) ENGINE=MyISAM;
CREATE TABLE jl_innodb_demo (id INT PRIMARY KEY, note VARCHAR(32)) ENGINE=InnoDB;
```

命令原文（`JINLIANG_SOURCE_DSN` = mysql://…/jinliang_src，目标 PG `jinliang_test`）：

```
./.work/jinliang run --job myisam-demo --resync --yes --snapshot-only
```

退出码 **0**。文案原文（同一句话在事件流与「说明」里各出现一次）：

```
[note] MY-08 部分表用 MyISAM 引擎（无事务）：降级：jl_myisam_demo(MyISAM) 用的是无事务引擎（MyISAM）——本轮照跑、不拒绝，但**不是一致性切面**：全量读取和位点不在同一个时间点，崩溃或并发写入时目标端可能与源端不一致；verify 全绿也不代表崩溃后仍然一致；要消除这个代价就把这几张表改成 InnoDB（见预检 MY-08）
...
[全量初始化] 降级：jl_myisam_demo(MyISAM) 用的是无事务引擎（MyISAM）——本轮照跑、不拒绝，但**不是一致性切面**：…
结果：阶段 done，复制 2 行，增量 0 行，用时 100ms，退出码 0
说明：只搬了存量数据（mode.frequency=once）；降级：jl_myisam_demo(MyISAM) 用的是无事务引擎（MyISAM）——…
```

对照（InnoDB 源，证明不误伤）：

```
./.work/jinliang run --job innodb-demo --resync --yes --snapshot-only
=> 退出码 0；无 MY-08；说明：只搬了存量数据（mode.frequency=once）
```

改前基线（同一台库、同一条命令，`origin/main` 的二进制）：
预检只发一句「…同步照跑，这几张表会被标注」，收尾只有「全量完成」+ 退出码 0，
**没有任何标注**——那句话当时没有实现，属于假绿。

## 自检

```
gofmt -l .                      → 空
go vet ./...                    → 通过
go test ./...                   → 全 ok（preflight 0.013s / engine 1.773s）
python3 scripts/validate_schema.py examples/*.yaml → ok ok
```

新增单测：`internal/preflight/engine_test.go`（判据三态 7 格 + 文案四段 + MY-08 卡片）、
`internal/engine/engine_degrade_test.go`（降级必须进 Result.Note，且不误伤）。

## 留给任务 2 / Ark 的三件事

1. `docs/DESIGN-NO-PRIMARY-KEY.md:126` 引的 `preflight/b0.go:313-352` 行号已经漂移（现 474+）。
2. MY-08 的口径文字建议改成「非事务引擎（MyISAM / MRG_MyISAM）」并引用
   `EngineVerdict` 三态；`MEMORY/CSV/ARCHIVE` 要不要并进来需要拍板。
3. `internal/mysqlsource/capability.go` **不适合**放这个判据：`Prober` 只有服务器级
   变量与 grant，拿不到 Catalog 的 `Table.Engine`；判据留在 preflight，文档引用
   `EngineVerdict` 即可，不要让 capability.go 再长一份。
