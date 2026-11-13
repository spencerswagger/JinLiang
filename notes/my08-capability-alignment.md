# MY-08 落码：判「引擎有没有事务」（照 Ark #127 的表，13/13 一致）

夜班 2026-11-13 任务 2/2 下半场。分支 `feat/my08-engine-verdict`（基于 #126 `58bc6f9`）。
口径真源：Ark `docs/my08-engine-verdict` @ `e489cd7`（PR #127）+ `notes/my08-engine-verdict-spec.md`。

## 0. 一句话

判据从「引擎名里有没有 MYISAM」换成**查服务器自报的 `information_schema.ENGINES.TRANSACTIONS`**；
`MyISAM` / `MRG_MYISAM` / `MEMORY` / `CSV` / `ARCHIVE` / `BLACKHOLE` / `FEDERATED` 一律**降级**
（warn 照跑 + 收尾标注 + 各带一句特有代价），**不拒绝、不「暂不判」**；`not_judged` 只剩
「非 MySQL 源」和「没读到表清单」，且不冒充 ok。

## 1. 改了哪些文件（全部在派工范围内）

| 文件 | 改了什么 |
|---|---|
| `internal/source/contract.go` | `Catalog` 新增 `Engines map[string]bool`（键=大写引擎名，值=`TRANSACTIONS='YES'`；键缺失=服务器没说 → 按没有事务处理） |
| `internal/connect/mysql.go` | `Catalog()` 调 `readEngines` 填 `cat.Engines`（预检/控制面这条链路）；读失败不阻断 → nil map → 全落降级 |
| `internal/mysqlsource/snapshot.go` | 同上（引擎/快照这条链路，`(*Source).readEngines`）——Ark 点名的「只改一处就假绿」那一格 |
| `internal/preflight/engine.go` | `JudgeEngines` 改成查表：`tx, known := cat.Engines[upper(engine)]`；`known && tx` 才算有事务。新增逐引擎 `engineCosts`；`EngineDegradeNote` 按 SPEC 四段重写（单行、单表时直接抄那条 ALTER） |
| `internal/preflight/b0.go` | MY-08 标题改「部分表用无事务引擎」（不再写死 MyISAM） |
| `internal/mysqlsource/capability.go` + `capability_test.go` | 只改注释：MY-08 的引用从「SPEC.md:107 / MyISAM→warn」改成「SPEC §3 / 有没有事务」，并写清判据不在 Prober 里 |
| 单测 | `preflight/engine_test.go` 全量重写成 13 格对齐；`mysqlsource/engines_wiring_test.go` 结构断言两处 Catalog + 判据不许再猜引擎名；`mysqlsource/integration_myisam_test.go` 扩到 MEMORY/CSV/ARCHIVE 并断言 `cat.Engines` |

`docs/SPEC.md` / `DESIGN-NO-PRIMARY-KEY.md` **一个字没动**（以 #127 为准）。

## 2. 逐条对齐表（Ark §2 的 13 行 → 代码结果 → 文档结论）

真库实测的 `information_schema.ENGINES`（本机 MySQL 8.0.28）作为第 1 行的输入：

```
ARCHIVE NO | BLACKHOLE NO | CSV NO | FEDERATED NULL | InnoDB YES | MEMORY NO | MRG_MYISAM NO | MyISAM NO
```

| # | 能力项 / 引擎 | 代码结果（本 PR） | 文档结论（#127） | 一致？ |
|---|---|---|---|---|
| 1 | `Catalog.Engines`（判据输入） | 两处 `Catalog()` 都填；8.0.28 实测 `INNODB=true`、`MYISAM/MRG_MYISAM/MEMORY/CSV/ARCHIVE/BLACKHOLE=false`、`FEDERATED` 为 NULL→false；读失败→nil 不报错 | 以服务器 `ENGINES` 为准 | ✅ |
| 2 | `InnoDB` | `transactional`，不发 MY-08 | `transactional` | ✅ |
| 3 | `MyISAM` / `MRG_MYISAM`（含历史写法 `MRG_MyIsam`） | `non_transactional`，`Names=["legacy"]`、`Tables=["legacy(MyISAM)"]`，大小写不敏感 | 降级 | ✅ |
| 4 | `MEMORY` / `CSV` / `ARCHIVE` / `BLACKHOLE` | 同一批返回 `non_transactional`，四张都点名、`SQL` 各一条 `ALTER … ENGINE=InnoDB;` | 同档降级 | ✅ |
| 5 | 未知引擎 `ROCKSDB`（ENGINES 里没有） | `non_transactional`，`Reason` 含「服务器没告诉我们 ROCKSDB 有没有事务」 | 查不到也降级 | ✅ |
| 6 | `Engines == nil` 但表是 `InnoDB` | `non_transactional`（**不**退化成 ok） | 读不到能力 ≠ 有事务 | ✅ |
| 7 | PG 源 | `not_judged` 且 `!= transactional`，不发 MY-08 | 不判 | ✅ |
| 8 | `SourceCat == nil`（MySQL 源） | `not_judged`，不发 MY-08 | 不判 | ✅ |
| 9 | 库里有 MyISAM 但只选 InnoDB | `transactional`、不发卡、`Names` 空 | 只判选中的表 | ✅ |
| 10 | `EngineDegradeNote` 四段 | 含 `降级` / `不是一致性切面` / `读取没有 read view` / `verify 全绿只说明「校验那一刻两边一样」` / `ENGINE=InnoDB` / `<表>(<真实引擎>)`；单行 | 四段齐、不许写死 MyISAM | ✅ |
| 11 | MY-08 卡片（只有 MEMORY 表） | `Status=warn`、`CanStart=true`、Title 无 `MyISAM`、Detail 点名 `hot(MEMORY)`、SQL 带 schema | warn + 标题中性 | ✅ |
| 12 | `runner.degradeNote` → `Result.Note` | 非事务时「说明」里出现降级；`transactional` 时一个字不加 | 预检说了收尾还要说 | ✅ |
| 13 | 引擎特有代价 | `MEMORY` 重启/崩溃即空+要重搬；`CSV`/`ARCHIVE` 不支持索引→恒无主键→指向 SCOPE-02/TARGET-02、interval 以 blocked 为准；`ARCHIVE` 只能 INSERT/SELECT、无可空列；`BLACKHOLE` 读永远 0 行；`FEDERATED` 表在远端；`InnoDB` 不加 | 命中才追加一句 | ✅ |

**13/13 一致，没有一格需要改文档或改口径。** 唯一「代码比文档多」的地方是文案组织（单表直接抄
ALTER、多表指向卡片 SQL），不改变任何结论。

## 3. 真库实测（MySQL 8.0.28 → PG `jinliang_test.my08demo`）

```
$ ./.work/jinliang run --job my08-demo --resync --yes --snapshot-only
结果：阶段 done，复制 9 行，增量 0 行，用时 161ms，退出码 0

[note] MY-08 部分表用无事务引擎：降级：jl_arc_demo(ARCHIVE)、jl_bh_demo(BLACKHOLE)、
       jl_csv_demo(CSV)、jl_mem_demo(MEMORY)、jl_myisam_demo(MyISAM) 用的是无事务引擎——
       本轮照跑、不拒绝，但**不是一致性切面**：ARCHIVE、BLACKHOLE、CSV、MEMORY、MyISAM
       没有事务（服务器自报 TRANSACTIONS=NO/NULL）；全量读取和位点不在同一个时间点（读取没有
       read view），崩溃或并发写入时目标端可能与源端不一致；verify 全绿只说明「校验那一刻两边
       一样」，不说明崩溃后仍然一致；ARCHIVE：不支持任何索引…；ARCHIVE：只能 INSERT/SELECT…；
       BLACKHOLE：写入即丢、读永远 0 行…；CSV：不支持任何索引…；MEMORY：整表在内存，
       实例重启/崩溃即空…；要消除这个代价就把这几张表改成 InnoDB（逐表一条 ALTER TABLE
       <库>.<表> ENGINE=InnoDB;（共 5 张，完整语句见 MY-08 卡片的 SQL）…）
[全量初始化] 全量完成：9 行，5 块，6 张表
[全量初始化] 降级：…（同上）
说明：只搬了存量数据（mode.frequency=once）；降级：…
```

- 选中的 `jl_innodb_demo` **没有**出现在 MY-08 里（不误伤）。
- 收尾「说明」里有降级、事件流里也有：预检说了、收尾再说一次。
- 降级**不影响退出码**：not done 的那次（目标表归属冲突）照样退 1；这次 done 退 0。

## 4. 测试

```
go test ./internal/preflight/ ./internal/engine/ -v   → PASS 计数见下（0 FAIL）
go test ./...                                         → 全 ok
go test -tags integration ./internal/mysqlsource/     → ok（含扩到 MEMORY/CSV/ARCHIVE 的真库格）
gofmt -l .  → 空 ; go vet ./... → 通过
```

关键三类断言（都是本 PR 新增/改写的）：

- **InnoDB 不误伤**：`TestJudgeEnginesAlignsWithMY08Table/2`、`/9`、`TestMY08CardIsEngineNeutral`。
- **MyISAM 降级**：`/3`（MyISAM、MRG_MYISAM 大写回报、MRG_MyIsam 历史写法）。
- **`not_judged` 不当 ok**：`/7`（PG）、`/8`（没读到清单）+ `/6`（`Engines=nil` 必须降级而不是 ok）。
- **接线不许只改一处**：`TestBothCatalogImplementationsFillEngines`（两个 Catalog 文件都必须读
  `information_schema.ENGINES` 并写 `cat.Engines`）；`TestJudgeDoesNotGuessByEngineName`（判据里不许再出现
  `Contains(...,"MYISAM")`）。

## 5. 留给后面的事

1. `mysqlsink` 侧：MySQL→MySQL 方向仍待接通（`D6`），目标端一律建 InnoDB，不进 MY-08（SPEC 已写）。
2. `SCOPE-02` / `TARGET-02` 与 MY-08 的分界：本 PR 只在 `CSV`/`ARCHIVE` 的代价句里指向它们，
   没有合并卡片（SPEC 明确「别把两张卡并成一张」）。
3. `FEDERATED` 本机未安装（`SUPPORT=NO`），所以第 5 格（未知引擎）实际也覆盖了它：真库上它可能
   连 `ENGINES` 键都没有 → 走「服务器没告诉我们」→ 降级。
