# DDL 第③步（按表暂停）恢复计划与验收判据

> **口径已翻转（2026-11-11）**：第 4 条（「整 run 失败」）、第 6 条（E1015 措辞与 `其它表也不会继续推进` 那个短语）、
> §2.3(1)（「其它表继续跑不存在」）全部作废。现行口径：**只暂停该表、其它表继续、位点前进、恢复=整表重搬**，
> 以 `docs/CONTRACT-PAUSE-AND-POSITION.md §1` 为准。本文件其余部分（验收判据、归属、切片顺序）仍有效。

基线：main `b71eae89a3e15db1d8e299fca5f9f009788ea1a4`（#64 已合入）。
本文件是 B1 的任务书与验收清单；契约全文见 `notes/contract-paused-tables.md`（同 PR 落盘）。

## §1 范围

**要做（B1）**

1. `_jl_meta.paused_tables` 一列 JSON（PG 目标端），形状与语义按契约 §1：key = **裸表名**，与 `_jl_chunk.table_name` 逐字节同源；`reason` 只放稳定码；`at` 记第一次不覆盖。
2. `PauseTable` / `PausedTables` / `LoadPausedTables` / 写路径快检；`prepare` 之后读回一次（跨轮可见）。
3. **暂停登记是独立一条 `UPDATE _jl_meta SET paused_tables=…, updated_at=…`**，发生在数据事务已回滚之后；**不与 `position` 同条 UPDATE，位点不推进**。
4. 结构不一致时的行为：**整 run 失败**（不是「只停该表、其它表继续」）、位点不推进、`paused_tables` 留痕、`last_error` 点名表与列；结构对齐后重跑继续，**不重搬历史**。
5. 清表 × 暂停：「**清表不清记录，搬成清该表，搬败留记录**」（契约 §2）；出清条件 = 本轮表集合 / `job_hash` 变化。
6. E1015 三处（`Why` / `Next` / `Risk`）措辞按契约 §3 改定，删掉「若当前版本已支持按表暂停」三处括注，`codes_test.go` 的断言同步改成新短语 `其它表也不会继续推进`。
7. MySQL 目标端（`mysqlsink` 同构列落地之前）：`PauseTable` **明确失败**（四段式），`plan` 标注「目标端不支持按表暂停」；**严禁「UPDATE 影响 0 行当成功」**。

**一并收的 #64 尾巴**：`compatibleType` 只比 base、`ErrCollationMismatch` 无码、`fail()` 空表名拼接。

**不做**：「其它表继续跑」（位点表达不了「跳过某表」，也没有暂存队列；见 §4）；`mysqlsink` 同构列实现（B2）；`verify --fix`；Owl 在 #75 留的两条残余；`docs/` 外壳（另提 docs PR，排在代码之后）。

**分支策略**：**不 rebase `#68`**（历史里带旧语义 commit：`dropTableEvents` + 位点推进 + 「其它表继续写」注释，进 main 会误导后来人）。从 `b71eae89` 重开分支，只搬可复用件：`paused_tables` 列、`TablePausedError`、暂停缓存三块。`#68` 标为 superseded 并指向本 PR。

## §2 契约引用（实现方不得自行改口径）

| 契约 | 位置 | 一句话 |
|---|---|---|
| 形状与跨轮语义 | `notes/contract-paused-tables.md §1` | 一列 JSON、裸表名 key、唯一写点、独立 UPDATE、位点不推进、跨轮存活三条判据 |
| 清表 × 暂停 | `… §2` | 清表不清记录，搬成清该表，搬败留记录（含 8 条边界穷举） |
| E1015 措辞与语义边界 | `… §3` | 三处措辞定稿、删括注、缺表缺列走 E1013、`Next` 不许 `--resync` |
| 位点权威 | `docs/STATE-SCHEMA.md §2` | 目标库 `_jl_meta.position` 是唯一权威 |
| 位点与数据同事务 | `docs/DECISIONS.md #1` | 崩溃重放天然幂等 |
| 续跑判据 | `docs/SNAPSHOT.md §4` | 判据是「位点能否从原地重放」，**不读 `paused_tables`** |

三条跨契约不变量（违反即打回）：

1. `planCopy` 的输入里**不许**出现 `paused_tables`。
2. 暂停登记的 UPDATE **只碰** `paused_tables` / `updated_at`。
3. `EnsureSchema`（含结构核对）**必须早于** `cleanBeforeRecopy` —— 否则会「先清空、后报结构不对」，变成真丢数据。

## §3 验收判据（六条，全部可证伪）

门禁在 `gofmt -l .` 空 + `go vet ./...` + `go test ./...` 之外，还要下面六条。每条都要有**真库**证据，不接受「跑通了」。

**1. 目标端列类型与源端一致**：真跑源端 `ALTER TABLE … ALTER COLUMN TYPE`（至少 bigint→text、varchar 放宽各一例），从目标端 `information_schema.columns` 读出实际类型 == 类型字典期望值。证据：两侧 `information_schema` 查询输出。

**2. 数据不丢**：改类型前后 `count(*)` + 逐行指纹一致（值语义不变时）；语义会变的按转换规则**逐行断言**（`CAST` 回原类型仍相等）。

**3. 增量继续可用**：改类型后继续写 N 行（真库），位点前进、`runs.code` 空、目标端行数 = 源端行数。证据：`_jl_meta.position` 前后值 + 行数。

**4. 暂停路径可证伪**：
- (a) `_jl_meta.paused_tables` 里该表有记录、`reason` 是稳定码（`type_changed` / `schema_drift`）、**跨轮仍在**；
- (b) `runs` 结局 failed、`last_error` 点名**表 + 列**；
- (c) **暂停前后 `SELECT position FROM _jl_meta WHERE job_key=$1` 逐字节相等**；
- (d) 批次里含暂停表时，目标端 **0 行**写入（不出现半批）；
- (e) **`keys(paused_tables) ⊆ 本轮选中的表名集合`**；且重搬成功那张表的 key **已消失**、**其它表的记录仍在**（防「整列覆盖」）。

**5. 变异验证（每条改坏都必须红）**
1. 删掉 `classifyError` 的 `PausableError` 分支 → `runs.code` 空、用例红；
2. 删掉写行前的暂停判定 → 目标端出现不该有的行、用例红；
3. 删掉 `LoadPausedTables` → 第 4(a) 条的「跨轮仍在」红；
4. **把「暂停登记」与「位点推进」合成一条 UPDATE → 用例必红**。

**6. 顺序不变量**：`EnsureSchema`（含结构核对）早于 `cleanBeforeRecopy`：代码里留注释 + 一条用例/断言把顺序钉住（结构不一致时**目标表未被清空**）。

## §4 已知边界（不假装有）

1. **「其它表继续跑」不存在**：位点表达不了「跳过某表」，也没有暂存队列。遇到结构不一致是**整 run 失败**、位点不推进、可观测、一条不丢。「按表暂停」目前的真实含义是「记录是哪张表停的」，不是「别的表还在动」。
2. **prepare 与流式路径行为不同**：`EnsureSchema` 发现结构不一致仍整 run 失败（不按表暂停），但共用 `E1015`；同一个码覆盖两个时机是有意的。**漂移判定也一样**：prepare 期只判**本任务建的表**（`_jl_object` 前提）「目标端有、源端没有的列」，用户表（append/keep）多出的列不算漂移（只出 note）；streaming 期所有表都判。文案一律写「改名或删列」——prepare 阶段这两者无法区分，不许断言「列被改名」。
3. **MySQL 目标端**在 B2 之前不支持：明确失败 + `plan` 标注；不支持时**不许**静默。
4. **暂停记录不参与续跑判定**：判据只有块进度 + 位点 + 模式。暂停是后果记录，不是状态机的一环。
5. **「空表 + 仍暂停」中间态是可能的**：`copyFresh` 会清掉该表数据但保留记录，本轮随后失败。必须发 note，不许静默。
6. **脏 JSON 按「没有暂停」处理**（少写而不是多写），但必须发 note。
7. **`at` / `reason` 不覆盖**：第一次的最有价值；将来加原因码走常量，不在中文句子里判等。
8. **出清只在三个条件下发生**（同 `job_key`、`job_hash` 未变、仍在本轮表集合里）。
9. 本轮**不含**：`mysqlsink` 同构列、`docs/` 外壳、`verify --fix`、Owl 在 #75 的两条残余、`#64` 已合部分的复查项。
10. **写路径快检在引擎层不可达（代码级理由，逐状态穷举）**：试过「流式暂停 → 手工对齐 → 同模式重跑」这条路，结论仍是不可达，原因是**三条退出路径各自堵死**：
    - **结构未对齐**：`EnsureSchema` 在 `prepare` 里就失败（E1015），run 在进入写路径之前结束；
    - **结构已对齐**：`reconcilePaused`（在 `decideCopyPlan` **之后**调，`progressByTable` 已有值）里 `copyDoneFor(表)` 为真 → 当场 `ClearPausedTable` 并发 note，写路径拿不到记录。注意 `copyDoneFor` 只在「**这张表一条块进度都没有**」时才当作已完成（`continuous`/`interval` 的情形，契约 §1.2 例外），**半截拷贝（有块标记、无 `Done`）一律不算完成**——所以「半截拷贝 + 记录」会保留（`TestReconcilePausedKeepsHalfCopiedAndClearsDone` 第一格，走 `engine.Run` 的是 `TestRunKeepsOrClearsPauseRecordByChunkProgress`）；**interval 模式例外**：它没有「存量」概念，在 `prepare` 的早退分支里单独收口，`copyDoneFor` 恒真是契约 §1.2 例外段写明的设计；
    - **存量没搬完**（半截拷贝）：`planCopy` 判 `copyResume`/`copyFresh`，写路径排在搬运之后；搬运成功那一刻 `snapshot()` 在 `MarkTableDone` 前调 `ClearPausedTable`，同样先解除；搬运失败则 run 直接结束。
    因此「结构已对齐 + 记录仍在 + 走到 `Apply`」三件事不能同时成立。判据 4(d) 的落点因此在 **pgsink 层**：`TestApplyRefusesPausedTable`（第二个 Sink 实例、只靠 `LoadPausedTables` 读回 → 整批拒绝 + 目标端 0 行），变异「删掉 `Apply` 暂停判定」→ 该格红。
    引擎层「不会带着暂停记录继续写」靠**上游三处**：①`EnsureSchema` 结构不一致即失败；②`reconcilePaused` 对齐且存量完成即解除；③搬运成功即解除。`Apply` 的判定是**最后一道闸**（契约 §1.2 的写路径快检），不是主路径——保留它是因为删掉等于撤掉这道闸。
