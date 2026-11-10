# 契约：按表暂停 × 异构增量位点（冻结版 v1.1）

> **落地状态**（2026-11-11 冻结、v1.1 按 Owl II 评审修订）：A 片已合 `#93`、B 片已合 `#97`、C 片（存储层）已合 `#95`；
> **剩片 D**＝MySQL 目标端接进 `pauseSink` + `status`/`doctor` 展示 + E1015 文案 + 删死代码 `copyDoneFor`（见 §6）。
> 帽子上的语义口径（跳过 + 位点前进 + 恢复整表重搬）已随 `#93` 生效，不再改；改口径要重新裁决并升 v2。
>
> 基线：**冻结基线** main `67db571`（`runSource` 收口已合入、`paused_tables` 已在 pgsink）。
> §5 的「现状」列行号以这个 commit 为准（评审已逐条核对命中），闭环状态标在裁决列末尾。
> 冲突清单见 §5；实现方红线见 §1.5、§4.6。

本文件裁决四件事：**按表暂停的状态机**、**异构增量位点**、**MySQL 5.7/8.0/8.4 的能力矩阵**、
**Core 与 Data 要接的接口**。凡与本文件冲突的旧文字（`notes/contract-paused-tables.md` 的 §1.5/§3/§4 R1、
`internal/core/codes.go` 的 E1015 三句）一律以本文件为准。

---

## §1 裁决 A：按表暂停 = 跳过该表、其它表继续、位点继续前进

### 1.1 一句话

> **DDL 让某张表没法继续跟 → 只停这张表的写入，其它表照常提交，位点照常前进；
> 记录落在 `_jl_meta.paused_tables`；结构对齐后**整表重搬**补齐，搬成那一刻才解除。**

「跳过」之所以不是「静默丢数据」：跳过的窗口被 `paused_tables` 显式记账（行数 + 位点），
解除动作被定义成**整表重搬**（新视图读当前状态），不是「从旧位点重放」——欠账有人还、有人看得见。

### 1.2 触发者与触发点（唯一）

**只有结构核对能触发暂停，不解析 DDL 语句文本。**

| 时机 | 核对方式 | 原因码 |
|---|---|---|
| prepare | `EnsureSchema` 比对目标列与源端列（本任务建的表才判 `schema_drift`，用户表多出的列只发 note） | `type_changed` / `schema_drift` |
| streaming | 收到 `EvDDL` → **重读源端 `information_schema`** 拿该表新列定义 → 与目标端比对 | `type_changed` / `schema_drift` |

为什么不在 DDL 文本上做解析：MySQL 的 `ALTER` 语法组合（`MODIFY`/`CHANGE`/`RENAME COLUMN`/`ALTER … ALGORITHM=INSTANT`）
在 5.7/8.0/8.4 上接受的写法不同，解错一次就是「以为跟上了、其实列没了」。DDL 事件**只当信号**用（「这张表变了，去重读」）。

### 1.3 状态机（表级；**四个状态全部是派生的，不新增状态列/字段**）

| 状态 | 判据（只用两个可重算的事实 + 本轮内存） | 进入条件 | 迁移 |
|---|---|---|---|
| `active` | `paused_tables` 无该 key | 结构核对通过 | → `paused` |
| `paused` | key 存在 ∧ 本轮 prepare 结构核对**未**通过 | 结构核对不通过 | → `recopy_pending`；→ 删记录（表出范围 / `job_hash` 变） |
| `recopy_pending` | key 存在 ∧ 本轮 prepare 结构核对通过（`MarkTableRecopyPending` 在本步清掉块标记） | prepare 里 `MarkTableRecopyPending` + `forceRecopy` | → `recopying`（同一轮内立刻进搬运计划） |
| `recopying` | key 存在 ∧ 该表块标记已清、且已开始重建（有块进度但无 `chunk_no=0`） | 搬运进行中 | → `active`（搬成 → 删 key）；→ `recopy_pending`（重搬失败/崩，下一轮重来；崩在清标记与首块之间同样落回这里） |

- **不做「手工暂停」命令**：一期暂停只有一个来源（结构核对）。多一个入口就多一套「谁来解除」的语义。
- `recopy_pending` **不落库**：它是「记录还在 + 结构已对齐」的派生结果，每轮 prepare 重算一次。
  跨轮的唯一证据是「记录还在」+「块标记被清」——所以崩在重搬中间，下一轮仍会被判回 `recopy_pending`，不需要额外字段。
- **暂停表的事件分两类，语义不同**（这里统一，别再分叉）：
  - 数据事件（`Insert/Update/Delete/Truncate`）→ **丢弃**，只记跳过行数；
  - `EvDDL` → **不应用到目标端**（不执行 DDL），**也不用于当轮判对齐**（当轮判＝拿旧结构重搬，白跑）；
    它只是「下一轮 prepare 会重新核对」的理由。**对齐只在轮次边界（prepare）判**。
  - `EvBegin`/`EvCommit`/`EvHeartbeat` → **必须保留**（事务边界不属于任何表，丢了整批的事务配对就断了）。

### 1.4 记录形状（`_jl_meta.paused_tables`，一列 JSON 文本）

```json
{
  "orders": {
    "reason": "type_changed",
    "at": "2026-11-11T02:10:00Z",
    "detail": "amount: 目标 text / 源 decimal(12,2)",
    "resume": "recopy",
    "skipped_rows": 1284,
    "skipped_from": "mysql:v1:file=mysql-bin.000007;pos=194321;coord=mysql_file_pos",
    "last_skipped_pos": "mysql:v1:file=mysql-bin.000007;pos=201877;coord=mysql_file_pos"
  }
}
```

| 字段 | 语义 | 约束 |
|---|---|---|
| key | 规范化后的**裸表名** | 与 `_jl_chunk.table_name` 逐字节相同；不带 schema 前缀 |
| `reason` | `type_changed` / `schema_drift` | 常量、机器判等；不写中文句子 |
| `at` | 第一次暂停时刻 | UTC RFC3339，**永不覆盖**（后续重搬失败也不改） |
| `detail` | 人话（列名 + 两端类型 / 列名差异） | 不参与判等 |
| `resume` | `recopy`（本任务建的表，自动整表重搬）/ `manual`（用户自己的表，停下来问） | 决定恢复路径，见 §1.6 |
| `skipped_rows` / `skipped_from` / `last_skipped_pos` | 跳过窗口的记账 | 只用于展示与排障，**不参与正确性判定**；`skipped_from` 首次写、之后不覆盖 |

**字段集就这七个，不再加**：`recopy_pending`（见 §1.3）由「记录在 + 结构已对齐」派生，
不需要 `recopy_ready_at` 之类的落库字段；加字段等于把派生状态做成第二份真相。

老库补列：**MySQL 没有 `ADD COLUMN IF NOT EXISTS`（那是 MariaDB 方言）**，必须
「先查 `information_schema.COLUMNS`，没有才 `ALTER TABLE … ADD COLUMN`」，幂等靠「列已存在 → 跳过」
（`internal/mysqlsink/schema.go:206-211` 的 `addMissingColumns`、`internal/mysqlsink/apply.go:341` 的 ADD COLUMN 分支，
照这两个写）。PG 目标端仍可用 `ADD COLUMN IF NOT EXISTS`（`internal/pgsink/pgsink.go:276`）。
脏 JSON：按「无暂停」继续 + 一条 note（现状已有，保留）。

### 1.5 写路径：跳过与位点（**本节是本次裁决的核心变化**）

1. **判定权在引擎，不在 sink**（已落地 `#93`）。引擎 prepare 时读一次（`LoadPausedTables`），此后持有内存集合；
   写路径由 `internal/engine/pause_skip.go` 的 `pauseSink` 在提交前按表过滤；`sink.Apply` 收到什么写什么，
   **不再做暂停判定**（`pgsink` 里那个整批拒绝已经删掉，`TablePausedError` 只剩类型给码表用）。
2. **登记先行**：streaming 里第一次发现某表的结构对不上时，**先把 `paused_tables` 落库（独立事务），
   成功后才允许这一批跳过该表**。登记失败 → 整批失败（位点不动、目标端 0 行）。
   ——这一条替代旧红线「暂停绝不与位点同一条 UPDATE」：仍然是两条独立的写（登记一条、位点随数据事务一条），
   但**顺序**被钉死为「先登记、后推进」，否则「跳过了但没记下」＝永久静默缺失。
3. **位点继续前进**：跳过之后该批照常提交（数据 + 位点同事务），位点 = 该批最后一个完整事务的提交位点。
   只涉及暂停表的事务产生**空批**（`Apply(nil, pos)`），位点同样前进（否则下一轮会重读同一段，永远卡住）。
4. **事务边界事件保留**：只丢带表的数据事件与该表的 `EvDDL`；丢弃后的批次以数据事件开头是合法的
   「续段」（`source.ValidateBatch` 已允许，见 `internal/source/batch.go:86-91`）。
5. 计数：丢弃时累加 `skipped_rows`、更新 `last_skipped_pos`（与本批位点同值即可）；
   首次暂停发一条 note（「表 X 已暂停：<reason>，detail；其它表继续，跳过窗口会在重搬时补齐」）。

**可证伪断言**（写用例时照这个，不要只看「没报错」）：

| # | 断言 | 治什么 |
|---|---|---|
| A1 | 同一批里 A 表暂停、B 表有行 → 目标端 A=0、**B 行必须落库** | 「只停该表」真的成立 |
| A2 | 该批提交后 `position` 与批前**不相等**，且等于该批最后事务位点 | 位点真的前进 |
| A3 | 注入「登记失败」→ 该批整体失败、目标端 0 行、位点不动 | 不许「跳过了没记下」 |
| A4 | 暂停表的事件不产生任何写行（含 `DELETE`） | 跳过是跳过，不是半批 |
| A5 | 只涉及暂停表的事务 → `Apply` 收到空批、位点前进 | 不许卡死在同一段 |
| A6 | 一轮结束后 `keys(paused_tables) ⊆ 本轮选中的表名集合` | 出范围的记录要清 |

### 1.6 恢复：对齐判定 → 进搬运计划（清空 + 清块标记）→ 搬成才解除

1. **对齐判定**：prepare 的 `EnsureSchema` 不再报 `PausableError`（`type_changed` / `schema_drift`）；
   `schema_drift` 只在 `_jl_object` 有 `table:<schema>.<table>` 的表上判。
   **本轮刚发现不一致的表（`skippedTables`）不算对齐**——拿旧结构重搬一遍只会白跑一轮，还会把目标表清空。
2. **「欠一次整表重搬」必须是一个显式判据（新判据，前一版写「不需要新判据」是错的）**。
   判据 = `记录在 ∧ resume=recopy ∧ 本轮结构核对通过 ∧ 不是本轮刚暂停的表`。为什么不能靠 `planCopy` 推出来：
   - **单表**（最常见）：块标记被清 → 整本块账为空 → 落到「老状态兼容」那条 → `copySkip`，
     整个搬运阶段都不跑，暂停记录永远删不掉，而每轮还发一句「本轮整表重搬」——说了不做；
   - **多表**：判成 `copyResume`，但该表既不在 `From`（无从续）也不在 `Recopy` → 从表头重读，
     走的是 `BulkApply`（**只有 upsert，没有 delete**）→ 暂停窗口里源端删掉的行原样留着＝契约自己否决的假一致；
   - **`copyDoneFor`**（`internal/engine/engine.go:1297`）对「一条块标记都没有」返回 `true`，
     拿它当解除判据会当场把暂停解掉（这也是为什么解除判据不能是它）。
3. **落点（已落地 `#93`，改口径时照这个形状）**：`reconcilePaused` 判定第 2 步后
   ① `MarkTableRecopyPending(ref)`（删该表 `_jl_chunk` 全部行，**含 `chunk_no=0`**）；
   ② 记进 `forceRecopy[table]`；③ `commitPlan` 调 `applyForcedRecopy`：从 `Skip`/`From` 里删掉该表、
   追加进 `Recopy`、`Clean != cleanNone` 时补进 `CleanNames`、`Kind` 由 `copySkip` 提升为 `copyResume`；
   ④ `snapshot()` 的 `copyResume` 分支对 `Recopy` 里的表调 `recopyOne`（原因码 `recopyPaused`）：
   **清空该表 + `ResetTableChunks` + 从表头重读**。`Recopy` 是唯一「清空+清标记+表头重读」的分支，
   把表塞进任何别的分支都会退化成假一致。
4. **解除**：该表重搬搬成（`MarkTableDone` 写下 `chunk_no=0`）之后 `ClearPausedTable(ref)` **只删该 key**，发 note
   「表 X 已恢复，跳过窗口的 N 行已由整表重搬补齐」。
5. **`copyDoneFor` 现在是死代码**（零调用点）：删掉，不许它回到解除路径上。

- **`resume=manual`**（`on_exists=append`/`keep`，用户自己的表）：不许自动清空、不许自动重搬。
  保持 `paused`，`status` 里给三条选项（recreate / 手工清 / 把表移出范围），选完再重跑。
- **对齐只在轮次边界判**：streaming 里暂停表的 `EvDDL` 不应用、也不当轮判对齐（§1.3），
  本轮继续跳过，下一轮 prepare 走第 1–4 步。中途重搬会和正在消费的流抢同一张表
  （视图位点与全局位点之间会出现缺口），不在流式阶段做。
- **重搬中途 kill -9**：记录保留、块标记部分推进，下一轮重新判回 `recopy_pending` 并从块边界续跑，**不解除**。
- **`interval` / `continuous`** 没有存量可搬：结构一对齐就解除（并明说「解除 ≠ 两侧一致，要补齐请跑 full」），
  否则会永远停在暂停里。

### 1.7 失败与部分失败补偿矩阵

| 情形 | 行为 | 理由 |
|---|---|---|
| 暂停登记写库失败 | 整批失败（位点不动、目标端 0 行） | 记不下就不许跳过；失败有人看，缺数据没人看 |
| 暂停表的事件解析失败（脏行） | 整批失败（与现有解码失败同口径） | 不能把「读不懂」当成「可以扔」 |
| 重搬失败（结构仍不一致） | 保留 `paused`，更新 `detail`/`last_seen`，不动 `at`/`reason` | 第一次的时刻与原因最有价值 |
| 重搬失败（网络/锁） | 保留 `paused` + 块进度，可续跑 | 半个重搬不能让表回到 `active` |
| 目标端不支持记录（MySQL 目标端同构列落地前） | `PauseTable` **明确失败**（E1014 家族 + 下一步），`plan` 标注「此目标端不支持按表暂停：遇到结构不一致整 run 失败」 | 严禁「UPDATE 影响 0 行当成功」 |
| 记录指向的表已不在范围 | prepare 删记录 + note | 跨轮存活判据（现状 `reconcilePaused` 已实现，保留） |

### 1.8 观测与续跑

- `status`：新增一节（不是新表）：`paused[] = {table, reason, at, detail, resume, skipped_rows, skipped_from, last_skipped_pos}`；
  `SKIP` 态的表在概览里单列一行「已暂停 N 张（其中 M 张待重搬）」。
- `doctor`：有暂停表 → **warn，不阻断**，文案「这些表在重搬完成前与源端不一致；结构对齐后重跑会自动整表重搬补齐」。
- 事件流：首次暂停 / 重搬开始 / 重搬完成并解除，各一条 note（带表名、原因码、窗口位点）。
- 续跑：`paused_tables` 是**跨轮事实**，重启后仍暂停（结构没改就不该恢复写）。
  它**不进** `planCopy` 的判据（续跑判据只看位点可重放 + 块进度，见 §2.3）。

### 1.9 被否决的方案

| 被否决 | 为什么否决 |
|---|---|
| **整批拒绝、整 run 停**（基线实现，`pgsink.go:1049`；已由 `#93` 删除） | 与 `DESIGN.md §4A`「DDL 期间短暂暂停该表 / 拦截该表」、与验收「只暂停该表」直接矛盾；一张表坏了让几十张无关表一起停 |
| **按表位点 `_jl_table_pos`** | 推翻 `DECISIONS #1/#2`（位点与数据同事务、目标库位点是唯一权威）；崩在一半会出现「部分表前进、部分表没推进」且无法从单一权威重建；`E3001` 判定要重写 |
| **暂停期间位点不动，但其它表照写** | 位点是全局的：不推进就得重放同一批，其它表会被无限重复应用，做不到「继续」 |
| **恢复 = 从暂停时刻重放** | 跳过语义下位点已越过该表，重放不可能；除非按表位点（已否决） |
| **恢复 = 只 upsert 不清表** | 暂停窗口里的删除看不见，已删的行会永久留在目标端（假一致） |
| **解析 DDL 文本判定跟随/暂停** | 见 §1.2：语法组合跨版本不一致，解错一次就是静默丢列 |

---

## §2 裁决 B：异构增量位点

### 2.1 唯一权威：目标库那一条位点；它的**内容**是源端坐标

先把「权威」这个词拆开，避免与 `DECISIONS #2` 字面打架（两条说的是不同的事）：

| 说法 | 指的什么 | 谁说了算 |
|---|---|---|
| `DECISIONS #2`「目标库位点是唯一权威」 | **存储与判等**：以目标库 `_jl_meta.position` 那一条为准（崩了读它、续跑用它、对账比它） | 目标库（不变） |
| 本节「唯一来源是源端坐标」 | **内容格式与可重放性**：那一条里写的是源端坐标串，能不能从原地重放由源端决定 | 源端连接器（`ParsePosition`/`Replay`） |

- 位点串的来源只有源端，格式 `<kind>:v1:<payload>`（`internal/source/contract.go` 2026-11-05 裁决）。
  PG 是 `postgres:v1:0/16B6C50`；MySQL 是 `mysql:v1:file=…;pos=…;coord=…;gtid=…;t=…;srv=…`。
- **不存在「目标端自己的位点」**：目标端既没有与源端可比的写位置，也没有跨端可比的自增值。
  它在链路里的职责只有两条：① 与数据**同事务**保存；② 原样返回给 `ParsePosition`。
- 唯一例外是 MySQL 目标的 DDL/TRUNCATE 批次（隐式提交，天然三个提交点：数据 → DDL → 位点）：
  那一批必须用 `PositionDurable` 校验「位点确实落库了」才算提交完成
  （`internal/mysqlsink/state.go:425`）。
- `Kind()` 只表达端点类型（`postgres`/`mysql`），**不表达坐标变体**；变体（`mysql_file_pos`/`mysql_gtid`）
  在 payload 的 `coord=` 里，且**一旦写进目标库就不再改**（换轴 = 静默跳事务）。

### 2.2 对齐点：批边界 = 目标事务边界

| 规则 | 说明 |
|---|---|
| 一个批次 = 一个目标事务 = 数据 + 位点，原子提交 | `DECISIONS #1`；这是「重放幂等」的全部依据 |
| 位点 = 该批**最后一个完整提交事务**的提交位点 | 批次可含多个完整事务 |
| 切开的大事务必须携带**上一个已提交位点** | 半个事务的批次给事务中间的位点，崩溃重放的起点会漂（`sink.Sink.Apply` 不变量） |
| 位点只在事务边界前进 | 因此两次提交之间只需要**可判等**，不需要全序；「谁更晚」由连接器给（PG 的 LSN 序 / MySQL 的「文件号→偏移」序） |
| 同一时刻只有一个写者 | 跨事务按源端提交顺序；同事务按源顺序；并行只允许在源端按表拆 |

### 2.3 kill -9：回退窗口 = 0，重放窗口 ≤ 一个批次，**重搬行数 = 0**

三个崩溃点（`STATE-SCHEMA.md` 的三段分析，这里只写结论）：

| 崩溃点 | 目标端状态 | 重启后动作 |
|---|---|---|
| 已从源端读出、目标事务未提交 | 数据与位点都回滚 | 从**原位点**重放该批（重复部分由 upsert 吸收） |
| 目标事务已提交 | 数据与位点都在 | 从位点继续，不重放 |
| MySQL 目标 DDL 批次三提交之间 | 数据已落、位点未落 | 重放：数据 upsert 幂等；DDL 执行前判存在（`Error 1060/1061` 不炸） |

- **回退窗口 = 0**：不存在「位点超前于数据」或「数据超前于位点」——同事务提交保证了两者同时生效。
  因此**永远不需要按时间回退位点**（`TakenAt - 1s` 那套被否决，见 §2.7）。
- **重放窗口** = 那一个未提交批次（≤ `batch_rows` 或单事务上限），重复由主键 upsert 吸收；
  无主键表只允许 `insert-only` + 周期全量校验（无 PK 不能 upsert，重放会插重复行）。
- **重搬 0 行的判据（可证伪）**：两条同时成立才允许续跑、不重跑全量——
  1. `Replay(pos).OK`：位点仍能从原地重放。PG＝槽位在且 `confirmed_flush_lsn ≤ pos`；
     MySQL＝位点文件仍在 `SHOW BINARY LOGS` 里（GTID 轴则仍在 `@@gtid_executed` 里）。
  2. 目标端该表有块进度（`_jl_chunk` 有行）。
  任一条不成立 → **整轮重搬**，动作清单写明是哪一条（MySQL 的 `binlog` 已 purge → `E3001` → `--resync`）。
- **续跑不动位点**：不重建书签、不把位点挪到续跑时刻。剩下的块读「更晚但完整」的新视图，
  已搬区间的变更由增量从**原**位点重放收敛。
- **「0 重搬」量的是整轮，不是单表**（三个口径别混用）：

  | 口径 | 定义 | 断言 |
  |---|---|---|
  | 整轮（验收主口径） | `kill -9` 重启后这一轮**没有重跑任何全量** | `rowsCopied == 0`，`plan.Kind == copySkip` |
  | 单表（定位用） | 该表没有被从头重读 | `_jl_chunk` 里各表 `chunk_no=0` 仍在；`plan.From[table]` 非空 |
  | 暂停恢复（**允许 ≠ 0**） | 结构对齐后该表整表重搬 | 行数 = 该表行数；这是「欠账补齐」，与「重启重搬」是两个度量，报告里必须分开写 |

- 行数一致、逐行 `md5` 一致、位点只前进（PR #89 已按这个口径给出三组数字）。

### 2.4 租约边界：只管「谁可以写」，不管位点

- 租约 TTL 与位点提交**无耦合**：位点提交慢不影响续期，续期失败不改位点、不改 `paused_tables`。
- **失去租约 = 立刻停写**：不提交、不推进位点、不删暂停记录。半途退出交给「从目标端位点重放」收口。
- kill -9 后旧进程不可能再写（没人替它续租），新进程拿到租约后从目标端位点重放——
  这段窗口里唯一可能发生的坏情况是「旧进程最后一批已提交」，那正是位点所在，重放幂等。

### 2.5 GTID 与 file+pos：两套坐标都写，续跑轴固化

| 规则 | 说明 |
|---|---|
| 两套都写进 payload | `file/pos` 5.7 也能用；`gtid` 切主后仍对得上 |
| `coord` 固化续跑轴 | GTID 非空优先 `mysql_gtid`，否则 `mysql_file_pos`；解析后写回，之后不变 |
| `coord=mysql_gtid` 但集合为空 | 解析直接报错（自洽性检查，现状已实现） |
| `srv` 变了（换了实例/主从切换） | **不静默续**：报「位点来自另一个源库」（`E3001` 家族，动作是确认/`--resync`） |
| 位点已过 `binlog` 保留期 | `E3001` + `--resync`（唯一出路）；保留期 5.7 读 `expire_logs_days`，8.0/8.4 优先 `binlog_expire_logs_seconds` |
| 源端 `gtid_mode=OFF` | 默认走 file+pos，并在预检里标注「切主后可能需要 `--resync`」（`DESIGN.md §4A` 已定口径） |

### 2.6 幂等键与提交顺序

- 幂等键 = 目标端**主键/唯一键**上的 upsert；`UPDATE`/`DELETE` 同样按主键定位（行镜像里的旧值只用于审计）。
- 同批内同表的行按源顺序写；不允许为了吞吐把同表行重排（唯一键冲突时的最终值取决于顺序）。
- 目标端单值超上限 → 明确失败 + 下一步（不截断、不拆分，`TYPE-MAPPING.md §5.1②` 已定）。

### 2.7 被否决的方案

| 被否决 | 为什么 |
|---|---|
| **拿目标端 LSN/自增值当位点** | 目标端回滚、多写者、DDL 隐式提交都会让它与源端不可比；也无法据它判断「源端还能不能重放」 |
| **按秒回退再重放**（`TakenAt - 1s`） | 高并发下一秒上千事务，重复与乱序代价大；upsert 已经吸收重复，回退只会放大重放量 |
| **MySQL 只用 file+pos（不开 GTID 也当首选）** | 主从切换后同名文件+偏移指向别的事务 → 静默错位；GTID 优先、file+pos 兜底，且轴要固化 |
| **源端先 ack 再提交目标** | 崩溃即永久丢（PG 的 ack 会释放 WAL；MySQL 无服务端 ack 但会轮转 binlog）。保持 `DECISIONS #3`：停读不停 ack |

---

## §3 裁决 C：MySQL 5.7 / 8.0 / 8.4 的能力矩阵

### 3.1 差异表（只列影响同步的）

| 项 | 5.7 | 8.0 | 8.4 | 影响 | 缺失时（`Compensation`） |
|---|---|---|---|---|---|
| DDL 事件文本 | 有（`QUERY_EVENT`） | 有 | 有 | 只当「这张表变了」的信号，不解析 SQL | — |
| `binlog_row_metadata=FULL`（列名） | **无**（8.0.1+） | 有（默认 `MINIMAL`，需显式开） | 有 | 加列后没有列名 → 必须重读 `information_schema` 才能对齐列 | ①自动补偿：DDL 后重读元数据（多一次查询，不影响正确性） |
| 原子 DDL | **无** | 有 | 有 | 5.7 的 DDL 失败可能留下半改结构 | ①自动补偿：DDL 后**双读校验**（两次一致才继续），不一致 → 暂停该表（`detail` 写「DDL 可能未完成」） |
| GTID | 有（默认 OFF） | 有（默认 OFF） | 有（默认 OFF） | 切主后位点仍可对 | ②file+pos（默认轴）+ 预检标注 |
| 读位点语句 | `SHOW MASTER STATUS` | 同 | **`SHOW BINARY LOG STATUS`** | 取位点 | 双语法探测（`ProbeResult.StatusStatement`，已实现） |
| `binlog` 保留期变量 | `expire_logs_days` | `binlog_expire_logs_seconds`（优先） | 同 8.0 | 「位点还能不能重放」的窗口 | 读不到 → `Replay` 返回 unknown，按 `E3001` 处置 |
| `LOCK INSTANCE FOR BACKUP` | 无 | 有 | 有 | 取一致位点不冻库 | ①自动补偿：毫秒级 FTWRL |
| `binlog_transaction_compression` | 无 | 8.0.20+ | 有 | 解析必须能解 zstd | ①自动补偿（实现解压）；解不了 → 要求源端关闭 |
| 支持档 | EOL 2023-10 → **尽力** | EOL 2026-04 → 全 | 全 | 预检 warn（不阻断） | — |

**能力名（`source.Capability`）**：沿用 `CapGTID` / `CapBackupLock` / `CapBinlogCompress`，
新增 `CapAtomicDDL`、`CapBinlogRowMetadata`。判定一律**探测**，不按版本号分支（`DECISIONS #5`）。

### 3.2 「走追平 / 走暂停+重搬 / 走整轮重搬」判定表

| 事件 | 5.7 | 8.0 | 8.4 |
|---|---|---|---|
| 加列 / 加索引 / 放宽长度 / 用户同意删列 | **追平**（应用 DDL 后继续；5.7 无列名元数据 → 重读 `information_schema`） | 追平 | 追平 |
| 改列类型 / 改名 / 未同意删列 | **暂停该表 + 恢复整表重搬**（三个版本一致） | 同 | 同 |
| DDL 后结构读两次不一致（只可能 5.7） | **暂停该表**（`schema_drift`，`detail` = DDL 可能未完成） | — | — |
| `binlog` 位点已 purge / GTID 已被清理 | **整轮重搬**（`E3001` → `--resync`） | 同 | 同 |
| `coord=mysql_gtid` ∧ 位点 GTID 仍在 `@@gtid_executed` 里 | 追平（**只在这条轴上**；轴由位点里的 `coord` 固化） | 同 | 同 |
| `coord=mysql_file_pos`（源端 GTID 关着，或老位点没写 GTID） | 追平（按文件号→偏移；与 §2.5「不换轴」一致，**不因为现在开了 GTID 就改轴**） | 同 | 同 |
| 位点 `srv` 与当前源不同 | 不续跑（`E3001` 家族，确认/`--resync`） | 同 | 同 |
| `binlog_format` ≠ ROW | 预检 blocked（物理做不到：没有行镜像） | 同 | 同 |

**一致性要求**：上表必须是**表驱动**实现（能力 → 动作），不许散成按版本号的 `if`。
同一件事在三个版本上给出不同结果时，预检报告要写出「为什么不同」（一行白话）。

### 3.3 被否决

| 被否决 | 为什么 |
|---|---|
| 按版本号 `if 5.7 … else if 8.4` | `DECISIONS #5`：8.4 改名、云上魔改、9.x 删插件，版本号不是能力 |
| 「5.7 不支持就不做按表暂停」 | 暂停能力由**目标端 sink** 决定（有没有 `paused_tables` 列），与源端版本无关 |
| 「5.7 无原子 DDL 所以 DDL 一律拦截」 | 过度阻断：加列/加索引在 5.7 上同样安全可重放；双读校验能覆盖半成品结构 |
| 靠 `TableMap` 事件补齐列名（省掉重读元数据） | `TableMap` 只有列类型不含列名（`MINIMAL` 时连名的通道都没有）；按它推列名会静默写错列 |

---

## §4 裁决 D：接口清单（签名级）

### 4.1 读侧最小面 = 现有 `runSource`（冻结成员集合，不新增「某一种源」的概念）

`internal/engine/run_source.go` 的 `runSource` 已经是正确形状（引擎只认接口、构造函数按连接类型选适配器）。
**冻结**：`Kind / ParsePosition / Before / IsZero / RequiresExportedSnapshot / Prepare / SnapshotChunksFrom /
EndSnapshot / Stream / LastCommit / SafeAckPoint / Ack / Advance / Now / ServerEnd / Backlog / Replay / Count /
Fingerprint / FingerprintScope / Close`。

- `Before(a,b)` 是唯一的顺序判定入口：**引擎不得比较具体位点类型**（PG 的 `Compare`、MySQL 的 `Compare` 都留在适配器里）。
- `Now()` 是「追平目标」的唯一来源（替代 `pgsource.CurrentLSN` 的位置），`Backlog()` 返回字节落后量，
  **不知道就返回 -1，不许返回 0**（0 会被当成已追平）。
- `Replay(pos)` 是 §2.3 的判据接口，`Why` 字段直接进动作清单。
- `Advance/Ack` 在 MySQL 侧是 no-op（不是「假装成功」：binlog 由源端保留策略管理）。

### 4.2 目标端 `PauseRegistry`（PG / MySQL 同构；签名按**两侧实际实现**写，不再自造）

```go
// 暂停登记（internal/pgsink/paused.go、internal/mysqlsink/paused.go 逐字同形）
LoadPausedTables(ctx context.Context) error
PausedTables(ctx context.Context) (map[string]PausedTable, error)   // 键＝目标端裸表名
PauseTable(ctx context.Context, ref source.TableRef, rec PausedTable) error
NoteSkipped(ctx context.Context, ref source.TableRef, rows int64, pos string) error // pos 是位点串原文
ClearPausedTable(ctx context.Context, ref source.TableRef) error
MarkTableRecopyPending(ctx context.Context, ref source.TableRef) error // 删该表全部块行（含 chunk_no=0）
PausedDirty() bool                                                    // 脏 JSON → 本轮按「无暂停」

// 块进度：只这三条是两侧共有的，它们才是「重搬 0 行」的证据面
ChunkProgress(ctx context.Context) (map[string]ChunkProgress, error)   // 类型在各自连接器包里
MarkTableDone(ctx context.Context, ref source.TableRef) error
```

- `PausedTable` 字段与 §1.4 的 JSON 一一对应（`Reason/At/Detail/Resume/SkippedRows/SkippedFrom/LastSkippedPos`）。
- `PauseTable` 两边都收**完整记录**（`rec PausedTable`），不再用 `(reason, detail)` 两参数——那会让 `resume`/跳过记账没有入口。
- `ChunkProgress` 的返回类型在 `pgsink` / `mysqlsink` 各有一份 → 引擎侧用可选接口（`chunkChronicker`）适配，
  **不要求两个连接器共用同一个结构体**。
- **`sink.Apply` 不出现在这份接口里**：写路径不做暂停判定（§1.5 第 1 条）。

### 4.3 事件级契约（`EvDDL` 处理顺序，引擎侧固定）

1. 收到 `EvDDL`（表 T）→ **不立刻执行**；先算准「作用表」与语句原文（MySQL 要先去掉 `use db` / 注释前缀）。
2. **分类靠重读元数据**，不解析 SQL：重读源端 `information_schema` 拿 T 的列定义（5.7 双读校验）→ 与目标端比对。
3. 白名单四类（加列/加索引/放宽长度/同意删列）→ **按方向**交给目标端：
   - PG 目标：`pgsink` 执行，但**破坏性语句一律不执行**（`ddlDestructive` 先判：`DROP/RENAME/TRUNCATE` 宁可窄）；
   - MySQL 目标：`mysqlsink.applyDDL`，执行前先判是否已存在（`Error 1060/1061` 不炸，幂等）；
   - **MySQL → PG 的 DDL 本版不搬**（方言不同，执行了是错的一半）：只发 note 说「索引/约束/改类型请在目标端手工执行」，
     新列随 `Relation` 带来、由目标端按加列白名单自己补。静默跳过不行——会让人以为索引也过去了。
4. 非白名单 → `PauseTable`（**先落库**）→ 本批丢弃 T 的行事件与它的 `EvDDL`，其它表照写，位点前进。
5. 该表**已被暂停**时的 `EvDDL`：**不应用、也不用于当轮判对齐**（§1.3）——当轮判＝拿旧结构重搬一遍，白跑；
   本轮继续跳过，下一轮 prepare 重新核对（对齐了才进 §1.6 的重搬）。
6. `EvTruncate`：暂停表 → 丢弃（重搬会覆盖）；非暂停表 → 目标端截断。

### 4.4 错误分类：引擎不得 `import` 具体连接器包

基线（`67db571`）里 `classifyError` 用 `pgsource.IsNoPrivilege / IsObjectMissing / IsWalLevelTooLow / IsPositionGap`
判类 → 异构下 MySQL 的错误全部落到「未识别」（`E1016`/`E4002`），用户拿到的是「请把日志发给维护者」。
**已修（`#97`）**：连接器返回带语义的错误，引擎用 `source.ClassOf(err)` 判类，`engine.go` 里不再出现 `pgsource.` 前缀。

**契约要求（不要退回去）**：新增连接器错误一律实现 `source.ClassOf` 的语义分类；
`classifyError` 里不许再出现任何具体连接器包前缀。
（这一片可以单独 PR 做，但**必须在 ① 的验收之前完成**——否则 MySQL 源的真跑用例一报错就是「未识别」。）

### 4.5 Core / Data 勾选清单

> 状态（2026-11-11 夜）：**Core 侧已落地**（`#93`：`pause_skip.go` 的 `pauseSink` + `forceRecopy` + `applyForcedRecopy`；
> `#97` 的 `source.ClassOf` 收口）；**Data 侧存储层已落地**（`#95`：`mysqlsink/paused.go` + `chunk.go`）。
> 仍缺的一条是**引擎侧的接线**：`pauseSink.Apply` 里 `p.r.sink == nil` 时直接透传，MySQL 目标端还没包进 `pauseSink`
> ——「MySQL 目标端的表级暂停」在接上之前不成立。

**Core**
- [x] 引擎持有暂停集合，`sink.Apply` 删掉暂停判定（`pgsink` 不再产生 `TablePausedError`）
- [x] 批组装时按表丢弃：保留 `EvBegin/EvCommit`，丢该表的行事件与 `EvDDL`（`internal/engine/pause_skip.go`）
- [x] 首次暂停「登记先行」：`PauseTable` 成功后才允许本批跳过；失败即整批失败
- [x] `NoteSkipped` 每批累加一次（行数 + `last_skipped_pos`）
- [x] `reconcilePaused` 改判据：对齐后 `MarkTableRecopyPending` + `forceRecopy`，**不再**用 `copyDoneFor`
- [x] 重搬搬成 → `ClearPausedTable` 单 key + note
- [x] `resume=manual` 的用户表：不自动清空、不自动重搬，`status` 给三条选项
- [ ] `status` / `doctor` 的暂停展示（§1.8）——现状只有 note，`status` 那一节还没加
- [x] `classifyError` 去 `pgsource.` 依赖（`source.ClassOf`，§4.4）
- [ ] `E1015` 三句文案与 `codes_test` 断言（现文案仍写「整条同步停在这里」）
- [ ] 删死代码 `copyDoneFor`（`engine.go:1297`，零调用点）
- [ ] **MySQL 目标端接进 `pauseSink`**（`pauseSink.Apply` 的 `p.r.sink == nil` 透传分支要消除）
- [ ] 用例：A1–A6 + 恢复（清空+块标记+搬成解除）+ 重搬中途 kill -9 保留记录

**Data**
- [x] `mysqlsink` 加 `paused_tables` 列——**写法是「先查 `information_schema.COLUMNS` 再 `ALTER`」**
      （MySQL 没有 `ADD COLUMN IF NOT EXISTS`；`internal/mysqlsink/schema.go:206-211`、`apply.go:341`）
- [x] `PauseRegistry` 六个方法（`internal/mysqlsink/paused.go`）
- [x] 块记账 + `MarkTableRecopyPending`（`internal/mysqlsink/chunk.go`，删该表全部块行含 `chunk_no=0`）
- [x] `mysqlsink.Apply` 不做暂停判定
- [x] 缺列时 `PauseTable` 明确失败 + `preflight PAUSE-01` 不再替引擎许诺
- [ ] `chunk_no=0` 重放不刷新（`rows` 字段）——影响「重搬 0 行」的单表核对口径
- [ ] 用例：MySQL 目标端暂停登记/解除各一格；「UPDATE 影响 0 行」必须报错（不许静默）

### 4.6 实现方红线（越一条就打回）

1. 不许「跳过了但没记下」：登记必须先于跳过落库。
2. 不许把 `paused_tables` 当续跑/重搬判据（判据只有「位点可重放 + 块进度」）。
3. 不许在 `EnsureSchema` 通过时就解除暂停（必须等重搬搬成）。
4. 不许对用户自己的表（`on_exists=append/keep`）自动清空或自动重搬。
5. 不许把暂停表的数据事件「写一半」（整表整事件跳过，不出现半批）。
6. 不许在引擎里按版本号或 `import` 具体连接器包做判定。
7. 不许用 `TableMap`/DDL 文本推列名与列定义；一律重读 `information_schema`。
8. 不许声称「暂停期间数据会一致」——不一致窗口由 `status` 明示，直到重搬完成。

---

## §5 与现实现的冲突点（逐条给裁决）

> 「现状」列是**冻结基线** `67db571` 的行号（评审已核对命中）。基线之后 A/B/C 片已落地，
> 逐条闭环状态标在「裁决」列末尾的 ×：`#93`（A）、`#97`（B）、`#95`（C 存储层）。行号以基线为准，复查请对 commit。

| # | 现状（main `67db571`） | 裁决 |
|---|---|---|
| 1 | `internal/pgsink/pgsink.go:1049-1057`：命中暂停表 → 整批拒绝、位点不动 | **删**；跳过由引擎做，位点照常前进（§1.5）。`TestApplyRefusesPausedTable`（`paused_integration_test.go:170`）改成「引擎过滤后 sink 收到的批次里没有暂停表的行」 ×已闭环 `#93` |
| 2 | `internal/engine/engine.go:1042` `reconcilePaused`：对齐 + `copyDoneFor` 即解除 | **改判据**：对齐只触发 `MarkTableRecopyPending` + `forceRecopy`，解除要等重搬搬成（§1.6）。`paused_reconcile_test.go:28` 重写 ×已闭环 `#93` |
| 3 | `internal/engine/integration_paused_test.go:29` 断言「A=0 B=0、位点逐字节相等」 | **反转**：A=0、**B=1**、位点前进、A 的 `skipped_rows=1`（§1.5 A1/A2） ×已闭环 `#93` |
| 4 | `internal/core/codes.go:215-243` E1015：Next「位点没推进…不会重搬历史」、Risk「其它表也不会继续推进」、注释要求 grep 该短语 | **改文案**：只停该表 / 其它表继续 / 恢复等于整表重搬（会重搬该表存量）。`codes_test.go:123` 的断言短语同步换（**仍未做**：现在改文案就是描述未实现行为，随 `status` 展示那条一起收） |
| 5 | `notes/contract-paused-tables.md` §1.5「暂停期间位点不推进」、§3 E1015 措辞、§4 R1「登记绝不与位点同一条 UPDATE 的顺序约束」 | **作废**，以本文件 §1.5/§1.6/§3 为准；note 顶部已加横幅 |
| 6 | `internal/engine/pause_mysql_test.go:19`：MySQL 目标端 `PauseTable` 明确失败 | **保留到存储层落地**；`#95` 已给 MySQL 同构列，剩下的只是**引擎接线**（`pauseSink` 的 `p.r.sink == nil` 透传） ×存储层已闭环 `#95` |
| 7 | `internal/engine/engine.go:1627` `classifyError` 依赖 `pgsource.IsXxx` | **改**：连接器给语义错误，引擎按 `source.ClassOf` 判（§4.4） ×已闭环 `#97` |
| 8 | `mysqlsink` 无 `paused_tables`、无 `_jl_chunk` 记账 | **Data 补齐**；补齐前 MySQL 目标端暂停必须明确失败（不许静默丢记录） ×已闭环 `#95`（补列走「先查 `information_schema` 再 ALTER」） |
| 9 | `docs/STATE-SCHEMA.md` 的 `_jl_meta` 列清单里没有 `paused_tables` | 本文件 §1.4 先定形状；`STATE-SCHEMA.md` 的更新排在 ① 合入**之后**（它描述的是行为，不能先合） |
| 10 | `docs/DESIGN.md §4A`（`DESIGN.md:115`）「DDL 期间短暂暂停该表 / 拦截该表」 | **不改**：设计本来就是表级，是实现（整批拒绝）跑偏了。本次裁决让实现回到设计 |
| 11 | `internal/engine/engine.go:1297` `copyDoneFor`：无块标记 → `true` | **删**（零调用点）。曾是解除判据时会「当场解除」，现在是死代码，别让它回来 |

---

## §6 落地分片与验收

| 片 | 内容 | 归属 | 状态 |
|---|---|---|---|
| **A** | 跳过 + 位点前进 + 登记先行 + `reconcilePaused` 改判据 + `applyForcedRecopy` + 用例反转 | Core | 已合 `#93` |
| **B** | `classifyError` 去 `pgsource.` 依赖（`source.ClassOf`） | Core | 已合 `#97` |
| **C** | `mysqlsink` 同构暂停列 + 块记账 + `MarkTableRecopyPending` | Data | 已合 `#95` |
| **D** | MySQL 目标端接进 `pauseSink`（表级暂停在三组合上都成立）+ `status`/`doctor` 展示 + E1015 文案 + 删 `copyDoneFor` | Core | 未做 |

**验收（每条都要能证伪，不接受「看起来对」）**

1. 表级暂停真跑：A 表改类型、B 表持续插入 → 目标端 A 停、B 跟得上、`status` 显示 A 暂停与跳过行数。
2. 恢复追平：目标端对齐 A 的结构 → 重跑 → A 被**清空**重搬 + 增量收敛 → 两端行数与逐行 md5 一致。
3. 三种组合各给行数 + md5 + 耗时：MySQL→MySQL / MySQL→PG / PG→MySQL（口径与 PR #89 的 1.2 相同）；
   **表级暂停在三种组合上都要真跑一次**（MySQL 目标端要等片 D）。
4. 增量 `kill -9` 后重启：**整轮重搬 0 行**（`rowsCopied==0` ∧ `plan.Kind==copySkip`；单表口径见 §2.3）、
   位点只前进、行数与 md5 与源端一致。
5. 版本覆盖：`mysql-matrix` **4 腿**（5.7 / 8.0 / 8.4 / 9，`.github/workflows/mysql-matrix.yml`）
   ——它覆盖的是**连接器契约**；引擎级真库在 `ci.yml` 的 `b4-mysql-source` job（`.github/workflows/ci.yml:293`）。
   两处都要绿，别拿 4 腿当引擎级全覆盖。
6. 反向注入必败断言（门禁有效性）：把「登记先行」改成「先推进位点」→ A3 必须红；把恢复改成「只 upsert 不清表」→ 恢复用例必须红。
7. 门禁：`gofmt -l .` 空 / `go vet ./...` / `go test ./...` / `python scripts/validate_schema.py examples/*.yaml`。
