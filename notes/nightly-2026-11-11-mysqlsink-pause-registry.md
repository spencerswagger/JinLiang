# MySQL 目标端 PauseRegistry 落地（契约 §4.2/§4.5 Data 清单）

契约：`docs/CONTRACT-PAUSE-AND-POSITION.md`（PR #90，`4c50656`）。基线 main `02c6f75`。
分支 `feat/mysqlsink-pause-registry`。

## 1. 六个方法（逐个）

| 方法 | 实现 | 关键点 |
|---|---|---|
| `LoadPausedTables(ctx)` | 读 `_jl_meta.paused_tables` → 内存 `pausedCache` | 脏 JSON 按「无暂停」+ `PausedDirty()=true`（与 pgsink 同口径：少写比多写安全） |
| `PausedTables(ctx)` | 直接读库（不看缓存） | status/doctor 要看库里真值 |
| `PauseTable(ctx, ref, rec)` | 单 key 读改写（`SELECT … FOR UPDATE` → merge → `UPDATE`） | `at`/`reason` **不覆盖**；`resume` 缺省 `recopy`；跳过记账不清零；写回带 `updated_at=NOW(6)` |
| `NoteSkipped(ctx, ref, rows, pos)` | 累加 `skipped_rows`、`skipped_from` 首次不覆盖、`last_skipped_pos` 跟本批 | 未登记的表**不写**（不凭空造暂停记录） |
| `ClearPausedTable(ctx, ref)` | 只删该 key | 幂等（key 不在 = 成功）；**状态行不存在 = 报错**（那是另一回事） |
| `MarkTableRecopyPending(ctx, ref)` | `DELETE FROM _jl_chunk WHERE job_key=? AND table_name=?` | 含 `chunk_no=0`（完成标记）；只清这一张表 |

签名与 `pgsink` 逐字一致（`pgsink.PausedTable` 直接复用），并在本包声明了一个 `PauseRegistry`
接口做**编译期断言**（`var _ PauseRegistry = (*Sink)(nil)`）：签名漂了直接编译失败，
而不是等引擎运行时 `ok == false` 静默退回「不支持」。
> 没放进 `internal/sink` 的原因：那需要把 `PausedTable` 也搬过去，等于同时改 PG 侧写路径与所有调用方。
> 归属留给 Ark/Core 收口，本包先钉死签名。

## 2. `paused_tables` 列（老库补列）

- 新库：`metaDDL` 里 `_jl_meta` 直接带 `paused_tables text NULL`；
- 老库：**MySQL 没有 `ADD COLUMN IF NOT EXISTS`**（那是 MariaDB 方言，8.0/8.4 执行直接语法错），
  所以 `ensurePausedColumn` 先查 `information_schema.columns` 再决定要不要 `ALTER`，结果缓存一次；
- 补不上（缺 ALTER 权限 / 只读副本）：`ErrPauseUnsupported` + 四段式（发生了什么 / 怎么查 / 两条出路 / 风险），
  **`PauseTable` 明确失败**——绝不是「UPDATE 影响 0 行当成功」。

## 3. 块进度记账（原来一块都不记）

先说事实：**MySQL 目标端的通用全量路径（`engine.copyGeneric`）原来完全没写 `_jl_chunk`**。
`_jl_chunk` 表存在、`DoneChunks/ResetChunks` 也在，但没有任何一行块标记——
所以「这张表欠一次重搬」（`MarkTableRecopyPending` 清的目标）在 MySQL 目标端无从表达。

这次补上：

- `MarkChunkDone(ctx, ref, no, from, to, rows)`：每块写完之后单独记一笔（`ON DUPLICATE KEY UPDATE`，幂等）；
- `MarkTableDone(ctx, ref)`：块号 0 = 存量搬完，`rows` = 前面各块合计；
- `ChunkProgress(ctx)`：`table → {Done, LastNo, Cursor, Rows}`，字段与 pgsink 同名同义；
- 引擎侧 `copyGeneric` 加一个**可选接口** `chunkChronicker`（PG 走 `BulkApply` 同事务写标记，
  非 PG 只能分两步）。**数据先落、标记后落**：崩在中间只会「数据在、标记没写」→ 下一轮重搬靠 upsert 收敛；
  反过来的顺序崩了就是「标记说搬完、数据缺着」，那是静默丢行。

真跑证据（PG → MySQL，3000 行、chunk_rows 2000）：

```
table_name  chunk_no  done  rows
my_orders        0     1    3000   ← 完成标记
my_orders        1     1    2000
my_orders        2     1    1000
```

与 pgsink 的**已知差别**（写进注释里，免得被当成同一个东西）：MySQL 目标端不是
「数据 + 标记 + 位点同一事务」，而是两步；代价是崩在中间多搬一次，收益是不用把 `copyGeneric`
改成事务化批量写。

## 4. `Apply` 不做暂停判定（与 pgsink 同步）

`mysqlsink.Apply` 本来就没有暂停判定（这是正确的方向），这次补的是**最后一道防线**：
拿到的事件里若有 `pausedCache` 中的表 → 立刻报「引擎没有过滤掉暂停表（内部错误）」。
缓存为空（没调 `LoadPausedTables`）时这段是空转，不影响既有路径。

## 5. 用例（真 MySQL 8.0.28，全部通过，1.6s / 6 格）

| 用例 | 断言 |
|---|---|
| `TestIntegrationPauseRegisterThenClear` | 登记→读回→解除；`at` 不被第二次登记覆盖；重复解除幂等 |
| `TestIntegrationPauseOneTableKeepsOthers` | 登记/解除 A 不动 B（单 key 读改写，不是整列重建） |
| `TestIntegrationNoteSkippedAccounting` | 100+28=128；`skipped_from` 首次不覆盖；`last_skipped_pos` 跟本批；未登记表不凭空造记录 |
| `TestIntegrationMarkTableRecopyPendingClearsChunks` | 清该表**全部**块标记（含 chunk_no=0），B 表不动 |
| `TestIntegrationPausedColumnBackfillAndExplicitFailure` | 删列模拟老库 → 自动补列后登记成功；**状态行不存在必须报错**（文案点名） |
| `TestIntegrationPausedJSONShape` | 列里是合法 JSON、key 是裸表名、六个字段都在 |

另：`go test ./...` 全绿；`go test -tags=integration ./internal/mysqlsink` 全部 27s 通过（含原有 DDL/upsert/width 用例）。

## 6. 缺列场景的明确失败输出（原样）

```
MySQL 目标端不支持按表暂停：给 `_jl_meta` 补 `paused_tables` 列失败（目标端是 MySQL 8.0.28）。
你能做什么：① 用有 ALTER 权限的账号手工执行 `ALTER TABLE _jl_meta ADD COLUMN paused_tables text NULL`；
② 或者换 PostgreSQL 目标端。风险：没有（本次没有跳过任何表，也没有推进位点）
```

`plan`/preflight 侧：`PAUSE-01` 从「目标端不支持按表暂停」改成 MySQL 的
「支持（老库缺列会先补；补不上会明确失败并说明原因）」；`executeGeneric` 在 `EnsureMeta`
之后主动问一次 `PauseRegistryReady`，补不上就发 note（不致命：这条任务可能整轮用不到暂停）。

## 7. 仍未接的一段（不在本 PR，属 Core）

引擎的暂停集合与写路径（`pause_skip.go`、`r.paused *pgsink.PausedTable`、`r.sink *pgsink.Sink`）
是 **pgsink 专属类型**：`streamGeneric` 没有包 `pauseSink`，`copyGeneric` 也不查暂停集合。
也就是说 **MySQL 目标端目前还不会「只停某张表」**——本 PR 把「能记、能清、能对账」的底座做齐了，
真正按表跳过要等引擎把 `r.sink` 换成接口并让 `streamGeneric` 走 `pauseSink`（Core 的 §4.5 清单）。
在那之前，MySQL 目标端遇到结构不一致仍是「整 run 失败」——但至少现在会**明确说要补什么**，
并且 `paused_tables` 的形状与 PG 侧完全同构，接上去不用改存储。
