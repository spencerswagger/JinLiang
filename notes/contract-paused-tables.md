# 契约：按表暂停（`paused_tables`）

> **状态**：骨架。本文件先按已下发的三条契约与红线落盘，等 Ark 的契约全文到齐后**逐字替换**（本文件是 `notes/plan-64-ddl-type-change.md` §2 引用的目标，先建立以免引用断链）。

## §1 形状与跨轮语义

**存储**：`_jl_meta.paused_tables`，**一列 JSON 文本**（不建新表；老库用 `ALTER TABLE … ADD COLUMN IF NOT EXISTS`）。一行一个 `job_key`：

```json
{"orders":{"reason":"type_changed","at":"2026-11-10T08:59:00Z","detail":"v: 目标 text / 源 bigint"}}
```

- `key` = **规范化后的裸表名**，与 `_jl_chunk.table_name` **逐字节相同**（不带 schema、不用源端原始大小写）。
- `reason` 只放稳定码：`type_changed` / `schema_drift`。
- `at` 记**第一次**登记的时间，后续不覆盖。
- `detail` 给人看（哪一列、两端各是什么）。

**写点唯一**：`PauseTable`。**读点**：`LoadPausedTables`（prepare 后一次）、`PausedTables`（status/doctor/UI）、`isPausedLocked`（写路径快检）。

**跨轮存活**当且仅当：`job_key` 未变 + `job_hash` 未变 + 该表仍在本轮 `tables`。任一条不成立就在本轮 prepare 结束时清掉并发 note；因此**一轮结束后 `keys ⊆ 本轮表集合`**。

**位点**：暂停登记是**独立一条** `UPDATE _jl_meta SET paused_tables=…, updated_at=…`，发生在数据事务**已回滚之后**；**绝不与 `position` 同条 UPDATE**；位点不推进（判据：暂停前后 `SELECT position` 逐字节相等）。`runs` 记 failed，`last_error` 点名表与列。

## §2 清表 × 暂停

**「清表不清记录，搬成清该表，搬败留记录」**。

- `copyFresh` 只清数据 + `_jl_chunk`，**保留**暂停记录（清表时有记录 → 保留 + note）。
- 某表重搬成功后**只清该表**记录；失败保留。
- 解除挂在「`EnsureSchema` 通过 **+ 存量搬完**」，不是只 `EnsureSchema` 通过。
- 重搬中途崩 → 记录保留、判定不看记录。
- `on_exists=recreate` 同上。
- 记录指向的表已不在源端/范围 → prepare 清掉 + note。
- 既暂停又在 `Recopy` 列表 → 不跳过，先清后搬。
- 流式暂停 → 整 run 失败、位点不推进。
- 脏 JSON → 按「无暂停」继续，但发 note。

**红线**：清表路径不许 `DELETE FROM _jl_meta`；不许整列覆盖（只删一个 key，先读后写）；不许用内存表集合重建整个 JSON；`planCopy` 判定输入**不许**读 `paused_tables`；中间态必须能只靠「块进度 + 位点」重建。

## §3 E1015 措辞与语义边界

- `Message` 不变：`表结构不一致`。
- `Why` 覆盖两个时机（prepare 的 `EnsureSchema` 类型不一致 / 运行时 `EvDDL` 改类型、改名、删列），并说明「具体哪种、哪张表哪一列看 `last_error`」。
- `Next` 三条：① 看 `last_error` 点名的表列、目标端对齐结构后重跑（位点没推进，不会重搬历史）② 暂时不处理也行，但整 run 会失败、位点不推进 ③ 不想跟这张表就从清单里去掉（按新范围重算 `job_key`，需要 `--accept-change`）。
- `Risk`：整条同步停在这里、位点不推进、一条变更都不会丢；该表后续变更不写入，**其它表也不会继续推进**；最常见误判是当单表问题不管。
- **三处「若当前版本已支持按表暂停」括注全部删除**（不是改，是删）。
- 边界：缺表缺列走 **E1013**，不许走 E1015；`last_error` 必须到字段级；`Next` 不许出现 `--resync`；统一 grep 短语「其它表也不会继续推进」。

## 与实现的对齐（本 PR 落地情况）

| 条目 | 落在哪 |
|---|---|
| §1 存储/写点/读点/位点不推进 | `internal/pgsink/paused.go`；用例 `TestPauseTableRegistersKeyKeepsPositionAndRecovers` |
| §1 跨轮存活 + `keys ⊆ 本轮表集合` | `internal/engine/engine.go` 的 `reconcilePaused`；用例断言 `at` 跨轮不变 |
| §2 清表 × 暂停 | `cleanBeforeRecopy` 不清记录；`snapshot()` 搬成后 `ClearPausedTable`（只删该 key）；pgsink 用例断言「删 a 之后 b 的记录仍在」 |
| §3 E1015 | `internal/core/codes.go` + `codes_test.go`（`TestSchemaMismatchSaysWholeRunStops`） |
| 写路径快检 | `pgsink.Apply` 遇到暂停表整批拒绝（`TablePausedError`）；pgsink 用例 `TestApplyRefusesPausedTable` |
