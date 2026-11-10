# 按表暂停（裁决 A）落地：只停该表、位点前进、恢复靠整表重搬

分支 `jinliang/pause-per-table`（基于 main `c7e4f2e`）。契约：`docs/CONTRACT-PAUSE-AND-POSITION.md`
（Ark 的 `contract/pause-and-position`，冻结版 v1）。本文件记**实现怎么落的、证据到哪、还差什么**。

---

## §1 做了什么（逐条对契约 §4.5「Core」清单）

| 契约要求 | 落在哪 | 状态 |
|---|---|---|
| 引擎持有暂停集合，`sink.Apply` 删掉暂停判定 | `engine.pause_skip.go`（`loadPausedSet`/`pausedRecord`）、`pgsink.Apply` 删块 | 完成 |
| 批组装时按表丢弃：保留 `EvBegin/EvCommit`，丢该表的行事件与 `EvDDL` | `pauseSink.filter` | 完成 |
| 首次暂停「登记先行」：`PauseTable` 成功后才允许跳过；失败即整批失败 | `pauseSink.Apply` 先 `registerPause` 再重试 | 完成 |
| `NoteSkipped` 每批累加（行数 + `last_skipped_pos`） | `pgsink.NoteSkipped` + `pauseSink.noteSkipped` | 完成 |
| `reconcilePaused` 改判据：对齐后 `MarkTableRecopyPending`，**不再**用 `copyDoneFor` 当解除条件 | `engine.reconcilePaused` 重写 | 完成 |
| 重搬搬成 → `ClearPausedTable` 单 key + note | `snapshot()`：**先 `MarkTableDone` 再解除**（顺序按契约 §1.6 步 4 翻转） | 完成 |
| `resume=manual` 的用户表：不自动清空、不自动重搬 | `resumeModeFor`（查 `_jl_object` 判所有权）+ reconcile 里 manual 分支 | 完成 |
| prepare 收集**全部**不一致的表，不是撞到第一张就返回；暂停表存量也不搬 | `pgsink.TakePendingPauses` + `engine.skippedTables`（copy 回调里丢弃） | 完成 |
| E1015 三句文案 + 用例反转 | `internal/core/codes.go`、`codes_test.go`（`TestSchemaMismatchSaysOnlyThatTableStops`） | 完成 |
| 用例 A1–A6 + 恢复四步 | `integration_paused_test.go`（A1/A2/A5 + 跳过记账 + 清表重搬）、`paused_reconcile_test.go`（标重搬/留记录/manual 不动/continuous 解除带代价） | 部分（见 §3） |

## §2 关键实现决定（会被问的三条）

1. **过滤包在 `refMappedSink` 外面**（`newPauseSink(newRefMappedSink(...))`）：事件里是源端表名，
   而 `paused_tables` 的 key 是目标端裸表名，靠 `r.targetRef` 对齐——与 `_jl_chunk.table_name` 同源。
2. **`skippedTables` 留在 `r.tables` 里**：跨轮存活判据是「这张表还在不在本轮范围内」，
   把它从表集合里删掉会让 `reconcilePaused` 认为「已出范围」并把暂停记录清掉（A6 反着红）。
   所以它只影响「搬不搬存量」，不影响「在不在范围」。
3. **`EnsureSchema` 不再等于「结构对齐了」**：它把不一致收进 `pendingPauses` 交给引擎。
   因此 `reconcilePaused` 必须跳过**本轮刚发现**的那些表，否则会当场把它们标成「待重搬」
   → 拿旧结构重搬一遍 → 搬运失败、整轮白跑（这一步实现时真的踩到过，用例能复现）。

## §3 证据（受环境影响，如实写）

**单元/静态**：`gofmt -l .` 空、`go vet ./...` 干净、`go test ./...` 全绿
（`internal/connect` 两条真连库用例在库不在线时红，属环境）。

**真库**：`TestPauseTableRegistersKeyKeepsPositionAndRecovers`（PG→PG 两张表，A 改类型 + A/B 各插一行）
在实现过程中跑到过「prepare 不再打死整轮、A 被登记、B 照写」的位置，但**没有拿到一次完整绿的收尾**：
本机 PostgreSQL 在测试期间**反复重启**（`pg_postmaster_start_time()` 22 秒一变，`connect refused` 与
`readiness 57P03` 交替出现，`dmesg` 里是 docker veth 反复重建），流式用例活不过 3 秒。
`03:0x` 之后 PG/MySQL 两个库同时下线，连 `internal/connect` 的两条健康检查都红。

**结论**：代码路径按契约落完，**验收数字（三种组合行数+md5+耗时、DDL 表级暂停真跑、kill -9 重搬 0 行）
这一轮拿不到**——不是「没跑」，是库在这段时间不可用。库回来后要补的清单：

1. `go test -tags=integration ./internal/engine -run 'Pause' -count=1`（三条）
2. 三种组合的 `scripts/b1_*.sh`（MySQL→MySQL / MySQL→PG / PG→MySQL）
3. DDL 改类型真跑：A 停、B 追平、`status` 显示 `skipped_rows`、对齐后重搬追平（行数 + md5）
4. 反向注入：把 `MarkTableRecopyPending` 换成「只 upsert 不清表」→ 恢复用例必红（幻行那条已经埋好数据：
   暂停窗口里源端删了 50 行，用例断言重搬后剩 251 行）

## §4 没做的（明确边界，不假装）

1. **`ExitPaused=3`**：Ark 的硬口径 3 要求「跑完但有表没写」用退出码 3 区分。这要动 CLI/`Result`/
   `status` 的既有语义（现在只有 0/1/130），且 `docs/ERRORS.md` 与 CLI 文档在别人手里——
   **没动**，留给单独一片做（改之前要确认控制面/CLI 用例不依赖退出码集合）。
2. **`mysqlsink` 同构暂停列**（契约片 C，Data）：MySQL 目标端仍走 `pauseUnsupportedError` 明确失败。
3. **`classifyError` 去 `pgsource.` 依赖**（契约片 B）：没动。
4. **streaming 中途「结构又对齐了」的路径**（契约 §1.6 最后一条）：本轮只在 prepare 判定对齐；
   流式期间一直跳过，等下一次 `run` 收口。

## §5 变异验证（写下来的、还没跑的）

| 变异 | 应该红的地方 |
|---|---|
| `pauseSink.filter` 不丢暂停表事件 | `integration_paused_test.go` ⑤（A=0 会变成 1） |
| `registerPause` 失败仍继续提交 | 还没写这一格（A3）——**欠一条** |
| `MarkTableRecopyPending` 改成「只 upsert 不清表」 | 恢复用例（251 行 / md5） |
| `snapshot` 里解除改回「结构对齐就解除」 | `paused_reconcile_test.go`（记录会消失） |
