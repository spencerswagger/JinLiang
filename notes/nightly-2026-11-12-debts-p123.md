# 三条欠账收口（P1 块记账 rows / P2 两步写崩溃 / P3 reason 时态）

基线 main `b470361`。分支 `fix/data-debts-p123`。不改引擎状态机（`r.sink` 抽接口是 Core 的事）、不写门禁脚本。

---

## P1 `chunk_no=0` 的 `rows` 重放不刷新

**复现（真库，先红后绿）**：`TestIntegrationChunkStampRefreshesOnReplay`（`internal/mysqlsink/chunk_replay_integration_test.go`）

```
记块 1=2000、块 2=500 → 盖章 → 完成标记 rows=2500
重放块 1（2000 → 3000）→ 重新盖章
ChunkProgress.Rows = 3500（各块之和，对）
完成标记 rows   = 2500（**陈旧**）   ← 修复前这里就是断言失败点
```

**误判方向（修复前）**：完成标记是「这张表一共搬进去多少行」的唯一权威行，
一旦与各块之和脱节，两边会**永久**不一致：

- 展示层（`status` / doctor / verify 摘要）少报已搬行数；
- **判据层**：任何「按已搬行数判断搬没搬完 / 要不要重搬」的逻辑被陈旧值带偏——
  偏小的方向就是「该重搬的判成已搬完」＝**静默缺行**；偏大则白搬一遍。
  目前续跑判据走 `Done / LastNo / Cursor`（不看 rows），所以今天还没真的误判，
  但块记账已经在写（#95），这行迟早被当成行数依据——不能留一个会脱节的权威值。

**改法**：`MarkTableDone` 的 upsert 里刷新 rows。

- MySQL：`ON DUPLICATE KEY UPDATE done=1, rows=VALUES(rows)`
- PG（同一个缺口，SQL 不同、坑一样）：`ON CONFLICT … DO UPDATE SET done=true, rows=EXCLUDED.rows`
  + 对应用例 `TestChunkStampRefreshesOnReplay`（`internal/pgsink/chunk_integration_test.go`）。

**自查（要求的那条）：`MarkTableRecopyPending` 清完 `chunk_no=0` 之后首次写块的起点**
用例 `TestIntegrationRecopyPendingRestartsAtChunkOne`：清完之后 `ChunkProgress` **没有任何记录**
（连 Done 都没有）→ 重搬第一块从块号 **1**、游标 **空**（表头）开始 → 只写一块时 `Done=false`
（Done 只能来自 chunk_no=0）→ `LastNo=1 / Cursor=n1 / Rows=1200`。PASS。

**结论：修复**（不是降级）。

---

## P2 两步写之间 kill -9

**现状（两步的形状）**：① `PauseTable` 单 key `SELECT … FOR UPDATE` 读改写 + 独立事务提交；
② 同一批的「数据 + 位点」在另一个事务里提交（该表事件已被引擎丢掉）。崩在中间会留下
「记录在、位点没动、那批数据没写」。

**用例**（`internal/mysqlsink/paused_crash_integration_test.go`，两个 Sink 实例共享同一目标库 = 真崩真起）

1. `TestIntegrationPauseCrashBetweenRegisterAndSkip`（崩在 ①②之间，PASS 0.94s）
   - 首搬完成（块标记 + 完成标记 + 位点 `pos=500`）→ 登记暂停 → `Close()` 模拟进程消失；
   - 重启后：**记录在**、位点仍是 `500`（没被暂停带走）；
   - **不变量①**：暂停登记后写该表的事件 → 被写路径拒绝（`引擎没有过滤掉暂停表`），
     目标端仍是 10 行——「暂停已登记但该表行被写进去」这条被挡住；
   - **不变量②**：走契约 §1.6 四步（清块标记 → 清表 → 重搬 12 行 → 解除）→ 12 行、记录清空。
2. `TestIntegrationPauseCrashAfterSkipKeepsRecoverable`（崩在 ②之后，PASS 0.38s）
   - 登记暂停 → 空批推位点（`pos=900`，模拟「只剩别的表的事件」）→ `Close()`；
   - 重启后：记录在，而**块标记仍说「搬完了」**——这是危险态（暂停窗口里的行没有）；
   - 断言 `MarkTableRecopyPending` 把它翻回「欠一次重搬」（清完 `ChunkProgress` 里没有该表），
     再解除记录。不翻就会「暂停解除了、数据其实缺着」。

**结论：修复**（补的是用例与可证伪断言；写顺序本身没改——契约 §1.5 第 2 条「登记先行」已在 #95 落到位）。

---

## P3 `reason` 更新语义跨目标端不一致

**现状**：PG 侧第二次登记**覆盖** `reason`；MySQL 侧在 #95 里做成了**保留第一次**。
同一个 JSON 形状、两种时态，诊断时要先分清是哪个目标端。

**决定：统一成「最新」**（不是文档化差异）。理由：`at` 已经承担「停了多久」，
`reason`/`detail` 该描述「现在为什么停着」；保留首次原因会出现自相矛盾的记录
（`reason=type_changed` 而 `detail` 写着「列被改名」），用户还要二次解释。

**改法**：`mysqlsink.PauseTable` 去掉 `rec.Reason = old.Reason`（`rec.At = old.At` 保留）。
两边现在都是：

| 字段 | 时态 |
|---|---|
| `at` | **第一次**登记的时刻，永不覆盖 |
| `reason` / `detail` | **最新**一次（当前为什么停着） |
| `skipped_rows / skipped_from / last_skipped_pos` | **累计**（同一段欠账，重登记不清零） |

时态写进了 `internal/mysqlsink/paused.go` 的文件头（PG 侧同一口径，不需要两套图例）。
用例 `TestIntegrationPauseRegisterThenClear` 现在断言：第二次登记（`schema_drift`）后
`reason=schema_drift`、`detail=第二次`、`at` 仍是第一次那一刻。

> `status` / `doctor` 的暂停条目（契约 §1.8）还不存在（Core 的清单），等它落地时
> 按上表直接渲染即可——两边语义已一致，不需要「按目标端区分首/最新」的标注。

**结论：修复（统一），不是降级。**

---

## 自检

- `gofmt -l .` 空 / `go vet ./...` 绿 / `go test ./...` 全绿；
- `-tags=integration ./internal/mysqlsink`：全绿（含新增 4 格：P1 两格 + P2 两格）；
- `-tags=integration ./internal/pgsink -run TestChunk`：PASS（新增 PG 侧重放用例）；
- `-tags=integration ./internal/engine -run TestMySQLTarget`：12 条 PASS（块记账断言未受影响）。
