# A3（登记失败 → 不许「跳过了但没记下」）的覆盖地图

`registerPause` 有**两个调用点**，性质不同，别当成一件事：

| 调用点 | 时机 | owner | 用例 | 强度 |
|---|---|---|---|---|
| `pauseSink.Apply`（写路径） | 流式每一批命中 `PausableError` | **Gate** | `TestPauseRegistrationFailureFailsWholeBatch`（`pause_registration_failure_test.go`，假目标端） | **结构性**：`inner` 只被调 1 次（= 不许原地重试）；注入 `register-pause-neutered` 已 KILLED |
| `prepare` / `prepareGeneric`（对账期） | prepare 期发现结构不一致 → 登记 | —（**空缺**） | 无 | — |

## 我撤掉了什么、为什么

1. `#101` 的 `integration_pause_register_fail_test.go`（PG 目标端、prepare 期）**已删除**：
   断言的行为与 Gate 那条相同（「登记失败 → 整轮失败、0 行、位点不动、无半条记录」），
   两个 owner 各写一份只会互不认账。行为的所有权归 Gate。
2. **没有**改写成 MySQL 目标端版本，原因写清（不是降级，是做不出同等强度）：
   - 要拿「**不许原地重试**」这个结构性判据，必须能数出「登记尝试了几次」。
     PG 上我用触发器 `RAISE`（写路径在事务里，`RAISE` 之前的探针表插入会随之回滚吗？
     ——不会，因为探针表插入发生在 `RAISE` **之前**且 PG 的 plpgsql 用子事务语义，
     实际上也回滚；#101 那条正是靠 `RAISE` 让 `PauseTable` 失败、靠「run 失败 + 0 行」判的，
     **它也没有数尝试次数**）。
   - MySQL 上更糟：触发器体只能是单条语句或用 `BEGIN…END`（需要 `DELIMITER`/`multiStatements`
     的区别对待），而任何「先记账再报错」的写法都会被**语句级回滚**一起带走 —— 计数拿不到。
     拿不到计数，就只能比文案（「错误里有没有那句包裹语」），那正是任务里明令禁止的降级。
   - 剩下的替代口径（performance_schema digest 计数）依赖 CI 镜像里 `performance_schema` 开着，
     不可靠；不写「可能会假绿」的断言。

## 结论与建议

- **写路径**：Gate 的用例（假目标端 + 调用计数）是唯一 owner，强度够（结构性判据）。
- **对账期**：目前**没有**用例。它与写路径共用 `registerPause`，差别只在「谁先发现结构不一致」。
  MySQL 目标端这一路（`detectPrepareDrift` → `PauseTable`）的**正常路径**已由
  `TestMySQLTargetPauseOnlyThatTableAndRecovers` 覆盖（登记成功 → 跳过 → 恢复）；
  「登记失败」这一支建议由 Gate 用同一套假目标端手法补一格（`prepareGeneric` 的 fake 版），
  那不需要真库、也不会比文案。
- 若要真库覆盖，PG 目标端可以回到 `#101` 那版（`git show 0b0d155^:internal/engine/integration_pause_register_fail_test.go`），
  但它**没有**「尝试次数」这条结构性判据，只钉「整轮失败 + 0 行 + 位点不动 + 无记录」。
