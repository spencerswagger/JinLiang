# 运行时：生命周期、暂停恢复、热更新

## 1. 状态机

阶段定义与合法迁移在 [`internal/state/phase.go`](../internal/state/phase.go)（已带测试）：

```
idle -> preflight -> prepare -> snapshot -> catchup -> streaming -> draining -> done
                        |                    |          |
                        +------> failed <----+----------+--> paused --+
                                                                       +--> streaming / snapshot
```

规则：
- `preflight` 不过 -> 回 `idle`（等用户修好再来），**不算失败**。
- `snapshot` 不能直接跳 `streaming`，必须经 `catchup` 追平（防空洞）。
- `paused` 允许回 `snapshot`（快照失效时整段重来）。
- `done` / `failed` **不持有租约**，其他实例可以接手。

## 2. 启动序列

```
1. 解析 yaml（strict）-> 算 job_hash
2. 连源/目标（只读探测）-> 预检 -> 有 blocked 就退出码 3
3. 抢租约（目标库 _ds_lease，TTL 30s）
   |- 未过期 -> E1005 退出（绝不双写）
   |- 已过期 -> 接管，记一条 action
4. 读 _ds_meta
   |- job_hash 变了 -> 打印差异，要求 run --accept-change（不自动改）
   |- phase=snapshot -> 读 _ds_chunk 续跑未完成块
   |- phase=streaming -> 从目标库位点继续
5. 执行 prepare 动作（需确认的部分已确认）
6. 进入 snapshot / catchup / streaming
```

## 3. 暂停、恢复、优雅退出

| 事件 | 行为 |
|---|---|
| `SIGINT` / `SIGTERM` | 进入 `draining`：停读 -> flush 当前批次并提交（含位点）-> advance 源端 -> 释放租约 -> 退出码 0 |
| 用户 `jinliang pause` | 同上，但保留租约（TTL 续期）与阶段，`resume` 原地继续 |
| 可重试错误 | 退避 5s/20s/60s，<=3 次；仍失败 -> `paused`，退出码 4 |
| 用户可解错误 | 立即 `paused`，退出码 3，打印定制步骤 |
| 租约失联（续期失败） | 视为双写风险 -> 立刻停写并退出码 1，让另一个实例独占 |

**退出前必须提交或丢弃整个未完成事务**，不允许"半批"落库。

## 4. 热更新（`reload`）

`SIGHUP` 或 `jinliang reload` 重新读 yaml，比对后分类处理：

| 字段 | 能否热改 |
|---|---|
| `content.*`（列/过滤/转换）、`conflict.policy`、`reliability.batch_*`、`guard.*` | 可以，下一批生效 |
| `observability.*` | 可以，指标端点立即切换 |
| `source.*` / `target.*` / `mode.*` / `content.schema_follow` | 不行，必须重启任务（会提示原因） |

`job_hash` 只覆盖「影响语义」的字段，纯展示字段变动不触发变更提示。

## 5. 时间与时区

- 源端时间按列类型解释；`timestamptz` 统一转 UTC 存储。
- 延迟测量用「源提交时间 -> 目标可见时间」，源端拿不到提交时间时（PG `track_commit_timestamp=off`）用读取时刻估算，并在状态里标注 `estimated`。
- 内部所有时间用 UTC，只在输出时按 `TZ` 渲染。

## 6. 崩溃恢复

见 [STATE-SCHEMA.md](STATE-SCHEMA.md)：目标库 `_ds_meta.position` 是唯一权威，源端书签只是护栏；
三种崩溃点都推过（提交前 / 提交后未 advance / advance 中途失败），结论是不重不丢。
