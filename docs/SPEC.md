# 津梁实现级设计：任务定义、命令面、预检、错误模型

> 这一层把 DESIGN.md 的口径落成能实现的东西。仍然不写功能代码。

## 1. 任务定义 `job.yaml`

见 [examples/job.yaml](../examples/job.yaml)。四条硬规则：

1. **strict 解析**：出现未知字段直接报错，不静默忽略 —— 打错字比报错更危险。
2. **凭证不入 yaml**：`dsn` 只接受 `env:NAME` 或 `file:/path`，日志与 `plan` 输出一律脱敏。
3. **默认值必须显式**：`plan` 打印生效值（含默认），没有隐藏开关。
4. **可逆**：所有代做动作在 `Actions` 里带 `undo`；yaml 是唯一真源，CLI/UI 改的都是它。

### 字段语义补充

| 字段 | 关键语义 |
|---|---|
| `mode.frequency` | 五档：`once` / `continuous+initial` / `continuous` / `interval` / `full_interval`。只有前两类承诺秒级；`interval` 需要 `incremental_column` |
| `mode.lookback` | 定时增量的重叠窗口：每轮从「水位 − lookback」开始，重复部分靠 upsert 幂等 |
| `target.on_exists` | 默认 `error`（撞表就停），`recreate` 需要二次确认 + 备份提示 |
| `content.schema_follow` | `whitelist` 只跟随四类 DDL（加列、放宽长度、加索引、可选删列），其余拦截并暂停该表 |
| `guard.on_exceed` | `pause` 最保守；`invalidate` 只在 PG 13+（有 `max_slot_wal_keep_size`）；`advance` 会跳过变更，必须显式确认 |

## 2. 命令面

| 命令 | 作用 | 会不会写库 |
|---|---|---|
| `jinliang plan job.yaml` | 预演：解析、探测、预检、算体量、打印所有生效值与动作清单 | 只读 |
| `jinliang preflight job.yaml` | 只跑检查，输出三级报告 | 只读 |
| `jinliang run job.yaml` | 执行任务（全量 + 增量 / 按频次） | 会 |
| `jinliang status [job]` | 一屏：阶段、位点、延迟、吞吐、队列、重试、告警 | 只读 |
| `jinliang verify job.yaml [--table t]` | 分块校验，输出订正 SQL | 只读（`--fix` 才写） |
| `jinliang preview job.yaml --table orders --rows 10` | 展示「源行 → 目标行」 | 只读 |
| `jinliang doctor` | 体检：孤儿对象、位点落后、租约冲突、目标 schema 漂移 | 只读 |
| `jinliang gc [--job x] [--force]` | 清理孤儿对象；默认 dry-run | `--force` 才写 |
| `jinliang conflicts list\|resolve` | 人工冲突处理（`manual` 策略下） | `resolve` 会 |
| `jinliang undo <action-id>` | 撤销某个代做动作 | 会 |

**退出码**：`0` 成功 · `3` 需要你做一个动作或确认一个风险 · `4` 可重试失败已用尽 · `5` 冲突未决 · `1` 一般失败 · `2` 参数错误。

**`3` 的含义分两种，提示里会说清**：物理前置没满足（例如没开变更流，跑不了）、或者你确认风险即可继续（例如无主键大表）。**后者允许 `--accept-risk` 直接继续。**

## 3. 预检清单（固定 ID，报告里可引用）

### 通用
`NET-01` 连通性 · `NET-02` TLS · `NET-03` 时区一致（源与目标 `TimeZone` 不同会导致时间戳偏移）· `VER-01` 版本在支持档内。

### PostgreSQL
| ID | 检查 | 不通过时 |
|---|---|---|
| PG-01 | `wal_level = logical` | blocked（需重启），给 ALTER SYSTEM + 重启 + 回滚 |
| PG-02 | `max_replication_slots` 余量 ≥1 | autofixable（`ALTER SYSTEM` + reload） |
| PG-03 | `max_wal_senders` 余量 ≥1 | autofixable |
| PG-04 | 账号有 `REPLICATION`（或 superuser） | blocked 或 autofixable（我们建账号并授权） |
| PG-05 | `pg_hba.conf` 允许 replication 连接（用真连验证） | blocked（给具体行 + 需 reload） |
| PG-06 | 目标表有主键 / `REPLICA IDENTITY` | **warn**：无主键只能 insert-only、不可块级续跑；照跑，附改善建议（加主键） |
| PG-07 | `max_slot_wal_keep_size` 是否存在 | PG≤12：记一条「无磁盘保护阀」补偿说明 |
| PG-08 | 是否 primary（`pg_is_in_recovery()`） | blocked（指向正确节点） |
| PG-09 | `track_commit_timestamp` | 关：记「时间戳新者胜」精度受限 |
| PG-10 | `pg_wal` 占用 + slot 落后量 | 超 guard 阈值即告警 |

### MySQL
| ID | 检查 | 不通过时 |
|---|---|---|
| MY-01 | `log_bin = ON` | blocked（my.cnf + 重启） |
| MY-02 | `binlog_format = ROW` | blocked（需重启） |
| MY-03 | `binlog_row_image = FULL` | autofixable（`SET GLOBAL`，需 SYSTEM_VARIABLES_ADMIN） |
| MY-04 | 账号有 `REPLICATION SLAVE` / `REPLICATION CLIENT` / `SELECT` | blocked 或 autofixable（我们建账号） |
| MY-05 | `server_id` 唯一且未与源冲突 | blocked（撞号会把真正的从库踢下线，给建议值） |
| MY-06 | `binlog_expire_logs_seconds` ≥ 预期最长断线 | 提示风险（可 autofix） |
| MY-07 | `binlog_transaction_compression` | 默认自身解压；解不了才 blocked，并提示「关闭源端压缩」 |
| MY-08 | 表的存储引擎 | MyISAM → **warn**（无事务，崩溃时可能不一致），照跑并标注 |
| MY-09 | 认证插件可用（8.4/9.x 移除 native） | autofixable（建 caching_sha2 独立账号） |
| MY-10 | 主从角色（`SHOW REPLICA STATUS` / `SHOW BINARY LOG STATUS`） | 指向主库，或说明只读副本的限制 |

## 4. 错误模型

每个错误必须回答「我该做什么」，而不是抛一个数据库原文。

```
Code      E1001                 稳定编号，写进文档，可被 issue 引用
Class     user_action | retryable | fatal | bug
Message   人话（中文），不含表名以外术语
Detail    原始错误 / SQLSTATE，只在 --verbose 显示
NextStep  一到三条可执行动作
DocURL    对应文档锚点
```

分类决定行为：`retryable` → 按 5s / 20s / 60s 退避重试（≤3 次）；`user_action` → 暂停任务并挂起等处理；`fatal` → 停止；`bug` → 停下并提示带日志提 issue。

**示例**
| Code | Class | Message | NextStep |
|---|---|---|---|
| E1001 | user_action | 源库没有开启「变更流水」（wal_level=replica） | 执行给出的 ALTER SYSTEM；重启实例；回来 `jinliang run` 续跑 |
| E1002 | user_action | 目标表已存在，按当前设置停止 | 改 `on_exists: append`，或换 schema |
| E2001 | retryable | 目标库连接被重置 | 自动重试中；若持续，检查网络/连接数上限 |
| E3001 | fatal | 位点与源库不连续（变更流水已被清理） | 需要重新初始化：`jinliang run --resync` |
| E4001 | bug | 解码遇到未支持的类型 oid=… | 附日志提 issue |

## 5. 指标（Prometheus）

| 指标 | 含义 |
|---|---|
| `jinliang_up` | 进程存活 |
| `jinliang_phase` | 阶段：snapshot / catchup / streaming / paused |
| `jinliang_lag_bytes` | 位点落后字节（PG: `pg_current_wal_lsn() − confirmed_flush_lsn`） |
| `jinliang_lag_seconds` | 最近一次源提交 → 目标可见的延迟 |
| `jinliang_rows_total{phase,result}` | 已应用行数 |
| `jinliang_tx_total{result}` | 事务数（ok / retry / dead_letter） |
| `jinliang_batch_rows` / `jinliang_batch_wait_seconds` | 批次大小与等待（调 group commit 用） |
| `jinliang_queue_depth` | 背压队列水位 |
| `jinliang_slot_wal_bytes` | 同步书签保留的 WAL（护磁盘） |
| `jinliang_conflict_total` | 冲突计数（manual 策略下要盯） |
| `jinliang_dead_letter_rows` | 死信积压 |
| `jinliang_verify_mismatch_total` | 校验发现的不一致行数 |

## 6. 一致性校验协议

1. **分块**：按主键区间切块（每块 10 万行），源与目标各自算 `count(*)` 与 `sum(hash)`。
2. **哈希函数**：对每行所有列做规范化（NULL 哨兵、时间统一 UTC、浮点用十进制字符串、二进制按 hex）后取 `xxhash64`，再求和 —— 服务端算，不拉数据。
3. **抽样**：不一致的块拉全行逐列比对，输出字段级差异。
4. **订正**：生成 `INSERT ... ON CONFLICT/DUPLICATE KEY UPDATE` 语句，`--fix` 才执行并记录到 `_jl_conflict`。
5. **自测**：测试里故意破坏目标库（改一行、删一行、改精度），`verify` 必须报出。
