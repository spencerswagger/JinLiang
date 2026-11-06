# 津梁实现级设计：任务定义、命令面、预检、错误模型

> **2026-11-05 已改口径**：控制面是 WebUI + 内嵌数据库（见 CONTROL-PLANE.md）。
> 下文 `job.yaml` 保留为**导入/导出格式**，运行时真源是控制库里的 `jobs` 记录；
> 命令面里的 `plan` / `preview` 已并入 UI，CLI 只保留 `--json` 版给自动化。

> 这一层把 DESIGN.md 的口径落成能实现的东西。仍然不写功能代码。

## 1. 任务定义（控制库记录，yaml 为导入导出格式）

见 [examples/job.yaml](../examples/job.yaml)。四条硬规则：

1. **strict 解析**：出现未知字段直接报错，不静默忽略 —— 打错字比报错更危险。
2. **凭证不入 yaml**：`dsn` 只接受 `env:NAME` 或 `file:/path`，日志与 `plan` 输出一律脱敏。
3. **默认值必须显式**：`plan` 打印生效值（含默认），没有隐藏开关。
4. **可逆**：所有代做动作在 `Actions` 里带 `undo`；**控制库是唯一真源**，yaml 只是它的导入导出格式。

### 字段语义补充

| 字段 | 关键语义 |
|---|---|
| `mode.frequency` | 五档：`once` / `continuous+initial` / `continuous` / `interval` / `full_interval`。只有前两类承诺秒级；`interval` 需要 `incremental_column`（语义与「第二轮为什么又搬了几万行」见 §1.2） |
| `mode.lookback` | 定时增量的重叠窗口：每轮从「水位 − lookback」开始，重复部分靠 upsert 幂等。**缺省 = 一个 `mode.interval`，最少 1 分钟**；窗口大小是在「漏读」和「多写」之间取，宁重不漏（见 §1.2） |
| `target.on_exists` | 默认 `error`（撞表就停），`recreate` 会清空目标表、需要二次确认 + 备份提示（语义见 §1.1） |
| `content.schema_follow` | `whitelist` 只跟随四类 DDL（加列、放宽长度、加索引、可选删列），其余拦截并暂停该表 |
| `guard.on_exceed` | `pause` 最保守；`invalidate` 只在 PG 13+（有 `max_slot_wal_keep_size`）；`advance` 会跳过变更，必须显式确认 |
| `reliability.tx_oversize` | 两种形状都收：字符串 `spill`（默认）/ `fail`（旧写法，**行为不变**），或对象 `{mode, bytes, events}`（`bytes` ≥1 MiB、`events` ≥1000，可只给其中一项）。取值顺序 **yaml 显式值 > 环境变量 `JINLIANG_TX_MAX_*` > 内置默认（128 MB / 100 万事件）**；解析已支持两种形态（旧写法导出仍是字符串），`bytes`/`events` 的运行期生效在 B5 接；`spill` 未实现，两种 `mode` 当前都表现为超限报 `E1011`（详见 STREAMING §1） |

### 1.1 重搬与 `on_exists: recreate` 的语义（已知限制）

`on_exists: recreate` 会在重搬时**清空目标表**。若你在建任务之后往目标表手工写入过数据，**请先自行备份**——那部分数据会被一并清掉。B1 将加入「建表时行数/校验和」的比对，自动识别这种外部写入。

为什么现在只能靠提示、不能自动判断（免得读者以为我们在瞎谨慎）：

- 「这张表是不是本任务建的」靠目标端 `_jl_object` 里的 `table:<schema>.<table>` 记录判断；
- **块标记（`_jl_chunk`）证明不了所有权**——`BulkApply` 对 `append` 复用的表同样会写块标记，所以「有块标记」不等于「表是我们建的」；
- 于是存在唯一一个仍会删数据的组合：**记录在（表确实是我们建的）+ 用户事后往该表补写过行**。这种情况下重搬会连同补写的行一起清掉，目前只有 `recreate` 的二次确认能兜住；B1 的「建表时刻 vs 现在」行数/校验和比对才能把这一格也堵上。

`on_exists: append` 没有这个问题（不清表），但它有自己的代价：**源端删掉的行会在目标端残留**，因为 append 不负责删除。这是语义选择，不是缺陷——要消除残留就选 `recreate`，代价是上面那条。

### 1.2 `interval` / `full_interval` 的语义（靠水位轮询，不碰变更流水）

**先取表尾不会变成每轮重扫**：水位 `W0 = 搬快照之前`的表尾，下一轮从 `W0 − lookback` 读，只重读「快照期间新增的行 + 一窗回看」，而且**只发生一次**——那一轮结束 `saveWatermark(next)` 就把水位推到位。

所以 `interval` 任务第二轮如果搬了几万行，**那是正常的**：它搬的是「第一轮搬快照期间新写进来的行」，不是重复搬全量。第三轮起就只剩窗口内的零星增量。

三条配套语义（都容易误读，一起写在上面这段旁边）：

- **`interval` 不碰变更流水**（不建 slot、不建 publication），进度唯一依赖 `_jl_meta.watermark`。代价是**晚提交的行只能靠 `lookback` 回看窗口捞回**：`mode.lookback` 缺省值 = **一个 `mode.interval`，且最少 1 分钟**（间隔小于 1 分钟时仍取 1 分钟）。窗口越大越不容易漏，但重复读的行也越多——重复读由目标端 upsert 吸收，**宁重不漏**：漏读是重跑也补不回来的错，重读只是多一次写。
- **没有水位列的表**（`mode.incremental_column` 指向的列在表上不存在）只能**每轮整表重搬**。判据是 **fail-closed**：源库 `reltuples = -1`（从没 `ANALYZE` 过，等于「体量未知」）→ **拒绝**；已知且 **> 10 万行** → **拒绝**（大表每分钟一次全表拷贝不可接受）；小表 → 只 warn，继续搬。拒绝时给三条真出路：`ANALYZE` 一次 / 给表加更新时间列（推荐）/ 改 `full_interval` 明确接受周期整表重跑。
- **`full_interval`** 是「明确接受整表周期重跑」的那一档，不需要水位列，也不做上面的行数拦截。

**为什么这条写这么细**：定时增量的两档（`interval` / `full_interval`）与另外三档（`once` / `continuous` / `continuous+initial`）的差别是「有没有变更流水」，而用户看得见的差别只是「第二轮为什么又搬了这么多行」。这一段就是为了省掉那次误报。

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
