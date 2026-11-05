# 津梁 JinLiang · 设计基线 v3（先设计不实现）

Go 主干 · **MIT** · 对外开源 · 一期只做 MySQL 与 PostgreSQL（含同构）· 无 UI · 无同表双向
**测试矩阵驱动：9 个 PG 大版本 + 4 个 MySQL 家族，全部覆盖。**

---

## 0. 设计原则

1. **用户只谈数据，不谈机制。** 不出现 WAL / binlog / LSN / GTID / slot / publication / LogMiner。
2. **能替他做的不问，做不到的教他做。** 三级：已满足 / 我可代做（可逆、不动数据、不重启）/ 需要你或 DBA。
3. **每个自动动作可查可回滚。**
4. **`job.yaml` 是唯一真源**（UI 二期）。
5. **一切靠矩阵证明，不靠"本地跑过"。** 连接器必须交 `testmatrix.yaml`，缺失即 PR 红。
6. **不拒绝，只提示（2026-11-05 拍板）。** 工具的本分是把风险讲清楚、给出改善路径，决定权在用户。
   全产品只有两类地方会停下来：**物理上做不到**（没有变更流、没有权限、连的是只读副本）
   与**安全底线**（私钥文件权限不对就不读它）。其余一律提示 + 允许：无主键表照跑（insert-only）、
   超大表照跑（建议改增量）、MyISAM 照跑（标注无事务）、不认识的类型按 text 落地（标注丢语义）。
7. **提示即文档。** 用户不会去读文档，也不会被丢链接说服。每条提示必须自带四段：
   **发生了什么 / 为什么 / 你现在能做什么（含命令）/ 这么做的风险**。文档只服务开发者与贡献者。

---

## 1. 一期范围

| 组合 | 方向 | 说明 |
|---|---|---|
| MySQL → MySQL | 单向 | 同构，含跨版本（5.7 → 8.4 这种升级割接） |
| PostgreSQL → PostgreSQL | 单向 | 同构，含跨版本（10 → 18 升级割接，主力场景） |
| MySQL ↔ PostgreSQL | 单向（两个方向） | 异构，双向留二期 |

不做：同表双向、Web UI、Oracle/达梦/金仓等（框架留好，二期接）。

---

## 2. 版本矩阵（本期要全覆盖的）

### 2.1 PostgreSQL：10 – 18，**9 个大版本**

| 版本 | 发布 | EOL | 支持档 | 关键差异（影响同步） |
|---|---|---|---|---|
| 10 | 2017-10 | 2022-11 | 尽力 | 逻辑复制原生首发（pgoutput）；**无** TRUNCATE 复制、**无** `max_slot_wal_keep_size`、**无** `pg_replication_slot_advance()` |
| 11 | 2018-10 | 2023-11 | 尽力 | + TRUNCATE 复制、+ slot advance |
| 12 | 2019-10 | 2024-11 | 尽力 | 分区/权限调整；仍无 slot 磁盘阀 |
| 13 | 2020-09 | 2025-11 | 尽力 | **+ `max_slot_wal_keep_size`（磁盘阀）**、+ `publish_via_partition_root` |
| 14 | 2021-09 | 2026-11 | 全 | **+ 流式大事务（protocol v2）**、+ 二进制初始拷贝 |
| 15 | 2022-10 | 2027-11 | 全 | **+ 行过滤 / 列清单**、+ 两阶段提交解码（`TWO_PHASE`） |
| 16 | 2023-09 | 2028-11 | 全 | + 订阅端并行 apply；+ `pg_stat_io` |
| 17 | 2024-09 | 2029-11 | 全 | **+ slot 随主备切换存活（failover）**、+ `pg_createsubscriber` |
| 18 | 2025-09 | 2030-11 | 全 | + 冲突检测视图 `pg_stat_subscription_stats`、+ `idle_replication_slot_timeout`；**宣称含 DDL 复制能力（需实测确认）** |

### 2.2 MySQL：4 个家族

| 版本 | 状态 | 支持档 | 关键差异（影响同步） |
|---|---|---|---|
| 5.7 | EOL 2023-10 | 尽力 | 无 `LOCK INSTANCE FOR BACKUP`、无原子 DDL、无 `binlog_expire_logs_seconds`（用 `expire_logs_days`）、默认 `mysql_native_password`、`SHOW MASTER STATUS` 可用 |
| 8.0 | EOL 2026-04 | 全 | `caching_sha2_password` 默认、原子 DDL + INSTANT ADD COLUMN（8.0.12+）、`LOCK INSTANCE FOR BACKUP`、`binlog_transaction_compression`（8.0.20+，**解析必须支持解压**） |
| 8.4 LTS | 2032 之前 | 全 | **`mysql_native_password` 默认关闭**、**`SHOW MASTER STATUS` 已移除**（改 `SHOW BINARY LOG STATUS`）、MASTER/SLAVE 术语移除、GTID tags |
| 9.x 创新版 | 滚动 | 全（跟最新） | **`mysql_native_password` 插件彻底移除**、`default_authentication_plugin` 变量移除 |

> MariaDB 不在本期（binlog 前缀 `Annotate_rows`/GTID 格式不同），但连接器留同族复用点。

### 2.3 覆盖数字

| 矩阵 | 全笛卡尔 | 说明 |
|---|---|---|
| PG 同构 | 9 × 9 = **81** | 含同版本、跨版本、升级、降级（降级只验「给提示并允许继续」） |
| MySQL 同构 | 4 × 4 = **16** | |
| MySQL → PG | 4 × 9 = **36** | 异构主力 |
| PG → MySQL | 9 × 4 = **36** | |
| **合计链路** | **169** | × 8 个场景 ≈ **1352 个测试单元** |

八场景：S1 初始化快照 / S2 增量 CRUD / S3 大事务 / S4 DDL 白名单 / S5 崩溃恢复（kill -9）/ S6 孤儿清理 / S7 类型边界 / S8 性能基线。

---

## 3. 分层矩阵（把 169 条管住，而不是每次都跑全量）

| 层 | 内容 | 链路数 | 何时跑 |
|---|---|---|---|
| **T1 自洽** | 每版本 → 同版本（PG 10→10 … 18→18；MySQL 5.7→5.7 … 9.x→9.x） | 13 | 每 PR（只跑改动连接器那部分）× 3 场景 |
| **T2 相邻升级** | n → n+1（PG 10→11 … 17→18；MySQL 5.7→8.0→8.4→9.x） | 11 | 每日 nightly |
| **T3 极端跨度** | 最老 ↔ 最新（PG 10↔18、MySQL 5.7↔9.x）；降级方向只验「给出提示并允许继续」 | 4 | nightly |
| **T4 异构轮转** | MySQL 4 × PG 9 × 2 方向，按周轮转抽样（每周换一个源版本组合） | 72 → 每周抽 12 | nightly 抽样 / release 全量 |

- **PR**：改动连接器的 T1（13 条里的子集）× 3 场景 + L1 单测 → 目标 **≤15 分钟**。
- **nightly**：T1 + T2 + T3 + T4 抽样 ≈ 40 链路 × 8 场景 ≈ 320 单元 → 并行 20，**30–50 分钟**。
- **release（每周/里程碑）**：169 链路 × 8 场景 ≈ 1352 单元 + 影子对拍 1000 seeds + 性能基线 → **2–3 小时**。

**为什么这么分**：169 条全量每 PR 跑不现实；但**一个版本都不许漏**——漏的那条正好是用户的生产版本。分层保证每周全量覆盖一遍，同时 PR 不堵。

---

## 4. 能力探测，而不是版本号分支

按版本号写 `if version >= 13` 是维护灾难（补丁版回溯、厂商分支、云上魔改）。**一律运行时探测能力**，探测结果进预检报告：

| 能力 | 探测方式 | 缺失时的降级 |
|---|---|---|
| 逻辑复制原生 | 建临时 slot / 读 `pg_settings.wal_level` | PG ≤ 9.6：提示「没有内置变更流」，给替代（定时增量 / 一次性） |
| 快照导出 | `CREATE_REPLICATION_SLOT ... EXPORT_SNAPSHOT` 是否被接受 | 短锁取位点（并如实标注取位点瞬间） |
| slot 磁盘阀 | `pg_settings` 里有没有 `max_slot_wal_keep_size` | **只能告警，不能自动失效**，界面上明说「你的版本没有磁盘保护阀」 |
| 流式大事务 | `pgoutput` 协议版本协商 | 大事务全量缓冲，超内存即 spill 到磁盘；不设硬性拒绝 |
| 行过滤 / 列清单 | `pg_publication_rel.prattrs`/`prqual` 存在性 | 全表传输 + 客户端侧过滤（带宽上升，界面标出） |
| TRUNCATE 复制 | pgoutput 消息支持 | 目标端定期校验 + 告警「源端 TRUNCATE 不会被同步」 |
| `SHOW BINARY LOG STATUS` | 语句是否可用 | 回退 `SHOW MASTER STATUS`（5.7–8.0） |
| `LOCK INSTANCE FOR BACKUP` | 语句是否可用 | 回退 FTWRL；云上被禁则「快照后取位点 + 重叠去重」 |
| binlog 压缩 | `binlog_transaction_compression` 变量 | 默认继续并尝试自行解压；解不了才提示「需关闭源端压缩」 |
| 认证插件 | `mysql.user.plugin` 分布 | `caching_sha2` / `native` 三态适配；8.4/9.x 缺 native 时给迁移建议 |

**产品表现**：预检报告里写「你的 PG 12 没有磁盘保护阀，我们会用告警代替自动失效」，而不是抛一句 `max_slot_wal_keep_size is not a valid parameter`。

---

## 4A. 降级与补偿：先补偿，再提示（永不因风险拒绝）

**原则：降级不能降正确性。** 每个能力缺口按三步走 ——
① **自动补偿**（用户无感，首选）→ ② **有代价的替代**（如实标注，可开关）→ ③ **提示风险并允许继续**（给出改善路径，不阻断）。
只有「补偿会让结果不一致」或「补偿需要用户不授予的权限」时才落到 ③，且必须给出他能走的下一步。

### PostgreSQL

| 缺口 | 涉及版本 | ① 自动补偿 | ② 有代价替代 | ③ 最后提示（仍可继续） |
|---|---|---|---|---|
| 无 slot 磁盘阀 | 10–12 | 外部监控 slot 落后量，超阈值**暂停并告警**；同时主动推进位点 | 定期全量校验 + 重建 | 用户既不许推进也不许告警时 |
| 无 `pg_replication_slot_advance()` | 10 | 用临时解码连接消费到目标 LSN 后丢弃（**显式确认**，因为会越过变更） | 保持不动，只告警 | — |
| 无 TRUNCATE 复制 | 10 | 定时校验 + 自动订正（清空重建）；或源端触发器把 TRUNCATE 写进 `_jl_event` | 只告警，交给用户 | — |
| 无流式大事务 | ≤13 | 大事务 **spill 到磁盘临时文件**，不占内存 | 缓冲上限调到可用内存内 | 事务超过 512 MB 且用户禁用 spill |
| 无行过滤 / 列清单 | ≤14 | 快照阶段只 SELECT 需要的列；增量阶段客户端侧过滤 | 建议用视图当源 | — |
| 无两阶段解码 | ≤14 | 侦测 `max_prepared_transactions > 0` + 存在未完成 prepared xact 时暂停告警 | 建议置 0 | 用户有 2PC 且不接受暂停 |
| **目标版本更低**（如 18 → 17） | 任意降级 | 逐特性核对并落地：虚拟生成列→普通列；`uuidv7()`→客户端生成；新类型→TEXT/JSONB；行过滤→客户端过滤；DDL 复制→关闭（我们自己做白名单） | 目标端加影子列 / 落地 JSON | **只提示这一张表**，允许继续 |

### MySQL

| 缺口 | 涉及版本 | ① 自动补偿 | ② 有代价替代 | ③ 最后提示（仍可继续） |
|---|---|---|---|---|
| 无 `LOCK INSTANCE FOR BACKUP` | 5.7 | 毫秒级 FTWRL 取位点 | 快照后取位点 + 5 分钟重叠去重（标注极小重复可能） | 用户要求零重复且禁锁 |
| native 认证被禁 / 移除 | 8.4 / 9.x | **自动建独立 caching_sha2 账号**（不动用户原账号） | 建议升级客户端驱动 | — |
| `SHOW MASTER STATUS` 移除 | 8.4+ | 双语法探测；后备读 `performance_schema.log_status` | — | — |
| binlog 事务压缩 | 8.0.20+ | **实现 zstd 解压**（首选，源码端开了也必须能读） | 建议源端关闭（有体积代价） | 既不关也解不了 → 提示并要求确认后继续尝试 |
| 无原子 DDL | 5.7 | DDL 白名单只允许可安全重放的；DDL 期间短暂暂停该表 | 轮询 `information_schema` 感知结构变化 | 拦截该表 |
| GTID 关闭 | 通用 | file+pos 位点（默认） | GTID 可用时优先（更稳） | — |

### 判定顺序（实现时要写成一张表驱动，不是散落的 if）
`探测能力 → 查补偿表 → 有 ① 就用 ① 并在预检里记录"已补偿" → 否则问用户是否接受 ② 的代价 → 都不行才 ③`。
预检报告里每一处降级都要有一行白话说清："你的 PG 12 没有磁盘保护阀，我们改用「落后 5 GB 就暂停」来护住磁盘。"

---

## 5. EOL 政策（诚实的支持档）

- **全支持档**：PG 14–18、MySQL 8.0/8.4/9.x — 缺陷必修，CI 全场景。
- **尽力档**：PG 10–13、MySQL 5.7 — 仍进矩阵、仍跑 nightly，但上游不再出安全补丁，文档与预检里明示「已 EOL，建议升级」，缺陷优先级降一级。
- **不支持**：PG ≤ 9.6（无原生 pgoutput）、MySQL ≤ 5.6（无 row image 完整支持）、MariaDB（本期）。

---

## 6. 其余设计（v1 继承，简述）

- **预检三级** + 定制教程（按实例真实值渲染）+ **可逆准备全部前置，最后只剩"重启一次"**。
- **垃圾清理**：孤儿四条判定、默认 dry-run、`doctor`/`gc`、租约防双跑、位点与数据同事务（幂等）。
- **Catalog**：行数估算 / 主键 / 有无稳定主键（硬提示）/ 类型映射预览 / 单表覆盖 / 目标同名表默认报错。
- **频次五档**：一次性 / 初始化+实时 / 实时仅增量 / **定时增量（只要 SELECT）** / 定时全量（>500 万行提示：建议改增量，不阻断）。
- **方向**：一期单向 + 分表双向；同表双向二期。
- **内容**：列 / 行 / 结构可选；DDL 只跟随四类，其余拦截暂停。
- **转换**：类型/单位/时区/编码/枚举/脱敏/计算列；不做聚合、join、脚本；失败进死信表。
- **一致性接缝**：PG `EXPORT_SNAPSHOT` + 分块读 + 切流；MySQL 毫秒级取位点 + 一致性快照；位点两套（file+pos 与 GTID）。

---

## 7. 测试架构

- **L1 单元**（类型映射、DDL 解析、表达式、位点编解码、GC 判定）→ 每 PR，秒级。
- **L2 连接器契约**（连接/预检/catalog/快照/增量/位点/清理/类型边界）→ 真容器。
- **L3 端到端**（源容器 → 目标容器）→ 真容器 × 2。
- **L4 故障注入**（kill -9、网络分区、容器重启、磁盘满、slot 被删、schema 漂移）。
- **L5 性能基线**（退化 >15% 就红）。
- **影子模型对拍**：内存影子库 vs 目标库逐行比对；schema fuzz + 操作序列 fuzz；四条属性（最终一致 / 重放幂等 / 位点不倒退 / 不串位）；失败最小化成 seed。
- **校验器自测**：故意破坏目标库，`verify` 必须报出。
- **语料零本地依赖**：TPC-H / northwind / sakila / 各版本官方 regression DDL。
- **覆盖度报表进 README**：`postgres 10→10 … 18→18 = 9/9 ✔`、`mysql 5.7→5.7 … 9.x→9.x = 4/4 ✔`、`异构 72 条中已验证 12/12 轮转`。

---

## 7A. 性能指标与开销评估

### 7A.1 目标（SLO，都要进 L5 回归）

| 指标 | 目标 | 怎么测 |
|---|---|---|
| 快照吞吐 | 单核 ≥20k 行/s（200 B 窄行）、≥50 MB/s；8 核 ≥100k 行/s | TPC-H lineitem 1M / 10M / 100M 三档 |
| 增量吞吐 | 单流 ≥20k ops/s 峰值、≥10k ops/s 稳态（窄行） | sysbench 写入压测驱动 |
| 端到端延迟 | p50 <30 ms、p99 <100 ms（同机房，batch 窗口 1 ms） | 源端写 `clock_timestamp()`，目标端记录可见时间打点 |
| 内存 | 空转 <80 MB；增量 <256 MB；快照（4 并发块）<512 MB | RSS + pprof |
| CPU | 空转 <2%；20k 行/s 时 <1 核 | cgroup 统计 |
| 冷启动 | <200 ms（进程到首条业务日志） | 脚本计时 |
| 二进制体积 | <25 MB | 构建产物 |
| 位点落后 | 稳态 <5 MB WAL、<1 s | `pg_current_wal_lsn() - confirmed_flush_lsn` |
| verify | ≥50 MB/s/核；1M 行抽样 <30 s | 独立校验基准 |
| 退化门槛 | 任一指标比基线差 >15% 即红 | 基线 JSON 入库，release 比对 |

### 7A.2 对用户系统的开销（预检里就要算给用户看）

| 侧 | 开销项 | 控制手段 |
|---|---|---|
| 源库 | 1 个复制连接；快照期额外只读连接（默认 ≤4） | 可配 `--snapshot-conns` |
| 源库 | 快照读压力 | 每连接 QPS 上限（默认 200），读按 PK 分块，块间可 sleep |
| 源库 | WAL / binlog 保留 | slot 落后阈值 5 GB / 30 min；binlog 保留需覆盖最长断连（预检提示） |
| 源库 | 参数占用 | PG 需 `max_wal_senders` +1、`max_replication_slots` +1；MySQL `server_id` 唯一 |
| 目标库 | 写放大 | 批量 500 行/事务（可调）；建议大表同步完再建二级索引 |
| 目标库 | 状态与死信 | 状态表每任务 <10 MB；死信表上限 + 轮转 |
| 磁盘 | slot 保留 | 阈值告警 + 可配自动失效（13+）或主动推进（补偿） |
| 网络 | 带宽 | 可选 zstd 压缩（CPU 换带宽，默认关） |

### 7A.3 测试自身的成本模型（GitHub Actions，公开仓库免费）

| 门禁 | 估算 | 墙钟 |
|---|---|---|
| PR | 3 链路 × 3 场景 × 90 s + L1 30 s | **12–15 min**（并行 6） |
| nightly | 40 链路 ×（8 场景 × 60 s + 60 s 起停）≈ 6 机器小时 | **20–30 min**（并行 20） |
| release | 169 链路 × 9 min ≈ 25 机器小时 | **1.5–2.5 h**（并行 20） |

磁盘约束：runner 只有 14 GB → **用 matrix 让每个 job 只拉自己那一个镜像**，不要把 9 个 PG + 4 个 MySQL 全缓存（约 6 GB，会挤爆 workspace）。
性能测试固定 runner 规格，并在结果里记录 CPU 型号做归一化，避免跨机漂移。

---

## 8. 可行性矩阵

| 能力 | 可行性 | 主要风险 |
|---|---|---|
| PG 10–18 全覆盖 | 高 | 老版本镜像需验证（`postgres:10` 等仍在 Docker Hub）；PG 10–12 无磁盘阀 |
| MySQL 5.7–9.x 全覆盖 | 高 | 8.4/9.x 语法与认证移除 → 双语法 + 三态认证 |
| 一致性快照接缝 | 中 | 云上锁被禁、版本差异 → 每版本真机 |
| 垃圾清理 | 高 | 误删活跃 slot → 四条判定 + dry-run |
| 选表/类型映射 | 高 | 类型语义差 → 映射字典 + schema fuzz |
| 五档频次 | 高 | 档 4 漏数、档 5 压垮源 |
| 分表双向 | 中高 | 表级互斥校验 |
| 同表双向 | 低（二期） | 时钟/回环/冲突 |
| DDL 跟随 | 中 | 解析覆盖 → 白名单四类（PG 18 原生 DDL 复制需实测） |
| 高吞吐 | 中高 | PG 单 slot 单线程解码 → 按表拆 slot |
| 转换 DSL | 高 | 类型失败 → 死信表 |
| MQ 端 EOS | 低 | 跨系统无法原子 |

---

## 9. 开源形态

- **MIT**（已定）；仓库 `spencerswagger/JinLiang`，二进制 `jinliang`（短写 `jl`）。文件头最少，采用门槛最低；后续若要商业化再考虑双许可或换牌。
- 仓库结构：`cmd/dbsync` + `internal/{core,source,sink,transform,state,preflight,catalog,gc,testkit}` + `connectors/{mysql,postgres}`（各带 `testmatrix.yaml`）+ `docs/`。
- `internal/testkit` 对外可用 —— 让别人写连接器时也能跑同一套契约测试，这是把覆盖变成生态能力的关键。
- CI：GitHub Actions 公开仓库（4 vCPU / 16 GB / 14 GB SSD，免费无限分钟）+ testcontainers-go；容器每 job 起一套复用、镜像预热。

---

## 10. 待确认

1. **命名**（见下，六个候选已确认在 `spencerswagger` 名下均未被占用）。
2. 跨版本降级按「逐特性补偿优先，实在不行只提示那一张表」处理。

### 命名候选（两字，气质对齐 雁书 / 灵枢 / 兰台）

| 名字 | slug | 出处与含义 | 为什么贴 |
|---|---|---|---|
| **津梁** | JinLiang | 渡口与桥，《说文》"津，水渡也" | 异构两端之间的桥；文气最接近兰台、灵枢 |
| **飞挽** | FeiWan | 「飞刍挽粟」，古代高速转运军粮 | 最贴"低延迟 + 高吞吐 + 持续运输" |
| 漕运 | CaoYun | 古代大规模水路转运 | 强调吞吐与持续，但略"重" |
| 通渠 | TongQu | 打通水道、引流 | 直白好记，工程师友好；但易与 Alibaba Canal（"渠"）联想 |
| 驿传 | YiChuan | 驿站接力传递 | 与"雁书"（传文件）语义偏近，易撞 |
| 转输 | ZhuanShu | 《史记》"转输"= 转运输送 | 稳、朴实，但记忆点弱 |

**推荐**：`津梁`（气质路线）或 `飞挽`（技术意象路线）。二进制名可短取 `jl` / `fw`，或直接用全拼。

---

## 11A. 视图能做什么、不能做什么（2026-11-05 修正）

之前写「视图只支持快照」是错的 —— **视图不能 CDC，但能增量**。

| 能力 | 视图 | 说明 |
|---|---|---|
| 一次性快照 | ✓ | 就是一条 `SELECT`，与表相同 |
| 定时增量（`interval`） | ✓ | 需要你指定一个水位列（如视图里的 `updated_at`）；靠「水位 + 重叠回看」取增量，**不需要变更流** |
| 实时增量（CDC） | ✗ | 视图没有自己的变更流水；这是物理事实，不是我们的限制 |
| 简单投影视图（单表、无聚合、无 join） | ✓ | 可选「跟随基表 CDC + 列映射」，二期实现 |

推论：`mode.frequency=interval` 对视图完全成立；选 `continuous` 时提示「视图没有变更流，建议改用定时增量」。

---

## 11B. 术语表（提示里出现这些词时，必须同时给白话解释）

| 内部叫法 | 提示里怎么说 | 白话解释 |
|---|---|---|
| 死信（dead letter） | 「没写进去的那几条」 | 解析或写入失败、暂时无法落库的变更记录；存在 `_jl_dead_letter`，修好根因后可重放 |
| `raw_row` | 「原始那一行」 | 死信里保存的完整行数据（JSON），供重放与排查；可配置只存哈希 |
| 位点（position） | 「同步进度」 | 已经读到源端变更流水的哪个位置 |
| 书签（slot） | 「同步书签」 | 向数据库登记的读取进度，占一点磁盘；删任务时自动清理 |
| 快照 | 「初始化读数」 | 先把存量搬一次，再从流水接着追 |
| 命名空间 `_jl_` | 「津梁自己的表」 | 所有状态表与对象以 `_jl_` 开头（jinliang），删任务时只清这一前缀 |

---

## 11C. 不做独立文档站（2026-11-05 拍板）

理由：用户没耐心读文档，宁可让 AI 助手读代码和日志帮他分析。
所以：
- 错误与预检提示必须**自解释**（四段式），不把 `DocURL` 当作必要路径；
- `docs/` 只服务开发者、贡献者与未来的维护者；
- `--json` 输出是给 CI 与 AI 助手消费的一等接口（结构化、可解释、字段稳定）。

---

## 附 A：实现级文档

- [SPEC.md](SPEC.md)：任务定义 `job.yaml`、命令面与退出码、预检清单（PG-01…/MY-01…）、错误模型（E1xxx/E2xxx/E3xxx/E4xxx）、指标名、一致性校验协议。
- [STATE-SCHEMA.md](STATE-SCHEMA.md)：`_jl_*` 表 DDL（PG / MySQL 两版）、位点与数据同事务的三种崩溃点分析、恢复时序、孤儿清理四判定。
- [TYPE-MAPPING.md](TYPE-MAPPING.md)：类型映射字典与探测兜底。
- [SNAPSHOT.md](SNAPSHOT.md)：初始化分块、一致性起点、断点与接缝。
- [STREAMING.md](STREAMING.md)：增量流的事务组装、批提交、位点推进、背压与限速。
- [CONFLICT.md](CONFLICT.md)：冲突策略、死信与人工处理。
- [VERIFY.md](VERIFY.md)：三级一致性校验与订正。
- [RUNTIME.md](RUNTIME.md)：生命周期状态机、暂停恢复、热更新、优雅退出。
- [ARCHITECTURE.md](ARCHITECTURE.md)：分层、数据流、背压取舍、扩展点。
- [SECURITY.md](SECURITY.md)：凭证与最小权限、TLS、日志脱敏、审计。
- [BUILD.md](BUILD.md)：构建产物、部署、升级与状态表迁移。
- [PLAN.md](PLAN.md)：`plan` 的输出与交互。
- [ROADMAP.md](ROADMAP.md)：里程碑与验收判据。
- [IDENTITY.md](IDENTITY.md)：job_key / instance_id / 对象命名的三层身份，多实例共存与孤儿所有权。
- [ERRORS.md](ERRORS.md)：错误码全集与四段式文案纪律。
- [CONNECTOR-SDK.md](CONNECTOR-SDK.md)：连接器开发指引与建表规则。
- [DECISIONS.md](DECISIONS.md)：14 条决策记录（ADR）。
- `schema/job.schema.json`：任务定义的 JSON Schema，CI 用 `scripts/validate_schema.py` 校验示例。
- [OPEN-QUESTIONS.md](OPEN-QUESTIONS.md)：未决清单。
- 代码契约：[`internal/source/contract.go`](../internal/source/contract.go)、[`internal/sink/contract.go`](../internal/sink/contract.go) —— 已编译通过，CI 校验。

## 附：真机证据（仅校验设计假设，不作项目依赖）

| 项 | 值 |
|---|---|
| PG 17.11 | `wal_level=replica`、`max_slot_wal_keep_size=-1`、`max_replication_slots=10`、`track_commit_timestamp=off` |
| `CREATE PUBLICATION` @replica | 成功（仅 WARNING）→ 可前置 |
| `pg_create_logical_replication_slot` @replica | 失败：requires `wal_level >= logical` → 硬门槛在最后一步 |
| `pg_export_snapshot()` | 可用 |
| MySQL 8.0.28 | `log_bin=1`、`ROW`、`FULL`、`gtid_mode=OFF`、`server_id=1`、binlog 保留 30 天 |