# 全量初始化：分块、断点与一致性

初始化做三件事：**给一个一致性视图**、**把存量读出来**、**把读取起点交给增量**。

## 1. 分块策略（按主键形态选）

| 主键形态 | 策略 |
|---|---|
| 单列整型 | 取 `min`/`max`（或 `reltuples` 估算区间）均分为 N 块，`WHERE pk >= a AND pk < b` |
| 复合主键 | 以首列切分区间，块内用 keyset 游标推进（`ORDER BY k1,k2` + 复合 `>` 条件） |
| 单列文本 / UUID | 不切区间，用 **keyset 游标**：`WHERE pk > $last ORDER BY pk LIMIT n` |
| 无主键（PG） | 按 `ctid` 区间切分（`WHERE ctid >= '(a,0)' AND ctid < '(b,0)'`） |
| 无主键（MySQL） | **只能单连接顺序扫**（InnoDB 隐藏 rowid 不可用）→ 只有表级断点，失败整表重来；预检里提示，**不因表大而拒绝** |

分块大小默认 10 万行；块太大内存与锁压力大，太小则往返过多。

## 2. 并发与限速

- 每块一个连接，并发数 = `snapshot.conns`（默认 4，上限 16）。
- 每连接 QPS 上限 `max_read_qps`（默认 200），块间可 `sleep`，避免把源库打满。
- 快照期内存 ≈ `conns × chunk_rows × 行宽`；`plan` 会算出来给用户看。

## 3. 一致性起点（各库不同）

**PostgreSQL（10+）**
1. 通过复制协议 `CREATE_REPLICATION_SLOT … (EXPORT_SNAPSHOT)` 建同步书签并导出一致性视图 id。
2. 每个分块连接 `BEGIN ISOLATION LEVEL REPEATABLE READ; SET TRANSACTION SNAPSHOT '<id>'` —— 所有块看到同一时刻的数据。
3. 快照结束（所有块读完）→ 取消导出、开始消费变更流，从书签起点追平。

**MySQL**
1. 取位点：`FLUSH TABLES WITH READ LOCK`（毫秒级）→ `SHOW BINARY LOG STATUS`（8.4+）/ `SHOW MASTER STATUS`（≤8.0）→ 解锁。
2. `START TRANSACTION WITH CONSISTENT SNAPSHOT` 建立一致性读。
3. 云上禁锁时降级：先起一致性快照，再取位点，通过 `mode.lookback` 回看窗口 + upsert 幂等去重（如实标注极小重复可能）。

## 4. 断点

- `_jl_chunk(job_key, table_name, chunk_no, done, rows)` 每完成一块更新。
- 重启：读未完成块继续；快照 id 已失效（源库重启 / 超过 `idle_replication_slot_timeout`）→ 整段重来，并在动作清单里说明原因。
- MySQL 无主键表：只有表级断点，重来成本要在 `plan` 里标出。

## 5. 与增量的接缝

```
[快照开始] ── 块 1 ── 块 2 ── … ── 块 N ── [快照结束]
                    ↘ 变更流一直在缓冲（同一书签） ↙
               切换条件：快照读完 && 增量已追到快照起点之后
```
切换期间允许短暂积压，但**不允许丢**：书签在快照期间不推进，避免变更流水被清理。

## 6. 空表 / 单行 / 极稀疏主键

- 空表：直接跳过，记 0 行。
- 单行：不切块。
- 极稀疏（如 `id` 只有 1 和 10^12）：区间切分会退化成大量空块 → 先用 `percentile_disc`/采样取 N 个边界值再切，避免 99% 空扫描。
