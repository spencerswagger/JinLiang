# `plan` 的输出与交互

`plan` 是这个产品最重要的命令：**它让用户在写库之前，看到全部后果**。

## 1. 人类可读模板

```
津梁 plan · orders-to-report
源  postgres 17.11 (public: orders*, customers)  目标  mysql 8.0.43 (report)

预检 3 项需要注意
  [auto]  PG-02 max_replication_slots 余量 0 → 我们改为 12（ALTER SYSTEM + reload）
          undo: ALTER SYSTEM SET max_replication_slots = 10;
  [dba ]  PG-01 wal_level = replica，需要 logical（要重启实例）
          SQL : ALTER SYSTEM SET wal_level = 'logical';
          影响：连接中断 10–30 秒；WAL 增长约 10–20%
  [info]  PG-07 本版本无 slot 磁盘阀 → 改用「落后 5 GB 暂停并告警」补偿

能力与补偿
  snapshot_export  ✓   streaming_tx  ✓   slot_advance  ✓
  slot_disk_limit  ✗ → auto: 落后阈值告警

范围  12 张表，估算 8.4 M 行 / 2.1 GB（其中 orders 占 71%）
类型映射  3 处需要确认
  orders.amount_cents  int        → decimal(12,2)  (无损，按 scale 0.01 转换)
  orders.updated_at    timestamptz→ datetime(6)    (needs_confirm: 统一 UTC)
  customers.meta       jsonb      → json           (lossy: 键序)

将执行的动作（写库，需确认）
  1. 建只读账号 jinliang_ro 并授权（可撤销）
  2. 建 publication ds_orders_to_report（可撤销）
  3. 目标端建 12 张表（可撤销：DROP 前会先写备份清单）
  4. 建 _ds_* 6 张状态表（可撤销）

预计  初始化 6–9 分钟（QPS 上限 200/连接 × 4） → 之后持续增量，p99 延迟约 40 ms
下一步  jinliang run job.yaml            （确认执行）
        加 --yes 跳过确认              用 --json 给 CI 消费
```

## 2. 机器可读 `--json`

```json
{
  "job": "orders-to-report",
  "resolved": { "mode.frequency": "continuous+initial", "batch_rows": 500, "...": "全部生效值含默认" },
  "source": { "kind": "postgres", "version": "17.11" },
  "target": { "kind": "mysql", "version": "8.0.43" },
  "capabilities": { "snapshot_export": {"supported": true}, "slot_disk_limit": {"supported": false, "compensation": {"level": "auto", "note": "落后 5 GB 暂停并告警"}} },
  "checks": [ { "id": "PG-01", "status": "blocked", "detail": "wal_level = replica", "sql": "ALTER SYSTEM SET wal_level = 'logical';", "who": "dba" } ],
  "estimates": { "tables": 12, "rows": 8400000, "bytes": 2100000000, "snapshot_seconds": 480 },
  "actions": [ { "id": "act-1", "what": "建只读账号", "undo": "DROP ROLE jinliang_ro;" } ],
  "type_map": [ { "table": "orders", "column": "updated_at", "from": "timestamptz", "to": "datetime(6)", "loss": "needs_confirm" } ],
  "warnings": ["customers.meta jsonb → json 会重排键序"],
  "can_start": false,
  "exit_code": 3
}
```

## 3. 交互规则

| 场景 | 行为 |
|---|---|
| 全是 `ok` + `auto` | 打印摘要，`run` 时问一次「执行以上 4 个动作？」 |
| 有 `costly` / 类型 `needs_confirm` | 逐条列出差异，必须显式 `--accept <id>` 或交互确认 |
| 有 `blocked` | 退出码 3，打印定制教程；**不提供"忽略"开关** |
| CI 环境 | `--json` 输出 + 退出码判定，绝不弹交互 |
| 目标表已存在 | 默认直接判 blocked（`on_exists: error`），除非显式改配置 |

## 4. 与 `run` 的关系

`run` 先跑一遍 `plan`（同样的探测与预检），**探测结果缓存 5 分钟**避免重复压库；
`plan` 与 `run` 的差异只有一步：`run` 在确认后执行 `actions`，然后进入快照/增量。
`--json` 的 `estimates` 是估算不是承诺，`run` 会在 30 秒后给出真实速率并修正 ETA。
