# P0：verify 不再漏报（聚合 + 全表顺序扫描）

## 先证伪：改之前四条反例里三条是绿的

`SampleRows=1`（抽样只覆盖第 1 行）下，目标端改第 3 行 / 改精度 / 两行互换，旧实现都返回：

```
Level:data Status:ok OK:true Tables:1 Findings:[] Stats:map[checksum:… matched:1 rows_source:3 rows_target:3 sampled:1]
```

对应测试：`TestAggregateCatchesOneValueChange`、`TestAggregateCatchesPrecisionChange`、
`TestAggregateCatchesSwappedRows`（三条 FAIL）；`TestAggregateCatchesDeletedRow` 因
`count` 被抓而 PASS。修完后四条全绿。

## 修法

1. **L1 聚合是判据**：全表 `count`、数值列 `sum/min/max`（`big.Rat` 保十进制精度）、
   文本 `sum(length)`、NULL 数；源/目标各算一遍，任一不一致 → `aggregate_mismatch`
   （带列名与两端值）。
2. **L2 全表顺序扫描是判据**：按主键顺序同时读两端，逐行规范化比对；每 1000 行算
   **顺序敏感**块 checksum。两行互换时 count/sum/min/max 都不变，但块 checksum 必变 →
   `block_checksum_mismatch`（第 N 块 + 起始主键）。
3. **L3 行级下钻**：同块内逐列 diff → `row_mismatch`（主键 + 列 + 源/目标值）；
   缺行/多行 → `missing_row`/`extra_row`（都带主键）。
4. **抽样降为补充**：只有 L1/L2 都一致后才跑（`runSampling`），绝不单独判通过。
5. 无主键表：只做聚合，明确 `no_pk_aggregate` warn（无法逐行定位）。

## 测试

单测（`aggregate_negative_test.go`）：

- `TestAggregateCatchesOneValueChange`：断言失败 + `orders.amount` + 主键 3
- `TestAggregateCatchesDeletedRow`：断言失败 + `missing_row` + 主键 3
- `TestAggregateCatchesPrecisionChange`：`2.3450 → 2.3400`，断言 `metrics.v` + 主键 2 +
  `aggregate_mismatch`
- `TestAggregateCatchesSwappedRows`：两行互换，断言 `row_mismatch` + `block_checksum_mismatch`
  （第 0 块）

真库（`integration_test.go`）：`TestPGVerifyCatchesSwapAndPrecision`（PG 真库互换 +
numeric(12,4) 精度截断）。

## doctor 幽灵判据确认

已经是 heartbeat：`internal/doctor/doctor.go` 用
`COALESCE(heartbeat_at, started_at)` + `StaleAfter(30s)=90s`，适配器探测
`runs.heartbeat_at` 列；旧库没有该列时只 warn 不判红。**这条 P1 已在 main，不需要再改。**

## 影响

- `verify` 现在默认全表扫描（按主键有序），成本与表大小线性相关；无主键表只聚合。
- `docs/VERIFY.md` 已按实现更新；抽样明确标为「补充、不是判据」。
- nightly 集成按资源隔离分组（engine/pgsink 各自在 public 建 `_jl_*`，不能并发）。

## Owl #47 必改：数值口径统一（避免 PG→MySQL 天天假红）

- 行级 `canonicalRow`/`firstDiff` 现在接收 numeric 列集合：`numeric`/`decimal` 先
  `toRat` 再统一成 `RatString()`，与 L1 的 `big.Rat` 同口径；`"1.0000"` 与 `"1.00"`、
  `"0.10"` 与 `"0.1"` 判相等。
- 新用例 `TestNumericSameValueDifferentScaleIsEqual`（源 PG `1.0000/0.10`，目标 MySQL
  `1.00/0.1`，必须通过）；`TestAggregateCatchesPrecisionChange` 改为真正不同值
  `2.3450 → 2.3400`，断言 `metrics.v` + 主键 2 + `aggregate_mismatch`。
- 文档修正：互换是**逐行比对**发现，块 checksum 只负责定位（不是唯一判据）。

## 日常例检的便宜路：`--aggregate-only`

- `runtime.DB.AggregateSQL`：两端各一条 `count/sum/min/max/text_len/nulls` 查询
  （PG 用 `::text`/`CAST AS text`，MySQL 用 `CAST AS CHAR`），秒级。
- `jinliang verify --level data --aggregate-only` 只跑 L1；不一致时提示改跑完整 data 级定位。
- 测试：`TestPGVerifyAggregateOnly`、`TestMySQLVerifyAggregateOnly`（真库）。
- 完整全表扫描仍是默认；10M 行分钟级，留给发布前/周检，文档已写清量级。
