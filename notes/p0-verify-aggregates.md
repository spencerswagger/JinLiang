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
