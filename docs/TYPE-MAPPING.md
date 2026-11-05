# 类型映射

## 1. 四条原则

1. **绝不静默降级。** 凡是 `lossy` 以上，`plan` 必须列出来，用户确认才继续。
2. **字典优先，探测兜底。** 字典没覆盖的类型（`unsupported`）→ 在目标库建一个影子列试写一行，成功则采纳，失败则拒绝该列并给替代。
3. **列级可覆盖。** `job.yaml` 的 `content.tables.<t>.columns.map` 可以强制指定目标类型，人的判断优先于字典。
4. **拒绝的是列，不是任务。** 单列无法映射时，其余列照常同步，该列进告警清单。

实现见 [`internal/catalog/typemap.go`](../internal/catalog/typemap.go)（已带表驱动测试）。

## 2. PostgreSQL → MySQL

> MySQL 侧默认 `utf8mb4`，单行上限 65535 字节 —— 宽表会被这一条卡住。

| 源类型 | 目标类型 | 代价 | 说明 |
|---|---|---|---|
| boolean / bool | tinyint(1) | 无损 | 读回时按布尔解释 |
| smallint / int2 | smallint | 无损 | |
| integer / int / int4 | int | 无损 | |
| bigint / int8 | bigint | 无损 | |
| smallserial / serial / bigserial | smallint / int / bigint | 无损 | 目标端按自增处理 |
| real / float4 | float | **lossy** | MySQL FLOAT 单精度，值域更窄 |
| double precision / float8 | double | 无损 | |
| numeric(p,s) / decimal(p,s) | decimal(p,s) | 无损 | 精度原样保留 |
| numeric（未给精度） | decimal(65,30) | **needs_confirm** | 按最大值落库，存储开销大 |
| money | decimal(19,2) | **needs_confirm** | 货币符号与小数位需确认 |
| varchar(n) / character varying(n) | varchar(n) | 无损 | n > 16383 时改 `text`（见下） |
| varchar(n>16383) | text | **needs_confirm** | 超 utf8mb4 行宽 |
| char(n) / bpchar | char(n) | 无损 | |
| text | text | 无损 | |
| bytea | longblob | 无损 | |
| date | date | 无损 | |
| timestamp | datetime(6) | **lossy** | 丢时区，按会话时区存 |
| timestamptz / timestamp with time zone | datetime(6) | **needs_confirm** | 建议统一按 UTC 存，读取方要知道 |
| time | time(6) | 无损 | |
| timetz | time(6) | **needs_confirm** | 时区偏移被丢弃 |
| interval | varchar(64) | **needs_confirm** | 无等价类型 |
| uuid | char(36) | 无损 | |
| inet / cidr | varchar(45) | 无损 | |
| macaddr | varchar(17) | 无损 | |
| json | json | 无损 | |
| jsonb | json | **lossy** | 键序、重复键处理不同 |
| xml / tsvector | text | **needs_confirm** | 按文本落地 |
| 任意 `xxx[]` 数组 | json | **needs_confirm** | 元素类型需人工确认 |
| geometry 等扩展类型 | 视扩展 | **unsupported** | 需人工指定 |

## 3. MySQL → PostgreSQL

| 源类型 | 目标类型 | 代价 | 说明 |
|---|---|---|---|
| tinyint(1) | boolean | **needs_confirm** | 生态里当布尔用，但 2..127 会溢出 |
| tinyint（其他） | smallint | 无损 | 有符号 -128..127 |
| tinyint unsigned | smallint | 无损 | |
| smallint / smallint unsigned | smallint / integer | 无损 | |
| mediumint / unsigned | integer | 无损 | |
| int / integer | integer | 无损 | |
| int unsigned | bigint | 无损 | |
| bigint | bigint | 无损 | |
| bigint unsigned | numeric(20,0) | **needs_confirm** | 超 bigint 上限 |
| decimal(p,s) | numeric(p,s) | 无损 | |
| float | real | **lossy** | 单精度语义接近，但 PG real 值域不同 |
| double | double precision | 无损 | |
| bit(n) | bit(n) | 无损 | |
| char(n) / varchar(n) | char(n) / varchar(n) | 无损 | |
| tinytext / text / mediumtext / longtext | text | 无损 | |
| binary / varbinary / *blob | bytea | 无损 | |
| date | date | 无损 | |
| datetime | timestamp | **lossy** | 无时区，按会话时区解释 |
| timestamp | timestamptz | **needs_confirm** | MySQL timestamp 自带会话时区语义 |
| time | time | 无损 | |
| year | integer | **needs_confirm** | 年份转整数 |
| json | jsonb | **lossy** | 键序、重复键处理不同 |
| enum | text + CHECK | **needs_confirm** | 取值清单需人工确认 |
| set | text | **needs_confirm** | 如需数组语义要人工指定 |
| geometry | geometry | **needs_confirm** | 目标需 PostGIS |

## 4. 三个必踩的坑

1. **utf8mb4 行宽**：宽表（多列 varchar）在 MySQL 会直接建表失败。处理：预检按列宽估算行字节数，超 65535 就建议把长列改 `text`，并在 `plan` 里标出。
2. **字符集与排序**：源端 GBK/GB18030 的列要显式转 UTF-8，否则中文变问号；MySQL 的 `utf8mb3` 别当 `utf8mb4` 用。
3. **浮点与金额**：金额永远走 `decimal`，任何"看起来能过"的 float 都是隐患；`numeric(65,30)` 只在前端确实没给精度时兜底。

## 5. 探测兜底流程

```
字典命中 → 按 Mapping 落地
字典未命中（unsupported）
  → 目标库建影子列（名字带 _ds_probe_ 前缀）
  → 写一行代表性值（NULL / 最小 / 最大 / 边界字符串）
  → 成功：采纳该类型，记一条 action（可撤销）
  → 失败：拒绝该列，其余列继续，命令退出码 3
```
