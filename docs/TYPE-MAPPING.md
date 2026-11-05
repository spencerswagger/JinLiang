# 类型映射

## 1. 四条原则

1. **绝不静默降级。** 凡是 `lossy` 以上，`plan` 必须列出来，用户确认才继续。
2. **字典优先，探测兜底。** 字典没覆盖的类型 → 先在目标库建影子列试写一行；成功则采纳，失败则**按文本落地并提示**（不拒绝这一列）。
3. **列级可覆盖。** `job.yaml` 的 `content.tables.<t>.columns.map` 可以强制指定目标类型，人的判断优先于字典。
4. **永不拒绝。** 无法映射的列以文本/JSON 落地并在 `plan` 里标红；能不能接受由用户决定。

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

## 5. JSON、PARTIAL_JSON 与旧式 DECIMAL

### 5.1 MySQL JSON 怎么交付（值级别规则）

列类型映射只是第一步（§3 表里 MySQL `json` → PG `jsonb`，代价 lossy）。真正会咬人的是**值**：binlog 行镜像里的 JSON 是 `json_binary` 二进制，不是文本。

规则落在 [`internal/source/contract.go`](../internal/source/contract.go) 的 `KindJSON`：

- 源端把 `json_binary` 解成 **UTF-8 JSON 文本**，用 `KindJSON` + `Value.S` 交付。解码器归连接器（`internal/mysqlsource`），不属于类型字典。
- **不许用 `KindBytes` 交付 JSON**：那是 BYTEA/BLOB 的通道，sink 会原样写进去，目标上得到一列「像 JSON 的乱码」，而全链路一声不响。

三件事定死：

| 情况 | 处理 | 为什么 |
|---|---|---|
| ① 解不出来（非法/损坏） | **报错终止本批**，不降级为文本 | 源库的 json 列本身保证合法，解不出只可能是数据损坏或格式理解错。把碎片当文本写进目标，等于用「看起来成功」掩盖数据损坏——比失败更难查 |
| ② 单值超过目标上限 | **明确失败**并给下一步（该列改 `text`、或排除该列），绝不截断、不拆分 | 截断后的 JSON 语法上仍「合法」，静默丢数据；PG 受 varlena 1GB 上限约束，MySQL JSON 受 `max_allowed_packet` 约束 |
| ③ 方言语义差异 | 写进映射说明，`plan` 里可见（见下） | 用户要能自己判断「这列能不能用 jsonb」 |

方言差异（决定要不要为此把该列改成 `text`）：

- **键序**：两边都不保证顺序（MySQL 与 `jsonb` 都会重排），别依赖。
- **重复键**：两边都是「后者覆盖」，但覆盖顺序在极少数情况下不同——有重复键的数据不要跨方言期望逐字节一致。
- **数字精度**：MySQL 8.0 的 JSON 数字按 int64/uint64/double 存，**大整数与高精度小数在源头就可能已经失真**；PG `jsonb` 用 numeric 保精度，「PG 看起来更准」不代表数据更真。
- **`\u0000`**：MySQL 允许 JSON 字符串里含 `\u0000`，**PG 的 `jsonb` 直接拒绝**（0x00 不能进 text）。遇到就明确报错，建议该列落 `text` 或排除。

### 5.2 PARTIAL_JSON：必须关掉，不能「试试看」

MySQL 8.0 开了 `binlog_row_value_options=PARTIAL_JSON` 之后，UPDATE 的 binlog 只带 JSON 列的**差异**，我们读不出整行。

自查：

```sql
SHOW GLOBAL VARIABLES LIKE 'binlog_row_value_options';   -- 期望返回空串
```

关闭（dba、全局）：

```sql
SET GLOBAL binlog_row_value_options = '';
```

- 预检项 `MY-04` 会把它标成 `autofixable` 并给出上面这条 SQL。不关就开跑，会在解析到第一条 JSON 更新时报错退出——**不是猜着拼**（拼出来的行看起来完整，但 JSON 列是错的）。
- 回滚：`SET GLOBAL binlog_row_value_options = 'PARTIAL_JSON';`（只有在你有别的消费者依赖它时才值得回滚）。

### 5.3 旧式 DECIMAL：不支持，也没有开关

MySQL 5.0 及更早用旧编码存 DECIMAL；5.1 起统一为 `NEWDECIMAL`（表类型码 246）。我们只实现 `NEWDECIMAL`。

- 症状：报「这一列是旧式 DECIMAL 编码，本连接器不支持（源库版本过旧）」。
- 自查：`SELECT VERSION();`——出现这条基本就是源库太老，或你中继了一份很老的 binlog。
- **怎么「关」：没有开关。** 三条路任选：① 升级源库；② 在 5.1 及以后执行 `ALTER TABLE t MODIFY c DECIMAL(p,s)` 重建该列（会把旧编码改写成 `NEWDECIMAL`）；③ 在 `content.tables.<t>.columns.exclude` 里排除该列。
- 为什么明确失败而不是按文本兜底：旧式编码是变长、依赖精度的打包格式，猜错就是**静默写错数字**——在金额列上这是最坏的一种错。

## 6. 探测兜底流程

```
字典命中 → 按 Mapping 落地
字典未命中（unsupported）
  → 目标库建影子列（名字带 _jl_probe_ 前缀）
  → 写一行代表性值（NULL / 最小 / 最大 / 边界字符串）
  → 成功：采纳该类型，记一条 action（可撤销）
  → 失败：按 text 落地 + 提示「该列会丢失类型语义」，继续同步；不阻断任务
```
