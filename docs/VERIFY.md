# 一致性校验 `verify`

## 1. 三级校验（判据顺序不能反）

| 级 | 做法 | 能发现 | 不能发现 |
|---|---|---|---|
| L1 聚合（判据） | 全表 `count(*)`、`sum/min/max`（数值列）、`sum(length)`、NULL 数；源/目标各算一遍，任一不一致即失败 | 少行、多行、数值变化、精度截断、NULL 数变化 | **两行互换**（count/sum/min/max 可能全不变） |
| L2 全表顺序扫描 + 块 checksum（判据） | 按主键顺序逐行读两端，应用侧规范化整行逐行比对；同时算**顺序敏感**块 checksum 用于定位到块 | 值互换、重复行、文本等长改写、行级任何差异 | --（全表，不依赖抽样） |
| L3 行级下钻（定位） | 对不一致块逐列 diff，输出主键、列名、源/目标值 | 精确到字段与行 | -- |
| 抽样（补充，不是判据） | L1/L2 都一致后再抽 N 行做二次确认（并发写入时有用） | 扫描后的新变更 | 不能单独判通过；抽样通过不等于一致 |

**为什么必须全表而不是抽样**：抽样是概率证据，漏掉一行就会把「不一致」报成「通过」——
校验器说谎比不校验更危险。两行互换由**逐行比对**发现（块 checksum 负责定位到块，
不是唯一判据）；L1 的 count/sum/min/max 对互换可能完全不变。

**跨端指纹不可直接比**：MySQL 指纹是客户端 sha256（`mysql-canon-v1`：列名排序 + 主键定序），
PG 是服务端 `sum(hashtextextended)`（`pg-hashtextextended-v1`），两个数永远不可能相等。
`verify` 先看两侧 `FingerprintScope`：不同就**不做指纹比对**，直接走逐行全表比对，并在
`fingerprint_scope_mismatch` 与 `Report.Fingerprint` 里如实说明（不静默、不假红）。

**数值口径**：`numeric`/`decimal` 从 `database/sql` 取出是字符串（`"1.0000"`、`"1.00"`），
行级与聚合都先 `big.Rat` 归一（`RatString()`）再比，**同一逻辑值不同 scale 必须判相等**，
否则 PG→MySQL 会天天假红。

**为什么 L1 不用跨库哈希函数**：PG 只有 `md5`，MySQL 有 `crc32` 与 `md5`，两者的
"行签名"数值不同源、不可直接比较。所以聚合用**可加统计量**（跨库语义一致），
真正的行级校验放在应用侧（L2/L3），服务端只负责初筛。这是有意的取舍，不是省事。

## 2. 规范化规则（两边必须完全一致）

- NULL -> 哨兵字节，与空串、反斜杠 N 区分。
- 时间 -> UTC 微秒整数；`timestamptz` 与 `datetime` 差异在映射阶段已确定口径。
- 浮点 -> 定点十进制字符串（避免二进制尾差），并记录精度。
- 二进制 -> 小写 hex。
- JSON -> 键排序后紧凑序列化。
- 字符串 -> 保持原字符集（GBK 源会先转 UTF-8），emoji 与组合字符不规范化（按码点）。

## 3. 扫描与并发

默认**全表按主键顺序扫描**（无主键表只做聚合，并明确 warn「无法逐行定位」；声明 `unique_key` 也**不**升级成行级比对——那是挂账 D2，见 `DESIGN-NO-PRIMARY-KEY.md`）；
块大小 1000 行，块级 checksum 用于定位。`--tables` 只校验指定表。
抽样只作为 L1/L2 之后的补充确认（`--sample` 控制行数，默认 100），不参与判据。

**成本与日常例检**：全表扫描 = 两端各一条 `SELECT 全列 … ORDER BY pk` + 逐行规范化解码，
10M 行是分钟级。**跨端（PG→MySQL / MySQL→PG）默认就是最贵的跑法**：两端全表扫 + 排序 +
逐行解码，不能靠「只比指纹」省掉（指纹作用域不同，见上）。日常例检用
`jinliang verify --level data --aggregate-only`：L1 下推成两端各一条
`count/sum/min/max` 聚合查询（秒级）；全表行级比对留给发布前/周检。
结论行按**实际执行的级**逐级枚举，例如「结构级 ✓ / 数据级 ✓（行级全表比对 3 表；
1 表仅聚合（无主键）） / 一致性 ✓」；只跑聚合时数据级会标「仅聚合，未做行级全表比对」，
不会冒充三级；`--json` 的 `mode` 由执行记录推导（`aggregate_only` / `data_partial` /
`all_partial` 等），不由 `--aggregate-only` 这个 flag 推导。

## 4. 订正

- 输出：不一致块清单 + 每块前 10 行差异（字段级）。
- `--fix` 生成并执行 upsert / delete，**先打印条数与样例再等确认**。
- 每次订正写 `_jl_conflict`（或 `_jl_repair`），留痕可追溯。

## 5. 只读保证

`verify` 默认对源与目标都只读：只用 `SELECT`，不开事务写。
`--fix` 是唯一例外，且必须先有 `plan` 输出的确认。

## 6. 校验器自测（四条反例是验收标准）

测试里故意破坏目标库，`verify` 必须**判失败并指到具体行/列/块**：

1. 改 1 行值（`TestAggregateCatchesOneValueChange`）
2. 删 1 行（`TestAggregateCatchesDeletedRow`）
3. 改精度 `2.3450 → 2.3400`（`TestAggregateCatchesPrecisionChange`）
4. 两行互换（`TestAggregateCatchesSwappedRows`，证明只看 count/sum 不够）

真库版本见 `TestPGVerifyCatchesSwapAndPrecision`；改之前这四条里有三条是红的
（verify 仍报通过，只有删行被 count 抓到），修完后四条全绿——先证伪再修，
是这个模块的验收方式。
