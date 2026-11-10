# 夜班 2026-11-11 ① MySQL 源 → 引擎的 DDL/类型信号 + 三组合真数字

接 22:05 那轮（23:41:51 容器被外部重建切断，不是 OOM、不是代码崩）。三段接续：先修信号与类型，再按裁决 A 改判据。
分支 `feat/mysql-ddl-type-signal`，已 rebase 到裁决 A 之后的 main（`b2642a9`，含 #93）。

---

## ① 断点

- **已完成（中断前）**：PR #89（MySQL 源接引擎三处接线 + 增量 kill -9 续跑），CI 8+4 个 job 全绿，等 Owl II 评审。
- **本轮补的是**：真 MySQL 源「DDL 改类型 → 只把那张表记成暂停 → 对齐后恢复追平」的链路，
  以及 `varchar` 长度单位这个**真 bug**（见 §5.1）。
- **当时的缺口、现在已解**：开工时「只暂停该表」还等契约；收尾时 Ark 的裁决 A 已下
  （`#90` 契约冻结、`#93` 落地：只停该表 + 位点照常前进 + 恢复时整表重搬）。
  本 PR 不碰写路径，只提供它需要的输入（见 §6）；「其它表继续跑」**已经在主干生效**，
  用例里旁证表断言的是「5 行」（旧契约下是 4 行）。

---

## ② 改动文件

| 文件 | 改了什么 | 为什么 |
|---|---|---|
| `internal/mysqlsource/ddl_table.go`（新） | `ddlTargetTable()`：从 DDL 原文认出作用表 | Query 事件只有「默认库 + 语句」；不给表名，引擎的「在不在范围内」判定恒落空 → **源端 DDL 被静默丢掉**，连「本版不搬 DDL」那声提示都发不出 |
| `internal/mysqlsource/assembler.go` | `statementEvent` 填 `Table.Schema/Table` | 同上 |
| `internal/mysqlsource/relation.go` | 类型渲染按**字节→字符**换算（VARCHAR/CHAR，按列字符集 mbmaxlen）；TEXT 与 BLOB 按字符集分开；VARBINARY 不再报成 varchar；`enrichRelation` 改成**宽松同型**判定 | binlog 给的是**字节**上限，utf8mb4 的 `VARCHAR(64)` 记 256；TEXT/BLOB 共用类型字节（§5.1、§5.3） |
| `internal/mysqlsource/decode.go` | VARBINARY 恒按字节交（`KindBytes`）；可打印判定拒绝控制字符 | `\x00\x01` 被当成 Go 字符串 → 写 bytea 直接 `cannot find encode plan`（§5.4） |
| `internal/pgsink/pgsink.go`（typeAliases） | 加 `char → character` | MySQL 的 `char(10)` 与 PG information_schema 的 `character(10)` 被判成「类型不一致」，一个 CHAR 列就能把整轮停掉（§5.5） |
| `internal/pgsink/pgsink.go` | `tableInfo.srcTypes`（源端原文类型）+ 改类型提示改成「源端 旧→新（目标端映射 旧→新）」 | 之前两句都是**目标端映射后**的类型，跨引擎时变成 `text → text`，用户看不懂改了什么 |
| `internal/engine/run_source.go` | DDL 归属判定加大小写兜底（只影响「要不要发提示」，数据行仍精确匹配） | 表名来自 SQL 原文，大小写与 information_schema 未必逐字一致 |
| `internal/engine/integration_mysql_type_pause_test.go`（新） | 真 MySQL 源改类型的端到端用例（12s） | ②的验收，见 §3 |
| `internal/engine/integration_mysql_incr_kill_test.go` | 位点断言：只认 `mysql:v1:` 行、必须唯一、三个位点都要能解析出 binlog 偏移 | 原来 `SELECT ... FROM _jl_meta LIMIT 1` —— 库里多一行就**读错行、断言空转**（§5.2） |
| `internal/mysqlsource/ddl_table_test.go`（新） | 19 格形态用例 | 认错表比认不出更坏 |

---

## ③ 三种组合（今晚本地实测，MySQL 8.0.28 / PG 17.11，都是 20000 行）

| 组合 | 行数 | md5（两端一致） | 耗时 | 增量追平 |
|---|---|---|---|---|
| MySQL→MySQL（3306 源 / 3307 目标） | 20000 | `20000:1754a8061009120825b22d79de07c9e2` | 全量+追平 8.90s（整轮 15s） | 4.10s |
| MySQL→PG | 20000 | `20000:025e8072a9b12ed98a6dbf9b58712454` | 全量 318ms（62.8k 行/秒），整轮 12s | 0.24s |
| PG→MySQL | 20000 | `20000:d336b80c0d35ef06aa21fc7f51cfff68` | 整轮 27s | 0.28s |

**kill -9 重搬 0 行**

| 场景 | 行数 | md5 | 重搬 | 重启耗时 |
|---|---|---|---|---|
| MySQL→MySQL 增量中途 kill -9 | 19996 | `19996:264184727d82ddbb0ab4eeb6566a1d24` | **0 行** | 6.51s |
| MySQL→PG 增量中途 kill -9（Go 集成用例） | 21850 | `21850:8873f5e5054c409dab2c543544031737` | **0 行** | 6.19s |

MySQL→PG 那条的位点（本轮改成真断言之后）：`675108 → 719304`（中断）`→ 753216`（追平），只前进。

> 本轮新增用例 `TestMySQLSourceTypeChangePausesOnlyThatTableAndRecovers`（真 MySQL 源）：
> 先全量、再流式改列类型、看暂停记录与位点，对齐后**整表重搬补齐**（裁决 A 的恢复路径，
> 不是「从原位点重放」）。耗时随 CI 机器在 12–20s 之间（A 之后多了一段重搬，本地约 19–24s）。

> 环境说明：容器重建后宿主上的 PG/MySQL 都没了。工作区里加了一套本地起库脚本（`.work/env_up.sh`，
> 不入库）：PG 17 用嵌套 bwrap 以 uid 102 起（沙盒里 `su`/`setuid` 后 `getpwuid` 解析不出来），
> 两个 MySQL 8.0.28 实例（3306/3307，ROW + FULL + GTID）用 `/.toolchain/mysql` 起。

---

## ④ 版本 × 行为矩阵（5.7 / 8.0 / 8.4 / 9.x）

本地没有 docker，这四条腿在 Actions 上真跑（PR #89 的 `mysql-matrix` 四腿全绿：132 PASS / 0 FAIL 每腿）。

| 差异点 | 5.7 | 8.0 | 8.4 | 9.x | 我们怎么处理 |
|---|---|---|---|---|---|
| GTID 变量 | `gtid_executed` 可用 | 同 | 同 | 同 | 位点同时带 `file:pos` 与 gtid 集合；`coord=mysql_gtid` 只是坐标变体，不进 `Kind()` |
| `SHOW MASTER STATUS` | 有 | 有 | **已移除**（改 `SHOW BINARY LOG STATUS`） | 同 8.4 | 启动探测两个都试，谁通行用谁（**能力探测，不按版本号分支**） |
| 原子 DDL（8.0.0+） | 无 | 有 | 有 | 有 | 我们只在**提交后**读 binlog，原子 DDL 只是少一类「DDL 半途崩」的中间态；事件形态不变 |
| binlog 里的 DDL 事件 | QUERY | QUERY | QUERY | QUERY | 都靠文本解析出作用表（`ddl_table.go`）；解析不出就不猜 |
| `binlog_row_metadata=MINIMAL`（默认） | 无列名 | 无列名 | 无列名 | 无列名 | 按序号用 catalog 补列名（`enrichRelation`）；**FULL 时列名/字符集直接来自 binlog** |
| 默认认证插件 | `mysql_native_password` | `caching_sha2_password` | `caching_sha2_password` | 同 8.4 | 连接器不假设插件；DSN 里不给 `allowNativePasswords=false` 之类的硬开关 |
| 默认字符集/排序 | `latin1` / `latin1_swedish_ci` | `utf8mb4` / `utf8mb4_0900_ai_ci` | 同 8.0 | 同 8.0 | **不按版本猜**：列字符集一律取 binlog 的 collation id（FULL 元数据）或 information_schema；`charsetMaxLen` 只做「字节→字符」换算 |
| VARCHAR 长度单位 | 字节 | 字节 | 字节 | 字节 | 除以字符集 mbmaxlen（§5.1 的 bug 就是漏了这一步） |
| `binlog_row_image` 默认 | FULL | FULL | FULL | FULL | 依赖 FULL；非 FULL 时 DELETE 前镜像不全，preflight 明确提示 |
| DDL 可解析性 | 同 | 同 | 同 | 同 | 多表语句（`RENAME TABLE a TO b, c TO d`）**不猜**，表名留空 → 不发提示（不误导） |

---

## ⑤ DDL 改类型时向上暴露的信号（样例，真跑输出）

源端：`ALTER TABLE engine_my_type_change MODIFY COLUMN v VARCHAR(64) NULL`（原来是 `BIGINT`）。

```
[实时同步中] 源端 DDL 没有搬到目标端（本版不搬 DDL）：ALTER TABLE engine_my_type_change MODIFY COLUMN v VARCHAR(64) NULL。
             数据行照常同步；索引/约束/改类型需要手工在目标端执行
[实时同步中] 已登记暂停：表 engine_my_type_change（原因码 type_changed），位点没有推进
[实时同步中] 表 engine_my_type_change 与源端的列类型不一致（列 v：源端 bigint → varchar(64)（目标端映射 bigint → text））
…
结果：阶段 failed，复制 0 行，增量 1 行，用时 325ms，退出码 1
```

目标端 `_jl_meta.paused_tables` 里只有**这一张表**（同一批次另一张表不登记）：

```json
{"engine_my_type_change":{"reason":"type_changed",
  "at":"2026-11-11T02:15:03Z",
  "detail":"表 engine_my_type_change 与源端的列类型不一致（列 v：源端 bigint → varchar(64)（目标端映射 bigint → text））"}}
```

给引擎/门禁的三条机器可判值：`reason`（稳定码）、`detail` 里的 `列名 + 源端旧类型 + 源端新类型`、
以及事件本身的 `source.Event{Kind: EvDDL, Table: {Schema, Table}}`（**本轮修好的：之前 Table 只有库、没有表名**）。

对账口径：`paused_tables` 的 key 是**目标端裸表名**，与 `_jl_chunk.table_name` 逐字节同源；
`reason` 只用 `type_changed` / `schema_drift` 两个码，中文句子进 `detail`。

---

## 5. 本轮发现的两个缺陷（都已修，附判据）

### 5.1 类型渲染把字节当字符（CHAR / VARCHAR 都中）

binlog 的 VARCHAR/CHAR 元数据是**字节**上限：utf8mb4 的 `VARCHAR(64)` → 256、`CHAR(10)` → 40。
`typeNameAndArgs` 直接拿它拼 `varchar(256)` / `char(40)`：

- E1015 / 暂停提示会说「源端 bigint → varchar(256)」「char(10) → char(40)」，用户明明写的是 64 / 10；
- 目标端按 Relation 补列（`ADD COLUMN`）时会用这个错长度；
- 类型字典比对（`compatibleType`）按这个字符串比，长度维度静默偏宽。

> 更正一句 PR 初稿里写错的话：**建表不走 binlog 类型**，走的是 catalog（information_schema），
> 所以「MySQL→MySQL 目标端会建成 varchar(256)」是我写错了（Owl II 也复现不出来）。
> 受影响的只有：提示文案、按 Relation 补的列、以及 `compatibleType` 的比对输入。

修法：渲染类型前先取列字符集（`TableMap.collationFor` → `charsetMaxLen`），VARCHAR/CHAR 的 meta 除以
mbmaxlen（除不尽就按字节报，不猜）。判据：`TestTypeNames` 加了 `utf8mb4 256→varchar(64)`、
`utf8mb3 300→varchar(100)`、`char(40)→char(10)`、`varbinary 不除` 四格。

### 5.3 `enrichRelation` 把「同一列」和「改了类型」判反了（MINIMAL 的默认路径）

MINIMAL 元数据下没有列名/字符集，只能按序号用 catalog 补。原来的规则是「列数一致就整体套 catalog」：

- 列数不变的 `MODIFY COLUMN v VARCHAR(64)` → catalog 把它**盖回旧类型**，改类型在流上彻底看不见；
- 反过来一律信 binlog 也不行：MINIMAL 下 char(10) 会渲染成 char(40)、TEXT 会渲染成 blob，
  与 catalog 一比就是「每一张带字符类的表都停下」。

改成**宽松同型**（`looseSameType`）：长度允许 `×1..×4`（字符集未知）、`varbinary≈varchar`、
TEXT 与 BLOB 按尺寸档视为同类；**其余一律不同型 → 保留 binlog 的新类型**。
判据：`TestLooseSameType` 14 格（含「必须判不同」的 5 格）+ `TestEnrichRelationKeepsBinlogTypeWhenTypeChanged`。

### 5.4 VARBINARY 被当成字符串 → 写 bytea 直接失败

`decodeStringCell` 按「内容是不是可打印 UTF-8」决定 Kind，而可打印判定只看 U+FFFD：
`\x00\x01` 这种值被判成字符串，写给 PG 的 bytea 时 `cannot find encode plan`（真库实测，整块数据失败）。
修法：VARBINARY **按定义就是字节**（不看内容），可打印判定拒绝控制字符。
**仍有缺口（下一轮）**：判据仍是「类型字节 + 内容」，而准的判据是 `TABLE_MAP.collationFor(col)==63`
（binary 排序规则，FULL 与 MINIMAL 都有）。所以 `BINARY(4)='abcd'` 这种「内容合法的二进制列」
仍会被当字符串，pgsink 侧 `$n::text::bytea` 遇到以反斜杠开头的值会被 PG **静默改写**（Owl II 的对抗例）。
判据：类型改类型用例里加了一张 `engine_my_chartypes`（CHAR/TEXT/VARBINARY/VARCHAR 四列），
全量与增量都必须原样过去（`encode(vb,'hex')='0001'`、`c='中文十'`、`t='文本'`）。

### 5.5 `char(10)` vs `character(10)`：一个 CHAR 列就能停掉整轮

MySQL 侧写 `char(10)`，PG 的 `information_schema` 回 `character(10)`；`typeAliases` 里有
`bpchar→character` 却没有 `char→character`，`compatibleType` 判不兼容 → prepare 期 E1015 整轮失败。
修法：别名表加 `char → character`。判据：`TestCompatibleType` + 上面那张 chartypes 表的真库用例。

### 5.2 位点断言空转（我自己的用例，假绿）

`integration_mysql_incr_kill_test.go` 原来 `SELECT position FROM _jl_meta LIMIT 1`。
同一套 PG 上只要还有别的任务的行，就会读到别人的位点（实测读到过 `postgres:v1:0/2E67668`），
而 `myPosLess` 解析不出 binlog 偏移 → 比较恒 false → **三条位点断言全变成空转，用例照样绿**。

修法：只认 `mysql:v1:` 前缀、必须**正好一行**（0 行/多行都当场红并打印库里所有行），
三个位点在比较前逐个断言「能解析出 `pos=`」。
判据：修完之后同一条用例的位点是 `675108 → 719304 → 753216`，真的在比。

---

## 6. 与「裁决 A」的关系（原阻塞点已解）

派工里那句「只暂停该表」在我开工时还是个契约缺口；本轮收尾时**裁决已经下了**：
`#90 契约冻结：按表暂停=跳过该表+位点前进+恢复整表重搬`，实现挂在 `#93`。
所以本 PR **不碰写路径**，只提供它需要的输入：

1. **认出 DDL 作用在哪张表**（`source.Event.Table`）——没有它，跳过/记录都无从谈起；
2. **源端前后类型**（`pgsink` 的 `srcTypes` + `detail` 里的 `bigint → varchar(64)`）——
   这是「恢复时整表重搬」的值不值得做的判断依据，也是给用户看的唯一线索；
3. **类型渲染正确**——按字节报字符会让「改类型」判定本身失真（§5.1）。

与 #93 的接触点只有 `pgsink.Apply` 的写路径（它改跳过语义，我改的是消息与类型），
`_jl_meta.paused_tables` 的形状两边都不动。

### 精度边界（更正）

上一版这里写「MINIMAL 拿不到列字符集」，**写错了**：MINIMAL 缺的是**列名**，
列字符集在 TABLE_MAP 的可选元数据里仍然有（CI 的 b4 是 MINIMAL，报出来的就是 `varchar(64)`）。
真正只能按字节报的是 **5.7**（那套元数据更少）——所以 `looseSameType` 的 ×2..×4 主要是给 5.7 兜底。

### 契约已冻结：裁决 A（#90 / #93）

本 PR 的用例已按 A 重写：
- 旁证表**照跑到 5 行**（旧契约下停在 4 行）；
- **位点照常前进**（旧契约下要求不动）；
- 跳过的那一行靠**恢复时整表重搬**补回来（不是靠位点重放）；
- run **不失败**，`--stop-when-idle` 正常收尾（旧契约下是 E1015 失败）。
本 PR 不碰这条写路径，只给它提供「哪张表 + 源端前后类型」。
