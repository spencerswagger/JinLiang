# 夜班 2026-11-11 ① MySQL 源 → 引擎的 DDL/类型信号 + 三组合真数字

接 22:05 那轮（23:41:51 容器被外部重建切断，不是 OOM、不是代码崩）。
分支 `feat/mysql-ddl-type-signal`，基线 main `67db571`。

---

## ① 断点

- **已完成（中断前）**：PR #89（MySQL 源接引擎三处接线 + 增量 kill -9 续跑），CI 8+4 个 job 全绿，等 Owl II 评审。
- **本轮补的是**：真 MySQL 源「DDL 改类型 → 只把那张表记成暂停 → 对齐后恢复追平」的链路，
  以及 `varchar` 长度单位这个**真 bug**（见 §5.1）。
- **仍未做**：「其它表继续跑」需要 Ark 定契约（§6）。

---

## ② 改动文件

| 文件 | 改了什么 | 为什么 |
|---|---|---|
| `internal/mysqlsource/ddl_table.go`（新） | `ddlTargetTable()`：从 DDL 原文认出作用表 | Query 事件只有「默认库 + 语句」；不给表名，引擎的「在不在范围内」判定恒落空 → **源端 DDL 被静默丢掉**，连「本版不搬 DDL」那声提示都发不出 |
| `internal/mysqlsource/assembler.go` | `statementEvent` 填 `Table.Schema/Table` | 同上 |
| `internal/mysqlsource/relation.go` | VARCHAR 长度：字节 → 字符（按列字符集 mbmaxlen 换算）；`mysqlTypeName` 多一个 `maxLen` | binlog 给的是**字节**上限，utf8mb4 的 `VARCHAR(64)` 记 256 → 之前报成 `varchar(256)`（§5.1） |
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

### 5.1 `varchar` 把字节当字符（真 bug，影响 MySQL→MySQL 建表与所有提示文案）

binlog 的 VARCHAR 元数据是**字节**上限：utf8mb4 的 `VARCHAR(64)` → 256。
`typeNameAndArgs` 直接拿它拼 `varchar(256)`：

- MySQL→MySQL：目标端建表会建成 `varchar(256)`（现在 `m2m_orders.payload` 实测是 `varchar(64)`）；
- E1015 提示会说「源端 bigint → varchar(256)」，用户明明写的是 64；
- 类型字典比对（`compatibleType`）按这个字符串比，长度维度静默偏宽。

修法：渲染类型前先取列字符集（`TableMap.collationFor` → `charsetMaxLen`），`varchar` 的 meta 除以
mbmaxlen（除不尽就按字节报，不猜）。`varbinary` **不除**（没有字符集）。
判据：`TestTypeNames` 加了 `utf8mb4 256→varchar(64)`、`utf8mb3 300→varchar(100)`、`varbinary 不除` 三格；
MySQL→MySQL 真跑后查 `information_schema.columns` 得 `varchar(64)`。

### 5.2 位点断言空转（我自己的用例，假绿）

`integration_mysql_incr_kill_test.go` 原来 `SELECT position FROM _jl_meta LIMIT 1`。
同一套 PG 上只要还有别的任务的行，就会读到别人的位点（实测读到过 `postgres:v1:0/2E67668`），
而 `myPosLess` 解析不出 binlog 偏移 → 比较恒 false → **三条位点断言全变成空转，用例照样绿**。

修法：只认 `mysql:v1:` 前缀、必须**正好一行**（0 行/多行都当场红并打印库里所有行），
三个位点在比较前逐个断言「能解析出 `pos=`」。
判据：修完之后同一条用例的位点是 `675108 → 719304 → 753216`，真的在比。

---

## 6. 阻塞点（需要 Ark / 主持人点头）

**「DDL 改类型只暂停该表」= 只停该表 + 其它表继续跑 —— 这一步没做，因为它要改契约。**

现状（今晚再确认）：暂停记录**只登记出问题的那张表**（这是对的），但位点只有一个 →
整条同步停下，同一批次里别的表也不写。用例里已经把这个事实写成断言
（`TestMySQLSourceTypeChangePausesOnlyThatTableAndRecovers` 判据③：旁证表停在 4 行，
一旦改成「其它表继续跑」，这条断言改成 5）。

三个选项与代价见 `notes/nightly-2026-11-10-mysql-source-into-engine.md` §2.2；
本面推荐 **A（位点前进 + 恢复时整表重搬）**：块级续跑已经具备，不需要「按表位点」这种新状态。
需要 Ark 定的是：`_jl_meta.paused_tables` 的值里加一个 `resume: recopy` 字段、
以及 `pgsink.Apply` / `mysqlsink.Apply` 写路径从「整批拒绝」改成「跳过该表的行、其它照写」。

在这之前，我能保证的是：**改类型不会静默写错**（响亮失败 + 只指名那张表 + 位点不从暂停里前进），
对齐后从原位点重放追平、一条不丢。
