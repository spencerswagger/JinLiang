# 设计：无主键表同步（insert-only / 显式唯一键）· v1 定稿

> **状态：设计定稿，尚未实现。** 本文件描述的是**目标行为**，不是现状；
> 每条口径都标了「今天能不能跑」。实现切片见 §5，落地状态由后续 PR 回填到本注。
>
> 基线：`main` `0143986`（`#112` 报错文案、`#113` MY-08 preflight + 单测、`#114` MY-08 矩阵 smoke 已合入）。
> 本文件**不改** `MY-08`/`MY-11` 的编号与语义，也**不推翻** `#112` 已改的 `ErrNoUniqueKey` 文案——只在它上面扩展一句。
> 口径冲突时以本文件为准；本文件与 `docs/SPEC.md` 冲突时，先提出来由主持人裁决，不各自改。

本文件裁决五件事：**两条路线各进哪一版**、**探测项与错误码落点**、**预检四档**、
**`content.tables.unique_key` 的字段语义**、**实现切片**。

---

## §1 口径选择：insert-only 与「显式业务唯一键」

### 1.1 现状（逐条可核对，别凭印象）

| 能力 | 现状 | 依据 |
|---|---|---|
| 源端采集无主键表 | **已支持**：PG 按 `ctid` 游标切块；MySQL 只能单连接顺序扫（`InnoDB` 隐藏 `rowid` 读不到），只有表级断点 | `internal/pgsource/pgsource.go:591-615`；`internal/mysqlsource/snapshot.go:279-299`、`chunksnapshot.go:405-410` |
| 无主键表续跑 | **已支持（降级）**：不按块续，走 `chunkPlan.Recopy` → `recopyOne` 整表重来，只重来这一张 | `internal/engine/resume.go:235-260`、`engine.go:680-705` |
| 源端「有没有主键」的探测 | **已有**：`source.Table.HasStablePK`，预检出 `SCOPE-02` warn | `internal/source/contract.go:92-105`、`internal/preflight/b0.go:265-310` |
| PG 目标端写无主键表 | **已 insert-only**：`conflictClause` 无主键返回空串 → 多行纯 `INSERT` | `internal/pgsink/pgsink.go:2094-2097` |
| PG 目标端删无主键表 | **直接报错**（无码、无四段式、整条任务停）：`表 x 没有主键，无法安全删除目标端对应行` | `internal/pgsink/pgsink.go:1729-1733` |
| MySQL 目标端写无主键表 | **不成立**：写路径一律 upsert，目标无唯一键即 `ErrNoUniqueKey` 整批失败——**全量也走 `Apply`，所以全量就炸** | `internal/mysqlsink/apply.go:208-228`、`upsert.go:11-39`；`engine/target.go:498-540`（`copyGeneric` 也调 `Apply`） |
| MySQL 目标端删 | `deleteSQL` 无主键直接报错 | `internal/mysqlsink/upsert.go:93-96` |
| 目标端唯一键探测 | 只有 `mysqlsink.hasUniqueKey`：`COUNT(*) … NON_UNIQUE = 0`。**列组合、NULL、前缀索引全没看** | `internal/mysqlsink/upsert.go:18-39` |
| 「有唯一键」≠「按主键 upsert」 | MySQL 的 `ON DUPLICATE KEY` **不指定冲突目标**，命中的是**任意**唯一索引：目标端恰好有另一条唯一索引时，我们会按它更新，语义与源端主键不一致，而且不报错 | `internal/mysqlsink/upsert.go:46-90`（拼 SQL 时没有冲突列清单） |
| 目标端唯一键**预检** | **没有任何一条**（`TARGET-01` 只管「目标端已有同名表」） | `internal/preflight/b0.go:130-150` |
| `verify` 无主键表 | 只做聚合（`all_partial`），明确标注 | `internal/verify/verify.go:207-290`、`:439` |
| 可跑方向 | PG→PG（全量+增量）、PG→MySQL（**只全量**）、MySQL→PG（全量+增量）；**MySQL→MySQL 未接通** | `internal/core/direction.go:39-46` |

**三个真实缺口**（本设计就补这三个）：

1. MySQL 目标端**根本没有 insert-only 路径** → 无主键表连全量都跑不了；
2. 无主键表收到 BACKFILL/UPDATE/DELETE 时，PG 目标端抛一句裸错误**整条任务停**（连带几十张无关的表），
   MySQL 目标端更早死在 `ErrNoUniqueKey`；
3. 目标端「到底有没有唯一键」开跑前看不到，用户第一次知情是在 `last_error` 里。

### 1.2 两条路线

| | 路线 A：insert-only（默认） | 路线 B：显式业务唯一键 `unique_key`（可选） |
|---|---|---|
| 语义 | 目标端只 `INSERT`，从不 `UPDATE`/`DELETE` | 用用户声明的一组列做冲突目标 + 删除定位条件 |
| 适用 | 无唯一键的表；全量（`once`/`full_interval`）；只追加的增量 | 表无主键但业务上有身份（订单号、外部 id、`(tenant_id, biz_no)`），且目标端**真有**唯一索引 |
| 拿到什么 | 能同步、不阻断、不用改源表结构 | 幂等（重放不插重复）+ 改/删可表达 |
| 代价 | 重放窗口内可能重复行；改/删不可表达（只能整表重搬补齐） | 用户必须保证唯一性；**谎报 = 静默重复/误删** |
| 进哪一版 | **v1** | **v1**（字段 + 核对 + 写路径；默认关闭） |

**为什么两条都进 v1**：只做 A，`continuous` 模式的用户只要表里有一次 `UPDATE` 就永远停在「暂停 + 欠一次重搬」，
3–5 人日的投入换不来可用；只做 B，则所有无主键表都被迫改配置才能跑，与「不拒绝，只提示」相悖。
A 是地板，B 是天花板。

### 1.3 组合规则（四条，实现时不许改）

- **R1 身份判定只认源端。** 一张表按什么身份写，只看两件事：源端 `HasStablePK`，或用户显式声明 `unique_key`。
  **绝不因为「目标端恰好有个唯一索引」就自动把 insert-only 升成 upsert**——那是拿别人的索引猜你的行身份，
  猜错的表现是「悄悄改了一行本该新增的数据」。目标端索引只用于**核对声明**，不用于发现身份。
- **R2 `unique_key` 不参与切块与续跑。** 无主键表仍然整表重来（`resume.go:235-260` 不动）。
  理由：源端没有索引保证这组列唯一，拿它当 keyset 游标会漏行；「源端唯一」这件事我们无从实证。
- **R3 声明必须可核对，核对不过就是 `blocked`。** 判据见 §4.3。谎报唯一键不是「风险」，是「配置与事实不符」：
  MySQL 的 `ON DUPLICATE KEY` 在没有唯一索引时**不报错、照插**，等于静默写重复。
- **R4 没有身份的表，改/删不猜。** 既不「把 UPDATE 当 INSERT 插一版」（目标端留两版，行数膨胀且越滚越多），
  也不「按全表条件 DELETE」（会清空目标表）。只做一件事：**这一批先登记暂停、跳过它、位点照常前进**，
  欠一次整表重搬（复用 `#93`/`#102` 的按表暂停机制，见 §2.3）。

### 1.4 进 v1 / 挂账

**进 v1**

1. MySQL 目标端 insert-only 写路径（多行纯 `INSERT`，不拼 `ON DUPLICATE KEY`）；
2. 无主键表收到 `EvUpdate`/`EvDelete` → 新原因码 `no_pk_write` 的 `PausableError`
   → 按表暂停 + `skipped_rows` 记账 + 退出码 3 → 下一轮整表重搬补齐
   （`EvTruncate` 不受影响：它是表级动作，不需要行身份）；
3. `content.tables.<表>.unique_key`：字段 + 校验 + 目标端唯一索引核对 + 两个 sink 的 upsert/delete 用它；
4. 预检 `SCOPE-02` 按 mode 分四档（§3）；新增 `TARGET-02` 目标端唯一键三档；
5. 码表 `E1017` + `docs/ERRORS.md` 同批落地；`ErrNoUniqueKey` 文案点名 `unique_key`（`#112` 挂账收口）。

**挂账（不进 v1，写进 §7）**

- 源端唯一键**自动推荐**（读源端 `information_schema.STATISTICS` 给候选，省用户手填）；
- `verify` 对无主键表做**行级**比对（可按 `unique_key` 排序，但那要先把聚合口径改成「有键就用键」）；
- 目标端缺主键（源端有主键）从「整批失败」改成「E1015 按表暂停」——是另一条口径，别塞进这 3–5 人日；
- `NULLS NOT DISTINCT`（PG 15+）唯一索引作为 `unique_key`；
- 基于 `unique_key` 的块级续跑（要先把「源端唯一」实证出来）；
- MySQL→MySQL 方向接通后补的矩阵格（见 §2.4「待接通」）。

### 1.5 被否决的方案

| 方案 | 为什么否 |
|---|---|
| 无主键就以全表条件 `DELETE` 再插 | 一次事件 = 清空目标表，最坏情况是「源端改一行，目标端少一千万行」 |
| 把 `UPDATE` 降级成 `INSERT`（保留两版） | 目标端行数只增不减，`verify` 每次都红，且永远回不到「等于源端当前状态」 |
| 自动挑目标端某个唯一索引当身份 | 违反 R1；用户后来的索引变更会**静默改变同步语义** |
| `continuous` 每轮整轮重搬（靠「重搬代替 upsert」） | 无主键表通常是大表（没有主键多半是历史宽表），每轮全表重搬等于不可用 |
| 无主键表一律 `blocked` | 违反 DESIGN 原则 6；而且 PG 目标端今天已能跑 insert-only，退回去是砸已有能力 |

---

## §2 能力探测与补偿矩阵

### 2.1 探测项落点（编号规则）

| 探测项 | 覆盖什么 | 状态 | 现状 |
|---|---|---|---|
| `SCOPE-02`（已有，改文案） | **源端**选中的表没有稳定主键 | 按 mode 四档（§3） | `preflight/b0.go:265-310`，今天统一 warn 且只在非 `once`/`full_interval` 发 |
| `TARGET-02`（**新增**） | **目标端**这张表有没有可用于 upsert/删除的唯一键；用户声明的 `unique_key` 是否真的对得上 | ok / warn / blocked 三档 | 不存在 |
| `MY-08`（不动） | MySQL 源端表的**存储引擎**（MyISAM → warn 照跑） | warn | `preflight/b0.go:313-352`（`#113`） |
| `MY-11`（不动） | MariaDB → blocked | blocked | `mysqlsource/capability.go:261-270`（`#113` 从 MY-08 挪过来） |

**本设计不占用任何 `MY-x`**，理由三条：

1. `MY-01…MY-11` 是**MySQL 源端**能力项（变更流水、行镜像、引擎、认证插件、主从角色）。「有没有主键」与引擎/账号无关，
   且 **PG 源同样成立**——放进 `MY-x` 就得再给 PG 抄一份 `PG-xx`，同一件事两套文案，迟早分叉。
   所以它落在跨引擎的 `SCOPE` 家族（`SCOPE-01` 是「表在不在」，`SCOPE-02` 是「表有没有主键」，同族同粒度）。
2. `MY-09`/`MY-10` 是 SPEC 的**保留位**（认证插件、主从角色），不许挪用；`MY-11` 已被 MariaDB 检查占用。
3. 目标端检查在本仓库已有家族：`TARGET-01`（目标端已有同名表）。目标端唯一键就是 `TARGET-02`，
   不新开前缀、不新开 `TARGET-03`（声明的核对也并进同一条的三档里——同一件事的不同结论，不是两条检查）。

> **下一个可用的 MySQL 源端编号是 `MY-12`**（`MY-08` 引擎 / `MY-09` 认证 / `MY-10` 角色 / `MY-11` MariaDB 之外）。
> 将来若真要做「源端唯一键自动推荐」，用 `MY-12`，别回头挤 `08…11`。

### 2.2 补偿矩阵（`Compensation{Level, Note, ConfigKey}`）

`source.Level` 只有三档（`internal/source/contract.go:34-50`）：`auto`（我们补，用户无感）/ `costly`（有代价、要用户知情）/
`blocked`（物理做不到，必须给替代）。无主键表逐格如下：

| # | 源表身份 | 模式 | 目标端 | 探测项 | Level | Note（进预检报告的人话） | ConfigKey |
|---|---|---|---|---|---|---|---|
| 1 | 无主键 | `once` / `full_interval` | 任意 | `SCOPE-02` ok | `auto` | 每轮整轮重搬，不需要行身份；搬完前目标端是部分数据 | — |
| 2 | 无主键 | `continuous` / `continuous+initial` | 任意 | `SCOPE-02` warn | `costly` | 只追加：改/删会让这张表暂停并欠一次整表重搬；重放窗口（≤ 一个批次）内可能重复行 | `content.tables.<表>.unique_key` |
| 3 | 无主键 | `interval` | 任意 | `SCOPE-02` **blocked** | `blocked` | 每轮按水位重读同一批行，没有身份 = **每轮必然重复** | 同上 |
| 4 | 无主键 + 声明 `unique_key`，目标端核对通过 | 任意 | 任意 | `TARGET-02` ok | `auto` | 声明被采纳：upsert + 删除定位都成立（用户无需再做别的） | — |
| 5 | 无主键 + 声明 `unique_key`，目标端核对**不过** | 任意 | 任意 | `TARGET-02` **blocked** | `blocked` | 声明与目标端事实不符：那条唯一索引不存在/含 NULL/是前缀索引 | 同上 |
| 6 | 有主键 | 任意 | 目标端没有可用唯一键（`append` 复用用户表） | `TARGET-02` warn | `costly` | upsert 会失败；加索引或换空 schema。**目标端若只有另一条无关的唯一索引，今天会被判成「有键」并照它更新**（见 §1.1 那行，挂账 D9） | — |
| 7 | 有主键 | 任意 | 任意 | `TARGET-02` warn | `auto` | 表已有主键：`unique_key` 声明被忽略（按主键走） | — |

格 3 为什么是 `blocked` 而不是 `warn`：见 §3.3（与 MY-08 的差异不是随意的）。

### 2.3 错误码与原因码

**新增 `E1017`（user_action）** —— 只在「无主键表收到写不进去的变更」时出现：

| 字段 | 内容 |
|---|---|
| Message | 这张表没有主键，改动和删除写不进去 |
| Why | 目标端只能按主键或唯一索引定位一行；没有它，改动会变成再插一行，删除不知道删哪一行 |
| Next[0] | 给源表和目标表都加上主键或唯一索引（推荐：两边一致，位置与索引名都不用另配） |
| Next[1] | 或者在 `content.tables.<表>` 里写 `unique_key: [列…]`（必须是目标端**已有**的唯一索引，且列都非空） |
| Next[2] | 或者把 `mode.frequency` 改成 `once` / `full_interval`：每轮整表重搬，改和删不用逐条写 |
| Risk | 这张表会暂停、其它表照常、位点照常前进；跳过的那段要等整表重搬才补齐，期间它和源端不一致 |

约束（`internal/core/codes_test.go:14-36` 会机械校验）：Message ≤ 40 汉字、Next 1–3 步、Why/Risk 必填。
**同批要改** `docs/ERRORS.md`（用户表加一行）——文档是手抄的，代码改了文档不改就是假说明书。

**原因码 `no_pk_write`** 进 `_jl_meta.paused_tables[].reason`（稳定机器码，现有两个是 `type_changed`/`schema_drift`，
`internal/pgsink/paused.go:32-50`）。它与 `E1015` 的区别：E1015 是「结构对不上」，E1017 是「结构没错，但没有身份」。
`Detail` 里写人话（哪张表、哪条变更、需要什么），不写进 Message。

**`ErrNoUniqueKey` 保留，语义收窄**（不删）：它以后只在「用户声明了 `unique_key`，但运行时发现目标端索引消失/不匹配」
或「源端有主键、目标端缺对应唯一键」时出现。文案按 Owl `#112` 挂账同步点名 `unique_key`（§4.5 第 1 条）。

### 2.4 测试矩阵新增格数（真实算术，不是「翻倍」）

口径与 `DESIGN.md §2.3` 一致：**1 格 = 1 条链路 × 1 个场景的一次可执行用例**。
无主键语义与**版本号无关**（它取决于索引形态，不取决于 5.7 还是 9.x），所以只在 T1 + 各层代表链路上铺，
不做 169 链路全翻——翻了也只是重复跑同一段逻辑。

每条「无主键变体」跑 3 个场景：
**S-NP1** 全量搬完（insert-only 落全量，行数与源端一致）·
**S-NP2** 增量纯 INSERT（不暂停、位点前进）·
**S-NP3** 增量加一次 `UPDATE`/`DELETE`（→ 该表暂停 + `skipped_rows` + 退出码 3 → 下一轮重搬后收敛）。

| 矩阵层 | 链路 | 变体场景 | 新增格 | 今天能跑？ |
|---|---|---|---|---|
| **PR 门禁**（改动 sink/engine 的 PR 必跑） | PG 18→18、MySQL 8.4→PG、MySQL 5.7→PG、PG 18→MySQL | 3 / 3 / 3 / 1（PG→MySQL 只全量） | **10** | 能 |
| nightly T1（自洽） | PG 10→10 … 18→18（9 条） | 3 | **27** | 能 |
| nightly T1（MySQL 同版本 4 条） | MySQL 5.7→5.7 … 9.x→9.x | 3 | 12 | **不能**（方向未接通） |
| nightly T2（相邻升级代表） | PG 10→11、PG 17→18 | 3 | **6** | 能 |
| nightly T3（极端跨度代表） | PG 10→18（3）、PG 18→10 降级只验「提示并允许」（1） | 3 / 1 | **4** | 能 |
| nightly T4（异构抽样） | 每周 4 条代表链（两方向 × 2 组合） | 3 | 12/周（摊每晚 ≈ 2） | 能 |
| **nightly 小计** | | | **≈ 39 格/晚** | |
| release T4（全量） | MySQL→PG 36 条 × 3 = 108；**PG→MySQL 36 条 × 1 = 36**（该方向只全量） | | **144** | 能 |
| release `unique_key` 路线 | PG 18→18（once 幂等、continuous upsert+delete 两场景 = 2）、PG 18→MySQL（once = 1） | | **3** | 能（MySQL 目标端只有全量） |
| **release 小计** | 上面 T1/T2/T3 的 27+6+4，加 T4 的 144，加 `unique_key` 的 3 | | **184 格** | |

- **PR 增量 10 格**，目标仍是 ≤15 分钟（现有 T1 子集上再加，不加新链路类型）。
- **nightly 增量 ≈ 39 格/晚**（相对现基线 320 单元/晚，+12%）。
- **release 增量 184 格**（相对 1352 单元，+13.6%）。
- **待接通 12 格**：`MySQL→MySQL` 的 4 条 T1 链路 × 3 场景。今天**一格都跑不了**——
  也就是说 **MySQL 目标端的 insert-only 只能靠单测 + 本地真库 smoke 证明**，矩阵要等方向接通才补得上。
  这条必须写进 PR 描述与 `notes/`，别让「矩阵绿」被误读成「目标端已全覆盖」。
- **矩阵证明不了的一格**：`MySQL 目标端 + 增量改/删 → 暂停`——同因（PG→MySQL 只全量）。归入同一笔挂账。

> 真实格数的另一面：**不要**把 `verify` 的行级比对、`interval` 的水位重复列进上面任何一格。
> 前者 v1 仍是「仅聚合」（`verify.go:207-290`），后者 v1 是 `blocked`——它们各自有独立的判据，别混进这 184 格里充数。

---

## §3 preflight 行为：无主键表什么时候是 warn、什么时候是 blocked

### 3.1 四档（`SCOPE-02`，按 `mode.frequency` 分）

| 模式 | 状态 | 卡片标题 | 为什么是这个档 |
|---|---|---|---|
| `once` / `full_interval` | **ok** | 无主键表不影响这个模式 | 每轮 `copyFresh` + `markClean`（`resume.go:183-196`），结果等于源端当前状态，不需要行身份。代价（搬完前是部分数据）写进 Detail，不单开卡片。**边角**：第二轮起会先清本任务写过的行；若这张目标表是你自己的（`append`），会按既有口径停下来让你选（`reusedTableRecopyError`，本设计不改它） |
| `continuous` / `continuous+initial` | **warn** | 部分表没有主键：只追加，改/删会让它暂停 | insert-only 能跑，`UPDATE`/`DELETE` 触发按表暂停并欠一次重搬——有代价、可接受、用户要知情 |
| `interval` | **blocked** | 定时增量 + 无主键：每轮必然重复 | 见 §3.3 |
| 未指定 / 空 | **warn** | 同 `continuous` | 解析阶段缺省按增量口径，宁可多说一句 |

Detail 必须**分档写**，不许共用一句话（这是 `PAUSE-01` 的教训：同一次输出里两处口径互相打脸，
`preflight/b0.go:155-201` 已按 mode 分流，`SCOPE-02` 照做）。

### 3.2 与 `MY-08`（MyISAM）一致还是差异

**一致的半边：都是 warn 照跑。** 两者都是「能同步，但有代价」，都不该阻断——`DESIGN.md` 原则 6 的原话就是
「MyISAM 照跑（标注无事务）、无主键表照跑（insert-only）」。它们的口径来自同一条：**决定权在用户，我们只把代价说清**。

**差异的半边（必须写进卡片，不然用户会觉得两条自相矛盾）：**

| | `MY-08`（MyISAM） | `SCOPE-02`（无主键） |
|---|---|---|
| 风险的性质 | **概率性**：崩溃/断电时目标端可能不一致 | **确定性**：改/删**每一写都写不进去** |
| 用户能做什么 | 改引擎（停机重建表）或接受 | 加主键/唯一键、声明 `unique_key`、或换 `once`/`full_interval` |
| 是否影响「结果正确」 | 不影响正常路径 | 影响：跳过窗口内目标端与源端不一致，等重搬才收敛 |
| 档位结论 | 一律 warn | `once`/`full_interval` ok、`continuous` warn、`interval` **blocked** |

### 3.3 为什么 `interval` 是 blocked（而不是学 MY-08 一律 warn）

`interval` 的写路径是 `runIntervalRound` 的 `ApplyRowsRange`/`BulkApply`（`engine/interval.go:225,241`）：
**每轮把水位范围内的行重读一遍**。有唯一键时靠 upsert 吸收重复；没有身份时就是**每轮再插一遍**——
不是「重放窗口内可能重复」，是「第二轮起必然重复、且每轮线性增长」。这不是风险，是这个模式在我们手里做不到。
所以按 `blocked` 的既定含义给三条替代（加键 / 声明 `unique_key` / 换 `continuous+initial` 或批式模式），
而不是让用户点一下 `--accept-risk` 去接受一份注定重复的数据。

（同样的道理也解释了为什么 `SCOPE-02` 今天对 `interval` 发的那句「重复跑会插重复行」是**对的**，
只是档位太低——今天它是 warn，本就是安全债。）

### 3.4 `TARGET-02` 三档

| 目标端情况 | 状态 | Detail 要点 |
|---|---|---|
| 目标表有主键（或与源端主键同列的唯一索引） | ok | 直接可以 upsert |
| 目标表没有可用唯一键，源端也无主键且没声明 | warn | 「只追加」+ 指到 `content.tables.<表>.unique_key` 与 `once`/`full_interval` 两条路 |
| 声明了 `unique_key`，但目标端没有匹配的唯一索引（§4.3 判据） | **blocked** | 逐条点名哪条判据不过（不存在 / 含 NULL / 前缀索引 / 列不一致），给三条改法 |
| 源端有主键、目标端缺对应唯一键（`on_exists=append` 复用用户表） | warn | upsert 会失败；加索引或换空 `target.schema` |

`TARGET-02` 需要目标端 catalog 带上**唯一索引**：现有 `source.Table` 只有 `PrimaryKey`
（`contract.go:92-105`），所以要在 `connect.mysql`/`connect.postgres` 的 `Catalog` 里补
`UniqueKeys [][]string`（非主键、非部分、非表达式、全列的唯一索引）。这是 v1 唯一的**契约结构扩展**，
必须同步 `docs/CONNECTOR-SDK.md` 的 Catalog 字段表。

---

## §4 用户面改动

### 4.1 字段

```yaml
content:
  tables:
    order_events:            # 这张表源端没有主键
      unique_key: [tenant_id, event_no]   # 新增：声明行身份
```

```go
// internal/control/jobspec.go（与 columns / where / transform 并列，不在 columns 里面）
type TableDoc struct {
	Columns   *ColumnsDoc      `yaml:"columns,omitempty" json:"columns,omitempty"`
	Where     string           `yaml:"where,omitempty" json:"where,omitempty"`
	Transform []map[string]any `yaml:"transform,omitempty" json:"transform,omitempty"`
	UniqueKey []string         `yaml:"unique_key,omitempty" json:"unique_key,omitempty"` // 新增
}
```

- **字段名** `unique_key`：与 MySQL 的 `UNIQUE KEY`、catalog 里的 `primary_key` 对齐，用户不需要翻译。
  不用 `identity`（太抽象）、不用 `key`（会与「键值对」混淆）、不用 `business_key`（业务与否是用户的事）。
- **类型** `[]string`，只接受数组。`unique_key: "a,b"` 是**解析错误**（strict 解析，`SPEC.md §1` 规则 1：
  打错字比报错更危险）。
- **位置**：`TableDoc` 顶层，**不放** `columns` 里。它是「身份」，`columns` 是「搬哪些列/怎么映」——
  两件事混在一起，用户会以为 `unique_key` 也会过滤列。
- **JSON schema**：`schema/job.schema.json` 的 `content.tables.*` 加
  `"unique_key": { "type": "array", "items": { "type": "string" }, "uniqueItems": true }`。

### 4.2 语义

1. 只对**源端无稳定主键**的表生效（`HasStablePK=false`）。
2. 生效后：写路径用 `INSERT … ON DUPLICATE KEY UPDATE`（MySQL）/ `ON CONFLICT (… ) DO UPDATE`（PG）；
   `DELETE` 用它做 `WHERE` 定位；`EvTruncate` 不受影响。
3. **不改变**：搬哪些列、`where` 过滤、切块策略、续跑判据（R2）。
4. 源端**有**主键的表声明了 `unique_key` → 忽略 + warn（`TARGET-02` 格 7），不改行为。
   理由：主键就是身份，换一组列当身份会让「同一条数据两次同步用两个身份」。
5. 声明后仍**不允许**双向/冲突策略参与（`CONFLICT.md §1` 不变：无主键表不进冲突判定；`unique_key` 只是让写路径有定位，
   不把它升格成「主键」去参与 `manual`/`newest` 那套判定）。

### 4.3 与 `include` / `exclude` / `map` / `where` 的关系（校验规则）

| 关系 | 规则 | 违反时 |
|---|---|---|
| `columns.include` / `exclude` | `unique_key` 的每一列都必须在**搬运列集合**里（include 之后、exclude 之后） | 参数错误（`jobspec` 校验期就能判，不用连库）：「`unique_key` 引用的列 `x` 不在搬运列里」 |
| `columns.map` | `unique_key` 的列**不允许**被 `map` 成不同类型（映射可能引入舍入/截断 → 定位失配） | 参数错误：「`unique_key` 的列不能同时在 `columns.map` 里换类型」 |
| `where` | 无限制。有 `where` 时目标端只会有满足条件的行，`unique_key` 的定位仍然成立（`DELETE` 用源端旧镜像的值定位） | — |
| `transform` | v1 不参与（`transform` 目前只有契约、无实现）；若将来实现，`unique_key` 列被改写 → 同 `map` 处理 | 挂账 |
| 空 / 重复列 | 不接受空数组、空列名、重复列 | 参数错误 |

**目标端核对判据**（`TARGET-02` 格 5 的 `blocked` 判据，五条全过才算「唯一键可用」）：

1. 存在一个**唯一**索引（`NON_UNIQUE=0` / `indisunique`）；
2. **非部分**索引（PG `indpred IS NULL`）、**非表达式**索引（列全是普通列）；
3. 索引的**列集合**与声明**完全一致**（顺序无关，但不多不少）；
4. **没有前缀长度**（MySQL `SUB_PART IS NULL`）——`UNIQUE KEY (url(64))` 不保证整列唯一；
5. 每一列都 **NOT NULL**——两边都把 NULL 当「互不相等」，含 NULL 的列组合不构成唯一性。

### 4.4 `unique_key` 的传递路径

```
control.JobDoc.Content.Tables[t].UniqueKey
  ├─ preflight.Input.UniqueKeys   （预检 TARGET-02，key 用源端裸表名，与 SCOPE-02 同口径）
  └─ engine → sink.Options.UniqueKeys（key 用目标端裸表名，与 _jl_chunk.table_name / paused_tables 同源）
```

只喂给**两个 sink**（写路径）与**预检**。引擎不需要它：改/删写不进去这件事由 sink 返回
`PausableError` 表达，引擎只看原因码（`pause_skip.go:60-90` 现有形状，不新增接口）。

### 4.5 报错文案同步项（挂账清单，实现切片 S3 同批收口）

1. **`internal/mysqlsink/apply.go:218-222` 的 `ErrNoUniqueKey` 文案**（Owl 在 `#112` 的挂账）：
   现在只有「① 加主键/唯一索引 ② 从 `content.tables` 去掉」。要**加一条**：
   「或声明 `unique_key: [列…]`（指向目标端已有的唯一索引）」。风险句保留。
2. `internal/mysqlsink/upsert.go:93-96` `deleteSQL` 的裸错误 → 换成 `E1017` 四段式（或 `PausableError` + `no_pk_write`）。
3. `internal/pgsink/pgsink.go:1729-1733` `deleteTx` 的裸错误 → 同上（**这是今天最刺眼的一处**：用户看到一句话，任务就停了）。
4. `internal/core/codes.go` 加 `CodeNoIdentity Code = "E1017"` + `specs` 条目；`docs/ERRORS.md` 加行。
5. `docs/SPEC.md:91` 的 `PG-06` 标注「实现形态 = `TARGET-02`」（跨引擎同一条），避免同一件事写成两条检查。
6. `docs/STREAMING.md:77`、`docs/CONFLICT.md:6`、`docs/SNAPSHOT.md:12-13`、`docs/VERIFY.md:40`、`docs/OPEN-QUESTIONS.md#1`：
   各加一句「口径见 `DESIGN-NO-PRIMARY-KEY.md`」，不再各自表述。
7. `docs/CONNECTOR-SDK.md`：Catalog 字段表加 `UniqueKeys`（§3.4）。
8. `internal/verify/verify.go:439` 那句「增量只能 insert-only」在声明 `unique_key` 后不再准确 →
   文案改成「这张表没有主键：本版只比行数（声明 `unique_key` 后仍不做行级比对，见挂账 D2）」。**别**承诺没实现的能力。

---

## §5 实现切片（给 Core / Data / Gate 派活用）

| 片 | 内容 | 涉及模块 | 验收（可执行） | 估 |
|---|---|---|---|---|
| **S1** insert-only 写路径 | MySQL 目标端：目标无唯一键 + 源端无身份 + 未声明 → 多行纯 `INSERT`（不拼 `ON DUPLICATE KEY`）；取消 `hasUniqueKey` 的硬失败；`ErrNoUniqueKey` 只在「声明了但对不上」或「源端有主键、目标缺键」时出现 | `internal/mysqlsink/{upsert,apply}.go`、`internal/pgsink/pgsink.go`（对齐） | 单测：无主键源表 → 生成 SQL 不含 `ON DUPLICATE KEY`；本地真库 PG→MySQL 全量搬完、行数一致 | 1d |
| **S2** 改/删 → 按表暂停 | 新 `PausableError`（`Reason()="no_pk_write"`）在两个 sink 的 `EvUpdate`/`EvDelete` 分支前置判定（**写之前**判定，别写到一半）；引擎侧 `pauseSink` 登记 + `skipped_rows` + 退出码 3；下一轮重搬补齐；**`E1017` 码表 + `ERRORS.md` 由这片落地**（它出现的时点就是这片） | `internal/pgsink`、`internal/mysqlsink`、`internal/engine/{pause_skip,pause_generic,resume}.go`、`internal/core/codes.go`、`docs/ERRORS.md` | 集成：无主键表 `UPDATE` → 该表 `paused_tables.reason=no_pk_write`、`skipped_rows≥1`、**其它表行数继续增长**、退出码 3；重跑后该表与源端一致 | 1.5d |
| **S3** `unique_key` | `TableDoc.UniqueKey` + 校验 + schema；`source.Table.UniqueKeys`（两个 `connect`）；`TARGET-02` 三档 + `preflight.Input.UniqueKeys`；两个 sink 的 upsert/delete 用声明；§4.5 其余文案（1/2/3/5–8） | `internal/control/jobspec.go`、`schema/job.schema.json`、`internal/connect/{mysql,postgres}.go`、`internal/source/contract.go`、`internal/preflight/b0.go`、两个 sink | 单测：五条核对判据逐条红/绿；集成：声明真唯一索引 → 重放两次行数不变、`DELETE` 生效；声明不存在的索引 → `TARGET-02` blocked + 四段式 | 1.5d |
| **S4** 矩阵格 | §2.4 的 PR 10 格 + nightly 39 格；`testmatrix.yaml` 标注「待接通 12 格」 | `test/`、`scripts/`、`testmatrix.yaml` | PR 门禁 ≤15 分钟；nightly 报告里能单独筛出 `no-pk` 变体 | 1d |

- **S1/S2 可并行**（S2 只依赖 `PausableError` 的形状，不依赖 S1 的 SQL 生成）；**S3 依赖 S1**（写路径要先有 insert-only 才能对比 upsert）；
  **S4 依赖 S1+S2+S3**。
- 总计 5 人日（S1 1d + S2 1.5d + S3 1.5d + S4 1d），正好压在排期 3–5 人日的上限；
  若砍范围，**先砍 S3**（v1 只留 insert-only + 暂停 = 2.5d），但那样 `continuous` 用户遇到第一次 `UPDATE`
  就只能整表重搬，要把这句话写进发布说明。

---

## §6 实现方红线（越一条就打回）

1. **不许因为「没有主键」拒绝开跑**。warn 照跑；只有 `interval`（必然重复）与「声明核对不过」（配置与事实不符）是 blocked。
2. **不许在写路径上把 `UPDATE` 降级成 `INSERT`**，也不许按全表条件 `DELETE`（§1.5 已否）。
3. **身份只认源端**（R1）：目标端有唯一索引 ≠ 可以 upsert。
4. **`unique_key` 不参与切块/续跑**（R2）：无主键表仍整表重来。
5. **暂停必须登记先行**：先 `PauseTable` 落库成功，才允许这一批跳过（`pause_skip.go` 现有三条线，别绕）。
6. **改/删的判定必须在写入之前**：一批里先扫事件类型，命中就整批返回 `PausableError`，
   不许「写了一半才发现定位不了」——那会留下半批数据 + 位点不动。
7. **文案分档**：`SCOPE-02` 四档、`TARGET-02` 三档，同一份报告里不许出现两句互相矛盾的话
   （`PAUSE-01` 踩过：引擎 note 说「只停这一张表」、预检卡片说「不做按表暂停」）。
8. **矩阵证明不了的必须写在 PR 描述与 `notes/`**：MySQL→MySQL 未接通，目标端增量这一半今天只有单测 + 本地真库。

---

## §7 欠账清单（编号供后续 issue 引用）

| # | 欠账 | 为什么现在不做 |
|---|---|---|
| D1 | 源端唯一键**自动推荐**（读源端 `information_schema.STATISTICS` 给候选） | 推荐错的代价比让用户手填大；先把核对做扎实 |
| D2 | `verify` 对无主键表做**行级**比对（按 `unique_key` 排序） | 要把「有键就用键」的口径先定（`verify` 现在按 `HasStablePK` 一刀切），属另一条口径 |
| D3 | 目标端缺主键（源端有主键）从「整批失败」改成 `E1015` 按表暂停 | 与「无主键表」是镜像问题，塞进来会让 S2 的范围翻倍 |
| D4 | `NULLS NOT DISTINCT`（PG 15+）唯一索引作为 `unique_key` | 只影响 PG 15+ 一小撮场景，v1 先按「列必须 NOT NULL」保守处理 |
| D5 | 基于 `unique_key` 的块级续跑 | 需要先实证「源端这组列真的唯一」，否则会漏行 |
| D6 | MySQL→MySQL 接通后补的 12 格矩阵（§2.4）+ 目标端增量改/删一格 | 方向未接通（`direction.go:39-46` 的 `pendingDirections`） |
| D7 | `transform` 与 `unique_key` 的冲突规则 | `transform` 本身只有契约没有实现 |
| D8 | `jinliang status` 的暂停展示（含 `skipped_rows`） | 已知债（契约 §1.8 片 D2）；**文案不许再把用户指去 status 找它** |
| D9 | MySQL 目标端 upsert **不指定冲突列**：命中任意唯一索引即可更新，可能与源端主键语义不一致 | 与「无主键表」相邻但不同源；v1 只在 `TARGET-02` 的 Detail 里点明，改 SQL 形状要连带改 5.7 的 `VALUES()` 变体，属独立一片 |

---

## §8 与现有文档的对齐（本文件落地时要改的段落）

| 文档 | 改什么 |
|---|---|
| `docs/SPEC.md` | `SCOPE-02` 补一行（四档）；`PG-06` 注明实现形态是 `TARGET-02`；`MY-08`/`MY-11` **不动** |
| `docs/ERRORS.md` | 加 `E1017` 一行（与 `codes.go` 同批） |
| `docs/DESIGN.md` §0 原则 6 | 「无主键表照跑（insert-only）」后面补「改/删 → 只停这张表 + 欠一次整表重搬；`unique_key` 可升级成 upsert」 |
| `docs/SNAPSHOT.md` §1/§4 | 无主键表两行补「整表重来不拖累其它表」与「`unique_key` 不改变切块」 |
| `docs/STREAMING.md` §7 | 把「只允许 insert-only 且必须开启定期全量校验」改成「insert-only；改/删触发按表暂停 + 整表重搬；或声明 `unique_key`」 |
| `docs/CONFLICT.md` §1 | 补「`unique_key` 不是主键，不进冲突判定」 |
| `docs/OPEN-QUESTIONS.md` #1 | 结论行加链到本文件 |
| `docs/CONNECTOR-SDK.md` | Catalog 加 `UniqueKeys`（§3.4） |
