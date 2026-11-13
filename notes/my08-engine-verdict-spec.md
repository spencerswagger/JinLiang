# MY-08 唯一定义口径（Ark · 2026-11-13 夜班任务 2/2 上半场）

**一行结论**：MY-08 = 「源端这张表的存储引擎**有没有事务**」这一个能力项，判据取**服务器自报的
`information_schema.ENGINES.TRANSACTIONS`**，三态 `transactional / non_transactional / not_judged`；
`MyISAM`、`MRG_MYISAM`、`MEMORY`、`CSV`、`ARCHIVE`、`BLACKHOLE`、`FEDERATED` **一律降级（warn 照跑 + 收尾标注）**，
不拒绝、不「暂不判」；引擎名查不到也是降级（**读不到能力 ≠ 有事务**）；只有「非 MySQL 源」与「没读到表清单」是 `not_judged`（不冒充 ok）。

对齐基线：`origin/main` = `6f5e2d1`；代码基线 = PR #126（`feat/myisam-preflight-degrade` @ `58bc6f9`，未合并）。

---

## 1. 为什么把判据从「引擎名含 MYISAM」换成「服务器自报事务能力」

今天 main（`6f5e2d1`）上 MY-08 的实现是 `strings.Contains(strings.ToUpper(t.Engine), "MYISAM")`
（`internal/preflight/b0.go`，`#113`/`#114` 引入）。这条判据有两处硬伤：

1. **已经在跑的假绿**：`MEMORY` / `CSV` / `ARCHIVE` / `BLACKHOLE` 同样没有事务，但名字里没有 `MYISAM` →
   判据返回「有事务」→ 不发卡 → `全量完成 + 退出码 0`，用户以为拿到了一致性切面。
   （`FEDERATED` 是 `TRANSACTIONS = NULL`，连黑名单都写不进去。）
2. **永远追不上**：黑名单要我们逐个版本去补；`SHOW ENGINES` / `information_schema.ENGINES`
   里服务器自己有一列 `TRANSACTIONS`，这是现成的能力探测。硬规则 5「能力探测而不是版本号分支」的同一条道理：
   **能力探测，不是名字猜测**。

实测（本机 MySQL 8.0.28，`information_schema.ENGINES` 原样）：

```
ENGINE              TRANSACTIONS  SUPPORT
ARCHIVE             NO            YES
BLACKHOLE           NO            YES
CSV                 NO            YES
FEDERATED           NULL          NO
InnoDB              YES           DEFAULT
MEMORY              NO            YES
MRG_MYISAM          NO            YES
MyISAM              NO            YES
PERFORMANCE_SCHEMA  NO            YES
```

于是判据退化成一个查表：`TRANSACTIONS = 'YES'` 才有事务，`NO` / `NULL` / 空 / 表里没有这个引擎名 → 没有事务。
不需要维护任何引擎名单，将来 MyRocks、插件引擎、MySQL 9 新增引擎自动落对。

---

## 2. 逐条对齐表（Core 照这张表落码）

| # | 能力项（`capability.go` / 契约里的名字） | SPEC.md MY-08 结论 | `EngineVerdict` | 引擎 → 期望判定 | 一句话验收断言 |
|---|---|---|---|---|---|
| 1 | `source.Catalog.Engines map[string]bool`（**新增**，键=大写引擎名，值=`TRANSACTIONS='YES'`） | 「以服务器 `information_schema.ENGINES` 为准」 | —（判据的输入） | 8.0.28：`INNODB=true`、`MYISAM=false`、`MRG_MYISAM=false`、`MEMORY=false`、`CSV=false`、`ARCHIVE=false`、`BLACKHOLE=false`；`FEDERATED` 键可缺（`NULL`→ `false`/不填） | 真库：`cat.Engines["INNODB"]` 为 true 且 `["MYISAM"]` 为 false；探针读不到时 map 为空且**不报错**（不阻断，同 `SCOPE-01`） |
| 2 | `preflight.JudgeEngines`（唯一判据，`internal/preflight/engine.go`） | 「唯一入口，预检卡片与收尾都只读它」 | 三态 | `InnoDB` → `transactional` | 只选 InnoDB 表：`Verdict==EngineTransactional`、不发 MY-08、`CanStart` |
| 3 | 同上 | 「MyISAM 系降级」 | `non_transactional` | `MyISAM` → 降级；`MRG_MYISAM`（TABLES 里可能回报 `MRG_MYISAM` 大写）→ 降级 | `Verdict==EngineNonTransactional`、`Names==["legacy"]`、`Tables==["legacy(MyISAM)"]`；`MRG_MYISAM` 小写输入同样降级（大小写不敏感） |
| 4 | 同上 | 「一切无事务引擎同档：降级（不是拒绝、不是暂不判）」 | `non_transactional` | `MEMORY` / `CSV` / `ARCHIVE` / `BLACKHOLE` → 降级 | 四引擎同批：`Verdict==EngineNonTransactional`、`Tables` 含 `m(MEMORY)` 等四条、`SQL` 里各有 `ALTER TABLE … ENGINE=InnoDB;` |
| 5 | 同上 | 「引擎名查不到也降级」 | `non_transactional` | `Engines` 里没有的引擎名（如 `ROCKSDB`）→ 降级 | `Verdict==EngineNonTransactional` 且 `Reason` 含「服务器没告诉我们」；断言**不许**退化成 `transactional` |
| 6 | 同上 | 「读不到能力 ≠ 有事务」 | `non_transactional` | `SourceCat.Engines == nil` 但表是 `InnoDB` → 降级 | `Verdict==EngineNonTransactional`（这是**故意**的：宁可多一句提示，也不许一次假绿） |
| 7 | 同上 | 「非 MySQL 源不判」 | `not_judged` | PG 源 → 不判 | `Verdict==EngineNotJudged` 且 `Verdict != EngineTransactional`（不冒充 ok）、不发 MY-08 |
| 8 | 同上 | 「没读到表清单不判」 | `not_judged` | `SourceCat==nil`（MySQL 源）→ 不判 | 同上；`SCOPE-01` 负责「表在不在」那一半 |
| 9 | 未选中表不误伤 | 「只判选中的表」 | `transactional` | 选中的只有 `InnoDB`，库里有 `MyISAM` 但未选 | 不发 MY-08；`Names` 为空 |
| 10 | `preflight.EngineDegradeNote`（唯一文案） | 「降级、非一致性切面」四段 | — | 任意 `non_transactional` | Detail 同时含：`降级`、`不是一致性切面`、`verify`、`ALTER TABLE`；含 `<表>(<引擎>)` 实际引擎名 |
| 11 | MY-08 卡片 Title/Status | 「部分表用无事务引擎」+ warn | — | 只有 `MEMORY` 表时 | `Status==warn`、`CanStart==true`、Title/Detail **不出现** `MyISAM`（否则标题就是假话） |
| 12 | `runner.degradeNote` → `Result.Note`（收尾「说明」） | 「预检说了、收尾还要说一次」 | — | `non_transactional` | `Result.Note` 含 `降级`；`transactional` 时 `Note` 一个字不加（不为了安全给每次跑都塞一句话） |
| 13 | 引擎特有代价（`engine.go` 里一张 `map[引擎]一句话`） | 「各多一句自己的代价」 | — | `MEMORY` / `CSV` / `ARCHIVE` / `BLACKHOLE` / `FEDERATED` | 命中才追加：`MEMORY` → 含「重启即空」；`CSV`/`ARCHIVE` → 含「不支持索引」+ 指 `SCOPE-02`；`BLACKHOLE` → 含「读永远 0 行」；`FEDERATED` → 含「表在远端」；`InnoDB` 不加 |

**没有 `MY-x` 新号**：MY-08 保号、改口径，不新增检查项（`MY-09`/`MY-10` 是保留位，`MY-11` 是 MariaDB，下个可用源端号仍是 `MY-12`）。

---

## 3. MEMORY / CSV / ARCHIVE（外加 BLACKHOLE / FEDERATED）为什么是「降级」

**先否掉另外两档：**

- **拒绝（blocked）不成立**：三张表 8.0.28 上都能建、能读、能搬（实测见 §5）。缺的只是「同一时间点」。
  `DESIGN.md` 原则 6 只允许两类地方停：物理上做不到、安全底线不对。这里两条都不沾。
- **「暂不判」不成立**（这是本任务要消灭的那一格）：`not_judged` 的正确含义是「**没看**」（源不是 MySQL、没读到清单），
  而这三张表是「**看了，并且知道它们没有事务**」。把它们放进 `not_judged` 等于让它们悄悄走正常路径——
  空白就是下一个假绿。另外 Core 的实现里 `not_judged` 不发卡，所以「暂不判」在用户那儿等价于「没问题」。

**归入 `non_transactional`，但每张表多一句自己特有的代价**（不能并成一句 MyISAM）：

| 引擎 | 额外代价（实测/文档依据） |
|---|---|
| `MEMORY` | 整表在内存，实例重启/崩溃即空。源端变空后目标端仍留着旧行，**要重搬才收敛**（增量里没有 delete 事件承载它） |
| `CSV` | **不支持任何索引**：8.0.28 实测 `ALTER … ADD PRIMARY KEY` → `ERROR 1069 max 0 keys allowed` → 这类表**恒无主键** |
| `ARCHIVE` | 同样无索引（实测 `ADD PRIMARY KEY` → `ERROR 1030 … Unknown error -1`），且**不支持可空列**（实测建表 `ERROR 1178`）、只能 `INSERT`/`SELECT` |
| `BLACKHOLE` | 写入即丢、**读永远 0 行**：「全量完成 0 行」对它成立，但没有意义 |
| `FEDERATED` | 表在远端；位点是本机变更流的位置，两者不保证同一时间点（`TRANSACTIONS` 列是 `NULL`，本机 `SUPPORT=NO` 未安装） |

**与 `SCOPE-02` / `TARGET-02` 的分界（不许合并）**：`CSV`/`ARCHIVE` 恒无索引 ⇒ 恒无主键，那个结论由
`SCOPE-02`（源端身份）与 `TARGET-02`（目标端唯一键）给。两卡同时出现时：
`interval` + 无主键 = **blocked**（以更严的那张为准），`once`/`full_interval` 下 `SCOPE-02` 给 ok 而 MY-08 仍是降级。
MY-08 只说「有没有事务」这一半，并在 Detail 里引一句「本表也没有主键，另见 `SCOPE-02`/`TARGET-02`」。
这与 `PG-06`／`TARGET-02` 的分界是同一条规矩（源端一半、目标端一半，别混读）。

---

## 4. 四个易踩的边界（写进 SPEC，防止下次改回去）

1. **`not_judged` 不是 ok**。它是「没判」，调用方拿到它不许发 ok，也不许当成「没问题」。今天 `JudgeEngines` 已经这么写，
   SPEC 要把它写成契约，否则下一个人会把 `not_judged` 折成 ok 去「减少噪音」。
2. **查不到 ≠ 有事务**（第 2 条的反面）。空 map / 未知引擎名一律降级。代价是探针彻底失败时每张表都多一句提示——
   这个代价是**故意**付的：宁可多一句，也不许一次假绿。
3. **档位与 `mode` 无关**。`once` 也不是一致快照（读取没有 read view，且 `once` 同样会存快照起点位点），
   `interval` 更没有一致性切面。所以 MY-08 **不按 `mode` 分档**——别照抄 `SCOPE-02`/`PAUSE-01` 的四档分流，
   那两条的模式差异是真的（写路径不同），MY-08 的模式差异是假的。
4. **只判源端**。目标端 MySQL 由 `mysqlsink` 一律建 `ENGINE=InnoDB`（`internal/mysqlsink/schema.go`，硬编码），
   目标端引擎不进 MY-08；`MySQL→MySQL` 方向本身还在待接通（`D6`）。

---

## 5. 实测记录（本机 MySQL 8.0.28，`jinliang_src`）

```
CREATE TABLE … ENGINE=MEMORY / CSV / ARCHIVE / BLACKHOLE / MyISAM / InnoDB   -- 全部成功（ARCHIVE 要求所有列 NOT NULL）
SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES
  → zz_arc2 ARCHIVE / zz_bh BLACKHOLE / zz_csv CSV / zz_innodb InnoDB / zz_mem MEMORY / zz_myisam MyISAM / zz_mrg MRG_MYISAM
ALTER TABLE zz_csv   ADD PRIMARY KEY (id) → ERROR 1069 (42000): Too many keys specified; max 0 keys allowed
ALTER TABLE zz_arc2  ADD PRIMARY KEY (id) → ERROR 1030 (HY000): Got error -1 - 'Unknown error -1' from storage engine
ALTER TABLE zz_bh    ADD PRIMARY KEY (id) → 成功（有 PRIMARY）
ALTER TABLE zz_mem   ADD PRIMARY KEY (id) → 成功（有 PRIMARY）
CREATE TABLE … (id INT NULL, …) ENGINE=ARCHIVE → ERROR 1178: The storage engine for the table doesn't support nullable columns
```

另：**引擎名大小写不统一**——`information_schema.TABLES.ENGINE` 回报 `MRG_MYISAM`（全大写），
而 `information_schema.ENGINES.ENGINE` 也是 `MRG_MYISAM`，但历史代码/文档里写的是 `MRG_MyISAM`。
查表一律 `strings.ToUpper` 归一化，别按原样取键。

---

## 6. 实现清单（给 Core，按 §2 的表落）

1. **契约**（`internal/source/contract.go`）：`Catalog` 加 `Engines map[string]bool`（`json:"engines,omitempty"`），
   注释写清「键=大写引擎名；值=`TRANSACTIONS='YES'`；键不存在=服务器没说 → 按没有事务处理」。
   不放进 `CapSet`：那是**服务器级**能力键，引擎事务性是**逐表**的，塞进去会逼出一个按引擎名的键空间。
2. **两处 Probe 都要填**（这是本次最容易漏的一格）：
   - `internal/connect/mysql.go` 的 `Catalog()`（预检/控制面走这条）；
   - `internal/mysqlsource/snapshot.go` 的 `Catalog()`（引擎与快照走这条）。
   只改一处 → 会出现「预检说降级、收尾不标注」（或反过来）。查询一条：
   `SELECT ENGINE, COALESCE(TRANSACTIONS,'') FROM information_schema.ENGINES`。
   读失败**不阻断**（与 `SCOPE-01` 同口径），留空 map → 全部落 `non_transactional`（第 6 格）。
3. **判据改写**（`internal/preflight/engine.go`）：删掉 `strings.Contains(…,"MYISAM")`，改成
   `tx, known := cat.Engines[upper(t.Engine)]`；`known && tx` → 事务；其余（`!known`、`!tx`）→ 无事务。
   `Reason` 分两句：「<引擎> 没有事务」 vs 「服务器没告诉我们 <引擎> 有没有事务」。
4. **文案**：`EngineDegradeNote` 的「（MyISAM）」要改成实际引擎名（`MEMORY` 表说 `MyISAM` 就是假话），
   卡片 Title 从「部分表用 MyISAM 引擎（无事务）」改成「部分表用无事务引擎」；再加 §3 那张「引擎特有代价」的 `map`。
5. **单测**：§2 表的 13 行各一条，重点是第 5/6 格（未知引擎、空 map **不许**变成 ok）。
6. **集成**（真库）：`mysqlsource/integration_myisam_test.go` 扩到 `MEMORY`/`CSV`/`ARCHIVE` 三张
   （注意 `ARCHIVE` 不接受可空列、`CSV` 不能建索引），断言 MY-08 点名三张 + `InnoDB` 不被点名 + `CanStart`；
   另断言 `cat.Engines["INNODB"]==true`、`["MYISAM"]==false`（探针接线断了就红）。
7. **P3 建议（顺手，可不做）**：代码注释里 `docs/SPEC.md:107` 这类**行号**引用已经过期（实际是 113 行，
   而且这一改还会漂）。改成锚点（`SPEC.md §3 MySQL 表的 MY-08`）比行号耐用。

---

## 7. SPEC.md 最终文字（已落地到 `docs/SPEC.md`）

见本分支 `docs/SPEC.md` §3「### MySQL」表里的 MY-08 行 + 紧随其后的
`#### MY-08（表的存储引擎）：降级口径` 整段（表格行 + 判据三条 + 四段文案 + 引擎特有代价表 + 措辞约束）。

同时改了 `docs/DESIGN-NO-PRIMARY-KEY.md` 两处（只做一致性，不改该设计本身）：
- §2.1 探测项落点表里 `MY-08` 那行：口径改成「有没有事务」、覆盖引擎名、现状指向 `preflight/engine.go` 的 `JudgeEngines`（`#126`）；
- §3.2 加一句：`CSV`/`ARCHIVE` 恒无索引 ⇒ 恒无主键，两张卡各管一半、`interval` 下以 `SCOPE-02` 的 blocked 为准。

## 8. 不做的（留档，避免下次重复讨论）

- **不加 `FLUSH TABLES WITH READ LOCK` 覆盖整段全量**：那才是 MyISAM 真正的一致性切面，但要整段停写（不是取位点时那毫秒级），
  大表上是不可接受的停机。宁可「照跑 + 明说」，也不偷偷加锁、也不拒绝。SPEC 里写清了这条取舍。
- **不动 `DESIGN.md` 原则 6 / `README.md`** 的「MyISAM 照跑（标注无事务）」：新口径是它的超集（「无事务」现在指能力，不再指某一个引擎名），
  没有与 main 相反的陈述。
- **不给 MY-08 开子编号**（`MY-08a` 内存表…）：一张卡 + 逐引擎一句代价足够；编号膨胀会让人以为要多确认几次。
