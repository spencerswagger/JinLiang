# 夜班 2026-11-10 · MySQL 源接进引擎（①）+ 按表暂停的契约缺口（②）

分支 `feat/mysql-source-engine-resume` → PR **#89**（基于 main `67db571`）。
本地：MySQL 8.0.28（源 `jinliang_src`@3306；MySQL→MySQL 的第二个实例我起在 3307）、PG 17.11。

---

## ① MySQL 源接进 engine —— 结论与真数字

**先说结论**：`mysqlsource` 在 main 上**已经接进引擎主循环**（`internal/engine/run_source.go` 的
`myRunSource` + `verify.go:openSource`，随 #73 的接线重写一起落地），MySQL→PG 的
全量/增量/块级续跑在 main 上就是通的。今晚补的是「真跑起来才会撞上的三处接线错误」，
以及**缺的那半块验收证据**：MySQL 源「增量跑到一半 kill -9」的续跑（昨晚只有 PG 源那条）。

### 1.1 三处修复（都在 PR #89）

| # | 症状 | 根因 | 修法 |
|---|---|---|---|
| 1 | 目标库与源库同实例时，MySQL→MySQL 增量报 `Unknown column 'col_1'` | 变更流过滤只比**表名**，目标库里的同名表漏进来 → 读到自己的写入再写一遍；`binlog_row_metadata=MINIMAL` 没列名，目标库那张同名表不在 known 里，补不出真名 | 判等键改成 **schema + 表名**（`tableKey`），并写清为什么 |
| 2 | `--stop-when-idle` / 按停后**退出码 1、阶段 failed**、提示「未识别的数据库错误，请把日志发给维护者」 | 取消时引擎关连接打断阻塞读，MySQL 源把 `use of closed network connection` 当网络错误上抛 | mysqlsource：读失败先看 `ctx.Err()`，取消就返回取消；非 PG 目标端的流再加一格「是我们自己要停的」 |
| 3 | 目标表没有唯一键时，任务**永远停在「实时同步中」**，日志一个字都没有 | `batcher.flushLoop` 里 Apply 失败只 `return`，错误留在 goroutine 里；提交再也不发生、位点不前进、`--stop-when-idle` 等不到安静 | 记住第一个错误 + `onError` cancel 流；调用方在流结束后先查 `b.Err()`，报根因不报「流被打断」 |

三格单测 + 一格入 CI 的真库用例（见下）。

### 1.2 验收数字

**MySQL→PG（Go 集成用例 `TestMySQLSourceIncrementalResumeAfterSIGKILL`，本地 + CI 都绿）**

| 项 | 本地 | CI（b4-mysql-source） |
|---|---|---|
| 全量 | 20,000 行 | 20,000 行 |
| kill -9 时目标端 | 21,200 行（源端 21,850） | 21,200 行（源端 22,050） |
| 重启**重搬行数** | **0** | **0** |
| 重启耗时 | 6.19s | 6.10s |
| 追平后行数 | 21,850 | 22,050 |
| md5（两端一致） | `21850:d475e77b01884265c7cb99d5b2c72533` | `22050:646e63f479ab…` |

| 位点 | 只前进（`pos=170446344 → 170480108`），不回退 | 同 |

口径与 #74 的 PG 源那条逐字相同：**重搬 0 行**（不是「少搬」，是没重跑全量）、行数一致、
逐行 md5 一致、位点只前进。停机期间源端还插了 500 行、改了 100 行、删了 50 行 ——
这些只能由「重启后从落库位点重放」补上（全量的视图更早、跳过更补不了）。

> CI 那一列是 **`983c9fe`** 那轮的输出；head `fc78e93` 上同一用例实际是 `21,850` /
> `b0f56c…`（同一口径、同一条用例，数字随源端并发写入量浮动）。别把它读成头 sha 的数。

**MySQL→MySQL（`scripts/b1_mysql_to_mysql.sh`，两个实例：3306 源 / 3307 目标）**

| 场景 | 行数 | md5 | 耗时 |
|---|---|---|---|
| 全量 → 追平 | 20,000 | `20000:ea88a48349ef8c5d812bbfeb07a7855f`（两端一致） | 7.44s |
| 增量中途 kill -9 → 重启 | 19,996 | `19996:48e0c3aede558039f6a950160d83aab4`（两端一致） | **重搬 0 行**，5.28s |

**三种组合的第三个（PG→MySQL）本地跑不了**：共享 PG 实例上有别的项目在跑长事务
（`lingshu_dev`，反复起新 xid 的批量 INSERT），`CREATE_REPLICATION_SLOT … EXPORT_SNAPSHOT`
会等它结束（PG 的 slot 创建要等所有 xact 收尾，是**实例级**的）。
把 main 基线 stash 掉重跑同样卡住 → 与本 PR 无关，是环境。CI 上独占实例，b1-engine 是绿的。
命令与判据在 `scripts/b1_pg_to_mysql.sh`（full / kill / fullinterval 三模式）。

### 1.3 版本覆盖

沙盒里没有 docker，5.7 / 8.4 / 9.x 只能在 Actions 上跑。本 PR 的 `mysql-matrix` 四个腿全绿：

| 版本 | 用例 | 覆盖 |
|---|---|---|
| 5.7 | 132 PASS / 0 FAIL | 分块快照、复合主键、无主键、稀疏大键、BINARY/时间主键续跑、整事务流、重放幂等、DDL 加列、DDL 拒绝、租约、位点持久性闸门、行宽预检 |
| 8.0 | 132 PASS / 0 FAIL | 同上 |
| 8.4 | 132 PASS / 0 FAIL | 同上（`SHOW MASTER STATUS` 已移除） |
| 9.x | 132 PASS / 0 FAIL | 同上（`mysql:9` 镜像真跑到了，不是「未测」） |

### 1.4 自检

- `gofmt -l .` 空；`go vet ./...` 绿；
- `go test -race ./...` → **21 包 / 493 用例 / FAIL 0 / SKIP 0**；
- `go test -tags=integration ./internal/mysqlsource ./internal/mysqlsink` 绿；
- `./internal/engine -run TestMySQLSource` 三格绿（本地 26.6s）。

CI（PR #89）：`build` / `race` / `testmatrix` / `schema` / `cross-compile` /
`control-plane` / `b1-engine` / `b4-mysql-source` **8 个 job 全 success**；`mysql-matrix` 4 个腿全 success。

---

## ② DDL 改类型只暂停该表 —— 现状是「硬契约」，要改得 Ark 点头

**没有动代码**。原因：现在「其它表也停」不是疏漏，是**写死在三处的契约**：

1. `internal/pgsink/pgsink.go` `Apply` 开头：命中暂停表 → **整批拒绝**，注释写着
   「整条任务只有一个位点，跳过等于把这批变更永久丢掉」；
2. `internal/engine/integration_paused_test.go` ⑤：断言「位点没推进，这一批一条都不该落库：A=0 B=0」；
3. `notes/contract-paused-tables.md` §1.2：`isPausedLocked`「仅用于**留痕与整批拒绝**，**不用于跳过写行**」。

按要求「需要契约变更就写清改什么/为什么/影响谁」，所以这里只给证据与提案。

### 2.1 现状证据（今晚实测）

`TestPauseTableRegistersKeyKeepsPositionAndRecovers`（PG→PG，两张表，A 改类型 + A/B 各插一行）：
- 本轮失败并挂 E1015，`_jl_meta.paused_tables` 只登记 A（key = 裸表名，与 `_jl_chunk.table_name` 逐字节相同）；
- 位点前后**逐字节相等**；
- **B 的那一行也没进目标端**（A=0 / B=0）——这就是「整任务停」的具体形状；
- 把目标端 A 的列类型对齐后重跑：解除暂停、从原位点重放，两张表指纹一致（含 B 的那一行）。

也就是说：**「暂停只登记该表」已经做到了，「其余表继续同步」没有**，恢复路径是有的。

### 2.2 要改什么（给 Ark 的三个选项）

**A. 位点前进 + 恢复时整表重搬（推荐）**
- 写路径：命中暂停表的行**跳过**，其它表照写，位点正常前进；暂停记录里加 `resume: recopy`。
- 恢复：解除暂停时把该表 `_jl_chunk` 标记清掉 → 下一轮从块边界整表重搬（块级续跑已具备）。
- 为什么推荐：不需要「按表位点」这种新状态；块级续跑（#74）已经能保证重搬便宜且可中断。
- 代价：解除暂停后该表要重搬一遍（行数大 = 一次全量）；重搬期间该表目标是部分数据。

**B. 按表位点**
- `_jl_meta` 之外再加每表位点（`_jl_table_pos`），恢复时从该表自己的位点重放。
- 代价：位点从「唯一权威一条」变成「一表一条」，崩在一半会出现「部分表前进、部分表没推进」，
  `DECISIONS #1/#2`（位点与数据同事务 / 目标库位点是唯一权威）要重新论证一遍。

**C. 维持现状，只把文案说清楚**
- 代价：用户要的是「一张表坏了别的表继续跑」，这一点做不到；如果今晚不打算动语义，
  至少把 `PAUSE-01` 的文案改成「整条任务会停下」，别让人以为只停了一张表。

**影响谁**：`pgsink.Apply` / `mysqlsink.Apply` 写路径、`_jl_meta.paused_tables` 形状（选项 A 加一个字段）、
`paused_reconcile` 的解除判据、`status`/`doctor` 展示、`integration_paused_test.go` 的 ⑤ 断言、
`notes/contract-paused-tables.md` §1.2。引擎对外接口与状态机不用动。

**我倾向 A**：改动面最小、和 #74 的能力对得上，且不需要推翻「位点唯一」这条根。

---

## ③ #59 合并门禁

PR #59 已于 **2026-11-07 02:08 合入**（`state=closed, merged_at` 非空），工作流 `review-gate.yml` 在。
今晚实测（`gh workflow run review-gate.yml -f pr=89`，run `42846214534`）：
- 判定逻辑自测全 PASS；
- 对本 PR 的输出是 `review-gate: FAIL PR 还没有 review（CI 绿不等于可合并）` +
  `::warning::review-gate 未通过（advisory，不挡合并）` —— **fail-closed 且走 advisory**，
  正是 #59 设计的形状。转 required 只需删 `continue-on-error`。

---

## 遗留 / 环境坑（写下来别再踩）

- 沙盒里**后台进程不跨 execute 调用存活**：起第二个 mysqld（3307）必须和 e2e 在同一个调用里，
  脚本见 `.work/run_m2m_local.sh`（未入库，`.work/` 在 gitignore）。
- 被 kill 的 Go 测试会留下**半开的复制连接**：PG 那边 slot 显示 `active`，新用例报
  `replication slot … is active for PID`；MySQL 那边表现为下一次 `CREATE_REPLICATION_SLOT` 等锁。
  清理：`SELECT pg_terminate_backend(pid) FROM pg_stat_replication;` + 再 drop 槽。
- `TestMySQLTargetFullCopyAndIncremental` 单跑会卡在 `CREATE_REPLICATION_SLOT`——**不是它坏了**，
  是共享 PG 上有别的项目的长事务。
- MySQL 目标端的 `--stop-when-idle` 以前会「等不到安静」：源头是 flushLoop 吞错误（本次已修）+
  同实例自写自读（本次已修：按库过滤）。
