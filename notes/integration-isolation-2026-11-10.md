# 集成测试的顺序/状态依赖：三处共享资源 + 逐处证据（2026-11-10）

触发：Core 报「`TestMySQLSourceFullCopyAndIncremental`、`TestRecopyDoesNotRepeatCleaning`
整包跑红、单跑绿」，判断是「反复跑留下的状态」。独立复核结论：**方向对，但不是一个原因，是三个**；
性质都是**测试隔离**，不是生产代码把状态放错位置（下面有反证实验）。

复现命令（沙盒本地 PG 17.11 `wal_level=logical` / `max_replication_slots=10`，MySQL 8.0.28）：

```
go test -count=1 -tags=integration ./internal/...        # 整棵树、包级并行
go test -count=1 -tags=integration -p 1 ./internal/...   # 整棵树、包级串行
```

## 一、共享资源 #1：逻辑复制书签（实例级，默认上限 10）——**主因**

一轮 `./internal/engine/` 用例会建 **6~10 个**书签；而书签是**实例级**资源
（`max_replication_slots`，PG 默认 10），不还就给后面的用例断粮。

实测泄漏点（`internal/engine`）：

1. `resumeFixture.dropJobSlots`（teardown）第一行就是 `if f.key == "" { return }`——
   `f.key` 只有调用过 `jobKey()` 的用例才有，其余用例**书签全留下**；
2. 同一函数只删 `NOT active` 的书签——被 kill -9 的子进程还挂着的 active 书签**永远删不掉**；
3. 用 `engine.Run()` 直接跑的用例（`integration_recopy_test.go` 里那一批）压根没有回收钩子。

证据（同一台库、同一个包）：

| 阶段 | 命令 | 结果 |
|---|---|---|
| 修前 | 清空书签后跑一次 `-p 1 ./internal/engine/` | 跑完**残留 6 个**书签，本轮内 5 条用例红 |
| 修前 | `-p 1 ./internal/...`（整棵树） | 日志里 `all replication slots are in use (SQLSTATE 53400)` **19 次** |
| 修后 | 连跑两次 `-p 1 ./internal/engine/` | 残留 **0~2 个**，`53400` **0 次** |

所以「整包红、单跑绿」不是用例之间的顺序依赖，而是**资源没还**：单条用例最多用 1~2 个书签
（在 10 以内，绿），整包用完就把实例预算吃光，第二轮从第一条开始红。

顺带发现（**用户侧文案缺口**，本 PR 不改）：`SQLSTATE 53400` 在引擎里是
`未识别的数据库错误：这条错误还没分类`——而错误提示里恰好把「书签数已满（max_replication_slots）」
列为三大常见原因之一。也就是说，用户真撞上这一条时，看到的是「未识别」，不是「书签满了」。

## 二、共享资源 #2：目标库 `public._jl_*` 状态表（库级，但被当草稿纸）

`internal/engine` 与 `internal/pgsink` 都用默认 schema `public`，而 engine 用例里到处是
`DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance`。
一旦两个包并行（默认 `-p`），或者包内残留 goroutine 没退，就会出现：

- `记录块进度失败: ERROR: relation "public._jl_chunk" does not exist (SQLSTATE 42P01)`
- `准备目标表失败: ERROR: relation "public._jl_object" does not exist (SQLSTATE 42P01)`
- `清掉的租约持有者="del-recreate#…#…"（进程还活着=true）` / `租约被别的实例接管了`

修完 #1 之后仍有 3 处 `42P01`，全部属于这一类。

## 三、共享资源 #3：MySQL 源库整库（库级，且 mysqlsource **全库枚举**）

`mysqlsource` 的 Snapshot 枚举**整个源库**，而 `verify`/`doctor`/`engine` 的 MySQL 用例在
同一个库里建/删自己的表（`jl_verify_orders`、`engine_mysql_orders`）。于是：

- `Error 1412 (HY000): Table definition has changed, please retry transaction`
- `Error 1146 (42S02): Table 'jinliang_src.jl_verify_orders' doesn't exist`

nightly 的 `integration` job 早就用「逐包串行调用」绕开这一条（注释里写了）。

## 四、判性：测试隔离，不是生产状态错位

反证（都是刚跑的）：

- 三个被点名的用例，**在干净状态下单跑全绿**：
  `TestDeleteRunningJobThenRecreateRuns` ok 10.3s、`TestRecopyDoesNotRepeatCleaning` ok 7.1s、
  `TestMySQLSourceFullCopyAndIncremental` ok 11.5s（每次先清书签 + 清 `public._jl_*` + 清 MySQL 表）。
- 把目标端 `public._jl_*` 全删掉再单跑 `TestMySQLSourceStopAfterCancelsPromptly`：**ok 2.4s**
  ——「首跑/干净库」不是坏的，坏的是**别人同时把表 DROP 掉**。

结论：`_jl_*` 放在目标库、书签按 jobKey 命名、mysqlsource 枚举整库，这三条都是**有意的生产语义**；
出问题的是**测试把「一个部署 = 一个目标库」的假设，套在「所有包共用同一个库」上**。
按派工口径：属测试隔离问题 → 修隔离，不用测试改动掩盖生产问题。

## 五、这个 PR 做了什么 / 没做什么

做（`internal/engine`，只动测试代码）：

- 新增 `trackJobSlots`：每条用例建完任务就挂 `t.Cleanup`，按 `job.JobKey` 反查书签名（`jl_<key>%`）并回收；
- `dropSlotsLike`：收尾时先 `pg_terminate_backend` 踢掉挂着的读者，再删书签（原来只删 `NOT active` 的）；
- `resumeFixture.dropJobSlots`：`f.key` 为空时从控制库按任务名反查 `job_key`，不再直接 return；
- `TestMain`（`main_integration_test.go`）：包开跑/收尾各回收一次源库里**空闲**的 `jl_%` 书签，
  把上一轮泄漏的清掉——这样「连跑两次」才是可重复的。

两条使用约束（写给以后加用例的人，别踩）：

1. **`JINLIANG_TEST_PG_SRC_DSN` 必须是测试专用库，回收默认关**：`reclaimSlots` 删的是源库里
   **所有空闲的 `jl_%` 槽**，所以现在**只有显式设 `JINLIANG_TEST_RECLAIM_SLOTS=1` 才回收**（`reclaimEnabled()`）。
   CI 的集成步骤里显式打开（那里每个 job 自己 `docker run` 一套 PG、独占）；本地默认不回收，
   避免把别人的空闲槽一起收走——对方下一次连流会报槽不存在，更糟的是流上被 `pg_terminate_backend`
   打断成 `57P01`（已在共用 PG 上真实发生过）。
2. **新加的 fixture 必须自己对 `trackJobSlots`**：`TestMain` 只在包首尾兜，救不了**轮内**泄漏。
   新用例（比如 MySQL 源、resync 那些同样会建槽的）忘了挂，就会在同一轮里重新吃满 10 个槽。

将来把 §6 第 1 条落成门禁时，判据用「残留 ≤ 2 **且** `53400` = 0 次」，别写「= 0」（实测就是 0~2，写死会让新门禁自己 flaky）；
断言要**独立成一步**（`TestMain` 只能清、不能判红）。

没做（留给下一步，别混进这个 PR）：

- `public._jl_*` 的按包隔离（要给 pgsink 传 `Options.Schema`、给 engine 传 `TargetDoc.Schema`，
  并且 engine 用例里几十处未限定表名的断言要跟着改 search_path）；
- MySQL 按包独立库（`mysqlsource` 的 DSN 要派生库名，`verify`/`doctor`/`engine` 同样）；
- **整包顺序门禁**（`go test -count=1 -tags=integration -p 1 ./internal/...` 进 CI）。
  原因写在上面：#2/#3 没修完之前这道门一定是红的，现在加进去等于把门禁挂成常红。

## 六、复现用的判据（下一步验收同一套）

```
# 1) 书签不泄漏
清空书签 → go test -count=1 -tags=integration -p 1 ./internal/engine/ → 再查 pg_replication_slots = 0
# 2) 整棵树、包级串行、连跑两次全绿（当前仍红：42P01 + MySQL 1412/1146）
go test -count=1 -tags=integration -p 1 ./internal/...  ×2
# 3) 整棵树、包级并行（最终目标，需要 #2/#3 修完）
go test -count=1 -tags=integration ./internal/...
```
