# #59 合并门禁收口：能拦住错误合并的门禁（2026-11-10 夜）

基线 `origin/main c7e4f2e`（#89 已合）。分支 `jinliang/merge-gate-59` → PR **#92**。

## 0. 一句话

门禁原来只证明「测试跑绿了」。这次补的是反向证据：**把实现或判据改坏，门禁必须红**。
10 条注入（7 条判据 + 3 条实现）全部被抓住，同一条命令也跑三组合验收（行数+md5+耗时）。

## 1. 门禁入口

```bash
scripts/merge_gate.sh --pr 92          # 合并前（含 review 锚定）
scripts/merge_gate.sh --db             # 加「注入实现 → 用例必须红」（要真库）
scripts/merge_gate.sh --acceptance      # 加三组合验收（要 MySQL + PG）
scripts/merge_gate.sh --all --pr 92     # = CI 的 merge-gate workflow
```

退出码：**0 放行 / 1 拦截 / 2 用法或环境缺失（同样不放行）**。
任何一层红 → 一行 `MERGE-GATE: FAIL（n/4 层不通过）`，完整输出在 `.localdb/merge-gate.log`。

四层：判据自检 → review 自检 → 反向注入（logic）→ review 锚定；`--db` 追加实现注入，
`--acceptance` 追加三组合矩阵。

本地实跑（MySQL 8.0.28 / PG 17.11）：

```
$ scripts/merge_gate.sh --db
| gate_checks selftest | PASS | selftest: 19 条判据全部符合预期 |
| review_gate_logic selftest | PASS | … |
| mutation: logic 档 | PASS | mutation-gate: 7/7 条注入全部被抓住 |
| mutation: engine 档（注入实现，用例必须红） | PASS | mutation-gate: 3/3 条注入全部被抓住 |
MERGE-GATE: PASS（4 层全绿）
```

## 2. 反向注入必败断言表

`scripts/mutation_gate.py`：注入只发生在**影子拷贝**里（工作区一个字节不改），每条单独起一份
影子，注入点要求「恰好命中 1 处」——多处命中直接判 INJECT-FAILED，避免「改错了地方还
以为测到了」。判定：注入后检测器**退出非 0** = 被抓住（killed）；仍然绿 = 存活（门禁放行
了错误实现）→ 脚本退出 1。

| # | 名称 | 注入点 | 期望失败信号 | 实际（rc / 关键输出） | fail? |
|---|---|---|---|---|---|
| 1 | fp-compare-neutered | `gate_checks.py` 的 `if a[1] != b[1]:` → `if False:` | `gate_checks.py selftest` 红（「fp md5 不同」） | rc=1，`1/19 条判据不符合预期` | 是 |
| 2 | rows-count-neutered | `gate_checks.py` 的 `if src != expect:` → `if False:` | selftest 红（「rows 源少一行」） | rc=1，`FAIL rows 源少一行 —— 期望 拦截，实际 通过` | 是 |
| 3 | recopy-check-neutered | `gate_checks.py` 的 `if copied > allow:` → `if False:` | selftest 红（recopy 1 / 整轮两条） | rc=1，`2/19 条判据不符合预期` | 是 |
| 4 | pause-keys-neutered | `gate_checks.py` 的 `if got != exp:` → `if False:` | selftest 红（多/少登记两条） | rc=1，`2/17 条判据不符合预期` | 是 |
| 6 | review-deny-word-weakened | `review_gate_logic.py` 的 `DENY` 正则 `[1-9]` → `[8-9]` | `review_gate_logic.py selftest` 红（mustfix-2 / history-zero） | rc=1，`必改 2 条` 被误放行 | 是 |
| 7 | review-stale-accepts | `review_gate_logic.py` 的 `if anchor.get("production_touched"):` → `if False and …` | selftest 红（production-changed / test-hand-added） | rc=1 | 是 |
| 8 | pause-filter-keeps-all | `pause_skip.go` 的 `if _, paused := …; !paused {` → `true \|\| !paused` | `TestPauseSinkFilterDropsOnlyPausedTableRows` FAIL | rc=1，`应只剩 Begin / B 的 INSERT / Commit，得到 6 条` | 是 |
| 9 | position-not-persisted | `pgsink.setPositionTx` 首行 `return nil` | `TestPositionPersistedWithData` FAIL | rc=1，`位点没落盘：期望 postgres:v1:0/AA` | 是 |
| 10 | chunk-resume-degraded | `resume.planCopy` 的 `p.Kind = copyResume` → `copyFresh` | `TestPlanCopy` FAIL | rc=1，`结论错：想要 resume，得到 fresh` | 是 |

结论：**9/9 被抓住**（logic 6/6、engine 3/3）。

一条**未被抓住**的注入（记成缺口，不藏）：把 `engine.registerPause` 首行改成 `return nil`
（不登记暂停）后，`TestPauseTableRegistersKeyKeepsPositionAndRecovers` 仍然是绿的。原因是
裁决 A 之后这条用例的主要断言变成了「只跳该表 + 位点前进 + 对齐后整表重搬」，
**「登记失败必须整批失败」这条没有被端到端盯住**（`pauseSink.Apply` 里那条 `return rerr`）。
本门禁没有把它塞进必过清单（塞进去会常红），而是写在这里交给 Core 补一格用例。

两条设计取舍（踩过）：
- 「块进度读不出来」第一版注入在 `pgsink.DoneChunks`，用重量级集成用例 `TestChunkResumeAfterSIGKILL`
  当检测器，**跑出过一次 SURVIVED**（用例受残留状态影响，重跑绿）——门禁自己会 flaky 就不能上线。
  改成注入 `planCopy` + 单元用例 `TestPlanCopy`（毫秒级、纯函数），确定性才有保证。
- 判据（md5/行数/0 行重搬/暂停位点）从 shell 里搬进 `gate_checks.py` 的纯函数，就是为了让
  「把 `!=` 改成 `==`」这类改动能被 selftest 立刻照出来；shell 里的 `[ "$a" = "$b" ]` 没有这个待遇。

## 3. 组合矩阵（行数 / md5 / 耗时）

`scripts/acceptance_matrix.sh --rows 20000 --kill`，每格一行
`RESULT combo=… rows=… md5=… seconds=… status=…`。

本地（MySQL **8.0.28** / PG 17.11，`--kill`）：

| 组合 | MySQL | 行数 | md5 | 耗时 | 备注 |
|---|---|---|---|---|---|
| MySQL→PG | 8.0.28 | 21995 | `83646f7706a8dd35af431553dd003cf3` | 6.43s | 增量追平 0.13s；kill -9 后**重搬 0 行**、重启 6.43s |
| PG→MySQL | 8.0.28 | 20000 | `56f0ee0aa328b9411c44a0405d294dad` | 2.41s | 本版只全量；增量**明确拒绝**（断言） |
| MySQL→MySQL | 8.0.28 | 20000 | `56f0ee0aa328b9411c44a0405d294dad` | 2.35s | 本版只全量；增量**明确拒绝**（断言） |

CI（PR #92 的 `merge-gate` workflow，`rows=20000`，MySQL→PG 带 `--kill`）：

跑在 `merge-gate` workflow（run `42870646277`，结论 success）：

| 组合 | MySQL 5.7.44 | MySQL 8.0.46 | MySQL 8.4.13 |
|---|---|---|---|
| MySQL→PG（全量+增量+kill -9） | 21995 行 / `83646f77…` / 6.39s（追平 0.20s，重启**重搬 0 行**） | 21995 / `83646f77…` / 6.40s（0.20s，重搬 0 行） | 21995 / `83646f77…` / 6.40s（0.21s，重搬 0 行） |
| PG→MySQL | 20000 / `330ccea0…` / 0.29s（增量 ok） | 20000 / `330ccea0…` / 0.38s（增量 ok） | 20000 / `330ccea0…` / 0.32s（增量 ok） |
| MySQL→MySQL | 20000 / `330ccea0…` / 0.26s（增量 ok） | 20000 / `330ccea0…` / 0.37s（增量 ok） | 20000 / `330ccea0…` / 0.29s（增量 ok） |

三点值得记下来：

1. **md5 跨版本一致**：同一组合在 5.7 / 8.0 / 8.4 上是同一个指纹（`83646f77…` / `330ccea0…`）。
   这是「矩阵能横向比」的前提；不一致就说明类型映射或字符集在某版本上跑偏了。
2. **PG→MySQL / MySQL→MySQL 的增量在 #89/#93 之后是通的**（`incremental=ok`，md5 也对得上）。
   本文件早先写的「只全量/明确拒绝」是**旧口径**，已按实测改掉；脚本也不再写死「必须被拒」。
3. **MySQL→PG 的 kill -9 重启是 0 行重搬**（三版本一致），这是 ① 的验收口径。

> 矩阵里「拒绝」不是失败：`core.direction.go` 把 MySQL→MySQL 标为 pending，引擎对
> 「MySQL 目标端 + 增量」明确报错（`target.go` 的 incrementalGate）。矩阵把这件事**当成判据**
> 断言下来，避免哪天静默降级成「只搬一次」还没人发现。

## 4. 崩溃回归与表级暂停

| 项 | 用例 / 命令 | 关键输出（本地实跑） |
|---|---|---|
| MySQL 增量 kill -9 → 重搬 0 行 | `go test -tags=integration -run '^TestMySQLSourceIncrementalResumeAfterSIGKILL$' ./internal/engine/` | `全量完成：20000 行` → `kill -9：目标端 21200 行（源端此刻 21850 行）` → `续跑：重搬 0 行、用时 6.45s → 追平后 21850 行，md5 21850:6b8307812c0461a5a5e2ecd7a373e5fc`，位点只前进 |
| 同上（脚本口径） | `scripts/acceptance_matrix.sh --only mysql-postgres --kill` | `gate: OK [kill -9 重启] 重启后重搬 0 行（允许 0）` |
| 表级暂停真跑 | `go test -tags=integration -run '^TestPauseTableRegistersKeyKeepsPositionAndRecovers$' ./internal/engine/` | `暂停：位点 "postgres:v1:0/2229FE0" 前后逐字节相等；key="pause_a" 与块标记同源；解除后两端指纹一致`，PASS 3.67s |
| 位点与数据同事务 | `go test -tags=integration -run '^TestPositionPersistedWithData$' ./internal/pgsink/` | `位点：0/AA →（坏批失败，不动）→ 0/CC；行数 3 → 3 → 5`，PASS 0.05s |

**口径提醒**：「表级暂停」目前是「暂停登记只记该表 + 位置不动 + 整轮失败」，**不是**
「其它表继续同步」——后者是契约变更，Data 在 #89 的 notes 里给了三个选项等 Ark 裁决。
本门禁断言的正是当前契约（`notes/contract-paused-tables.md` §1.5），不替 Ark 改语义。

## 5. 环境与复跑

沙盒里没有 docker；每次 `execute` 是新沙盒（`/tmp` 与后台进程都不跨调用）。本地栈放在工作区：

```
scripts/local_stack.sh up        # PG 55432（wal_level=logical）+ MySQL 53306/53307，写 .localdb/stack.env
. .localdb/stack.env
scripts/merge_gate.sh --all
scripts/local_stack.sh down
```

踩过的坑（都写进脚本注释了）：`/etc` 是 700 root（`su` 直接 Critical error，用 `setpriv`）；
`mysqladmin ping` 在 Access denied 时也返回 0；MySQL 8.0.28 的 `WITH RECURSIVE` 在 5.7 没有。

## 6. 阻塞点 / 未做

1. **PG→MySQL / MySQL→MySQL 两格在 CI 上口径不同**：本地（MySQL 8.0.28）两格的增量都是
   「被拒 + 行数不变」（1.96s / 2.28s）；CI runner 上同样的 job 文档会进流（`--stop-after 10s`
   能正常收尾、日志为空、退出码非 0），说明**增量路径在不同版本/环境下行为不一致**。
   矩阵现在两种结论都接受，但只认「md5 一致」或「一行没多 + 有原因」——这正是这次要收的口径：
   门禁不去猜产品语义，只要求证据闭合。**行为不一致本身**是一件要报给 Core/Data 的事：
   同一个二进制、同一份 job，8.0.28 拒绝、CI 的 8.0.46 进流。
2. **MySQL→MySQL / PG→MySQL 的增量真的没有**（引擎明确拒绝）。矩阵把「拒绝」当判据，
   但「每天能跑增量」这件事要等引擎那条路接上——不归本次门禁。
3. `merge-gate` 目前是**独立 workflow**，还没进 branch protection 的 required checks：
   按 #59 的节奏，先观察一周（advisory），再和 `review-gate` 一起转 required。
4. 反向注入的 engine 档需要真库；CI 里跑在 `engine-mutation` job，本地靠 `local_stack.sh`。

## 7. 缺口闭合（2026-11-10 夜，第二轮）

三个缺口都是在既有门禁上补，没有重写已通过的部分。

### 7.1 `b1_mysql_to_mysql.sh killcopy` 从「能跑过」改成「能证伪」

| 项 | 内容 |
|---|---|
| 用例 | `ROWS=20000 scripts/b1_mysql_to_mysql.sh ./dist/jinliang killcopy`（块 2000 行/块） |
| 旧行为 | kill 在 4000/20000 行时 `_jl_chunk` **0 行**，重启**整轮重搬 20000 行**，脚本仍 PASS——因为它只比「两端最终一致」，而整轮重搬当然也一致 |
| 新判据 | ① `_jl_chunk` 行数 > 0（没有块记账 = 谈不上块级续跑）；② 重启重搬行数 ≤ 1 块（`重搬 = 重启搬的行数 − 未搬完的行数`） |
| 是否先红过 | **是**：本地实跑 `killcopy exit=1`，输出 `FAIL: _jl_chunk 里 0 行 —— 目标端没写块记账` |
| 现在的行为（诊断用，断言临时旁路） | `结果：阶段 done，复制 20000 行` —— 即整轮重搬；其中 4000 行是已提交块的重搬（`重搬 = 20000 − (20000−4000)`），超过 1 块 2000 行 → 判据会红 |
| 当前状态 | **挂起（已知红）**：等 Data 补 `mysqlsink` 块记账。断言已写死，落地那天自动变绿；CI 里这一步 `continue-on-error` 且命名写明「已知红」，不会静默 |

### 7.2 MySQL→MySQL 有 CI 格了

| 项 | 内容 |
|---|---|
| job | `merge-gate.yml` 的 `mysql-mysql`（两个独立 MySQL 实例：源 53306 / 目标 53307） |
| 参数 | `ROWS=20000`，与本地一致；三步：全量+增量 / 增量中途 kill -9（重搬 0 行）/ 块级续跑（已知红） |
| CI 实跑（run 42874043879） | 全量+增量 `PASS: 20000 行，全量 5.55s，增量追平 3.41s，指纹 20000:5c49f8b9…`；kill -9 `PASS: 重启重搬 0 行，两端 19996:54c437d6…` |
| killcopy（CI） | `_jl_chunk 行数：0` → `FAIL: _jl_chunk 里 0 行 …` —— **按设计红**，步骤 `continue-on-error`，job 仍绿但输出里留着 FAIL |
| 本地同口径 | 全量 20000 行 / `20000:ea88a483…`；增量 19996 行 / `19996:9e0cf148…` / 重搬 0（md5 随 `NOW(6)` 变化，判据是源==目标，不是固定指纹） |

### 7.3 引擎层 kill -9 覆盖 5.7 / 8.4

| 项 | 内容 |
|---|---|
| 用例 | `TestMySQLSourceIncrementalResumeAfterSIGKILL`（`internal/engine`，引擎层，不是连接器契约层） |
| 覆盖 | `acceptance` job 每个 MySQL 腿各跑一遍：5.7.44 / 8.0.46 / 8.4.13，判据「重启重搬 0 行」 |
| 探测而不是版本号 | 状态语句由 `mysqlsource` 能力探测决定（先 `SHOW BINARY LOG STATUS`，失败退 `SHOW MASTER STATUS`；见 `internal/mysqlsource/capability.go`，单测 `capability_test.go` 覆盖两条路径）。CI 步骤里也把探测结果打出来，红的时候能区分「版本没覆盖」和「行为退化」 |
| 当前状态 | **绿**（5.7 / 8.0 / 8.4 三腿都 PASS，见 run 42870646277 起的九格表） |

### 7.4 回归：停止请求之后的真实流错误不许被记成「已停止」

| 项 | 内容 |
|---|---|
| 用例 | `TestStopRequestDoesNotSwallowLateRealError`（`internal/engine/stop_late_error_test.go`，构建标签 `knownred`） |
| 触发条件 | ① 流在跑，`--stop-after`（或追平）先把 `stopHit` 置 true 并 cancel ctx；② 连接器在 ctx 取消之后因为**自己的原因**返回真实错误（binlog 被清理 / 复制权限被收回 / 网络断开）；③ `streamGeneric` 的收尾 switch 命中 `case stopHit.Load() \|\| idleHit.Load():`，把真实错误丢掉 |
| 是否先红过 | **是**：`go test -tags=knownred -run TestStopRequestDoesNotSwallowLateRealError ./internal/engine/` → `真实流错误被记成「正常收尾」（reachedDone）：退出码会变成 130（已停止）` |
| 为什么用标签不用 Skip | Skip 会让人以为跑过了；`knownred` 保证常规 `go test ./...` 不受影响，而 merge-gate 有一条 `continue-on-error` 的步骤天天跑它——修好后那一步自己变绿，去掉标签即可 |
| 当前状态 | **挂起（已知红）**，交 Core 修 `target.go` 的收尾判据（建议：先判「错误是否由我们 cancel 引起」，再判 stopHit） |

### 7.5 补上「暂停登记落库失败」这条守卫（第 10 条注入）

上一轮自报的洞：把 `engine.registerPause` 置空后，原有的暂停集成用例仍绿——裁决 A 之后
没人盯「登记失败＝整批失败」。

| 项 | 内容 |
|---|---|
| 用例 | `TestPauseRegistrationFailureFailsWholeBatch`（`internal/engine/pause_registration_failure_test.go`，`package engine`，`-tags=integration`） |
| 为什么直接测 `pauseSink.Apply` | prepare 阶段的结构核对是**另一条**登记路径（`prepare` 里直接调 `sink.PauseTable`），不经过 `registerPause`。整轮跑很容易被它抢走，用例就变成测 prepare 了；这里用一个「Apply 永远返回 `TypeChangedError`」的假目标端，把**写路径**单独拎出来，时间上完全确定 |
| 故障怎么造 | 目标端 `_jl_meta` 挂触发器：`paused_tables` 一被更新就 `RAISE`（模拟列被锁 / 库只读 / 磁盘满）。不改引擎，只在目标端制造真实故障 |
| 断言 | ① Apply 必须失败；② 错误带 `registerPause` 的包裹语「另外：登记这张表的暂停状态也失败了」；③ 触发器原话在错误里（证明 `PauseTable` 的 UPDATE 真跑了）；④ **假目标端只被调用 1 次**（登记失败立刻整批失败，不许原地重试）；⑤ `paused_tables` 里没有半条记录 |
| 注入项 | `register-pause-neutered`：`engine.registerPause` 首行 `return nil` |
| 是否先红过 | **是**（先写了整轮版用例，跑出 `登记表 X 的暂停失败（本轮一行都没搬）`——那是 prepare 路径；据此改成直测写路径） |
| 当前状态 | **绿**：本地 `PASS (0.09s)`；注入 `register-pause-neutered` → `KILLED`（`错误里没有 registerPause 的包裹语`）。`--db` 层现在 **logic 6/6 + engine 7/7 = 13/13** |

需要 Core 打洞吗：**不需要**。用例只依赖导出能力（`pgsink.Open/EnsureMeta/PauseTable`）+ 目标端触发器，
一行引擎代码都没动。

### 7.6 `merge-gate` 从 advisory 转 required 的判据（写死，别靠记忆）

观察期一周，期满按下面三条**客观判据**决定（全部满足才转）：

1. **拦截成功**：观察期内至少出现 1 次真实拦截（红），且被拦的原因是它该拦的（结论过期 / 注入存活 /
   矩阵不一致），不是环境抖动；
2. **无误报**：同一窗口内没有「门禁红但查下来是实现没问题」的记录（含 flaky：同一 commit 重跑
   结论不同也算一次误报）；
3. **红灯可归因**：每次红都能在 job 日志里直接指到判据名（`gate_checks` / `mutation` / `acceptance`
   / `review` 四层之一），不允许出现「红了但不知道哪条判据」。

任一条不满足 → 继续 advisory，并在 notes 里记下差在哪；**不按时间自动转**。
转的时候只删 `continue-on-error` 并把已有 check 名加进 required，不改 job 名（与 #59 的节奏一致）。

**量化口径（可核对，不是「观察一周」）**：

| 判据 | 门槛 | 怎么核对 |
|---|---|---|
| 样本量 | 最近 **≥10 次** PR 触发的 `merge-gate` run（或 ≥7 天，取先到者） | `gh run list --workflow merge-gate` |
| 无误报 | 上述窗口内「红但查下来是实现没问题」= **0**；同一 head 重跑结论不同（flaky）= **0** | run 记录逐条比对 head 与结论 |
| 拦截有效 | 上述窗口内**至少 2 次**因判据拦截，且原因归属正确（review 过期 / 注入存活 / 矩阵不一致 / 台账未收口之一） | 每次红在 notes 记一行：head + 判据名 + 归属 |
| 红灯可归因 | 每次红都能指到四层判据之一的**名字**（`gate_checks` / `known_red` / `mutation` / `acceptance` / `review`） | job 日志里的判据行 |
| 回退条款 | 转 required 后 **7 天内出现 2 次误报或 1 次 flaky** → 立即退回 advisory，并记 notes | 同上 |

四条同时满足才转；**不按时间自动转**。

## 7.7 已知红不许无限期挂着（三元组 + 机器对账 + 自证）

把「先武装断言、先红」做成 `continue-on-error` 很省事，但没有收口机制就会永远红着放行。
现在每一格都有三元组，并且**由 `scripts/known_red.py` 每天对账**：

| 已知红步骤 | 责任人 | 转绿用例 | 移除 `continue-on-error` 的触发条件 |
|---|---|---|---|
| `块级续跑（块进度 > 0 且重启重搬 ≤ 1 块）` | **Data** | `scripts/b1_mysql_to_mysql.sh killcopy`（`_jl_chunk > 0` 且重启重搬 ≤ 1 块） | `mysqlsink` 在全量搬到 MySQL 目标端时写 `_jl_chunk`；**该实现合入后**这一步自动转阻塞（探测：`internal/mysqlsink` 非测试代码里出现写 `_jl_chunk` 的动作） |
| `回归（knownred）：停止请求之后的真实流错误不许被记成「已停止」` | **Core** | `TestStopRequestDoesNotSwallowLateRealError`（`-tags=knownred`） | `target.go` 收尾判据改成「先判错误是否由我们 cancel 引起，再看 `stopHit`」；**该修复合入后**删 `continue-on-error` 并去掉 `knownred` 标签（探测：这条用例转绿） |

对账规则（`known_red.py` 的 `classify`，纯函数 + 4 条 selftest）：

| 实现落地 | 步骤挂着 `continue-on-error` | 结论 |
|---|---|---|
| 否 | 是 | ✅ 合法（打印倒计时清单） |
| **是** | **是** | ❌ **拦截**：`断言未激活`（断言写好了没人打开，等于没有） |
| 否 | 否 | ❌ 拦截：`提前激活`（实现没落地就把步骤改成阻塞，CI 会常红） |
| 是 | 否 | ✅ 已收口 |

- **门禁输出里可见**：`merge_gate.sh` 每次都在结论表之前打印「⚠ known-red 未收的账」表
  （步骤 / 责任人 / 转绿用例 / 移除条件 / 现状），不埋在 YAML、也不只躺在日志里。
- **能自证**：注入项 `known-red-stale-suspended` —— 把 `classify` 的「已落地 + 仍挂起」判据改松，
  `known_red.py selftest` 立刻红（含 fixture 场景「步骤仍挂起 + 检测器 outcome=success → 判 1」），
  注入框架记为 **KILLED**。
- **真红过一次（不是推演）**：run `42893465770` 的 logic job 直接判红，原文
  `❌ 实现已落地；步骤仍挂起 — 断言未激活（…）：chunk.go 里已经有写 _jl_chunk 的动作`。
  这次是**真的**：#95（Data 的块记账）刚合入，而步骤还挂着 `continue-on-error` —— 台账按设计拦住了。

**这次真红还暴露了一个判据缺陷（已修）**：用「静态代码里有没有写 `_jl_chunk`」当「实现落地」
太粗——#95 只落了**记账**，引擎通用全量路径还没读块进度，行为仍然红（本地实测：`_jl_chunk` 2 行，
重启仍搬 20000 行，重搬 4000 行 > 1 块）。所以判据改成**用检测器这一步的真实 outcome**：

- `known_red.py verify --id <id> --result <success|failure>`：`success` 才等于「实现落地」；
- 这个 `--result` 由 mysql-mysql job 的 `steps.killcopy.outcome` 提供（步骤本身带 `continue-on-error`，
  `outcome` 仍是 `failure`，`conclusion` 才是 success —— 正好可用来区分「步骤挂起」与「检测器能过」）；
- logic job 里对这类「要真库」的已知红只打提示（`由 mysql-mysql job 的 verify 步骤对账`），不判死。

当前两格状态（2026-11-11 00:20）：

| 步骤 | 实现 | 步骤 | 状态 |
|---|---|---|---|
| killcopy | ① 块记账已落（#95）；② 续跑接线**未落**（重启仍整轮重搬） | 仍挂起 | ✅ 合法已知红 |
| late-error | 未修（knownred 用例仍红） | 仍挂起 | ✅ 合法已知红 |
- 三元组失联也会红：步骤名改了/删了，`step_suspended` 找不到 → 判失败（台账与 YAML 失联）。

## 7.8 「0 重搬」三口径（各自落在哪、数字属于哪个口径）

契约把「0 重搬」拆成三个**互不替代**的度量。之前的矩阵只有一种口径，容易把「整轮没搬」和
「该表没搬」混成一件事。现在每条断言都带口径名，输出里也打出来：

| 口径 | 定义 | 落在哪个用例/脚本 | 输出里的标记 |
|---|---|---|---|
| ① 整轮（验收主口径） | `rowsCopied==0 ∧ plan.Kind==copySkip` | `acceptance_matrix.sh` 的 MySQL→PG kill 那格 → `gate_checks.py recopy --scope whole`；`plan.Kind==copySkip` 由 `TestPlanCopySingleTableResumeMetric` 的反面断言（表已搬完 → copySkip）钉住 | `[整轮口径] 重启后整轮重搬 0 行` |
| ② 单表 | `chunk_no=0` 仍在 ∧ `plan.From[table]` 非空 | `b1_mysql_to_mysql.sh killcopy` → `gate_checks.py recopy --scope table`（结果面）；`plan.From` 非空由 `TestPlanCopySingleTableResumeMetric` 直接断言（判据面） | `[单表口径] 重启搬 N 行 ≈ 未搬完 M 行（±1 块）` |
| ③ 暂停恢复 | 该表重搬行数 == 该表行数，**允许 ≠ 0** | `TestPauseTableRegistersKeyKeepsPositionAndRecovers` 新增断言 `res2.RowsCopied == 251` | `[暂停恢复口径] 该表重搬 251 行 = 表行数（本口径允许非 0）` |

`gate_checks.py recopy` 现在必须显式给 `--scope whole|table|pause`，selftest 24 条覆盖三种口径
（含「单表口径整表重搬必须红」「暂停恢复多搬/少搬必须红」）。

## 7.9 killcopy：记账转阻塞（绿）；续跑是**结构性红**，单列跟踪 job（不阻塞）

> **更正**（2026-11-12，撤回「killcopy 可以整格转阻塞」）：转阻塞的前提是「实现已落地」，
> 而这里只落了一半，且缺的那一半在**引擎**不在 Data：
> `copyGeneric` 把 `nil` 传给 `SnapshotChunksFrom`（`internal/engine/target.go:456`），
> `executeGeneric` 从不读 `ChunkProgress` → **当前必然整轮重搬**。所以在 Core 接通前，
> 把它放进门禁 job 只会把整条门禁堵死。处理：单列成跟踪 job `chunk-resume-tracking`
> （job 级 `continue-on-error`，**不阻塞合并**），门禁 job 只留能绿的部分。

Data 的块记账（#95）落地后，按「实现已落地 + 步骤仍挂起 = 判 1」的规则，把这一格**拆成两步**：

| 步骤 | 判据 | 依赖 | 实跑结果 |
|---|---|---|---|
| `块进度记账（阻塞）` | 搬运期逐块（`chunk_no>0 ∧ done=1`）+ 完成后表标记 `chunk_no=0` 的 `rows` **等于各块之和**、等于表行数；记账不许虚报 | 只依赖目标端记账（#95 已落） | **PASS**：`搬运期 2 块 / 4000 行；完成后表标记 rows=20000 = 各块之和 = 表行数` |
| `块级续跑（known-red）` | 单表口径：重启搬 ≈ 未搬完 ±1 块 | 引擎通用全量路径读块进度（**未落**，`executeGeneric` 至今不调 `DoneChunks`） | **仍红**：`中断 4500 行 → 重启搬 20000 行（应补 15500 + 多出 4500）` → `[单表口径] 超界` |

为什么没有把整格直接转阻塞：`executeGeneric` 里 `DoneChunks` 零调用点（`grep -n "DoneChunks("
internal/engine/*.go` 只有接口声明），硬转阻塞会让 `mysql-mysql` job 常红，等于把 known-red 变成 noise。
现在按「结构性红」处理：**移出**门禁 job、单列 `chunk-resume-tracking`（不阻塞合并），
workflow 注释与台账都写成事实：「generic 路径未读块进度（`target.go:456`），当前必然整轮重搬」。
责任人 **Core**；转阻塞条件＝读路径接通 **且** 该格重搬行数 ≤1 块。台账 probe 仍用这一步的真实
`outcome`——接线一旦落地，`verify` 会立刻判「断言未激活」逼着转阻塞。

### 7.10 采纳 Owl 的四条建议（都已落）

| 建议 | 落法 |
|---|---|
| `merge-gate.yml` 的 paths 太窄 | 补 `internal/core/**`、`internal/source/**`、`internal/mysqlcanon/**`、`cmd/**`（方向表/分类语义/值规范化/CLI 退出码都是门禁输入面） |
| `acceptance_matrix.sh` 的 `seconds` 被 restart 覆盖 | 拆成 `copy_seconds`（全量+追平）与 `restart_seconds`（kill 后重启），`RESULT` 行两个字段都在，三组合可比 |
| `mutation_gate.py` 的 killed 判据是 `rc!=0` | 收紧：**go 检测器必须出现 `--- FAIL:`** 才算 killed；出现 `[build failed]`/`cannot use`/`undefined:` 记 `COMPILE-FAILED`（不算抓住）；非 go 检测器（python selftest / known_red）仍按退出非 0 |
| 与本 PR 无关的 `notes/instabox-*` | 7 个文件（约 1030 行）移出仓库（副本挪到工作区 `local-notes/`），PR diff 只剩门禁相关 |
