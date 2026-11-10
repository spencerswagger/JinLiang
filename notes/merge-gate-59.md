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
| 4 | pause-position-neutered | `gate_checks.py` 的 `if before != after:` → `if False:` | selftest 红（「pause 位点动了」） | rc=1，`1/19 条判据不符合预期` | 是 |
| 5 | pause-keys-neutered | `gate_checks.py` 的 `if got != exp:` → `if False:` | selftest 红（多/少登记两条） | rc=1，`2/19 条判据不符合预期` | 是 |
| 6 | review-deny-word-weakened | `review_gate_logic.py` 的 `DENY` 正则 `[1-9]` → `[8-9]` | `review_gate_logic.py selftest` 红（mustfix-2 / history-zero） | rc=1，`必改 2 条` 被误放行 | 是 |
| 7 | review-stale-accepts | `review_gate_logic.py` 的 `if anchor.get("production_touched"):` → `if False and …` | selftest 红（production-changed / test-hand-added） | rc=1 | 是 |
| 8 | position-not-persisted | `pgsink.setPositionTx` 首行 `return nil` | `TestPositionPersistedWithData` FAIL | rc=1，`位点没落盘：期望 postgres:v1:0/AA` | 是 |
| 9 | ddl-pause-skipped | `engine.registerPause` 首行 `return nil` | `TestPauseTableRegistersKeyKeepsPositionAndRecovers` FAIL | rc=1，`应只登记 1 张暂停表，得到 map[]` | 是 |
| 10 | chunk-resume-degraded | `resume.planCopy` 的 `p.Kind = copyResume` → `copyFresh` | `TestPlanCopy` FAIL | rc=1，`结论错：想要 resume，得到 fresh` | 是 |

结论：**10/10 被抓住**（logic 7/7、engine 3/3）。

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

5.7 / 8.4 由 PR #92 的 `merge-gate` workflow 出数（沙盒没有 docker）：

| 组合 | MySQL 5.7 | MySQL 8.0 | MySQL 8.4 |
|---|---|---|---|
| MySQL→PG | CI | CI | CI |
| PG→MySQL | CI | CI | CI |
| MySQL→MySQL | CI | CI | CI |

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

1. **5.7 / 8.4 的格子只有 CI 出数**：沙盒没有 docker，本机只有 MySQL 8.0.28。workflow 已随 PR
   跑，数字回填在本文件。若 CI 腿因镜像差异红（比如 8.4 权限名），修的是 workflow，不是放宽判据。
2. **MySQL→MySQL / PG→MySQL 的增量真的没有**（引擎明确拒绝）。矩阵把「拒绝」当判据，
   但「每天能跑增量」这件事要等引擎那条路接上——不归本次门禁。
3. `merge-gate` 目前是**独立 workflow**，还没进 branch protection 的 required checks：
   按 #59 的节奏，先观察一周（advisory），再和 `review-gate` 一起转 required。
4. 反向注入的 engine 档需要真库；CI 里跑在 `engine-mutation` job，本地靠 `local_stack.sh`。
