# #92 × #93 复盘：两格红是判据旧口径，不是引擎

日期：2026-11-10 18:35 UTC ｜ 分支 `jinliang/merge-gate-59`（PR #92）

## 1. 两格红（postgres-mysql / mysql-mysql）到底红在哪

CI（run 42868874313）的事实不是「增量被拒」，而是：

```
-- 17:53:07 增量（应被拒）开始
-- 17:53:17 增量被拒后行数不变        ← 行数确实没多
FAIL: 增量被拒但日志里没有一句原因
[实时同步中] … 结果：阶段 done，增量 0 行，退出码 130
jinliang run: 已手动停止
```

- 脚本用 `--stop-after 10s` + `timeout`；**增量其实已经接通并进了流**，
  10s 后是脚本自己把任务停掉的（rc=130）。
- 旧判据要求「rc≠0 + 日志里有一句『只搬全量/还没接上/…』」。现在既不是拒绝，
  rc 也不是引擎给的失败码，于是它把「跑起来了」判成「拒绝了但没说原因」。
- **不是引擎的锅，也不是 DDL 注入**：这两格的增量步骤只做 insert/update/delete，
  没有改类型。MySQL 目标端「不支持按表暂停」只在真的命中结构不一致时才触发
  （`pauseUnsupportedError` → E1014 家族，文案「表 X … 当前目标端（mysql）还没有暂停记录这一列」），
  这次日志里的 `PAUSE-01` 只是 prepare 的一条 note，不是失败原因。

## 2. 判据怎么翻的（`scripts/acceptance_matrix.sh`）

三组合统一成：`continuous+initial`（流在造增量之前就建好）→ 全量+追平 →
源端增/改/删三笔 → 再跑一轮 `--stop-when-idle 3s`。

只认两条可证伪的事：

1. 退出码 0（自己追平收尾，不是被 timeout / Ctrl-C 打断）；
2. **跑完之后**源与目标的内容指纹一致（行数 + 逐行 md5）。

自己踩的一个坑：第一版把目标指纹在「跑之前」取好传给比较函数，等于拿
「跑之前」比「跑之前」，增量 0 行也会绿。已改成跑完再取（`incr_check` 里注释写明）。

本地实测（MySQL 8.0.28，源/目标**两个实例**，ROWS=2000）：

```
RESULT combo=postgres-mysql rows=2000 md5=915ced9f9564a3def41e2dc4cbfb8454 seconds=10.61 status=pass mysql=8.0.28 incremental=ok
RESULT combo=mysql-mysql    rows=2000 md5=915ced9f9564a3def41e2dc4cbfb8454 seconds=10.19 status=pass mysql=8.0.28 incremental=ok
```

注意：MySQL→MySQL 源/目标**不能放同一个实例**（目标端的写入会进源端 binlog，
读到自己刚写的行 → 「已应用 3 行」无限循环）。CI 本来就是两个容器，本地复现也要两个。

## 3. 反向注入翻转（`scripts/mutation_gate.py`）

`pgsink.Apply` 的暂停判定已搬到 `internal/engine/pause_skip.go`，原注入
`ddl-pause-skipped`（把 `registerPause` 置空）在新语义下**存活**——不再成立。
现在 engine 档 6 条，本地 `--tier engine` **6/6 全部被抓住**：

| 注入 | 检测器 | 结果 |
|---|---|---|
| `pause-filter-keeps-all`：filter 不丢暂停表 | `TestPauseSinkFilterDropsOnlyPausedTableRows` | KILLED |
| `forced-recopy-neutered`：`applyForcedRecopy` 直接 return | `TestApplyForcedRecopy*`（两条） | KILLED |
| `commit-plan-alias`：`r.plan = *plan` 放在 `applyForcedRecopy` 之前 | 同上 | KILLED |
| `reconcile-clears-early`：对齐就 `ClearPausedTable`（旧判据） | `TestReconcilePausedMarksRecopyAndKeepsRecord` | KILLED |
| `position-not-persisted` / `chunk-resume-degraded`（原有） | 各自用例 | KILLED |

`--tier logic` 6/6、`gofmt -l` 空、`go vet ./...`、`go test ./...`、
`validate_schema.py examples/*.yaml` 全绿。

## 4. 还需要别人收的两条（我没改）

1. **A3 没有用例**：`registerPause` 返回 nil（=跳过了但没登记）现在没有用例能抓住。
   旧注入 `ddl-pause-skipped` 存活的原因就是它——契约 §1.5「登记先行」要求
   「登记失败 → 整批失败、目标端 0 行、位点不动」，Core 需要一个注入型用例。
   我把它从 mutation 清单里拿掉了（留着会天天红），但缺口记在这里。
2. **`PAUSE-01` 的 OK 文案还是旧口径**：`internal/preflight/b0.go:152` 写
   「结构对不上的表会被记录并停下来，位点不推进」——#93 之后位点是照常前进的。
   建议 Core 改成「只暂停该表，其它表继续、位点照常前进；结构对齐后整表重搬补齐」。
3. **契约文件没在 main 上**：`docs/CONTRACT-PAUSE-AND-POSITION.md` 只在
   `contract/pause-and-position`（PR #90 仍 OPEN），而 #93 的提交信息与代码注释都在引用它。
   要么合 #90，要么把契约随 #92 之前先落 main，否则 main 里是一条悬空引用。
