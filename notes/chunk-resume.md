# 块级续跑（全量中断后从已提交的块边界接着搬）

结论：**能续、续出来的结果与一次性全量逐表一致、两种中断（kill -9 / 正常取消）都覆盖**。
证据与判据都在 `internal/engine/resume_kill_test.go`（真库、真子进程、真 SIGKILL）。

## 1. 真源：块标记 = 一个事务

`_jl_chunk(job_key, table_name, chunk_no, range_from, range_to, done, rows)` 与数据、
位点在**同一个事务**里提交（`pgsink.BulkApply`）。因此：

- 已提交的块永远完整；被 kill -9 时正在写的那一块整个回滚 → **不会有半块**；
- 重启判定只读目标库，不需要本地文件、不需要源端配合。

块号 1 起、一表一套，`range_to` 是 keyset 右边界 = 下一块的 `From` = **续跑锚点**。
块号 `0` 保留给「这张表存量已搬完」的**完成标记**（`pgsink.TableDoneChunkNo`）。
完成标记不能拿「最后一块」代替：一页读空时根本不会产生新块，而「有标记」只说明搬过。

## 2. 判定（`engine.planCopy`，纯函数，穷举单测）

输入：目标端块进度 + `_jl_meta.position` + 源端书签已确认位点 + 模式 + 有无稳定主键。

| 情形 | 结论 |
|---|---|
| 至少一张表没搬完，且位点/书签能重放 | **续跑**：有主键的表从最后一块的右边界读；已搬完的表跳过；无主键表整表重来（只它一张） |
| 书签不在 / 书签已越过位点 / 没有位点 | **整轮重搬**（只在「上一轮断在半截」时才先清本任务建的表，一个事务里清数据+清标记） |
| 全部搬完且模式不要重搬（continuous+initial） | **不搬** |
| once / full_interval 且上一轮已经搬完 | **整轮重搬**（模式语义，与之前一致；不清表，靠 upsert 幂等） |
| 老状态（升级前没有块标记，只有 phase） | 按 phase 兜底认定「已搬完」，不因升级把同步好的任务重搬一遍 |
| 目标端不记块进度（MySQL 全量路径） | 回到「整轮重搬」，不假装能续 |

换代的地方：以前用 `_jl_meta.phase == snapshot` 判「上次没搬完」，但**正常取消**会把
phase 写成 `done`（drain 的既有语义），于是「取消过的半截全量」在 continuous+initial
下会被当成「搬完了」直接跳过 → 静默少数据。现在 phase 不参与判定（只做升级兜底）。

## 3. 为什么续跑读「更晚的视图」是安全的

导出的快照视图随进程消失（PG 的 exported snapshot 绑在导出会话上），所以剩下的块只能
用一个**新的** `REPEATABLE READ` 视图读。这不会丢变更，前提是**位点不许往前挪**：

- 增量仍从**中断前那个位点**重放（`_jl_meta.position` 原样保留，续跑路径不重建书签）；
- 已搬区间里：中断期间的 INSERT/UPDATE/DELETE 都被重放 → 收敛到最新；
- 未搬区间里：续跑的新视图读到的是「更晚但完整的一段」，其中重复的部分靠 upsert 幂等。

反过来说：**只要把位点挪到续跑时刻，已搬区间里新增的行就再也没人搬**。这条写成了
`assertResume` 的断言（`after.position == posBefore`）和
`TestChunkResumeReconcilesChangesDuringDowntime`。

## 4. 实测（本机 PG，200k 行 / 块 1000）

| 场景 | 中断点 | 续跑搬 | 用时 | 结果 |
|---|---|---|---|---|
| kill -9 | 100 块 / 100000 行 | 100000 行 | 1.94s | 200000 行，md5 与基准、源端一致，位点未挪 |
| SIGINT 正常取消 | 100 块 / 100000 行 | 100000 行 | 1.41s | 同上 |
| kill -9 + 停机期间改源端 | 100 块 / 101000 行 | — | — | 追平后 200400 行，md5 一致（删了再插回 100 行 / 改 200 行 / 删 100 行 / 表尾 +500 行） |

`scripts/b1_pg_e2e.sh kill`（300k 行）：kill 在 200000 行 → 重启只搬 100000 行 →
停机期间源端删掉的 id=5 由增量重放删除 → 指纹一致。
`scripts/b1_pg_e2e.sh append`（用户自己的表）：续跑接上，用户的 3 行完好，共 300003 行。

变异验证：把 `planCopy` 强行改成永远 `copyFresh`，两条续跑用例立刻红
（「重启后搬了 200000 行…这是整轮重搬」）——用例确实盯着这件事。

## 5. 必改 1：清标记与推位点必须同一个事务

原来的写法是两次写：`prepare` 里 `EnsureMeta` 把 `_jl_meta.position` 推到新视图的位点，
`snapshot` 里再清块标记。中间崩一次（kill -9 / OOM / TRUNCATE 报错后人工重跑）就留下
「新位点 P2 + 上一轮的块标记 1..50」；下次 `planCopy` 读到 anyMarker/not-done/StoredCP=P2/
书签可用 → 判成续跑：从旧游标接着搬、增量却从 P2 重放，(P1,P2] 的增/改/删永不重放，
而且退出码 0、日志写着「续跑」。

现在：

- `pgsink.StartFreshCopy(ctx, pos)`：一个事务里 `DELETE _jl_chunk`（整个 job_key，含本次
  未选中的表）+ 推进位点。库里只剩两种状态——「旧位点 + 旧标记」或「新位点 + 无标记」。
- `prepare` 在从头搬时**不**写位点（`writePos=""`，`EnsureMeta` 用 COALESCE 保留旧值）。
- 覆盖到的入口：必改窗口本身；本次未选中的表残留旧标记（同一个 DELETE 按 job_key 清掉）。
  未覆盖：`on_exists=recreate` 把表重建后残留的 Done 标记（属 Owl 建议项，另排）。
- `pgsource.slotPosition` 顺带把 `wal_status='lost'`（max_slot_wal_keep_size 把 WAL 丢了）
  当成「书签不可用」：书签还在但重放不了，当「还能续」一样是静默丢一段。

新用例 `TestSlotLossForcesFullRecopyNotResume`：kill 中途 → drop 书签 → 改已搬区间 →
用另一条连接压住目标表的 ACCESS SHARE 让 TRUNCATE 卡在锁上（窗口确定命中）→ kill -9 →
断言**位点没被提前推**，再跑一次必须整轮重搬且内容 md5 与源端一致。
变异验证（把位点改回 prepare 推）：立刻红——「从头搬的窗口里位点被提前推了……」。

## 6. 必改 2（新 P0）：`wal_status` 不能混进「书签在不在」

上一版把「存在」和「能不能重放」压进 `slotPosition` 的一个 `exists` 返回值：lost 时返回
`exists=false`，而 `Prepare` 正是靠 `exists` 决定 drop / reuse / 新建 → lost 时既不 drop 也不
reuse，直接建书签，PG 报 `slot already exists` → **每次 run 都失败、重跑也治不好**（`--resync`
只在文案里）；`Cleanup` 同样不 drop lost 书签 → 孤儿占 `max_replication_slots`。

现在拆成两件事：

- `pgsource.slotState` → `{LSN, Exists, WalStatus}`，`Replayable() = Exists && WalStatus != "lost"`；
- `Prepare`：`Exists && (Snapshot || !Replayable())` → drop + recreate（lost 也算），
  理由分别写进动作清单；建书签失败的提示把「同名书签还在」列成第一条；
- `Cleanup`：只要 `Exists` 就 drop，不看可重放；
- `decision`：`SlotConfirmed` 仍以「可重放」为准（lost = 续不了）；`planCopy` 的 `allDone`
  分支也看这一条——存量都搬完但书签追不回位点，不再报「不用搬」，而是整轮重搬；
  continuous 模式（本来就不搬存量）遇到这种情况直接停下，四段式说清能做什么，不悄悄挪位点。
- `stateCapable` 里零调用者的 `ResetChunks` 删掉（`pgsink.ResetChunks` 一并删）：
  单独一个「只清标记」的方法等于把必改 1 的窗口重新交给调用方。

新用例 `TestLostSlotSelfHealsByFullRecopy`：真造 lost（`max_slot_wal_keep_size='64kB'`
→ reload → 灌 WAL + `pg_switch_wal()` + `CHECKPOINT` → 轮询到 `wal_status='lost'`，轮不到
直接失败）→ 重跑一次必须自愈：整轮重搬 120200 行、md5 与源端一致、该任务名下书签数 = 1。
变异验证（把 lost 塞回 `exists=false`）：立刻红，报
`replication slot "jl_…_1" already exists`。

## 7. 必改 3：重搬不清表 = 退出 0 + 脏行

两条同源的洞，症状都是「退出码 0 + 位点悄悄前跳 / 脏行留下」：

1. `allDone + 追不回位点 → copyFresh` 当时没置「要清表」，而 `BulkApply` 只有 upsert、
   没有 delete：书签丢失那段里源端删掉的行永远留在目标端，日志还写着「整轮重搬」。
2. 老状态兜底（没有块标记、只有 phase）直接 `copySkip`：`Prepare` 会 drop+recreate 书签，
   位点跳到当前，那段变更永不重放。

现在 `chunkPlan.Clean` 是三态而不是 bool：

- `cleanNone`：首次搬，或上次搬完之后没有空洞；
- `cleanMarked`：半截拷贝 —— 只清「上次写过块」的表；
- `cleanAll`：上次「搬完」了但位点/书签追不回来 —— 选中表整批清（那批行整批都可能过期）。

`snapshot()` 里统一走 `cleanBeforeRecopy`：只清本任务建的表，清不动就停在
`reusedTableRecopyError`，且文案按原因分叉（`recopyHalfCopy` / `recopyGap` / `recopyNoPK`，
以前一律说「上次搬到一半就断了」，在 gap 场景是错的）。老状态兜底也补了
`SlotReplayable && !SlotAhead`，continuous 的四段式同样补了 `SlotAhead`。

新用例 `TestLostSlotAfterCompletedCopyDropsDeletedRows`：完整搬一次 → 造 lost →
源端删 3 行 → 重跑。改前红（`目标端 120200 行、源端 120197 行：源端删掉的行没被带走`），
改后绿（重搬 120197 行、目标端 120197 行、md5 与源端一致）。

## 8. 必改 4：`cleanAll` 的判据不能踩在本轮会改写掉的状态上

上一版把「整批清」判据挂在老状态分支里，而那条分支的证据是 `initialCopyDone(phase, …)`：
`snapshot()` 一开始就 `phaseTo(Snapshot)`，所以只要清表清到一半崩了，下一轮 phase 就是
`snapshot` → 判据假 → 退化成「只 upsert 重搬」→ 空洞期源端删掉的行全留下（退出 0）。

现在把**每轮可重算的事实**提到最前面：

```
unreplayable := StoredCP != "" && (!SlotReplayable || SlotAhead)
if Capable && unreplayable { copyFresh + cleanAll }   // ① 三个出口合一
```

顺序也定死了：`interval/continuous` 早退 → 可重算事实 → 块进度（allDone/续跑）→ 老状态兜底
（只按 phase，且已经被 ① 挡在前面）。`once`/`full_interval` 遇追不回位点同样走 ①（按「要清」实现，
等 Ark 口径）。幂等：重搬完成后 position 与书签确认位点相等 → `SlotAhead=false` → 不会反复清表。

顺带：`copyFresh` 不再带 `Skip`（带了就不盖完成标记 → 下一轮以「续跑」名义空跑一次全量阶段）；
`cleanBeforeRecopy` 按 `selectedTables()` 顺序遍历（原来 map 随机）；`recopyReason.sentence()`
三支用单测钉住。

两条新用例：`TestCleanCrashMidwayRerunsCleanAll`（崩在清表中间→重跑仍整批清：清 1 次、重搬
120197 行、md5 一致；改前红「目标端 120200 行、源端 120197 行」）与
`TestRecopyDoesNotRepeatCleaning`（连续两轮：第二轮清 0 次、搬 0 行、不再进全量阶段，且上一轮
必须留下 1 条完成标记；改前红「应有 1 条完成标记，实际 0 条」）。

## 9. Ark 口径（2026-11-10）：清不清由「是不是整轮重搬」决定

不再是「位点坏没坏才清」，而是**凡是从头搬就清**：`planCopy` 每个 `copyFresh` 出口都挂
`cleanAll`，`unreplayable` 只回答「为什么这轮必须整轮重搬」。

- 于是 `once` / `full_interval` 每轮整轮重搬也清表：这两种模式没有变更流承载 delete，
  清空重建是「本轮结果 = 源端当前状态」的唯一实现；绑在位点上会让同一模式因位点不同
  而对 delete 表现不同。
- 清哪些表：只清**我们写过的**（有块标记）；一张都没写过（首次搬）→ 一行都不清
  （append 复用用户表，清了就是删用户数据）。「老状态」整本块账缺失但有位点/阶段证据时整批清。
  **判据不能是「phase 非空」**：EnsureMeta 在读状态之前就写了 `phase='prepare'`，拿它当
  「搬过」会把第一次搬判成要清表（append 用户表被直接拒）——这个坑我踩过一次。
  兜底那格最终收窄成 `StoredCP != "" || PriorPhase == snapshot`（`copiedBefore`），
  并单测钉住 `{StoredCP:"", PriorPhase:"prepare"}` → `cleanNone` 且 `CleanNames` 为空。
- 缺位点（一块都没搬完）单独一个理由 `recopyNoPos`，不借「书签追不回」，也不会让
  continuous+initial 的用户看到「once / full_interval」这种不相干的话。
- `interval` 仍不追 delete：完成信息里写明「水位增量只补新增与更新，不同步删除」，
  并指向 continuous+initial / 定期 once。
- 清不动（用户自己的表）**不许静默降级**：`reusedTableRecopyError` 给两条都做得到的路
  （recreate / 换空 schema；「手动删掉我们写的行」在 #74「凡从头搬就清表」之后已失效，故不再给出），并说明 `verify` 会逐条报「目标端多出一行」、
  本版还没有按块补 delete 的自动修正。
- 清表时的 note 讲明维护窗口：「从这一刻到这张表搬完之前，目标端是部分数据，不是旧但完整的快照」。

用例四格齐：①搬完+源端删行→重跑一致（`TestLostSlotAfterCompletedCopyDropsDeletedRows`）
②崩在清表中间再重跑（`TestCleanCrashMidwayRerunsCleanAll`）③连续两轮不重复清表
（`TestRecopyDoesNotRepeatCleaning`）④once 两轮之间删行→第二轮清表重搬后一致
（`TestOnceRerunDropsRowsDeletedBetweenRuns`）；另有「首次搬不许清表」两条单测
（含 `phase=prepare` 那条真实首次态）。

## 10. 留给 Ark 的口径（我没自己改文档）

1. `docs/SNAPSHOT.md §4` 现在写「快照 id 已失效 → 整段重来」。实测：导出的视图**必然**
   随进程失效（kill -9 之后一定失效），真正的判据是**书签还能不能从原位点重放**。
   建议改成：「书签追不回原位点（被清理 / 已越过位点）→ 整段重来，并说明原因」。
2. `_jl_chunk` 新增块号 0 = 表完成标记（不新增列、不改 schema）。`docs/STATE-SCHEMA.md`
   的 `_jl_chunk` 说明可以补一句，否则下一个人会以为是脏数据。
3. MySQL 目标端的全量路径还没有块记账 → 不支持块级续跑（判定里显式降级为整轮重搬）。
   要接通需要 mysqlsink 记块进度，属于 ②/③ 那条线。
