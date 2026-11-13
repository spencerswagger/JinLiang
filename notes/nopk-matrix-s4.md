# S4：无主键变体矩阵落地（PR / nightly / release 格数 + 待接通 12 格）

> **回填（2026-11-13，`#124` 之后）**：定稿 §2.4 已按本文件的真实算术改成 **T4 `3/3/1/1` = 8/周、
> nightly 小计 38.1**，`docs/DESIGN.md` §2.3/§3 的「降级只验提示并允许继续」已改成「只验允许继续」
> （降级提示卡记为 `DESIGN-NO-PRIMARY-KEY.md §7 D12`）。下面第 1 节记的是**落地当时**的差异，留作证据，不再是最新口径。

定稿：`docs/DESIGN-NO-PRIMARY-KEY.md` §2.4（矩阵新增）、§5 S4。

## 1. 格数对账（真实算术，没有改数字迁就管线）

| 层 | 定稿 | 实际 | 说明 |
|---|---|---|---|
| PR 门禁 | 10 | **10** | PG18→18(3) + MySQL8.4→PG(3) + MySQL5.7→PG(3) + PG18→MySQL(1，只全量) |
| nightly 固定 | ≈39 | **37/晚 + T4 8/周 = 38.1/晚** | 定稿自己的算术是 38.7（进位 ≈39），差 0.6/晚，见下 |
| nightly T4 全量 | 12/周 | **8/周** | MySQL→PG 2 条 ×3 + PG→MySQL 2 条 ×1 |
| release | 184 | **184** | 27+6+4+144+3 |
| 待接通 | 12 | **12**（pending，永远不算 pass） | MySQL→MySQL 4 条 T1 ×3 |

**nightly 那 1 格不是丢了，是产品口径下不存在**：定稿 T4 按「4 条链 × 3 场景 = 12/周」算，
但 T4 的 2 条 PG→MySQL 链**只全量**（`internal/core/direction.go` 的 Modes），
3 场景里只有 `snapshot` 可达 → 真实是 2×3 + 2×1 = 8/周。定稿的「≈39」是估算，
这里按**登记的格**算成 38.1。已在 `test/nopk-matrix.yaml` 头部写明。

## 2. 落了什么

| 文件 | 作用 |
|---|---|
| `test/nopk-matrix.yaml` | 格的**唯一登记表**：场景定义 + 块（链路 × 版本对 × 场景）+ 四层（pr/nightly/release/pending）。数字从这里算出来 |
| `scripts/nopk_matrix.py` | 逐格执行器：展开 → 选格（层/链路/版本/场景/本周 due）→ 调 `TestNoPKMatrixCell` → 每格一行 `RESULT nopk …`。缺 DSN 退出 78（红），pending 永不进 pass |
| `internal/engine/integration_nopk_matrix_test.go` | 格的**可执行承载**（真库、真写路径、真暂停），构建标签 `integration && nopkmatrix`：只有执行器带 `nopkmatrix`，普通集成 job（b1-engine / nightly integration）不会把它跑起来。11 个链路×场景分支，未接通的链路直接在测试里拒跑 |
| `scripts/check_testmatrix.py` | 新增 `check_nopk()`：算术（10/37+8/184/12）、方向与场景相容、待接通必须 pending、跨文件（连接器 `no_pk` 段）一致 |
| `connectors/{mysql,postgres}/testmatrix.yaml` | 各自的 `no_pk` 段：MySQL `pending_cells: 12`，PG `0` |
| `.github/workflows/merge-gate.yml` | 新 job `无主键变体 × MySQL 5.7/8.4（PR 10 格）`，门控在**步骤级**（矩阵 job 不用 job 级 `if`，#109 的教训） |
| `.github/workflows/nightly.yml` | pg-matrix 加 T1 自洽（3 格/版本）+ T4 抽样（PG→MySQL）；mysql-matrix 加 T4 抽样（MySQL→PG）；新 job `nopk-pairs` 跑 T2/T3（含 18→10 降级） |
| `.github/workflows/release.yml` | `nopk-release-cross`（4×9=36 job，各 4 格）+ `nopk-release-pg`（13 job，各 3–5 格）= 184 格 |
| `scripts/ci/nopk_dbs.sh` | CI 起真库（PG 对 / 跨库对），就绪判据是**真查询**（PG 连续两次、MySQL `select 1`），并断言 `wal_level=logical` / `binlog_row_image=FULL` |

## 3. 两处「按产品真实行为改格」的记录

1. **`unique_key_once` 不能按「upsert 幂等」写**。`once` / `full_interval` 是**每轮整轮重建**
   （`internal/engine/target.go` 的 `sinkDeclaredUniqueKeys` 对这两种模式返回 nil，§4.2），
   声明不进写路径。第一版我按「预置同键行 + once 快照合并」写，本地真库直接红：
   `duplicate key value violates unique constraint`（没有 ON CONFLICT）。改成验真实口径：
   **连跑两轮不翻倍、且第二轮结果等于源端当前状态**（中间在源端改一行、删一行）。
   改/删的 upsert 由 `unique_key_continuous` 那一格覆盖（continuous 下声明生效，实测绿）。
2. **降级格只证「允许继续」**（文案已按事实改，不再说「提示并允许」）。产品今天**没有**降级专属
   提示卡片（全仓 `TargetVer` 只用于 `preflight/b0.go:53` 的回显，没有任何版本比较），本格断言的是
   `preflight.CanStart == true` + 两个版本在 NET-01/NET-02 的 Detail 里可见 + 引擎照跑搬到行数一致；
   测试里显式 `t.Logf` 写清「不冒充已提示」。要真做到「提示并允许」得产品加一张卡片——这是欠账，
   不在 S4 范围（不改产品代码）。

## 4. 本地证据（真库：PG 17.11 + MySQL 8.0.28，`scripts/local_stack.sh up`）

逐格直接调测试（等价于执行器选中的那格）：

| 链路 | 场景 | 结果 |
|---|---|---|
| pg-pg | snapshot / insert / pause / unique_key_once / unique_key_continuous / downgrade(17→17，仅冒烟) | 全绿 |
| mysql-pg | snapshot / insert / pause | 全绿 |
| pg-mysql | snapshot / unique_key_once | 全绿 |
| mysql-matrix（待接通） | snapshot | **红**（「方向未接通」）：证明这 12 格不会被算成绿 |

执行器本身也跑通：`scripts/nopk_matrix.py --layer nightly --link pg-pg --src-version 17 --dst-version 17`
→ `NOPK_SUMMARY cells=3 pass=3 fail=0`。

**本地证不了的**（必须等 CI）：
- 多版本档（PG 10–16/18、MySQL 5.7/8.4/9.x）：本机只有 PG 17 + MySQL 8.0.28；
- 真正的降级（18→10）与 T2/T3 跨版本；
- `mysql-pg` 的 non-8.0 档。

## 5. 矩阵仍然证明不了的一格（必须写进 PR）

**MySQL 目标端 + 增量改/删 → 暂停**：PG→MySQL 只全量（`direction.go`），这一格没有可执行的链路。
归到 D6 同一笔挂账（MySQL→MySQL 未接通）。「MySQL 目标端 insert-only」今天只有
PG→MySQL 全量格 + 单测证明——别把「矩阵绿」读成「MySQL 目标端已全覆盖」。

## 5.5 Owl II 评审后的修正（P0×1 / P1×2 / P2×3 全部落地）

| 编号 | 问题 | 修法 |
|---|---|---|
| P0-1 | `nopk-pr` 的 summary 步在 gate=false（只改文档）时 `[ -f ] && …` 返回 1 → **job 红**，一旦加进 required 就把 docs-only PR 全卡死（#109 的另一种形式） | summary 步改 `shopt -s nullglob` + `|| true`（四个 workflow 同一写法），只做整形、不冒充失败原因 |
| P1-1 | 选格为空也 exit 0 → 新假绿入口（`--src-version 99` → `cells=0` 绿） | 空选 → `EX_CONFIG` rc=2；新增 `--expect-cells N`（PR 腿 3/3/4、nightly T1 3、T4 1、pairs 3/3/3/1 全写进 CI） |
| P1-2 | 降级格文案写「提示并允许」，产品根本没有降级提示 | 登记表/notes/PR/CI 标签统一成「只验允许继续；提示未实现」，并报 Ark 核 §2.4 T3 那半句 |
| P2-1 | `--include-pending` 是空开关（pending 分支在判断之前就 continue） | 真跑 pending 格（用例会红，rc=1）；显式 `--layer pending` 只报挂账（pending=12、rc=0） |
| P2-2 | 「差 1 格」口径不准 | 改成「定稿算术 38.7（进位 ≈39）→ 实际 38.1，差 0.6/晚 = T4 少 4 格/周」，并点明是定稿 T4 行与 release 行的内部不一致 |
| P2-3 | release 的 49 个 job 一次没跑过（`on: push: tags` 且无 dispatch） | release 加 `workflow_dispatch`；新增 `.github/workflows/nopk-dry-run.yml`（手动、只起一套真库，参数与 CI 一致）先干跑 cross / pg 各一次 |
| P3 | `FULL_ONLY_LINKS` 零引用、`expand` 的 `if_due` 未用、临时文件不删、`mysql-matrix` 死分支、`no_pk.scenarios` 不校验、gate 正则漏 `scripts/ci/` 与 nightly/release、mysql-pg 收敛后不验新值、`want` 与定稿零绑定 | 全部收掉：删死代码、`TemporaryDirectory` 自动清理、`no_pk.scenarios` 按「声明==实际用到」校验、gate 正则补 `scripts/ci/` 与四个 workflow、mysql-pg 补 `v=1042` 断言、校验器把定稿 §2.4 的 `**10**`/`**184 格**`/`**待接通 12 格**`/`≈ 39` 当锚点（文档改了不同步就红） |

## 5.6 CI 首跑踩到的一条（已修）

第一版把用例挂在 `integration` 一个标签下 → `b1-engine` 跑整包集成时把它也跑起来，
缺 `JINLIANG_NOPK_LINK` 直接红（`--- FAIL: TestNoPKMatrixCell (0.00s)`）。
改成 `integration && nopkmatrix` 双标签（执行器 `-tags=integration,nopkmatrix`），
并给执行器加一条守卫：**rc=0 但日志里没有 `--- PASS: TestNoPKMatrixCell`** 也算失败
（防「改了用例名/漏了标签 → 一个用例都没跑却报绿」）。

## 6. 自检

```
gofmt -l .                     → 空
go vet ./...                   → 0
go test ./...                  → ok
python scripts/validate_schema.py examples/*.yaml → ok
python scripts/check_testmatrix.py
  testmatrix: postgres 9/9 (81), mysql 4/4 (16), cross 72 — 分层正确
  nopk: pr 10/10 · nightly 37 每晚 + T4 8/周（≈38.1/晚，定稿 ≈39）· release 184/184 · 待接通 12/12（pending，不计入覆盖）
相关集成用例（既有 no-pk 组 + 新格）→ 全绿
```
