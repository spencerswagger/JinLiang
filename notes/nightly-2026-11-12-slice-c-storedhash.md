# 片 C 的 Data 侧：`StoredJobHash` + 未绑 `pauseReg` 时的暂停可见性

基线 main `3dc56bc`。分支 `feat/data-slice-c-storedhash`。不改引擎状态机（`pauseReg` 绑定是 Core 的事，**故意不绑**照旧）。

---

## 1. `StoredJobHash`：现状（缺）→ 已补

**改动前**：`internal/mysqlsink` **没有** `StoredJobHash`。全仓只有 `pgsink` 有；
`internal/engine/engine.go:1144` 的 `reconcilePaused` 直接调 `r.sink.StoredJobHash(ctx)`（pgsink 类型），
注释里写着「`mysqlsink` 也还没有这个方法」——这就是 Core 说的片 C 缺口。

**存储层本来就有这个值**：`_jl_meta.job_hash`（`EnsureMeta` 每次 prepare 写，`ON DUPLICATE KEY UPDATE` 会刷新）。
缺的只是读回它的方法，而且**它不在契约 §4.2 的六个方法里**，所以 `PauseRegistry` 接口拼不齐。

**改动**：`internal/mysqlsink/paused.go` 新增

```go
// 没有状态行（首次运行前）返回空串：引擎把「空」当成「没有历史」处理。
func (s *Sink) StoredJobHash(ctx context.Context) (string, error)
```

**评审修的一条（关键）**：`EnsureMeta` 原来在冲突分支里刷新 `job_hash`，于是**读回来恒等于本轮 hash**，
`sameJob := stored == "" || stored == hashOf(doc)` 恒真——判据空转（PG 侧的 `EnsureMeta` 不刷新，
MySQL 照抄就成了 no-op）。已去掉 `job_hash=VALUES(job_hash)`，并补了「h1 → h2 → 读回仍是 h1」那一格
（原来只有「写一次能读回」，抓不到覆盖）。顺带修掉 `writePosition` 把 `job_key` 当 `job_hash` 传的旧笔误
（与 pgsink 对齐成空串）。

**与 `paused_tables` 的写入是不是同事务**：不是，也不需要。`job_hash` 由 `EnsureMeta` 在 prepare 写、
`paused_tables` 由 `PauseTable` 单独事务写；判据是「读回来的 hash 是否等于本轮 hash」，
读晚一点只会更准（本轮 hash 在 prepare 就定了）。真正要保证的是「读得到」——
读不到时 reconcile 只能全清或干脆不判，两种都会误删/误留记录。

**用例**：`TestIntegrationStoredJobHashSurvivesRestart`（真 MySQL 8.0.28，PASS 0.41s）

- 没有状态行 → 返回空串、不报错；
- `EnsureMeta(hash)` 写 → `Close()` → **新实例**（模拟重启）→ 读回**逐字相等**；
- 再 `PauseTable` → hash **不变**（两个写互不干扰）。

---

## 2. 未绑 `pauseReg` 时：`status`/`doctor` 必须说清「记录在、但写路径不认」

**前提（Core #99 的口径，照旧）**：MySQL 目标端**故意不绑** `pauseReg`——恢复路径（reconcile / 强制重搬 /
搬成解除）仍在 PG 侧，绑上去就是「流式期暂停永不解除」的静默缺数。所以本轮**没有**加任何绑定。

**这个前提下的真实风险**：存储层有记录、而写路径根本不看它 → 用户看到「已登记暂停」以为表停了，
实际还在写，目标端与源端不一致。所以展示层不能只抛一个 `map[table]record`。

**改动**（`internal/mysqlsink/paused.go`，Data 侧自足，不依赖引擎）：

```go
const (
    PauseEnforcementStorageOnly = "storage-only"   // 记录能读写清，但引擎侧还没接管
    PauseEnforcementEngineBound = "engine-bound"   // 片 C 绑上之后改成这个
)
func (s *Sink) PauseEnforcement() string
func (s *Sink) PauseSummary(ctx) PauseView       // Records / Tables / Dirty / Enforcement / ReadErr
func (v PauseView) Lines() []string               // status / doctor 直接拼进输出
```

三个附加事实缺一不可：`Dirty`（JSON 坏了、已按无暂停继续）、`Enforcement`（写路径认不认）、
`ReadErr`（读失败**绝不**返回空集合假装「没有暂停」）。

**`Enforcement` 由引擎填，不由 sink 自报**（评审改的）：sink 不可能知道引擎有没有把它包进
`pauseSink`，让它自报「我被绑定了没有」就是替别人声明，而且绑定时忘了改也不会红（preflight
PAUSE-01 刚踩过同一个坑）。所以 Data 侧只给词汇表（`PauseEnforcementStorageOnly/EngineBound`）
与纯函数 `RenderPauseView(v PauseView) []string`，值由调用方（引擎）填。

**输出样例**（`TestIntegrationPauseViewWhenEngineNotBound` 真库日志原文）：

```
暂停记录     1 张表：jl_b3_view_tbl
              ⚠ 这些记录**只存在存储层**（pause enforcement=storage-only）：当前目标端的引擎侧尚未接管暂停，被记录的表**仍在被写入**，目标端数据可能与源端不一致。
              本版**不会**自动重搬、也不会自动解除这些记录：请手工把结构对齐并核对数据；引擎侧接管之后才会走「整表重搬补齐 → 搬成解除」。
```

（第一版这句写的是「重跑即可，本轮会整表重搬补齐」——storage-only 下那是**假话**：引擎不认这些记录，
重跑既不 `MarkTableRecopyPending` 也不重搬。评审抓到并用例现在**反向钉住**：storage-only 文案里
不许出现「重跑即可 / 本轮会整表重搬补齐」，engine-bound 那一支才允许。）

读失败时（`_jl_meta` 被删/权限被收）：

```
暂停记录     读不到：MySQL 目标端不支持按表暂停：给 `_jl_meta` 补 `paused_tables` 列失败…（不要当成「没有暂停」——先修权限/连接再判断）
```

用例：`TestIntegrationPauseViewWhenEngineNotBound`（PASS 0.11s，断言含表名 / `storage-only` / 「仍在被写入」/
「整表重搬补齐」）、`TestIntegrationPauseViewReadFailureIsNotSilent`（PASS 0.22s，断言「别当成没有暂停」）。

**当前接线状态（比「没有暂停小节」更严重）**：`engine.StatusFor` **无条件**调
`pgsink.ReadState`（PG 专用），MySQL 目标端必然失败 → `Note="目标端还没有运行状态（还没跑过）"`、
phase/position 全空。也就是说，对一个**正在跑的 MySQL 任务**，`jinlang status` 会说「还没跑过」。
这不是「整段不显示」，是**给出了错的结论**。
`PauseSummary/Lines` 就是给 `status`/`doctor`（契约 §1.8，Core 的清单）现成的一段，接上去一行调用即可；
在那之前，Data 侧只有这个方法能看见状态。

---

## 3. P1 / #94 订正的状态

- `chunk_no=0` 重放不刷新：**已完成**，随 **#103 / `e368938`** 合入（MySQL+PG 两侧 `MarkTableDone`
  都刷新 rows，两侧各一格复现用例；`MarkTableRecopyPending` 后首块起点也有用例）。
- #94 两处注释订正：**已完成**，随 **#104 / `3dc56bc`** 合入（判据改成与正文逐条对应的五条，
  notes §① 改成「当时缺口、现已解」；全文不再把作废旧口径当现状）。
- 本条（片 C Data 侧）**插在其后**，顺序按要求。

---

## 自检

- `gofmt -l .` 空；`go vet ./...` 绿；`go test ./...` 全绿；
- `-tags=integration ./internal/mysqlsink`：全绿（含新增 3 格：StoredJobHash 跨重启 / PauseView 未绑形态 / 读失败不静默）。
