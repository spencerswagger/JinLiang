# B4 · verify / doctor / gc（轨 C）

分支 `feat/b4-verify-doctor-gc`，基线 `9efd877`。

## 已实现

| 能力 | 入口 | 说明 |
|---|---|---|
| verify 结构级 | `jinliang verify --level structure` | 目标表存在、列齐全、类型可映射（用 `internal/catalog` 字典）、可空性收紧、主键一致 |
| verify 数据级 | `--level data` | 行数、等距主键抽样、逐列规范化比对、抽样校验和、主键覆盖 |
| verify 一致性级 | `--level consistency` | PG slot confirmed_flush / MySQL binlog file:pos vs 目标 `_jl_meta.position`；容忍滞后可配；格式不认识 = 红，绝不字符串不等也算过 |
| doctor | `jinliang doctor --source … --target … --job-key …` | 控制库完整性/版本/管理员/幽灵运行（`ended_at IS NULL` 且超时）+ 源端变更流水/书签磁盘 + 目标端运行时表/租约/位点/死信/冲突 |
| gc | `jinliang gc [--apply] [--force-cross-instance]` | 四判定：本实例创建 / 本实例在用 / 其他实例在用 / 无法判定；默认 dry-run、默认只清第一类；unknown 永不删 |
| conflicts | `jinliang conflicts list` | 只读查询 `_jl_conflict`（resolve 未实现，命令里写明） |
| dead-letter | `jinliang dead-letter list / retry --id N [--dry-run]` | 按主键把 `raw_row` upsert 回目标表；成功写 `_jl_action` 并删除死信；不推进权威位点（幂等） |

## 未实现（明确标注，不留半截）

- `jinliang undo`：返回 `undo 今晚未实现：动作回滚需要按 _jl_action 逐类校验…`，退出码非 0。
- `conflicts resolve`：只提供查询；命令提示人工决定后再改目标行。
- `verify --fix`：订正属于引擎轨道，本 PR 不碰写路径。

## 验收证据

### 1) verify 三级正例 / 反例

真实 PG（jinliang_src → jinliang_test）：

```
$ jinliang verify --source $JINLIANG_TEST_PG_SRC_DSN --target $JINLIANG_TEST_PG_DSN \
    --tables jl_cli_demo --job-key clidemo
[structure] OK（1 表，10 ms）
[data] OK（1 表，6 ms）
  统计：map[checksum:a0311eb47f638c1b matched:3 rows_source:3 rows_target:3 sampled:3]
[consistency] 不通过：source_position_missing（物理 slot 没有 restart LSN，如实报红）
```

真实 MySQL（binlog 位点对得上）：

```
$ jinliang verify --source $JINLIANG_TEST_MYSQL_SRC_DSN --target $JINLIANG_TEST_MYSQL_DSN \
    --tables jl_cli_demo --job-key clidemo
[structure] OK  [data] OK  [consistency] OK
  - WARN position_behind：目标端落后源端 422 字节（在容忍范围内）
结论：三级校验通过
```

反例（人为造）：

```
# 改目标一行 → data 红
$ jinliang verify … --level data
  - ERROR row_mismatch jl_cli_demo.amount：源端 s20.00 / 目标端 s21.00

# 删目标一列 → structure 红
$ jinliang verify … --level structure
  - ERROR missing_column jl_cli_demo.name

# 目标位点超前 → consistency 红（单测 + 集成测试）
  - ERROR target_ahead / position_gap / position_unparseable
```

单测同时钉住：缺列 + 类型不符、行数差 + 值差、缺行、位点超前、位点落后超阈值、位点不可解析、位点不可得（不能算通过）。

### 2) gc 四判定

`go test ./internal/gc`：

- `OwnedByThisInstance` → 唯一一条 `Delete=true`
- `InUseByThisInstance`（租约 / active）→ 不删
- `InUseByOtherInstance`（其他实例租约 / active）→ 不删；`--force` 也不动「仍被租用/active」的对象
- `Unknown`（未登记 / 登记表读不到）→ 即使 `--force` 也不删，只报告

真实库集成测试 `TestGCIntegrationOtherInstanceSlotIsNeverTouched`：
造 3 个真 slot（别人的 + 我的空闲 + 未登记），断言 dry-run 一个不删；apply 只删 `jl_gctest_2`；
`jl_gctest_1`（其他实例租用）与 `jl_gctest_3`（未登记）都还在。

本机实跑（同库还有别的轨道的真实 slot）：

```
jl_clidemo_1      unknown                  delete=false
jl_t32nfdq5iz_1   in_use_by_other_instance delete=false（active=true）
jl_t32nfdq5iz     in_use_by_other_instance delete=false（其他实例有效租约）
deletable=0
```

### 3) 集成测试缺 DSN 必须红

```
$ env -u JINLIANG_TEST_PG_SRC_DSN go test -tags=integration -run TestPGVerifyIntegration ./internal/verify
--- FAIL: TestPGVerifyIntegration
    integration_test.go:47: JINLIANG_TEST_PG_SRC_DSN 未设置：集成测试需要真实数据库；缺 DSN 必须失败，不允许跳过
FAIL
```

DSN 名只用真名：`JINLIANG_TEST_{PG,MYSQL}[_SRC]_DSN`，先 `env | grep JINLIANG_TEST` 核对；无硬编码。

### 4) 门禁

```
$ gofmt -l .            # 空
$ go vet ./...          # 通过
$ go test ./...         # 全绿
$ go test -tags=integration ./internal/verify ./internal/gc ./internal/doctor ./internal/remedy   # 全绿（B6 起每包独立 schema）
$ scripts/check_binary_size.sh dist/jinliang
binary dist/jinliang: 19591328 bytes (18 MB), limit 25 MB → OK
```

## Owl 复审整改（4 条，各自一个 commit）

1. **幽灵 run 不再按 started_at 判死**：`RunInfo` 加 `HeartbeatAt` / `HeartbeatKnown`，
   判定改为 `COALESCE(heartbeat_at, started_at)` + `StaleAfter(30s)=90s`（与
   runs-liveness 同一口径）；旧控制库没有心跳列时只 `warn`，不把健康系统判红。
   适配器直接探测 `runs.heartbeat_at` 列（#5 合并前后都可用）。
   测试：`TestLongRunWithFreshHeartbeatIsHealthy`、`TestLongRunWithoutHeartbeatIsWarningNotProblem`、
   `TestGhostRunDetected`（心跳过期才 problem）。
2. **publication 不再被默认档误删**：第二遍判定——同 `job_key` 的 slot 仍 `active`
   时，publication 一并判 `InUseByThisInstance`/`InUseByOtherInstance` 且 `Delete=false`。
   测试：`TestPublicationProtectedWhileSameJobSlotIsActive`、`TestPublicationIsEligibleWhenJobIsIdle`；
   真库用例 `TestGCIntegrationOtherInstanceSlotIsNeverTouched` 里补了
   `CREATE PUBLICATION jl_gctestpub` → 计划可清 → 真删 → 源端消失。
3. **版本化 binlog 位点**：`BinlogPos` 先解析 `file=…;pos=…`（带不带 `mysql:v1;` 前缀都行），
   再退回 `file:pos`；`gtid:` 明确 `ok=false`。
   测试：`TestBinlogPosFormats`（9 个用例，含两种格式与畸形输入）。
4. **remedy 有测试了**：`TestRetryDeadLetterIsIdempotentAndDoesNotMovePosition`
   ——重放两次：目标表只有一行、第二次报「不存在」、`_jl_meta.position` 保持 `POS-1`。

CI 集成步骤同步加上 `./internal/remedy`。

## Owl 复审二（跨轨必改）：PG 版本化位点

- `ParseLSN` 先用 `source.SplitPosition` 剥 `<kind>:v1:` 壳（Ark 定的唯一入口），
  再解析裸 LSN；`postgres:v1:0/16B6C50` 与「两边都版本化」都能比较。
- 外来 kind（`mysql:v1;file=…;pos=…` / `mysql:v1:…`）返回
  `ErrPositionKindMismatch`，verify 报 `position_kind_mismatch`（E1010），
  **不** 报 `position_unparseable`；载荷坏才是 `ErrPositionMalformed`。
- MySQL 侧对称：`ParseBinlogPos` 同时认 `mysql:v1:<file:pos>` 与
  `mysql:v1;file=…;pos=…`，PG 位点传给它会报 E1010。
- 用例：`TestParseLSNBare`、`TestParseLSNVersioned`、`TestCompareLSNBothVersioned`、
  `TestParseLSNVersionedBadPayload`、`TestParseLSNRejectsForeignKind`；
  verify 层 `TestConsistencyAcceptsVersionedPositions`、`TestConsistencyForeignKindIsE1010`；
  MySQL `TestBinlogPosFormats`、`TestParseBinlogPosForeignKind`。

P3：main 已有 `race` job（`go test -race ./...`），合并 #5 后自动覆盖本分支的
verify/gc/runtime/doctor/remedy 单测，不再另加。`-p 1` 的夹具权宜已由 B6 清掉：
集成测试改成每包独立 schema（jl_test_verify / jl_test_gc / jl_test_remedy），
可以并行跑，CI 不再串行。

## 共享契约与边界

- 未改 `internal/source/contract.go`、`internal/sink/contract.go`；未动 `internal/engine`、`internal/mysqlsource`、`internal/control`、`internal/httpapi`。
- 新增 `internal/runtime` 作为 `_jl_*` 表的唯一访问点（两种方言），避免与引擎/连接器复制 SQL。
- `cmd/jinliang/commands.go` 只改了 `cmdDoctor`（+ 新增文件），`main.go` 只加命令分发与 usage。
- CI：在既有 `control-plane` job 内补第二套测试库与 4 个 DSN，加一步 `go test -tags=integration`；未新增 job、未改其余 3 个 job。

## 已知限制

- 数据级抽样是「单列整数主键等距抽样；否则按主键前缀抽样」，不是随机抽样（如实写进统计）。
- 一致性级不解析 GTID；MySQL 8.4 若只暴露 GTID，会走 `gtid:` 字符串比较，格式不相同时报 `position_unparseable`。
- 冲突处理只有查询；`resolve` / 自动订正留给后续。
