# 实现顺序（里程碑，不承诺日期）

每个里程碑的验收都是**可执行的检查**，不是"感觉完成"。

## M0 设计（已完成 2026-11-05）

- 产出：DESIGN / SPEC / TYPE-MAPPING / SNAPSHOT / PLAN / ARCHITECTURE / RUNTIME / STREAMING / CONFLICT / VERIFY / SECURITY / BUILD / STATE-SCHEMA / OPEN-QUESTIONS。
- 完成判据：接口能编译、类型字典有测试、阶段机有测试、CI 绿。

## M0.5 控制面（新增，必须先于 M1）

- 内容：内嵌 SQLite 数据模型（connections/jobs/runs/settings/audit）、凭证加密、REST API、SSE、内嵌 WebUI 骨架（总览 / 连接管理 / 新建任务向导 / 任务详情）、首启建管理员、`serve`/`import`/`export`/`run --job`。
- 验收：从零起 `serve` → 建连接（测连成功）→ 建任务（选表）→ 预检 → run → 详情页 SSE 实时看到阶段与行数；控制库重启后任务仍在；导出 yaml 能被 `schema/job.schema.json` 校验。
- 判据：不经浏览器也能用 API 跑完全流程（headless 可达）。

## M1 核心闭环（PG -> PG）

- 内容：预检 -> 准备 -> 快照（EXPORT_SNAPSHOT + 分块断点）-> catchup -> streaming -> draining。
- 验收：1M/10M 行快照跑通；`kill -9` 后重启不重不丢；影子模型对拍 200 seeds 绿；`plan` 与 `status` 可用。

## M2 MySQL 源

- 内容：binlog 解析、位点（file+pos / GTID 双套）、毫秒级取位点、5.7/8.0/8.4/9.x 能力探测与降级。
- 验收：MySQL 全家族 × 3 场景矩阵绿；8.4 语法与 9.x 认证两个坑有回归用例。

## M3 MySQL 目标（异构）

- 内容：类型映射落地、utf8mb4 行宽预检、upsert 方言、`on_exists` 三种策略。
- 验收：MySQL<->PG 双向映射表全量用例；宽表行宽超限被预检拦住。

## M4 运维面

- `verify` / `doctor` / `gc` / `conflicts` / `dead-letter` / `undo`。
- 验收：故意破坏目标库，`verify` 必须报；孤儿书签能被 `gc --dry-run` 列出且 `--force` 才删。

## M5 五档频次

- 内容：`interval`（水位列 + 重叠回看 + 复合水位）、`full_interval`、超阈值只提示不阻断。
- 验收：只有 SELECT 权限的账号能跑通定时增量；同值/时钟漂移场景不漏数（对拍验证）。

## M6 性能与矩阵门禁

- 内容：L5 基线（吞吐/延迟/内存/冷启）、PR/nightly/release 三门禁、覆盖度报表。
- 验收：单核 >=20k 行/s、p99 <100ms、空转 <80MB；release 覆盖 169 链路。

## M7 二期（不在本期承诺）

同表双向、Web UI、Oracle/达梦/金仓、MQ sink、MariaDB。
