# 津梁 JinLiang

> 津梁：渡口与桥。在异构数据库之间摆渡数据。

**状态：M1 实现中。** 控制面（内嵌 SQLite + WebUI）、PG→PG 引擎、MySQL 源、`verify`/`doctor`/`gc` 已落地；调度（B5）与性能优化进行中。

## 这是什么

一个用 Go 写的数据库同步工具，目标是**让不懂数据库内部机制的人也能把同步做对**：
用户只谈自己的数据（哪些表、哪些列、多久一次、哪个方向），
WAL / binlog / 复制槽 / 位点这些底层概念由工具负责探测、代做、补偿、清理。

## 一期范围

| 组合 | 方向 |
|---|---|
| MySQL → MySQL | 单向（含跨版本割接） |
| PostgreSQL → PostgreSQL | 单向（含跨版本割接） |
| MySQL ↔ PostgreSQL | 单向 |

版本：PostgreSQL 10–18（9 个大版本）、MySQL 5.7 / 8.0 / 8.4 / 9.x。
不做：同表双向、Web UI、其他数据库。

## 设计要点

- **预检三级**：已满足 / 我可代做（可逆、不动数据、不重启）/ 必须你或 DBA（给定制 SQL + 影响 + 回滚）。
- **降级先补偿、不拒绝**：自动补偿 → 有代价替代 → 最后只给提示（含风险与改善路径），决定权在你；降级不降正确性。
- **不拒绝，只提示**：无主键表照跑（insert-only）、超大表照跑、MyISAM 照跑、不认识的类型按 text 落地 —— 全部只提示风险。只有「物理做不到」与「私钥文件权限不对」会停。
- **提示即文档**：不做文档站；每条提示自带「发生了什么 / 为什么 / 你能做什么 / 风险」。
- **垃圾清理**：孤儿复制槽四条判定，默认只列不删；崩溃恢复靠租约 + 位点与数据同事务。
- **能力运行时探测**，不按版本号分支（8.4 移除了 `SHOW MASTER STATUS`，PG 10 没有 slot 磁盘阀）。
- **测试矩阵驱动**：每个连接器必须带 `testmatrix.yaml`，缺失即 CI 红；影子模型对拍一致性。

## 文档

**设计（全部完成）与实现状态**

| 文档 | 内容 |
|---|---|
| **[docs/CONTROL-PLANE.md](docs/CONTROL-PLANE.md)** | **控制面（最新口径）：内嵌数据库 + WebUI 是一等公民；两个数据库的分工、数据模型、API、凭证加密、UI 结构与测试** |
| [docs/DESIGN.md](docs/DESIGN.md) | 设计基线：原则、版本矩阵、降级补偿、性能与测试架构 |
| [docs/SPEC.md](docs/SPEC.md) | `job.yaml`、命令面与退出码、预检清单、错误模型、指标 |
| [docs/TYPE-MAPPING.md](docs/TYPE-MAPPING.md) | 类型映射字典与探测兜底 |
| [docs/SNAPSHOT.md](docs/SNAPSHOT.md) | 全量初始化：分块、一致性起点、断点 |
| [docs/STREAMING.md](docs/STREAMING.md) | 增量流：事务组装、批提交、位点、背压与限速 |
| [docs/CONFLICT.md](docs/CONFLICT.md) | 冲突策略、死信、人工介入 |
| [docs/VERIFY.md](docs/VERIFY.md) | 三级一致性校验与订正 |
| [docs/STATE-SCHEMA.md](docs/STATE-SCHEMA.md) | `_jl_*` 状态表、位点同事务的幂等证明、崩溃恢复时序 |
| [docs/RUNTIME.md](docs/RUNTIME.md) | 生命周期状态机、暂停恢复、热更新、时钟 |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | 分层与依赖方向、数据流、背压、扩展点 |
| [docs/SECURITY.md](docs/SECURITY.md) | 凭证、最小权限账号、TLS、日志、审计、供应链 |
| [docs/BUILD.md](docs/BUILD.md) | 构建、发布物、systemd、升级与状态表迁移 |
| [docs/PLAN.md](docs/PLAN.md) | `plan` 的人类模板与 `--json` 输出、确认规则 |
| [docs/ROADMAP.md](docs/ROADMAP.md) | 里程碑与每阶段的验收判据 |
| [docs/IDENTITY.md](docs/IDENTITY.md) | 身份与命名：多实例连同一个库如何不冲突、不误删、不双写 |
| [docs/ERRORS.md](docs/ERRORS.md) | 错误码全集（四段式提示：发生了什么/为什么/能做什么/风险） |
| [docs/CONNECTOR-SDK.md](docs/CONNECTOR-SDK.md) | 新增一个数据库要做什么：接口、建表规则、测试矩阵、验收五条 |
| [docs/DECISIONS.md](docs/DECISIONS.md) | 14 条决策记录（选了什么/为什么/代价），改设计前先读 |
| [docs/OPEN-QUESTIONS.md](docs/OPEN-QUESTIONS.md) | 已决与未决清单 |
| [schema/job.schema.json](schema/job.schema.json) | 任务定义的 JSON Schema（编辑器与 AI 助手都靠它理解配置） |
| [examples/job.yaml](examples/job.yaml) | 任务定义示例（带全部字段与注释） |

代码层契约已可编译、可测试：`internal/source/contract.go`、`internal/sink/contract.go`、
`internal/catalog/typemap.go`（15 个用例）、`internal/state/phase.go`（状态机 17 个断言）。

## 覆盖度报表

口径：下表是 `go test ./...` 的**单元测试**语句覆盖；`engine` / `pgsource` /
`pgsink` / `mysqlsource` / `runtime` / `remedy` 的真实覆盖来自
`go test -tags=integration`（需要真库，nightly 跑），所以它们的单元数字偏低
不代表没测。`sink` / `testkit` / `transform` 目前只有契约与文档，没有可执行测试。

<!-- coverage:start -->
| 包 | 语句覆盖 | 口径 |
|---|---:|---|
| `cmd/jinliang` | 0.3% | 单元 |
| `internal/catalog` | 68.0% | 单元 |
| `internal/connect` | 72.3% | 单元 |
| `internal/control` | 65.4% | 单元 |
| `internal/core` | 84.6% | 单元 |
| `internal/doctor` | 45.0% | 单元 |
| `internal/dsn` | 87.7% | 单元 |
| `internal/engine` | 14.4% | 单元 + 集成（nightly） |
| `internal/gc` | 68.8% | 单元 |
| `internal/httpapi` | 68.0% | 单元 |
| `internal/mysqlsource` | 51.2% | 单元 + 集成（nightly） |
| `internal/netcheck` | 96.4% | 单元 |
| `internal/pgsink` | 17.4% | 单元 + 集成（nightly） |
| `internal/pgsource` | 8.8% | 单元 + 集成（nightly） |
| `internal/preflight` | 88.8% | 单元 |
| `internal/runtime` | 11.6% | 单元 + 集成（nightly） |
| `internal/source` | 96.8% | 单元 |
| `internal/state` | 82.2% | 单元 |
| `internal/verify` | 60.3% | 单元 |

**总语句覆盖：44.1%**（单元测试；集成覆盖见 nightly 产物）
<!-- coverage:end -->

## 性能基线

`scripts/bench.sh` 一次跑出五项：快照吞吐、增量 p99、空转内存、冷启、二进制体积。
口径固定（10 万行 / 每批 500 / 2 批预热），报告写 `bench/latest.json`；当前基线在
`bench/baseline.json`（release 用它做 >15% 回归对比；CPU 型号不同时只查绝对阈值）。

```bash
JINLIANG_TEST_PG_DSN=... scripts/bench.sh            # 报告模式
JINLIANG_TEST_PG_DSN=... scripts/bench.sh --enforce  # 门禁：任一项不达标退出 1
```

本机基线（AMD Ryzen 5 1500X / 8 vCPU / Linux 5.15）：快照 **167,965 行/s**、
增量 apply 10,544 行/s、p99 **61.7 ms**、空转 **15.9 MB**、冷启 **27 ms**、二进制 **19.12 MB**。
20k 行/s 的目标是**快照 COPY** 路径；增量 apply 当前逐行 Exec（500 次/事务），
约 10k 行/s，这是已知差距——基线把它固定下来，避免以后悄悄变慢。

## 自检

```bash
gofmt -l .                                   # 空
go vet ./... && go test ./...                # 绿
python scripts/validate_schema.py examples/*.yaml
```

## 目录

```
cmd/jinliang/        入口：serve / import / export / run --job / doctor
internal/control/    控制面：SQLite 存储、凭证加密、REST API、SSE、内嵌 WebUI
internal/            core / source / sink / transform / state / preflight / catalog / gc / testkit
connectors/          连接器（各带 testmatrix.yaml）
docs/                设计文档
web/                 模板 + 静态资源（embed 进二进制，无 Node 构建）
```

## 许可证

MIT
