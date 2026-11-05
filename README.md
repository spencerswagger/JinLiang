# 津梁 JinLiang

> 津梁：渡口与桥。在异构数据库之间摆渡数据。

**状态：设计阶段，暂无可用功能。** 当前仓库只有骨架、连接器可测性清单与设计文档。

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

**设计（全部完成，代码尚未实现）**

| 文档 | 内容 |
|---|---|
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

## 自检

```bash
gofmt -l .                                   # 空
go vet ./... && go test ./...                # 绿
python scripts/validate_schema.py examples/*.yaml
```

## 目录

```
cmd/jinliang/        命令行入口
internal/            core / source / sink / transform / state / preflight / catalog / gc / testkit
connectors/          连接器（各带 testmatrix.yaml）
docs/                设计文档
```

## 许可证

MIT
