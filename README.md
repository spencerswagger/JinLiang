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
- **降级先补偿**：自动补偿 → 有代价替代 → 才拒绝；降级不降正确性。
- **垃圾清理**：孤儿复制槽四条判定，默认只列不删；崩溃恢复靠租约 + 位点与数据同事务。
- **能力运行时探测**，不按版本号分支（8.4 移除了 `SHOW MASTER STATUS`，PG 10 没有 slot 磁盘阀）。
- **测试矩阵驱动**：每个连接器必须带 `testmatrix.yaml`，缺失即 CI 红；影子模型对拍一致性。

## 文档

| 文档 | 内容 |
|---|---|
| [docs/DESIGN.md](docs/DESIGN.md) | 设计基线：原则、版本矩阵、降级补偿、性能与测试架构 |
| [docs/SPEC.md](docs/SPEC.md) | 实现级：`job.yaml`、命令面、预检清单、错误模型、指标、校验协议 |
| [docs/STATE-SCHEMA.md](docs/STATE-SCHEMA.md) | 状态表 DDL、位点与数据同事务的幂等证明、崩溃恢复时序、孤儿清理 |
| [examples/job.yaml](examples/job.yaml) | 任务定义示例（带全部字段与注释） |

代码层的契约已经落地，可编译可校验：`internal/source/contract.go`（连接器接口与能力探测）、`internal/sink/contract.go`（写入与同事务提交位点）。

## 目录

```
cmd/jinliang/        命令行入口
internal/            core / source / sink / transform / state / preflight / catalog / gc / testkit
connectors/          连接器（各带 testmatrix.yaml）
docs/                设计文档
```

## 许可证

MIT
