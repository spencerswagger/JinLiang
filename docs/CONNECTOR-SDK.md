# 连接器开发指引（新增一个数据库要做什么）

一期只做 postgres / mysql，但接口从第一天就是给第三方用的。

## 1. 五个文件

```
connectors/<name>/
  source.go       实现 source.Source（读侧）
  sink.go         实现 sink.Sink（写侧，可选）
  catalog.go      枚举库表、探测能力、类型字典补充
  typemap.go      该库与目标库的映射规则（或在 catalog 里注册）
  testmatrix.yaml 可测性清单 —— 没有它 CI 直接红
```

## 2. 读侧要实现的 8 个方法

`Kind / Version / Probe / Catalog / Preflight / Prepare / Snapshot / Stream / Advance / Cleanup`
（接口见 `internal/source/contract.go`）。

三条硬要求：

1. **能力探测而不是版本判断**：`Probe` 返回 `CapSet`，缺能力时必须带 `Compensation{Level, Note, ConfigKey}`。
2. **位点可序列化**：`Position.String()` 的结果会写进目标库；同一个字符串必须能反解回同一位置。
3. **`Cleanup` 幂等**：重复调用不报错，且**只清理自己命名空间**（`jl_<job_key>`）内的对象。

### 2.1 `Catalog` 的字段契约（第三方连接器最容易漏的地方）

`Catalog` 返回 `source.Catalog{Tables: []source.Table}`，每张表至少要把这几项填对——
写路径的幂等与预检都靠它们，缺一个就会在别人机器上变成静默错误：

| 字段 | 含义 | 漏填的后果 |
|---|---|---|
| `TableRef{Schema,Table}` | 目标端拼 SQL 用真名 | 表名大小写/库名不对 → 写到错地方或写不进去 |
| `Columns`（顺序） | 行值的**唯一**权威顺序（`Row.Values` 按它排） | 列错位 → 静默写错列 |
| `PrimaryKey` | 源端主键列名（顺序有意义） | upsert/delete 定位不到行 |
| `HasStablePK` | 源端有没有**稳定**主键（视图/无主键表为 false） | 被误当 upsert 表 → 重放插重复行 |
| `UniqueKeys` | 目标端侧要核对的唯一索引列组（`[][]string`，每项一组列；**不含主键**——主键由 `PrimaryKey` 单独给，预检会把两者都当候选） | 声明 `unique_key` 的表核对不到索引 → 预检 `TARGET-02` 永远 blocked |

`UniqueKeys` **只用于核对用户声明**（`content.tables.<表>.unique_key`），**不用来发现行身份**：
身份只认源端主键或用户声明（R1）。收进来的索引必须已经过滤掉部分索引、表达式索引与前缀索引，
可空列可以照收（可空性由预检判，含 NULL 的组合不构成唯一性）。口径见
`DESIGN-NO-PRIMARY-KEY.md §3.4/§4.3`。

## 3. 写侧要实现的 5 个方法

`Kind / EnsureSchema / Apply / Verify / Cleanup`，其中 **`Apply(ctx, events, position)` 必须把位点和数据放在同一个事务里提交** —— 这是幂等的根基，接口上用参数强制，不许省。

## 4. 建表规则（`EnsureSchema`）

| 项 | 规则 |
|---|---|
| 表名/列名 | 与源端同名；目标库大小写规则不同则按目标端规范化并在 `plan` 里列出 |
| 主键 | 复制源端主键；无主键表建表但不建 PK，并提示 insert-only |
| 索引 | 默认 `create_indexes: defer`：先同步数据，结束时再建二级索引（快很多） |
| 字符集 | MySQL 目标用 `utf8mb4` / `utf8mb4_0900_ai_ci`（8.0+）或 `utf8mb4_general_ci`；PG 目标用源库同名 collation，取不到就用默认 |
| 默认值 / NOT NULL | 忠实复制；无法表达时降级为可空并在提示里说明 |
| 注释 | 复制表与列注释（便于人读） |
| 分区 | 一期不复制分区结构，数据落在单表；在 `plan` 里提示 |
| 自增 | 源端自增/序列 → 目标端自增；但同步期间由我们显式写入值，同步结束后按最大值重置序列 |

## 5. 类型字典扩展

在 `catalog.Map` 的规则里加条目，或提供 `RegisterMapping(from, to, base, Mapping)`。
每条必须标 `Lossless / Lossy / NeedsConfirm / Unsupported`（Unsupported 现在会自动按 text 兜底，不再拒绝）。

## 6. 测试矩阵

```yaml
name: <db>
image: <docker image>
versions: ["..."]
tier: A            # A 公开镜像；B 需授权/自托管；C 人工
startup: { args: [...], env: {...} }
capabilities: [snapshot, cdc, ...]
scenarios: [snapshot, incremental_crud, large_transaction, ddl_whitelist, crash_recovery, orphan_cleanup, type_boundary]
```

契约测试用 `internal/testkit`：拉起容器 → 建 schema（fuzz）→ 跑 8 个场景 → 与影子模型对拍。
**验收清单（五条全绿才算完成）**：
1. `Snapshot` 能被打断后按块续跑；
2. `Stream` 断线重连后不重不丢（对拍证明）；
3. `Probe` 在最低支持版本上给出正确的补偿；
4. `Cleanup` 不会删到别的实例的对象（用两个 instance 跑一遍）；
5. `testmatrix.yaml` 的每个版本都在 nightly 里跑过一遍。

## 7. 提 PR 前自检

```
gofmt -l .           # 空
go vet ./...         # 干净
go test ./...        # 绿
python -m jsonschema -i examples/job.yaml schema/job.schema.json   # 通过
```
