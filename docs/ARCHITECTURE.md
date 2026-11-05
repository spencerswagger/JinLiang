# 架构

## 1. 形态

**单静态二进制，零外部依赖，无 Kafka。** 一个进程既能做一次性迁移，也能长期跑增量。
`CGO_ENABLED=0`（驱动全用纯 Go：pgx / go-mysql），交叉编译到 linux(amd64/arm64) 与 darwin。

## 2. 分层与依赖方向

```
cmd/jinliang            CLI 解析、退出码、人类/JSON 输出
   │
internal/core           任务编排：生命周期、背压、批处理、错误分类
   ├── internal/preflight   探测与预检（三级报告 + 代做动作）
   ├── internal/catalog     库表枚举、类型映射、目标端建表
   ├── internal/transform   声明式转换（映射/过滤/脱敏/计算列）
   ├── internal/state       租约、位点、阶段、冲突、死信
   ├── internal/gc          孤儿对象清理
   └── internal/testkit     连接器契约测试（对外可用）
            ▲                    ▲
            │ 实现接口            │ 实现接口
     connectors/postgres      connectors/mysql
```

**硬规则**：连接器之间不许互相 import；`core` 只依赖接口，不依赖具体连接器；
连接器也不许直接写状态表（必须经 `state`），否则幂等破功。

## 3. 运行时数据流

```
[源] 读取/解码 ──▶ 事务组装 ──▶ 转换(可并行) ──▶ 有界队列 ──▶ 批写入(单线程) ──▶ 事务提交(数据+位点)
        │                │                                        │
        │                └─ 大事务 spill 到磁盘                    └─ 提交成功才 ack 源端
        └─ 心跳 / DDL / TRUNCATE 事件 ──▶ 控制面
```

- **读取是单流**（一个复制流一个 goroutine），解码后分发给转换 worker。
- **写入是单线程且有序**：跨事务必须按源端提交顺序落地，否则会写出因果错乱。
  要并行就往源端拆（按表拆多个流/多个 job），不是在应用侧乱序。
- **ack 时机**：只有目标库事务提交成功才推进源端位点；否则 WAL 增长但不丢数据。

## 4. 背压

| 水位 | 行为 |
|---|---|
| 队列 < 70% | 正常 |
| 70–90% | 暂停从源端读取（不是暂停 ack），触发 `jinliang_queue_depth` 告警 |
| > 90% | 强制 flush 当前批次；同时 `guard` 检查书签落后，超阈值按配置 pause |

**关键取舍**：暂停读会让 WAL 堆积（源端磁盘压力），暂停 ack 会丢数据。我们永远选前者，
并用 `guard.slot_warn_lag` 把 WAL 堆积变成显式告警，而不是静默撑爆磁盘。

## 5. 内存预算

`conns × chunk_rows × 行宽`（快照）+ `queue_capacity × 行宽`（增量）+ 大事务 spill 缓冲。
默认：队列 8192 行、单事务内存上限 128 MB、超限 spill 到 `--spill-dir`。

## 6. 扩展点

| 扩展点 | 一期做法 |
|---|---|
| 新数据库 | 实现 `source.Source` / `sink.Sink` + 交 `testmatrix.yaml`，用 `testkit` 跑契约测试 |
| 转换 | 只做声明式（yaml），**不做脚本沙箱**；复杂逻辑请用视图当源 |
| 输出端（MQ/数仓） | 二期。`sink.Sink` 接口已留位（`Apply` 带位点参数，MQ 端会退化为至少一次） |
| embedding / 二次开发 | 连接器与 core 均可作为库引用 |

## 7. 错误传播

`context` 取消贯穿所有阻塞点；错误按四分类（user_action / retryable / fatal / bug）冒泡到
`core`，由它决定「重试、暂停、停止」。**任何 goroutine 不许 panic 穿透**，统一 recover 后
按 `bug` 类上报并带上日志路径。
