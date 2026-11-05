# 身份与命名：多实例连同一个库怎么办

> 场景：同一个源库被两个津梁部署连着；同一个目标库上有多个任务；容器重启后本地状态丢了；
> 两个人在两台机器上用了同一个 `job:` 名字。以下每一条都必须不冲突、不误删、不双写。

## 1. 三层身份，各管一件事

| 层 | 由什么决定 | 用途 | 不用于 |
|---|---|---|---|
| **job_key** | 「源 + 目标 + 范围」的 sha256 前 10 位 base32，**存在控制库 `jobs.job_key`** | 任务身份、状态表主键、对象命名、租约排他 | 展示 |
| **instance_id** | 首次启动随机生成（16 字节 → 26 位 base32），登记在目标库 `_jl_instance` | 审计、租约持有者、对象所有权、MySQL `server_id` 派生 | 判断是否同一任务 |
| **object name** | `jl_<job_key>` / `jl_<job_key>_<n>` | 源端 slot / publication / 临时快照 | 展示 |

**关键结论：用户起的 `job:` 名字只做展示，允许重复。** 判断"是不是同一个任务"永远看 `job_key`。

## 2. job_key 怎么算

```
job_key = base32( sha256( normalize(source_dsn) | normalize(target_dsn) | normalize(scope) ) )[:10]
```

- `normalize` 去掉密码（改密码不换身份）、统一大小写与空白。
- `scope` 是表清单/过滤条件的规范化串：**选了不同的表就是不同的任务**。
- 10 位 base32 = 50 bit，同库里的任务数远达不到碰撞量级；真撞了还有 `_jl_meta.name` 与审计可查。

推论（正是你要的效果）：

| 场景 | 结果 |
|---|---|
| 两台机器同步**同一个源到不同目标** | 两个 job_key → 两个 slot（`jl_aaa...` / `jl_bbb...`），互不干扰 |
| 两台机器同步**同一源到同一目标** | 同一个 job_key → 租约排他，**第二个实例被拒（E1005）而不是双写** |
| 两个任务在同一源上选了**不同的表** | 两个 job_key → 两个 publication，互不覆盖 |
| 两个任务名字都叫 `orders` 但源不同 | 两个 job_key，名字重复无所谓 |

## 3. 对象命名规则

| 对象 | 命名 | 长度 |
|---|---|---|
| PG 同步书签 | `jl_<job_key>_<n>` | 16–17 字节（PG 上限 63） |
| PG 发布清单 | `jl_<job_key>` | 13 |
| 临时导出快照 | 由 PG 生成，登记进 `_jl_object` 便于回收 | — |
| MySQL `server_id` | `hash(instance_id | job_key) mod span → [2^20, 2^31)` | 数值 |

`ObjectName()` 会校验 63 字节上限；超了直接报错（这是编程错误，不是用户配置问题）。

## 4. 实例 ID 从哪来

优先级：
1. `--instance <id>` 或 `JINLIANG_INSTANCE`（显式，适合容器集群）；
2. 本地状态目录 `instance`（默认 `~/.config/jinliang/instance`，0600）；
3. 都没有 → 生成 26 位随机 ID 并登记 `_jl_instance`。

**容器重启导致 ID 变更是允许的**：旧租约按 TTL 过期，旧对象仍可用 `job_key` 找到；
`gc` 通过 `_jl_object.instance_id` 判断"是不是本实例建的"。

## 5. 孤儿清理的所有权（防止互相删）

`gc` 只处理同时满足以下条件的对象：
1. 名字符合 `jl_<job_key>...` 且能在 `_jl_object` 找到登记；
2. `_jl_lease` 里没有该 `job_key` 的有效租约；
3. 对象的 `instance_id` **等于当前实例**（默认），或用户显式 `--force` 并确认打印出来的对方实例信息；
4. 源端对象确实不在使用（PG `active=false`）。

第 3 条是这次新增的：**A 实例默认不会删 B 实例的书签**。

## 6. 并发迁移状态表

两个实例可能同时启动并都想迁移 `schema_version`：

```
BEGIN;
  PG: SELECT pg_advisory_lock(hashtext('jinliang:migrate'));
  MY: SELECT GET_LOCK('jinliang:migrate', 30);
  重读 _jl_version
  若已是最新 → 释放锁，继续
  否则按顺序执行迁移脚本（只加列/加表），更新 _jl_version
  释放锁
COMMIT;
```

拿不到锁 → 等待（最多 30s）→ 重读；仍拿不到就提示"另一个实例正在迁移"，**不阻断启动的其余部分**（读操作不受影响）。

## 7. 对用户可见的提示（四段式）

```
[提示] 这两个任务看起来是同一个同步：源 shop@db1、目标 report@db2、表 orders,customers
       为什么：它们算出了同一个任务指纹 aaaabbbbcc。
       能做什么：(a) 若确实同一个任务，请在另一个实例上停掉它（租约会保护你）；
                 (b) 若想并行跑两份，请改 job.yaml 的 job 名字 **并** 缩小范围，让指纹不同。
       风险：不改会撞租约，后启动的那个直接退出（E1005），不会双写。
```

## 8. 测试

`internal/state/identity_test.go` 覆盖：密码轮换不改指纹、范围变化改指纹、
对象名长度上限、`server_id` 落在区间且确定性 —— 这些是"多实例不冲突"的最小证据。