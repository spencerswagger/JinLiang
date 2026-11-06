# 安全

## 0. 控制面凭证（2026-11-05 新增）

- 连接的密码存控制库 `connections.secret_enc`：**AES-256-GCM**，每条独立 nonce。
- 密钥：`--key-file`（强制 0600）→ `JINLIANG_KEY` → 首次启动生成 `data-dir/key`（0600）。
- UI 与 API **永不回传明文**（只回 `has_secret: true`）；导出 yaml 时省略密码或写 `env:` 占位。
- 控制库本身 0600；`--data-dir` 不允许落在世界可读目录。

## 1. 凭证

- 只接受 `env:NAME` 与 `file:/path`；**禁止**在 yaml 或命令行里写明文 DSN。
- `file:` 读取时检查权限：不是 0600 就**不读这个文件**并提示 `chmod`。这是全产品唯一的硬停：安全底线。
- 内存中只保留必要字符串；日志与 `plan` 输出统一脱敏（密码、token、连接串）。
- 可选系统 keyring（linux secret-service / macOS keychain）二期做；一期用文件 + 环境变量。

## 2. 最小权限账号

**PostgreSQL**（我们代建时用这套）：
```sql
CREATE ROLE jinliang_ro LOGIN PASSWORD '...' REPLICATION;
GRANT SELECT ON <选中表> TO jinliang_ro;
-- 需要读全库时：GRANT pg_read_all_data（14+）或按 schema 授权
```
**MySQL**：
```sql
CREATE USER 'jinliang_ro'@'%' IDENTIFIED BY '...';   -- 8.4/9.x 用 caching_sha2_password
GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON <db>.* TO 'jinliang_ro'@'%';
```
需要目标端建表时再加 `CREATE`（可只给特定 schema），同步完建议收回。

## 3. 传输

- 默认 `verify-full`（PG `sslmode=verify-full`，MySQL `tls=true` + 校验 CA）。
- 允许降级到 `require`/`prefer`，但必须在 `plan` 里标红并让用户确认。
- 支持自定义 CA 路径与客户端证书。

## 4. 日志与数据

- 默认**不打行数据**；`--debug-rows` 显式开启并打印警告（含字段名与截断值）。
- 日志里的 DSN 一律打码成 `postgres://user:***@host/db`。
- 死信里的 `raw_row` 是真实数据：`job.yaml` 可配 `dead_letter.store_row: none` 只存哈希。

## 5. 审计

每次代做动作（建账号、建表、建书签、清理）都记一条 action：谁（工具/你/DBA）、何时、SQL、undo。
落地：本地 `audit/*.jsonl`（默认）+ 可选 `_jl_action` 表（目标库）。
`jinliang undo <action-id>` 可撤销，撤销本身也记一条。

## 6. 供应链

- 依赖极少（pgx、go-mysql、yaml 解析），`go mod verify` 进 CI。
- Release 产 tgz + sha256；可用 `go version -m jinliang` 查构建信息（SBOM 的轻量替代）。
- 漏洞报告走 GitHub Security Advisory（仓库开启 private reporting）。

## 7. 部署与多副本（会互相踢的地方）

> **同一 `job_key` 的两个实例会互相踢**：目标端租约按 `job_key` 唯一，删除任务会删掉租约行；另一个用同 `job_key` 在跑的实例（例如拷了 data-dir 起第二副本）会在约 10 秒内被判「租约丢了」并自行停止。**`instance_id` 必须按实例唯一，不要拷 data-dir 起第二个实例。**

为什么会这样（不是缺陷，是租约的代价）：

- 目标端 `_jl_lease` 按 `job_key` 唯一——「同一份同步只能有一个写入者」就靠它。租约 TTL 默认 **30s**，续租间隔是 **`leaseTTL/3`（默认 10s）**。
- **删除任务时会删掉这行租约**，好让目标端那个还在跑的进程自己停下（跨进程 `cancel` 不适用）。它下一次续租发现「行没了」会**立刻** `loseLease` 并按 `interrupted`/失败收尾，所以最坏约 **10 秒**（不是等 TTL 到期）。
- **这 10 秒里两个写入者会同时写**：同一 job 同主键的 upsert 多数幂等，但**「删了重建时改了 `content.tables` / `on_exists`」不幂等**——那是两份不同的意图在抢同一张表。
- 目前只有 **PostgreSQL 目标端**会在删除任务时清租约行；MySQL 目标端的状态表带租约，但删除路径还没接，CLI 会明说「该目标端没有租约（不支持租约能力），跳过清租约这一步」，**不报假成功**。

部署上的三条硬规矩：

1. **`instance_id` 按实例唯一**——它由 `--instance` / `data-dir/instance` 决定，**拷 data-dir 起第二副本 = 两个实例同 id**，租约会走进「同机」分支而 pid 跨容器不可比，出现双写窗口。
2. **多副本共享同一个目标库时，用不同的 data-dir + 显式不同的 `--instance`**，并确保它们跑的是不同 `job_key`（同一 `job_key` 本来就应该只有一个写入者）。
3. 想确认「现在谁持有租约」，看 `_jl_lease.holder` 与 `expires_at`；`doctor` 会给活跃运行摘要，历史页给 `finish_reason`。
