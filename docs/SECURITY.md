# 安全

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
