# InstaBox 真机演示记录（2026-11-06）

> 目的：在 InstaBox 沙箱里跑一次真实同步，给 spencer 看真数字。
> **结论先说：origin/main 跑不了 PG → MySQL。** 引擎在开跑前就把这条组合挡掉
> （`internal/engine/engine.go:114` + `internal/httpapi/runctl.go:167`），
> `internal/mysqlsink` 整个包在 main 上没有任何调用方（只在自身测试里被引用）。
> 所以本次演示按 **PG → PG** 跑同规模数据，指标口径不变；MySQL 目标端等 B2。

## 0. 机器

| 项 | 值 |
|---|---|
| 入口 | `ssh -p 2222 ibssh_3gvrzfblcbfkqqcsoio3ewuagu@ssh.instabox.sh` |
| 规格 | 4 vCPU / 8 GiB / 20 GiB，Linux 6.18.60-instabox |
| 工具 | docker 29.8.2、compose v5.6.0、go 1.27.1、jq、python3（**无 psql/mysql 客户端、无 bc**） |
| 镜像 | `postgres:16-alpine`、`mysql:8.0`（已在机内） |
| 沙箱现状 | 未销毁，容器仍在跑（pgsrc / pgreport / mysink + serve 进程） |

## 1. 编译与上传（本机 = 我的代码树）

```sh
cd /worktrees/JL-QA && git checkout --detach origin/main   # ce3d210
export PATH=/.toolchain/go/bin:$PATH GOMODCACHE=/.cache/go-mod GOCACHE=/.cache/go-build
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o dist/jinliang ./cmd/jinliang
scp -P 2222 dist/jinliang ibssh_...@ssh.instabox.sh:~/jinliang
```

- 产物：**29,393,607 字节（28.0 MiB）**，`sha256 714621bf288d200f592f1a18b1655cb1ddc21204a999cad1ec91a8222c4ea9a9`
- `jinliang version` → `jinliang dev`（version 未注入，`-ldflags -X main.version=` 没传）

## 2. 容器

```sh
# 源（PG）
docker run -d --name pgsrc -e POSTGRES_USER=jinliang -e POSTGRES_PASSWORD=*** \
  -e POSTGRES_DB=src -p 127.0.0.1:55432:5432 postgres:16-alpine \
  -c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10 -c listen_addresses="*"

# 目标（MySQL，按要求开了位点）——本次没派上用场（见 §0），容器留着
docker run -d --name mysink -e MYSQL_ROOT_PASSWORD=*** -e MYSQL_DATABASE=report \
  -p 127.0.0.1:53306:3306 mysql:8.0 \
  --server-id=1 --log-bin=mysql-bin --binlog-format=ROW --binlog-row-image=FULL

# 目标（PG，实际用于演示）
docker run -d --name pgreport -e POSTGRES_USER=jinliang -e POSTGRES_PASSWORD=*** \
  -e POSTGRES_DB=report -p 127.0.0.1:55433:5432 postgres:16-alpine \
  -c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10
```

核对过：`wal_level=logical`、`max_replication_slots=10`、`max_wal_senders=10`；
MySQL `log_bin=1 / binlog_format=ROW / binlog_row_image=FULL / server_id=1`。

MySQL 账号（为 curl 链路准备，最终没跑到）：
`CREATE USER 'jinliang'@'%' IDENTIFIED WITH mysql_native_password BY '***';`

## 3. 数据（源端）

```sql
CREATE TABLE customers (id bigint PRIMARY KEY, name text NOT NULL, city text,
                        updated_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE orders (id bigint PRIMARY KEY, customer_id bigint NOT NULL,
                     amount_cents bigint NOT NULL, status smallint NOT NULL,
                     region text NOT NULL, note text,
                     updated_at timestamptz NOT NULL DEFAULT now());
-- orders 200,000 行 / customers 20,000 行（=220,000 行，窄行，src 库 28 MB）
```

DSN（口令掩码；真实口令 `jinliang`）：

- 源 `postgres://jinliang:***@127.0.0.1:55432/src?sslmode=disable`
- 目标 `postgres://jinliang:***@127.0.0.1:55433/report?sslmode=disable`
- MySQL（未用）`mysql://jinliang:***@127.0.0.1:53306/report`

## 4. 无浏览器链路（curl，全部走 REST + Bearer）

```sh
./jinliang serve --listen 127.0.0.1:8080 --data-dir ~/data --api-token demo &
B=http://127.0.0.1:8080; AUTH="Authorization: Bearer demo"

curl -sS -c /tmp/cj $B/api/session                       # 种 jl_csrf
CSRF=$(grep -w jl_csrf /tmp/cj | cut -f7)
curl -sS -b /tmp/cj -X POST $B/api/setup -H 'Content-Type: application/json' \
     -H "X-CSRF-Token: $CSRF" -d '{"password":"***"}'     # 201 + token

curl -sS -X POST $B/api/connections -H "$AUTH" -d '{"name":"pg-src","dsn":"…55432…"}'
curl -sS -X POST $B/api/connections -H "$AUTH" -d '{"name":"pg-report","dsn":"…55433…"}'
curl -sS -X POST $B/api/connections/$ID/test -H "$AUTH"
curl -sS "$B/api/catalog?connection_id=$SRC" -H "$AUTH"
curl -sS -X POST $B/api/jobs -H "$AUTH" -d @spec.json     # contacts: orders+customers
curl -sS -X POST $B/api/jobs/$JOB/preflight -H "$AUTH"
curl -sS -X POST $B/api/jobs/$JOB/run -H "$AUTH" -d '{}'
curl -sS -N  "$B/api/jobs/$JOB/events" -H "$AUTH"         # SSE 抓阶段
```

脚本落盘（沙箱内同名）：`~/demo.sh`（setup/job/run）、`~/demo3.sh`（clean/run/incr/counts）、`~/lat.sh`。
本机副本：`notes/instabox-demo.sh`、`notes/instabox-demo2.sh`、`notes/instabox-demo3.sh`、`notes/instabox-latency.sh`。

任务 spec（PG→PG 版；MySQL 版唯一差别是 `target.kind=mysql/schema=report`）：

```json
{"version":1,
 "source":{"kind":"postgres","scope":{"schema":"public","include":["orders","customers"]}},
 "target":{"kind":"postgres","schema":"public","on_exists":"append","create_indexes":"defer"},
 "mode":{"frequency":"continuous+initial"},
 "content":{"tables":{"orders":{},"customers":{}}}}
```

preflight：`can_start=true`，7 项检查全 ok，唯一 warn 是 `TLS-01`（sslmode=disable）。

## 5. 数字

| 指标 | 值 |
|---|---|
| 全量耗时（POST /run → phase=streaming） | **3,139 ms / 220,000 行 ≈ 70k rows/s** |
| 其中 prepare+catchup | 2,829 ms 时进 catchup，3,139 ms 转 streaming |
| 增量（20 insert + 10 update + 5 delete）→ 追平（目标端 md5 与源端一致） | **184 ms** |
| 端到端延迟（插入→目标可见，n=30） | **p50 48 ms / p90 55 ms / p99 69 ms / max 69 ms**（含 `docker exec psql` 每轮约 40 ms 探测开销，真值 ≤ 这些数） |
| 目标端行数 | orders **200,045** = 源；customers **20,000** = 源 |
| 抽样校验和（orders，全表 md5(string_agg)） | 源 `1d3cc7788be4911d89446b85d714497b` = 目标 |
| 抽样校验和（customers） | 源 `7997010697edaa609e98ce2ddafabcdb` = 目标 |
| `jinliang verify --job-key` | structure OK / data OK（100 抽样全中）/ consistency OK（gap 0 字节） |
| 客户端二进制 | 29,393,607 B（28.0 MiB） |
| 日志 WARN/ERROR | serve.log 共 19 行，**无 WARN/ERROR**（只有 TLS-01 出现在 preflight 里） |

`doctor` 摘要：实例 ID 正常 / 控制库 integrity_check=ok / schema 4 / 口令已设 /
1 条运行在跑（心跳 1m30s 内）/ 源端目标端检查跳过（没给 DSN）/ **结论：正常**。

`gc --dry-run`（需 `--instance`，否则报「不知道本实例 ID」）：

```
slot        jl_lqgimr4rdl_1   in_use_by_this_instance
publication jl_lqgimr4rdl     in_use_by_this_instance
可清理 0 个
```

## 6. 发现的坑（真机才暴露）

### A. PG → MySQL 在 main 上跑不了（阻塞性）

- `internal/engine/engine.go:114`：`src.Kind != postgres || tgt.Kind != postgres` 直接返回
  「B1 只做了 PostgreSQL → PostgreSQL」。
- `internal/httpapi/runctl.go:167 checkEngineSupported` 在 HTTP 入口再挡一次（返回 400 `user_action`）。
- `internal/mysqlsink/*` 在 main 上**没有任何调用方**（只有自身测试），即包写完了但没接线。
- 影响：Ops/演示脚本里「PG 源 + MySQL 目标」的组合目前只能到「建连接、测连成功、preflight 通过」，
  点 run 会被拒。要么等 B2，要么演示改用 PG 目标。

### B. 删除「正在跑」的任务会泄漏运行，并锁死目标端（P1）

复现：任务在 streaming 时 `DELETE /api/jobs/{id}` → 引擎 goroutine 不退出，继续持有目标端租约
（`_jl_lease` 里 holder=`<jobname>#<pid>#<nonce>`，30s 续期）。随后在同一目标库建新任务并 run：

```
E1005 另一个实例正在跑这个任务：同一台机器上还有一个进程在跑这个任务（pid 2584）
      下一步：确认另一台机器上的进程是否还在：jinliang status
```

（pid 2584 就是当时那个 serve 自己；`_jl_lease` 行在删除任务之后仍每 30s 续期，
证明引擎 goroutine 没退出。新任务的 run 立刻在 prepare 阶段 failed。）

但那个 pid 就是**本机的 serve 自己**，「去那台机器上停掉」无从下手。绕法：`kill` serve 重启，
或手工 `drop table _jl_lease/_jl_object/_jl_chunk`。建议：删任务时先 stop 它的活跃 run；
四段式文案把「同一个 pid」这种情况单独说（「本进程还有一条运行没停，先 stop 再删任务」）。

### C. `lag_ms` 有列有字段，但没人写

`control.Run.LagMs` 与 runs 表 `lag_ms` 列都在，`SetRunProgress` 也接受它，
但 engine/httpapi/jobrun 里没有任何地方传值 → 界面上永远是 null。
所以「端到端延迟 p50/p99」只能像 §5 那样从外部探测得到；建议引擎心跳时把 lag 写进去。

### D. 无 `psql` / `mysql` 客户端、无 `bc`

沙箱里只有 docker + jq + python3。脚本里的浮点计时别用 `bc`（改成 `date +%s%3N` 整数运算），
两端 SQL 一律 `docker exec -i <ctr> psql/mysql`。

## 7. 现场还在的东西（交给 Ops 收）

- 容器：`pgsrc`(55432) / `pgreport`(55433) / `mysink`(53306)，数据仍在
- 进程：`./jinliang serve --listen 127.0.0.1:8080 --data-dir ~/data --api-token demo`（pid 7133）
- 控制库：`~/data/jinliang.db`（1 任务 / 3 连接 / 25 审计）
- 文件：`~/jinliang`、`~/demo*.sh`、`~/lat.sh`、`~/events.log`、`~/serve.log`、`/tmp/jl_ids*.sh`
