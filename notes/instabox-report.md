# 津梁 InstaBox 真机测试报告

- 环境：InstaBox 沙箱 4 vCPU / 8 GiB / 20 GiB，Linux 6.18.60-instabox，docker 29.8.2
- 被测：`origin/main`（开工 `ce3d210`，收工 `7931997`——后者只改构建参数，引擎能力没变）
- 编译：`CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath ./cmd/jinliang`（debug）
- 数据：`pgsrc`(PG 16.15, wal_level=logical) → `pgreport`(PG 16.15)；`mysink`(MySQL 8.0.46, ROW/FULL) 只用于验证"不支持"
- 驱动方式：`jinliang serve` + 纯 curl REST/Bearer（无浏览器）；脚本 `notes/instabox-harness.py`

## 1. 场景表

| # | 场景 | 结果 | 证据 |
|---|---|---|---|
| 1 | 全量 20 万行（PG→PG） | **PASS** | 220,046 行 **3.14 s**（≈70k rows/s）；3 轮复跑 320k 行 3.57–4.81 s |
| 2 | 增量 20 insert / 10 update / 5 delete | **PASS** | 目标端 md5 与源端相等 **184 ms**；无重复 |
| 3a | 端到端延迟（插入→目标可见，n=30） | **PASS** | **p50 48 ms / p90 55 / p99 69 / max 69**（含 `docker exec psql` 每轮 ~40 ms 探测开销，真值更低） |
| 3b | 中途 `kill -9` + 重启续跑（数据正确性） | **PASS** | 杀在 rows_done=5,000（目标已提交 11,000 行）→ 重启标 `interrupted` → 重跑 **5.9 s**；最终 orders 200,046/200,046、customers 20,000/20,000，`count(*)=count(distinct id)`（**无重复**）、md5 一致 |
| 3c | 同上：重搬量 | **PASS**（代价高） | `pg_stat_user_tables`：customers `n_tup_ins=20,000`、orders `=200,046`、`n_tup_upd=0` → **整表 TRUNCATE 后全量重搬**，不是从 8 个已完成块续搬（块书签不用于续搬） |
| 3d | 同上：不重启 serve 能否重跑 | **FAIL** | `POST run` → **409**「控制库里还有一条上次没结束的运行…等一会儿再点」，实测等 10 分钟仍 409；只有再重启一次 serve（启动时回收）才能跑 |
| 4a | DDL 加列 `orders.vip`（白名单①） | **PASS / 偶发 FAIL** | 干净时机：目标加列 + 数据落地（`vip=true`，rows_done 继续推进）；同一操作在另一任务上整任务 `failed`：「orders 的行有 7 列，表定义是 8 列（源端结构变了？）」「源表 public.orders 多了列 vip：表结构在同步中途变了（白名单只允许加列，需重新预检）」 |
| 4b | DDL 改列宽 `note text → varchar(500)`（白名单②） | **PASS** | 目标列 `character varying:500`，300 字符数据写入成功 |
| 4c | DDL 建索引（白名单③） | **不支持（静默）** | 目标 `pg_indexes` 查不到该索引，任务不报错、继续同步；PG 逻辑复制不传 DDL，白名单第三类实际做不到 |
| 4d | DDL 改类型 `bigint → numeric(12,2)`（非白名单） | **FAIL** | 目标列仍是 `bigint`；写 12.34 报 `SQLSTATE 22P02` → **整任务 failed**（不是只停这张表）；错误前缀「未识别的数据库错误…发给维护者」，**无 E 码** |
| 4e | DDL 改列名 `city → town`（非白名单） | **PASS** | 目标列跟随成 `id,name,town,updated_at`，20,003 行 `town` 全有值（旧数据没丢） |
| 5a | 大事务：单事务 5 万行 | **PASS** | 250,049/250,049，追平 **2.12 s**，phase=streaming |
| 5b | 超限事务（把 `tx_oversize.bytes` 压到 1 MiB） | **PASS（诚实）** | **[E1011]** 四段式齐全；目标行数前后都是 250,049 → **没有半截**；实测触发点「已缓冲约 2,389 行 / 1.0 MB」 |
| 6a | PG → MySQL 点 run | **不支持** | HTTP **422**，`code: null`；「引擎暂时跑不了这种组合 / 引擎暂时只支持 PostgreSQL → PostgreSQL（当前 postgres → mysql）：MySQL 目标端在 B2」 |
| 6b | MySQL → PG 点 run | **不支持** | 422，同上；**文案说反了**（源是 MySQL，却说「MySQL 目标端在 B2」） |
| 6c | MySQL → MySQL 点 run | **不支持** | 422，同上 |
| 7a | `verify` 三级（干净） | **PASS** | structure OK / data OK / consistency OK，gap 0 字节 |
| 7b | `verify`（目标端**改 1 行**） | **FAIL（漏报）** | 仍报「三级校验通过」（抽样 53 行没抽到那 1 行）——**这是最危险的一条：校验给了错误结论** |
| 7c | `verify`（目标端**删 1 行**） | **PASS** | `row_count_mismatch orders：源端 200045，目标端 200044（差 -1）`，结论「有级别未通过」 |
| 8 | `doctor` | **PASS（有误报）** | 控制库 `integrity_check=ok`、schema 4、口令已设；但空闲 >90 s 的健康 streaming 任务被报成「发现幽灵运行（心跳已过期）」 |
| 9a | `gc --dry-run` + 孤儿 slot | **PASS** | 手建 `jl_orphan_1` → 列为 `unknown`「没有 _jl_object 登记…无法判定归属」，**可清理 0 个**（不动未知对象） |
| 9b | `gc --apply` | **PASS** | 删掉本实例空闲的 2 个对象（slot + publication），源端 slot 10 → 9 |
| 10 | 冲突 / 死信 | **不支持** | 目标库只有 `_jl_chunk/_jl_instance/_jl_lease/_jl_meta/_jl_object`，**没有 `_jl_conflict`/`_jl_dead_letter`**；CLI 报「目标库没有 _jl_conflict（任务还没跑过，或初始化没建表）」。`internal/runtime/tables.go` 定义了读写结构，引擎里没有写入方 |
| 11a | 纯 curl 建连接→建任务→preflight→run→SSE 到 done | **PASS** | once 模式 **2.2 s** 收尾，SSE 末条 `done/completed`，rows_done 220,046，`exit_code=0` |
| 11b | 重启 serve 后状态还在 | **PASS** | uptime 归零、pid 变（10721→10825），5 个任务 / 11 个连接 / 历史 run 全在，两端 count 一致 |
| 12 | 连续 3 轮全量+增量（泄漏） | **PASS（有增长）** | 3 轮全 streaming、count/md5 均一致；**RSS 430→435 MB（峰值 495 MB）**、threads 恒定 10、**fds 31→34（每轮 +1）**；goroutine 数拿不到（无 pprof 端点） |
| 13 | 客户端二进制体积 | **PASS** | debug 构建 **29,393,607 B**；发布参数 `-ldflags "-s -w"` → **20,144,288 B**（与 #43 口径一致，<25 MB 预算） |

## 2. 已知缺陷

### P0

**P0-1 `verify` 抽样漏报，给出「三级校验通过」的错误结论**
- 复现：目标端 `update orders set amount_cents = amount_cents + 1 where id = 12345` → `jinliang verify --tables orders --job-key …` 仍然「三级校验通过」。
- 原因：data 级只比对 `SampleRows=100`（本次实际抽样 53 个主键）内的行，聚合 checksum 变了也不参与判定；行数没变就查不出来。
- 影响：这是最后一道数据正确性防线，漏报等于告诉用户「没问题」。建议：行数一致时也校验全表 checksum/分块 checksum，或把抽样比例与 checksum 做成两级结论（至少输出「抽样通过 / 全量未校验」）。

### P1

**P1-1 心跳永不刷新 → 空闲任务被误判死亡，重启把健康任务标 interrupted**
- `control.Store.TouchRun()` 存在但**没有任何调用方**；`runs.heartbeat_at` 从 run 开始到结束一直是启动那一刻（实测 07:46:45 起 10 分钟不变，期间数据一直在同步）。
- 后果 1：`doctor` 把空闲 >90 s 的健康任务报「幽灵运行（心跳已过期）」。
- 后果 2：重启 serve 时 `RecoverStaleRuns` 把正在跑的任务标成 `failed/interrupted`（「进程在运行中被终止，心跳已过期」——进程其实是被正常重启的）。
- 建议：引擎在 `SetRunProgress`/心跳 tick 里调 `TouchRun`；空闲也要续心跳。

**P1-2 强杀后不重启 serve，任务永远起不来（且文案说「等一会儿再点」）**
- 复现：`kill -9` → 立刻重启 serve（此时心跳还新鲜，启动回收不触发）→ 之后每 90 s 都试一次 `run`，10 分钟仍 **409**「等一会儿再点『开始』（超过心跳阈值后 serve 会收尾）」。
- 实际：只有**再重启一次 serve** 才回收。文案与行为不一致。
- 建议：启动时按 `owner_instance == 本实例` + 进程 pid 存活判定立即回收；或请求时若判定 stale 就地回收（而不是只回一句话）。

**P1-3 删除「正在跑」的任务不停引擎 goroutine，目标端租约残留锁死后续任务**
- 复现：streaming 中 `DELETE /api/jobs/{id}` → 引擎继续写、`_jl_lease` 每 30 s 续期；在同目标库新建任务 run → **E1005**「另一个实例正在跑这个任务：同一台机器上还有一个进程在跑这个任务（pid=自己）」，给的下一步（「去那台机器上 stop」）无从执行。
- 建议：删除任务先 stop 其活跃 run 并释放租约；四段式对「本进程自己」单独给话术。

**P1-4 DDL 加列存在竞态：旧行 + 新 Relation 混批 → 整任务 failed**
- 干净时机正常跟随（4a）；同一操作在另一任务上整任务 `failed`：「orders 的行有 7 列，表定义是 8 列」，错误无 E 码、前缀「未识别的数据库错误」。
- 建议：DDL 到达时先把在途批次按旧 Relation 落库，再切新 schema；或对齐到事务边界。

**P1-5 非白名单改类型：目标不跟随 + 整任务 failed + 错误无码**
- `bigint → numeric(12,2)`，目标列仍 `bigint`，写 12.34 报 `22P02`，整任务停（覆盖 orders+customers 整任务，不是只停 orders）。
- 建议：Relation 里类型 OID 变了就应当按「白名单外」明确拒绝并给出 E 码（现有 E1012/E1013 可挂），而不是落到 22P02 的「未识别」。

### P2

**P2-1 能力边界错误没有错误码**：三组合拒绝都是 `code: null`，客户端无法分支；且 `mysql → pg` 的 `why` 说成「MySQL 目标端在 B2」，方向说反。

**P2-2 `gc` 只有单目标库视角，slot 会被历史任务吃光**：每个任务建一个 slot（`jl_<job_key>_1`），其他目标库创建的对象在 `gc --target <本库>` 里一律 `unknown` → 永不清理；`max_replication_slots=10` 很快耗尽，新任务报的是**未分类**错误「建同步书签失败…all replication slots are in use (SQLSTATE 53400)」。建议给 E 码 + 指引 `pick a bigger max_replication_slots` / `jinliang gc --all`。

**P2-3 白名单「加索引」做不到**：PG 逻辑复制不传 DDL，建索引在目标端完全不跟随也不报错，与 SPEC 的白名单口径不符。建议文档降级口径，或加 catalog 差异巡检。

**P2-4 冲突与死信只有读侧**：`_jl_conflict`/`_jl_dead_letter` 没有被创建、没有写入方，`conflicts list`/`dead-letter list` 永远报「目标库没有…」。要么实现写入，要么把这几个命令标成未实现（别让人以为能用）。

**P2-5 文件描述符每轮 +1**：3 轮全量各 +1 fd（31→34），run 结束后连接/游标疑似没关；goroutine 数无法观测（serve 没开 pprof 端点），建议加 `/debug/pprof` 或 `/api/meta` 里暴露 goroutine 数。

## 3. 结论

- **PG→PG 主链路是可信的**：20 万行 3.1 s、增量 184 ms、kill -9 后数据零丢失零重复、大事务超限有 E1011 且不落半截、重启后状态完整。
- **不可信的是「结论类」能力**：`verify` 会漏报（P0），心跳/幽灵运行会误报，DDL 白名单里的加列会偶发把整任务打死。
- **MySQL 目标端（含 MySQL 源）当前完全不可用**：`internal/mysqlsink` 有包无接线，`engine.go:114` 与 `runctl.go:167` 双重拦截，报错无码。

## 4. 复现

```sh
# 沙箱内
ssh -p 2222 <user>@ssh.instabox.sh
cd ~ && python3 harness.py s5|ops|ui|s2|s3|s3b|s4|stab|err
```
harness 源码 `notes/instabox-harness.py`，各场景的前置容器命令见 `notes/instabox-demo.md` §2。
