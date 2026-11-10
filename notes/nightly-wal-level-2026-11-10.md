# nightly 长期红：定位与修法（2026-11-10）

PR #77 / 分支 `jinliang/nightly-wal-level` / 基线 `43554b1`。

## 一、现象

nightly 连续四次 `schedule` 全红：`42308052749`（11-06）、`42416727996`（11-07）、
`42527281442`（11-08）、`42735064126`（11-10）。每次约 1m40s~2m 就红。

## 二、根因（三件事，不止一件）

派工说「PG service 没开 wal_level=logical」，这条对，但**只占三分之一**。
按 job 逐个看日志（`gh api .../jobs/<id>/logs`）：

| job | 红的原因 |
|---|---|
| `integration` | `ERROR: logical decoding requires wal_level >= logical (SQLSTATE 55000)`；另有 `CAP-01 源库没开逻辑变更流（wal_level=replica）`。就是派工说的那条。 |
| `pg-matrix` ×9 | 全部红在 `JINLIANG_TEST_MYSQL_SRC_DSN 未设置：集成测试需要真实数据库；缺 DSN 必须失败，不允许跳过`——PG-only 的 job 里跑了 `verify`/`doctor`，它们有 MySQL 侧用例。 |
| `mysql-matrix` ×4 | 红在 `JINLIANG_TEST_PG_SRC_DSN 未设置`（`verify`/`doctor` 的 PG 侧用例）；`5.7` 另有 `log_bin=0`（5.7 官方镜像默认关闭 binlog）。 |
| `bench` | success（它只用 PG 的 `JINLIANG_TEST_PG_DSN`，不碰逻辑解码）。 |

三条同一族：**`services:` 不能给容器传启动参数**（只能 `env` / `ports` / `options`，
`options` 是 `docker create` 的选项，不是容器命令）。所以
- `image: postgres:*` 的 `wal_level` 只能是镜像默认 `replica`；
- `image: mysql:5.7` 的 `log_bin` 只能是 `OFF`。

`#75` 之前这些缺口靠 `t.Skip` 静默通过；`#75` 把「缺 DSN 就红」落到 verify/doctor 之后，
`pg-matrix` / `mysql-matrix` 就直接红了——这是**正确行为**，红的是配置，不是代码。

## 三、改法（只动 `.github/workflows/nightly.yml`）

1. PG 一律 `docker run` 起：`-c wal_level=logical -c max_replication_slots=40
   -c max_wal_senders=40`；版本与 `ci.yml` 的 `b1-engine` 一致（`postgres:16`），
   `pg-matrix` 保留 10–18 版本矩阵。轮询用真查询（`docker exec … select 1`），
   不用 `pg_isready`——initdb 期间临时实例会应答，随后重启，psql 会撞
   "the database system is shutting down"。
2. MySQL 源一律 `docker run` 起：`--server-id=1 --log-bin=mysql-bin
   --binlog-format=ROW --binlog-row-image=FULL`，与 `mysql-matrix.yml` 同一套参数
   （`binlog_row_metadata` 故意保持镜像默认 MINIMAL，测「binlog 里没有列名」那条最难的路）。
3. 四个 job 都补齐缺的那一侧真库：`pg-matrix` 加 `mysql:8.0` service，
   `mysql-matrix` 加 `postgres:16`（logical）。
4. **新增「断言数据库前提」步骤**（这就是「默认路径上不会漏」）：
   `wal_level=logical`、`max_replication_slots`/`max_wal_senders` ≥ 20、`log_bin=1`、
   `binlog_format=ROW`、`binlog_row_image=FULL`，任一条不满足**当场失败**并给出修法
   （比如「services 传不了启动参数，必须 docker run … -c wal_level=logical」）。
5. 测试步骤加 `-v`，跑完再 `grep '^[[:space:]]*--- SKIP'`：出现即失败。
   跳过＝没验（#75 的教训），这条把「静默通过」的路堵在 workflow 层。

没有改：测试断言、业务代码、DSN 变量名、矩阵版本列表、门禁阈值。

## 四、验证（Actions 实跑）

- nightly（workflow_dispatch，`jinliang/nightly-wal-level`）：
  https://github.com/spencerswagger/JinLiang/actions/runs/42744528527
  `conclusion=success`，02:41:56Z → 02:45:06Z ≈ **3m10s**。
  - `integration`：3m06s，PASS=326（含子用例）/ SKIP=0 / FAIL=0；
    断言输出 `PG: wal_level=logical max_replication_slots=40 max_wal_senders=40`、
    `MySQL: log_bin=1 binlog_format=ROW binlog_row_image=FULL`。
  - `pg-matrix (10..18)` 9/9 success，各 69~83s；`pg-matrix (10)` PASS=53 / SKIP=0 / FAIL=0。
  - `mysql-matrix (5.7/8.0/8.4/9.4)` 4/4 success，各 ~66s；
    `MySQL 5.7: log_bin=1 binlog_format=ROW binlog_row_image=FULL server_id=1`。
  - `bench` success（`scripts/bench.sh --enforce`）。
- PR #77 的 `ci`：build / race / testmatrix / schema / control-plane / b1-engine /
  b4-mysql-source 全 success。
- `b1-engine`：PASS=139 / SKIP=0 / FAIL=0（与 main 基线 139/0/0 一致）；
  本机同命令（带 MySQL 源 DSN、不 `-skip`）PASS=141，差的正好是
  `TestMySQLSource*` 两条（b1-engine 有意 `-skip`，它们在 b4-mysql-source 里跑），
  即**该配置下一条都没丢**。

## 五、同源隐患（只列，没改）

1. `ci.yml` / `b4-mysql-source`：`docker run … postgres:16` **没带** `-c wal_level=logical`
   （目标端目前不需要逻辑解码，但只要那条 job 里跑任何建书签的用例就红），
   也没给 `max_replication_slots`/`max_wal_senders`。
2. `ci.yml` / `control-plane`：PG 带了 `wal_level=logical`，但只**打印**不断言；
   也没有 `max_replication_slots`/`max_wal_senders`（默认 10）。
3. `ci.yml` / `b1-engine`：同上，只打印 `wal_level`，没有断言；`-skip 'TestMySQLSource'`
   是有意的（那条在 b4 跑），但它意味着「b1 绿 ≠ MySQL 源那条绿」，两处要一起看。
4. `release.yml` / `bench-regression`：`services: postgres:16`——同一个「services 起 PG」
   写法（默认 replica），只是当前用例不碰逻辑解码。
5. 默认路径上的 `t.Skip`（`go test ./...` 会跑到）：
   - `internal/connect/connect_test.go:37` / `:75`：缺 DSN 就 Skip，所以 PR 的 `build`
     job 里 `TestPostgresProbeAndCatalog` / MySQL 那条**从来没真跑过**；
   - `internal/httpapi/server_test.go:1867`：`当前矩阵不含 PG→MySQL`；
   - `internal/jobrun/directions_test.go:29`：E1014 未落地；
   - `internal/mysqlsource/stream_test.go:44`：本机不能监听（`mysql-matrix.yml` 跑完
     不检查 SKIP，这条 Skip 了 job 照样绿）；
   - `internal/mysqlsink/integration_test.go:805`：没有源端 DSN 时跳过跨端比对。
6. `mysql-matrix.yml`：参数是对的（`docker run` + `--log-bin` + ROW/FULL），但跑完
   既不断言配置、也不检查 SKIP——「跑在什么配置上」靠人读 yaml。

## 六、复现

```
gh workflow run nightly.yml --ref <branch>
gh run view <run-id>
# 单 job 的原始日志（能看见 SQLSTATE 55000）：
gh api repos/spencerswagger/JinLiang/actions/jobs/<job-id>/logs | grep -E "wal_level|FAIL:"
```
