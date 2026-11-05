# 津梁状态表与幂等

状态全部落在**目标库**，不依赖本地文件、不依赖外部协调器。

## 1. 表结构

### PostgreSQL
```sql
-- 身份：job_key 由「源 + 目标 + 范围」指纹算出，用户起的名字只做展示（见 IDENTITY.md）
CREATE TABLE IF NOT EXISTS public._jl_instance (
  instance_id  text        PRIMARY KEY,       -- 26 位随机 base32
  label        text,                          -- 人可读，如 "prod-a"
  host         text,
  first_seen   timestamptz NOT NULL DEFAULT now(),
  last_seen    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public._jl_version (
  schema_version int NOT NULL,                -- 状态表结构版本，只向前迁移
  migrated_at    timestamptz NOT NULL DEFAULT now(),
  migrated_by    text NOT NULL
);

CREATE TABLE IF NOT EXISTS public._jl_lease (
  job_key      text        PRIMARY KEY,       -- 同一个 job_key 只允许一个实例跑
  instance_id  text        NOT NULL,
  label        text,
  expires_at   timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS public._jl_meta (
  job_key       text        PRIMARY KEY,
  name          text        NOT NULL,          -- 用户起的名字，仅展示，可重复
  instance_id   text        NOT NULL,          -- 创建者（孤儿判定与审计用）
  schema_version int        NOT NULL DEFAULT 1,
  job_hash      text        NOT NULL,          -- job.yaml 语义字段的哈希
  schema_hash   text,                          -- 源端表结构指纹，漂移时报警
  phase         text        NOT NULL,
  position      text,                          -- 各连接器自序列化（PG LSN / MySQL file:pos:gtid）
  watermark     text,                          -- interval 模式水位（水位列 + 主键复合）
  snapshot_id   text,
  started_at    timestamptz NOT NULL DEFAULT now(),
  updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public._jl_object (   -- 我们建的源端/目标端对象登记表（孤儿判定依据）
  object_name  text        PRIMARY KEY,        -- jl_<job_key> / jl_<job_key>_<n>
  object_kind  text        NOT NULL,           -- slot / publication / temp_snapshot
  job_key      text        NOT NULL,
  instance_id  text        NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now(),
  dropped_at   timestamptz                      -- 软删，供审计
);

CREATE TABLE IF NOT EXISTS public._jl_chunk (
  job_key    text    NOT NULL,
  table_name text    NOT NULL,
  chunk_no   int     NOT NULL,
  range_from text,
  range_to   text,
  done       boolean NOT NULL DEFAULT false,
  rows       bigint  NOT NULL DEFAULT 0,
  PRIMARY KEY (job_key, table_name, chunk_no)
);

CREATE TABLE IF NOT EXISTS public._jl_conflict (
  id         bigserial PRIMARY KEY,
  job_key    text        NOT NULL,
  table_name text        NOT NULL,
  pk         text        NOT NULL,
  policy     text        NOT NULL,
  source_row jsonb,
  target_row jsonb,
  resolved   boolean     NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public._jl_dead_letter (
  id         bigserial PRIMARY KEY,
  job_key    text        NOT NULL,
  table_name text        NOT NULL,
  reason     text        NOT NULL,
  position   text,
  raw_row    jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public._jl_action (
  id         bigserial PRIMARY KEY,
  job_key    text        NOT NULL,
  action_id  text        NOT NULL,
  what       text        NOT NULL,
  sql        text,
  undo       text,
  actor      text        NOT NULL,              -- instance_id / you / dba
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public._jl_event (
  id         bigserial PRIMARY KEY,
  job_key    text        NOT NULL,
  kind       text        NOT NULL,
  table_name text,
  payload    text,
  created_at timestamptz NOT NULL DEFAULT now()
);
```

### MySQL
```sql
CREATE TABLE IF NOT EXISTS `_jl_lease` (
  job_key      char(10)    NOT NULL PRIMARY KEY,
  instance_id  varchar(32) NOT NULL,
  label        varchar(64),
  expires_at   datetime(6) NOT NULL
);
CREATE TABLE IF NOT EXISTS `_jl_meta` (
  job_key       char(10)    NOT NULL PRIMARY KEY,
  name          varchar(128) NOT NULL,
  instance_id   varchar(32) NOT NULL,
  schema_version int        NOT NULL DEFAULT 1,
  job_hash      char(64)    NOT NULL,
  schema_hash   char(64),
  phase         varchar(16) NOT NULL,
  position      text,
  watermark     text,
  snapshot_id   varchar(128),
  started_at    datetime(6) NOT NULL,
  updated_at    datetime(6) NOT NULL
);
CREATE TABLE IF NOT EXISTS `_jl_instance` (
  instance_id varchar(32) NOT NULL PRIMARY KEY,
  label       varchar(64),
  host        varchar(255),
  first_seen  datetime(6) NOT NULL,
  last_seen   datetime(6) NOT NULL
);
-- _jl_object / _jl_chunk / _jl_conflict / _jl_dead_letter / _jl_action / _jl_event / _jl_version 同构，略
```

**命名空间纪律**：所有对象以 `_jl_` 前缀；`gc` 只删这个前缀下的东西，永不 drop 用户表。

**多实例共存（重要）**：
- 状态表是**按 `job_key` 分区**的，不是按名字；名字相同但源/目标不同 → 两个 `job_key` → 互不干扰。
- 源端对象（slot / publication）名里带 `job_key`，同一源库被多个实例/多个任务连也不撞。
- `_jl_object` 记录「谁建的」，`gc` 默认只清**本实例创建**的；跨实例清理必须 `--force` 且先打印对方实例信息。
- 状态表结构迁移用数据库锁串行化：PG `pg_advisory_lock(hashtext('jinliang:migrate'))`，
  MySQL `GET_LOCK('jinliang:migrate', 30)`；拿不到锁就等待并重读版本，不乱改。

## 2. 幂等：位点与数据同事务

```sql
BEGIN;
  -- 一批数据（幂等写法：有主键就 upsert，无主键表必须走 insert-only + 全量校验）
  INSERT INTO report.orders (...) VALUES (...)
    ON DUPLICATE KEY UPDATE ...;
  -- 位点和数据在同一事务里推进
  UPDATE _jl_meta SET position = ?, updated_at = ? WHERE job_key = ?;
COMMIT;
```

崩溃点分析：

| 崩溃时点 | 结果 |
|---|---|
| 提交前 | 事务整体回滚；重启后从旧位点重放，目标端没有半批数据 |
| 提交后、`Advance` 源端位点前 | 目标位点已前进；重启后**以目标位点为权威**，源端 slot 由 `Advance` 追平，不重复应用 |
| 提交后、`Advance` 中途失败 | 同上；下次 `Advance` 幂等对齐 |

**权威规则**：目标库的 `_jl_meta.position` 是唯一权威；源端 slot 只是「别让变更流水被清掉」的护栏。

## 3. 崩溃恢复时序

```
0. instance_id：--instance / 本地 state 文件 / 新生成并登记 _jl_instance（见 IDENTITY.md）
1. 启动 → 尝试 INSERT _jl_lease(job_key, instance_id, now()+ttl)
   ├─ 冲突且未过期 → 另一个实例在跑：报 E1005，退出（绝不双写）
   └─ 冲突但已过期 → 接管（记一条 action："接管了超时租约"）
2. 每 5s 续租（UPDATE expires_at）
3. 读 _jl_meta：phase / position / snapshot_id
   ├─ phase=snapshot → 读 _jl_chunk 找未完成块，继续；快照 id 过期则整段重来
   ├─ phase=catchup/streaming → 从目标位点继续流
   └─ 无记录 → 新任务
4. 若源端 slot 不存在但 _jl_meta 有 position → 判定「变更流水已被清理」，E3001 要求 --resync
5. 正常结束 → phase=done，Cleanup 清 slot / publication / 临时快照；保留 meta 供查历史
```

## 4. 孤儿清理（`gc`）

判定四条，全部满足才进「可清理」清单：
1. 对象名属于本工具命名空间（`jl_<job>_*`）；
2. `active = false`（有人在读绝不删）；
3. 没有活动任务认领（`_jl_lease` 无有效记录，或该 job 已 `done`）；
4. 落后量已超 guard 阈值（或 `--force`）。

默认 `dry-run` 只列不删，输出每条的前置判定结果。
