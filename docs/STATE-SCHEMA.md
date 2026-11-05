# 津梁状态表与幂等

状态全部落在**目标库**，不依赖本地文件、不依赖外部协调器。

## 1. 表结构

### PostgreSQL
```sql
CREATE TABLE IF NOT EXISTS public._ds_lease (
  job_id     text        PRIMARY KEY,
  holder     text        NOT NULL,
  expires_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS public._ds_meta (
  job_id      text        PRIMARY KEY,
  job_hash    text        NOT NULL,          -- job.yaml 的哈希；变了就要提示
  phase       text        NOT NULL,          -- snapshot / catchup / streaming / paused / done
  position    text,                          -- 各连接器自序列化（PG LSN / MySQL file:pos:gtid）
  snapshot_id text,                          -- 初始化读数快照标识，结束后清空
  started_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public._ds_chunk (   -- 快照断点，块级续跑
  job_id     text    NOT NULL,
  table_name text    NOT NULL,
  chunk_no   int     NOT NULL,
  range_from text,
  range_to   text,
  done       boolean NOT NULL DEFAULT false,
  rows       bigint  NOT NULL DEFAULT 0,
  PRIMARY KEY (job_id, table_name, chunk_no)
);

CREATE TABLE IF NOT EXISTS public._ds_conflict (
  id         bigserial PRIMARY KEY,
  job_id     text        NOT NULL,
  table_name text        NOT NULL,
  pk         text        NOT NULL,
  policy     text        NOT NULL,          -- manual / source_wins / …
  source_row jsonb,
  target_row jsonb,
  resolved   boolean     NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public._ds_dead_letter (
  id         bigserial PRIMARY KEY,
  job_id     text        NOT NULL,
  table_name text        NOT NULL,
  reason     text        NOT NULL,
  position   text,
  raw_row    jsonb       NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public._ds_event (   -- 补偿用的载体：源端触发器写 TRUNCATE / DDL 事件
  id         bigserial PRIMARY KEY,
  job_id     text        NOT NULL,
  kind       text        NOT NULL,
  table_name text,
  payload    text,
  created_at timestamptz NOT NULL DEFAULT now()
);
```

### MySQL
```sql
CREATE TABLE IF NOT EXISTS `_ds_lease` (
  job_id     varchar(128) NOT NULL PRIMARY KEY,
  holder     varchar(128) NOT NULL,
  expires_at datetime(6)  NOT NULL
);
CREATE TABLE IF NOT EXISTS `_ds_meta` (
  job_id      varchar(128) NOT NULL PRIMARY KEY,
  job_hash    char(64)     NOT NULL,
  phase       varchar(16)  NOT NULL,
  position    text,
  snapshot_id varchar(128),
  started_at  datetime(6)  NOT NULL,
  updated_at  datetime(6)  NOT NULL
);
-- _ds_chunk / _ds_conflict / _ds_dead_letter / _ds_event 同构，略
```

**命名空间纪律**：所有对象以 `_ds_` 前缀；`gc` 只删这个前缀下的东西，永不 drop 用户表。

## 2. 幂等：位点与数据同事务

```sql
BEGIN;
  -- 一批数据（幂等写法：有主键就 upsert，无主键表必须走 insert-only + 全量校验）
  INSERT INTO report.orders (...) VALUES (...)
    ON DUPLICATE KEY UPDATE ...;
  -- 位点和数据在同一事务里推进
  UPDATE _ds_meta SET position = ?, updated_at = ? WHERE job_id = ?;
COMMIT;
```

崩溃点分析：

| 崩溃时点 | 结果 |
|---|---|
| 提交前 | 事务整体回滚；重启后从旧位点重放，目标端没有半批数据 |
| 提交后、`Advance` 源端位点前 | 目标位点已前进；重启后**以目标位点为权威**，源端 slot 由 `Advance` 追平，不重复应用 |
| 提交后、`Advance` 中途失败 | 同上；下次 `Advance` 幂等对齐 |

**权威规则**：目标库的 `_ds_meta.position` 是唯一权威；源端 slot 只是「别让变更流水被清掉」的护栏。

## 3. 崩溃恢复时序

```
1. 启动 → 尝试 INSERT _ds_lease(job_id, holder, now()+ttl)
   ├─ 冲突且未过期 → 另一个实例在跑：报 E1005，退出（绝不双写）
   └─ 冲突但已过期 → 接管（记一条 action："接管了超时租约"）
2. 每 5s 续租（UPDATE expires_at）
3. 读 _ds_meta：phase / position / snapshot_id
   ├─ phase=snapshot → 读 _ds_chunk 找未完成块，继续；快照 id 过期则整段重来
   ├─ phase=catchup/streaming → 从目标位点继续流
   └─ 无记录 → 新任务
4. 若源端 slot 不存在但 _ds_meta 有 position → 判定「变更流水已被清理」，E3001 要求 --resync
5. 正常结束 → phase=done，Cleanup 清 slot / publication / 临时快照；保留 meta 供查历史
```

## 4. 孤儿清理（`gc`）

判定四条，全部满足才进「可清理」清单：
1. 对象名属于本工具命名空间（`ds_<job>_*`）；
2. `active = false`（有人在读绝不删）；
3. 没有活动任务认领（`_ds_lease` 无有效记录，或该 job 已 `done`）；
4. 落后量已超 guard 阈值（或 `--force`）。

默认 `dry-run` 只列不删，输出每条的前置判定结果。
