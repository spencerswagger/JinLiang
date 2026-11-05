# 控制面：内嵌数据库 + WebUI 是一等公民

> 2026-11-05 拍板：**WebUI 是控制面，CLI 是运维工具**；**任务与连接信息存进内置数据库，配置文件只是导入/导出格式**。
> 这份文档取代 SPEC 里「`job.yaml` 是唯一真源」的旧口径。

## 1. 为什么改

| 旧设计（YAML-first） | 问题 |
|---|---|
| 任务写在 `job.yaml` 里，CLI 读文件跑 | 用户要手写 YAML、记字段名——这正是我们说要避免的"让用户理解实现细节" |
| 连接串写在文件里 | 明文密码躺在磁盘/git/聊天记录里；改一次要编辑文件 |
| 看进度靠 `status` 命令 | 同步是长期运行的东西，用户想看的是"现在到哪了、卡在哪"，不是敲命令看一屏文本 |
| 多任务要多个文件 | 没有统一视图：哪些在跑、哪些失败、哪些今天没跑 |

**结论**：同步是一个"持续存在的系统"，不是一个"一次性的脚本"。系统就该有控制面（数据库 + 界面），配置文件只做搬运。

## 2. 两个数据库，分工必须清楚

| | 控制库（本地 SQLite） | 运行时库（目标端 `_jl_*`） |
|---|---|---|
| 存什么 | 连接、任务定义、运行历史、设置、凭证密文 | 阶段、权威位点、块断点、租约、冲突、死信、审计 |
| 在哪 | 本机 `--data-dir`（默认 `~/.config/jinliang/`） | **每个目标数据库里** |
| 谁是权威 | 任务的"应该做什么" | 同步"已经到哪了" |
| 丢了会怎样 | 任务定义丢失（可从 yaml/备份恢复） | 位点丢失 → 必须重新初始化 |
| 为什么这样分 | 单机部署简单、备份容易 | 位点必须与数据同事务提交（幂等根基），不能放在本地 |

**红线**：运行时位点永远不在控制库里。控制库只记"这个任务跑到过哪一步"的摘要（供 UI 展示），真权威在目标端。

## 3. 控制库数据模型（SQLite）

```sql
-- 连接：密码只存密文，UI 永远看不到明文
CREATE TABLE connections (
  id            TEXT PRIMARY KEY,          -- uuid7
  name          TEXT NOT NULL,             -- 用户起的名字，如「生产订单库」
  kind          TEXT NOT NULL,             -- postgres | mysql
  host          TEXT NOT NULL,
  port          INTEGER NOT NULL,
  database      TEXT NOT NULL,
  username      TEXT NOT NULL,
  secret_enc    BLOB NOT NULL,             -- AES-256-GCM(密码)，密钥见 §5
  params_json   TEXT NOT NULL DEFAULT '{}',-- sslmode / charset / 额外参数
  last_test_at  TEXT, last_test_ok INTEGER, last_test_msg TEXT,
  created_at    TEXT NOT NULL, updated_at TEXT NOT NULL
);

-- 任务：语义字段整体存 JSON（字段与 job.yaml schema 一致，便于导入导出）
CREATE TABLE jobs (
  id            TEXT PRIMARY KEY,          -- uuid7，稳定不变
  job_key       TEXT NOT NULL UNIQUE,      -- 指纹（docs/IDENTITY.md），同一同步的唯一身份
  name          TEXT NOT NULL,             -- 展示名，可重复
  source_id     TEXT NOT NULL REFERENCES connections(id),
  target_id     TEXT NOT NULL REFERENCES connections(id),
  spec_json     TEXT NOT NULL,             -- mode/content/conflict/reliability/guard
  enabled       INTEGER NOT NULL DEFAULT 0,
  created_at    TEXT NOT NULL, updated_at TEXT NOT NULL
);

-- 每次运行一行（历史与"最近一次状态"）
CREATE TABLE runs (
  id         TEXT PRIMARY KEY,
  job_id     TEXT NOT NULL REFERENCES jobs(id),
  phase      TEXT NOT NULL,                -- 见 internal/state/phase.go
  started_at TEXT NOT NULL, ended_at TEXT,
  rows_done  INTEGER DEFAULT 0,
  lag_bytes  INTEGER, lag_ms INTEGER,
  last_error TEXT,                         -- 错误码 + 一句话
  exit_code  INTEGER,
  stub            INTEGER NOT NULL DEFAULT 0, -- B0 桩：这一轮没有搬数据
  heartbeat_at    TEXT,                    -- 活着的心跳；过期 = 进程死了
  owner_instance  TEXT,                    -- 哪个控制面实例在跑（settings.instance_id）
  finish_reason   TEXT                     -- completed | failed | stopped | interrupted
);
-- 一个任务同时只能有一条未结束的运行：进程被 kill -9 后，
-- 靠 heartbeat_at 过期 + 这条约束把「永远显示在跑」和「再开第二个」一起关死。
CREATE UNIQUE INDEX idx_runs_one_active ON runs(job_id) WHERE ended_at IS NULL;

CREATE TABLE settings (k TEXT PRIMARY KEY, v TEXT NOT NULL);  -- instance_id / schema_version / 默认值
CREATE TABLE audit (
  id TEXT PRIMARY KEY, at TEXT NOT NULL, actor TEXT NOT NULL,
  action TEXT NOT NULL, detail_json TEXT
);
```

连接与任务的增删改查全部走服务端；UI 只提交字段，不提交 SQL。

**运行存活语义**（B0 起生效）：

- `heartbeat_at` 由运行中的独立 ticker 每 30s 刷新（`--heartbeat` / `JINLIANG_HEARTBEAT_SECONDS`
  可调，5–600s）。**与阶段推进无关**：暂停、长事务、卡在一次大 batch 里都照打，
  否则「活着但没动」的 run 会被下一次启动误判为死掉。
- 过期阈值 = **3× 心跳**（默认 90s），`serve` 启动时打印，并按它执行 `RecoverStaleRuns`：
  心跳过期的 open run 标 `finish_reason=interrupted`、`phase=failed`、写一条审计。
- 迁移 v2 只收尾**心跳过期**的遗留行；心跳新鲜的 open run 留给它自己的进程收尾——
  只跑 200ms 的 `doctor`/`export` 也会打开同一个控制库，它无权替别人判死。
- 收尾一律落到可表达终态（done/failed），历史页用 `finish_reason` 覆盖显示
  「已完成 / 已停止 / 已中断 / 失败」：`phase=done` 既可能是跑完也可能是手动停止，
  只看 phase 会撒谎。手动停止是唯一允许不走阶段表的收尾（在 preflight 也能停）。
- 有运行历史的任务**不许换连接/改表范围**（会换 job_key，目标端同步状态变孤儿），
  返回 `ErrIdentityLocked`；要换就新建任务。

## 4. API 面

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/api/connections` | 列表（密码永远不回传，只回 `has_secret: true`） |
| POST/PUT | `/api/connections[/{id}]` | 新建/修改；保存前自动跑一次连通性与能力探测 |
| POST | `/api/connections/{id}/test` | 单测连接（返回版本、能力、耗时） |
| GET | `/api/catalog?connection_id=…&schema=…` | 枚举库表给 UI 勾选（带行数估算/主键/类型） |
| GET/POST/PUT | `/api/jobs[/{id}]` | 任务 CRUD（`spec_json` 与 yaml schema 同构） |
| POST | `/api/jobs/{id}/preflight` | 预检报告（四态） |
| POST | `/api/jobs/{id}/run` `\|/pause` `\|/resume` `\|/stop` | 控制 |
| GET | `/api/jobs/{id}/events` | **SSE**：阶段迁移、批次提交、延迟、错误（UI 实时刷新）。帧格式见 §4A |
| GET | `/api/jobs/{id}/verify` | 校验（流式进度） |
| GET | `/api/jobs/{id}/export` | 导出 yaml（与 `job.schema.json` 一致） |
| POST | `/api/jobs/import` | 导入 yaml（逐份导入；返回 `{total, imported[], skipped[], dry_run}`，有跳过时 **207**，不是 201） |
| GET | `/api/doctor` `/api/gc` | 体检与孤儿清理（默认 dry-run） |

### 4A. SSE 帧约定（客户端依赖，改动等于破坏兼容）

```
id: <boot_epoch>-<seq>
event: phase | progress | note | error | done
data: {"epoch":"…","seq":42,"type":"phase","phase":"snapshot", …}

event: reset
data: {"epoch":"…","reason":"控制面进程重启过，之前的事件序号已经失效；下面从这一轮的开头重新发送"}
```

- `seq` 是**进程内**计数器，重启归零；`boot_epoch` 每次启动重写（`settings.boot_epoch`）。
  重连带 `Last-Event-ID` 时先比 epoch：**不匹配发 `reset` 再从当前环形缓冲重发**，
  绝不拿旧 seq 去算补档（那会静默漏掉重启前后之间的事件）。
- 服务端**先 Subscribe 再回放**，重叠部分按 `seq` 去重：两步之间发布的事件不会丢，
  最多重复一次；反过来写会永久丢事件。
- 浏览器必须 `addEventListener('<type>')`，不能只挂 `onmessage`（带 `event:` 的帧不会进 onmessage）。
- 心跳是注释行 `: ping`（15s），不影响事件解析。
- `event:` 名是**闭集** `phase|progress|note|error|done`：服务端遇到未知 type 会退回「无名事件」
  （只发 `id`+`data`，落 `onmessage`），否则浏览器按名字订阅会静默丢掉它。加新 type 要同时改
  `control.EventTypes` 与 `web/static/app.js`。
- `Last-Event-ID` 只带序号（没有 epoch）一律当作未知 epoch：发 `reset`，不从环形缓冲里按数字补。

### 4B. 导入/导出的多文档语义

- `export --all` 与 `GET /api/export` 写出 `---` 分隔的**多文档**流，一份一个任务。
- 导入逐份解析：`import` 回报「导入 N 份 / 跳过 M 份」并逐条列出跳过原因；
  **有跳过时 CLI 退出码非 0**；HTTP 分档：部分成功 `207`、**一份都没进 `422`**
  （207 属于 2xx，`curl -f` 会当成功，那和「不能看起来像成功」矛盾）。
- 空文档（只有注释/空白）跳过；单文档入口（`ParseJobYAML`）收到多份会明确报错，不静默取第一份。

错误一律走 `internal/core/codes.go` 的结构：`{code, class, message, why, next[], risk}`，UI 直接渲染四段式卡片，不需要翻译。

## 5. 凭证加密

- **算法**：AES-256-GCM；每条连接独立 nonce。
- **密钥来源三选一**（按优先级）：`--key-file`（0600 强制）→ 环境变量 `JINLIANG_KEY` → 首次启动生成 `data-dir/key`（0600）。
- **不写日志、不回传、不进导出**：导出 yaml 时密码字段写 `env:JINLIANG_DSN` 占位或直接省略，由用户在导入时补齐。
- 备份控制库 = 备份"配置 + 密文"；没有 key 文件则无法解密（这是特性，不是缺陷，要在提示里说明）。

## 6. WebUI 结构（单二进制内嵌）

页面清单，按"用户一天怎么用"排序：

1. **总览**：在跑的任务卡（阶段、延迟、吞吐、最近错误）、今日完成/失败、磁盘与书签占用。
2. **任务详情**：实时流（SSE）+ 阶段时间线 + 最近 100 条事件 + 「系统为你做了什么」动作清单（可 undo）。
3. **新建任务向导**（核心）：选连接 → 勾表（catalog 带行数与主键）→ 选频次/方向/内容 → 预览类型映射与风险 → 预检 → 确认执行。**全程不出现 WAL/slot/LSN**。
4. **连接管理**：新建/测连/能力探测结果/被哪些任务引用。
5. **运行历史**：每次运行的阶段与错误，可下钻。
6. **诊断**：doctor 结果、gc 孤儿清单、冲突与死信处理。
7. **设置**：实例 ID、默认批次、监听地址、备份/导出。

**交互铁律**：
- 任何操作前先给「将要发生什么」（就是 `plan` 的 UI 版本）；危险动作二次确认 + 可撤销。
- 数字墙优先、文案 ≤150 字、移动端可用（他在手机上看）。
- 错误卡片必须能直接"照着做"：按钮化 next step（复制 SQL / 重试 / 打开对应连接）。

**技术选择**：Go `html/template` + 内嵌静态资源 + htmx（单 js 文件 vendored）+ 一份手写 CSS（不引入 Node 构建链）。SSE 用原生 `EventSource`。理由：单二进制、无前端工具链、可被 AI 助手直接读。

## 7. CLI 的新职责（退位但不消失）

| 命令 | 用途 |
|---|---|
| `jinliang serve` | 起控制面（默认 `127.0.0.1:8080`），首次启动引导建管理员口令 |
| `jinliang version` / `doctor` | 基础运维 |
| `jinliang import/export` | 与控制库交换 yaml（GitOps、迁移、备份） |
| `jinliang run --job <id\|name>` | 无 UI 的 headless 执行（容器/CI/无头环境） |
| ~~`plan`~~ / ~~`preview`~~ | 已被 UI 的预检与预览取代；CLI 只留 `--json` 版给自动化 |

`job.yaml` 保留为**导入导出格式**，schema 不变（`schema/job.schema.json` 继续维护），但**不再是运行时的真源**。

## 8. 首次启动与迁移

```
jinliang serve
  → 没有 data-dir 时创建：jinliang.db + key（0600）+ instance_id
  → 浏览器打开：设置管理员口令（argon2id）
  → 引导页：新建第一个连接（可"从 yaml 导入"）
```

旧 yaml 项目：`jinliang import --file job.yaml --connection-from-env` → 建连接（凭证从 `env:` 读一次并加密入库）→ 建任务 → 预检。

## 9. 安全边界

- 默认只监听 `127.0.0.1`；`--listen 0.0.0.0` 必须显式并打印风险提示。
- 会话 cookie：HttpOnly + SameSite=Lax + 24h；登录失败限速（5 次/分钟）。
- CSRF：所有写操作校验双提交 token；SSE 只读。
- 不提供多租户/多用户角色（v1 单管理员）；不做 OAuth（二期可选）。
- 控制库权限 0600，key 0600；`--data-dir` 不允许落在世界可读目录。

## 10. 测试

| 层 | 内容 |
|---|---|
| 单元 | 加密/解密、DSN 解析、spec_json 校验（复用 job.schema）、job_key 派生 |
| HTTP | 每个 handler 的表驱动测试（成功/校验失败/未授权/CSRF 缺失） |
| UI | 模板渲染快照（关键页面结构存在关键元素）；Playwright 冒烟（v0.2 起，GHA 跑） |
| 端到端 | 起 `serve` → API 建连接/任务 → run → 读 SSE 到 done（不经浏览器） |

## 11. 代价（诚实记录）

1. 工程量增加：多一层存储 + API + UI，比 CLI 重；M1 之前必须先有 B0。
2. 单二进制体积变大（内嵌模板/静态资源，预计 +1–2 MB，仍 <25 MB）。
3. 控制库是新的故障点：需要备份策略与损坏检测（SQLite `PRAGMA integrity_check`）。
4. headless 场景必须保留 `run --job`，否则容器里没法用。
5. 与"提示即文档"一致：UI 才是提示的主战场，`docs/` 只服务贡献者。