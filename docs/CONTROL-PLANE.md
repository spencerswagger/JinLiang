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
  kind       TEXT NOT NULL DEFAULT 'sync',  -- sync | verify | stub，取值见下方
  phase      TEXT NOT NULL,                -- 见 internal/state/phase.go
  started_at TEXT NOT NULL, ended_at TEXT,
  rows_done  INTEGER DEFAULT 0,
  lag_bytes  INTEGER, lag_ms INTEGER,
  last_error TEXT,                         -- 错误码 + 一句话
  exit_code  INTEGER,
  finish_reason  TEXT,                     -- completed | failed | stopped | interrupted（见下方）
  heartbeat_at   TEXT,                     -- 运行中每 30s 刷新一次，用于判活
  owner_instance TEXT,                     -- 起这一轮运行的实例（settings.instance_id）
  stub       INTEGER NOT NULL DEFAULT 0    -- 已弃用：保留一版兼容，B1 收口后删除（见下方）
);

-- 一个任务同时只能有一条未结束的运行（partial unique index，迁移 v2 已建）。
-- 没有它：进程被杀后那行永远 ended_at IS NULL，历史页永久显示在跑、还能再开第二个。
CREATE UNIQUE INDEX idx_runs_one_active ON runs(job_id) WHERE ended_at IS NULL;

CREATE TABLE settings (k TEXT PRIMARY KEY, v TEXT NOT NULL);  -- instance_id / schema_version / 默认值
CREATE TABLE audit (
  id TEXT PRIMARY KEY, at TEXT NOT NULL, actor TEXT NOT NULL,
  action TEXT NOT NULL, detail_json TEXT
);
```

连接与任务的增删改查全部走服务端；UI 只提交字段，不提交 SQL。

**`runs.kind` 的取值**（2026-11-05 裁定）：

| 值 | 含义 | 何时产生 |
|---|---|---|
| `sync` | 正常同步运行 | B1 起的真实运行；也是迁移回填的默认值 |
| `verify` | 校验运行（`GET /api/jobs/{id}/verify` 背后的那一轮） | B1 的校验功能落地后 |
| `stub` | B0 骨架运行：只推进阶段，不搬运任何数据 | **仅 B0**；B1 起不再产生 |

迁移（照此口径，不许一次删列）：

```sql
ALTER TABLE runs ADD COLUMN kind TEXT NOT NULL DEFAULT 'sync';
UPDATE runs SET kind = 'stub' WHERE stub = 1;   -- 回填历史桩运行
```

**`runs.finish_reason` 的取值**：

| 值 | 含义 |
|---|---|
| `completed` | 正常跑完 |
| `failed` | 失败结束（`last_error` 里有错误码） |
| `stopped` | 用户手动停止：phase 落 `done`，**但 `finish_reason='stopped'`**，绝不伪装成正常完成 |
| `interrupted` | 进程被 kill / 断电 / 升级时发现的心跳过期遗留运行（phase 落 `failed`） |

**存活与收尾口径**（`heartbeat_at` / `owner_instance`）：

- 运行中每 **30s** 刷新一次 `heartbeat_at`；`owner_instance` 记起这一轮运行的实例身份（`settings.instance_id`）。
- **`serve` 启动时只收「心跳过期 3 轮」的 open run**（3×30s＝90s；用 `COALESCE(heartbeat_at, started_at)` 早于阈值判定）：补 `ended_at`、`finish_reason='interrupted'`、非终态 phase 落 `failed`，`last_error` 写「进程在运行中被终止」。
- **只收过期的，不无条件收所有 open run**：同一个控制库也会被 `export` / `import` / `run` 这类短命令打开，它们不该把 serve 手里正跑的那行标成中断——那会白放开活跃槽，还能再起第二个 run。所以这个收尾**只在 `serve` 启动时做，CLI 不做**。
- 判据用「心跳过期」而不是「owner 不是我」：同机重启后 `instance_id` 不变，用 owner 判活会误杀正在跑的那一行。
- 配一条 partial unique index `idx_runs_one_active`：同一任务同时只能有一行 open run（`ended_at IS NULL`）。
- 这三列与索引由独立一条迁移加（同样是 `ADD COLUMN`，顺序：加列 → 收尾过期 run → 建唯一索引）。**迁移版本号必须唯一且严格递增**：同版本号的两条会被静默跳过（`migrate()` 只按读到的 `schema_version` 判断，不报错），后果是列永远不存在、`runs` 查询全线炸。

- **「最近一次运行」只认 `kind='sync'`**：`verify` 与 `stub` 可以出现在历史列表里，但**不得进入「最近一次同步运行」的判定**——否则一轮校验或一轮桩运行会把真实同步进度盖掉，页面显示"刚跑过"，实际上从没同步。
- **`runs` 是控制面的运行摘要，不是运行时位点**（§2 红线）：真权威位点在目标端 `_jl_*`，这里只记「这个任务跑到过哪一步」，位点永远不进控制库。
- **`stub` 列已弃用**：保留一版做兼容，**B1 收口后再删**，不许一次删列。删除时同步做四件事：① 本表删掉该列；② `internal/control/migrate.go` 加一条对应的 `ALTER TABLE runs DROP COLUMN stub`；③ 历史页/详情页去掉读 `stub` 的分支，改读 `kind='stub'`；④ 导出与 API 契约里的 `stub` 字段（`Run` 的 JSON 字段）一并移除。

## 4. API 面

**鉴权三档**：`公开`（不需要登录，只用于首启与存活探测；个别端点另有「仅本机」限定，见对应行）· `已配置`（只要求管理员口令已设置，未设置时回 428 并引导 `/api/setup`）· `需登录`（会话 cookie 或 `Authorization: Bearer`）。

**CSRF**：只有「cookie 会话 + 写方法」需要 `X-CSRF-Token`（双提交）；带 `Authorization: Bearer` 的请求不需要——Bearer 不会被浏览器自动带上，不存在跨站伪造。

### 4.1 认证与首启

| 方法 | 路径 | 说明 | 鉴权 | CSRF | 幂等 | 成功 |
|---|---|---|---|---|---|---|
| GET | `/api/health` | 存活探测，只回版本与 uptime | 公开 | 不需要 | 是（只读） | 200 |
| GET | `/api/session` | 是否已设口令、调用方是否已登录；**顺带种下 `jl_csrf` cookie**（首启端点要 CSRF，token 只能由 GET 侧发） | 公开 | 不需要 | 是（只读） | 200 |
| POST | `/api/setup` | 设置管理员口令（argon2id），只允许一次；成功后发会话。反代场景请先在控制台本机完成首启 | **仅本机**（未设口令时：非本机 → 403；已配置 → 409；限速 5 次/分钟）。「本机」= `RemoteAddr` 是 loopback **且** 请求不带 `Forwarded` / `X-Forwarded-For` / `X-Real-IP` 任一转发头——带了就按非本机 fail-closed | **需要** | 否（第二次 409） | 201 |
| POST | `/api/login` | 口令换会话 token | 公开（限速 5 次/分钟） | 不需要（此时还没有会话） | 否（每次发新会话） | 200 |
| POST | `/api/logout` | 丢掉当前会话并清 cookie | 已配置 | 需要 | 是 | 200 |
| GET | `/api/meta` | 版本、`instance_id`、`schema_version`、`data_dir`、鉴权模式 | 已配置 | 不需要 | 是（只读） | 200 |

### 4.2 连接

| 方法 | 路径 | 说明 | 鉴权 | CSRF | 幂等 | 成功 |
|---|---|---|---|---|---|---|
| GET | `/api/connections` | 列表（密码永远不回传，只回 `has_secret: true`） | 需登录 | 不需要 | 是（只读） | 200 |
| POST | `/api/connections` | 新建；可只传 `dsn` 一条串；保存后自动跑一次测连（测连失败不阻断保存） | 需登录 | 需要 | **否**（同坐标重复 POST 会得到两条连接） | 201 |
| GET | `/api/connections/{id}` | 单条（含 `used_by_jobs`） | 需登录 | 不需要 | 是（只读） | 200 |
| PUT | `/api/connections/{id}` | 修改；**被任务引用时坐标（`kind/host/port/database/username`）不可改**，改则 409 `ErrIdentityLocked`（见 `docs/IDENTITY.md` §2.1）；改名称/口令/`params` 允许 | 需登录 | 需要 | 是（同 body 重复提交结果一致） | 200 |
| DELETE | `/api/connections/{id}` | 删除；仍被任务引用时 409 | 需登录 | 需要 | 是（第二次 404） | 200 |
| POST | `/api/connections/{id}/test` | 单测连接（版本、能力、耗时）；**连通失败也回 200**，看 `ok` 字段 | 需登录 | 需要 | 是（会刷新 `last_test_*`） | 200 |
| GET | `/api/catalog?connection_id=…&schema=…` | 枚举库表给 UI 勾选（带行数估算/主键/类型） | 需登录 | 不需要 | 是（只读） | 200 |

### 4.3 任务

| 方法 | 路径 | 说明 | 鉴权 | CSRF | 幂等 | 成功 |
|---|---|---|---|---|---|---|
| GET | `/api/jobs` | 列表（带两端连接名与最近一次运行） | 需登录 | 不需要 | 是（只读） | 200 |
| POST | `/api/jobs` | 新建（`spec_json` 与 yaml schema 同构）；同指纹 409 并指向已有任务 | 需登录 | 需要 | 否（同指纹 409） | 201 |
| GET | `/api/jobs/{id}` | 单条 = 任务 + `spec` + 最近 20 次运行 | 需登录 | 不需要 | 是（只读） | 200 |
| PUT | `/api/jobs/{id}` | 修改（保留 `id` 与 `created_at`） | 需登录 | 需要 | 是（同 body 一致；改完指纹撞别的任务 409） | 200 |
| DELETE | `/api/jobs/{id}` | 删除任务，**连同它的运行历史**（审计记录保留） | 需登录 | 需要 | 是（第二次 404） | 200 |
| POST | `/api/jobs/{id}/preflight` | 预检报告（四态：`ok`/`autofixable`/`warn`/`blocked`），只读 | 需登录 | 需要 | 是（只读） | 200 |
| POST | `/api/jobs/{id}/run` | 开始一轮运行；已经是运行中则 409 | 需登录 | 需要 | 否（同任务不并行） | 202 |
| POST | `/api/jobs/{id}/pause` | 在安全点暂停；没在跑则 409 | 需登录 | 需要 | 是（重复暂停无副作用） | 202 |
| POST | `/api/jobs/{id}/resume` | 从暂停恢复；没暂停则 400 | 需登录 | 需要 | 否 | 202 |
| POST | `/api/jobs/{id}/stop` | 收尾后结束；没在跑则 409 | 需登录 | 需要 | 是（重复停止无副作用） | 202 |
| GET | `/api/jobs/{id}/events` | **SSE**：阶段迁移、批次提交、延迟、错误（UI 实时刷新）；只认会话 cookie 或 `Authorization` 头，**不接受 query 里的 token** | 需登录 | 不需要（只读） | 是 | 200 |
| GET | `/api/jobs/{id}/export` | **导出 yaml（规范路径）**：与 `job.schema.json` 一致的完整 JobDoc；会记一条审计 | 需登录 | 不需要 | 是（只读） | 200 |
| GET | `/api/jobs/{id}/yaml` | **已弃用别名（deprecated，将在后续版本移除）**：与上一条完全等价，只为现有 e2e / UI / CLI 保留一个周期；新代码一律用 `/export` | 需登录 | 不需要 | 是（只读） | 200 |
| POST | `/api/jobs/import` | 导入 yaml（逐份导入，先校验再落库）；同指纹是**更新**既有任务，不是报冲突 | 需登录 | 需要 | **是**（同坐标复用连接、同指纹更新任务） | 400 请求体不是合法 yaml/json · 201 全成功 · 207 部分成功 · 422 零成功（body 带 `{total, imported[], skipped[]}`） |
| GET | `/api/export` | 导出全部任务，每份之间用 `---` 分隔 | 需登录 | 不需要 | 是（只读） | 200 |

> 导出路径以 `/api/jobs/{id}/export` 为规范；`/yaml` 是实现早期自行引入的名字，现降为别名，保留一个周期后移除（移除时同步改 `E1009` 的提示文案）。

### 4.4 运维

| 方法 | 路径 | 说明 | 鉴权 | CSRF | 幂等 | 成功 |
|---|---|---|---|---|---|---|
| GET | `/api/runs` | 运行历史（`?job_id=` 过滤、`?limit=` 上限 500）；按 `started_at` 倒序 | 需登录 | 不需要 | 是（只读） | 200 |
| GET | `/api/audit` | 控制面操作审计（`?limit=`）；按 `at` 倒序 | 需登录 | 不需要 | 是（只读） | 200 |
| GET | `/api/settings` | 设置项 + `instance_id` / `schema_version` / 密钥来源 / 监听地址 | 需登录 | 不需要 | 是（只读） | 200 |
| PUT | `/api/settings` | 改设置（当前只允许 `default_batch_rows`），白名单外 400 | 需登录 | 需要 | 是 | 200 |
| GET | `/api/doctor` | 体检：控制库完整性、数据量、活跃运行、监听地址风险 | 需登录 | 不需要 | 是（只读） | 200 |

### 4.5 未实现与未注册的路径

- `GET /api/jobs/{id}/verify`、`GET /api/gc`：**B1 才做**。在那之前它们返回 404 + `E1009`，不做假装成功的空实现。
- 其他任何未注册的 `/api/*`：一律返回 **404 + JSON 错误信封（`E1009`）**，绝不落到 WebUI 的 `GET /` 上被 HTML 兜底——否则「这个接口到底有没有实现」就再也自证不了了。
- **注册路由必须带方法**（`GET /api/x` 这种），前缀与精确路径都不例外。不带方法的前缀注册（`/api/`）会与 `GET /` 的路径宽度冲突并在注册期 panic；不带方法的**精确**路径（`/api/gc`）同样会与 `GET /api/` 的兜底冲突 panic。带方法之后更具体的模式优先，兜底不会挡住将来新增的路由。这是实测结论，不是风格偏好。

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
| `jinliang run --job <id\|name> --resync [--yes]` | **显式整轮重搬**：重建书签/位点，先清本任务建的表再重搬。E3001 唯一的自救路径；不换 `job_key`、不删历史。属危险动作（不可撤销），非交互环境必须显式 `--yes` |
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
- 多副本部署（同一目标库、多个实例）的租约与 `instance_id` 规矩见 [`SECURITY.md` §7](SECURITY.md)。

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