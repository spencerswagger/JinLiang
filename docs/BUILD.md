# 构建、发布、升级

## 1. 构建

```bash
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=$(git describe --tags --always)" \
  -o dist/jinliang ./cmd/jinliang
```
- 纯 Go 依赖 -> 交叉编译到 linux/amd64、linux/arm64、darwin/arm64。
- 产物 <25 MB 是设计目标（L5 性能基线里会检查）。
- `gofmt -l` 为空、`go vet` 干净、`go test ./...` 全绿是合入前提（CI 已强制）。

## 1A. 控制面内嵌

- WebUI 用 `embed.FS` 打进二进制（模板 + htmx + 一份手写 CSS），**不引入 Node 构建链**。
- 控制库用纯 Go SQLite（`modernc.org/sqlite`），保持 `CGO_ENABLED=0` 与交叉编译。
- 体积预算：UI 资源 <1 MB，SQLite 驱动约 +3 MB ⇒ 仍守 <25 MB 目标。

## 1B. headless 验收（M0.5 判据：不经浏览器跑完整个流程）

```bash
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o dist/jinliang ./cmd/jinliang
. ./.keys/env.sh            # 或自己 export 四个 DSN
scripts/e2e_headless.sh ./dist/jinliang
```

- 脚本自举：先 `go run ./test/fixture` 在测试库里建 `e2e_*` 两张表，空库也能跑。
- 四个 DSN 全从环境读（`JINLIANG_TEST_{PG,MYSQL}[_SRC]_DSN`），脚本里没有字面量连接串；
  **缺哪个就打印 SKIP 并列出覆盖清单**，不静默通过。
- 链路：建连接（真测连拿版本）→ catalog 枚举 → 建任务 → 预检（含 blocked 时给出可执行下一步）→
  `run` 桩 → SSE 看到全部阶段 → 运行历史落库 → export yaml（断言不含口令）→
  **重启进程验数据仍在** → `doctor`/`export`/`import` 往返 → yaml 过 `schema/job.schema.json`。
- 只有一种引擎时目标端回落同一实例（换 schema），单库也能验完整链。
- 离线沙盒装不上 `jsonschema`/`pyyaml` 时，`validate_schema.py` 会显式 SKIP，
  由 `scripts/validate_schema_lite.py`（stdlib、同一份 schema 文件）做硬断言；CI 用真 jsonschema。

## 2. 发布物

| 文件 | 说明 |
|---|---|
| `jinliang-<os>-<arch>.tar.gz` | 二进制 + LICENSE + README |
| `checksums.txt` | sha256 |
| Docker 镜像（可选） | `FROM gcr.io/distroless/static`，只 COPY 二进制 |
| `testmatrix.yaml` | 随仓库发布，供用户核对哪些版本被验证过 |

### 2.1 打 tag 前的前提（release 会自己验）

- `release.yml` 在打 tag 时会先跑**三平台交叉编译**（linux/amd64、linux/arm64、darwin/arm64，
  约 80s，与 `ci.yml` 的 `cross-compile` job 同一段脚本），编不过就红；
- 历史前提（现在是双保险）：**tag 应从「已经在 main 上跑过 `ci`」的 commit 打**——
  这样被发出去的版本在合并时就已经过一次平台构建验证；
- 性能回归（`bench/baseline.json`）、二进制体积（<25 MB）、单元覆盖度（≥40%）也在同一个 workflow 里，任一不达标即失败。

## 3. 部署

systemd 单元示例：
```ini
[Unit]
Description=jinliang sync
After=network-online.target

[Service]
ExecStart=/usr/local/bin/jinliang run /etc/jinliang/orders.yaml
Restart=on-failure
RestartSec=10
EnvironmentFile=/etc/jinliang/env
DynamicUser=yes
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes

[Install]
WantedBy=multi-user.target
```

## 4. 版本与状态表迁移

- 二进制版本语义：`MAJOR.MINOR.PATCH`；`0.x` 期间允许破坏性变更，但必须写迁移说明。
- `_jl_meta.schema_version` 记录状态表结构版本；启动时只做**向前迁移**（加列、加表）。
- 不支持状态表降级：回滚二进制前需 `jinliang run --resync` 或在旧版本上不能跑（文档写清）。
- `job_hash` 与 `schema_version` 不匹配时，启动只提示不自动改。

## 5. 升级路径（用户视角）

```
1. 停旧进程（优雅退出，位点已提交）
2. 换二进制
3. jinliang doctor            # 体检：孤儿对象、位点、租约
4. jinliang run job.yaml      # 自动向前迁移状态表，续跑
```
