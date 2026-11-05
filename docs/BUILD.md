# 构建、发布、升级

## 1. 构建

```bash
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=$(git describe --tags --always)" \
  -o dist/jinliang ./cmd/jinliang
```
- 纯 Go 依赖 -> 交叉编译到 linux/amd64、linux/arm64、darwin/arm64。
- 产物 <25 MB 是设计目标（L5 性能基线里会检查）。
- `gofmt -l` 为空、`go vet` 干净、`go test ./...` 全绿是合入前提（CI 已强制）。

## 2. 发布物

| 文件 | 说明 |
|---|---|
| `jinliang-<os>-<arch>.tar.gz` | 二进制 + LICENSE + README |
| `checksums.txt` | sha256 |
| Docker 镜像（可选） | `FROM gcr.io/distroless/static`，只 COPY 二进制 |
| `testmatrix.yaml` | 随仓库发布，供用户核对哪些版本被验证过 |

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
- `_ds_meta.schema_version` 记录状态表结构版本；启动时只做**向前迁移**（加列、加表）。
- 不支持状态表降级：回滚二进制前需 `jinliang run --resync` 或在旧版本上不能跑（文档写清）。
- `job_hash` 与 `schema_version` 不匹配时，启动只提示不自动改。

## 5. 升级路径（用户视角）

```
1. 停旧进程（优雅退出，位点已提交）
2. 换二进制
3. jinliang doctor            # 体检：孤儿对象、位点、租约
4. jinliang run job.yaml      # 自动向前迁移状态表，续跑
```
