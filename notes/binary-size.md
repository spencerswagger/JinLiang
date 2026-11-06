# 二进制体积核查（29.4 MB 从哪来）

## 结论：29.4 MB 是 debug 构建，不是发布产物

同一提交（`ce3d210`）实测：

| 构建方式 | 字节 | 说明 |
|---|---:|---|
| `go build ./cmd/jinliang` | 29,419,441 | debug：DWARF + 符号表 |
| `-trimpath -ldflags "-w"`（留符号） | 22,128,317 | 只去 DWARF |
| `CGO_ENABLED=0 -trimpath -ldflags "-s -w"` | 20,144,288 | **发布参数，19.21 MiB** |

差值 **9.3 MB** 全部是调试信息，不是依赖膨胀；发布产物仍在 25 MB 预算内，**不需要改预算**。

## 发布产物里谁占空间（`-w` 构建上 `go tool nm` 归因，含内联/别名误差）

| 依赖 | 约 |
|---|---:|
| `modernc.org/sqlite/lib`（+libc） | 6.8 MB |
| `pgx/v5/pgtype` + `pgconn` + `pgproto3` | 3.2 MB |
| `crypto/tls` + `crypto/x509` + nistec | 1.6 MB |
| std `runtime` / `net` / `net/http` / `reflect` / `slices` | 2.5 MB |
| 业务代码（control/httpapi/engine/verify/gc/doctor…） | ~1.5 MB |
| WebUI embed（htmx 50 KB + 模板/CSS 约 50 KB） | ~0.1 MB |

sqlite（控制库）、pgx（连接）、TLS 都是必需项；`-s -w`/`-trimpath` 已用。
试过 `-tags osusergo,netgo` 与 `-buildid=`：体积变化 <100 字节，不值得。

## 改了什么

- `scripts/check_binary_size.sh`：
  - 明确打印发布构建参数；
  - 构建后验证产物**无符号表**（`go tool nm` 失败）、`trimpath=true`、`CGO_ENABLED=0`；
  - 新增 `--from-file PATH`：校验现成产物，debug 构建直接 FAIL 并指出 29.4 vs 20.1。
- README：补「二进制体积」小节与 debug/发布差异，避免把构建方式误判成预算问题。
- `bench/baseline.json` 刷新到当前 main：快照 `BulkApply` 83,052 行/s、
  增量（#33 批量 upsert 后）56,886 行/s、p99 16.99ms、空转 15.8MB、冷启 29ms、
  发布二进制 19.21MB。

## 没做什么

- 没有为了压数字砍功能或换掉 sqlite/pgx。
- 没有改 25 MB 预算（发布产物仍达标）。
