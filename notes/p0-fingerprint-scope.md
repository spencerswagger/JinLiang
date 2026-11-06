# verify：跨端指纹不可比 → 改走逐行全表（不再永久假红）

## 问题

MySQL 指纹是客户端 sha256（`mysql-canon-v1`：列名参与、按列名排序、主键定序），
PG 是服务端 `sum(hashtextextended)`（`pg-hashtextextended-v1`）——两个数**永远不可能相等**。
若跨端直接比指纹，PG→MySQL 的 `verify` 会每次失败（今晚第三次同类假红：
`big.Rat` vs 文本、`float32` 尾数、现在这个）。

## 改法

- `internal/verify/fingerprint.go`：`FingerprintScoper` + `FingerprintComparable`；
  `runtime.DB` 按 kind 暴露 `FingerprintScope()`（PG / MySQL）。
- 两侧 scope 不同、或一端未声明 → `Report.Fingerprint{Comparable:false}` +
  数据级 `fingerprint_scope_mismatch`（warn），**明确写「已改用逐行全表比对」**；
  数据级本来就走全表顺序扫描（这轮之前已实现），不会静默假通过。
- 顺带修跨端文本口径：text 列 `[]byte` 按字符串、`text_len` 按原始字节，
  避免 PG `string` vs MySQL `[]byte` 的 `s…` / `x'hex'` 差异造成行级假红。
- 结论行在数据级注明「跨端指纹不可比，已改用逐行比对」（不冒充普通通过）。

## 测试

- `TestFingerprintScopeMismatchUsesRowScan`：PG scope vs MySQL scope → 不可比、有 warn、
  结论带限定语、`tables_full_scan == 1`。
- `TestFingerprintSameScopeComparable`：同 scope 不报警。
- 真库 `TestCrossEngineVerifyPGToMySQL`：PG 源表 → MySQL 目标表，数据一致时
  逐行比对通过，`Report.Fingerprint.Comparable=false` + `fingerprint_scope_mismatch`。

## 验证

```
go test ./internal/verify
go test -tags=integration ./internal/verify
```
