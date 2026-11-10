# 三条自报缺口的收口（P1 TRUNCATE 白名单 / P2 二进制列 / P3 5.7 字符数）

基线 main `0c1eecb`。分支 `fix/data-p1p2p3`。不动 engine 状态机、不写门禁脚本。

---

## P1 `TRUNCATE` 白名单（安全优先）

**现状**：`mysqlsink.applyImplicitCommitStatements` 收到源端 `EvTruncate` 就
`TRUNCATE TABLE`，**不看所有权**；`TruncateTable`（重搬路径）同样是裸 TRUNCATE。
重搬那条路上引擎确实先问过 `DidCreateTable`，但 sink 自己没有任何闸门——
下一个调用方（比如 Core 把暂停接进通用路径、或某条新写路径）绕过去就没人挡。

**改法**：新增 `internal/mysqlsink/truncate.go`，把「清一张表」收敛成唯一入口
`truncateOwned(ctx, ref, reason)`：

| 判定 | 行为 |
|---|---|
| `_jl_object` 里有 `schema.table#table` 且同一 `job_key` | 清 |
| 有登记但**不是**本任务建的 / 没有登记 | **明确失败**（`ErrTruncateNotOwned`，可判等）+ 四段式：谁的表、为什么想清、三条出路（`on_exists=recreate` / 移出 `content.tables` / 手工 TRUNCATE）、风险（不可回滚） |
| 查不出来（`_jl_object` 读失败） | **也失败**，且文案点名「是被 `_jl_object` 挡住」，与「不是我们的」区分开——判断不了就别动数据 |

两个调用点都走它：`TruncateTable`（重搬）与 `Apply` 里的 `EvTruncate`（源端清空跟随）。
**不静默跳过**：跳过之后重搬只会把存量 upsert 进去，「本该清掉的行」继续留着，
用户看到的是「同步成功」+ 一张内容错的目标表。

**真跑用例**（`internal/mysqlsink/truncate_integration_test.go`，MySQL 8.0.28）

| 用例 | 断言 | 结果 |
|---|---|---|
| `TestIntegrationTruncateRefusesUserTable` | 用户表（未登记、7 行）→ 拒绝 + 行还在 + 文案含三条出路 | PASS 0.27s |
| `TestIntegrationTruncateEventRefusesUserTable` | 同一张用户表收到 `EvTruncate` → 拒绝 + 5 行仍在 | PASS 0.26s |
| `TestIntegrationTruncateOwnedTableWorks` | 登记过的表：`TruncateTable` 与 `EvTruncate` 都照常清 | PASS 0.44s |
| `TestIntegrationTruncateFailsClosedWhenOwnershipUnknown` | 删掉 `_jl_object` → 失败且不是 `ErrTruncateNotOwned`、3 行仍在 | PASS 0.48s |

误伤用例的关键输出（原样）：

```
目标表不是本任务建的，拒绝清空：`jl_b3_user_owned`（0123456789）。为什么会想清它：结构对齐后要整表重搬，
先清掉上次写进去的行。你能做什么：① 想让这张表归本任务管 → 在任务里把它改成可重建（on_exists=recreate），
或先手工清空它再重跑（`TRUNCATE TABLE `jl_b3_user_owned``，只在你确认里面没有自己的数据时做）；
② 不想让本任务碰它 → 把它从 content.tables 里去掉后重新 import；③ 只是想让源端这次 TRUNCATE 跟过去
→ 手工在目标端执行同一条 TRUNCATE。风险：手工清空是**不可回滚**的，表里的行找不回来；本实现不会替你猜
```

**结论：修复**（不是降级）。顺带确认没打坏别的路径：`TestMySQLTarget*` 12 条全 PASS（含重搬清表、
`full_interval`、用户表崩溃硬停）。

**评审后修正（Owl II 必改 ②③）**
- `ErrTruncateNotOwned` 原来会落到 `hint()` 的兜底「未识别的数据库错误 → 把日志发给维护者」，
  把三条出路盖住了：已加专门分支（用户自己就能解决的事不找维护者），用例 `TestHintTruncateNotOwnedGivesRealOptions`。
- 出路③ 原来是**死路**：只手工 TRUNCATE 不会放行（失败批的位点没越过这条事件，重跑撞同一道闸）。
  文案改成「手工清空 **+** 改 recreate/移出范围才放行」，并写明为什么。

---

## P2 二进制列按排序规则判定

**现状**：`decodeStringCell/decodeBlob` 用「内容是不是可打印 UTF-8」猜。于是
`BINARY(4)='abcd'`（内容恰好合法）被当成 `KindString` → pgsink 走 `$n::text::bytea`，
值以反斜杠开头时被 PG **静默改写**（不报错、数据已变）；类型串也把 binary 写成 varchar，
`binary↔char` 的改动在「改类型」判定里看不见（该暂停不暂停）。

**改法**：判据换成 **TABLE_MAP 的排序规则**——`binary` 是固定 id `63`
（`TableMap.columnBinaryKind(i)`，三态：`binBinary` / `binText` / `binUnknown`）：

- `binBinary`：`BINARY/VARBINARY/BLOB` 一律 `KindBytes`，内容长什么样都不影响；
- `binText`：`CHAR/VARCHAR/TEXT` 一律 `KindString`（非法 UTF-8 仍退回字节，不改写内容）；
- `binUnknown`（5.7 没有可选元数据）：退回内容启发式，并在注释里写明这是**兜底不是常态**。

类型串侧沿用上一轮已修的字符集渲染（`charset=="binary"` → `binary/varbinary/blob`）。

**真跑用例**（`internal/mysqlsource/integration_binary_test.go`，真 binlog）

`TestIntegrationBinaryVsTextDecoding`：一张表同时带 `BINARY(4)/VARBINARY(8)/BLOB/CHAR(10)/VARCHAR(16)/TEXT`，
灌两行（一行内容全是可打印 ASCII，一行是 `X'5C313233'` / `X'00FF'`）：

- 判据①：`BINARY(4)`、`VARBINARY(8)`、`BLOB` 都 `KindBytes` 且内容 `abcd` 原样；`X'5C313233'` 是 4 字节且首字节 `0x5C`（**这条就是原先被静默改写的形态**）；
- 判据②：`CHAR(10)`/`VARCHAR(16)`/`TEXT` 都 `KindString`（`中文` / `hello` / `文本`）；
- 判据③：类型串 `[int binary(4) varbinary(8) blob char(10) varchar(16) text]`——binary 与 char 分开，改类型判定不会再漏。

结果：**PASS 0.89s**。

**评审后修正（Owl II 必改 ①，CI 的 `mysql 5.7` 腿红）**

5.7 没有排序规则 → `binUnknown` → 内容启发式 → `BINARY(4)='abcd'` 仍然被判成字符串。
不按「5.7 就跳过用例」降级，改成**从 catalog 补判据**：`FillColumnKinds(tm, known)` 把
`information_schema.columns.CHARACTER_SET_NAME`（`binary` 即按字节）写进 `TableMap.ColumnKinds`，
在 **TABLE_MAP 解析完、行事件解码之前**（`Parser.OnTableMap` 回调）填好。
于是 5.7 与 8.0 走同一条判定，内容启发式只剩「catalog 也没有」的兜底。

- 用例 `TestFillColumnKindsCovers57`：没有 catalog → `binUnknown`（已知边界）；
  有 catalog（binary/binary/utf8mb4）→ `binBinary/binBinary/binText`，且 `BINARY(4)='abcd'` 交字节；
- `TestFillColumnKindsRefusesWhenColumnCountDiffers`：列数对不上就不补（不按序号错位猜）；
- 真库用例改走 `s.Catalog(...)` + `SetKnownTables`（与引擎 `newMyRunSource` 同一条前置）。

**第二轮（5.7 腿又红两次，两次都是真发现）**

1. `CHARACTER_SET_NAME` 对 `BINARY/VARBINARY/BLOB` 是 **NULL**（真库实测），
   把「字符集空」当成「不知道」→ 又退回内容启发式。改成 `catalogBinaryKind`：
   有字符集看字符集，没有就看**类型名**（binary/varbinary/blob 家族 → 字节；
   char/varchar/text 家族 → 字符）。用例 `TestCatalogBinaryKind` 10 格 + 真库 information_schema 断言。
2. 解码对了、**类型串**还是 `char(4)`（5.7 的 binlog 只说字符家族）。基名纠正原来放在
   `enrichRelation` 里，而 Relation 是 assembler 在 TABLE_MAP 处就造好的——不经过 enrich 的
   调用方看到的还是 `char(4)`。改成在 `tableFromMapWith` 里按 `columnBinaryKind` 就地纠
   （`rerenderForKind`：char↔binary / varchar↔varbinary / text↔blob，长度仍取 binlog）。
   用例 `TestEnrichRelationFixesBinaryBaseOn57` + `TestFillColumnKindsCovers57` 里的类型串断言。
3. 顺带把真库用例从 `RunStream` 改到 **`Stream`**（引擎走的就是它，会补列名/换算字符数）——
   原来那条用例绕过了 enrich，所以「Relation 长什么样」这件事其实没被验到。

结果：CI **13 个 job 全绿**，含 `mysql 5.7` 腿。

**结论：修复。**

---

## P3 5.7 字符数精度

**现状**：5.7 的 TABLE_MAP 没有可选元数据，拿不到列字符集，VARCHAR/CHAR 的长度只能按**字节**报
（utf8mb4 的 `VARCHAR(64)` → `varchar(256)`）。后果两条：提示里的数字错；与 catalog（字符）比对时
把「同一列」判成改类型 → 每张多字节字符列都假暂停。

**选的路：保守换算（不是降级）**——用**流开始时从 information_schema 读到的字符集**做换算：

`bytesToCharsType("varchar(256)", "utf8mb4") = "varchar(64)"`（CHAR 同理；`varbinary`/`blob`/`text` 不动）。

同时把 `looseSameType` 的**长度倍数放行彻底删掉**：以前允许 `varchar(N) ≈ varchar(N×2..4)`，
在字符集未知时这条会**漏暂停**——latin1 的 `varchar(64)→varchar(128)`（字节恰好 ×2）
会被当成「utf8mb3 的同一列」放过。现在长度必须逐字相等，换算已经在上游做完。

**误差方向（写进代码注释与这里）**：换算用的是「流开始那一刻」的字符集。
若源端在流中把**列字符集本身**也改了，我们会按旧字符集算 → 长度对不上 → **多暂停一次**（安全方向），
不会出现「该停没停」。catalog 也对不上（列数变了）时不做换算、保持字节原样，同样走「暂停」。

**用例**（`internal/mysqlsource/relation_enrich_test.go`）

- `TestBytesToCharsType` 8 格：utf8mb4/utf8mb3/latin1/空字符集（原样返回）/varbinary 不除/text 不动/除不尽不猜 —— 全 PASS；
- `TestEnrichRelationConverts5_7ByteLengths`：binlog 只有 `varchar(256)`、catalog 是 `varchar(64)`+utf8mb4
  → 换算成 `varchar(64)`、补上字符集、**与 catalog 判成同一列**（不再假暂停）；
- `TestLooseSameType` 改成新语义：`varchar(256)` vs `varchar(64)`、`char(40)` vs `char(10)`、
  `varchar(128)` vs `varchar(64)` 一律**判不同**（宁可多暂停）。

**结论：修复 + 明确误差方向**（不是「5.7 下不支持自动判定」的降级）。
残余限制：只有「源端在流中改了列字符集」这一种情况会多暂停一次——安全方向，一次对齐即可。

---

## 自检

- `gofmt -l .` 空；`go vet ./...` 绿；`go test ./...` 全绿；
- `go test -tags=integration ./internal/mysqlsink`：**40 条 PASS**（含新增 4 条 TRUNCATE 用例、6 条 PauseRegistry 用例）；
- `go test -tags=integration ./internal/engine -run TestMySQLTarget`：**12 条 PASS**（重搬清表/整轮重搬/用户表崩溃硬停都不受影响）；
- `go test -tags=integration ./internal/mysqlsource -run TestIntegrationBinaryVsTextDecoding`：PASS 0.89s。
