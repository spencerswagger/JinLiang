# S3（unique_key）落地笔记 · PR #120

## 关键口径（照定稿，不自己改）

- 身份顺序：**源端主键 > 用户声明 `unique_key` > 无身份（insert-only）**。源端有主键时声明被忽略 + warn（格 7）。
- 声明只是「核对目标端事实」的凭据，**不会**因为目标端有唯一索引就自动升级 upsert（R1）。
- TARGET-02 与 PG-06 保持两条独立闸：前者问「目标端定位得了吗」，后者问「源端解得出旧行吗」。
- `unique_key` 不参与切块/续跑（R2）。

## 实现要点（踩过的地方）

- **MySQL 的 `ON DUPLICATE KEY UPDATE` 命中任意唯一索引**，声明与索引不一致时不会报错，只会静默按别的键更新 →
  写前必须自己核对（`verifyDeclaredKey`：`information_schema.STATISTICS` + `COLUMNS.IS_NULLABLE`），核对不过就停。
- **PG 的 `ON CONFLICT (…)` 没有匹配索引会直接报错**，但错误文案是 constraint 名，仍然要提前核对给四段式
  （`ensureDeclared`，版本门控 `indnkeyatts` 以排除 INCLUDE 列）。
- 声明生效时更新集必须排除声明列（否则 `ON CONFLICT DO UPDATE SET 键列=EXCLUDED.键列` 无意义/会改键）。
  声明覆盖全列 → `DO NOTHING`。
- 缓存：`declaredCache`（mysqlsink）/`declaredChecked`（pgsink）按 schema.table 缓存；mysqlsink 的 DDL 跟随
  与索引重建要清（`clearDeclaredCache`），否则加完唯一索引还按旧结论报错。
- **S2（#117）只把 `NoPKWriteError` 接到了 pgsink**：mysqlsink 的「无身份改/删」还是裸错误 → 整轮失败。
  S3 顺手补齐：`&pgsink.NoPKWriteError{...}`（engine 已有 `errors.As` 分支挂 E1017），MySQL 目标端现在
  也是「只停这张表 + skipped_rows 记账 + 位点前进」。验收：`TestMySQLTargetNoPKUpdatePausesOnlyThatTable`。
- 真库 e2e 夹具注意：目标表由用例自建时必须让**列类型按类型字典能对上**（`event_no text` → MySQL 是 `text`，
  但 `text` 不能整列建唯一索引 → 源端与目标端都用 `varchar(64)`），否则 `detectPrepareDrift` 会把这张表当
  「结构对不上」先暂停掉（报「登记暂停失败：任务状态行不存在」——因为首轮 `_jl_meta` 还没建）。
- `E1017` 码表条目（`core/codes.go` + `docs/ERRORS.md`）由 Ark 落；engine 里已有 `codeNoIdentity = "E1017"` 常量。

## 自检

`gofmt -l .` 空 / `go vet ./...` / `go test ./...` / `go vet -tags integration ./...` /
`python3 scripts/validate_schema.py examples/*.yaml` 全绿；PR CI 20/20 pass。
