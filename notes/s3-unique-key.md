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

## Owl 第 2 轮（PR #120 复核后）

- P2-1：`once`/`full_interval` 下声明**不生效**——engine 不再把声明交给 sink，预检把声明归到
  「被忽略」提示（含目标表还不存在的情况，不再 blocked）。
- P2-2：目标端主键与源端不同组、但另有一条与源端主键同列的唯一索引 → 预检判 ok（真库实测能 upsert）。
- P2-3：`columns.include/exclude/map` 目前只有校验、还没实现过滤，所以「声明被忽略」的文案
  写成「列过滤能力落地后…才会生效」，不承诺今天做不到的事；`hasUsablePK` 的列判据今天恒真，与
  pgsink 保持同形。
- `checkDeclared` 的去重 key 改成 `schema.table`（与 `declaredCache` 同口径）。

## Owl 第 3 轮（cfb5c74 复核）

- 必改 1（假绿）：`srcTable` 把源端主键写死 `["id"]`，撞上目标 `PK(id)` 走了前面那句
  `sameColumnSet` 早退 → 新分支一次没进。拆出 `srcTablePK(name, hasPK, pk...)`，那条用例
  现在源端主键是 `tenant_id,event_no`（变异验证：删掉新分支会红）。
- 必改 2：新分支把 `uniqueKeyMatches` 的 `why` 丢了——「键存在但含可空列」被说成「不存在/42P10」。
  现在按 `why` 分流：可空列单独一档 warn（真实风险＝NULL 互不相等 → 重放插重复）。
- P2-1（口径分叉）：拆成 `declaredUniqueKeys()`（预检，raw）与 `sinkDeclaredUniqueKeys()`
  （写路径，once/full_interval 返回 nil），`engine.go` 的预检吃 raw、两个 sink 吃过滤后的；
  用例 `TestDeclaredUniqueKeysSplitByMode` 钉住。
- P2-2（engine 那半零用例）：同上那个用例覆盖两种整轮重搬模式。
