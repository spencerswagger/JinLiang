# E1017「三处同改」：与 PR #120 的重叠（2026-11-13）

## 事实

「三处同改」在我动手前**已经存在于 PR #120 的分支上**，是 `#120` 的最后一个提交：

```
c06a891 三处同改：E1017 的 Next 补 unique_key、ERRORS.md 同步、uniqueKeyConfigLanded 置 true
```

- `internal/core/codes.go`：E1017 的 `Next` 三条 → ① 加主键/唯一索引 ② `content.tables.<表>.unique_key`
  （写明前提：指向目标端**已有**的唯一索引、列都非空、源端已有主键时被忽略）③ `once`/`full_interval` 或移出 `content.tables`；
- `docs/ERRORS.md`：同一行同步，四段式一致；
- `internal/core/codes_noidentity_test.go`：`uniqueKeyConfigLanded = true`。

## 我做的事

1. 按任务在本地另做了一版（同样的三处）。合并 `origin/pr120` 时**两边都改了同一批行** → 冲突
   （`codes.go` 与 `codes_noidentity_test.go`）。
2. 于是改为**验证而不是重复提交**：在 `origin/pr120`（`c06a891`）的树上跑我 `#119` 里那 3 条守卫用例
   → `ok github.com/spencerswagger/jinLiang/internal/core`（`TestNoIdentityFourParts` /
   `TestNoIdentityNextOnlyNamesRealActions` / `TestErrorsDocE1017MatchesCatalogue` 全绿），
   即 `#120` 的版本满足守卫要求（含「声明那条必须写清目标端已有 + 列非空」这条新断言）。
3. 丢弃自己那版，避免两个 PR 改同一批行、白跑两轮 CI。

## 遗留（合并后必做，否则文档与代码打架）

`docs/DESIGN-NO-PRIMARY-KEY.md` §2.3 的「落地记录」是 `#119` 写的，原话是
「第三条 `unique_key` **暂缓**……S3 合入时必须一次性改三处」。`#120` 合入后这句就过期了
（`#120` 的分支早于 `#119`，它的树上根本没有这段），需要改成「已补回 / 三处同改完成」。
这一段**不能提前改**：main 上还没有 `#120` 的代码，提前写「已完成」就是假话。

## 收尾（`#120` 合入为 `a99337c` 之后）

- 上面那条遗留**已回填**：`docs/DESIGN-NO-PRIMARY-KEY.md` 的状态注、§1.1 现状表（按 `a99337c` 逐条刷新）、
  §2.3 落地记录、§4.5 收口状态、§7 新增 `D11`。`#120` 没碰这份设计文档，只写了 `notes/s3-unique-key.md`。
- 顺带核到一条**真问题**（记 `D11`）：`mysqlsink` 的 `ErrNoUniqueKey` 文案第 ② 条让用户去声明 `unique_key`，
  但该错误被触发的典型情形是「源端**有**主键、目标端缺键」（`applyUpserts` 的 `default` 分支）——
  那种表上 `declaredKeyFor` 恒返回 nil（§4.2 第 4 条：源端有主键时声明被忽略），照 ② 做不会好转。
  `TARGET-02` 的 warn 档写的是对的（「加与源端一致的主键，或换空 `target.schema`」），两处口径不一致。
  修法在 `internal/mysqlsink/apply.go`（Data 的树），本 PR 只记欠账不动代码。
- 码表措辞的两点建议（不阻塞）：② 补「列集合要和那条索引完全一致」（最常见的 blocked 原因）；
  「源端已有主键时这条会被忽略」在 E1017 的上下文里是噪音（E1017 只在没有**可用**主键时出现）。
