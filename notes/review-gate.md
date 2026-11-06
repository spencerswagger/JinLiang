# review-gate：用机器挡住「旧结论盖新代码」

## 是什么

`scripts/check_review_gate.sh <pr>` 只认两件事：

1. 最新一条 review 的 `commit_id` == PR **当前 head**（旧 head 的通过不算）；
2. 正文明确放行：`必改 0` / `可合并` / `准予合并` / `放行`。

任一不满足 → 退出 1，并打印「是 head 不匹配，还是最新结论不是放行」。

GHA `review-gate.yml` 在 `pull_request`（synchronize/reopened/edited）与
`pull_request_review`（submitted/edited/dismissed）上跑；**现在是 advisory**
（失败亮红但不挡合并），稳定一周后转 required。

## 三种真实输入的实跑输出

### ① 最新 review 在当前 head 且放行 → PASS（PR #10）

```
$ scripts/check_review_gate.sh 10
review-gate: PASS PR #10 head=7dfa977281e5 reviewer=spencerswagger：**可合并（必改 0）**，按 head `7dfa977` 复核：…
exit=0
```

### ② 最新 review 在旧 head → FAIL（PR #55）

```
$ scripts/check_review_gate.sh 55
review-gate: FAIL 最新结论针对 205cdcdcfb3a，当前 head 是 88aca4d5b6b2；旧 head 的通过不算（reviewer=spencerswagger）
exit=1
```

### ③ 最新 review 是「必改 N 条」→ FAIL（PR #54）

```
$ scripts/check_review_gate.sh 54
review-gate: FAIL 最新结论不是放行（reviewer=spencerswagger，state=COMMENTED）：**必改 2 条**（head `1b256b9`）——…
exit=1
```

## 设计取舍

- **只取最新一条**：旧结论即使写着「可合并」也不算；这正是 #53 事故的根因。
- **head 用完整 SHA 比对**：review 的 `commit_id` 与 PR `head.sha` 都是完整 SHA，不做前缀匹配。
- **放行词是白名单**：`必改 0`/`可合并`/`准予合并`/`放行`；「必改 2 条」不会误命中，
  「未发现必改 0」这类反话有风险，但 Owl 的模板是固定开头（必改 N 条 / 可合并），可接受。
- **advisory 起步**：直接 required 会把「Owl 还没看的正常 PR」全锁死；先观察一周。

## 绕过

需要管理员操作；必须在 PR 里写明原因与负责人（事后追认），不允许作者自己点掉检查。

## 追加：`--admin` 绕过必须留痕

`scripts/check_review_gate.sh admin <pr>`（GHA 在 `pull_request: closed` 合并后跑）：

- 正常合并：合并提交第一父提交 == PR `base.sha` → PASS；
- 疑似 `--admin`：base 已前进（如 #55：base `de9ddb3`，合并 parent `06ae9a8`，
  merge `fd62b2e`）→ PR 上必须有含 `--admin` 与原因的 comment，否则 FAIL。

实跑（真实数据）：

```
$ scripts/check_review_gate.sh admin 55
review-gate: FAIL PR #55 合并时 base 已前进（base de9ddb362465，合并 parent 06ae9a8fd96f，
merge fd62b2e8d7cf，by=spencerswagger）——疑似 --admin 绕过；PR 上没有说明理由的 comment
exit=1

$ scripts/check_review_gate.sh admin 53
review-gate: PASS PR #53 正常合并（base 与合并 parent 一致，by=spencerswagger）
exit=0
```

advisory 起步，与 review-gate 同一个 workflow；转 required 时把 `admin-merge-audit (advisory)`
的 `continue-on-error` 去掉并加进 branch protection。

## 第二轮必改：门禁自己不误放行

旧实现两个洞：`不可合并` 含「可合并」会 MATCH；`CHANGES_REQUESTED` 没特判。
现在（`scripts/review_gate_logic.py`，可 `scripts/check_review_gate.sh selftest`）：

- `state in (DISMISSED, CHANGES_REQUESTED)` → 硬否决，不看正文；
- 先否决词：`不可合并|不予放行|不通过|必须修|必改\s*[1-9]` 命中即 FAIL；
- 放行必须在行首结论行：`(?m)^[*_>#\-\s]*(结论[:：]\s*)?... (可合并|必改 0|...)`；
- 取完 reviews 再读一次 head，两次不一致 → FAIL（head 竞态）。

selftest 覆盖 8 个输入（`不可合并` / `不予放行` / `上一轮必改 0，本轮必改 2 条` /
`必改 2 条` / `CHANGES_REQUESTED` + 可合并 / 旧 head / 正常放行）全部按预期。

三种真 PR 复测：

```
# ① 当前 head + 放行
$ scripts/check_review_gate.sh 10        → PASS，exit 0
# ② 旧 head + 放行（#44：review 46288d8，head 0ffb544）
$ scripts/check_review_gate.sh 44        → FAIL「结论针对 46288d8，当前 head 0ffb544」，exit 1
# ③ 当前/旧 head + 必改 2 条（#55）
$ scripts/check_review_gate.sh 55        → FAIL「正文含否决词…必改 2 条」，exit 1
```

advisory→required 顺序：job 名保持 `review-gate` / `admin-merge-audit` 不变，
先删 `continue-on-error`，再把这两个已有 check 加进 required；不要同时改名。

## 锚定判据修订（strict 死锁）

旧规则「review.commit == head」在 `strict: true` 下必然死锁：合并前要 merge main → head 前进 →
锚点失效 → 再等 review。现在改为两条同时满足：

1. `review.commit` 是当前 head 的**祖先**（GitHub compare status=`ahead`；rebase/force push 后
   `diverged` 直接拒）；
2. `review..head` 的差异**不包含本 PR 改的文件**（只允许别人已合的改动/merge main 进来）。

任何本 PR 的新改动（哪怕一行注释）仍然要求重审。失败信息分三种：
「结论早于本 PR 的新改动」「结论不是放行/是否决词」「结论不是 head 祖先」。

真 PR 复测：

```
# ① 当前 head + 放行
$ scripts/check_review_gate.sh 10   → PASS
# ② review 旧 + 仅 merge main（#31：review c63dee1，head ed0c5f1，diff 27 文件均非本 PR）
$ scripts/check_review_gate.sh 31   → PASS（head 前进仅来自 merge main / 别人已合的改动，未触及本 PR 文件）
# ③ review 旧 + 本 PR 文件又改了（#44）
$ scripts/check_review_gate.sh 44   → FAIL「结论早于本 PR 的新改动…涉及 internal/engine/engine.go」
# ④ 必改 2 条（#55）
$ scripts/check_review_gate.sh 55   → FAIL「首行结论含否决词…必改 2 条」
```
