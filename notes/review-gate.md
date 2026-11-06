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
