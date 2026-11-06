#!/usr/bin/env bash
# 合并门禁：PR 的最新一条 review 必须是「针对当前 head 的明确放行」。
#
# 为什么需要它（真实事故）：Owl 的结论是异步到的，而且锚在某个 head 上；
# #53 就是带着三条必改合进了 main——人肉记不住「这条结论是对哪版代码说的」。
#
# 规则只有两条，任一不满足就失败：
#   1) 最新 review 的 commit_id == PR 当前 head（旧 head 的通过不算）；
#   2) 正文明确放行：含「必改 0」/「可合并」/「准予合并」/「放行」。
#
# 用法：scripts/check_review_gate.sh <pr-number>
# 环境：GH_TOKEN（Actions 默认提供）、GITHUB_REPOSITORY（可省略，本地用 gh repo view）
set -euo pipefail

PR="${1:-}"
if [ -z "$PR" ] || ! printf '%s' "$PR" | grep -qE '^[0-9]+$'; then
  echo "用法: scripts/check_review_gate.sh <pr-number>" >&2
  exit 2
fi
command -v gh >/dev/null || { echo "review-gate: 缺 gh CLI" >&2; exit 2; }
command -v python3 >/dev/null || { echo "review-gate: 缺 python3" >&2; exit 2; }

if [ -z "${GITHUB_REPOSITORY:-}" ]; then
  GITHUB_REPOSITORY="$(gh repo view --json nameWithOwner --jq .nameWithOwner)"
fi
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

gh api "repos/${GITHUB_REPOSITORY}/pulls/${PR}" >"$tmp/pr.json" || {
  echo "review-gate: 读不到 PR #${PR}（权限或编号不对）" >&2
  exit 1
}
# 一页 100 条足够（当前仓库远低于此）；按 submitted_at 取最新。
gh api "repos/${GITHUB_REPOSITORY}/pulls/${PR}/reviews?per_page=100" >"$tmp/reviews.json" || {
  echo "review-gate: 读不到 PR #${PR} 的 review" >&2
  exit 1
}

python3 - "$PR" "$tmp/pr.json" "$tmp/reviews.json" <<'PY'
import json
import re
import sys

pr_no, pr_path, reviews_path = sys.argv[1], sys.argv[2], sys.argv[3]
pr = json.load(open(pr_path, encoding="utf-8"))
reviews = json.load(open(reviews_path, encoding="utf-8"))
head = (pr.get("head") or {}).get("sha") or ""
if not head:
    print(f"review-gate: FAIL 读不到 PR #{pr_no} 的 head", file=sys.stderr)
    sys.exit(1)
if not reviews:
    print(f"review-gate: FAIL PR #{pr_no} 还没有 review（CI 绿不等于可合并）", file=sys.stderr)
    sys.exit(1)

# 最新一条：submitted_at 最大；同时间按 id 大者。
latest = max(reviews, key=lambda r: (r.get("submitted_at") or "", r.get("id") or 0))
body = latest.get("body") or ""
state = latest.get("state") or ""
commit = latest.get("commit_id") or ""
who = (latest.get("user") or {}).get("login") or "?"
summary = re.sub(r"\s+", " ", body.strip())[:80] or "（无正文）"

if state.upper() == "DISMISSED":
    print(f"review-gate: FAIL 最新 review 已被 dismissed（{who}，{summary}）", file=sys.stderr)
    sys.exit(1)

if commit != head:
    print(
        f"review-gate: FAIL 最新结论针对 {commit[:12] or '（空）'}，当前 head 是 {head[:12]}；"
        f"旧 head 的通过不算（reviewer={who}）",
        file=sys.stderr,
    )
    sys.exit(1)

approved = re.search(r"必改\s*0|可合并|准予合并|放行", body)
if not approved:
    print(
        f"review-gate: FAIL 最新结论不是放行（reviewer={who}，state={state}）：{summary}",
        file=sys.stderr,
    )
    sys.exit(1)

print(f"review-gate: PASS PR #{pr_no} head={head[:12]} reviewer={who}：{summary}")
PY
