#!/usr/bin/env bash
# 合并门禁（advisory）：
#   review 模式：PR 最新 review 必须锚在当前 head 且明确「必改 0/可合并」。
#   admin  模式：合并若是在 base 已前进后发生的（疑似 --admin 绕过），
#                PR 上必须有 comment 说明理由。
#
# 为什么需要它（真实事故）：Owl 的结论异步到达、锚在旧 head，#53 就是带着必改
# 合进了 main；#55 又用 --admin 跳过「必须与 base 同步」。人肉记不住，机器记得住。
#
# 用法：
#   scripts/check_review_gate.sh <pr-number>
#   scripts/check_review_gate.sh admin <pr-number>
# 环境：GH_TOKEN（Actions 默认提供）、GITHUB_REPOSITORY（可省略）
set -euo pipefail

MODE=review
if [ "${1:-}" = "admin" ]; then
  MODE=admin
  shift
fi
PR="${1:-}"
if [ -z "$PR" ] || ! printf '%s' "$PR" | grep -qE '^[0-9]+$'; then
  echo "用法: scripts/check_review_gate.sh [admin] <pr-number>" >&2
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

if [ "$MODE" = "review" ]; then
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
if not re.search(r"必改\s*0|可合并|准予合并|放行", body):
    print(
        f"review-gate: FAIL 最新结论不是放行（reviewer={who}，state={state}）：{summary}",
        file=sys.stderr,
    )
    sys.exit(1)
print(f"review-gate: PASS PR #{pr_no} head={head[:12]} reviewer={who}：{summary}")
PY
  exit $?
fi

# ── admin 模式：合并时 base 已前进 = 疑似 --admin 绕过，必须有说明 ──
MERGE_SHA="$(python3 -c 'import json,sys;print((json.load(open(sys.argv[1])).get("merge_commit_sha") or ""))' "$tmp/pr.json")"
if [ -z "$MERGE_SHA" ]; then
  echo "review-gate: PASS PR #${PR} 还没合并（admin 检查跳过）"
  exit 0
fi
gh api "repos/${GITHUB_REPOSITORY}/git/commits/${MERGE_SHA}" >"$tmp/commit.json" || {
  echo "review-gate: 读不到合并提交 ${MERGE_SHA}" >&2
  exit 1
}
gh api "repos/${GITHUB_REPOSITORY}/issues/${PR}/comments?per_page=100" >"$tmp/comments.json" || {
  echo "review-gate: 读不到 PR #${PR} 的 comment" >&2
  exit 1
}
python3 - "$PR" "$tmp/pr.json" "$tmp/commit.json" "$tmp/comments.json" <<'PY'
import json
import re
import sys

pr_no, pr_path, commit_path, comments_path = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
pr = json.load(open(pr_path, encoding="utf-8"))
commit = json.load(open(commit_path, encoding="utf-8"))
comments = json.load(open(comments_path, encoding="utf-8"))
base = (pr.get("base") or {}).get("sha") or ""
parents = [p.get("sha") or "" for p in (commit.get("parents") or [])]
merge_sha = pr.get("merge_commit_sha") or ""
who = (pr.get("merged_by") or {}).get("login") or "?"

if not parents:
    print(f"review-gate: WARN PR #{pr_no} 合并提交没有 parent，无法判断是否 --admin", file=sys.stderr)
    sys.exit(0)

# squash/merge 的第一父提交就是合并时的 base tip。
if parents[0] == base:
    print(f"review-gate: PASS PR #{pr_no} 正常合并（base 与合并 parent 一致，by={who}）")
    sys.exit(0)

# base 已前进：强制要求 PR 上有一条说明理由的 comment。
reason = None
pattern = re.compile(r"--admin|admin\s*合并|管理员绕过|bypass", re.IGNORECASE)
for c in comments:
    body = c.get("body") or ""
    if pattern.search(body):
        reason = re.sub(r"\s+", " ", body.strip())[:100]
        author = (c.get("user") or {}).get("login") or "?"
        break
if reason:
    print(f"review-gate: PASS PR #{pr_no} 疑似 --admin 合并，但已有说明（{author}）：{reason}")
    sys.exit(0)

print(
    f"review-gate: FAIL PR #{pr_no} 合并时 base 已前进（base {base[:12]}，合并 parent {parents[0][:12]}，"
    f"merge {merge_sha[:12]}，by={who}）——疑似 --admin 绕过；PR 上没有说明理由的 comment（需含 --admin 与原因）",
    file=sys.stderr,
)
sys.exit(1)
PY
