#!/usr/bin/env bash
# 合并门禁（advisory）：
#   review 模式：PR 最新 review 必须锚在当前 head 且明确「必改 0/可合并」。
#   admin  模式：合并若是在 base 已前进后发生的（疑似 --admin 绕过），
#                PR 上必须有 comment 说明理由。
#
# 为什么需要它（真实事故）：Owl 的结论异步到达、锚在旧 head，#53 就是带着必改
# 合进了 main；#55 又用 --admin 跳过「必须与 base 同步」。人肉记不住，机器记得住。
#
# 判定逻辑在 scripts/review_gate_logic.py（可 selftest）；本脚本只负责取数据。
#
# 用法：
#   scripts/check_review_gate.sh <pr-number>
#   scripts/check_review_gate.sh admin <pr-number>
#   scripts/check_review_gate.sh selftest
# 环境：GH_TOKEN（Actions 默认提供）、GITHUB_REPOSITORY（可省略）
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
LOGIC="$HERE/review_gate_logic.py"
MODE=review
if [ "${1:-}" = "admin" ]; then
  MODE=admin
  shift
fi
if [ "${1:-}" = "selftest" ]; then
  exec python3 "$LOGIC" selftest
fi
PR="${1:-}"
if [ -z "$PR" ] || ! printf '%s' "$PR" | grep -qE '^[0-9]+$'; then
  echo "用法: scripts/check_review_gate.sh [admin] <pr-number> | selftest" >&2
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
  # 两次 API 之间 head 可能前进：取完 reviews 再读一次，竞态直接失败。
  gh api "repos/${GITHUB_REPOSITORY}/pulls/${PR}" >"$tmp/pr2.json" || {
    echo "review-gate: 复读 head 失败" >&2
    exit 1
  }
  # 锚定判据：review commit 是 head 祖先，且 review..head 未触及本 PR 文件。
  HEAD_SHA="$(python3 - "$tmp/pr.json" <<'PY'
import json, sys
print((json.load(open(sys.argv[1])).get("head") or {}).get("sha") or "")
PY
)"
  REVIEW_SHA="$(python3 - "$tmp/reviews.json" <<'PY'
import json, sys
rs = json.load(open(sys.argv[1]))
if not rs:
    print("")
else:
    r = max(rs, key=lambda x: (x.get("submitted_at") or "", x.get("id") or 0))
    print(r.get("commit_id") or "")
PY
)"
  if [ -n "$REVIEW_SHA" ] && [ "$REVIEW_SHA" != "$HEAD_SHA" ]; then
    gh api "repos/${GITHUB_REPOSITORY}/compare/${REVIEW_SHA}...${HEAD_SHA}" >"$tmp/compare.json" || {
      echo "review-gate: 读不到 compare ${REVIEW_SHA:0:12}...${HEAD_SHA:0:12}" >&2
      exit 1
    }
    gh api --paginate "repos/${GITHUB_REPOSITORY}/pulls/${PR}/files?per_page=100" --jq '.[].filename' >"$tmp/prfiles.txt" || {
      echo "review-gate: 读不到 PR #${PR} 的文件清单" >&2
      exit 1
    }
    python3 - "$tmp/prfiles.txt" "$tmp/prfiles.json" <<'PY'
import json, sys
files = [l.rstrip("\n") for l in open(sys.argv[1], encoding="utf-8") if l.strip()]
open(sys.argv[2], "w", encoding="utf-8").write(json.dumps(files))
PY
  else
    echo '{}' >"$tmp/compare.json"
    echo '[]' >"$tmp/prfiles.json"
  fi
  python3 "$LOGIC" review "$tmp/pr.json" "$tmp/pr2.json" "$tmp/reviews.json" "$tmp/compare.json" "$tmp/prfiles.json"
  exit $?
fi

MERGE_SHA="$(python3 - "$tmp/pr.json" <<'PY'
import json, sys
print((json.load(open(sys.argv[1])).get("merge_commit_sha") or ""))
PY
)"
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
python3 "$LOGIC" admin "$tmp/pr.json" "$tmp/commit.json" "$tmp/comments.json"
