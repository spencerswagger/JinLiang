#!/usr/bin/env bash
# 合并门禁（#59 收口）：把「能拦住错误合并」的判据串成一条命令。
#
#   scripts/merge_gate.sh --pr 89                 # 合并前跑（含 review 锚定）
#   scripts/merge_gate.sh --db                    # 加跑「注入实现 → 用例必须红」（要真库）
#   scripts/merge_gate.sh --acceptance            # 加跑三组合验收（要两套库）
#   scripts/merge_gate.sh --all --pr 89           # 全量（CI 里就是这条）
#
# 四层判据（任何一层红 = 不放行）：
#   1. 判据自检      scripts/gate_checks.py selftest        —— 「判据本身」必须能红
#   2. review 自检   scripts/review_gate_logic.py selftest  —— #59 的锚定/否决词逻辑
#   3. 反向注入      scripts/mutation_gate.py --tier logic  —— 改坏判据 → 自检必须红
#   4. review 锚定   scripts/check_review_gate.sh <pr>      —— 真 PR 的结论必须锚在当前 head
#   --db 追加：注入实现（位点/暂停/块进度）→ 用例必须红
#   --acceptance 追加：MySQL→PG / PG→MySQL / MySQL→MySQL 三组合行数+md5+耗时
#
# 退出码：0 = 放行 / 1 = 拦截（判据不成立）/ 2 = 用法或环境缺失（同样不放行）
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
PR=""
DB=0
ACC=0
LOG="${MERGE_GATE_LOG:-$ROOT/.localdb/merge-gate.log}"
mkdir -p "$(dirname "$LOG")" 2>/dev/null || true

usage() { sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; }

while [ $# -gt 0 ]; do
  case "$1" in
    --pr) PR="${2:-}"; shift 2 ;;
    --db) DB=1; shift ;;
    --acceptance|--acc) ACC=1; shift ;;
    --all) DB=1; ACC=1; shift ;;
    -h|--help) usage; exit 2 ;;
    *) echo "merge-gate: 不认识的参数 $1" >&2; usage; exit 2 ;;
  esac
done
if [ -n "$PR" ] && ! printf '%s' "$PR" | grep -qE '^[0-9]+$'; then
  echo "merge-gate: --pr 要的是 PR 编号（数字），拿到的是 ${PR}" >&2; exit 2
fi

declare -a NAMES=() RESULTS=() DETAILS=()
step() { # $1=名称 $2=命令（用 bash -c 跑，输出进日志）
  local name="$1"; shift
  local out rc
  out="$( "$@" 2>&1 )"; rc=$?
  NAMES+=("$name")
  if [ $rc -eq 0 ]; then
    RESULTS+=("PASS"); DETAILS+=("$(printf '%s\n' "$out" | tail -1)")
  else
    RESULTS+=("FAIL"); DETAILS+=("rc=$rc $(printf '%s\n' "$out" | tail -3 | tr '\n' ' ' )")
  fi
  printf '\n== %s (rc=%s) ==\n%s\n' "$name" "$rc" "$out" >>"$LOG"
  return 0
}

echo "merge-gate: $(date -u +%FT%TZ) root=$ROOT pr=${PR:-（未指定，只做本地自检）}" | tee -a "$LOG"

step "gate_checks selftest" python3 "$ROOT/scripts/gate_checks.py" selftest
step "review_gate_logic selftest" python3 "$ROOT/scripts/review_gate_logic.py" selftest
step "mutation: logic 档" python3 "$ROOT/scripts/mutation_gate.py" --tier logic
if [ "$DB" = 1 ]; then
  step "mutation: engine 档（注入实现，用例必须红）" python3 "$ROOT/scripts/mutation_gate.py" --tier engine
fi
if [ "$ACC" = 1 ]; then
  step "acceptance: 三组合（行数+md5+耗时）" "$ROOT/scripts/acceptance_matrix.sh"
fi
if [ -n "$PR" ]; then
  step "review 锚定（PR #$PR）" "$ROOT/scripts/check_review_gate.sh" "$PR"
fi

echo
echo "| 判据 | 结果 | 摘要 |"
echo "|---|---|---|"
fail=0
for i in "${!NAMES[@]}"; do
  echo "| ${NAMES[$i]} | ${RESULTS[$i]} | ${DETAILS[$i]//|/\\|} |"
  [ "${RESULTS[$i]}" = "PASS" ] || fail=$((fail + 1))
done

if [ $fail -ne 0 ]; then
  echo
  echo "MERGE-GATE: FAIL（$fail/${#NAMES[@]} 层不通过）——不放行；完整输出 $LOG"
  exit 1
fi
echo
echo "MERGE-GATE: PASS（${#NAMES[@]} 层全绿）"
