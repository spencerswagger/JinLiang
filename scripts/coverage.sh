#!/usr/bin/env bash
# 覆盖度报表：包级数字 + 总语句覆盖，可写回 README。
#
# 口径（重要，别只看总百分比）：
#   - 这里的数字来自 `go test ./...`（**单元测试**）；
#   - engine / pgsource / pgsink / mysqlsource / runtime / remedy 的真实覆盖
#     要靠 `go test -tags=integration`（需要真库，见 nightly），所以它们的单元
#     数字偏低是正常的，不代表没测；
#   - sink / testkit / transform 目前只有契约或文档，没有可执行测试。
#
# 用法：
#   scripts/coverage.sh                  # 打印报表
#   scripts/coverage.sh --write-readme   # 写回 README 的 coverage 标记之间
#   scripts/coverage.sh --check --min 40 # 总覆盖 < 40% 退出 1（release 门禁）
set -euo pipefail

CHECK=0
MIN=40
WRITE=0
while [ $# -gt 0 ]; do
  case "$1" in
    --check) CHECK=1 ;;
    --min) shift; MIN="$1" ;;
    --write-readme) WRITE=1 ;;
    -h|--help) sed -n '2,18p' "$0"; exit 0 ;;
    *) echo "coverage: 不认识的参数 $1" >&2; exit 2 ;;
  esac
  shift
done

cd "$(dirname "$0")/.."
export PATH="/.toolchain/go/bin:${PATH}"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

raw="$tmp/cover.raw"
go test -coverprofile="$tmp/cover.out" ./... >"$raw" 2>&1 || {
  cat "$raw" >&2
  echo "coverage: go test 失败" >&2
  exit 1
}
go tool cover -func="$tmp/cover.out" >"$tmp/func.txt"

python3 - "$raw" "$tmp/func.txt" "$tmp/table.md" <<'PY'
import sys
raw, func, out = sys.argv[1], sys.argv[2], sys.argv[3]
integration_only = {"engine", "pgsource", "pgsink", "mysqlsource", "runtime", "remedy", "pgwire"}
rows = []
for line in open(raw, encoding="utf-8", errors="replace"):
    if not line.startswith("ok  ") or "coverage:" not in line:
        continue
    parts = line.split()
    pkg = parts[1]
    try:
        pct = float(line.split("coverage:")[1].split("%")[0].strip())
    except (IndexError, ValueError):
        continue
    short = pkg.replace("github.com/spencerswagger/jinliang/", "")
    note = "单元"
    tail = short.rsplit("/", 1)[-1]
    if tail in integration_only:
        note = "单元 + 集成（nightly）"
    rows.append((short, pct, note))

total = None
for line in open(func, encoding="utf-8"):
    if line.startswith("total:"):
        total = line.split()[-1].rstrip("%")
if total is None:
    sys.exit("coverage: 读不到总覆盖率")

with open(out, "w", encoding="utf-8") as f:
    f.write("| 包 | 语句覆盖 | 口径 |\n|---|---:|---|\n")
    for short, pct, note in rows:
        f.write(f"| `{short}` | {pct:.1f}% | {note} |\n")
    f.write(f"\n**总语句覆盖：{total}%**（单元测试；集成覆盖见 nightly 产物）\n")
print(total)
PY
total=$(tail -1 "$tmp/table.md" | sed -n 's/.*覆盖：\([0-9.]*\)%.*/\1/p')
[ -n "$total" ] || { echo "coverage: 解析总覆盖失败" >&2; exit 1; }

if [ "$WRITE" -eq 1 ]; then
  python3 - "$tmp/table.md" <<'PY'
import sys
table = open(sys.argv[1], encoding="utf-8").read().rstrip()
p = "README.md"
s = open(p, encoding="utf-8").read()
start = "<!-- coverage:start -->"
end = "<!-- coverage:end -->"
if start not in s or end not in s:
    sys.exit("README 缺少 coverage 标记")
s = s.split(start)[0] + start + "\n" + table + "\n" + end + s.split(end, 1)[1]
open(p, "w", encoding="utf-8").write(s)
print("README 覆盖度已更新")
PY
else
  cat "$tmp/table.md"
fi

if [ "$CHECK" -eq 1 ]; then
  python3 -c "import sys;t=float('$total');m=float('$MIN');sys.exit(0 if t>=m else 1)" || {
    echo "coverage: 总覆盖 ${total}% < 门槛 ${MIN}%" >&2
    exit 1
  }
  echo "coverage: 总覆盖 ${total}% ≥ 门槛 ${MIN}%"
fi
