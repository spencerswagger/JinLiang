#!/usr/bin/env bash
# 三组合一致性矩阵（**一条命令复跑**）：
#   MySQL→MySQL / MySQL→PG / PG→MySQL，每格一行：
#   目标表名 / 源行数 / 目标行数 / 源 md5 / 目标 md5 / 快照段秒数 / 增量段秒数。
#
#   scripts/matrix3.sh                  # 20000 行，输出落 notes/matrix3/<日期>/
#   ROWS=100000 scripts/matrix3.sh      # 换行数（三格同时换，才比得动）
#   OUT=notes/matrix3/manual scripts/matrix3.sh
#
# 它做四件事：
#   ①起本地栈（PG 17.11 + 两个 MySQL 8.0.28；已在跑就复用，数据目录在工作区 .localdb）
#   ②编二进制（CGO_ENABLED=0，与 CI 一致）
#   ③按组合**串行**跑 scripts/acceptance_matrix.sh：每格的 stdout/stderr 全落
#     notes/matrix3/<日期>/<组合>/console.log，引擎日志（snapshot/incremental/verify）同目录
#   ④把三格结果拼成 matrix.md（交付的那张表）
#
# 为什么串行：三格共用同一个 PG 实例和同一个二进制，并发会互抢 _jl_lease 租约，
# 而且耗时没法归因。
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
STAMP=${STAMP:-$(date +%Y%m%d)}
OUT=${OUT:-$ROOT/notes/matrix3/$STAMP}
ROWS=${ROWS:-20000}
COMBOS=${COMBOS:-mysql-mysql mysql-postgres postgres-mysql}

mkdir -p "$OUT"
log() { printf '[matrix3] %s\n' "$*" >&2; }

# ── ① 本地栈 ──────────────────────────────────────────────────
if ! "$ROOT/scripts/local_stack.sh" status 2>/dev/null | grep -q "在跑"; then
  log "本地栈没跑，起（PG 17 + MySQL 8.0.28 ×2，第一次初始化约 20s）"
  "$ROOT/scripts/local_stack.sh" up || { log "本地栈起不来"; exit 78; }
else
  log "本地栈已在跑，复用（数据目录 .localdb/）"
fi
# shellcheck disable=SC1091
. "$ROOT/.localdb/stack.env"

# ── ② 二进制 ──────────────────────────────────────────────────
mkdir -p "$ROOT/.localdb/bin"
log "编二进制 → .localdb/bin/jinliang"
( cd "$ROOT" && CGO_ENABLED=0 go build -trimpath -o .localdb/bin/jinliang ./cmd/jinliang ) \
  || { log "go build 失败"; exit 1; }

# ── ③ 逐组合跑 ────────────────────────────────────────────────
FAILED=0
for combo in $COMBOS; do
  mkdir -p "$OUT/$combo"
  log "===== $combo（${ROWS} 行）====="
  bash "$ROOT/scripts/acceptance_matrix.sh" --only "$combo" --rows "$ROWS" \
       --logdir "$OUT" --json "$OUT/$combo/result.json" 2>&1 | tee "$OUT/$combo/console.log"
  rc=${PIPESTATUS[0]}
  echo "$rc" > "$OUT/$combo/exit_code"
  [ "$rc" -eq 0 ] || { log "$combo 失败（rc=$rc），日志 $OUT/$combo/console.log"; FAILED=$((FAILED+1)); }
done

# ── ④ 汇总成矩阵 ──────────────────────────────────────────────
MYVER=$("$ROOT/.localdb/bin/jinliang" version 2>/dev/null | head -1)
PGVER=$(psql "$JINLIANG_TEST_PG_DSN" -tAc 'show server_version' 2>/dev/null)
MYSQLVER=$(mysql --protocol=TCP -h 127.0.0.1 -P 53316 -ujinliang -pjinliang -N -e 'select version()' 2>/dev/null)

python3 - "$OUT" "$ROWS" "${PGVER:-?}" "${MYSQLVER:-?}" <<'PY'
import json, os, sys
out, rows, pgver, myver = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
order = ["mysql-mysql", "mysql-postgres", "postgres-mysql"]
label = {"mysql-mysql": "MySQL → MySQL", "mysql-postgres": "MySQL → PG",
         "postgres-mysql": "PG → MySQL"}
rows_out = []
for c in order:
    p = os.path.join(out, c, "result.json")
    if not os.path.exists(p):
        rows_out.append((label[c], None)); continue
    try:
        data = json.load(open(p, encoding="utf-8"))
    except Exception:
        data = []
    rec = {r.get("combo"): r for r in data if r.get("combo")}
    rows_out.append((label[c], rec.get(c)))

lines = []
lines.append("# 三组合一致性矩阵（快照 / 增量分段）\n")
lines.append(f"- 生成：`scripts/matrix3.sh`（行数 ROWS={rows}，PG {pgver}，MySQL {myver}）")
lines.append(f"- 原始输出：`{os.path.relpath(out)}/<组合>/console.log` + `snapshot.log` / `incremental.log` / `verify.log` / `result.json`")
lines.append("- 复跑：`scripts/matrix3.sh`\n")
lines.append("| 组合 | 目标表名 | 源行数 | 目标行数 | 源 md5 | 目标 md5 | 快照(s) | 增量(s) | 结论 |")
lines.append("|---|---|---|---|---|---|---|---|---|")
for name, r in rows_out:
    if not r:
        lines.append(f"| {name} | - | - | - | - | - | - | - | **未跑/失败** |")
    elif r.get("status") == "pass":
        same = "一致" if r.get("src_md5") == r.get("dst_md5") and r.get("src_rows") == r.get("dst_rows") else "**不一致**"
        lines.append("| {n} | `{t}` | {sr} | {dr} | `{sm}` | `{dm}` | {ss} | {is_} | pass（{same}） |".format(
            n=name, t=r.get("target", "?"), sr=r.get("src_rows", "?"), dr=r.get("dst_rows", "?"),
            sm=r.get("src_md5", "?"), dm=r.get("dst_md5", "?"),
            ss=r.get("snapshot_seconds", "?"), is_=r.get("incremental_seconds", "?"), same=same))
    else:
        lines.append(f"| {name} | - | - | - | - | - | - | - | **FAIL** {r.get('rc','')} |")
md = "\n".join(lines) + "\n"
open(os.path.join(out, "matrix.md"), "w", encoding="utf-8").write(md)
print(md)
PY

log "矩阵写入 $OUT/matrix.md"
[ "$FAILED" -eq 0 ] || { log "$FAILED 格失败"; exit 1; }
log "全部通过"
