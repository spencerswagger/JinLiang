#!/usr/bin/env bash
# 性能基线：五项实测（吞吐 / p99 / 空转内存 / 冷启 / 二进制体积）。
#
# 口径固定，换机器要重测并记录 CPU：
#   - 快照吞吐：走**产品路径** pgsink.BulkApply（临时落地区 → COPY → INSERT…SELECT
#     → 块记账 + 位点同事务），固定 10 万行 / 每块 5000；裸 COPY 只作为上限参考。
#   - 增量 p99：单连接 pgsink.Apply（数据 + 位点同事务），固定 10 万行 / 每批 500 /
#     2 批预热；p99 的样本数 = 批次数（200），不是单行延迟。
#   - 空转内存：serve 起来后、无任何请求时的 RSS（median of 3）。
#   - 冷启：进程 exec 到 /api/health 返回 200 的墙钟（median of 3）。
#   - 二进制：CGO_ENABLED=0 -trimpath -ldflags "-s -w" 的产物大小。
#
# 用法：
#   scripts/bench.sh                     # 报告模式：打印 PASS/FAIL，退出码 0
#   scripts/bench.sh --enforce           # 门禁模式：任一项不达标退出 1（release 用）
#   BENCH_ROWS=200000 scripts/bench.sh   # 换规模（会写进报告）
#
# 需要：JINLIANG_TEST_PG_DSN（真实 PostgreSQL 目标库）、curl、ps、python3。
set -euo pipefail

ENFORCE=0
OUT="${BENCH_OUT:-bench/latest.json}"
ROWS="${BENCH_ROWS:-100000}"
BATCH="${BENCH_BATCH:-500}"
WARMUP="${BENCH_WARMUP:-2}"

# 目标（docs/DESIGN.md §7A.1）；可用环境变量覆盖，但报告里会写实际阈值。
T_ROWS="${BENCH_MIN_ROWS_PER_SEC:-20000}"
T_P99="${BENCH_MAX_P99_MS:-100}"
T_IDLE="${BENCH_MAX_IDLE_MB:-80}"
T_COLD="${BENCH_MAX_COLD_MS:-200}"
T_SIZE="${BENCH_MAX_SIZE_MB:-25}"

while [ $# -gt 0 ]; do
  case "$1" in
    --enforce) ENFORCE=1 ;;
    --out) shift; OUT="$1" ;;
    --rows) shift; ROWS="$1" ;;
    --batch) shift; BATCH="$1" ;;
    -h|--help) sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "bench: 不认识的参数 $1" >&2; exit 2 ;;
  esac
  shift
done

cd "$(dirname "$0")/.."
# 本机沙盒的 Go 在 /.toolchain；换机器时不要写死，否则会静默用系统 go。
if [ -d "/.toolchain/go/bin" ]; then export PATH="/.toolchain/go/bin:${PATH}"; fi
command -v curl >/dev/null || { echo "bench: 缺 curl" >&2; exit 2; }
command -v python3 >/dev/null || { echo "bench: 缺 python3" >&2; exit 2; }
if [ -z "${JINLIANG_TEST_PG_DSN:-}" ]; then
  echo "bench: JINLIANG_TEST_PG_DSN 未设置；吞吐/p99 需要真实 PostgreSQL 目标库" >&2
  echo "       原因：这两项测的是真实写入路径，不能用 mock 代替。" >&2
  exit 2
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$(dirname "$OUT")"

echo "== 津梁性能基线 =="
echo "机器: $(nproc 2>/dev/null || echo '?') vCPU, $(go version), $(uname -srm)"
echo "规模: ${ROWS} 行 / 每批 ${BATCH} / 预热 ${WARMUP} 批"
echo

# ── 1) 二进制体积 ───────────────────────────────────────────────
mkdir -p "$tmp/dist"
CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o "$tmp/dist/jinliang" ./cmd/jinliang
SIZE_BYTES=$(wc -c <"$tmp/dist/jinliang")
SIZE_MB=$(python3 -c "print(f'{$SIZE_BYTES/1048576:.2f}')")

# ── 2) 冷启 + 空转内存（3 次取中位） ───────────────────────────
cold_ms=()
idle_mb=()
PORT_BASE=$((18000 + RANDOM % 500))
for i in 0 1 2; do
  dir="$tmp/data-$i"
  port=$((PORT_BASE + i))
  start_ns=$(date +%s%N)
  "$tmp/dist/jinliang" serve --listen "127.0.0.1:${port}" --data-dir "$dir" >"$tmp/serve-$i.log" 2>&1 &
  pid=$!
  ok=0
  for _ in $(seq 1 400); do
    if curl -fsS --max-time 1 "http://127.0.0.1:${port}/api/health" >/dev/null 2>&1; then ok=1; break; fi
    sleep 0.005
  done
  end_ns=$(date +%s%N)
  if [ "$ok" -ne 1 ]; then
    echo "bench: serve 没能在 2s 内就绪（第 $i 次）；日志：" >&2
    tail -20 "$tmp/serve-$i.log" >&2 || true
    kill "$pid" 2>/dev/null || true
    exit 2
  fi
  cold_ms+=($(( (end_ns - start_ns) / 1000000 )))
  rss_kb=$(ps -o rss= -p "$pid" | tr -d ' ')
  idle_mb+=($(python3 -c "print(f'{$rss_kb/1024:.1f}')"))
  kill -TERM "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
done
median() { printf '%s\n' "$@" | sort -n | sed -n '2p'; }
COLD_MS=$(median "${cold_ms[@]}")
IDLE_MB=$(median "${idle_mb[@]}")

# ── 3) 吞吐 + p99（真实 PG apply） ─────────────────────────────
apply_json="$tmp/apply.json"
# 3 次取中位：共享机器/磁盘抖动会让单次 p99 出现 180ms 的假尖峰；门禁看中位数，
# 原始三次数据仍在报告里（apply_runs）。
for i in 1 2 3; do
  go run ./scripts/benchapply --rows "$ROWS" --batch "$BATCH" --warmup "$WARMUP" \
    --json-out "$tmp/apply-$i.json" >/dev/null
done
python3 - "$tmp/apply-1.json" "$tmp/apply-2.json" "$tmp/apply-3.json" "$apply_json" <<'PY'
import json, statistics, sys
runs = [json.load(open(p)) for p in sys.argv[1:4]]
def med(k):
    return statistics.median(float(r[k]) for r in runs)
rep = dict(runs[0])
rep["rows_per_sec"] = med("rows_per_sec")
rep["p50_ms"] = med("p50_ms")
rep["p99_ms"] = med("p99_ms")
rep["raw_copy_ceiling_rows_per_sec"] = med("raw_copy_ceiling_rows_per_sec")
rep["apply_runs"] = [
    {k: r[k] for k in ("rows_per_sec", "p50_ms", "p99_ms", "raw_copy_ceiling_rows_per_sec")}
    for r in runs
]
rep["note"] += "；吞吐/p99 为 3 次中位数（原始三次见 apply_runs）"
open(sys.argv[4], "w").write(json.dumps(rep, ensure_ascii=False, indent=2) + "\n")
PY

# 快照：产品路径 BulkApply（不是裸 COPY）。同包集成用例里直接构造 tableInfo。
bulk_json="$tmp/bulk.json"
JINLIANG_BULK_BENCH_OUT="$bulk_json" JINLIANG_BULK_BENCH_ROWS="$ROWS" \
  go test -count=1 -tags=integration -run '^TestBulkApplyThroughput$' ./internal/pgsink >"$tmp/bulk.log" 2>&1 || {
    echo "bench: BulkApply 基线失败（这是产品路径，不能降级成裸 COPY 顶替）" >&2
    cat "$tmp/bulk.log" >&2
    exit 2
  }
SNAP_PER_SEC=$(python3 -c "import json;print(f'{json.load(open(\"$bulk_json\"))[\"rows_per_sec\"]:.0f}')")
BULK_P99=$(python3 -c "import json;print(f'{json.load(open(\"$bulk_json\"))[\"p99_ms\"]:.2f}')")
RAW_COPY=$(python3 -c "import json;print(f'{json.load(open(\"$apply_json\"))[\"raw_copy_ceiling_rows_per_sec\"]:.0f}')")
ROWS_PER_SEC=$(python3 -c "import json;print(f'{json.load(open(\"$apply_json\"))[\"rows_per_sec\"]:.0f}')")
P99_MS=$(python3 -c "import json;print(f'{json.load(open(\"$apply_json\"))[\"p99_ms\"]:.2f}')")
P50_MS=$(python3 -c "import json;print(f'{json.load(open(\"$apply_json\"))[\"p50_ms\"]:.2f}')")
CPU_MODEL=$(python3 -c "import json;print(json.load(open(\"$apply_json\")).get(\"cpu_model\",\"\"))")

# ── 4) 判定 ────────────────────────────────────────────────────
verdict() { # value target mode(min/max)
  python3 - "$1" "$2" "$3" <<'PY'
import sys
v, t, mode = float(sys.argv[1]), float(sys.argv[2]), sys.argv[3]
ok = v >= t if mode == "min" else v <= t
print("PASS" if ok else "FAIL")
PY
}
V_ROWS=$(verdict "$SNAP_PER_SEC" "$T_ROWS" min)
V_P99=$(verdict "$P99_MS" "$T_P99" max)
V_IDLE=$(verdict "$IDLE_MB" "$T_IDLE" max)
V_COLD=$(verdict "$COLD_MS" "$T_COLD" max)
V_SIZE=$(verdict "$SIZE_MB" "$T_SIZE" max)

printf '%-14s %14s %14s  %s\n' "指标" "实测" "目标" "结论"
printf '%-14s %14s %14s  %s\n' "吞吐(快照)" "${SNAP_PER_SEC} 行/s" "≥ ${T_ROWS}" "$V_ROWS"
printf '%-14s %14s %14s  %s\n' "p99 延迟" "${P99_MS} ms" "≤ ${T_P99} ms" "$V_P99"
printf '%-14s %14s %14s  %s\n' "空转内存" "${IDLE_MB} MB" "≤ ${T_IDLE} MB" "$V_IDLE"
printf '%-14s %14s %14s  %s\n' "冷启" "${COLD_MS} ms" "≤ ${T_COLD} ms" "$V_COLD"
printf '%-14s %14s %14s  %s\n' "二进制" "${SIZE_MB} MB" "< ${T_SIZE} MB" "$V_SIZE"
echo
echo "说明：快照=产品路径 pgsink.BulkApply（p99 ${BULK_P99} ms/块）；裸 COPY 上限 ${RAW_COPY} 行/s 仅参考；"
echo "      增量 apply ${ROWS_PER_SEC} 行/s（p50 ${P50_MS} ms，数据+位点同事务，p99 样本=200 批）；"
echo "      空转=ps 的 RSS（不是 Go heap）；冷启=exec→/api/health 返回 200；均不含源端解码与网络。"

python3 - "$OUT" <<PY
import json, os
rep = {
  "metric": "b6-baseline",
  "rows": $ROWS,
  "batch_rows": $BATCH,
  "warmup_batches": $WARMUP,
  "snapshot_rows_per_sec": float("$SNAP_PER_SEC"),
  "bulk_apply_rows_per_sec": float("$SNAP_PER_SEC"),
  "bulk_apply_p99_ms": float("$BULK_P99"),
  "raw_copy_ceiling_rows_per_sec": float("$RAW_COPY"),
  "rows_per_sec": float("$ROWS_PER_SEC"),
  "apply_runs": json.load(open("$apply_json")).get("apply_runs", []),
  "p50_ms": float("$P50_MS"),
  "p99_ms": float("$P99_MS"),
  "idle_mb": float("$IDLE_MB"),
  "cold_start_ms": $COLD_MS,
  "binary_mb": float("$SIZE_MB"),
  "binary_bytes": $SIZE_BYTES,
  "cpu_model": """$CPU_MODEL""",
  "num_cpu": int(os.cpu_count() or 0),
  "thresholds": {
    "rows_per_sec_min": $T_ROWS, "p99_ms_max": $T_P99,
    "idle_mb_max": $T_IDLE, "cold_start_ms_max": $T_COLD, "binary_mb_max": $T_SIZE,
  },
  "verdict": {
    "snapshot_rows_per_sec": "$V_ROWS", "bulk_apply_rows_per_sec": "$V_ROWS", "p99_ms": "$V_P99", "idle_mb": "$V_IDLE",
    "cold_start_ms": "$V_COLD", "binary_mb": "$V_SIZE",
  },
}
os.makedirs(os.path.dirname("$OUT") or ".", exist_ok=True)
open("$OUT", "w").write(json.dumps(rep, ensure_ascii=False, indent=2) + "\n")
print(f"报告已写入 $OUT")
PY

if [ "$ENFORCE" -eq 1 ]; then
  if printf '%s\n' "$V_ROWS" "$V_P99" "$V_IDLE" "$V_COLD" "$V_SIZE" | grep -q FAIL; then
    echo "bench: 有指标未达标（--enforce）" >&2
    exit 1
  fi
fi
