#!/usr/bin/env python3
"""性能回归对比：当前 bench 报告 vs 仓库里的基线。

两种检查分开：
  1. 绝对阈值（20k 行/s、p99≤100ms、空转≤80MB、冷启≤200ms、<25MB）——每次都查；
  2. 回归对比（比基线差 >15%）——只在 CPU 型号一致时查；机器不同就只警告，
     因为跨机器比吞吐/延迟没有意义（DESIGN §7A.1 要求记录 CPU 做归一化）。

用法：scripts/bench_compare.py bench/baseline.json bench/latest.json
退出码：0 通过（或机器不同只警告），1 回归/超阈值。
"""
import json
import sys

TOLERANCE = 0.15
# (报告字段, 中文名, 方向)：max 表示越小越好，min 表示越大越好。
METRICS = [
    ("snapshot_rows_per_sec", "快照吞吐", "min"),
    ("p99_ms", "p99 延迟", "max"),
    ("idle_mb", "空转内存", "max"),
    ("cold_start_ms", "冷启", "max"),
    ("binary_mb", "二进制体积", "max"),
]
ABSOLUTE = {
    "snapshot_rows_per_sec": ("min", 20000.0),
    "p99_ms": ("max", 100.0),
    "idle_mb": ("max", 80.0),
    "cold_start_ms": ("max", 200.0),
    "binary_mb": ("max", 25.0),
}


def load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def main():
    if len(sys.argv) != 3:
        sys.exit("用法：bench_compare.py BASELINE CURRENT")
    base, cur = load(sys.argv[1]), load(sys.argv[2])
    failed = False

    print(f"基线: {sys.argv[1]}  ({base.get('cpu_model', '?')})")
    print(f"当前: {sys.argv[2]}  ({cur.get('cpu_model', '?')})")
    same_machine = (base.get("cpu_model") or "") == (cur.get("cpu_model") or "") and base.get("cpu_model")

    print("\n绝对阈值：")
    for key, name, _ in METRICS:
        mode, limit = ABSOLUTE[key]
        val = float(cur.get(key, 0))
        ok = val >= limit if mode == "min" else val <= limit
        print(f"  {name:<8} {val:>12.2f}  目标 {('≥' if mode == 'min' else '≤')} {limit:g}  {'PASS' if ok else 'FAIL'}")
        failed = failed or not ok
    print("\n回归对比（>15% 变差即红）：")
    if not same_machine:
        print("  WARN: CPU 型号不同，跳过回归对比（跨机器比吞吐/延迟没有意义；仍会查绝对阈值）")
    else:
        for key, name, mode in METRICS:
            b = float(base.get(key, 0))
            c = float(cur.get(key, 0))
            if b <= 0:
                continue
            worse = (b - c) / b if mode == "min" else (c - b) / b
            ok = worse <= TOLERANCE
            print(f"  {name:<8} 基线 {b:>10.2f} → 当前 {c:>10.2f}  变化 {worse*100:+.1f}%  {'PASS' if ok else 'FAIL'}")
            failed = failed or not ok

    if failed:
        print("\nbench_compare: FAIL", file=sys.stderr)
        return 1
    print("\nbench_compare: PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
