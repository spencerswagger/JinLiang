#!/usr/bin/env python3
"""无主键变体矩阵的逐格执行器（docs/DESIGN-NO-PRIMARY-KEY.md §2.4 / §5 S4）。

格的定义在 `test/nopk-matrix.yaml`（1 格 = 1 条链路 × 1 个场景的一次可执行用例）。
本脚本只做三件事：**展开 → 选格 → 逐格调起 `TestNoPKMatrixCell`**，并把每格结果
打成一行 `RESULT nopk …`（nightly/release 报告靠这个前缀单独筛出 no-pk 变体）。

用法（CI 与本地同一份）：

    scripts/nopk_matrix.py --list                       # 看全部格（含 pending / 本周 due）
    scripts/nopk_matrix.py --layer pr                   # PR 门禁的 10 格
    scripts/nopk_matrix.py --layer nightly --if-due     # nightly：固定格 + 本周该跑的 T4
    scripts/nopk_matrix.py --layer release              # release：184 格
    scripts/nopk_matrix.py --layer pr --link pg-pg      # 只跑某条链路
    scripts/nopk_matrix.py --layer nightly --if-due --link pg-pg \
        --src-version 16 --dst-version 16               # 某个 job 里只跑它那几格

环境（缺了就是 78 = 红，不静默跳过；与 scripts/acceptance_matrix.sh 同口径）：
    pg-pg      JINLIANG_TEST_PG_SRC_DSN  + JINLIANG_TEST_PG_DSN
    mysql-pg   JINLIANG_TEST_MYSQL_SRC_DSN + JINLIANG_TEST_PG_DSN
    pg-mysql   JINLIANG_TEST_PG_SRC_DSN  + JINLIANG_TEST_MYSQL_DSN
    mysql-mysql JINLIANG_TEST_MYSQL_SRC_DSN + JINLIANG_TEST_MYSQL_DST_DSN（**pending，不跑**）

「待接通」12 格（MySQL→MySQL）永远是 `status=pending`，**永远不会算进 pass**；
只有显式 `--include-pending` 才会去跑它们——那时用例会以「方向未接通」失败，
这正是「不许把待接通当已覆盖」的机械证明。
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover - CI installs pyyaml
    sys.exit("需要 pyyaml：pip install pyyaml")

ROOT = Path(__file__).resolve().parent.parent
REGISTRY = ROOT / "test" / "nopk-matrix.yaml"

# 链路 = 源 kind → 目标 kind；DSN 变量按链路给（与仓库既有集成测试一致）。
LINKS = {
    ("postgres", "postgres"): ("pg-pg", ["JINLIANG_TEST_PG_SRC_DSN", "JINLIANG_TEST_PG_DSN"]),
    ("mysql", "postgres"): ("mysql-pg", ["JINLIANG_TEST_MYSQL_SRC_DSN", "JINLIANG_TEST_PG_DSN"]),
    ("postgres", "mysql"): ("pg-mysql", ["JINLIANG_TEST_PG_SRC_DSN", "JINLIANG_TEST_MYSQL_DSN"]),
    ("mysql", "mysql"): ("mysql-mysql", ["JINLIANG_TEST_MYSQL_SRC_DSN", "JINLIANG_TEST_MYSQL_DST_DSN"]),
}

# 只在这些方向上「增量/改删」可达（direction.go 的 Modes）：PG→MySQL 只全量。
FULL_ONLY_LINKS = {"pg-mysql"}


def link_of(block: dict) -> tuple[str, list[str]]:
    key = (block["source"], block["target"])
    if key not in LINKS:
        raise SystemExit(f"nopk-matrix: 不认识的链路 {key}（块 {block.get('name')}）")
    return LINKS[key]


def iso_week() -> int:
    return dt.date.today().isocalendar()[1]


def due_pairs(block: dict, week: int) -> list[list[str]]:
    """weekly 块按周轮转：每周取 pick 条链（确定性，按 ISO 周序取模）。"""
    pairs = block["pairs"]
    pick = int(block.get("pick", len(pairs)))
    if pick >= len(pairs):
        return pairs
    start = (week * pick) % len(pairs)
    return [pairs[(start + i) % len(pairs)] for i in range(pick)]


def expand(reg: dict, week: int, if_due: bool) -> list[dict]:
    """把块展开成格。weekly 块在 if_due 下只留本周该跑的那几条链。"""
    blocks = reg["blocks"]
    cells: list[dict] = []
    for layer, spec in reg["layers"].items():
        for bname in spec["blocks"]:
            block = dict(blocks[bname], name=bname)
            weekly = block.get("schedule") == "weekly"
            # 展开**整张轮转表**（这样 --list 能看见全量），due 只决定「本周跑不跑」。
            due = due_pairs(block, week) if weekly else block["pairs"]
            pairs = block["pairs"]
            for src_v, dst_v in pairs:
                for scenario in block["scenarios"]:
                    link, _ = link_of(block)
                    cells.append({
                        "layer": layer,
                        "block": bname,
                        "link": link,
                        "source": block["source"],
                        "target": block["target"],
                        "src": str(src_v),
                        "dst": str(dst_v),
                        "scenario": scenario,
                        "status": block.get("status", "active"),
                        "reason": block.get("reason", ""),
                        "weekly": weekly,
                        "due": (not weekly) or ([src_v, dst_v] in due),
                    })
    # 格 id 里带层/块/链路/版本/场景：报告里能一眼定位。
    for c in cells:
        c["id"] = f"{c['layer']}/{c['block']}/{c['link']}/{c['src']}->{c['dst']}/{c['scenario']}"
    return cells


def version_matches(label: str, filt: str) -> bool:
    """标签匹配：筛 "9.4"（job 的版本档）要能命中标签 "9"（定稿的「9.x」）。"""
    return filt == label or filt.startswith(label + ".")


def select(cells: list[dict], args) -> list[dict]:
    out = []
    for c in cells:
        if args.layer and c["layer"] not in args.layer:
            continue
        if args.block and c["block"] not in args.block:
            continue
        if args.link and c["link"] not in args.link:
            continue
        if args.scenario and c["scenario"] not in args.scenario:
            continue
        if args.src_version and not any(version_matches(c["src"], v) for v in args.src_version):
            continue
        if args.dst_version and not any(version_matches(c["dst"], v) for v in args.dst_version):
            continue
        if c["status"] == "pending" and not (args.include_pending or args.list):
            continue  # pending 只在 --list / 汇总里出现，绝不混进执行
        if c["weekly"] and args.if_due and not c["due"]:
            # 本周不排它：明确报 not-due（可观测），不是「悄悄跳过」。
            c = dict(c, status="not-due")
        out.append(c)
    return out


def require_dsns(cell: dict) -> list[str]:
    _, names = LINKS[(cell["source"], cell["target"])]
    missing = [n for n in names if not os.environ.get(n, "").strip()]
    return missing


def run_cell(cell: dict, timeout: int, verbose: bool) -> tuple[str, str]:
    """跑一格；返回 (status, detail)。"""
    env = dict(os.environ)
    env["JINLIANG_NOPK_LINK"] = cell["link"]
    env["JINLIANG_NOPK_SCENARIO"] = cell["scenario"]
    env["JINLIANG_NOPK_SRC_VERSION"] = cell["src"]
    env["JINLIANG_NOPK_DST_VERSION"] = cell["dst"]
    cmd = ["go", "test", "-tags=integration", "-count=1", "-timeout", f"{timeout}s",
           "-run", "^TestNoPKMatrixCell$", "-v", "./internal/engine/"]
    with tempfile.NamedTemporaryFile(prefix="nopk-", suffix=".log", delete=False) as f:
        log_path = f.name
    try:
        with open(log_path, "w", encoding="utf-8") as log:
            proc = subprocess.run(cmd, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
                                  timeout=timeout + 30)
        rc = proc.returncode
        tail = Path(log_path).read_text(encoding="utf-8", errors="replace").strip().splitlines()[-30:]
    except subprocess.TimeoutExpired:
        rc = 124
        tail = [f"超时（{timeout}s + 30s 外层）"]
    if rc == 0:
        return "pass", ""
    detail = " | ".join(x.strip() for x in tail[-6:] if x.strip())
    if verbose:
        print("\n".join(tail), file=sys.stderr)
    return "fail", detail


def main() -> int:
    ap = argparse.ArgumentParser(description="无主键变体矩阵逐格执行器（test/nopk-matrix.yaml）")
    ap.add_argument("--layer", action="append", choices=["pr", "nightly", "release", "pending"],
                    help="层（可重复）；缺省=全部")
    ap.add_argument("--block", action="append", help="块名（可重复）")
    ap.add_argument("--link", action="append", choices=sorted({v[0] for v in LINKS.values()}),
                    help="链路（可重复）")
    ap.add_argument("--scenario", action="append", help="场景（可重复）")
    ap.add_argument("--src-version", action="append", dest="src_version", help="源版本标签（可重复）")
    ap.add_argument("--dst-version", action="append", dest="dst_version", help="目标版本标签（可重复）")
    ap.add_argument("--week", type=int, default=None, help="ISO 周序（默认本周；用于复跑某一周的 T4）")
    ap.add_argument("--if-due", action="store_true", dest="if_due",
                    help="weekly 块只跑本周该跑的链（nightly 用）")
    ap.add_argument("--include-pending", action="store_true", dest="include_pending",
                    help="连 pending 格也跑（它们会以「方向未接通」失败：这是证明，不是回归）")
    ap.add_argument("--list", action="store_true", help="只列格，不执行")
    ap.add_argument("--limit", type=int, default=0, help="最多跑几格（排查用；0=不限）")
    ap.add_argument("--timeout", type=int, default=300, help="单格超时秒数（默认 300）")
    ap.add_argument("--json", default="", help="把逐格结果写成 JSON")
    ap.add_argument("-v", "--verbose", action="store_true", help="失败时把日志尾巴打到 stderr")
    args = ap.parse_args()

    reg = yaml.safe_load(REGISTRY.read_text(encoding="utf-8"))
    week = args.week if args.week is not None else iso_week()
    all_cells = expand(reg, week, args.if_due)
    selected = select(all_cells, args)

    # ── 汇总（对账用）────────────────────────────────────────
    def counts(layer: str, status: str | None = None, due_only: bool = False) -> int:
        """按**登记表**算格数（weekly 块按 pick 算，不是按整张轮转表算）。"""
        blocks = reg["blocks"]
        n = 0
        for bname in reg["layers"][layer]["blocks"]:
            block = blocks[bname]
            st = block.get("status", "active")
            if status is not None and st != status:
                continue
            weekly = block.get("schedule") == "weekly"
            if due_only and not weekly:
                continue
            pairs = int(block.get("pick", len(block["pairs"]))) if weekly else len(block["pairs"])
            n += pairs * len(block["scenarios"])
        return n

    if args.list:
        for c in selected:
            due = "" if not c["weekly"] else (" due" if c["due"] else " not-due")
            reason = f" reason={c['reason'][:60]}" if c["reason"] else ""
            print(f"CELL nopk {c['id']} link={c['link']} versions={c['src']}->{c['dst']} "
                  f"scenario={c['scenario']} status={c['status']}{due}{reason}")
        for layer in ("pr", "nightly", "release", "pending"):
            weekly = counts(layer, due_only=True)
            fixed = counts(layer) - weekly
            line = f"PLAN layer={layer} fixed={fixed} weekly={weekly} total={counts(layer)}"
            if counts(layer, "pending"):
                line += f" pending={counts(layer, 'pending')}"
            if weekly:
                line += f" per_night={fixed + weekly / 7:.1f}"
            print(line)
        print(f"PLAN week={week} selected={len(selected)}")
        return 0

    # ── 执行 ────────────────────────────────────────────────
    if args.limit:
        selected = selected[: args.limit]
    results = []
    failed = 0
    pending = 0
    not_due = 0
    for c in selected:
        if c["status"] == "not-due":
            not_due += 1
            print(f"RESULT nopk cell={c['id']} link={c['link']} versions={c['src']}->{c['dst']} "
                  f"scenario={c['scenario']} status=not-due")
            results.append({"cell": c["id"], "status": "not-due"})
            continue
        if c["status"] == "pending":
            pending += 1
            print(f"RESULT nopk cell={c['id']} link={c['link']} versions={c['src']}->{c['dst']} "
                  f"scenario={c['scenario']} status=pending reason={c['reason']}")
            results.append({"cell": c["id"], "status": "pending"})
            continue
        missing = require_dsns(c)
        if missing:
            print(f"EX_CONFIG: 格 {c['id']} 缺 DSN {', '.join(missing)}"
                  f"（缺 DSN 就是失败，不跳过；矩阵见 test/nopk-matrix.yaml）", file=sys.stderr)
            return 78
        status, detail = run_cell(c, args.timeout, args.verbose)
        if status != "pass":
            failed += 1
        print(f"RESULT nopk cell={c['id']} link={c['link']} versions={c['src']}->{c['dst']} "
              f"scenario={c['scenario']} status={status}" + (f" detail={detail}" if detail else ""))
        results.append({"cell": c["id"], "status": status, "detail": detail})

    passed = len(results) - failed - pending - not_due
    layers = sorted({c["layer"] for c in selected})
    summary = (f"NOPK_SUMMARY layers={','.join(layers) or '-'} cells={len(results)} "
               f"pass={passed} fail={failed} pending={pending} not_due={not_due}")
    print(summary)
    if args.json:
        Path(args.json).write_text(json.dumps({
            "week": week, "layers": layers, "summary": {
                "cells": len(results), "pass": passed, "fail": failed,
                "pending": pending, "not_due": not_due,
            }, "cells": results}, ensure_ascii=False, indent=2), encoding="utf-8")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
