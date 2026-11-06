#!/usr/bin/env python3
"""校验 testmatrix.yaml 的版本覆盖与分层（required / nightly）。

为什么需要脚本：矩阵是「覆盖率」的唯一事实来源。手写 YAML 很容易出现
「README 说 9/9，文件里只剩 5 个版本」这种漂移；CI 里跑一遍就能当场红。

规则（与 docs/DESIGN.md §2、§3 对齐）：
  - PostgreSQL：9 个版本，同构全组合 81；required = 9 自洽 + 8 相邻 + 2 极端 = 19；
  - MySQL：4 个版本，同构全组合 16；required = 4 自洽 + 3 相邻 = 7；
  - 异构：9×4×2 方向 = 72，全部 nightly（PR 不跑）。
"""
import sys
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover - CI installs pyyaml
    sys.exit("需要 pyyaml：pip install pyyaml")


def load(path):
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def check_versions(doc, want, label):
    versions = doc.get("versions") or []
    if len(versions) != want:
        raise AssertionError(f"{label}: versions 有 {len(versions)} 个，要求 {want}")
    if len(set(versions)) != len(versions):
        raise AssertionError(f"{label}: versions 有重复")


def sum_counts(items, field="count"):
    return sum(int(x.get(field, 0)) for x in items or [])


def check_matrix(doc, total, required, label):
    m = doc.get("matrix") or {}
    if int(m.get("total_pairs", 0)) != total:
        raise AssertionError(f"{label}: matrix.total_pairs={m.get('total_pairs')}，要求 {total}")
    req = m.get("required") or []
    night = m.get("nightly") or []
    if sum_counts(req) != required:
        raise AssertionError(f"{label}: required 组合数={sum_counts(req)}，要求 {required}")
    if sum_counts(night) != total:
        raise AssertionError(f"{label}: nightly 组合数={sum_counts(night)}，要求 {total}")
    scenarios = set(doc.get("scenarios") or [])
    for tier in req + night:
        unknown = set(tier.get("scenarios") or []) - scenarios
        if unknown:
            raise AssertionError(f"{label}/{tier.get('name')}: 未声明的场景 {sorted(unknown)}")


def main():
    root = Path(__file__).resolve().parent.parent
    pg = load(root / "connectors/postgres/testmatrix.yaml")
    my = load(root / "connectors/mysql/testmatrix.yaml")

    check_versions(pg, 9, "postgres")
    check_versions(my, 4, "mysql")
    check_matrix(pg, 81, 19, "postgres")
    check_matrix(my, 16, 7, "mysql")

    cross = pg.get("cross_matrix") or {}
    if int(cross.get("total_pairs", 0)) != 72:
        raise AssertionError(f"cross_matrix.total_pairs={cross.get('total_pairs')}，要求 72")
    if int(cross.get("required", 0)) != 0:
        raise AssertionError("跨库组合不进 PR（required 必须为 0）")
    if int(cross.get("nightly", 0)) != 72:
        raise AssertionError("cross_matrix.nightly 必须是 72")

    print("testmatrix: postgres 9/9 (81), mysql 4/4 (16), cross 72 — 分层正确")


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"testmatrix: FAIL: {e}", file=sys.stderr)
        sys.exit(1)
