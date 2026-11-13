#!/usr/bin/env python3
"""校验 testmatrix.yaml 的版本覆盖与分层（required / nightly）。

为什么需要脚本：矩阵是「覆盖率」的唯一事实来源。手写 YAML 很容易出现
「README 说 9/9，文件里只剩 5 个版本」这种漂移；CI 里跑一遍就能当场红。

规则（与 docs/DESIGN.md §2、§3 对齐）：
  - PostgreSQL：9 个版本，同构全组合 81；required = 9 自洽 + 8 相邻 + 2 极端 = 19；
  - MySQL：4 个版本，同构全组合 16；required = 4 自洽 + 3 相邻 = 7；
  - 异构：9×4×2 方向 = 72，全部 nightly（PR 不跑）。

另加一层：无主键变体矩阵（`test/nopk-matrix.yaml`，docs/DESIGN-NO-PRIMARY-KEY.md §2.4）。
它的格数是**从登记表算出来的**，再与定稿对账：
  - PR 10 格 / nightly 37 格每晚 + T4 8 格每周（≈38.1/晚）/ release 184 格 / 待接通 12 格；
  - 「待接通」必须是 pending 且带理由，**不许**混进任何 active 层（否则「矩阵绿」=假覆盖）；
  - 方向与场景要相容：PG→MySQL 只全量（`internal/core/direction.go`），
    所以 insert / pause 这类增量场景出现在该方向上就是登记表写错。
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


# 方向 → 是否支持增量（与 internal/core/direction.go 的 Modes 对齐：PG→MySQL 只全量）。
INCREMENTAL = {
    ("postgres", "postgres"): True,
    ("mysql", "postgres"): True,
    ("postgres", "mysql"): False,
}
PENDING_DIRECTIONS = {("mysql", "mysql")}


def mysql_labels(versions):
    """MySQL 版本标签：5.7.x→5.7、8.0.x→8.0、8.4.x→8.4、9.x→9（与定稿的「9.x」一致）。"""
    out = set()
    for v in versions:
        parts = str(v).split(".")
        out.add("5.7" if parts[0] == "5" else (".".join(parts[:2]) if parts[0] == "8" else parts[0]))
    return out


def check_nopk(root, pg, my):
    """校验无主键变体矩阵（test/nopk-matrix.yaml）：算术 + 方向相容 + 待接通标注 + 跨文件一致。"""
    path = root / "test" / "nopk-matrix.yaml"
    if not path.exists():
        raise AssertionError("缺 test/nopk-matrix.yaml（无主键变体矩阵的登记表）")
    doc = load(path)
    blocks = doc.get("blocks") or {}
    layers = doc.get("layers") or {}
    scenarios = set(doc.get("scenarios") or {})
    pg_ok = {str(v).split(".")[0] for v in (pg.get("versions") or [])}
    my_ok = mysql_labels(my.get("versions") or [])

    def want_versions(kind, v, where):
        ok = pg_ok if kind == "postgres" else my_ok
        if str(v) not in ok:
            raise AssertionError(f"{where}: 版本 {v} 不在连接器 testmatrix 的版本档里（{sorted(ok)}）")

    for name, b in blocks.items():
        src, tgt = b.get("source"), b.get("target")
        if (src, tgt) not in INCREMENTAL and (src, tgt) not in PENDING_DIRECTIONS:
            raise AssertionError(f"{name}: 不认识的方向 {src}→{tgt}")
        if not b.get("pairs"):
            raise AssertionError(f"{name}: pairs 不能为空")
        for pair in b["pairs"]:
            if len(pair) != 2:
                raise AssertionError(f"{name}: pair {pair} 必须是 [源版本, 目标版本]")
            want_versions(src, pair[0], name)
            want_versions(tgt, pair[1], name)
            if (src, tgt) in PENDING_DIRECTIONS:
                continue
            # 降级场景只允许在「目标版本更低」的链路上。
            if "downgrade" in b["scenarios"] and int(pair[0]) <= int(pair[1]):
                raise AssertionError(f"{name}: downgrade 只能标在目标版本更低的链路上，实际 {pair}")
        for sc in b["scenarios"]:
            if sc not in scenarios:
                raise AssertionError(f"{name}: 未声明的场景 {sc}")
            if (src, tgt) in PENDING_DIRECTIONS:
                continue  # 方向未接通：场景先按「接通后要跑的」登记，跑不了由 pending 标出来
            if sc in {"insert", "pause", "unique_key_continuous"} and not INCREMENTAL.get((src, tgt), False):
                raise AssertionError(
                    f"{name}: 场景 {sc} 需要增量写路径，但 {src}→{tgt} 只全量（direction.go）")
        pending = b.get("status") == "pending"
        if (src, tgt) in PENDING_DIRECTIONS and not pending:
            raise AssertionError(f"{name}: {src}→{tgt} 方向未接通，必须标 status: pending（不许当已覆盖）")
        if pending and not str(b.get("reason") or "").strip():
            raise AssertionError(f"{name}: pending 必须写 reason（说清为什么跑不了、何时能改 active）")
        if b.get("schedule") == "weekly":
            pick = int(b.get("pick", 0))
            if not 0 < pick < len(b["pairs"]):
                raise AssertionError(f"{name}: weekly 块的 pick={pick} 必须在 1..{len(b['pairs'])-1}")

    for layer, spec in layers.items():
        for bname in spec.get("blocks") or []:
            if bname not in blocks:
                raise AssertionError(f"layer {layer}: 引用了不存在的块 {bname}")
            if layer != "pending" and blocks[bname].get("status") == "pending":
                raise AssertionError(f"layer {layer}: 不能把 pending 块 {bname} 当已覆盖")
    if set(layers.get("pending", {}).get("blocks") or []) != {"pending-mysql-mysql"}:
        raise AssertionError("pending 层必须且只能登记 MySQL→MySQL 的待接通块")

    def count(layer, weekly=None, status=None):
        n = 0
        for bname in layers[layer]["blocks"]:
            b = blocks[bname]
            if status is not None and b.get("status", "active") != status:
                continue
            is_weekly = b.get("schedule") == "weekly"
            if weekly is not None and is_weekly != weekly:
                continue
            pairs = int(b.get("pick", len(b["pairs"]))) if is_weekly else len(b["pairs"])
            n += pairs * len(b["scenarios"])
        return n

    # 与定稿 §2.4 对账：PR 10 / nightly 37 每晚 + T4 8 每周（≈38.1）/ release 184 / 待接通 12。
    got = {
        "pr": count("pr"),
        "nightly_fixed": count("nightly", weekly=False),
        "nightly_weekly": count("nightly", weekly=True),
        "release": count("release"),
        "pending": count("pending"),
    }
    want = {"pr": 10, "nightly_fixed": 37, "nightly_weekly": 8, "release": 184, "pending": 12}
    if got != want:
        raise AssertionError(f"无主键变体格数与定稿不符：{got}，要求 {want}")
    if count("nightly", status="pending") or count("pr", status="pending") or count("release", status="pending"):
        raise AssertionError("active 层里出现了 pending 格：待接通不许混进已覆盖")

    # 跨文件一致：连接器自己的 testmatrix.yaml 要登记「待接通」这笔账。
    mp = (my.get("no_pk") or {})
    pp = (pg.get("no_pk") or {})
    if int(mp.get("pending_cells", -1)) != got["pending"]:
        raise AssertionError(f"mysql testmatrix 的 no_pk.pending_cells 应为 {got['pending']}，实际 {mp.get('pending_cells')}")
    if int(pp.get("pending_cells", -1)) != 0:
        raise AssertionError(f"postgres testmatrix 的 no_pk.pending_cells 应为 0，实际 {pp.get('pending_cells')}")
    for label, doc2 in (("mysql", my), ("postgres", pg)):
        reg = (doc2.get("no_pk") or {}).get("registry")
        if reg != "test/nopk-matrix.yaml":
            raise AssertionError(f"{label} testmatrix 的 no_pk.registry 应指向 test/nopk-matrix.yaml，实际 {reg}")

    return got


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

    nopk = check_nopk(root, pg, my)
    print("testmatrix: postgres 9/9 (81), mysql 4/4 (16), cross 72 — 分层正确")
    print(f"nopk: pr {nopk['pr']}/10 · nightly {nopk['nightly_fixed']} 每晚 + T4 "
          f"{nopk['nightly_weekly']}/周（≈{(nopk['nightly_fixed'] + nopk['nightly_weekly']/7):.1f}/晚，定稿 ≈39）"
          f" · release {nopk['release']}/184 · 待接通 {nopk['pending']}/12（pending，不计入覆盖）")


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"testmatrix: FAIL: {e}", file=sys.stderr)
        sys.exit(1)
