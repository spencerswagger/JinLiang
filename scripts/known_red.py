#!/usr/bin/env python3
"""known-red 台账：**已知会红的步骤不许无限期挂着放行**。

为什么需要它：把「先武装断言、先红」做成 `continue-on-error: true` 很省事，但没有收口机
制的话，两格会永远红着放行——「门禁能拦住错误合并」这句话就不成立了。所以每一格必须带
三元组：

    责任人 / 转绿的用例名 / **移除 continue-on-error 的触发条件**

并且由机器每天对一次账，而不是靠人记得：

  · 实现**还没落地**、步骤仍挂起   → 合法（打印倒计时清单，继续跑）；
  · 实现**已经落地**、步骤仍挂起   → **门禁判 1（拦截）**，理由「断言未激活」——
    断言写好了却没人打开，等于没有；
  · 实现没落地、步骤却已改成阻塞   → 判 1：「提前激活」——CI 会常红，先改回来。

用法：
  known_red.py list                    # 打印倒计时清单（责任人在哪、等什么）
  known_red.py check [--only ID]       # 对账；有问题退出 1
  known_red.py selftest                # 判据自检（四种世界各一条）
退出码：0 = 台账与现状一致 / 1 = 有问题（拦截）/ 2 = 用法或环境不成立。
"""

import argparse
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WORKFLOW = os.path.join(ROOT, ".github/workflows/merge-gate.yml")

# 台账。每条都必须是「可对账」的：probe 判「实现落地没有」，step 判「步骤还挂着没有」。
REGISTRY = [
    dict(
        id="mysqlsink-chunk-accounting",
        step="块级续跑（块进度 > 0 且重启重搬 ≤ 1 块）",
        owner="Data",
        detector="scripts/b1_mysql_to_mysql.sh killcopy（_jl_chunk > 0 且重启重搬 ≤ 1 块）",
        unblock="mysqlsink 在全量搬到 MySQL 目标端时写 `_jl_chunk`；合入后删掉该步骤的 continue-on-error",
        probe="mysqlsink-writes-chunks",
    ),
    dict(
        id="stop-swallow-late-error",
        step="回归（knownred）：停止请求之后的真实流错误不许被记成「已停止」",
        owner="Core",
        detector="TestStopRequestDoesNotSwallowLateRealError（-tags=knownred）",
        unblock="target.go 的收尾判据改成「先判错误是否由我们 cancel 引起，再看 stopHit」；合入后删 continue-on-error 并去掉 knownred 标签",
        probe="late-error-test-passes",
    ),
]


def _mysqlsink_writes_chunks():
    """实现落地没有：mysqlsink 的非测试代码里有没有「写 _jl_chunk」的动作。

    判据刻意粗一点（INSERT 语句或显式的 RecordChunk）：宁可早一点提示「该转绿了」，
    也不要因为写法不同而永远认为「还没落地」——那正是这条机制要防的。
    """
    d = os.path.join(ROOT, "internal/mysqlsink")
    pat = re.compile(r"INSERT\s+INTO\s+`?_jl_chunk|RecordChunk\(")
    for name in sorted(os.listdir(d)):
        if not name.endswith(".go") or name.endswith("_test.go"):
            continue
        with open(os.path.join(d, name), encoding="utf-8", errors="ignore") as f:
            if pat.search(f.read()):
                return True, "%s 里已经有写 _jl_chunk 的动作" % name
    return False, "mysqlsink 里还没有写 _jl_chunk 的代码（块记账未落地）"


def _late_error_test_passes():
    """实现落地没有：known-red 用例现在绿不绿（绿=已修）。"""
    try:
        proc = subprocess.run(
            ["go", "test", "-tags=knownred", "-count=1",
             "-run", "^TestStopRequestDoesNotSwallowLateRealError$", "./internal/engine/"],
            cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    except FileNotFoundError:
        return None, "本机没有 go 可执行文件，无法判断（不是「未落地」）"
    if proc.returncode == 0:
        return True, "用例已绿（收尾判据已修）"
    return False, "用例仍红（收尾判据未修）"


PROBES = {
    "mysqlsink-writes-chunks": _mysqlsink_writes_chunks,
    "late-error-test-passes": _late_error_test_passes,
}


def _yaml():
    try:
        import yaml  # noqa: F401
        return yaml
    except ImportError:  # pragma: no cover - CI 装了 pyyaml
        sys.exit("known-red: 需要 pyyaml（pip install pyyaml）")


def step_suspended(doc, step_name):
    """工作流里这个步骤还挂着 continue-on-error 吗。找不到步骤 → 报错（台账与 YAML 失联）。

    返回 (found, suspended)。
    """
    for job in (doc.get("jobs") or {}).values():
        for st in job.get("steps") or []:
            if (st.get("name") or "").strip() == step_name.strip():
                return True, bool(st.get("continue-on-error"))
    return False, False


def classify(landed, suspended):
    if landed is None:
        return None, "无法探测（缺 go / 缺 pyyaml 之类），台账暂时对不上"
    """三元组对账的唯一判据（纯函数，selftest 直接打它）。"""
    if landed and suspended:
        return False, "断言未激活（实现已落地，步骤还挂着 continue-on-error）"
    if (not landed) and (not suspended):
        return False, "提前激活（实现没落地就把步骤改成阻塞，CI 会常红）"
    if landed and not suspended:
        return True, "已收口（实现落地 + 断言已激活）"
    return True, "已知红（等实现，步骤挂起中）"


def check(only=None, workflow=None, probes=None):
    yaml = _yaml()
    workflow = workflow or WORKFLOW
    probes = probes or PROBES
    with open(workflow, encoding="utf-8") as f:
        doc = yaml.safe_load(f)

    rows, bad = [], 0
    for e in REGISTRY:
        if only and e["id"] != only:
            continue
        found, suspended = step_suspended(doc, e["step"])
        if not found:
            rows.append((e, False, False, "步骤不在工作流里（台账与 YAML 失联）", False))
            bad += 1
            continue
        landed, why = probes[e["probe"]]()
        ok, verdict = classify(landed, suspended)
        if ok is None:
            rows.append((e, landed, suspended, verdict + "：" + why, None))
            bad += 1
            continue
        rows.append((e, landed, suspended, verdict + "：" + why, ok))
        if not ok:
            bad += 1

    print("known-red 台账（责任人 / 等什么 / 现在什么状态）：")
    print("| 步骤 | 责任人 | 转绿用例 | 移除 continue-on-error 的条件 | 现状 |")
    print("|---|---|---|---|---|")
    for e, landed, suspended, verdict, ok in rows:
        mark = "✅" if ok else "❌"
        state = ("无法探测" if landed is None else ("实现已落地" if landed else "等实现")) + \
                ("；步骤仍挂起" if suspended else "；步骤已激活")
        print("| %s | %s | %s | %s | %s %s — %s |" %
              (e["step"], e["owner"], e["detector"], e["unblock"], mark, state, verdict))
    if bad:
        print("known-red: %d 条台账不成立（见上）" % bad)
        return 1
    print("known-red: 台账与现状一致（%d 条）" % len(rows))
    return 0


# ── selftest：四种世界各一条 ────────────────────────────────────
SELFTEST = [
    ("等实现 + 挂起 = 合法", False, True, True),
    ("实现已落地 + 挂起 = 拦截（断言未激活）", True, True, False),
    ("实现没落地 + 已激活 = 拦截（提前激活）", False, False, False),
    ("实现已落地 + 已激活 = 合法（已收口）", True, False, True),
]


def selftest():
    bad = 0
    for name, landed, suspended, want in SELFTEST:
        ok, why = classify(landed, suspended)
        if ok != want:
            bad += 1
            print("selftest: FAIL %s —— 期望 %s，实际 %s（%s）" %
                  (name, "通过" if want else "拦截", "通过" if ok else "拦截", why))
        else:
            print("selftest: ok   %s" % name)
    if bad:
        print("selftest: %d/%d 条不符合预期" % (bad, len(SELFTEST)))
        return 1
    print("selftest: %d 条判据全部符合预期" % len(SELFTEST))
    return 0


def main(argv):
    ap = argparse.ArgumentParser(prog="known_red.py")
    sub = ap.add_subparsers(dest="cmd")
    sub.add_parser("list")
    p = sub.add_parser("check")
    p.add_argument("--only", default="")
    p.add_argument("--workflow", default="")
    sub.add_parser("selftest")
    a = ap.parse_args(argv)
    if a.cmd == "selftest":
        return selftest()
    if a.cmd == "list":
        return check()
    if a.cmd == "check":
        return check(only=a.only or None, workflow=a.workflow or None)
    ap.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
