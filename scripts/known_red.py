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
        id="exit-code-3",
        step="暂停的 run 必须退 3（欠账不是失败）",
        owner="Panel/Core",
        detector="TestPausedRunExitsNeedsAction（-tags='integration exitcode3'）",
        unblock="引擎派生 ExitNeedsAction=3（失败 1 > 停止 130 > 正常收尾且有暂停表 3 > 0）；"
                "#109 已落地，这一步的 continue-on-error 已删（现为阻塞判据）；"
                "条目保留在台账里，对账人仍是 engine-mutation 的 verify 步骤",
        probe="job:engine-mutation",
    ),
    dict(
        id="chunk-resume-wiring",
        step="块级续跑（known-red：单表口径，重启搬 ≈ 未搬完 ±1 块）",
        owner="Core",
        detector="scripts/b1_mysql_to_mysql.sh killcopy（单表口径：重启搬 ≈ 未搬完 ±1 块）",
        unblock="引擎 generic 全量路径读块进度（当前 copyGeneric 把 nil 传给 SnapshotChunksFrom，"
                "internal/engine/target.go:456；executeGeneric 从不读 ChunkProgress → 必然整轮重搬）；"
                "接通且该格重搬行数 ≤1 块后转阻塞。注：块记账本身已落（#95），已是**阻塞**步骤",
        probe="mysqlsink-writes-chunks",
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
    """有 pyyaml 就用它；**没有也不能瘫**（CI 的 logic job 没装 pip 包）。

    没有 pyyaml 时退回逐行扫描：找到 `name: <步骤名>` 之后，到下一个 `- ` 开头的条目之前，
    只要出现 `continue-on-error: true` 就算挂起。YAML 是缩进结构，对我们这种「只看一步」
    的用途足够；宁可粗一点，也不要因为缺一个 Python 包就让门禁自己红。
    """
    try:
        import yaml  # noqa: F401
        return yaml
    except ImportError:
        return None


def step_suspended(doc, step_name, text=None):
    """工作流里这个步骤还挂着 continue-on-error 吗。找不到步骤 → found=False（台账与 YAML 失联）。

    返回 (found, suspended)。doc 为 None 时走逐行扫描（没有 pyyaml 的兜底）。
    """
    want = step_name.strip()
    if doc is not None:
        for job in (doc.get("jobs") or {}).values():
            for st in job.get("steps") or []:
                if (st.get("name") or "").strip() == want:
                    return True, bool(st.get("continue-on-error"))
        return False, False

    # 兜底：逐行扫描
    lines = (text or "").splitlines()
    for i, line in enumerate(lines):
        key = "name:"
        if key not in line:
            continue
        val = line.split(key, 1)[1].strip()
        if val.strip('"\'') != want:
            continue
        for j in range(i + 1, len(lines)):
            nxt = lines[j]
            if nxt.lstrip().startswith("- "):
                break
            if "continue-on-error:" in nxt and "true" in nxt.lower():
                return True, True
        return True, False
    return False, False


TEMPLATE_MARKERS = ("责任人", "转绿用例", "移除 continue-on-error")


def template_missing(text, step_name):
    """三件套模板自检：已知红步骤的注释必须写清「责任人 / 转绿用例 / 移除条件」。

    只认**这一步自己的注释块**（步骤名那一行往上 12 行 + 到 `run:`/`uses:` 之前），
    不认 job 级别的说明——否则「挂在 job 头上」也能糊过去，下一格已知红还是同一个形状。
    """
    lines = text.splitlines()
    want = step_name.strip()
    idx = None
    for i, line in enumerate(lines):
        if "name:" in line and line.split("name:", 1)[1].strip().strip('"\'') == want:
            idx = i
            break
    if idx is None:
        return list(TEMPLATE_MARKERS)
    # 只收「紧贴在步骤名上面的连续 # 注释」——往上遇到非注释行就停，
    # 免得把文件头的说明文字当成这一步的三件套（自证 fixture 就踩过这个坑）。
    above = []
    j = idx - 1
    while j >= 0 and lines[j].strip().startswith("#"):
        above.append(lines[j])
        j -= 1
    block = list(reversed(above)) + [lines[idx]]
    for j in range(idx + 1, len(lines)):
        l = lines[j]
        if l.strip().startswith("run:") or l.strip().startswith("uses:"):
            break
        block.append(l)
    blob = "\n".join(block)
    return [m for m in TEMPLATE_MARKERS if m not in blob]


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


def cross_job_ok(text, job, entry_id):
    """`job:` 型条目**不许恒真**：确认那个 job 里真的挂着 known_red.py verify --id <id>。

    之前这里是「probe 以 job: 开头就直接 ok」，于是两条台账都写成 job: 型，logic job
    自查永远绿；`mysqlsink-writes-chunks` 探针写好了也从没被调用过（probe 指成了
    `job:mysql-mysql`）。现在这里做结构性核对：
      · 工作流里没有这个 job            → 判红（台账指向不存在的 job）；
      · 该 job 里没有 verify --id <id>  → 判红（没人对账=恒真放行）。
    真正的判定仍由那个 job 的 verify 步骤做（它拿得到检测器这一步的 outcome）。
    """
    m = re.search(r"(?m)^  %s:\s*$" % re.escape(job), text)
    if not m:
        return False, "工作流里没有 job %r：台账指向了不存在的 job" % job
    rest = text[m.end():]
    nxt = re.search(r"(?m)^  [A-Za-z0-9_-]+:\s*$", rest)
    block = rest[:nxt.start()] if nxt else rest
    if not re.search(r"known_red\.py verify\s+--id\s+%s\b" % re.escape(entry_id), block):
        return False, "job %r 里没有 `known_red.py verify --id %s`：没人对账=恒真放行" % (job, entry_id)
    return True, "由 job %s 的 verify 步骤真实对账（跨 job，本 job 不重复判定）" % job


def check(only=None, workflow=None, probes=None):
    yaml = _yaml()
    workflow = workflow or WORKFLOW
    probes = probes or PROBES
    with open(workflow, encoding="utf-8") as f:
        text = f.read()
    doc = yaml.safe_load(text) if yaml else None

    rows, bad = [], 0
    for e in REGISTRY:
        if only and e["id"] != only:
            continue
        found, suspended = step_suspended(doc, e["step"], text)
        if suspended:
            missing = template_missing(text, e["step"])
            if missing:
                rows.append((e, None, suspended,
                             "注释缺三件套：%s（责任人 / 转绿用例 / 移除 continue-on-error）" % "、".join(missing),
                             False))
                bad += 1
                continue
        if e["probe"].startswith("job:"):
            ok, why = cross_job_ok(text, e["probe"][4:], e["id"])
            rows.append((e, None, suspended, why, ok))
            if not ok:
                bad += 1
            continue
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


FIXTURE = os.path.join(ROOT, "scripts/testdata/known-red-suspended.yml")
FIXTURE_MISSING = os.path.join(ROOT, "scripts/testdata/known-red-missing-owner.yml")


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
    # 端到端那条：fixture 里步骤仍挂起 + 检测器 outcome=success（= 实现已落地）→ 必须判 1。
    # 这是任务要求的「构造一次『实现已落地 + 步骤仍挂起』的场景，门禁必须红」的自证。
    rc_hit = verify("chunk-resume-wiring", "success", workflow=FIXTURE, quiet=True)
    rc_ok = verify("chunk-resume-wiring", "failure", workflow=FIXTURE, quiet=True)
    if rc_hit != 1:
        bad += 1
        print("selftest: FAIL 已落地 + 仍挂起 —— 应判 1，实际 %d" % rc_hit)
    else:
        print("selftest: ok   已落地 + 仍挂起 = 门禁红（fixture）")
    if rc_ok != 0:
        bad += 1
        print("selftest: FAIL 仍红 + 仍挂起 —— 应放行，实际 %d" % rc_ok)
    else:
        print("selftest: ok   仍红 + 仍挂起 = 放行（fixture）")

    # 三件套模板：真 workflow 必须齐全；故意缺「责任人」的 fixture 必须被点出来。
    entry = next(e for e in REGISTRY if e["id"] == "chunk-resume-wiring")
    step = entry["step"]
    real_missing = template_missing(open(WORKFLOW, encoding="utf-8").read(), step)
    fx_missing = template_missing(open(FIXTURE_MISSING, encoding="utf-8").read(), step)
    if real_missing:
        bad += 1
        print("selftest: FAIL 真 workflow 的三件套缺：%s" % "、".join(real_missing))
    else:
        print("selftest: ok   真 workflow 三件套齐全")
    if fx_missing != ["责任人"]:
        bad += 1
        print("selftest: FAIL 缺责任人的 fixture 应被点出 ['责任人']，实际 %r" % (fx_missing,))
    else:
        print("selftest: ok   三件套缺一项 = 判失败（fixture）")
    # `job:` 型条目：缺 verify 步骤必须判红（否则「跨 job」就是恒真放行的后门）
    fx_cross = os.path.join(ROOT, "scripts/testdata/known-red-no-verify.yml")
    ok, why = cross_job_ok(open(fx_cross, encoding="utf-8").read(), "engine-mutation", "exit-code-3")
    if ok:
        bad += 1
        print("selftest: FAIL job: 型条目缺 verify 步骤也应判红，实际放行（%s）" % why)
    else:
        print("selftest: ok   job: 型条目缺 verify 步骤 = 判红（fixture）")
    ok, why = cross_job_ok(open(WORKFLOW, encoding="utf-8").read(), "engine-mutation", "exit-code-3")
    if not ok:
        bad += 1
        print("selftest: FAIL 真 workflow 里 exit-code-3 的对账步骤不见了：%s" % why)
    else:
        print("selftest: ok   真 workflow 里 exit-code-3 有 verify 步骤")

    if bad:
        print("selftest: %d 条不符合预期" % bad)
        return 1
    print("selftest: %d 条判据全部符合预期" % (len(SELFTEST) + 4))
    return 0


def verify(entry_id, result, workflow=None, text=None, quiet=False):
    """由 DB job 用「检测器这一步的 outcome」对账：success = 实现已落地。"""
    e = next((x for x in REGISTRY if x["id"] == entry_id), None)
    if e is None:
        print("known-red: 台账里没有 %r" % entry_id, file=sys.stderr)
        return 2
    workflow = workflow or WORKFLOW
    yaml = _yaml()
    with open(workflow, encoding="utf-8") as f:
        wt = f.read()
    doc = yaml.safe_load(wt) if yaml else None
    found, suspended = step_suspended(doc, e["step"], wt)
    if not found:
        print("known-red: 步骤 %r 不在 %s 里（台账与 YAML 失联）" % (e["step"], workflow), file=sys.stderr)
        return 1
    landed = (result or "").strip() == "success"
    ok, verdict = classify(landed, suspended)
    if quiet:
        return 0 if ok else 1
    print("known-red[%s]：检测器 outcome=%s（%s），步骤%s → %s" %
          (entry_id, result, "实现已落地" if landed else "仍红",
           "仍挂起 continue-on-error" if suspended else "已激活", verdict))
    if landed and suspended:
        print("  ↑ 实现已经能过，断言还挂着：把这一步的 continue-on-error 删掉（责任人 %s）" % e["owner"])
    if ok:
        return 0
    return 1


def main(argv):
    ap = argparse.ArgumentParser(prog="known_red.py")
    sub = ap.add_subparsers(dest="cmd")
    sub.add_parser("list")
    p = sub.add_parser("check")
    p.add_argument("--only", default="")
    p.add_argument("--workflow", default="")
    q = sub.add_parser("verify")
    q.add_argument("--id", required=True)
    q.add_argument("--result", required=True, help="检测器这一步的 outcome：success / failure")
    q.add_argument("--workflow", default="")
    sub.add_parser("selftest")
    a = ap.parse_args(argv)
    if a.cmd == "selftest":
        return selftest()
    if a.cmd == "list":
        return check()
    if a.cmd == "check":
        return check(only=a.only or None, workflow=a.workflow or None)
    if a.cmd == "verify":
        # verify 必须看**真实工作流**（或显式给的 fixture），不接受「没有 continue-on-error 的假文件」
        return verify(a.id, a.result, workflow=a.workflow or None)
    ap.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
