#!/usr/bin/env python3
"""反向注入（mutation gate）：往「被判的东西」里注入破坏，门禁必须变红。

判据不是「测试跑绿了」，而是「**把实现改坏，测试会红**」。只跑绿灯的门禁拦不住
「位点不落盘也过」「跳过暂停也过」「md5 比对被注释掉也过」这三类事故。

两档：
  logic  —— 注入到门禁自己的判据（scripts/gate_checks.py、scripts/review_gate_logic.py），
            检测器是它们的 selftest。不需要数据库，秒级。
  engine —— 注入到实现（pgsink 的位点写入 / 引擎的按表过滤与恢复 / 块进度读取），
            检测器是对应用例。需要真库（PG）。
            注：#93 之后暂停判定在 `internal/engine/pause_skip.go`，`pgsink.Apply`
            只剩「引擎漏过滤就报错」的最后防线，所以注入点跟着搬出 pgsink。

每条的字段：名称、注入点（文件 + 一行代码）、期望失败信号、实际结果、是否 fail。
任何一条「注入后检测器仍然绿」= 门禁放行了错误实现 → 本脚本退出 1。

用法：
    scripts/mutation_gate.py                 # logic 档（默认）
    scripts/mutation_gate.py --tier all      # logic + engine（需要 DSN）
    scripts/mutation_gate.py --only fp-compare-neutered
    scripts/mutation_gate.py --json /tmp/report.json
退出码：0 = 全部注入都被抓住 / 1 = 有注入存活或注入点失配 / 2 = 用法或环境不成立。
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# 注入清单。find/replace 必须**只命中一处**（多处命中说明注入点不够具体，直接判失败，
# 避免「改错了地方却以为测到了」）。
MUTATIONS = [
    # ── logic 档：门禁自己的判据 ───────────────────────────────
    dict(
        name="fp-compare-neutered",
        tier="logic",
        file="scripts/gate_checks.py",
        find="    if a[1] != b[1]:",
        replace="    if False:  # SABOTAGE: 内容比对关掉",
        cmd=[sys.executable, "scripts/gate_checks.py", "selftest"],
        signal="selftest 退出 1（fp md5 不同那条必须红）",
        note="篡改期望 md5 / 把内容指纹比对关掉",
    ),
    dict(
        name="rows-count-neutered",
        tier="logic",
        file="scripts/gate_checks.py",
        find="    if src != expect:",
        replace="    if False:  # SABOTAGE: 源端行数不看了",
        cmd=[sys.executable, "scripts/gate_checks.py", "selftest"],
        signal="selftest 退出 1（rows 源少一行那条必须红）",
        note="行数校验改成只看目标端",
    ),
    dict(
        name="recopy-check-neutered",
        tier="logic",
        file="scripts/gate_checks.py",
        find="    if copied > allow:",
        replace="    if False:  # SABOTAGE: 0 行重搬校验关掉",
        cmd=[sys.executable, "scripts/gate_checks.py", "selftest"],
        signal="selftest 退出 1（recopy 1 / 整轮那两条必须红）",
        note="关闭「kill -9 后重搬 0 行」校验",
    ),
    dict(
        name="pause-filter-keeps-all",
        tier="engine",
        file="internal/engine/pause_skip.go",
        find="\t\tif _, paused := p.r.pausedRecord(key); !paused {",
        replace="\t\tif _, paused := p.r.pausedRecord(key); true || !paused { // SABOTAGE: 暂停表也照写",
        cmd=["go", "test", "-count=1", "-run", "^TestPauseSinkFilterDropsOnlyPausedTableRows$", "./internal/engine/"],
        requires="TestPauseSinkFilterDropsOnlyPausedTableRows",
        signal="TestPauseSinkFilterDropsOnlyPausedTableRows FAIL（暂停表照写=结构对不上还往目标端塞）",
        note="跳过 DDL 暂停（不再按表过滤）",
    ),
    dict(
        name="pause-keys-neutered",
        tier="logic",
        file="scripts/gate_checks.py",
        find="        if got != exp:",
        replace="        if False:  # SABOTAGE: 暂停登记的表不对也不管",
        cmd=[sys.executable, "scripts/gate_checks.py", "selftest"],
        signal="selftest 退出 1（pause 多/少登记了表那两条必须红）",
        note="把「只暂停该表」放宽成「登记了就行」",
    ),
    dict(
        name="review-deny-word-weakened",
        tier="logic",
        file="scripts/review_gate_logic.py",
        find='DENY = re.compile(r"不可合并|不予放行|不通过|必须修|必改\\s*[1-9]")',
        replace='DENY = re.compile(r"不可合并|不予放行|不通过|必须修|必改\\s*[8-9]")',
        cmd=[sys.executable, "scripts/review_gate_logic.py", "selftest"],
        signal="selftest 退出 1（mustfix-2 / history-zero 必须红）",
        note="把否决词收紧到「必改 8/9 条」，必改 2 条就会误放行（#53 事故的形状）",
    ),
    dict(
        name="review-stale-accepts",
        tier="logic",
        file="scripts/review_gate_logic.py",
        find='        if anchor.get("production_touched"):',
        replace='        if False and anchor.get("production_touched"):  # SABOTAGE: 旧结论盖新代码也放行',

        cmd=[sys.executable, "scripts/review_gate_logic.py", "selftest"],
        signal="selftest 退出 1（production-changed / test-hand-added 必须红）",
        note="把「旧结论盖新代码」的拦截关掉（#59 要防的正是这个）",
    ),
    # ── engine 档：实现（需要真 PG） ───────────────────────────
    dict(
        name="position-not-persisted",
        tier="engine",
        file="internal/pgsink/pgsink.go",
        find="func (s *Sink) setPositionTx(ctx context.Context, tx pgx.Tx, pos string) error {\n",
        replace="func (s *Sink) setPositionTx(ctx context.Context, tx pgx.Tx, pos string) error {\n\treturn nil // SABOTAGE: 位点不落盘\n",
        cmd=["go", "test", "-tags=integration", "-count=1", "-run", "^TestPositionPersistedWithData$", "./internal/pgsink/"],
        requires="TestPositionPersistedWithData",
        signal="TestPositionPersistedWithData FAIL（位点是唯一权威，不落盘=重启重搬/丢变更）",
        note="移除位点持久化",
    ),
    dict(
        name="register-pause-neutered",
        tier="engine",
        file="internal/engine/engine.go",
        find="func (r *runner) registerPause(ctx context.Context, err error) error {\n",
        replace="func (r *runner) registerPause(ctx context.Context, err error) error {\n\treturn nil // SABOTAGE: 不登记暂停\n",
        cmd=["go", "test", "-tags=integration", "-count=1", "-run",
             "^TestPauseRegistrationFailureFailsWholeBatch$", "./internal/engine/"],
        requires="TestPauseRegistrationFailureFailsWholeBatch",
        signal="TestPauseRegistrationFailureFailsWholeBatch FAIL（登记被跳过→错误退化成原始类型不一致，没有「登记失败」这条根因）",
        note="跳过 DDL 暂停的登记（不写 paused_tables）",
    ),
    dict(
        name="chunk-resume-degraded",
        tier="engine",
        file="internal/engine/resume.go",
        find="\tp.Kind = copyResume\n",
        replace="\tp.Kind = copyFresh // SABOTAGE: 块级续跑退化成整轮重搬\n",
        cmd=["go", "test", "-count=1", "-run", "^TestPlanCopy$", "./internal/engine/"],
        requires="TestPlanCopy",
        signal="TestPlanCopy FAIL（续跑退化=kill -9 后整轮重搬，重搬行数不为 0）",
        note="块进度读不出来 / 续跑退化成整轮重搬",
    ),
    # #93 之后「暂停判定」在引擎侧（internal/engine/pause_skip.go），pgsink.Apply 只剩
    # 「引擎漏过滤就报错」的最后防线。所以注入点跟着搬，别再对着 pgsink 打。
    dict(
        name="forced-recopy-neutered",
        tier="engine",
        file="internal/engine/engine.go",
        find="func (r *runner) applyForcedRecopy(plan *chunkPlan) {\n",
        replace="func (r *runner) applyForcedRecopy(plan *chunkPlan) {\n\treturn // SABOTAGE: 只清块标记，不接进本轮重搬\n",
        cmd=["go", "test", "-count=1", "-run", "^TestApplyForcedRecopy", "./internal/engine/"],
        requires="TestApplyForcedRecopyRescuesSingleTableCopySkip",
        signal="TestApplyForcedRecopy* FAIL（只清块标记不重搬=恢复永远差一段，还会说「本轮已补齐」）",
        note="把「欠一次整表重搬」变成只清块标记（单表会落到 copySkip，搬运阶段整个不跑）",
    ),
    dict(
        name="commit-plan-alias",
        tier="engine",
        file="internal/engine/engine.go",
        find="\tr.applyForcedRecopy(plan)\n\tr.plan = *plan\n",
        replace="\tr.plan = *plan\n\tr.applyForcedRecopy(plan) // SABOTAGE: 先赋值再补计划，r.plan 拿到旧值\n",
        cmd=["go", "test", "-count=1", "-run", "^TestApplyForcedRecopy", "./internal/engine/"],
        requires="TestApplyForcedRecopyOnlyTouchesThePausedTable",
        signal="TestApplyForcedRecopy* FAIL（值类型别名：r.plan 是改之前的计划，调用方那份才是新的）",
        note="commitPlan 先落值再补计划（指针也救不了：复制发生在补计划之前）",
    ),
    dict(
        name="reconcile-clears-early",
        tier="engine",
        file="internal/engine/engine.go",
        find="\t\tif r.forceRecopy == nil {\n",
        replace="\t\t_ = r.sink.ClearPausedTable(ctx, ref) // SABOTAGE: 对齐就解除（旧判据）\n\t\tr.forgetPaused(key)\n\t\tif r.forceRecopy == nil {\n",
        cmd=["go", "test", "-tags=integration", "-count=1", "-run", "^TestReconcilePausedMarksRecopyAndKeepsRecord$", "./internal/engine/"],
        requires="TestReconcilePausedMarksRecopyAndKeepsRecord",
        signal="TestReconcilePausedMarksRecopyAndKeepsRecord FAIL（结构对齐就解除=跳过窗口永远没人补，表显示已恢复而数据是旧的）",
        note="reconcilePaused 改回「结构对齐就解除」（契约 §1.6 第 4 步：解除要等重搬搬成）",
    ),
]

COPY_EXCLUDE = {".git", ".localdb", "dist", ".e2e-evidence", "node_modules"}


def make_shadow():
    """把工作区复制成影子树：注入只发生在影子里，工作区一个字节都不改。"""
    shadow = tempfile.mkdtemp(prefix="mutation-")
    def ignore(d, names):
        return [n for n in names if n in COPY_EXCLUDE]
    shutil.copytree(ROOT, shadow, dirs_exist_ok=True, ignore=ignore, symlinks=True)
    return shadow


def apply_mutation(shadow, m):
    path = os.path.join(shadow, m["file"])
    if not os.path.exists(path):
        return False, "文件不存在：%s" % m["file"]
    with open(path, encoding="utf-8") as f:
        src = f.read()
    hits = src.count(m["find"])
    if hits != 1:
        return False, "注入点在 %s 里命中 %d 处（要求恰好 1 处）" % (m["file"], hits)
    with open(path, "w", encoding="utf-8") as f:
        f.write(src.replace(m["find"], m["replace"], 1))
    return True, "已注入"


def test_present(shadow, name):
    if not name:
        return True
    for base, _, files in os.walk(shadow):
        for fn in files:
            if not fn.endswith("_test.go"):
                continue
            try:
                with open(os.path.join(base, fn), encoding="utf-8", errors="ignore") as f:
                    if ("func %s(" % name) in f.read():
                        return True
            except OSError:
                pass
    return False


def run(m, shadow, env):
    proc = subprocess.run(m["cmd"], cwd=shadow, env=env, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, text=True)
    tail = [l for l in proc.stdout.splitlines() if l.strip()][-4:]
    return proc.returncode, tail


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--tier", choices=["logic", "engine", "all"], default="logic")
    ap.add_argument("--only", default="")
    ap.add_argument("--json", default="")
    ap.add_argument("--keep", action="store_true", help="保留影子树（排查用）")
    a = ap.parse_args(argv)

    todo = [m for m in MUTATIONS
            if (a.tier == "all" or m["tier"] == a.tier)
            and (not a.only or m["name"] == a.only)]
    if not todo:
        print("mutation-gate: 没有匹配的注入项（--tier %s --only %r）" % (a.tier, a.only), file=sys.stderr)
        return 2

    env = dict(os.environ)
    if a.tier in ("engine", "all") and not env.get("JINLIANG_TEST_PG_DSN"):
        print("mutation-gate: engine 档要真库（缺 JINLIANG_TEST_PG_DSN）", file=sys.stderr)
        return 2

    shadow = make_shadow()
    rows, failed = [], 0
    try:
        for m in todo:
            ok, why = apply_mutation(shadow, m)
            if not ok:
                rows.append(dict(m, verdict="INJECT-FAILED", detail=why, rc=None))
                failed += 1
                print("mutation-gate: FAIL %-28s 注入失败：%s" % (m["name"], why))
                continue
            if m.get("requires") and not test_present(shadow, m["requires"]):
                rows.append(dict(m, verdict="MISSING-TEST", detail="检测器用例不存在：" + m["requires"], rc=None))
                failed += 1
                print("mutation-gate: FAIL %-28s 检测器用例不存在（%s）—— 先合「%s」再收门禁"
                      % (m["name"], m["requires"], m["name"]))
                continue
            rc, tail = run(m, shadow, env)
            killed = rc != 0
            rows.append(dict(m, verdict="KILLED" if killed else "SURVIVED", detail=" | ".join(tail), rc=rc))
            if not killed:
                failed += 1
            print("mutation-gate: %-8s %-28s rc=%s  %s" %
                  ("killed" if killed else "SURVIVED", m["name"], rc, tail[-1] if tail else ""))
            # 每条都从干净影子开始，避免上一条的注入影响下一条。
            shutil.rmtree(shadow); shadow = make_shadow()
    finally:
        if not a.keep:
            shutil.rmtree(shadow, ignore_errors=True)

    # 结论表
    print("\n注入点 | 名称 | 期望 | 实际 | 结论")
    for r in rows:
        print("%s | %s | %s | rc=%s %s | %s" %
              (r["file"], r["name"], r["signal"], r["rc"], (r["detail"] or "")[:80], r["verdict"]))
    if a.json:
        with open(a.json, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
        print("报告：%s" % a.json)
    if failed:
        print("mutation-gate: %d/%d 条注入存活（门禁拦不住这种破坏）" % (failed, len(rows)))
        return 1
    print("mutation-gate: %d/%d 条注入全部被抓住" % (len(rows), len(rows)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
