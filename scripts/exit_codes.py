#!/usr/bin/env python3
"""退出码口径：**「跑完了但有欠账」是 3，不是失败**（Panel 裁决，基于 `c72f36c`）。

派生单点（顺序不可换）：
    失败 → 1  >  用户停止 → 130  >  正常收尾且有暂停表 → 3  >  0

为什么要单独一个文件：`b1_pg_to_mysql.sh` 原来用文本 `grep "退出码 1|阶段 failed"` 判失败，
**暂停场景（3）会被漏报**——用户看到「一切正常」，其实有表被暂停了。口径只写一处，
脚本和门禁都用它，别再各写一套 if。

用法：
    exit_codes.py classify --code 3 [--log run.log]   # 打印 ok|failed|stopped|needs_action|unknown
    exit_codes.py needs-action --code 3               # 退出 0=是欠账 / 1=不是
    exit_codes.py selftest
"""

import argparse
import re
import sys

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_NEEDS_ACTION = 3
EXIT_STOPPED = 130

KIND = {
    EXIT_OK: "ok",
    EXIT_FAILED: "failed",
    EXIT_NEEDS_ACTION: "needs_action",
    EXIT_STOPPED: "stopped",
}

# 日志里那一行的形态：`结果：阶段 done，复制 12 行，…，退出码 3`
LOG_CODE = re.compile(r"退出码\s*(\d+)")


def classify(code=None, log=""):
    """按「失败 1 > 停止 130 > 欠账 3 > 0」判；code 缺失时从日志里读。

    返回 (kind, why)。**日志里明写「阶段 failed」时优先判失败**：那是终态事实，
    比退出码更能说明发生了什么（两者理论上一致，不一致时以更严重的为准）。
    """
    if code is None:
        m = LOG_CODE.search(log or "")
        code = int(m.group(1)) if m else None
    if "阶段 failed" in (log or ""):
        return "failed", "日志终态是 failed"
    if code is None:
        return "unknown", "读不到退出码（没有 code，也没有日志里的「退出码 N」）"
    kind = KIND.get(code)
    if kind is None:
        return "unknown", "未归档的退出码 %d" % code
    why = {
        "ok": "正常收尾",
        "failed": "失败",
        "needs_action": "跑完了但有欠账（有暂停表）——**不是失败**",
        "stopped": "用户停止",
    }[kind]
    return kind, why


SELFTEST = [
    ("0 = 正常", dict(code=0), "ok"),
    ("1 = 失败", dict(code=1), "failed"),
    ("130 = 用户停止", dict(code=130), "stopped"),
    ("3 = 有欠账（不是失败）", dict(code=3), "needs_action"),
    ("日志里 3、code 缺失", dict(log="结果：阶段 done，复制 5 行，退出码 3"), "needs_action"),
    ("日志终态 failed 优先", dict(code=0, log="结果：阶段 failed，退出码 0"), "failed"),
    ("读不到", dict(log="什么都没写"), "unknown"),
    ("未归档 7", dict(code=7), "unknown"),
]


def selftest():
    bad = 0
    for name, kw, want in SELFTEST:
        got, why = classify(**kw)
        if got != want:
            bad += 1
            print("selftest: FAIL %s —— 期望 %s，实际 %s（%s）" % (name, want, got, why))
        else:
            print("selftest: ok   %s" % name)
    if bad:
        print("selftest: %d/%d 条不符合预期" % (bad, len(SELFTEST)))
        return 1
    print("selftest: %d 条口径全部符合预期" % len(SELFTEST))
    return 0


def main(argv):
    ap = argparse.ArgumentParser(prog="exit_codes.py")
    sub = ap.add_subparsers(dest="cmd")
    c = sub.add_parser("classify")
    c.add_argument("--code", type=int, default=None)
    c.add_argument("--log", default="")
    c.add_argument("--kind-only", action="store_true", help="只打印 ok|failed|needs_action|stopped|unknown")
    n = sub.add_parser("needs-action")
    n.add_argument("--code", type=int, required=True)
    sub.add_parser("selftest")
    a = ap.parse_args(argv)

    if a.cmd == "selftest":
        return selftest()
    if a.cmd == "classify":
        log = ""
        if a.log:
            with open(a.log, encoding="utf-8", errors="ignore") as f:
                log = f.read()
        kind, why = classify(a.code, log)
        if a.kind_only:
            print(kind)
        else:
            print("%s：%s" % (kind, why))
        return 0 if kind in ("ok", "needs_action", "stopped") else 1
    if a.cmd == "needs-action":
        return 0 if a.code == EXIT_NEEDS_ACTION else 1
    ap.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
