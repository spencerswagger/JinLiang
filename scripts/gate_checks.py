#!/usr/bin/env python3
"""门禁判据（纯函数 + 命令行）：把「通过/拦截」的判断从 shell 里拿出来。

为什么单独一个文件：门禁最怕的不是「没跑」，而是**判据被悄悄改松**——
把 `!=` 改成 `==`、把 `if 不等` 改成 `if False`，脚本照样退出 0，谁都看不出来。
所以判据集中在这里，由 `selftest` 自带一组「必须红」的输入；`scripts/mutation_gate.py`
再往这些判据上**故意注入破坏**，确认 selftest 真的会红（见 notes/merge-gate-59.md）。

用法（脚本里用；失败即退出 1）：
    gate_checks.py selftest
    gate_checks.py fp   --src "20000:aa..." --dst "20000:aa..." [--label 组合]
    gate_checks.py recopy --copied 0
    gate_checks.py pause --before "postgres:v1:0/1" --after "postgres:v1:0/1" [--keys a,b] [--want a]
    gate_checks.py rows --src 20000 --dst 20000 --expect 20000

退出码：0 = 判据成立（放行）/ 1 = 判据不成立（拦截）/ 2 = 参数不成立（也是拦截）。
"""

import argparse
import sys


# ── 判据（纯函数） ─────────────────────────────────────────────

def parse_fp(text):
    """把 `<行数>:<md5>` 解析成 (行数, md5)。形态不对返回 None。

    空指纹（`-`、空串）一律视为「没算出来」，不是「一致」——这是第三次踩
    「拿不到数据就放行」（B5 reltuples=-1、verify 抽样、pr_files 为空），判据倒向拒绝。
    """
    if text is None:
        return None
    text = text.strip()
    if ":" not in text:
        return None
    n, digest = text.split(":", 1)
    n, digest = n.strip(), digest.strip()
    if not n.isdigit():
        return None
    if len(digest) < 8 or digest == "-":
        return None
    return int(n), digest


def fp_ok(src, dst):
    a, b = parse_fp(src), parse_fp(dst)
    if a is None or b is None:
        return False, "指纹没算出来（源=%r 目标=%r）：拿不到数据就拦截，不当成一致" % (src, dst)
    if a[0] != b[0]:
        return False, "行数不一致：源 %d / 目标 %d" % (a[0], b[0])
    if a[1] != b[1]:
        return False, "内容指纹不一致：源 %s / 目标 %s" % (a[1], b[1])
    return True, "%d 行，内容指纹一致 %s" % (a[0], a[1])


def recopy_ok(copied, allow=0):
    """崩溃重启后**重搬行数**必须是 0（或显式允许的块内回滚行数）。"""
    if copied < 0:
        return False, "重搬行数读不出来（%d）：读不到就拦截" % copied
    if copied > allow:
        return False, "重启后重搬了 %d 行（允许 %d）：这是整轮重搬，不是位点续跑" % (copied, allow)
    return True, "重启后重搬 %d 行（允许 %d）" % (copied, allow)


def pause_ok(before, after, keys=None, want=None):
    """按表暂停的三条硬判据：位点不动、只登记该表、登记键与块标记同源。"""
    if before is None or after is None:
        return False, "位点读不出来（前=%r 后=%r）：读不到就拦截" % (before, after)
    if before != after:
        return False, "暂停期间位点动了：前 %r / 后 %r（跳过=静默丢数据）" % (before, after)
    if want is not None:
        got = sorted(x for x in (keys or "").split(",") if x)
        exp = sorted(x for x in want.split(",") if x)
        if got != exp:
            return False, "暂停登记的表不对：实际 %s / 期望 %s" % (got, exp)
    return True, "位点不动（%r），暂停登记 %s" % (before, keys or "-")


def rows_ok(src, dst, expect):
    if src < 0 or dst < 0 or expect < 0:
        return False, "行数读不出来（源=%d 目标=%d 期望=%d）" % (src, dst, expect)
    if src != expect:
        return False, "源端 %d 行，期望 %d 行" % (src, expect)
    if dst != expect:
        return False, "目标端 %d 行，期望 %d 行" % (dst, expect)
    return True, "%d 行" % expect


# ── selftest：每个判据都要有「必须红」的输入 ─────────────────────

SELFTEST = [
    # (描述, 实际结果, 期望是否通过)
    ("fp 相同", fp_ok("20000:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "20000:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), True),
    ("fp md5 不同", fp_ok("20000:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "20000:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), False),
    ("fp 行数不同", fp_ok("19999:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "20000:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), False),
    ("fp 空指纹", fp_ok("", "20000:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), False),
    ("fp 占位符", fp_ok("20000:-", "20000:-"), False),
    ("fp 缺冒号", fp_ok("20000aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "20000:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), False),
    ("recopy 0", recopy_ok(0), True),
    ("recopy 1", recopy_ok(1), False),
    ("recopy 整轮", recopy_ok(20000), False),
    ("recopy 读不到", recopy_ok(-1), False),
    ("pause 位点不动", pause_ok("pos1", "pos1", "a", "a"), True),
    ("pause 位点动了", pause_ok("pos1", "pos2", "a", "a"), False),
    ("pause 多登记了表", pause_ok("pos1", "pos1", "a,b", "a"), False),
    ("pause 少登记了表", pause_ok("pos1", "pos1", "", "a"), False),
    ("pause 位点读不到", pause_ok(None, "pos1", "a", "a"), False),
    ("rows 对上", rows_ok(20000, 20000, 20000), True),
    ("rows 源少一行", rows_ok(19999, 20000, 20000), False),
    ("rows 目标多一行", rows_ok(20000, 20001, 20000), False),
    ("rows 读不到", rows_ok(-1, 20000, 20000), False),
]


def selftest():
    bad = 0
    for name, (ok, why), want in SELFTEST:
        if ok != want:
            bad += 1
            print("selftest: FAIL %s —— 期望 %s，实际 %s（%s）" %
                  (name, "通过" if want else "拦截", "通过" if ok else "拦截", why))
        else:
            print("selftest: ok   %s" % name)
    if bad:
        print("selftest: %d/%d 条判据不符合预期" % (bad, len(SELFTEST)))
        return 1
    print("selftest: %d 条判据全部符合预期" % len(SELFTEST))
    return 0


# ── 命令行 ────────────────────────────────────────────────────

def main(argv):
    if not argv or argv[0] in ("-h", "--help"):
        print(__doc__)
        return 2
    cmd, rest = argv[0], argv[1:]
    if cmd == "selftest":
        return selftest()

    p = argparse.ArgumentParser(prog="gate_checks.py " + cmd)
    p.add_argument("--label", default="")
    if cmd == "fp":
        p.add_argument("--src", required=True)
        p.add_argument("--dst", required=True)
    elif cmd == "recopy":
        p.add_argument("--copied", type=int, required=True)
        p.add_argument("--allow", type=int, default=0)
    elif cmd == "pause":
        p.add_argument("--before", required=True)
        p.add_argument("--after", required=True)
        p.add_argument("--keys", default=None)
        p.add_argument("--want", default=None)
    elif cmd == "rows":
        p.add_argument("--src", type=int, required=True)
        p.add_argument("--dst", type=int, required=True)
        p.add_argument("--expect", type=int, required=True)
    else:
        print("未知子命令 %r" % cmd, file=sys.stderr)
        return 2
    a = p.parse_args(rest)

    if cmd == "fp":
        ok, why = fp_ok(a.src, a.dst)
    elif cmd == "recopy":
        ok, why = recopy_ok(a.copied, a.allow)
    elif cmd == "pause":
        ok, why = pause_ok(a.before, a.after, a.keys, a.want)
    else:
        ok, why = rows_ok(a.src, a.dst, a.expect)

    label = ("[%s] " % a.label) if getattr(a, "label", "") else ""
    print(("gate: OK   " if ok else "gate: FAIL ") + label + why)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
