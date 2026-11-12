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
    gate_checks.py pause --keys a,b --want a
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
    """【整轮口径】崩溃重启后**整轮重搬行数**必须是 0（`rowsCopied==0 ∧ plan.Kind==copySkip`）。

    口径定义见契约 §「0 重搬的三口径」：整轮 / 单表 / 暂停恢复互不替代。
    这个函数只服务**整轮**：它回答的是「这次重启有没有重新搬存量」。
    """
    if copied < 0:
        return False, "重搬行数读不出来（%d）：读不到就拦截" % copied
    if copied > allow:
        return False, "[整轮口径] 重启后重搬了 %d 行（允许 %d）：这是整轮重搬，不是位点续跑" % (copied, allow)
    return True, "[整轮口径] 重启后整轮重搬 %d 行（允许 %d）" % (copied, allow)


def recopy_table_ok(resumed, remaining, chunk, allow_chunks=1):
    """【单表口径】块级续跑：只允许「中断时正在写的那一块」回滚重来。

    判据来自契约：`chunk_no=0` 仍在（表还没完成）∧ `plan.From[table]` 非空。
    在脚本里能观测到的是它的**结果**：重启搬的行数 ≈ 未搬完的部分（±1 块）。
    `plan.From` 的非空由单测 `TestPlanCopySingleTableResumeMetric` 直接钉住。
    """
    if min(resumed, remaining, chunk) < 0:
        return False, "[单表口径] 行数读不出来（重启=%d 未搬完=%d 块=%d）：读不到就拦截" % (resumed, remaining, chunk)
    lo = max(0, remaining - chunk * allow_chunks)
    hi = remaining + chunk * allow_chunks
    if not (lo <= resumed <= hi):
        return False, ("[单表口径] 重启搬了 %d 行，未搬完的是 %d 行（±%d 块×%d 行）："
                       "超出就是整表重搬，不是块级续跑" % (resumed, remaining, allow_chunks, chunk))
    return True, "[单表口径] 重启搬 %d 行 ≈ 未搬完 %d 行（±%d 块）" % (resumed, remaining, allow_chunks)


def recopy_pause_ok(recopied, table_rows):
    """【暂停恢复口径】该表重搬行数 == 该表行数，**允许 ≠ 0**。

    这是与「重启后重搬 0 行」完全不同的度量：暂停恢复就是要整表重搬补齐跳过窗口，
    所以这里判的是「搬够了」，不是「没搬」。
    """
    if recopied < 0 or table_rows < 0:
        return False, "[暂停恢复口径] 行数读不出来（重搬=%d 表行数=%d）" % (recopied, table_rows)
    if recopied != table_rows:
        return False, ("[暂停恢复口径] 该表重搬 %d 行，表里应有 %d 行：恢复必须整表搬齐"
                       "（本口径允许非 0，但必须相等）" % (recopied, table_rows))
    return True, "[暂停恢复口径] 该表重搬 %d 行 = 表行数（本口径允许非 0）" % recopied


def pause_ok(keys=None, want=None):
    """按表暂停的判据：**只登记该表**（不多不少）。

    位点这一条 2026-11-10 晚被裁决 A 改掉了：暂停表 = 只跳过这张表、其它表照写、
    位点照常前进，恢复时该表整表重搬（`internal/engine/pause_skip.go`）。
    所以「暂停期间位点必须不动」是**旧契约**，写进判据会在新实现上变成假红——
    这正是本文件开头说的：判据必须跟着契约定，而不是跟着记忆定。
    """
    if want is not None:
        got = sorted(x for x in (keys or "").split(",") if x)
        exp = sorted(x for x in want.split(",") if x)
        if got != exp:
            return False, "暂停登记的表不对：实际 %s / 期望 %s" % (got, exp)
    return True, "暂停登记 %s（不多不少）" % (keys or "-")


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
    ("单表口径 只补未完成", recopy_table_ok(16000, 16000, 2000), True),
    ("单表口径 允许 1 块回滚", recopy_table_ok(18000, 16000, 2000), True),
    ("单表口径 整表重搬", recopy_table_ok(20000, 16000, 2000), False),
    ("单表口径 少搬", recopy_table_ok(10000, 16000, 2000), False),
    ("暂停恢复 相等（非 0）", recopy_pause_ok(251, 251), True),
    ("暂停恢复 少搬", recopy_pause_ok(200, 251), False),
    ("暂停恢复 多搬", recopy_pause_ok(300, 251), False),
    ("pause 只登记该表", pause_ok("a", "a"), True),
    ("pause 多登记了表", pause_ok("a,b", "a"), False),
    ("pause 少登记了表", pause_ok("", "a"), False),
    ("rows 对上", rows_ok(20000, 20000, 20000), True),
    ("rows 源少一行", rows_ok(19999, 20000, 20000), False),
    ("rows 目标多一行", rows_ok(20000, 20001, 20000), False),
    ("rows 读不到", rows_ok(-1, 20000, 20000), False),
]


# 条数下限：删掉「必须红」的用例 = 悄悄放宽判据（Owl II 实测：删掉 fp 占位符那两条，
# mutation_gate --tier logic 仍然 9/9 绿）。少一条就判失败。
MIN_CASES = 24


def selftest():
    bad = 0
    if len(SELFTEST) < MIN_CASES:
        bad += 1
        print("selftest: FAIL 判据条数 %d < 下限 %d —— 删用例等于放宽判据" % (len(SELFTEST), MIN_CASES))
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
        p.add_argument("--scope", choices=["whole", "table", "pause"], default="whole",
                       help="整轮 / 单表 / 暂停恢复——三个互不替代的度量")
        p.add_argument("--copied", type=int, default=None)
        p.add_argument("--allow", type=int, default=0)
        p.add_argument("--resumed", type=int, default=None)
        p.add_argument("--remaining", type=int, default=None)
        p.add_argument("--chunk", type=int, default=None)
        p.add_argument("--recopied", type=int, default=None)
        p.add_argument("--table-rows", dest="table_rows", type=int, default=None)
    elif cmd == "pause":
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
        if a.scope == "whole":
            if a.copied is None:
                print("gate: FAIL [整轮口径] 缺 --copied", file=sys.stderr)
                return 2
            ok, why = recopy_ok(a.copied, a.allow)
        elif a.scope == "table":
            if None in (a.resumed, a.remaining, a.chunk):
                print("gate: FAIL [单表口径] 缺 --resumed/--remaining/--chunk", file=sys.stderr)
                return 2
            ok, why = recopy_table_ok(a.resumed, a.remaining, a.chunk)
        else:
            if None in (a.recopied, a.table_rows):
                print("gate: FAIL [暂停恢复口径] 缺 --recopied/--table-rows", file=sys.stderr)
                return 2
            ok, why = recopy_pause_ok(a.recopied, a.table_rows)
    elif cmd == "pause":
        ok, why = pause_ok(a.keys, a.want)
    else:
        ok, why = rows_ok(a.src, a.dst, a.expect)

    label = ("[%s] " % a.label) if getattr(a, "label", "") else ""
    print(("gate: OK   " if ok else "gate: FAIL ") + label + why)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
