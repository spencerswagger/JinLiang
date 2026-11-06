"""review-gate 的判定逻辑（可单测；脚本只负责取数据）。

为什么只能靠正文 + state 硬否决：我们从不点 GitHub 的 APPROVE（Owl 用
COMMENTED 发结论），所以 COMMENTED 不能当「非放行」；能硬判的只有
DISMISSED / CHANGES_REQUESTED。正文匹配必须「先否决词、再放行词，且放行词
出现在行首结论行」——否则「不可合并」「上一轮必改 0，本轮必改 2 条」都会误放行。
"""
import json
import re
import sys

DENY = re.compile(r"不可合并|不予放行|不通过|必须修|必改\s*[1-9]")
APPROVE = re.compile(r"(?m)^[*_>#\-\s]*(结论[:：]\s*)?[*_>#\-\s]*(可合并|准予合并|放行|必改\s*0)")
ADMIN_HINT = re.compile(r"--admin|admin\s*合并|管理员绕过|bypass", re.IGNORECASE)


def _summary(body, n=80):
    return re.sub(r"\s+", " ", (body or "").strip())[:n] or "（无正文）"


def decide_review(pr, pr2, reviews, compare=None, pr_files=None):
    head = (pr.get("head") or {}).get("sha") or ""
    head2 = (pr2.get("head") or {}).get("sha") or ""
    if not head:
        return False, "读不到 PR 的 head", "head_missing"
    if head2 != head:
        return False, f"检查期间 head 变了（{head[:12]} -> {head2[:12]}），请重新触发", "head_race"
    if not reviews:
        return False, "PR 还没有 review（CI 绿不等于可合并）", "no_review"

    latest = max(reviews, key=lambda r: (r.get("submitted_at") or "", r.get("id") or 0))
    body = latest.get("body") or ""
    state = (latest.get("state") or "").upper()
    commit = latest.get("commit_id") or ""
    who = (latest.get("user") or {}).get("login") or "?"
    summary = _summary(body)

    if state in ("DISMISSED", "CHANGES_REQUESTED"):
        return False, f"最新 review 是 {state}（{who}）：{summary}", "state_veto"
    # 否决词先于 head：一个锚在旧 head 的「必改 2 条」应该以「必改 2」被拒，
    # 而不是被 head 不匹配掩盖（两者都失败，但原因要指向真正的问题）。
    # 否决词只看**首行结论**：多轮正文后面引用「上轮必改 1 条」不代表本轮否决
    # （#31 就是首行可合并、末尾提到历史必改 1；全文匹配会误杀）。
    first_line = next((ln for ln in body.splitlines() if ln.strip()), "")
    if DENY.search(first_line):
        return False, f"首行结论含否决词（不可合并/不通过/必改 N 等）：{summary}", "deny_word"
    # 锚定判据不再是「commit == head」（strict 下 merge main 必然死锁），而是：
    #   1) review.commit 是 head 的祖先；
    #   2) review..head 的差异不包含本 PR 改的文件（只允许别人已合的改动/merge main）。
    # 任何本 PR 的新改动（哪怕一行注释）都要求重审。
    anchor_note = ""
    if commit != head:
        if not compare or compare.get("status") != "ahead":
            return False, (
                f"结论 {commit[:12]} 不是当前 head {head[:12]} 的祖先"
                f"（head_mismatch；status={(compare or {}).get('status', 'unknown')}；rebase/force push 后必须重审）"
            ), "head_mismatch"
        changed = {f.get("filename") for f in (compare.get("files") or []) if f.get("filename")}
        owned = {n for n in (pr_files or []) if n}
        # 输入缺失必须倒向拒绝：空清单时「未触及本 PR 文件」这句话是假的
        # （今晚第三次「拿不到数据就放行」——B5 reltuples=-1、verify 抽样、这里）。
        if not owned:
            return False, (
                "读不到本 PR 的文件清单（输入为空），无法判定 review..head 是否只含 merge main；"
                "按纪律拒绝而不是放行"
            ), "pr_files_missing"
        touched = sorted(changed & owned)
        if touched:
            return False, (
                f"结论早于本 PR 的代码改动（stale_conclusion，review {commit[:12]} → head {head[:12]}）："
                f"需要重审；涉及 {', '.join(touched[:5])}{'…' if len(touched) > 5 else ''}"
            ), "stale_conclusion"
        anchor_note = "（head 前进仅来自 merge main / 别人已合的改动，未触及本 PR 文件）"
    if not APPROVE.search(body):
        return False, f"最新结论不是行首放行（reviewer={who}，state={state}）：{summary}", "not_approved"
    return True, f"head={head[:12]} reviewer={who}：{summary}{anchor_note}", "pass"


def decide_admin(pr, commit, comments):
    merge_sha = pr.get("merge_commit_sha") or ""
    if not merge_sha:
        return True, "还没合并（admin 检查跳过）", "not_merged"
    base = (pr.get("base") or {}).get("sha") or ""
    parents = [p.get("sha") or "" for p in (commit.get("parents") or [])]
    who = (pr.get("merged_by") or {}).get("login") or "?"
    if not parents:
        return True, "合并提交没有 parent，无法判断是否 --admin", "unknown"
    if parents[0] == base:
        return True, f"正常合并（base 与合并 parent 一致，by={who}）", "normal"

    for c in comments:
        body = c.get("body") or ""
        if ADMIN_HINT.search(body):
            return True, (
                f"疑似 --admin 合并，但已有说明（{(c.get('user') or {}).get('login') or '?'}）："
                f"{_summary(body, 100)}"
            ), "admin_documented"
    return False, (
        f"合并时 base 已前进（base {base[:12]}，合并 parent {parents[0][:12]}，"
        f"merge {merge_sha[:12]}，by={who}）——疑似 --admin 绕过；"
        f"PR 上没有说明理由的 comment（需含 --admin 与原因）"
    ), "admin_undocumented"


def _load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def selftest():
    head = "a" * 40
    pr = {"head": {"sha": head}}
    pr2 = {"head": {"sha": head}}
    old = "b" * 40
    cases = [
        ("approval-bold", {"body": "**可合并（必改 0）**，head x", "state": "COMMENTED", "commit_id": head}, None, None, True, "pass"),
        ("not-mergeable", {"body": "不可合并", "state": "COMMENTED", "commit_id": head}, None, None, False, "deny_word"),
        ("not-released", {"body": "不予放行", "state": "COMMENTED", "commit_id": head}, None, None, False, "deny_word"),
        ("history-zero", {"body": "上一轮必改 0，本轮必改 2 条", "state": "COMMENTED", "commit_id": head}, None, None, False, "deny_word"),
        ("mustfix-2", {"body": "**必改 2 条**（都在地基上）", "state": "COMMENTED", "commit_id": head}, None, None, False, "deny_word"),
        ("changes-requested", {"body": "**可合并（必改 0）**", "state": "CHANGES_REQUESTED", "commit_id": head}, None, None, False, "state_veto"),
        ("merge-main-only", {"body": "**可合并（必改 0）**", "state": "COMMENTED", "commit_id": old},
         {"status": "ahead", "files": [{"filename": "internal/other.go"}]}, ["cmd/x.go"], True, "pass"),
        ("pr-file-changed", {"body": "**可合并（必改 0）**", "state": "COMMENTED", "commit_id": old},
         {"status": "ahead", "files": [{"filename": "cmd/x.go"}]}, ["cmd/x.go"], False, "stale_conclusion"),
        ("rebase-not-ancestor", {"body": "**可合并（必改 0）**", "state": "COMMENTED", "commit_id": old},
         {"status": "diverged", "files": []}, ["cmd/x.go"], False, "head_mismatch"),
        ("pr-files-missing", {"body": "**可合并（必改 0）**", "state": "COMMENTED", "commit_id": old},
         {"status": "ahead", "files": [{"filename": "internal/other.go"}]}, [], False, "pr_files_missing"),
        ("missing-prfiles-empty", {"body": "**可合并（必改 0）**", "state": "COMMENTED", "commit_id": old},
         {"status": "ahead", "files": [{"filename": "internal/other.go"}]}, [], False, "pr_files_missing"),
        ("missing-prfiles-none", {"body": "**可合并（必改 0）**", "state": "COMMENTED", "commit_id": old},
         {"status": "ahead", "files": [{"filename": "internal/other.go"}]}, None, False, "pr_files_missing"),
        ("approval-conclusion", {"body": "结论：可合并", "state": "COMMENTED", "commit_id": head}, None, None, True, "pass"),
        ("history-in-tail", {"body": "**可合并（必改 0）**，按 head x\n\n…上轮必改 1 条已修…", "state": "COMMENTED", "commit_id": head}, None, None, True, "pass"),
    ]
    ok_all = True
    for name, review, compare, prfiles, want_ok, want_cat in cases:
        review.setdefault("user", {"login": "probe"})
        review.setdefault("submitted_at", "2026-01-01T00:00:00Z")
        review.setdefault("id", 1)
        ok, msg, cat = decide_review(pr, pr2, [review], compare, prfiles)
        good = ok == want_ok and cat == want_cat
        ok_all = ok_all and good
        print(f"{'PASS' if good else 'FAIL'} {name}: cat={cat} ok={ok} :: {msg}")
    return 0 if ok_all else 1


def main():
    if len(sys.argv) < 2:
        sys.exit("用法: review_gate_logic.py review|admin|selftest ...")
    mode = sys.argv[1]
    if mode == "selftest":
        sys.exit(selftest())
    if mode == "review":
        if len(sys.argv) != 7:
            sys.exit("review 需要 pr.json pr2.json reviews.json compare.json prfiles.json")
        compare = _load(sys.argv[5]) if _load(sys.argv[5]) else {}
        prfiles = _load(sys.argv[6]) if _load(sys.argv[6]) else []
        ok, msg, cat = decide_review(_load(sys.argv[2]), _load(sys.argv[3]), _load(sys.argv[4]), compare, prfiles)
    elif mode == "admin":
        if len(sys.argv) != 5:
            sys.exit("admin 需要 pr.json commit.json comments.json")
        ok, msg, cat = decide_admin(_load(sys.argv[2]), _load(sys.argv[3]), _load(sys.argv[4]))
    else:
        sys.exit(f"不认识 mode {mode}")
    print(("review-gate: PASS " if ok else "review-gate: FAIL ") + msg, file=sys.stdout if ok else sys.stderr)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
