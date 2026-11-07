"""review-gate 的判定逻辑（可单测；脚本只负责取数据）。

为什么只能靠正文 + state 硬否决：我们从不点 GitHub 的 APPROVE（Owl 用
COMMENTED 发结论），所以 COMMENTED 不能当「非放行」；能硬判的只有
DISMISSED / CHANGES_REQUESTED。正文匹配必须「先否决词、再放行词，且放行词
出现在行首结论行」——否则「不可合并」「上一轮必改 0，本轮必改 2 条」都会误放行。

锚定不是「commit == head」（strict 下 merge main 必然死锁）：
  1) review commit 是 head 祖先；
  2) review..head 的差异不包含「本 PR 的生产代码」；测试文件允许改，但新增行
     必须全部能在 origin/main 同路径找到（挡住「顺手加一条永不失败的用例」）。
  3) 只有 merge main / 解冲突时才允许 head 前进；失败类别分 stale_conclusion
     （本 PR 代码/测试新增行又变了）与 head_mismatch（不是祖先）。
"""
import json
import re
import subprocess
import sys

DENY = re.compile(r"不可合并|不予放行|不通过|必须修|必改\s*[1-9]")
APPROVE = re.compile(r"(?m)^[*_>#\-\s]*(结论[:：]\s*)?[*_>#\-\s]*(可合并|准予合并|放行|必改\s*0)")
ADMIN_HINT = re.compile(r"--admin|admin\s*合并|管理员绕过|bypass", re.IGNORECASE)


def _summary(body, n=80):
    return re.sub(r"\s+", " ", (body or "").strip())[:n] or "（无正文）"


def _git(*args, cwd="."):
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True)


def _main_lines(path, cwd="."):
    """Return the set of lines of path in origin/main (empty if absent)."""
    r = _git("show", f"origin/main:{path}", cwd=cwd)
    if r.returncode != 0:
        return set()
    return set(r.stdout.splitlines())


def compute_anchor(review, head, pr_files, cwd="."):
    """Compute whether head only moved by merge main / conflict resolution.

    Returns a dict consumed by decide_review. Missing/unreadable inputs are
    represented explicitly so the decision layer can fail closed.
    """
    out = {"ancestor": False, "touched": [], "production_touched": [],
           "test_touched": [], "test_disallowed": [], "pr_files_missing": False,
           "compare_files_missing": False, "compare_files_truncated": False,
           "changed_count": 0, "error": ""}
    if not review or not head:
        out["error"] = "缺少 review 或 head"
        return out
    owned = {p for p in (pr_files or []) if p}
    # 早退时 ancestor 故意保持 false：decision 层先判 pr_files_missing，
    # 否则真实路径会被误归因成「不是祖先，请 rebase」（selftest 用同样形状）。
    if not owned:
        out["pr_files_missing"] = True
        return out

    # 确保两个 commit 与 origin/main 都在本地（Actions 用 fetch-depth: 0）。
    for ref in (review, head):
        if _git("rev-parse", "--verify", f"{ref}^{{commit}}", cwd=cwd).returncode != 0:
            _git("fetch", "--no-tags", "origin", ref, cwd=cwd)
    if _git("rev-parse", "--verify", "origin/main^{commit}", cwd=cwd).returncode != 0:
        _git("fetch", "--no-tags", "origin", "main:refs/remotes/origin/main", cwd=cwd)

    if _git("merge-base", "--is-ancestor", review, head, cwd=cwd).returncode != 0:
        out["error"] = "review commit 不是 head 的祖先"
        return out
    out["ancestor"] = True

    r = _git("diff", "--name-only", f"{review}..{head}", cwd=cwd)
    if r.returncode != 0:
        out["error"] = "git diff 失败: " + r.stderr.strip()
        return out
    changed = {ln for ln in r.stdout.splitlines() if ln}
    if not changed:
        # git diff 成功但没有文件（空 merge 提交）：默认拒绝而不是猜「只 merge 了 main」。
        # /compare 300 上限截断也会表现成「文件清单为空」，同类风险一并堵。
        out["compare_files_missing"] = True
        return out
    out["changed_count"] = len(changed)
    if len(changed) >= 300:
        # 本地 git diff 没有截断：≥300 只说明「这次改动面很大」，不是数据缺失。
        # 警告可以宽，拒绝必须准——不拒绝，只在通过消息里提示人工确认。
        out["compare_files_truncated"] = True
    touched = sorted(changed & owned)
    out["touched"] = touched

    for f in touched:
        if not f.endswith("_test.go"):
            out["production_touched"].append(f)
            continue
        out["test_touched"].append(f)
        diff = _git("diff", "--unified=0", f"{review}..{head}", "--", f, cwd=cwd)
        added = [ln[1:] for ln in diff.stdout.splitlines()
                 if ln.startswith("+") and not ln.startswith("+++")]
        if not added:
            continue
        main_lines = _main_lines(f, cwd=cwd)
        if any(ln not in main_lines for ln in added):
            out["test_disallowed"].append(f)
    return out


def decide_review(pr, pr2, reviews, anchor=None):
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
    # 否决词只看首行结论：多轮正文后面引用「上轮必改 1 条」不代表本轮否决。
    first_line = next((ln for ln in body.splitlines() if ln.strip()), "")
    if DENY.search(first_line):
        return False, f"首行结论含否决词（不可合并/不通过/必改 N 等）：{summary}", "deny_word"

    anchor_note = ""
    if commit != head:
        # 先判「输入缺失」再判祖先：真实早退路径会同时 ancestor=False，
        # 若先判祖先会把「读不到 pr_files」误报成「请 rebase/force push」。
        if not anchor:
            return False, "没有锚定数据（git/API 读不到），无法判定 head 前进原因；按纪律拒绝", "anchor_error"
        if anchor.get("pr_files_missing"):
            return False, (
                "读不到本 PR 的文件清单（输入为空），无法判定 review..head 是否只含 merge main；"
                "按纪律拒绝而不是放行"
            ), "pr_files_missing"
        if anchor.get("compare_files_missing"):
            return False, (
                "review..head 的文件清单为空（可能是空 merge 提交，或文件清单被截断）；"
                "默认拒绝而不是放行"
            ), "compare_files_missing"
        if anchor.get("compare_files_truncated"):
            anchor_note = (f"（本次锚定覆盖 {anchor.get('changed_count', 0)} 个文件的差异，"
                           "请人工确认是 merge main 所致；不拒绝）")
        if anchor.get("error"):
            return False, f"锚定计算失败：{anchor['error']}；按纪律拒绝", "anchor_error"
        if not anchor.get("ancestor"):
            return False, (
                f"结论 {commit[:12]} 不是当前 head {head[:12]} 的祖先"
                f"（head_mismatch；rebase/force push 后必须重审）"
            ), "head_mismatch"
        if anchor.get("production_touched"):
            files = ", ".join(anchor["production_touched"][:5])
            return False, (
                f"结论早于本 PR 的代码改动（stale_conclusion）：生产代码有改动（{files}），必须重审"
            ), "stale_conclusion"
        if anchor.get("test_disallowed"):
            files = ", ".join(anchor["test_disallowed"][:5])
            return False, (
                f"结论早于本 PR 的代码改动（stale_conclusion）：测试文件出现 origin/main 里不存在的新增行（{files}），"
                "像「顺手加一条永不失败的用例」，必须重审"
            ), "stale_conclusion"
        if anchor.get("test_touched"):
            anchor_note = "（仅合并冲突解决：生产代码零改动，测试新增行均来自 origin/main）"
        elif not anchor_note:
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
    old = "b" * 40
    pr = {"head": {"sha": head}}
    pr2 = {"head": {"sha": head}}

    def anchor(production=None, test=None, disallowed=None, touched=None, missing=False, ancestor=True, no_changes=False, truncated=False):
        return {
            "ancestor": ancestor, "pr_files_missing": missing,
            "compare_files_missing": no_changes, "compare_files_truncated": truncated,
            "changed_count": 400 if truncated else 0,
            "production_touched": production or [], "test_touched": test or [],
            "test_disallowed": disallowed or [], "touched": touched or [],
        }

    cases = [
        ("approval-bold", {"body": "**可合并（必改 0）**，head x", "state": "COMMENTED", "commit_id": head}, None, True, "pass"),
        ("not-mergeable", {"body": "不可合并", "state": "COMMENTED", "commit_id": head}, None, False, "deny_word"),
        ("not-released", {"body": "不予放行", "state": "COMMENTED", "commit_id": head}, None, False, "deny_word"),
        ("history-zero", {"body": "上一轮必改 0，本轮必改 2 条", "state": "COMMENTED", "commit_id": head}, None, False, "deny_word"),
        ("mustfix-2", {"body": "**必改 2 条**（都在地基上）", "state": "COMMENTED", "commit_id": head}, None, False, "deny_word"),
        ("changes-requested", {"body": "**可合并（必改 0）**", "state": "CHANGES_REQUESTED", "commit_id": head}, None, False, "state_veto"),
        ("merge-main-only", {"body": "**可合并（必改 0）**", "state": "COMMENTED", "commit_id": old}, anchor(), True, "pass"),
        ("anchor-error", {"body": "**可合并（必改 0）**", "state": "COMMENTED", "commit_id": old},
         {"ancestor": True, "error": "git diff 失败"}, False, "anchor_error"),
        ("merge-conflict-test-only", {"body": "**可合并（必改 0）**", "state": "COMMENTED", "commit_id": old},
         anchor(test=["internal/verify/x_test.go"]), True, "pass"),
        ("production-changed", {"body": "**可合并（必改 0）**", "state": "COMMENTED", "commit_id": old},
         anchor(production=["internal/engine/engine.go"]), False, "stale_conclusion"),
        ("test-hand-added", {"body": "**可合并（必改 0）**", "state": "COMMENTED", "commit_id": old},
         anchor(test=["internal/verify/x_test.go"], disallowed=["internal/verify/x_test.go"]), False, "stale_conclusion"),
        ("rebase-not-ancestor", {"body": "**可合并（必改 0）**", "state": "COMMENTED", "commit_id": old},
         anchor(ancestor=False), False, "head_mismatch"),
        ("missing-compare-files", {"body": "**可合并（必改 0）**", "state": "COMMENTED", "commit_id": old},
         anchor(no_changes=True), False, "compare_files_missing"),
        ("compare-files-truncated-warn", {"body": "**可合并（必改 0）**", "state": "COMMENTED", "commit_id": old},
         anchor(truncated=True), True, "pass"),
        # 真实 compute_anchor 早退时 ancestor=False；用例必须喂这个形状，
        # 否则测的是真实代码走不到的世界。
        ("pr-files-missing", {"body": "**可合并（必改 0）**", "state": "COMMENTED", "commit_id": old},
         anchor(missing=True, ancestor=False), False, "pr_files_missing"),
        ("approval-conclusion", {"body": "结论：可合并", "state": "COMMENTED", "commit_id": head}, None, True, "pass"),
        ("history-in-tail", {"body": "**可合并（必改 0）**，按 head x\n\n…上轮必改 1 条已修…", "state": "COMMENTED", "commit_id": head}, None, True, "pass"),
    ]
    ok_all = True
    for name, review, anc, want_ok, want_cat in cases:
        review.setdefault("user", {"login": "probe"})
        review.setdefault("submitted_at", "2026-01-01T00:00:00Z")
        review.setdefault("id", 1)
        ok, msg, cat = decide_review(pr, pr2, [review], anc)
        good = ok == want_ok and cat == want_cat
        ok_all = ok_all and good
        print(f"{'PASS' if good else 'FAIL'} {name}: cat={cat} ok={ok} :: {msg}")
    return 0 if ok_all else 1


def main():
    if len(sys.argv) < 2:
        sys.exit("用法: review_gate_logic.py review|admin|anchor|selftest ...")
    mode = sys.argv[1]
    if mode == "selftest":
        sys.exit(selftest())
    if mode == "anchor":
        if len(sys.argv) != 6:
            sys.exit("anchor 需要 out.json review head prfiles.json")
        out = compute_anchor(sys.argv[3], sys.argv[4], _load(sys.argv[5]))
        with open(sys.argv[2], "w", encoding="utf-8") as f:
            json.dump(out, f)
        sys.exit(0)
    if mode == "review":
        if len(sys.argv) != 6:
            sys.exit("review 需要 pr.json pr2.json reviews.json anchor.json")
        anchor = _load(sys.argv[5]) if _load(sys.argv[5]) else {}
        ok, msg, cat = decide_review(_load(sys.argv[2]), _load(sys.argv[3]), _load(sys.argv[4]), anchor)
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
