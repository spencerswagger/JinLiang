package control

import (
	"context"
	"strings"
	"testing"
	"time"
)

// 按 action 过滤必须下推到 SQL：诊断页原来取「最近 50 条」再在页面里筛 rejected，
// 忙时（一小时几百条 run/login）拒绝记录会整批掉出窗口——用户看到「没有记录」，
// 而实际刚刚发生过。这里造一堆别的 action 把窗口填满，再验拒绝记录仍取得到。
func TestListAuditByActionSurvivesBusyWindow(t *testing.T) {
	store, err := Open(Options{DataDir: t.TempDir(), Autogen: true})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()

	// 最早的拒绝记录：它一定会被后面的忙时记录挤出「最近 N 条」窗口
	if err := store.Audit(ctx, "api", ActionRunRejected, map[string]any{
		"job_id": "old-job", "note": "最早的拒绝",
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 60; i++ {
		if err := store.Audit(ctx, "api", ActionJobRun, map[string]any{"job_id": "busy", "i": i}); err != nil {
			t.Fatal(err)
		}
	}

	// 时间戳只有秒级精度，且 id 尾号是随机的 —— 光靠「插 60 条」在快机器上可能
	// 把拒绝记录仍留在前 50 条里（CI 上就出现过）。把时间钉成确定值，测试才稳定：
	// 拒绝记录放 2000 年，其余放 2030 年。
	if _, err := store.DB().Exec(`UPDATE audit SET at = '2000-01-01T00:00:00Z' WHERE action = ?`, ActionRunRejected); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DB().Exec(`UPDATE audit SET at = '2030-01-01T00:00:00Z' WHERE action = ?`, ActionJobRun); err != nil {
		t.Fatal(err)
	}

	// 先证明「取最近 50 条再筛」这条路确实已经看不到它（前提成立）
	recent, err := store.ListAudit(ctx, 50)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range recent {
		if e.Action == ActionRunRejected {
			t.Fatalf("前提不成立：最近 50 条里居然还有拒绝记录（需要加大填充量）")
		}
	}

	// 过滤下推到 SQL 之后，仍然取得到
	got, err := store.ListAuditByAction(ctx, ActionRunRejected, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Action != ActionRunRejected {
		t.Fatalf("按 action 过滤应取到 1 条拒绝记录，实际 %d 条", len(got))
	}
	if !strings.Contains(got[0].Detail, "最早的拒绝") {
		t.Fatalf("取到的不是那条拒绝记录：%s", got[0].Detail)
	}
}

// 计划必须走索引（Owl 用 EXPLAIN QUERY PLAN 实测过：没有这条索引时是
// `SCAN audit` + `USE TEMP B-TREE FOR ORDER BY`，也就是每次开诊断页全表扫 + 全排序）。
// 断言计划文本而不是耗时：耗时在不同机器上不稳定，计划是确定的。
func TestListAuditByActionUsesIndex(t *testing.T) {
	store, err := Open(Options{DataDir: t.TempDir(), Autogen: true})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	rows, err := store.DB().Query(`EXPLAIN QUERY PLAN
		SELECT id, at, actor, action, COALESCE(detail_json,'') FROM audit
		 WHERE action = ? ORDER BY at DESC, id DESC LIMIT ?`, ActionRunRejected, RejectedWindow)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(detail + "\n")
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	got := plan.String()
	if !strings.Contains(got, "idx_audit_action_at_id") {
		t.Fatalf("查询计划没走 idx_audit_action_at_id（会全表扫）：\n%s", got)
	}
	if strings.Contains(got, "TEMP B-TREE") {
		t.Fatalf("仍需要临时 B 树排序——索引少了 id 这个键：\n%s", got)
	}
	if strings.Contains(got, "SCAN audit") {
		t.Fatalf("仍是全表 SCAN：\n%s", got)
	}
}

// limit 的钳制要按方向做：limit=500 应截到 100，而不是回落成默认的 10。
func TestListAuditByActionClampsUpwards(t *testing.T) {
	store, err := Open(Options{DataDir: t.TempDir(), Autogen: true})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	for i := 0; i < 120; i++ {
		if err := store.Audit(ctx, "api", ActionRunRejected, map[string]any{"i": i}); err != nil {
			t.Fatal(err)
		}
	}
	got, err := store.ListAuditByAction(ctx, ActionRunRejected, 500)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 100 {
		t.Fatalf("limit=500 应被截到 100，实际 %d 条（回落成默认 10 就是那个 bug）", len(got))
	}
	zero, err := store.ListAuditByAction(ctx, ActionRunRejected, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(zero) != RejectedWindow {
		t.Fatalf("limit=0 应回落默认 %d，实际 %d", RejectedWindow, len(zero))
	}
}

// 审计保留策略（hugo 口径）：默认 180 天；登录、凭证/连接变更类动作永久保留；
// 清理动作本身要留痕。
func TestPruneAuditKeepsPermanentActions(t *testing.T) {
	store, err := Open(Options{DataDir: t.TempDir(), Autogen: true})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()

	old := time.Now().Add(-200 * 24 * time.Hour).UTC().Format(time.RFC3339)
	write := func(action string, at string) string {
		id, _ := NewID()
		if _, err := store.DB().Exec(
			`INSERT INTO audit (id, at, actor, action, detail_json) VALUES (?, ?, 'api', ?, '{}')`,
			id, at, action); err != nil {
			t.Fatal(err)
		}
		return id
	}
	fresh := time.Now().UTC().Format(time.RFC3339)
	oldRun := write(ActionJobRun, old)       // 过期普通记录 → 应被清
	oldLogin := write("login.ok", old)       // 过期登录 → 永久保留
	oldConn := write(ActionConnUpdate, old)  // 过期连接变更 → 永久保留
	oldPrune := write(ActionAuditPrune, old) // 清理动作本身 → 永久保留
	newRun := write(ActionJobRun, fresh)     // 未过期 → 保留

	// ① dry-run：只统计，不删
	dry, err := store.PruneAudit(ctx, time.Now().Add(-180*24*time.Hour), false)
	if err != nil {
		t.Fatal(err)
	}
	if dry.Deleted != 1 {
		t.Fatalf("dry-run 应只统计到 1 条可删，实际 %d（candidates=%v）", dry.Deleted, dry.Candidates)
	}
	if dry.Kept != 3 {
		t.Fatalf("永久保留应是 3 条（登录/连接/清理自身），实际 %d", dry.Kept)
	}

	// ② apply：普通记录没了，永久保留的还在
	res, err := store.PruneAudit(ctx, time.Now().Add(-180*24*time.Hour), true)
	if err != nil {
		t.Fatal(err)
	}
	if res.Deleted != 1 {
		t.Fatalf("应删除 1 条，实际 %d", res.Deleted)
	}
	exists := func(id string) bool {
		var n int
		if err := store.DB().QueryRow(`SELECT COUNT(*) FROM audit WHERE id = ?`, id).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n > 0
	}
	if exists(oldRun) {
		t.Fatal("过期普通记录没被清掉")
	}
	for name, id := range map[string]string{"登录": oldLogin, "连接变更": oldConn, "清理动作自身": oldPrune} {
		if !exists(id) {
			t.Fatalf("%s 的记录被误删了——这类动作必须永久保留", name)
		}
	}
	if !exists(newRun) {
		t.Fatal("未过期的记录被误删了")
	}

	// ③ 清理动作本身必须留痕
	entries, err := store.ListAuditByAction(ctx, ActionAuditPrune, 10)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, e := range entries {
		if strings.Contains(e.Detail, `"deleted":1`) {
			found = true
		}
	}
	if !found {
		t.Fatalf("清理动作没有留痕（或 detail 里没有删除条数）：%+v", entries)
	}
}

// job.delete 与 connection.delete 同样不可逆（删任务会把它的 runs 一起删），
// 必须永久保留；同时把「可清」的决定写下来作为断言——决定要落在代码里，
// 而不是「没进前缀列表所以默认被清」。
func TestPruneAuditPermanentActionsAreExplicit(t *testing.T) {
	permanent := []string{
		ActionConnCreate, ActionConnUpdate, ActionConnDelete, ActionConnTest,
		ActionJobDelete, ActionAdminSet, ActionAuditPrune, "login.ok", "session.start",
	}
	for _, a := range permanent {
		if !permanentAuditAction(a) {
			t.Fatalf("%q 必须永久保留", a)
		}
	}
	// 这些是「可以清」的**决定**（不是漏掉）：运行类记录的价值随时间衰减，
	// 而任务/连接的变更史才是追责证据。
	prunable := []string{
		ActionJobRun, ActionRunStub, ActionRunRejected,
		"job.run.lease_forced", "job.run.stop_timeout", "job.run.takeover",
		ActionJobCreate, ActionJobUpdate, ActionJobExport,
	}
	for _, a := range prunable {
		if permanentAuditAction(a) {
			t.Fatalf("%q 被标成永久保留，但它应当可清（决定写在测试里，不是默认）", a)
		}
	}
}
