package control

import (
	"context"
	"strings"
	"testing"
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
