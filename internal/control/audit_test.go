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
