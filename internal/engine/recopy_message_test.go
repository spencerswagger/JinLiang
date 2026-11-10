package engine

// 错误文案里**不许出现做不到的动作**。
//
// 背景：`reusedTableRecopyError` 曾给三条路，第 3 条是「手动删掉本任务写进去的行后重跑」。
// #74「凡从头搬就清表」之后它失效了——标记删了也会因 `copiedBefore` 整批点名，用户照做
// 必然再撞同一句拒绝。把做不到的动作写进错误，比不给建议更糟。

import (
	"strings"
	"testing"
)

func TestReusedTableRecopyMessageOnlyOffersWorkablePaths(t *testing.T) {
	e := &reusedTableRecopyError{table: "orders", jobKey: "abcdefghij", schema: "public", why: recopyGap}
	msg := e.Error()

	// 1) 不许出现做不到的动作（含同义改写——负向断言最容易被「手工清理」这类绕过去）
	for _, bad := range []string{"手动删掉", "删掉本任务写进去的行", "三选一", "手工清理", "手工删"} {
		if strings.Contains(msg, bad) {
			t.Fatalf("文案里出现做不到的动作 %q：\n%s", bad, msg)
		}
	}
	// 2) 两条真能走的路要在
	for _, want := range []string{"target.on_exists 改成 recreate", "换一个空的 target.schema", "二选一"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("文案缺少可执行的一条 %q：\n%s", want, msg)
		}
	}
	// 2b) **结构断言**：编号只有 1) 2)，没有第 3 条
	if strings.Contains(msg, "3)") {
		t.Fatalf("文案里出现了第 3 条编号（已经没有第 3 条路了）：\n%s", msg)
	}
	// 2c) 正向断言「为什么判定这不是我们的表」也要钉住
	if !strings.Contains(msg, "（不是本任务建的）") {
		t.Fatalf("要写清判定依据「（不是本任务建的）」：\n%s", msg)
	}

	// 3) 说清为什么停（不是我们懒），以及风险
	if !strings.Contains(msg, "是你自己的表") || !strings.Contains(msg, "风险") {
		t.Fatalf("要写清「为什么停」与「风险」：\n%s", msg)
	}
	// 4) 不许出现不存在的开关
	for _, bad := range []string{"--resync", "--accept-change", "--fix"} {
		if strings.Contains(msg, bad) {
			t.Fatalf("文案里出现未实现/不适用的开关 %q：\n%s", bad, msg)
		}
	}
}
