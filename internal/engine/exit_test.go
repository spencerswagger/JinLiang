package engine

// 退出码 3（`ExitNeedsAction`）的派生规则 —— Panel 裁决：**单点派生**、优先级
// `失败(1) > 用户停止(130) > 有暂停(3) > 正常(0)`，并且「有暂停表却退 0」正是要修的 bug。
//
// 这一格不需要库：`DecideExit` 是纯函数，四档 × 优先级都在这里钉死。

import (
	"testing"

	"github.com/spencerswagger/jinliang/internal/state"
)

func pausedOne() []PausedTableStat {
	return []PausedTableStat{{Table: "orders", Reason: "type_changed", SkippedRows: 3}}
}

func TestDecideExit(t *testing.T) {
	cases := []struct {
		name   string
		exit   int
		paused []PausedTableStat
		want   int
	}{
		{"正常收尾、没有暂停", ExitOK, nil, ExitOK},
		{"正常收尾、有暂停 → 3（不许退 0）", ExitOK, pausedOne(), ExitNeedsAction},
		{"失败优先于暂停", ExitFailed, pausedOne(), ExitFailed},
		{"失败、没有暂停", ExitFailed, nil, ExitFailed},
		{"用户停止优先于暂停", ExitStopped, pausedOne(), ExitStopped},
		{"用户停止、没有暂停", ExitStopped, nil, ExitStopped},
	}
	for _, c := range cases {
		if got := DecideExit(state.Done, c.exit, c.paused); got != c.want {
			t.Errorf("%s：期望 %d，得到 %d", c.name, c.want, got)
		}
	}
	// 常量本身也是契约：`ExitNeedsAction` 必须是 3（SPEC §74 / PLAN §55/66 的规范第 3 档），
	// 且不得与既有码冲突（0 完成 / 1 失败 / 2 用法错误 / 5 校验不一致 / 130 停止）。
	if ExitNeedsAction != 3 {
		t.Fatalf("ExitNeedsAction 必须是 3，得到 %d", ExitNeedsAction)
	}
	for _, other := range []int{ExitOK, ExitFailed, 2, 5, ExitStopped} {
		if ExitNeedsAction == other {
			t.Fatalf("ExitNeedsAction 与既有退出码 %d 冲突", other)
		}
	}
}

// phase 不参与判定：有暂停时 phase 仍是 done（用 `state.Paused` 会让 HoldsLease()=true，
// 把「停一张表」变成「整个任务停摆」）。
func TestDecideExitIgnoresPhase(t *testing.T) {
	if got := DecideExit(state.Done, ExitOK, pausedOne()); got != ExitNeedsAction {
		t.Fatalf("phase=done 也必须有暂停就 3，得到 %d", got)
	}
}
