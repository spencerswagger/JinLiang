package engine

import (
	"strings"
	"testing"
)

// SPEC MY-08 的降级标注必须落进 Result.Note，而不只是事件流里的一句话。
//
// 事件流是易失的（容器里没人读、日志被截断），`run` 的最后一行「说明：…」是
// 用户与 CI 一定看到的那一处。少了它，「全量完成 + 退出码 0」就是假绿：
// MyISAM 表根本没有一致性切面，用户却以为拿到了。
func TestFinishNotesCarriesEngineDegrade(t *testing.T) {
	r := &runner{degradeNote: "降级：legacy(MyISAM) 用的是无事务引擎"}
	res := Result{}
	r.finishNotes(&res)
	if !strings.Contains(res.Note, "降级") || !strings.Contains(res.Note, "legacy(MyISAM)") {
		t.Fatalf("降级必须进 Result.Note（CLI 的「说明」），得到 %q", res.Note)
	}

	// 已经有别的说明时是追加，不是覆盖：两条信息都得留下。
	res = Result{Note: "只搬了存量数据（mode.frequency=once）"}
	r.finishNotes(&res)
	if !strings.Contains(res.Note, "只搬了存量数据") || !strings.Contains(res.Note, "降级") {
		t.Fatalf("要和既有说明并存，得到 %q", res.Note)
	}

	// 没有降级（InnoDB 源）时一个字都不加：不许为了「安全」给每次跑都塞一句话。
	res = Result{Note: "只搬了存量数据"}
	(&runner{}).finishNotes(&res)
	if res.Note != "只搬了存量数据" {
		t.Fatalf("没有降级时 Note 不该变：%q", res.Note)
	}
}
