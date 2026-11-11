package engine

// 「0 重搬」三口径里第②条（单表口径）的**判据面**断言。
//
// 契约把「0 重搬」拆成三个互不替代的度量：
//   ① 整轮：`rowsCopied==0 ∧ plan.Kind==copySkip`（验收主口径，MySQL→PG 的 kill 那格用它）；
//   ② 单表：`chunk_no=0` 仍在（表还没搬完）∧ `plan.From[table]` 非空（从块边界接着读）；
//   ③ 暂停恢复：该表重搬行数 == 该表行数（**允许 ≠ 0**）。
//
// 脚本里能观测到的是①②的**结果**（重启搬多少行）；`plan.From` 这一半只有单测能直接钉。
// 所以这条用例专门管「计划表面上真的是续跑，而不是碰巧板了两行」：

import (
	"testing"

	"github.com/spencerswagger/jinliang/internal/pgsink"
	"github.com/spencerswagger/jinliang/internal/source"
)

func TestPlanCopySingleTableResumeMetric(t *testing.T) {
	tbl := source.Table{TableRef: source.TableRef{Schema: "public", Table: "orders"}, HasStablePK: true}
	in := copyInputs{
		Mode:   "continuous+initial",
		Tables: []source.Table{tbl},
		// chunk_no=0 在（表没搬完）+ 已提交 2 块 → 必须续跑，且 From 非空
		Progress:       map[string]pgsink.ChunkProgress{"orders": {Done: false, LastNo: 2, Cursor: `[{"t":"i","v":"4000"}]`, Rows: 4000}},
		StoredCP:       "postgres:v1:0/AA",
		SlotReplayable: true,
		Capable:        true,
	}
	p := planCopy(in)
	if p.Kind != copyResume {
		t.Fatalf("口径②：有块边界、位点可重放时必须续跑，实际 %v（%s）", p.Kind, p.Why)
	}
	from, ok := p.From["orders"]
	if !ok || from.LastNo == 0 || from.Cursor == "" {
		t.Fatalf("口径②：plan.From[orders] 必须非空（从块边界接着读），实际 %+v", p.From)
	}

	// 反面：表已经搬完（Done=true）→ 单表口径不成立（这时属于「整轮口径」的 copySkip）
	done := in
	done.Progress = map[string]pgsink.ChunkProgress{"orders": {Done: true, LastNo: 2, Cursor: "x", Rows: 4000}}
	if got := planCopy(done).Kind; got != copySkip {
		t.Fatalf("表已搬完时应 copySkip（整轮口径），实际 %v", got)
	}
}
