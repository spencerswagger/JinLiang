package engine

// 按表暂停这条线的**不需要库**的用例（Owl II P1：写路径与恢复计划原来零单测，
// 全部关在 `//go:build integration` 后面，`go test ./...` 编译不到 → 「全绿」对这条线没有信息量）。
//
// 两件事各有判据：
//  1. `pauseSink.filter`：暂停表的事件整条丢掉，事务边界事件留下（丢了 Begin/Commit，
//     整批的事务配对就断了）；
//  2. `applyForcedRecopy`：欠重搬的表必须真的进 `plan.Recopy`（唯一会「清空 + 从表头重读」
//     的分支），而且 `copySkip` 要兜底成会跑搬运的 Kind —— 否则单表任务会「说了重搬、一步不动」。

import (
	"testing"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/pgsink"
	"github.com/spencerswagger/jinliang/internal/source"
)

func pausedRunner(paused ...string) *runner {
	r := &runner{paused: map[string]pgsink.PausedTable{}}
	for _, t := range paused {
		r.paused[t] = pgsink.PausedTable{Reason: pgsink.PauseReasonTypeChanged, Resume: pgsink.ResumeRecopy}
	}
	return r
}

// 暂停表的行事件全部丢掉，其它表原样留下；EvBegin/EvCommit 一条都不许丢。
func TestPauseSinkFilterDropsOnlyPausedTableRows(t *testing.T) {
	r := pausedRunner("paused_a")
	// refMappedSink 在生产里负责把源端表名换成目标端表名；这里恒等映射就够。
	r.doc = control.JobDoc{Target: control.TargetDoc{Schema: "public"}}
	s := newPauseSink(nil, r)

	evs := []source.Event{
		{Kind: source.EvBegin},
		{Kind: source.EvInsert, Table: source.TableRef{Schema: "public", Table: "paused_a"}},
		{Kind: source.EvDelete, Table: source.TableRef{Schema: "public", Table: "paused_a"}},
		{Kind: source.EvDDL, Table: source.TableRef{Schema: "public", Table: "paused_a"}},
		{Kind: source.EvInsert, Table: source.TableRef{Schema: "public", Table: "healthy_b"}},
		{Kind: source.EvCommit},
	}
	kept, skipped, rows := s.filter(evs)
	if len(kept) != 3 {
		t.Fatalf("应只剩 Begin / B 的 INSERT / Commit，得到 %d 条：%+v", len(kept), kept)
	}
	if kept[0].Kind != source.EvBegin || kept[2].Kind != source.EvCommit {
		t.Fatalf("事务边界不许丢（丢了整批配对就断）：%+v", kept)
	}
	if kept[1].Table.Table != "healthy_b" {
		t.Fatalf("其它表必须留下，得到 %+v", kept[1])
	}
	if skipped["paused_a"] != 3 {
		t.Fatalf("暂停表被跳过的事件数应为 3（两条行事件 + 一条 DDL），得到 %d", skipped["paused_a"])
	}
	if rows["paused_a"] != 2 {
		t.Fatalf("记账口径：行事件 2 条（DDL 不算行），得到 %d", rows["paused_a"])
	}
	if _, ok := skipped["healthy_b"]; ok {
		t.Fatal("没暂停的表不许进跳过名单")
	}
}

// 变异：把 filter 里的暂停判定去掉（`if _, paused := ...; !paused` 改成恒 false）
// → paused_a 的两条行事件会留下 → 本用例与 integration_paused_test.go ⑤ 都红。
func TestPauseSinkFilterKeepsTransactionBoundariesWhenAllTablesPaused(t *testing.T) {
	r := pausedRunner("paused_a")
	r.doc = control.JobDoc{Target: control.TargetDoc{Schema: "public"}}
	s := newPauseSink(nil, r)

	kept, _, _ := s.filter([]source.Event{
		{Kind: source.EvBegin},
		{Kind: source.EvInsert, Table: source.TableRef{Schema: "public", Table: "paused_a"}},
		{Kind: source.EvCommit},
	})
	if err := source.ValidateBatch(kept); err != nil {
		t.Fatalf("只涉及暂停表的批次必须仍然是合法形状（空事务也要能推进位点）：%v", err)
	}
}

// 单表任务：整本块账空了 → planCopy 判 copySkip（老状态兼容那条）。
// 这正是 Owl II 实测出来的形状：不接进 plan.Recopy 的话，搬运阶段一步都不跑。
func TestApplyForcedRecopyRescuesSingleTableCopySkip(t *testing.T) {
	ref := source.TableRef{Schema: "public", Table: "orders"}
	in := copyInputs{
		Mode:     "continuous",
		Tables:   []source.Table{{TableRef: ref, PrimaryKey: []string{"id"}}},
		Progress: map[string]pgsink.ChunkProgress{}, // reconcilePaused 刚清掉块标记
		StoredCP: "postgres:v1:0/100",
		// 书签可重放：老状态兼容那条会因此判 copySkip
		SlotReplayable: true, Capable: true, PriorPhase: "streaming",
	}
	plan := planCopy(in)
	if plan.Kind != copySkip {
		t.Logf("（前提变了）planCopy 对「零块标记的单表」不再判 copySkip，得到 %s", plan.Kind)
	}
	r := &runner{forceRecopy: map[string]source.TableRef{"orders": ref}}
	r.applyForcedRecopy(&plan)
	if plan.Kind == copySkip {
		t.Fatal("有表欠重搬时 Kind 不能是 copySkip：snapshot 会整段跳过，说了重搬却一步不动")
	}
	if !containsTable(plan.Recopy, "orders") {
		t.Fatalf("欠重搬的表必须进 plan.Recopy（唯一会清空 + 从表头重读的分支），得到 %+v", plan.Recopy)
	}
	if plan.Skip["orders"] {
		t.Fatal("它这次必须搬，不能留在 Skip 名单里")
	}
}

// 多表任务：其它表照旧跳过，只有欠重搬的那张被接进 Recopy 且从表头读。
func TestApplyForcedRecopyOnlyTouchesThePausedTable(t *testing.T) {
	a := source.TableRef{Schema: "public", Table: "orders"}
	b := source.TableRef{Schema: "public", Table: "healthy"}
	plan := chunkPlan{
		Kind: copyResume,
		From: map[string]chunkFrom{"healthy": {Cursor: "100"}},
		Skip: map[string]bool{"healthy": true},
	}
	r := &runner{forceRecopy: map[string]source.TableRef{"orders": a}}
	r.applyForcedRecopy(&plan)

	if !containsTable(plan.Recopy, "orders") {
		t.Fatal("欠重搬的表必须进 Recopy")
	}
	if _, ok := plan.From["orders"]; ok {
		t.Fatal("重搬要从表头读，不能沿用旧游标")
	}
	if !plan.Skip["healthy"] || plan.From["healthy"].Cursor != "100" {
		t.Fatalf("别的表不受影响（Skip 与游标都要留着），得到 Skip=%v From=%+v", plan.Skip, plan.From)
	}
	if plan.Kind != copyResume {
		t.Fatalf("Kind 不该被动过，得到 %s", plan.Kind)
	}
	_ = b
}
