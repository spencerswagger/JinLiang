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
		// 前提变了就红：这条用例的全部意义是「单表 + 零块标记 → copySkip → 搬运阶段整个不跑」，
		// planCopy 的老状态分支一改，留着 Logf 会让这条用例安静地什么都不证明。
		t.Fatalf("前提变了：planCopy 对「零块标记的单表」不再判 copySkip，得到 %s", plan.Kind)
	}
	r := &runner{forceRecopy: map[string]source.TableRef{"orders": ref}}
	r.commitPlan(&plan)
	// 断言的是 **r.plan**：`snapshot()` 只读它。改局部变量那份等于没改（踩过一次）。
	if r.plan.Kind == copySkip {
		t.Fatal("有表欠重搬时 Kind 不能是 copySkip：snapshot 会整段跳过，说了重搬却一步不动")
	}
	if !containsTable(r.plan.Recopy, "orders") {
		t.Fatalf("欠重搬的表必须进 r.plan.Recopy（唯一会清空 + 从表头重读的分支），得到 %+v", r.plan.Recopy)
	}
	if r.plan.Skip["orders"] {
		t.Fatal("它这次必须搬，不能留在 Skip 名单里")
	}
	if !r.needSnapshot {
		t.Fatal("有表要重搬就必须真的跑搬运阶段（needSnapshot）")
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
	r.commitPlan(&plan)

	if !containsTable(r.plan.Recopy, "orders") {
		t.Fatal("欠重搬的表必须进 r.plan.Recopy")
	}
	if _, ok := r.plan.From["orders"]; ok {
		t.Fatal("重搬要从表头读，不能沿用旧游标")
	}
	if !r.plan.Skip["healthy"] || r.plan.From["healthy"].Cursor != "100" {
		t.Fatalf("别的表不受影响（Skip 与游标都要留着），得到 Skip=%v From=%+v", r.plan.Skip, r.plan.From)
	}
	if r.plan.Kind != copyResume {
		t.Fatalf("Kind 不该被动过，得到 %s", r.plan.Kind)
	}
	// **调用方那份也必须被改**：`prepare()` 后面还用局部 `plan` 做 `src.Prepare(...)` 与
	// `switch plan.Kind` 的决定。收值的写法会「r.plan 新、调用方旧」两份各说各话
	// （Owl II 探针：单表会重建书签 + 位点跳到新 LSN，与「续跑不动位点」相反）。
	if !containsTable(plan.Recopy, "orders") {
		t.Fatal("commitPlan 收值就会让调用方那份 plan 停在旧值（这里只该被改一次）")
	}
	_ = b
}

// 止损指令（维护者 2026-08-11）点名的那个坑：**「清块标记」不等于「下一轮会重搬」**。
//
// Owl II 复审实测：`planCopy` 在「单表 + 块标记已清空 + 位点在 + phase=paused」时判 **skip**
// （理由是「旧版本没有块进度记录，按阶段认定存量已搬完」）；两表时判 resume 但该表
// `From/Skip/Recopy` 全空、`Clean=cleanNone`（从表头重读却不清表）。
//
// 所以恢复路径**不能**依赖「清了标记自然就重搬」。这条用例把两件事同时钉住：
//  1. 前提是真实的（`planCopy` 确实会给 skip）；
//  2. `commitPlan` 必须把它救回来（`Recopy` + `CleanNames` + 非 skip 的 Kind），
//     而不是等 Ark 修订契约（M1）之后再说。
func TestPausedTableWithClearedChunksWouldSkipWithoutForcedRecopy(t *testing.T) {
	ref := source.TableRef{Schema: "public", Table: "orders"}
	in := copyInputs{
		Mode:     "continuous+initial",
		Tables:   []source.Table{{TableRef: ref, PrimaryKey: []string{"id"}}},
		Progress: map[string]pgsink.ChunkProgress{}, // reconcilePaused 刚把块标记清干净
		StoredCP: "postgres:v1:0/100",
		// 书签可重放 + 阶段是 paused（用户结构对不上那一轮的收尾阶段）
		SlotReplayable: true, Capable: true, PriorPhase: "paused",
	}
	plan := planCopy(in)
	if plan.Kind != copySkip {
		t.Fatalf("前提变了：零块标记 + phase=paused 现在判 %s —— 说明 planCopy 的「老状态兼容」"+
			"那条改了，请同步更新这条用例与 applyForcedRecopy 的必要性说明", plan.Kind)
	}

	r := &runner{forceRecopy: map[string]source.TableRef{"orders": ref}}
	r.commitPlan(&plan)
	if r.plan.Kind == copySkip {
		t.Fatal("清块标记之后靠 planCopy 自己会判 skip：解除暂停就变成「没搬也没清」，静默缺数据")
	}
	if !containsTable(r.plan.Recopy, "orders") {
		t.Fatal("必须显式进 plan.Recopy（那条分支才会清空 + 从表头重读）")
	}
	if !r.needSnapshot {
		t.Fatal("要重搬就必须真的跑搬运阶段")
	}
}

// 两表 + **从头搬**的轮次（位点追不回来）：待重搬的表必须被补进 `CleanNames`。
//
// 为什么专门构造这个形状（Owl II P3）：`plan.Written` 是按**清块标记之前**的块进度算的，
// 刚被 reconcilePaused 清过标记的那张表进不去 → 会「从表头重读却不清表」→ 暂停窗口里
// 被删的行留成幻行。普通 once 轮次判的是 resume（Clean=cleanNone），那条断言会被
// `Clean != cleanNone` 短路掉，等于什么都没钉住。
func TestForcedRecopyJoinsCleanNamesOnFreshRuns(t *testing.T) {
	a := source.TableRef{Schema: "public", Table: "orders"}
	b := source.TableRef{Schema: "public", Table: "healthy"}
	in := copyInputs{
		Mode:   "continuous+initial",
		Tables: []source.Table{{TableRef: a, PrimaryKey: []string{"id"}}, {TableRef: b, PrimaryKey: []string{"id"}}},
		Progress: map[string]pgsink.ChunkProgress{
			"healthy": {Done: true, LastNo: 1}, // orders 的标记刚被 reconcilePaused 清掉
		},
		StoredCP: "postgres:v1:0/100",
		// 书签追不回位点 → 整轮从头搬（copyFresh + cleanAll），这正是「清表」真正发生的那条路
		SlotReplayable: false, SlotWhy: "WAL 段已被删除", Capable: true, PriorPhase: "paused",
	}
	plan := planCopy(in)
	if plan.Kind != copyFresh || plan.Clean == cleanNone {
		t.Fatalf("前提变了：位点追不回应判 fresh + cleanAll，得到 %s / %v", plan.Kind, plan.Clean)
	}
	if containsName(plan.CleanNames, "orders") {
		t.Fatal("前提变了：CleanNames 居然已经含 orders —— 那这条用例就钉不住补丁了")
	}

	r := &runner{forceRecopy: map[string]source.TableRef{"orders": a}}
	r.commitPlan(&plan)
	if !containsName(r.plan.CleanNames, "orders") {
		t.Fatalf("待重搬的表必须补进清理名单（否则不清表只 upsert → 幻行），得到 %v", r.plan.CleanNames)
	}
	if !containsTable(r.plan.Recopy, "orders") {
		t.Fatal("也要进 Recopy（两个入口共用 recopyOne：判所有权 + 清空 + 清块标记）")
	}
	if r.plan.Skip["orders"] {
		t.Fatal("它这次必须搬，不能留在 Skip 名单里")
	}
}
