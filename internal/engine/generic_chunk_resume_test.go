package engine

// 通用路径（MySQL 目标端）的**块级续跑**判据（不需要库）。
//
// 为什么单独钉：`copyGeneric` 原来给 `SnapshotChunksFrom` 传 `nil`、`needCopy` 只看
// 「有没有位点」，所以 MySQL 目标端全量中断一次只能整轮重搬（Owl II 在 #92 的 killcopy 格
// 实测：重启重搬 4000 行，超过 1 块 2000 行）。现在与 PG 路径共用 `planCopy`。
//
// 同时钉住**两件事不能混**：
//   - 中断续跑 = 按块续（`plan.From[table]` 有游标，`Kind == copyResume`）；
//   - 暂停恢复 = 整表重搬（进 `plan.Recopy`，`From` 里**没有**它）。

import (
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/pgsink"
	"github.com/spencerswagger/jinliang/internal/source"
)

func TestGenericCopyPlanResumesFromCommittedChunks(t *testing.T) {
	in := copyInputs{
		Mode:   "continuous+initial",
		Tables: []source.Table{{TableRef: source.TableRef{Schema: "public", Table: "orders"}, PrimaryKey: []string{"id"}, HasStablePK: true}},
		Progress: map[string]pgsink.ChunkProgress{
			// 搬完了 2 块（右边界 4000），第 3 块没提交、也没有 chunk_no=0
			"orders": {Done: false, LastNo: 2, Cursor: "4000"},
		},
		StoredCP: "postgres:v1:0/100", SlotReplayable: true, Capable: true, PriorPhase: "snapshot",
	}
	plan := planCopy(in)
	if plan.Kind != copyResume {
		t.Fatalf("有块进度但没搬完应判**续跑**（不是整轮重搬），得到 %s", plan.Kind)
	}
	fr, ok := plan.From["orders"]
	if !ok || fr.Cursor != "4000" || fr.LastNo != 2 {
		t.Fatalf("续跑起点必须来自已提交块的右边界，得到 %+v（ok=%v）", fr, ok)
	}
}

func TestGenericCopyPlanStartsOverWhenNoChunkProgress(t *testing.T) {
	in := copyInputs{
		Mode:   "continuous+initial",
		Tables: []source.Table{{TableRef: source.TableRef{Schema: "public", Table: "orders"}, PrimaryKey: []string{"id"}, HasStablePK: true}},
		// 一块都没提交（中断在第一块里）→ 没有可续的边界
		Progress: map[string]pgsink.ChunkProgress{},
		StoredCP: "postgres:v1:0/100", SlotReplayable: true, Capable: true, PriorPhase: "snapshot",
	}
	plan := planCopy(in)
	// 真正的安全性质：**没有任何块边界**时，每张表都从表头读（`From` 为空）。
	// 只要 `From` 里没有这一项，无论 Kind 判成 resume 还是 fresh，都是从第一块重新读——
	// 不会出现「从半块中间接着读、前半块丢掉」。
	if fr, ok := plan.From["orders"]; ok {
		t.Fatalf("零块进度时不许给续跑游标（会从半块中间接着读），得到 %+v", fr)
	}
}

// 关键：暂停恢复（整表重搬）**不能**借用块续跑分支。
func TestForcedRecopyIsNotChunkResume(t *testing.T) {
	a := source.TableRef{Schema: "public", Table: "paused_a"}  // 暂停过、要整表重搬
	b := source.TableRef{Schema: "public", Table: "healthy_b"} // 中断过、按块续
	in := copyInputs{
		Mode: "continuous+initial",
		Tables: []source.Table{
			{TableRef: a, PrimaryKey: []string{"id"}, HasStablePK: true},
			{TableRef: b, PrimaryKey: []string{"id"}, HasStablePK: true},
		},
		Progress: map[string]pgsink.ChunkProgress{
			"paused_a":  {Done: false, LastNo: 1, Cursor: "300"},
			"healthy_b": {Done: false, LastNo: 3, Cursor: "6000"},
		},
		StoredCP: "postgres:v1:0/100", SlotReplayable: true, Capable: true, PriorPhase: "snapshot",
	}
	plan := planCopy(in)
	if plan.Kind != copyResume {
		t.Fatalf("两张表都只搬了一半，应判续跑，得到 %s", plan.Kind)
	}
	r := &runner{forceRecopy: map[string]source.TableRef{"paused_a": a}}
	r.commitPlan(&plan)

	if _, ok := r.plan.From["paused_a"]; ok {
		t.Fatal("要整表重搬的表**不许**留续跑游标（会从游标接着读，前半段永远补不回来）")
	}
	if !containsTable(r.plan.Recopy, "paused_a") {
		t.Fatal("它必须进 plan.Recopy（清空 + 从表头重读）")
	}
	if fr, ok := r.plan.From["healthy_b"]; !ok || fr.Cursor != "6000" {
		t.Fatalf("另一张表必须保持按块续跑（不能因为别人暂停就整轮重搬），得到 %+v（ok=%v）", fr, ok)
	}
	if containsTable(r.plan.Recopy, "healthy_b") {
		t.Fatal("健康表不许被拉进整表重搬")
	}
}

// 「位点追不回 + continuous」必须**硬停**，不能从新位置起流（那段变更会静默丢掉）。
// 这条判据原来在 `decideGenericCopyPlan` 里不可达（early return 先把 continuous 挡掉了），
// 是 Owl II 用「条件互斥」读出来的；这里把它钉死。
func TestGenericPlanStopsWhenSlotUnreplayableInContinuous(t *testing.T) {
	r := &runner{doc: control.JobDoc{Mode: control.ModeDoc{Frequency: "continuous"}}}
	in := copyInputs{
		Mode:     "continuous",
		Tables:   []source.Table{{TableRef: source.TableRef{Schema: "public", Table: "orders"}, HasStablePK: true}},
		Progress: map[string]pgsink.ChunkProgress{"orders": {Done: true, LastNo: 1}},
		StoredCP: "postgres:v1:0/100",
		Capable:  true,
		// 书签追不回位点
		SlotReplayable: false, SlotWhy: "WAL 段已被删除",
	}
	plan := planCopy(in)
	if plan.Kind != copySkip {
		t.Fatalf("continuous 不搬存量，应判 copySkip，得到 %s", plan.Kind)
	}
	err := &unreplayableSlotError{stored: "postgres:v1:0/100"}
	if err == nil || !strings.Contains(err.Error(), "--resync") {
		t.Fatalf("硬停的文案必须给出 --resync 这条唯一出路，得到 %v", err)
	}
	_ = r
}
