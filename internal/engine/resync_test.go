package engine

import (
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/pgsink"
	"github.com/spencerswagger/jinliang/internal/source"
)

// `--resync` 的判据必须能被穷举测：它改的是 plan（从「接着搬/不用搬」改成「整轮重搬」），
// 而 plan 决定的是「清哪几张表、从哪开始搬」。写错一格就是删用户数据或静默少搬。

func resyncRunner(mode string) *runner {
	return &runner{
		opt:   Options{Resync: true},
		tgtEP: dsn.Endpoint{Kind: dsn.KindPostgres, Database: "jl_target"},
		job:   control.Job{JobKey: "jobkey1234", Name: "orders"},
		doc:   control.JobDoc{Mode: control.ModeDoc{Frequency: mode}},
		tables: []source.TableRef{
			{Schema: "public", Table: "a"},
			{Schema: "public", Table: "b"},
			{Schema: "public", Table: "c"},
		},
		cat: source.Catalog{Tables: []source.Table{
			{TableRef: source.TableRef{Schema: "public", Table: "a"}},
			{TableRef: source.TableRef{Schema: "public", Table: "b"}},
			{TableRef: source.TableRef{Schema: "public", Table: "c"}},
		}},
	}
}

func planFor(progress map[string]pgsink.ChunkProgress, storedCP, priorPhase string) chunkPlan {
	return planCopy(copyInputs{
		Mode:       "", // 默认 continuous+initial
		Tables:     []source.Table{{TableRef: source.TableRef{Schema: "public", Table: "a"}}, {TableRef: source.TableRef{Schema: "public", Table: "b"}}, {TableRef: source.TableRef{Schema: "public", Table: "c"}}},
		Progress:   progress,
		StoredCP:   storedCP,
		PriorPhase: priorPhase,
		Capable:    true,
		// 前提：书签还能重放（--resync 要处理的是「用户想重来」，不是「位点已经追不回」）。
		SlotReplayable: true,
	})
}

// 常规判定打算「续跑」时，--resync 必须把它改成「从头搬 + 清表」，
// 而且清表名单只能是**我们写过的表**（没标记的 c 一个字节都不许动）。
func TestApplyResyncForcesFreshCopyAndKeepsCleanListNarrow(t *testing.T) {
	r := resyncRunner("")
	plan := planFor(map[string]pgsink.ChunkProgress{
		"a": {Done: true, LastNo: 3},
		"b": {LastNo: 2, Cursor: "id=200"},
		"c": {},
	}, "postgres:v1:0/1A2B3C", "streaming")
	if plan.Kind != copyResume {
		t.Fatalf("前提：常规判定应是续跑，得到 %s（%s）", plan.Kind, plan.Why)
	}
	if err := r.applyResync(&plan, "streaming", "postgres:v1:0/1A2B3C"); err != nil {
		t.Fatal(err)
	}
	if plan.Kind != copyFresh {
		t.Fatalf("--resync 之后必须是从头搬，得到 %s", plan.Kind)
	}
	if len(plan.From) != 0 || len(plan.Skip) != 0 || len(plan.Recopy) != 0 {
		t.Fatalf("从头搬不许留续跑状态：From=%v Skip=%v Recopy=%v", plan.From, plan.Skip, plan.Recopy)
	}
	if plan.Clean != cleanAll || plan.CleanWhy != recopyResync {
		t.Fatalf("必须挂 cleanAll/recopyResync，得到 clean=%v why=%v", plan.Clean, plan.CleanWhy)
	}
	if got := strings.Join(plan.CleanNames, ","); got != "a,b" {
		t.Fatalf("只该清本任务写过的表（a,b），得到 %q", got)
	}
	if !strings.Contains(plan.Why, "--resync") {
		t.Fatalf("要说明这是 --resync 触发的整轮重搬，得到 %q", plan.Why)
	}
	rep := r.resyncReport
	if rep == nil || rep.OldPosition != "postgres:v1:0/1A2B3C" || rep.OldPhase != "streaming" {
		t.Fatalf("报告要留住「从哪重建」：%+v", rep)
	}
	if strings.Join(rep.Tables, ",") != "a,b" {
		t.Fatalf("报告里的清表名单要跟 plan 一致，得到 %v", rep.Tables)
	}
}

// 老状态（升级前跑过、整本块账都没有）：分不清哪几张表有我们的行，只能整批清——
// 不清就补不掉这段时间源端删掉的行。判据必须与 markClean 同源（CopiedBefore）。
func TestApplyResyncFallsBackToAllTablesForLegacyState(t *testing.T) {
	r := resyncRunner("")
	plan := planFor(map[string]pgsink.ChunkProgress{}, "postgres:v1:0/1A2B3C", "done")
	// 前提：没有块标记 + 阶段说搬完了 → 常规判定是「不用搬」。
	if plan.Kind != copySkip {
		t.Fatalf("前提：常规判定应是不用搬，得到 %s", plan.Kind)
	}
	if len(plan.Written) != 0 || !plan.CopiedBefore {
		t.Fatalf("前提：没有块标记但搬过（CopiedBefore=true），得到 Written=%v CopiedBefore=%v",
			plan.Written, plan.CopiedBefore)
	}
	if err := r.applyResync(&plan, "done", "postgres:v1:0/1A2B3C"); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(plan.CleanNames, ","); got != "a,b,c" {
		t.Fatalf("老状态要整批清（a,b,c），得到 %q", got)
	}
}

// 首次搬（从没往目标端写过）：可以整轮搬，但**没有可清的表**。
func TestApplyResyncFirstRunCleansNothing(t *testing.T) {
	r := resyncRunner("")
	plan := planFor(map[string]pgsink.ChunkProgress{}, "", "")
	if err := r.applyResync(&plan, "", ""); err != nil {
		t.Fatal(err)
	}
	if plan.Kind != copyFresh {
		t.Fatalf("首次搬也是整轮搬，得到 %s", plan.Kind)
	}
	if plan.Clean != cleanNone || len(plan.CleanNames) != 0 {
		t.Fatalf("首次搬不该清任何表（尤其 append 复用的用户表），得到 %v %v", plan.Clean, plan.CleanNames)
	}
}

// continuous（只接增量）没有存量可重搬：--resync 只重建书签，不碰数据表。
func TestApplyResyncContinuousOnlyRebuildsBookmark(t *testing.T) {
	r := resyncRunner("continuous")
	r.srcEP.Kind = dsn.KindPostgres
	plan := planFor(map[string]pgsink.ChunkProgress{
		"a": {Done: true, LastNo: 3},
	}, "postgres:v1:0/1A2B3C", "streaming")
	if err := r.applyResync(&plan, "streaming", "postgres:v1:0/1A2B3C"); err != nil {
		t.Fatal(err)
	}
	if plan.Kind != copySkip {
		t.Fatalf("continuous 不搬存量，Plan 应为 skip，得到 %s", plan.Kind)
	}
	if plan.Clean != cleanNone || len(plan.CleanNames) != 0 {
		t.Fatalf("continuous 的 --resync 不许清表，得到 %v %v", plan.Clean, plan.CleanNames)
	}
	if !strings.Contains(plan.Why, "不搬存量") {
		t.Fatalf("要说清 continuous 只重建书签，得到 %q", plan.Why)
	}
}

// 不支持的位置必须**在动数据之前**拒绝，并且说清「什么都没动」。
func TestCheckResyncRejectsUnsupportedTargetAndMode(t *testing.T) {
	mysql := resyncRunner("")
	mysql.tgtEP.Kind = dsn.KindMySQL
	err := mysql.checkResync()
	if err == nil || !strings.Contains(err.Error(), "目标端是 mysql") {
		t.Fatalf("MySQL 目标端应被拒绝并说明原因，得到 %v", err)
	}
	if !strings.Contains(err.Error(), "什么都没动") {
		t.Fatalf("拒绝文案必须交代「什么都没动」，得到 %v", err)
	}

	interval := resyncRunner("interval")
	err = interval.checkResync()
	if err == nil || !strings.Contains(err.Error(), "interval 模式没有同步书签") {
		t.Fatalf("interval 模式应被拒绝并说明原因，得到 %v", err)
	}

	ok := resyncRunner("")
	if err := ok.checkResync(); err != nil {
		t.Fatalf("PG 目标端 + 默认模式应该允许 --resync，得到 %v", err)
	}
	if err := (&runner{}).checkResync(); err != nil {
		t.Fatalf("没带 --resync 时这一步必须是空操作，得到 %v", err)
	}

	// continuous 只接增量：--resync 的全部动作是「把位点挪到源端当前位置」。
	// PG 源能从书签回答；别的源还没有这个入口，必须拒绝而不是假装挪了位点。
	contMySQL := resyncRunner("continuous")
	contMySQL.srcEP.Kind = dsn.KindMySQL
	if err := contMySQL.checkResync(); err == nil || !strings.Contains(err.Error(), "没有书签") {
		t.Fatalf("continuous + 非 PG 源应被拒绝并说明原因，得到 %v", err)
	}
	contPG := resyncRunner("continuous")
	contPG.srcEP.Kind = dsn.KindPostgres
	if err := contPG.checkResync(); err != nil {
		t.Fatalf("continuous + PG 源应允许 --resync，得到 %v", err)
	}
}

// 「说了整轮重搬却没搬」必须是失败，不许以退出码 0 收尾。
func TestResyncWithoutSnapshotErrorIsLoud(t *testing.T) {
	msg := errResyncWithoutSnapshot.Error()
	for _, want := range []string{"--resync", "没有走到整轮重搬", "不会假装成功"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("兜底错误必须点明 %q，得到 %q", want, msg)
		}
	}
}
