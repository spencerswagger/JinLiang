//go:build integration

package engine_test

// prepare 期的结构漂移：**目标端有、源端没有的列**。
//
// 两条红线各一格：
//  1. 本任务建的表 → E1015 + reason=schema_drift，last_error 点名表与列，位点不动；
//  2. 用户自己的表（`_jl_object` 无登记）→ 只出 note、不报错（多出的列可能是用户自己的）。
// 变异：去掉 prepare 期比对 → 两格都红（第一格会静默加列、第二格没有 note）。

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/core"
	"github.com/spencerswagger/jinliang/internal/engine"
	"github.com/spencerswagger/jinliang/internal/pgsink"
)

func TestPrepareDriftOwnTableGetsE1015(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	srcDB := mustConn(t, srcDSN)
	dstDB := mustConn(t, dstDSN)
	table := "drift_ours"
	ctx := context.Background()

	// 源端：只有 id, v（模拟「源端删掉了 old_col」或「源端把它改名了」）
	exec(t, srcDB, "DROP TABLE IF EXISTS "+table)
	exec(t, srcDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, v text)")
	exec(t, srcDB, "INSERT INTO "+table+" VALUES (1,'x')")
	// 目标端：我们建的表（`_jl_object` 登记），但多一列 legacy
	exec(t, dstDB, "DROP TABLE IF EXISTS "+table)
	exec(t, dstDB, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")
	exec(t, dstDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, v text, legacy text)")

	store, jobID := onceJobSingleTable(t, srcDSN, dstDSN, "drift-ours", table)
	ensureStateTables(t, store, dstDSN, jobID)
	markTableCreatedByJob(t, store, dstDB, jobID, table) // 登记成「本任务建的」

	// **锁重入的防回归**：`reconcileColumns` 由已持 `s.mu` 的 `EnsureSchema` 调用，
	// 里面若误用加锁版 `DidCreateTable` 会自死锁（曾经实测挂到 300s 超时才被发现）。
	// 这里给 30s 上限：真死锁时立刻红，而不是把整包测试拖到超时。
	runCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	res, err := engine.Run(runCtx, engine.Options{Store: store, JobID: jobID, SnapshotOnly: true})
	if err == nil {
		t.Fatal("我们建的表多出源端没有的列：prepare 必须报结构漂移，不能静默加列")
	}
	run, gerr := store.GetRun(ctx, res.RunID)
	if gerr != nil {
		t.Fatal(gerr)
	}
	if run.Code != string(core.CodeSchemaMismatch) {
		t.Fatalf("应挂 %s，得到 %q；last_error=%s", core.CodeSchemaMismatch, run.Code, run.LastError)
	}
	for _, want := range []string{table, "legacy", pgsink.PauseReasonSchemaDrift} {
		if !strings.Contains(run.LastError, want) {
			t.Fatalf("last_error 要点名表、列与原因码 %q，得到：%s", want, run.LastError)
		}
	}
	t.Logf("自有表漂移：runs.code=%s，last_error 点名 %s/legacy", run.Code, table)
}

func TestPrepareDriftUserTableOnlyNotes(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	srcDB := mustConn(t, srcDSN)
	dstDB := mustConn(t, dstDSN)
	table := "drift_user"
	ctx := context.Background()

	exec(t, srcDB, "DROP TABLE IF EXISTS "+table)
	exec(t, srcDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, v text)")
	exec(t, srcDB, "INSERT INTO "+table+" VALUES (1,'x')")
	// 目标端：**用户自己的表**（不登记 `_jl_object`），多一列 mine
	exec(t, dstDB, "DROP TABLE IF EXISTS "+table)
	exec(t, dstDB, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")
	exec(t, dstDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, v text, mine text)")
	exec(t, dstDB, "INSERT INTO "+table+" VALUES (1,'x','我的列')")

	store, jobID := onceJobSingleTable(t, srcDSN, dstDSN, "drift-user", table)

	// 订阅事件流：这条 note 是「用户表降级」唯一的可观测出口，必须真断言到
	//（第一版只写了「note 由 PrepareNotes 单测覆盖」，而全仓零单测 → 删掉 emit 也全绿）。
	ch, stopSub := store.Bus().Subscribe(jobID)
	defer stopSub()
	sawNote := make(chan string, 64)
	go func() {
		for ev := range ch {
			if ev.Message != "" && strings.Contains(ev.Message, "不判为漂移") {
				sawNote <- ev.Message
			}
		}
	}()

	if _, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, SnapshotOnly: true}); err != nil {
		t.Fatalf("用户表多出的列不算漂移：不该失败，得到 %v", err)
	}
	select {
	case m := <-sawNote:
		if !strings.Contains(m, "不是本任务建的") {
			t.Fatalf("note 要说清「不是本任务建的」，得到：%s", m)
		}
		if !strings.Contains(m, table) {
			t.Fatalf("note 要点名那张表，得到：%s", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("用户表多出源端没有的列：必须留下可观测的 note（不许无声）")
	}
	// 用户那列的数据不能被我们弄丢
	var mine string
	if err := dstDB.QueryRow(ctx, "SELECT mine FROM "+table+" WHERE id=1").Scan(&mine); err != nil {
		t.Fatal(err)
	}
	if mine != "我的列" {
		t.Fatalf("用户自己的列被改动了：%q", mine)
	}
	// 这里同时钉三件：用户表不判漂移、不报错、用户列不被我们动 + **note 真的发出去了**。
	t.Logf("用户表多出的列：不报错、用户列数据完好（%q）", mine)
}
