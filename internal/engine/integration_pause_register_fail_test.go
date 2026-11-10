//go:build integration

package engine_test

// A3（契约 §1.5 第 2 条「登记先行」）的**可证伪**用例：
// 暂停登记失败时，这一批必须失败——目标端 0 行、位点不动——**不许**「跳过了但没记下」。
//
// 为什么这条重要：跳过语义一旦允许「先跳、登记失败也无所谓」，用户看到的是一张结构没对齐、
// 数据还少了一段的表，而 `paused_tables` 里什么都没有——连「该重搬」这件事都没人知道。
// 这是整条按表暂停里唯一一个**静默缺数据**的入口。
//
// 怎么把「登记失败」造出来（不用测试钩子）：给 `_jl_meta` 挂一个 BEFORE UPDATE 触发器，
// 一旦要改 `paused_tables` 就 RAISE。读路径照常（所以 prepare 仍能跑），
// 只有写入失败——正是我们要的那种失败。
//
// 为什么用触发器而不是 `REVOKE UPDATE`（Owl II 也问过这条）：
//  1. 测试/CI 连的 `jinliang` 是超级用户，`REVOKE` **对它不生效**，构造会静默失效；
//  2. 就算生效，它拦的是**整个** `_jl_meta` 写路径（`EnsureMeta` 的 upsert、租约心跳），
//     prepare 会在到达「登记」之前就失败 —— 测到的就不是 A3 了；
//  3. 触发器用 `NEW.paused_tables IS DISTINCT FROM OLD.paused_tables` 把范围收在**这一列**，
//     而且它是**自证**的：触发器没生效 → 这一轮会成功 → 下面第 ④ 步直接红，
//     不需要靠注释声称「构造是真的」。
//  4. 引擎侧一行都没为测试改过：`PauseTable` 里那条 UPDATE 就是生产那条，
//     触发器产生的是数据库侧的真实拒绝（与盘满/锁等待/权限被收回在引擎侧不可区分）。
//
// 反向注入（Gate 门禁第 10 条，两个注入点任选其一，用例都必须红）：
//  1. `internal/pgsink/paused.go` 的 `PauseTable` 开头加 `return nil`
//     → 登记变成「无条件成功」，run 不再失败 → 「这一轮必须失败」那条断言红；
//  2. `internal/engine/engine.go` prepare 里
//     `if err := sink.PauseTable(ctx, ref, rec); err != nil { return … }` 改成忽略错误
//     → 同样继续跑 → 同一条断言红。
//     （注意：把 `registerPause` 整个置空**不会**红——那只是「不登记」，随后重试仍会撞同一个
//     PausableError 并上抛，run 照样失败。真正要注入的是**吞掉登记失败**。）

import (
	"context"
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/engine"
)

const pauseBlockTrigger = `
CREATE OR REPLACE FUNCTION jl_block_pause_write() RETURNS trigger AS $$
BEGIN
  IF NEW.paused_tables IS DISTINCT FROM OLD.paused_tables THEN
    RAISE EXCEPTION 'test: paused_tables 写入被拦住（模拟登记失败）';
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;`

func TestPauseRegistrationFailureFailsRunAndWritesNothing(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	srcDB := mustConn(t, srcDSN)
	dstDB := mustConn(t, dstDSN)
	ctx := context.Background()
	table := "pause_reg_fail"

	for _, stmt := range []string{
		"DROP TABLE IF EXISTS " + table,
		"CREATE TABLE " + table + " (id bigint PRIMARY KEY, v bigint)",
		"INSERT INTO " + table + " SELECT g, g FROM generate_series(1,100) g",
	} {
		exec(t, srcDB, stmt)
	}
	exec(t, dstDB, "DROP TABLE IF EXISTS "+table)
	exec(t, dstDB, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")

	store, jobID := continuousJobBoth(t, srcDSN, dstDSN, "pause-reg-fail", table)
	// ① 首搬：把状态行、块标记都建起来（后面才有「位点不变」可断言）
	if _, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, SnapshotOnly: true}); err != nil {
		t.Fatalf("首搬失败: %v", err)
	}
	job, err := store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	var posBefore string
	if err := dstDB.QueryRow(ctx, "SELECT COALESCE(position,'') FROM _jl_meta WHERE job_key=$1",
		job.JobKey).Scan(&posBefore); err != nil {
		t.Fatal(err)
	}
	rowsBefore := countRowsIn(t, dstDB, table)

	// ② 源端改类型：下一次 prepare 会判「结构对不上」→ 走登记暂停这条路
	exec(t, srcDB, "ALTER TABLE "+table+" ALTER COLUMN v TYPE numeric(12,2)")
	exec(t, srcDB, "INSERT INTO "+table+" (id,v) VALUES (101, 1.25)")

	// ③ 让登记必然失败（只拦写、不拦读）
	exec(t, dstDB, pauseBlockTrigger)
	exec(t, dstDB, `DROP TRIGGER IF EXISTS jl_block_pause ON _jl_meta`)
	exec(t, dstDB, `CREATE TRIGGER jl_block_pause BEFORE UPDATE ON _jl_meta
		FOR EACH ROW EXECUTE FUNCTION jl_block_pause_write()`)
	t.Cleanup(func() {
		_, _ = dstDB.Exec(context.Background(), `DROP TRIGGER IF EXISTS jl_block_pause ON _jl_meta`)
		_, _ = dstDB.Exec(context.Background(), `DROP FUNCTION IF EXISTS jl_block_pause_write()`)
	})

	// ④ 这一**轮**必须失败（A3 在 prepare 期的形状：登记失败 → 整轮停，一行都不搬）。
	//    「这一批」那条（流式期，已经跑起来、批里混着别的表）由
	//    `pause_skip_test.go` 的 `TestPauseSinkApplyFailsWholeBatchWhenRegistrationFails` 钉。
	res, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, SnapshotOnly: true})
	if err == nil {
		t.Fatal("登记暂停失败时这一轮必须失败：否则「跳过了但没记下」= 静默缺数据")
	}
	if !strings.Contains(err.Error(), "暂停") {
		t.Fatalf("报错要指向「登记暂停失败」，得到：%v", err)
	}
	run, gerr := store.GetRun(ctx, res.RunID)
	if gerr != nil {
		t.Fatal(gerr)
	}
	if run.FinishReason != "failed" {
		t.Fatalf("这一轮的 finish_reason 应为 failed，得到 %q（code=%q，last_error=%s）",
			run.FinishReason, run.Code, run.LastError)
	}

	// ⑤ 目标端一行都不该多、位点逐字节不动
	if got := countRowsIn(t, dstDB, table); got != rowsBefore {
		t.Fatalf("登记失败时不许写入任何行：之前 %d 行，现在 %d 行", rowsBefore, got)
	}
	var posAfter string
	if err := dstDB.QueryRow(ctx, "SELECT COALESCE(position,'') FROM _jl_meta WHERE job_key=$1",
		job.JobKey).Scan(&posAfter); err != nil {
		t.Fatal(err)
	}
	if posAfter != posBefore {
		t.Fatalf("登记失败时位点必须逐字节不动：%q → %q", posBefore, posAfter)
	}
	// ⑥ 也不许留下半条暂停记录（触发器拦的就是这条写入）
	var paused string
	if err := dstDB.QueryRow(ctx, "SELECT COALESCE(paused_tables,'') FROM _jl_meta WHERE job_key=$1",
		job.JobKey).Scan(&paused); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(paused, table) {
		t.Fatalf("登记失败却留下了记录（那说明写入其实成功了，用例的构造是假的）：%q", paused)
	}
	t.Logf("A3：登记失败 → run failed、目标端仍 %d 行、位点 %q 未变、无暂停记录",
		rowsBefore, posBefore)
}
