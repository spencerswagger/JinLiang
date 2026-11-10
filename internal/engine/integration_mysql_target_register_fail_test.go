//go:build integration

package engine_test

// A3「登记失败 → 不许「跳过了但没记下」」的 **MySQL 目标端**覆盖。
//
// 为什么这一份归我（PG 目标端那份归 Gate，避免两个 owner 各写一份互不认账）：
// Gate 的 `TestPauseRegistrationFailureFailsWholeBatch` 直测 `pauseSink.Apply` 写路径（假目标端）；
// 这一份走**真 MySQL 目标端 + 真引擎入口**，钉的是「prepare 期发现结构不一致 → 登记暂停 →
// 登记失败 → 这一轮一行都不搬」。两条路径不同（`registerPause` 的两个调用点），行为同一条：
// **登记不成功就不许跳**。
//
// 造故障（不碰引擎代码）：目标端 `_jl_meta` 上挂 `BEFORE UPDATE` 触发器 —— `paused_tables`
// 一被更新就往探针表记一笔再 RAISE（等价于「列被锁 / 只读账号 / 磁盘满」）。
// 探针表同时给出**结构性判据**：触发器被点了几次 = 登记尝试了几次。重试就会 > 1。
//
// 四条断言（与 Gate 那条同等强度）：
//  1. 这一轮**失败**（不是「跳过了但没记下」）；
//  2. 错误**可归因**：`registerPause` 的包裹语 + 触发器原话都在；
//  3. **不许原地重试**：探针表计数 == 1（登记失败立刻整轮失败）；
//  4. `_jl_meta.paused_tables` 里**没有半条记录**。

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/engine"
)

const myPauseBlockTrigger = `
CREATE OR REPLACE TRIGGER jl_block_pause_upd BEFORE UPDATE ON _jl_meta
FOR EACH ROW
BEGIN
  IF NEW.paused_tables IS NOT NULL AND (OLD.paused_tables IS NULL OR NEW.paused_tables <> OLD.paused_tables) THEN
    INSERT INTO _jl_pause_probe (n) VALUES (1);
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'test: paused_tables 写入被拦住（模拟登记失败）';
  END IF;
END`

func TestMySQLTargetRegisterFailureFailsRunWithoutRetry(t *testing.T) {
	srcDSN, dstDSN := mysqlTargetDSNs(t)
	srcDB := mustConn(t, srcDSN)
	myDB := mustMySQLTarget(t, dstDSN)
	ctx := context.Background()
	table := "myt_reg_fail"

	for _, stmt := range []string{
		"DROP TABLE IF EXISTS " + table,
		"CREATE TABLE " + table + " (id bigint PRIMARY KEY, payload text, v bigint)",
		"INSERT INTO " + table + " SELECT g, 'r'||g, g FROM generate_series(1,100) g",
	} {
		exec(t, srcDB, stmt)
	}
	_, _ = myDB.ExecContext(ctx, "DROP TABLE IF EXISTS `"+table+"`")
	dropMySQLState(t, myDB)
	t.Cleanup(func() {
		_, _ = myDB.ExecContext(context.Background(), "DROP TRIGGER IF EXISTS jl_block_pause_upd")
		_, _ = myDB.ExecContext(context.Background(), "DROP TABLE IF EXISTS `_jl_pause_probe`")
		_, _ = myDB.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+table+"`")
	})

	store, jobID := newMySQLTargetJobMulti(t, srcDSN, dstDSN, "myt-reg-fail", "continuous+initial", table)
	// ① 首搬：把状态行、位点、块标记都建起来（后面才有「位点不动」可断言）
	if _, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, SnapshotOnly: true}); err != nil {
		t.Fatalf("首搬失败: %v", err)
	}
	posBefore := myPausedPosition(t, myDB)
	rowsBefore := myCount(t, myDB, table)

	// ② 源端改列类型：下次 prepare 的「源端列 vs 目标端实际列」对账会判结构不一致
	exec(t, srcDB, "ALTER TABLE "+table+" ALTER COLUMN v TYPE numeric(12,2)")

	// ③ 让登记必然失败（只拦 `paused_tables` 这一列的更新，其它路径照常）
	for _, stmt := range []string{
		"DROP TRIGGER IF EXISTS jl_block_pause_upd",
		"DROP TABLE IF EXISTS `_jl_pause_probe`",
		"CREATE TABLE `_jl_pause_probe` (n INT NOT NULL)",
		myPauseBlockTrigger,
	} {
		if _, err := myDB.ExecContext(ctx, stmt); err != nil {
			t.Fatalf("造故障失败（%s）: %v", stmt, err)
		}
	}

	// ④ 这一轮必须失败
	res, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, SnapshotOnly: true})
	if err == nil {
		t.Fatal("登记暂停失败时这一轮必须失败：否则「跳过了但没记下」= 静默缺数据")
	}
	// 断言 2：错误可归因（登记失败的包裹语 + 触发器原话）。
	// 注意走的是 **prepare 期对账**那条登记入口，包裹语是「登记表 X 的暂停失败（本轮一行都没搬）」；
	// 写路径那条入口的包裹语是「另外：登记这张表的暂停状态也失败了」（Gate 的用例覆盖那条）。
	for _, want := range []string{"的暂停失败", "paused_tables 写入被拦住"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("错误要可归因（应含 %q），得到：%v", want, err)
		}
	}
	run, gerr := store.GetRun(ctx, res.RunID)
	if gerr != nil {
		t.Fatal(gerr)
	}
	if run.FinishReason != "failed" {
		t.Fatalf("finish_reason 应为 failed，得到 %q（code=%q last_error=%s）",
			run.FinishReason, run.Code, run.LastError)
	}

	// 断言 3：**不许原地重试**（结构性判据）——登记尝试次数必须正好 1
	attempts := myProbeCount(t, myDB)
	if attempts != 1 {
		t.Fatalf("登记只该尝试 1 次（失败就整轮停），探针记到 %d 次：这就是「原地重试」", attempts)
	}

	// 断言 4：没有半条记录 + 一行都没多搬
	if rec := myPausedRecord(t, myDB, table); rec != nil {
		t.Fatalf("登记失败却留下了记录（构造是假的）：%+v", *rec)
	}
	if got := myCount(t, myDB, table); got != rowsBefore {
		t.Fatalf("登记失败时不许写入任何行：之前 %d 行，现在 %d 行", rowsBefore, got)
	}
	if pos := myPausedPosition(t, myDB); pos != posBefore {
		t.Fatalf("登记失败时位点必须逐字节不动：%q → %q", posBefore, pos)
	}
	t.Logf("MySQL 目标端 A3：登记失败 → 整轮 failed（错误可归因）、登记只尝试 %d 次、"+
		"无暂停记录、目标端仍 %d 行、位点 %q 未变", attempts, rowsBefore, posBefore)
}

// myProbeCount 数触发器被点了几次（= 登记尝试次数）。
func myProbeCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM `_jl_pause_probe`").Scan(&n); err != nil {
		t.Fatalf("读探针计数失败: %v", err)
	}
	return n
}
