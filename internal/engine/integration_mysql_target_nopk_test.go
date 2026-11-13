//go:build integration

package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/engine"
)

// PG 源无主键表 → MySQL 目标端：全量 insert-only 搬完、行数一致，追加的 INSERT 也照写。
//
// 这是 S1（docs/DESIGN-NO-PRIMARY-KEY.md §5）在真库上的验收：这条路径以前会死在
// `ErrNoUniqueKey`（全量也走 Apply），现在按「源端无身份 → 只 INSERT」照跑。
// 改/删要触发按表暂停（S2），这里**不碰**——收到 UPDATE 时本片仍是明确失败，不静默降级。
func TestMySQLTargetInsertOnlyNoPKSource(t *testing.T) {
	srcDSN, dstDSN := mysqlTargetDSNs(t)
	src := mustConn(t, srcDSN)
	dst := mustMySQLTarget(t, dstDSN)
	table := "engine_my_nopk"

	exec(t, src, "DROP TABLE IF EXISTS "+table)
	exec(t, src, "CREATE TABLE "+table+" (payload text, amount numeric(12,2))") // 无主键
	exec(t, src, "INSERT INTO "+table+" SELECT 'row-'||g, (g%100)::numeric/100 FROM generate_series(1,20) g")
	t.Cleanup(func() { exec(t, src, "DROP TABLE IF EXISTS "+table) })

	_, _ = dst.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+table+"`")
	dropMySQLState(t, dst)

	store, jobID := newMySQLTargetJob(t, srcDSN, dstDSN, "my-target-nopk", table, "continuous+initial")

	res, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, StopAfter: 3 * time.Second})
	if err != nil {
		t.Fatalf("无主键表全量失败（应 insert-only 照跑）: %v", err)
	}
	if res.RowsCopied != 20 {
		t.Fatalf("应复制 20 行，实际 %d（阶段 %s）", res.RowsCopied, res.Phase)
	}
	if n := myCount(t, dst, table); n != 20 {
		t.Fatalf("目标端应有 20 行，实际 %d", n)
	}

	// 追加一行（无改/删）：insert-only 路径的增量照写。
	exec(t, src, "INSERT INTO "+table+" VALUES ('new-row', 1.23)")
	if _, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, StopAfter: 8 * time.Second}); err != nil {
		t.Fatalf("无主键表增量（纯 INSERT）失败: %v", err)
	}
	if n := myCount(t, dst, table); n != 21 {
		t.Fatalf("追加后目标端应有 21 行，实际 %d", n)
	}
}
