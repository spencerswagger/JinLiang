//go:build integration && exitcode3

package engine_test

// Panel 裁决（基于 `c72f36c`）：**有暂停表的 run 不许退 0**，要退 `ExitNeedsAction = 3`。
// 派生单点：失败 1 > 用户停止 130 > 正常收尾且有暂停表 3 > 0。
//
// 这条用例现在**必然是红的**：主干还没有 3 这一档（`ExitOK/ExitFailed/ExitStopped` 之外
// 没有第四个常量，暂停场景走 `drain(ExitOK)`）。所以它挂在构建标签 `exitcode3` 后面，
// 由 `merge-gate` 的**跟踪 job**（不阻塞合并）每天跑；实现落地那天它会变绿，
// 而 `known-red.py` 的 verify 会判「断言未激活」逼着把它转成阻塞步骤。
//
// 为什么单独一条、不复用暂停用例：行为所有权归 Gate，但**断言面**是新的——
// 原来只断言「只停一张表 / 位点前进 / 恢复追平」，没有一条盯**退出码**。
// 这正是 Panel 说的「等于这个 bug 没被门禁覆盖」。

import (
	"context"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/engine"
)

// needsAction 与 Panel 裁决的取值一致（主干还没有这个常量，落地后改回引用常量）。
const needsAction = 3

func TestPausedRunExitsNeedsAction(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	srcDB := mustConn(t, srcDSN)
	dstDB := mustConn(t, dstDSN)
	ctx := context.Background()

	const ta = "exit3_a"
	exec(t, srcDB, "DROP TABLE IF EXISTS "+ta)
	exec(t, srcDB, "CREATE TABLE "+ta+" (id bigint PRIMARY KEY, payload text, v bigint)")
	exec(t, srcDB, "INSERT INTO "+ta+" SELECT g,'r'||g,g FROM generate_series(1,50) g")
	exec(t, dstDB, "DROP TABLE IF EXISTS "+ta)
	exec(t, dstDB, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")

	store, jobID := continuousJobBoth(t, srcDSN, dstDSN, "exit3", ta)

	// ① 首搬成功（此时没有暂停表）→ 应退 0
	if res, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, SnapshotOnly: true}); err != nil {
		t.Fatalf("首搬失败: %v", err)
	} else if res.ExitCode != 0 {
		t.Fatalf("没有暂停表时应退 0，实际 %d", res.ExitCode)
	}

	// ② 源端改类型 + 插一行 → 这张表被暂停（裁决 A：只停该表、位点前进、run 本身不失败）
	exec(t, srcDB, "ALTER TABLE "+ta+" ALTER COLUMN v TYPE numeric(12,2)")
	exec(t, srcDB, "INSERT INTO "+ta+" (id,payload,v) VALUES (51,'after-ddl',1.25)")

	res, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, IdleAfter: 500 * time.Millisecond})
	if err != nil {
		t.Fatalf("表级暂停不该让整轮失败（裁决 A）：%v", err)
	}
	if res.ExitCode != needsAction {
		t.Fatalf("有暂停表必须退 %d（Panel 裁决：失败 1 > 停止 130 > 欠账 3 > 0），实际 %d",
			needsAction, res.ExitCode)
	}
	// 反向：没有暂停表时不许退 3（别把 3 写成「只要跑过就 3」）
	// ③ 对齐后重跑：不再有暂停表 → 回到 0
	exec(t, dstDB, "ALTER TABLE "+ta+" ALTER COLUMN v TYPE numeric(12,2)")
	res2, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, IdleAfter: 500 * time.Millisecond})
	if err != nil {
		t.Fatalf("对齐后重跑失败: %v", err)
	}
	if res2.ExitCode == needsAction {
		t.Fatalf("对齐且没有暂停表时不该再退 %d（说明 3 被写成了「跑过就退」）", needsAction)
	}
}
