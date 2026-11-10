//go:build integration

package engine_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/core"
	"github.com/spencerswagger/jinliang/internal/engine"
)

// 真造 SQLSTATE 53400（all replication slots are in use）的端到端：
// 把源端剩余书签名额用「占位物理书签」填满 → 引擎建自己的逻辑书签时必撞 53400
// → `runs.code` 必须是 E1016、`last_error` 含 53400 与驱动原始消息。
//
// 为什么不改 `max_replication_slots`：它是 **postmaster 级**参数，改了必须重启实例；
// 在共用实例上重启会打断别人。**填满名额**能得到一模一样的真实 53400，且事后只需
// 删掉我们自己建的占位书签（t.Cleanup 兜底）。
//
// 这条用例证明的是「真实错误链上 pgwire.SQLState 能穿透」——只测自己包装的
// *pgconn.PgError 证明不了这一点。
func TestSlotExhaustedEndToEndGetsE1016(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	srcConn := mustConn(t, srcDSN)
	ctx := context.Background()

	// 我先清掉本用例可能遗留的占位书签（幂等）
	dropFillSlots := func() {
		rows, err := srcConn.Query(ctx,
			`SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'jl_fill_%' AND NOT active`)
		if err != nil {
			return
		}
		var names []string
		for rows.Next() {
			var n string
			if rows.Scan(&n) == nil {
				names = append(names, n)
			}
		}
		rows.Close()
		for _, n := range names {
			_, _ = srcConn.Exec(ctx, `SELECT pg_drop_replication_slot($1)`, n)
		}
	}
	dropFillSlots()
	t.Cleanup(dropFillSlots)

	var maxSlots, used int
	if err := srcConn.QueryRow(ctx, `SELECT setting::int FROM pg_settings WHERE name='max_replication_slots'`).Scan(&maxSlots); err != nil {
		t.Fatal(err)
	}
	if err := srcConn.QueryRow(ctx, `SELECT count(*) FROM pg_replication_slots`).Scan(&used); err != nil {
		t.Fatal(err)
	}
	free := maxSlots - used
	if free <= 0 {
		// 名额被占满本身就该报（nightly 的 integration 步骤 grep `--- SKIP` 即失败，
		// 跳过会让整步红、消息却写「有用例被跳过」，排查的人会去找跳过原因而不是谁泄漏了名额）。
		t.Fatalf("源端书签名额已满（%d/%d）——不是跳过，是环境里有人泄漏了书签：先查 pg_replication_slots", used, maxSlots)
	}
	// 填满：只建**物理**占位书签（最便宜，且不影响逻辑复制）
	for i := 0; i < free; i++ {
		name := fmt.Sprintf("jl_fill_%d_%d", time.Now().UnixNano()%1e6, i)
		if _, err := srcConn.Exec(ctx, `SELECT pg_create_physical_replication_slot($1)`, name); err != nil {
			t.Fatalf("占位书签建不出来（第 %d/%d 个）：%v", i+1, free, err)
		}
	}
	var afterUsed int
	if err := srcConn.QueryRow(ctx, `SELECT count(*) FROM pg_replication_slots`).Scan(&afterUsed); err != nil {
		t.Fatal(err)
	}
	if afterUsed <= used {
		t.Fatalf("名额没被占满（%d → %d），构造不出 53400", used, afterUsed)
	}

	// 源端表要真的存在（否则先撞 E1013，测不到书签名额这条）
	probe := "slot_full_probe"
	exec(t, srcConn, "DROP TABLE IF EXISTS "+probe)
	exec(t, srcConn, "CREATE TABLE "+probe+" (id bigint PRIMARY KEY, v text)")
	exec(t, srcConn, "INSERT INTO "+probe+" VALUES (1,'x')")
	store, jobID := onceJobSingleTable(t, srcDSN, dstDSN, "slot-exhausted", probe)
	res, runErr := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, SnapshotOnly: true})
	if runErr == nil {
		dropFillSlots()
		t.Fatal("书签名额已满：建书签必须失败（不许静默成功）")
	}
	run, gerr := store.GetRun(ctx, res.RunID)
	if gerr != nil {
		t.Fatal(gerr)
	}
	dropFillSlots() // 先恢复，再断言（断言失败也别把名额占着）

	if run.Code != string(core.CodeSourceResourceLimit) {
		t.Fatalf("真 53400 应挂 %s，得到 %q；last_error=%s",
			core.CodeSourceResourceLimit, run.Code, run.LastError)
	}
	if !strings.Contains(run.LastError, "53400") {
		t.Fatalf("last_error 必须原样保留 SQLSTATE 53400，得到：%s", run.LastError)
	}
	if !strings.Contains(strings.ToLower(run.LastError), "replication slot") {
		t.Fatalf("last_error 要保留驱动原始消息（all replication slots are in use），得到：%s", run.LastError)
	}
	// 事后恢复的**校验**（不只看「我删了」）：配置没被改、占位书签 0 条。
	var maxAfter int
	if err := srcConn.QueryRow(ctx, `SELECT setting::int FROM pg_settings WHERE name='max_replication_slots'`).Scan(&maxAfter); err != nil {
		t.Fatal(err)
	}
	if maxAfter != maxSlots {
		t.Fatalf("实例配置被留在了别的值：max_replication_slots %d → %d", maxSlots, maxAfter)
	}
	var leftover int
	if err := srcConn.QueryRow(ctx, `SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'jl_fill_%'`).Scan(&leftover); err != nil {
		t.Fatal(err)
	}
	if leftover != 0 {
		t.Fatalf("占位书签没清干净（残留 %d 条），会害后面的用例", leftover)
	}
	t.Logf("真造 53400：占满 %d/%d → runs.code=%s，last_error 含 53400 与驱动消息；"+
		"事后校验 max_replication_slots 仍 %d、占位书签残留 0", afterUsed, maxSlots, run.Code, maxAfter)
}
