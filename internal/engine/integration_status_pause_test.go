//go:build integration

package engine_test

// `status` 的暂停态**必须读得出来**（Owl II 复核 #109 必改 1）。
//
// 改前：`StatusFor` 造的 runner 没有 sink / pauseReg，`pauseRegistryOf()` 恒 nil →
// `status --json` 的 `paused_tables` 永远为空、PG 端的 `paused_note` 从不出现；
// MySQL 端更糟：还在打印「按表暂停在 MySQL 目标端暂不支持」（#102 早就支持了）。
//
// 这组用例钉三件事（两种目标端各一遍）：
//  1. 没有暂停 → `paused_tables` 是**空数组**（不是 null）且没有 note；
//  2. 有暂停 → 清单里有那张表（表名 / 原因码 / 跳过行数都对得上），note 为空；
//  3. 那一列是脏 JSON → 清单是 null + note 明说「不是没有暂停」。
//
// 为什么直接 UPDATE 那一列而不是走一次真实的类型漂移：这里测的是**读路径**
// （写路径由 integration_paused_test.go / integration_mysql_target_pause_test.go 覆盖）。
// 那条 UPDATE 与 `PauseTable` 落库的形状逐字一致：同一张表、同一列、同一份 JSON。

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/engine"
	"github.com/spencerswagger/jinliang/internal/pgsink"
)

func TestStatusReportsPausedTablesPGTarget(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	srcDB := mustConn(t, srcDSN)
	dstDB := mustConn(t, dstDSN)
	ctx := context.Background()

	table := "status_pause_pg"
	exec(t, srcDB, "DROP TABLE IF EXISTS "+table)
	exec(t, srcDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, payload text)")
	exec(t, srcDB, "INSERT INTO "+table+" SELECT g,'r'||g FROM generate_series(1,50) g")
	t.Cleanup(func() {
		exec(t, srcDB, "DROP TABLE IF EXISTS "+table)
		exec(t, dstDB, "DROP TABLE IF EXISTS "+table)
	})
	exec(t, dstDB, "DROP TABLE IF EXISTS "+table)
	exec(t, dstDB, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")

	store, jobID := continuousJobBoth(t, srcDSN, dstDSN, "status-pause-pg", table)
	job, err := store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	dropJobSlot(t, srcDSN, job.JobKey)

	if _, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, SnapshotOnly: true}); err != nil {
		t.Fatalf("首搬失败: %v", err)
	}

	// ① 没有暂停：空数组 + 没有 note（「没有暂停」与「读不到」必须是两种形状）。
	st, err := engine.StatusFor(ctx, engine.Options{Store: store, JobID: jobID})
	if err != nil {
		t.Fatalf("status 失败: %v", err)
	}
	if st.PausedTables == nil {
		t.Fatalf("目标端读得到、确实没有暂停时，paused_tables 必须是空数组（null 表示「读不到」）。note=%q", st.PausedNote)
	}
	if len(st.PausedTables) != 0 {
		t.Fatalf("还没暂停，清单却非空：%#v", st.PausedTables)
	}
	if st.PausedNote != "" {
		t.Fatalf("没有暂停时不该有 paused_note，得到 %q", st.PausedNote)
	}

	// ② 登记一张暂停（与 pgsink.PauseTable 落库的形状逐字一致）。
	rec := map[string]pgsink.PausedTable{table: {
		Reason: "type_changed", At: "2026-11-12T02:00:00Z",
		Detail:         "列 payload：目标端 text / 源端 bigint",
		Resume:         pgsink.ResumeRecopy,
		SkippedRows:    37,
		SkippedFrom:    "0/2000000",
		LastSkippedPos: "0/2000A00",
	}}
	blob, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	exec(t, dstDB, "UPDATE _jl_meta SET paused_tables=$1 WHERE job_key=$2", string(blob), job.JobKey)

	st, err = engine.StatusFor(ctx, engine.Options{Store: store, JobID: jobID})
	if err != nil {
		t.Fatalf("status 失败: %v", err)
	}
	if len(st.PausedTables) != 1 {
		t.Fatalf("有暂停时 status 必须列出那张表（改前这里恒为空），得到 %#v（paused_note=%q）",
			st.PausedTables, st.PausedNote)
	}
	got := st.PausedTables[0]
	if got.Table != table || got.Reason != "type_changed" || got.SkippedRows != 37 ||
		got.SkippedFrom != "0/2000000" || got.LastSkippedPos != "0/2000A00" {
		t.Fatalf("暂停表的字段没读全：%#v", got)
	}
	if !strings.Contains(got.Detail, "源端 bigint") {
		t.Fatalf("detail 要原样带出来（诊断靠它），得到 %q", got.Detail)
	}
	if st.PausedNote != "" {
		t.Fatalf("读得到时不该有 note，得到 %q", st.PausedNote)
	}

	// ③ 脏 JSON：读不到 → null + note（绝不能装成「没有暂停」）。
	exec(t, dstDB, `UPDATE _jl_meta SET paused_tables='{"`+table+`": ' WHERE job_key=$1`, job.JobKey)
	st, err = engine.StatusFor(ctx, engine.Options{Store: store, JobID: jobID})
	if err != nil {
		t.Fatalf("status 失败: %v", err)
	}
	if st.PausedTables != nil {
		t.Fatalf("脏 JSON 时清单必须是 null（未知），得到 %#v", st.PausedTables)
	}
	if !strings.Contains(st.PausedNote, "不是合法 JSON") {
		t.Fatalf("脏 JSON 必须落到 paused_note，得到 %q", st.PausedNote)
	}
}

func TestStatusReportsPausedTablesMySQLTarget(t *testing.T) {
	srcDSN, dstDSN := mysqlTargetDSNs(t)
	srcDB := mustConn(t, srcDSN)
	dstDB := mustMySQLTarget(t, dstDSN)
	ctx := context.Background()

	table := "engine_my_status_pause"
	exec(t, srcDB, "DROP TABLE IF EXISTS "+table)
	exec(t, srcDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, payload text)")
	exec(t, srcDB, "INSERT INTO "+table+" SELECT g,'r'||g FROM generate_series(1,20) g")
	t.Cleanup(func() {
		exec(t, srcDB, "DROP TABLE IF EXISTS "+table)
		_, _ = dstDB.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+table+"`")
	})
	_, _ = dstDB.ExecContext(ctx, "DROP TABLE IF EXISTS `"+table+"`")
	dropMySQLState(t, dstDB)

	store, jobID := newMySQLTargetJob(t, srcDSN, dstDSN, "my-status-pause", table, "once")
	job, err := store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID}); err != nil {
		t.Fatalf("全量失败: %v", err)
	}

	// ① 没有暂停：空数组、没有 note —— 而且**不许**再有那句过期的「暂不支持」。
	st, err := engine.StatusFor(ctx, engine.Options{Store: store, JobID: jobID})
	if err != nil {
		t.Fatalf("status 失败: %v", err)
	}
	if st.PausedTables == nil {
		t.Fatalf("MySQL 目标端读得到时也要给空数组（null 只留给「读不到」）。note=%q", st.PausedNote)
	}
	if len(st.PausedTables) != 0 {
		t.Fatalf("还没暂停，清单却非空：%#v", st.PausedTables)
	}
	if st.PausedNote != "" {
		t.Fatalf("没有暂停时不该有 paused_note，得到 %q", st.PausedNote)
	}
	if strings.Contains(st.PausedNote, "暂不支持") {
		t.Fatalf("「按表暂停在 MySQL 目标端暂不支持」是过期文案（#102 已支持），不该再出现：%q", st.PausedNote)
	}

	// ② 登记一张暂停（与 mysqlsink.PauseTable 落库的形状逐字一致）。
	blob := `{"` + table + `":{"reason":"schema_drift","at":"2026-11-12T02:00:00Z",` +
		`"detail":"列 payload 在目标端不存在","resume":"recopy","skipped_rows":9,` +
		`"skipped_from":"mysql-bin.000003:1200","last_skipped_pos":"mysql-bin.000003:4800"}}`
	if _, err := dstDB.ExecContext(ctx,
		"UPDATE `_jl_meta` SET paused_tables = ? WHERE job_key = ?", blob, job.JobKey); err != nil {
		t.Fatalf("写暂停记录失败: %v", err)
	}

	st, err = engine.StatusFor(ctx, engine.Options{Store: store, JobID: jobID})
	if err != nil {
		t.Fatalf("status 失败: %v", err)
	}
	if len(st.PausedTables) != 1 {
		t.Fatalf("MySQL 目标端有暂停时 status 必须列出来（改前恒为空/打印「暂不支持」），得到 %#v（paused_note=%q）",
			st.PausedTables, st.PausedNote)
	}
	got := st.PausedTables[0]
	if got.Table != table || got.Reason != "schema_drift" || got.SkippedRows != 9 ||
		got.LastSkippedPos != "mysql-bin.000003:4800" {
		t.Fatalf("暂停表的字段没读全：%#v", got)
	}
	if st.PausedNote != "" {
		t.Fatalf("读得到时不该有 note，得到 %q", st.PausedNote)
	}

	// ③ 脏 JSON → null + note。
	if _, err := dstDB.ExecContext(ctx,
		"UPDATE `_jl_meta` SET paused_tables = ? WHERE job_key = ?", "{oops", job.JobKey); err != nil {
		t.Fatalf("写脏 JSON 失败: %v", err)
	}
	st, err = engine.StatusFor(ctx, engine.Options{Store: store, JobID: jobID})
	if err != nil {
		t.Fatalf("status 失败: %v", err)
	}
	if st.PausedTables != nil {
		t.Fatalf("脏 JSON 时清单必须是 null（未知），得到 %#v", st.PausedTables)
	}
	if !strings.Contains(st.PausedNote, "不是合法 JSON") {
		t.Fatalf("脏 JSON 必须落到 paused_note，得到 %q", st.PausedNote)
	}
}
