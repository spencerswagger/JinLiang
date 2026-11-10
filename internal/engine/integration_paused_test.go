//go:build integration

package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/engine"
	"github.com/spencerswagger/jinliang/internal/pgsink"
)

// 按表暂停（docs/CONTRACT-PAUSE-AND-POSITION.md §1.5/§1.6）：**只停该表**。
//
// 契约要点（改这段前先读）：
//   - 命中暂停表的行**整条跳过**（不写、不计指纹），其它表照写，位点**照常前进**；
//     ——旧行为「整批拒绝、位点不动、其它表也停」已被否决（DESIGN.md §4 本来就是表级）；
//   - key = 目标端裸表名，与 `_jl_chunk.table_name` 逐字节相同；
//   - 跳过窗口记在 `skipped_rows`/`skipped_from` 里（可枚举、可补偿）；
//   - 恢复**不是**「结构对齐就解除」：对齐只把该表标成「欠一次整表重搬」（清块标记），
//     真正解除发生在重搬搬成那一刻；重搬要清表（暂停窗口里的 DELETE 看不见，只 upsert 会留幻行）。
func TestPauseTableRegistersKeyKeepsPositionAndRecovers(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	srcDB := mustConn(t, srcDSN)
	dstDB := mustConn(t, dstDSN)
	ctx := context.Background()

	ta, tb := "pause_a", "pause_b"
	for _, tbl := range []string{ta, tb} {
		exec(t, srcDB, "DROP TABLE IF EXISTS "+tbl)
		exec(t, srcDB, "CREATE TABLE "+tbl+" (id bigint PRIMARY KEY, payload text, v bigint)")
		exec(t, srcDB, "INSERT INTO "+tbl+" SELECT g,'r'||g,g FROM generate_series(1,300) g")
		exec(t, dstDB, "DROP TABLE IF EXISTS "+tbl)
	}
	exec(t, dstDB, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")

	store, jobID := continuousJobBoth(t, srcDSN, dstDSN, "pause-two", ta, tb)

	// ① 首搬（两张表都把存量搬完）
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
	if posBefore == "" {
		t.Fatal("首搬之后应有权威位点")
	}

	// ② 源端改 A 的类型（这一批会撞 TypeChangedError），同时给两张表各插一行
	exec(t, srcDB, "ALTER TABLE "+ta+" ALTER COLUMN v TYPE numeric(12,2)")
	exec(t, srcDB, "INSERT INTO "+ta+" (id,payload,v) VALUES (301,'after-ddl',1.25)")
	exec(t, srcDB, "INSERT INTO "+tb+" (id,payload,v) VALUES (301,'b-after',7)")

	// ③ 这一轮**不再整轮失败**：A 被暂停、A 的行不写、B 的行照写、位点照常前进。
	res, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, IdleAfter: 500 * time.Millisecond})
	if err != nil {
		t.Fatalf("只有一张表结构对不上，不该打死整轮（其它表要继续跑）: %v", err)
	}
	run, gerr := store.GetRun(ctx, res.RunID)
	if gerr != nil {
		t.Fatal(gerr)
	}
	if run.FinishReason != "completed" {
		t.Fatalf("这一轮应正常收尾（表级暂停不是 run 失败），得到 finish_reason=%q code=%q last_error=%s",
			run.FinishReason, run.Code, run.LastError)
	}

	// ④ 位点**必须前进**（跳过这张表不等于停下整条同步）+ 暂停 key 与块标记同源
	var posAfter string
	if err := dstDB.QueryRow(ctx, "SELECT COALESCE(position,'') FROM _jl_meta WHERE job_key=$1",
		job.JobKey).Scan(&posAfter); err != nil {
		t.Fatal(err)
	}
	if posAfter == posBefore {
		t.Fatalf("跳过了 A 之后位点必须前进（否则下一轮会重读同一段、永远卡住）：%q", posAfter)
	}
	paused, err := readPaused(t, dstDB, job.JobKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(paused) != 1 {
		t.Fatalf("应只登记 1 张暂停表，得到 %v", paused)
	}
	if _, ok := paused[ta]; !ok {
		t.Fatalf("暂停的应是 %s，得到 %v", ta, paused)
	}
	if !strings.Contains(paused[ta].Reason, pgsink.PauseReasonTypeChanged) {
		t.Fatalf("原因码应是 %s，得到 %q", pgsink.PauseReasonTypeChanged, paused[ta].Reason)
	}
	if paused[ta].At == "" {
		t.Fatal("暂停记录要带第一次登记的时间")
	}
	// key 与 `_jl_chunk.table_name` **逐字节相同**
	var chunkName string
	if err := dstDB.QueryRow(ctx, `SELECT table_name FROM _jl_chunk
		WHERE job_key=$1 AND table_name=$2 LIMIT 1`, job.JobKey, ta).Scan(&chunkName); err != nil {
		t.Fatalf("块标记里没有 %s 的行：%v", ta, err)
	}
	if chunkName != ta {
		t.Fatalf("暂停 key 与块标记 table_name 必须逐字节相同：%q vs %q", ta, chunkName)
	}
	for k := range paused {
		var n int
		if err := dstDB.QueryRow(ctx, `SELECT count(*) FROM _jl_chunk
			WHERE job_key=$1 AND table_name=$2`, job.JobKey, k).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			t.Fatalf("暂停 key %q 在 _jl_chunk 里找不到同名字段（两边不同源）", k)
		}
	}

	// ⑤ 只停该表：A 的那一行没进目标端，**B 的那一行必须进**（A1 断言）
	var aRows, bRows int
	if err := dstDB.QueryRow(ctx, "SELECT count(*) FROM "+ta+" WHERE id=301").Scan(&aRows); err != nil {
		t.Fatal(err)
	}
	if err := dstDB.QueryRow(ctx, "SELECT count(*) FROM "+tb+" WHERE id=301").Scan(&bRows); err != nil {
		t.Fatal(err)
	}
	if aRows != 0 {
		t.Fatalf("暂停表的行不许写进目标端，实际 A=%d", aRows)
	}
	if bRows != 1 {
		t.Fatalf("其它表必须照常推进（这就是「只停该表」）：B=%d，期望 1", bRows)
	}
	// 跳过窗口要记账（可枚举、可补偿），否则就是静默缺失。
	if paused[ta].SkippedRows < 1 {
		t.Fatalf("跳过窗口必须记账（skipped_rows ≥ 1），得到 %+v", paused[ta])
	}
	if paused[ta].SkippedFrom == "" {
		t.Fatalf("跳过窗口要记下起点位点（skipped_from），得到 %+v", paused[ta])
	}

	// ⑥ 对齐结构后重跑：**整表重搬** A（清表 + 从新视图搬），搬成才解除；B 不受影响
	exec(t, dstDB, "ALTER TABLE "+ta+" ALTER COLUMN v TYPE numeric(12,2)")
	// 暂停窗口里源端还删过 A 的行：只 upsert 不清表的话这些行会永久留在目标端（幻行），
	// 所以重搬必须走「清表」这条路。这条数据就是为了让「只 upsert」的写法必红。
	exec(t, srcDB, "DELETE FROM "+ta+" WHERE id BETWEEN 1 AND 50")
	t0 := time.Now()
	res2, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, IdleAfter: 700 * time.Millisecond})
	if err != nil {
		t.Fatalf("对齐结构后重跑应成功: %v", err)
	}
	caughtUp := time.Since(t0)
	if res2.RowsCopied < 1 {
		t.Fatalf("对齐后必须真的重搬了暂停表（RowsCopied=%d）：只从旧位点重放补不回跳过的行", res2.RowsCopied)
	}
	paused2, err := readPaused(t, dstDB, job.JobKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(paused2) != 0 {
		t.Fatalf("重搬搬成 → 必须解除暂停，仍有：%v", paused2)
	}
	for _, tbl := range []string{ta, tb} {
		if fp := tableFingerprint(t, dstDB, tbl); fp != tableFingerprint(t, srcDB, tbl) {
			t.Fatalf("%s 恢复后应与源端一致：目标 %s / 源 %s", tbl, fp, tableFingerprint(t, srcDB, tbl))
		}
	}
	var n int
	if err := dstDB.QueryRow(ctx, "SELECT count(*) FROM "+ta).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 251 {
		t.Fatalf("重搬后应剩 251 行（300+1 新行−50 删除），实际 %d：清表没做干净就是幻行", n)
	}
	t.Logf("按表暂停：位点 %q → %q（前进）；A 的那 1 行没写、B 的照写；"+
		"跳过记账 %d 行（起点 %s）；对齐后整表重搬 %d 行并解除，两端指纹一致，重搬追平用时 %.0fms",
		posBefore, posAfter, paused[ta].SkippedRows, paused[ta].SkippedFrom,
		res2.RowsCopied, float64(caughtUp.Milliseconds()))
}

// readPausedOrFail 是 readPaused 的「读不出来就算失败」版本（用例里到处用）。
func readPausedOrFail(t *testing.T, conn *pgx.Conn, jobKey string) map[string]pgsink.PausedTable {
	t.Helper()
	m, err := readPaused(t, conn, jobKey)
	if err != nil {
		t.Fatalf("读暂停记录失败: %v", err)
	}
	return m
}

// readPaused 直接读 _jl_meta.paused_tables 并按 JSON 解析（不经过代码里的缓存），
// 确认库里存的就是那份形状：key = 裸表名，值带 reason/at/detail。
func readPaused(t *testing.T, conn *pgx.Conn, jobKey string) (map[string]pgsink.PausedTable, error) {
	t.Helper()
	var raw string
	if err := conn.QueryRow(context.Background(),
		`SELECT COALESCE(paused_tables,'') FROM _jl_meta WHERE job_key=$1`, jobKey).Scan(&raw); err != nil {
		return nil, err
	}
	out := map[string]pgsink.PausedTable{}
	if strings.TrimSpace(raw) == "" {
		return out, nil
	}
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, fmt.Errorf("paused_tables 不是合法 JSON（%q）：%w", raw, err)
	}
	return out, nil
}

// continuousJobBoth 建一条 continuous+initial、选多张表的任务。
func continuousJobBoth(t *testing.T, srcDSN, dstDSN, name string, tables ...string) (*control.Store, string) {
	t.Helper()
	ctx := context.Background()
	store, err := control.Open(control.Options{DataDir: t.TempDir(), Autogen: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	mk := func(nm string, port int, db, user, pass string) string {
		c, err := store.CreateConnection(ctx, control.ConnectionInput{
			Name: nm, Kind: "postgres", Host: "127.0.0.1", Port: port,
			Database: db, Username: user, Password: pass,
			Params: map[string]string{"sslmode": "disable"},
		})
		if err != nil {
			t.Fatal(err)
		}
		return c.ID
	}
	srcID := mk("src", portOf(t, srcDSN), dbOf(t, srcDSN), userOf(t, srcDSN), passOf(t, srcDSN))
	dstID := mk("dst", portOf(t, dstDSN), dbOf(t, dstDSN), userOf(t, dstDSN), passOf(t, dstDSN))
	content := map[string]control.TableDoc{}
	for _, tbl := range tables {
		content[tbl] = control.TableDoc{}
	}
	job, err := store.CreateJob(ctx, control.JobInput{
		Name: name, SourceID: srcID, TargetID: dstID,
		Spec: control.JobDoc{
			Version: 1, Job: name,
			Source:  control.SourceDoc{Kind: "postgres", Scope: &control.ScopeDoc{Schema: "public"}},
			Target:  control.TargetDoc{Kind: "postgres", Schema: "public", OnExists: "append"},
			Mode:    control.ModeDoc{Frequency: "continuous+initial"},
			Content: &control.ContentDoc{Tables: content},
		},
	})
	if err != nil {
		t.Fatalf("建任务失败: %v", err)
	}
	return store, job.ID
}

// §3 判据 1/3：改类型的两种形态（bigint→text、varchar 放宽）与改类型后的增量。
//
// 1) 目标端列类型与源端一致：改类型前后各查一次 information_schema（两侧都查）；
// 3) 增量继续可用：改类型后继续写 N 行 → 行数相等、位点前进、runs.code 为空。
func TestTypeChangeTextAndVarcharWideningThenIncremental(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	srcDB := mustConn(t, srcDSN)
	dstDB := mustConn(t, dstDSN)
	table := "type_txt_varchar"
	ctx := context.Background()

	exec(t, srcDB, "DROP TABLE IF EXISTS "+table)
	exec(t, srcDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, v bigint, s varchar(20))")
	exec(t, srcDB, "INSERT INTO "+table+" SELECT g, g, 'x'||g FROM generate_series(1,500) g")
	exec(t, dstDB, "DROP TABLE IF EXISTS "+table)
	exec(t, dstDB, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")

	store, jobID := continuousJobBoth(t, srcDSN, dstDSN, "type-txt-varchar", table)
	if _, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, SnapshotOnly: true}); err != nil {
		t.Fatalf("首搬失败: %v", err)
	}
	before := [2][2]string{
		{columnType(t, srcDB, table, "v"), columnType(t, srcDB, table, "s")},
		{columnType(t, dstDB, table, "v"), columnType(t, dstDB, table, "s")},
	}
	if before[0] != before[1] {
		t.Fatalf("首搬后两端列类型应一致：源 %v / 目标 %v", before[0], before[1])
	}

	// 源端两种改法各一例：bigint→text（类型族都变了）、varchar(20)→varchar(60)（放宽）
	exec(t, srcDB, "ALTER TABLE "+table+" ALTER COLUMN v TYPE text")
	exec(t, srcDB, "ALTER TABLE "+table+" ALTER COLUMN s TYPE varchar(60)")
	srcAfter := [2]string{columnType(t, srcDB, table, "v"), columnType(t, srcDB, table, "s")}

	// 目标端故意只对齐第一列：第二列仍窄 → **这张表被暂停**（不许静默按旧类型写、也不许收窄截断），
	// 但整轮不再失败（契约 §1.2/§1.5：只停这一张表）。
	exec(t, dstDB, "ALTER TABLE "+table+" ALTER COLUMN v TYPE text")
	if _, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, SnapshotOnly: true}); err != nil {
		t.Fatalf("一张表类型对不上不该打死整轮: %v", err)
	}
	job0, jerr := store.GetJob(ctx, jobID)
	if jerr != nil {
		t.Fatal(jerr)
	}
	rec0, err := readPaused(t, dstDB, job0.JobKey)
	if err != nil {
		t.Fatal(err)
	}
	one, ok := rec0[table]
	if !ok {
		t.Fatalf("varchar 仍比源端窄 → 必须暂停这张表，得到 %v", rec0)
	}
	if one.Reason != pgsink.PauseReasonTypeChanged {
		t.Fatalf("原因码应为 %q，得到 %q", pgsink.PauseReasonTypeChanged, one.Reason)
	}
	if !strings.Contains(one.Detail, "s") {
		t.Fatalf("detail 要点名那一列（s），得到 %q", one.Detail)
	}
	// 目标端那列**没有被偷偷改宽**：改宽的只能是用户（我们只暂停，不动结构）
	if typ := columnType(t, dstDB, table, "s"); typ != "character varying" {
		t.Fatalf("目标端列类型不该被我们改，得到 %s", typ)
	}

	// 对齐第二列之后：暂停解除（走整表重搬）+ 两侧类型一致 + 增量继续可用（判据 3）
	exec(t, dstDB, "ALTER TABLE "+table+" ALTER COLUMN s TYPE varchar(60)")
	if _, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, SnapshotOnly: true}); err != nil {
		t.Fatalf("对齐后重跑应成功: %v", err)
	}
	if after := readPausedOrFail(t, dstDB, job0.JobKey); len(after) != 0 {
		t.Fatalf("结构对齐并重搬之后必须解除暂停，仍有 %v", after)
	}
	dstAfter := [2]string{columnType(t, dstDB, table, "v"), columnType(t, dstDB, table, "s")}
	if srcAfter != dstAfter {
		t.Fatalf("改类型后两端列类型应一致：源 %v / 目标 %v", srcAfter, dstAfter)
	}
	var posBeforeIncr string
	job, _ := store.GetJob(ctx, jobID)
	if err := dstDB.QueryRow(ctx, "SELECT COALESCE(position,'') FROM _jl_meta WHERE job_key=$1", job.JobKey).Scan(&posBeforeIncr); err != nil {
		t.Fatal(err)
	}

	// 改类型后继续写 20 行
	exec(t, srcDB, "INSERT INTO "+table+" SELECT g, 'v'||g, 'y'||g FROM generate_series(501,520) g")
	if _, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, IdleAfter: 700 * time.Millisecond}); err != nil {
		t.Fatalf("改类型后的增量应能跑: %v", err)
	}
	var srcN, dstN int
	if err := srcDB.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&srcN); err != nil {
		t.Fatal(err)
	}
	if err := dstDB.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&dstN); err != nil {
		t.Fatal(err)
	}
	if srcN != dstN || srcN != 520 {
		t.Fatalf("改类型后增量应把 20 行也写进去：源 %d / 目标 %d（应各 520）", srcN, dstN)
	}
	var posAfterIncr string
	if err := dstDB.QueryRow(ctx, "SELECT COALESCE(position,'') FROM _jl_meta WHERE job_key=$1", job.JobKey).Scan(&posAfterIncr); err != nil {
		t.Fatal(err)
	}
	if posAfterIncr == posBeforeIncr {
		t.Fatal("增量写成功之后位点必须前进")
	}
	t.Logf("§3-1 改类型前 %v → 改类型后 %v（两端一致）；§3-3 之后写 20 行，两端各 %d 行，位点 %s → %s",
		before[0], srcAfter, dstN, posBeforeIncr, posAfterIncr)
}
