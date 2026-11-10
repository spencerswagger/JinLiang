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
	"github.com/spencerswagger/jinliang/internal/core"
	"github.com/spencerswagger/jinliang/internal/engine"
	"github.com/spencerswagger/jinliang/internal/pgsink"
)

// 按表暂停（第③步/契约 2）：暂停登记、位点不动、key 与块标记同源、解除后恢复。
//
// 契约要点（改这段前先读）：
//   - 暂停登记是**独立一条 UPDATE**，位点在暂停前后逐字节相等；
//   - key = 目标端裸表名，与 `_jl_chunk.table_name` 逐字节相同；
//   - 位点只有一个 → 整条同步停下来，其它表也不会继续推进（不是「只停该表」）；
//   - 解除挂在「EnsureSchema 通过 + 存量搬完」，解除后从原位点继续重放，
//     一条变更都不丢。
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

	// ③ 这一轮必须失败并登记暂停
	res, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, IdleAfter: 500 * time.Millisecond})
	if err == nil {
		t.Fatal("源端改了列类型：这一轮必须失败（位点不推进），不能静默跳过")
	}
	run, gerr := store.GetRun(ctx, res.RunID)
	if gerr != nil {
		t.Fatal(gerr)
	}
	if run.Code != string(core.CodeSchemaMismatch) {
		t.Fatalf("应挂 E1015，得到 %q；last_error=%s", run.Code, run.LastError)
	}
	for _, want := range []string{ta, "v"} {
		if !strings.Contains(run.LastError, want) {
			t.Fatalf("last_error 要到字段级（应含 %q），得到：%s", want, run.LastError)
		}
	}

	// ④ 位点逐字节不动 + 暂停 key 与块标记同源
	var posAfter string
	if err := dstDB.QueryRow(ctx, "SELECT COALESCE(position,'') FROM _jl_meta WHERE job_key=$1",
		job.JobKey).Scan(&posAfter); err != nil {
		t.Fatal(err)
	}
	if posAfter != posBefore {
		t.Fatalf("暂停前后位点必须逐字节相等：%q → %q", posBefore, posAfter)
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

	// ⑤ 位点没推进 → 那一批两条 INSERT 都没写进去（包括**另一张表**的那条：
	//    位点只有一个，其它表也不会继续推进）
	var aRows, bRows int
	if err := dstDB.QueryRow(ctx, "SELECT count(*) FROM "+ta+" WHERE id=301").Scan(&aRows); err != nil {
		t.Fatal(err)
	}
	if err := dstDB.QueryRow(ctx, "SELECT count(*) FROM "+tb+" WHERE id=301").Scan(&bRows); err != nil {
		t.Fatal(err)
	}
	if aRows != 0 || bRows != 0 {
		t.Fatalf("位点没推进，这一批一条都不该落库：A=%d B=%d", aRows, bRows)
	}

	// ⑥ 按文案对齐结构后重跑：解除暂停、从原位点继续重放，两张表都追平
	exec(t, dstDB, "ALTER TABLE "+ta+" ALTER COLUMN v TYPE numeric(12,2)")
	t0 := time.Now()
	if _, err := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, IdleAfter: 700 * time.Millisecond}); err != nil {
		t.Fatalf("对齐结构后重跑应成功: %v", err)
	}
	caughtUp := time.Since(t0)
	paused2, err := readPaused(t, dstDB, job.JobKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(paused2) != 0 {
		t.Fatalf("结构对齐 + 存量已完成 → 必须解除暂停，仍有：%v", paused2)
	}
	for _, tbl := range []string{ta, tb} {
		if fp := tableFingerprint(t, dstDB, tbl); fp != tableFingerprint(t, srcDB, tbl) {
			t.Fatalf("%s 解除暂停后应与源端一致：目标 %s", tbl, fp)
		}
	}
	t.Logf("暂停：位点 %q 前后逐字节相等；key=%q 与块标记同源；解除后两端指纹一致，"+
		"整轮（含 700ms 静止判定）用时 %.0fms", posAfter, ta, float64(caughtUp.Milliseconds()))
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
