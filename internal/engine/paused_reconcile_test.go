//go:build integration

package engine

// `reconcilePaused` 的语义（docs/CONTRACT-PAUSE-AND-POSITION.md §1.6）：
// **结构对齐不等于恢复**。对齐只触发「欠一次整表重搬」——清掉该表的块标记（`chunk_no=0`
// 完成标记也清），记录**留着**；真正的解除发生在重搬搬成的那一刻（`snapshot()` 里）。
// 用户自己的表（`resume=manual`）连块标记都不许动，停下来问。
//
// 这与旧实现是反的：旧判据「结构对齐 + 存量已完成 → 当场解除」在半截拷贝的表上保留记录、
// 在块标记早已 Done 的表上**当场解除**——于是跳过窗口永远等不到重搬去补（表显示「已恢复」
// 而数据是旧的）。用例跟着翻转，变异（改回旧判据）必红。

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/pgsink"
	"github.com/spencerswagger/jinliang/internal/source"
)

func TestReconcilePausedMarksRecopyAndKeepsRecord(t *testing.T) {
	ctx := context.Background()
	raw := strings.TrimSpace(os.Getenv("JINLIANG_TEST_PG_DSN"))
	if raw == "" {
		t.Fatal("缺少 JINLIANG_TEST_PG_DSN")
	}
	cfg, err := pgx.ParseConfig(raw)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance"); err != nil {
		t.Fatal(err)
	}
	ep := dsn.Endpoint{Kind: "postgres", Host: cfg.Host, Port: int(cfg.Port),
		Database: cfg.Database, Username: cfg.User, Password: cfg.Password,
		Params: map[string]string{"sslmode": "disable"}}
	sink, err := pgsink.Open(ctx, pgsink.Options{
		Target: ep, JobKey: "reconjob01", JobName: "recon", InstanceID: "it",
		Schema: "public", OnExists: "append",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	// job_hash 留空：这格只测「记录怎么收口」，不测 job_hash 那条出清条件。
	if err := sink.EnsureMeta(ctx, "", "", "", ""); err != nil {
		t.Fatal(err)
	}
	ours := source.TableRef{Schema: "public", Table: "orders"}
	theirs := source.TableRef{Schema: "public", Table: "orders_user"}
	if err := sink.PauseTable(ctx, ours, pgsink.PausedTable{
		Reason: pgsink.PauseReasonTypeChanged, Detail: "v: 目标 text / 源 bigint",
		Resume: pgsink.ResumeRecopy,
	}); err != nil {
		t.Fatal(err)
	}
	if err := sink.PauseTable(ctx, theirs, pgsink.PausedTable{
		Reason: pgsink.PauseReasonTypeChanged, Detail: "v: 目标 text / 源 bigint",
		Resume: pgsink.ResumeManual,
	}); err != nil {
		t.Fatal(err)
	}
	// 两张表都有块标记（含 chunk_no=0 完成标记）：模拟「暂停前就已经搬完」，
	// 这正是旧判据会当场解除的那种形状。
	for _, tbl := range []string{"orders", "orders_user"} {
		for _, no := range []int{1, 0} {
			if _, err := conn.Exec(ctx, `INSERT INTO _jl_chunk (job_key,table_name,chunk_no,done,rows)
				VALUES ('reconjob01',$1,$2,true,10)`, tbl, no); err != nil {
				t.Fatal(err)
			}
		}
	}

	r := &runner{sink: sink, tables: []source.TableRef{ours, theirs},
		doc: control.JobDoc{Mode: control.ModeDoc{Frequency: "full"}}}
	if err := r.reconcilePaused(ctx); err != nil {
		t.Fatal(err)
	}

	got, err := sink.PausedTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got["orders"]; !ok {
		t.Fatal("结构对齐只该把它标成「欠一次重搬」，记录必须留着——解除要等重搬搬成")
	}
	if _, ok := got["orders_user"]; !ok {
		t.Fatal("用户自己的表（resume=manual）不许自动解除：要停下来问")
	}

	// 本任务建的表：块标记被清掉（含 chunk_no=0），planCopy 会把它排进本轮重搬。
	var n int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM _jl_chunk WHERE job_key='reconjob01' AND table_name='orders'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("对齐后应清掉 orders 的块标记（含 chunk_no=0），还剩 %d 行", n)
	}
	// 用户自己的表：一列都不许动。
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM _jl_chunk WHERE job_key='reconjob01' AND table_name='orders_user'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("resume=manual 的表不许动块标记，期望 2 行，实际 %d 行", n)
	}
	if _, ok := r.pausedRecord("orders"); !ok {
		t.Fatal("引擎的内存集合里应保留这张表（重搬期间写路径继续跳过它）")
	}
	t.Logf("收口：对齐 → 清块标记 + 留记录（等重搬搬成才解除）；manual 表两样都不动")
}

// **走真 `engine.Run`** 的那一格：continuous 模式下「结构对齐」怎么收口。
//
// continuous / interval 都没有存量可搬（契约 §1.2 例外段）：把它们标成「待重搬」会让记录
// 永远解不开（`planCopy` 在这两种模式下不排存量）。所以这里是**解除 + 一条说清代价的 note**：
// 「解除」不等于两侧数据已一致，要补齐得跑一次 full。
//
// 变异：把 reconcilePaused 里 `mode=="interval"||mode=="continuous"` 那支删掉 → 本格必红
// （记录会留着，且块标记被清成「待重搬」）。
func TestRunClearsPauseRecordInContinuousWithCaveat(t *testing.T) {
	ctx := context.Background()
	raw := strings.TrimSpace(os.Getenv("JINLIANG_TEST_PG_DSN"))
	if raw == "" {
		t.Fatal("缺少 JINLIANG_TEST_PG_DSN")
	}
	cfg, err := pgx.ParseConfig(raw)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	table := "recon_cont"
	for _, stmt := range []string{
		"DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance",
		"DROP TABLE IF EXISTS " + table,
	} {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}

	// 源端表得真的存在（CI 上没有本机手工建的那张）。
	srcCfg, err := pgx.ParseConfig(os.Getenv("JINLIANG_TEST_PG_SRC_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	srcConn, err := pgx.ConnectConfig(ctx, srcCfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		"DROP TABLE IF EXISTS " + table,
		"CREATE TABLE " + table + " (id bigint PRIMARY KEY, v text)",
		"INSERT INTO " + table + " SELECT g, 'r'||g FROM generate_series(1,5) g",
	} {
		if _, err := srcConn.Exec(ctx, stmt); err != nil {
			srcConn.Close(ctx)
			t.Fatalf("建源端表失败（%s）: %v", stmt, err)
		}
	}
	srcConn.Close(ctx)

	ep := dsn.Endpoint{Kind: "postgres", Host: cfg.Host, Port: int(cfg.Port),
		Database: cfg.Database, Username: cfg.User, Password: cfg.Password,
		Params: map[string]string{"sslmode": "disable"}}
	if err := pgsink.EnsureStateTables(ctx, ep, "public"); err != nil {
		t.Fatal(err)
	}
	store, jobID := continuousJobBothIn(t, os.Getenv("JINLIANG_TEST_PG_SRC_DSN"), raw, "recon-cont", table)
	job, err := store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO _jl_meta (job_key,name,instance_id,phase,position,paused_tables)
		VALUES ($1,$2,'it','streaming','postgres:v1:0/10',
		        jsonb_build_object($3::text, jsonb_build_object('reason','type_changed','at','2026-11-10T00:00:00Z','detail','v: 目标 text / 源 bigint'))::text)
		ON CONFLICT (job_key) DO UPDATE SET position=EXCLUDED.position, paused_tables=EXCLUDED.paused_tables`,
		job.JobKey, job.Name, table); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO _jl_chunk (job_key,table_name,chunk_no,done,rows)
		VALUES ($1,$2,1,true,10), ($1,$2,0,true,10)`, job.JobKey, table); err != nil {
		t.Fatal(err)
	}

	res, err := Run(ctx, Options{Store: store, JobID: jobID, SnapshotOnly: true})
	if err != nil {
		t.Fatalf("这一轮应能跑（continuous 不搬存量）：%v", err)
	}
	var raw2 string
	if err := conn.QueryRow(ctx, `SELECT COALESCE(paused_tables,'') FROM _jl_meta WHERE job_key=$1`,
		job.JobKey).Scan(&raw2); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(raw2, table) {
		t.Fatalf("continuous 对齐后必须解除（它没有存量可重搬，留着就永远解不开），实际 %q", raw2)
	}
	// 代价说明（「continuous 不搬存量」）由 TestReconcilePausedContinuousClearsWithCaveat 钉住；
	// 这里同时确认**没有**把它标成「待重搬」——块标记必须原样留着。
	var n int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM _jl_chunk WHERE job_key=$1`, job.JobKey).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("continuous 不该清块标记（清了也没有存量可搬），期望 2 行，实际 %d 行", n)
	}
	_ = res
}

// TestReconcilePausedContinuousClearsWithCaveat：解除时必须把代价说出来。
//
// 「解除」不等于「两侧已经一致」——continuous/interval 不搬存量，用户若以为一解除就补齐了，
// 会拿着一份没人告诉他不一致的数据继续用。
func TestReconcilePausedContinuousClearsWithCaveat(t *testing.T) {
	ctx := context.Background()
	raw := strings.TrimSpace(os.Getenv("JINLIANG_TEST_PG_DSN"))
	if raw == "" {
		t.Fatal("缺少 JINLIANG_TEST_PG_DSN")
	}
	cfg, err := pgx.ParseConfig(raw)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance"); err != nil {
		t.Fatal(err)
	}
	ep := dsn.Endpoint{Kind: "postgres", Host: cfg.Host, Port: int(cfg.Port),
		Database: cfg.Database, Username: cfg.User, Password: cfg.Password,
		Params: map[string]string{"sslmode": "disable"}}
	sink, err := pgsink.Open(ctx, pgsink.Options{
		Target: ep, JobKey: "reconjob02", JobName: "recon2", InstanceID: "it",
		Schema: "public", OnExists: "append",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	if err := sink.EnsureMeta(ctx, "", "", "", ""); err != nil {
		t.Fatal(err)
	}
	ref := source.TableRef{Schema: "public", Table: "cont_orders"}
	if err := sink.PauseTable(ctx, ref, pgsink.PausedTable{
		Reason: pgsink.PauseReasonTypeChanged, Detail: "v: 目标 text / 源 bigint",
	}); err != nil {
		t.Fatal(err)
	}

	var notes []string
	r := &runner{
		sink:   sink,
		tables: []source.TableRef{ref},
		doc:    control.JobDoc{Mode: control.ModeDoc{Frequency: "continuous"}},
		emitHook: func(ev control.Event) {
			if ev.Type == "note" {
				notes = append(notes, ev.Message)
			}
		},
	}
	if err := r.reconcilePaused(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := sink.PausedTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got["cont_orders"]; ok {
		t.Fatal("continuous 对齐后必须解除")
	}
	joined := strings.Join(notes, " | ")
	if !strings.Contains(joined, "不搬存量") {
		t.Fatalf("解除时必须说清代价（continuous 不搬存量、解除≠已一致），实际 notes=%q", joined)
	}
	t.Logf("continuous 解除并说明代价：%s", joined)
}

// continuousJobBothIn 是 package engine 版的两表/单表 continuous 任务构造（不依赖 engine_test 的 helper）。
func continuousJobBothIn(t *testing.T, srcDSN, dstDSN, name string, tables ...string) (*control.Store, string) {
	t.Helper()
	ctx := context.Background()
	store, err := control.Open(control.Options{DataDir: t.TempDir(), Autogen: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	mk := func(nm, dsnRaw string) string {
		cfg, err := pgx.ParseConfig(dsnRaw)
		if err != nil {
			t.Fatal(err)
		}
		c, err := store.CreateConnection(ctx, control.ConnectionInput{
			Name: nm, Kind: "postgres", Host: cfg.Host, Port: int(cfg.Port),
			Database: cfg.Database, Username: cfg.User, Password: cfg.Password,
			Params: map[string]string{"sslmode": "disable"},
		})
		if err != nil {
			t.Fatal(err)
		}
		return c.ID
	}
	content := map[string]control.TableDoc{}
	for _, tbl := range tables {
		content[tbl] = control.TableDoc{}
	}
	job, err := store.CreateJob(ctx, control.JobInput{
		Name: name, SourceID: mk("src", srcDSN), TargetID: mk("dst", dstDSN),
		Spec: control.JobDoc{
			Version: 1, Job: name,
			Source:  control.SourceDoc{Kind: "postgres", Scope: &control.ScopeDoc{Schema: "public"}},
			Target:  control.TargetDoc{Kind: "postgres", Schema: "public", OnExists: "append"},
			Mode:    control.ModeDoc{Frequency: "continuous"},
			Content: &control.ContentDoc{Tables: content},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return store, job.ID
}
