//go:build integration

package engine

// `reconcilePaused` 的「解除」判定必须只看块进度（`copyDoneFor`），而且**调用点必须在
// `decideCopyPlan` 之后**——那一步才给 `progressByTable` 赋值。提前调会让 map 为空、
// `copyDoneFor` 走 `!ok → true`，每张表都被判「已搬完」，暂停记录被无条件清掉：
// 半截拷贝的表下一轮显示「已恢复」，实际是半空的（契约点名的坏状态）。
//
// 前两格直接打 `copyDoneFor` + `reconcilePaused` 的语义（**绕开了 prepare**，所以它们
// 抓不到「调用点提前」这个洞——别把它们当调用点的证据）。调用点由下面那格
// `TestPrepareKeepsPauseRecordForHalfCopied` 走真 `prepare` 来钉。

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

func TestReconcilePausedKeepsHalfCopiedAndClearsDone(t *testing.T) {
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
	// job_hash 留空：这格只测「块进度 → 解除/保留」，不测 job_hash 那条出清条件
	//（非空会与 zero doc 的哈希不等，记录会被当「任务定义变了」清掉，测不到本意）。
	if err := sink.EnsureMeta(ctx, "", "", "", ""); err != nil {
		t.Fatal(err)
	}
	ref := source.TableRef{Schema: "public", Table: "orders"}
	if err := sink.PauseTable(ctx, ref, pgsink.PauseReasonTypeChanged, "v: 目标 text / 源 bigint"); err != nil {
		t.Fatal(err)
	}

	// 第一格：半截拷贝（有块标记、没有完成标记）→ 记录**必须仍在**
	r := &runner{
		sink:   sink,
		tables: []source.TableRef{ref},
		// 模拟 decideCopyPlan 读到的块进度：搬过 3 块，但没搬完
		progressByTable: map[string]pgsink.ChunkProgress{"orders": {LastNo: 3, Cursor: "3000"}},
	}
	if r.copyDoneFor("orders") {
		t.Fatal("半截拷贝不该算「存量已完成」")
	}
	if err := r.reconcilePaused(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := sink.PausedTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got["orders"]; !ok {
		t.Fatalf("半截拷贝 + 暂停记录：经过收口后记录必须仍在（否则界面显示「已恢复」而表是半空的），得到 %v", got)
	}

	// 第二格：Done 齐了 → **解除**
	r.progressByTable = map[string]pgsink.ChunkProgress{"orders": {Done: true, LastNo: 3}}
	if !r.copyDoneFor("orders") {
		t.Fatal("有完成标记就该算「存量已完成」")
	}
	if err := r.reconcilePaused(ctx); err != nil {
		t.Fatal(err)
	}
	got, err = sink.PausedTables(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got["orders"]; ok {
		t.Fatalf("存量已完成 + 结构对齐 → 必须解除，仍有 %v", got)
	}
	t.Logf("收口判定：半截拷贝保留记录、Done 齐后解除")
}

// **走真 `engine.Run`** 的那两格（不是手工构造 runner）：
//
//	A. continuous + 半截拷贝（有块标记、无 Done）+ 记录在 → 跑完记录**仍在**
//	B. Done 齐 + 记录在 → 跑完**解除**
//
// 为什么必须走 Run：上一版把第一次 `reconcilePaused` 提到 `decideCopyPlan` 之前，
// `progressByTable` 还是 nil → `copyDoneFor` 恒真 → 记录被无条件清掉。手工构造 runner 的
// 用例绕开了 prepare，绿了也抓不到；这格让调用点在链路里真的被执行。
// 变异：把第一次调用上的 `if r.mode()=="interval"` 拿掉 → A 必红。
func TestRunKeepsOrClearsPauseRecordByChunkProgress(t *testing.T) {
	ctx := context.Background()
	raw := strings.TrimSpace(os.Getenv("JINLIANG_TEST_PG_DSN"))
	if raw == "" {
		t.Fatal("缺少 JINLIANG_TEST_PG_DSN")
	}
	cfg, err := pgx.ParseConfig(raw)
	if err != nil {
		t.Fatal(err)
	}

	for _, halfCopied := range []bool{true, false} {
		name := "half"
		if !halfCopied {
			name = "done"
		}
		t.Run(name, func(t *testing.T) {
			conn, err := pgx.ConnectConfig(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close(ctx)
			table := "recon_" + name
			for _, stmt := range []string{
				"DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance",
				"DROP TABLE IF EXISTS " + table,
			} {
				if _, err := conn.Exec(ctx, stmt); err != nil {
					t.Fatal(err)
				}
			}

			// 源端表得真的存在（CI 上没有我本机手工建的那两张）
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

			// 先把状态表建出来（生产里由 EnsureStateTables 负责），否则下面没法播种子
			ep := dsn.Endpoint{Kind: "postgres", Host: cfg.Host, Port: int(cfg.Port),
				Database: cfg.Database, Username: cfg.User, Password: cfg.Password,
				Params: map[string]string{"sslmode": "disable"}}
			if err := pgsink.EnsureStateTables(ctx, ep, "public"); err != nil {
				t.Fatal(err)
			}
			store, jobID := continuousJobBothIn(t, os.Getenv("JINLIANG_TEST_PG_SRC_DSN"), raw, "recon-"+name, table)
			job, err := store.GetJob(ctx, jobID)
			if err != nil {
				t.Fatal(err)
			}
			// 目标端状态：有位点、有块标记；Done 那格再加完成标记
			if _, err := conn.Exec(ctx, `INSERT INTO _jl_meta (job_key,name,instance_id,phase,position,paused_tables)
				VALUES ($1,$2,'it','streaming','postgres:v1:0/10',
				        jsonb_build_object($3::text, jsonb_build_object('reason','type_changed','at','2026-11-10T00:00:00Z','detail','v: 目标 text / 源 bigint'))::text)
				ON CONFLICT (job_key) DO UPDATE SET position=EXCLUDED.position, paused_tables=EXCLUDED.paused_tables`,
				job.JobKey, job.Name, table); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.Exec(ctx, `INSERT INTO _jl_chunk (job_key,table_name,chunk_no,done,rows)
				VALUES ($1,$2,1,true,10)`, job.JobKey, table); err != nil {
				t.Fatal(err)
			}
			if !halfCopied {
				if _, err := conn.Exec(ctx, `INSERT INTO _jl_chunk (job_key,table_name,chunk_no,done,rows)
					VALUES ($1,$2,0,true,10)`, job.JobKey, table); err != nil {
					t.Fatal(err)
				}
			}

			if _, err := Run(ctx, Options{Store: store, JobID: jobID, SnapshotOnly: true}); err != nil {
				t.Fatalf("这一轮应能跑（continuous 不搬存量）：%v", err)
			}
			var raw2 string
			if err := conn.QueryRow(ctx, `SELECT COALESCE(paused_tables,'') FROM _jl_meta WHERE job_key=$1`,
				job.JobKey).Scan(&raw2); err != nil {
				t.Fatal(err)
			}
			stillPaused := strings.Contains(raw2, table)
			if halfCopied && !stillPaused {
				t.Fatalf("半截拷贝 + 记录在：跑完记录必须仍在（否则界面显示「已恢复」而表是半空的），实际 %q", raw2)
			}
			if !halfCopied && stillPaused {
				t.Fatalf("Done 齐 → 必须解除，实际仍记录着：%q", raw2)
			}
			t.Logf("%s：跑完 paused_tables=%q（halfCopied=%v）", name, raw2, halfCopied)
		})
	}
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
