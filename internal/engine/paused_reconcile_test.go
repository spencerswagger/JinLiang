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
