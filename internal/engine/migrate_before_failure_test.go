//go:build integration

package engine_test

// 旧 schema 的目标库 + 一个会在 preflight 失败的任务：
// **报错必须是真实原因，不能是 `column "paused_tables" does not exist`**，且迁移已完成。
//
// 真实场景：用户的目标库还是旧 schema，第一次跑因为「表不存在/租约/预检」在 prepare 之前
// 就失败——那时他该看到的是那条真原因，而不是一条让他以为要手工改表的列缺失错误。
// 变异：把状态表迁移挪回 prepare 之后（即只在 openSink 里做）→ 这格红。

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/spencerswagger/jinliang/internal/engine"
)

func TestOldSchemaMigratedEvenWhenPreflightFails(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	ctx := context.Background()
	cfg, err := pgx.ParseConfig(dstDSN)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	// 造「旧 schema」：_jl_meta 没有 paused_tables
	for _, stmt := range []string{
		"DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance",
		`CREATE TABLE _jl_meta (
			job_key text PRIMARY KEY, name text NOT NULL, instance_id text NOT NULL,
			schema_version int NOT NULL DEFAULT 1, job_hash text NOT NULL DEFAULT '',
			schema_hash text, phase text NOT NULL, position text, watermark text,
			snapshot_id text, started_at timestamptz NOT NULL DEFAULT now(),
			updated_at timestamptz NOT NULL DEFAULT now())`,
	} {
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}

	// 任务选的表在源端不存在（preflight 会把它拦下来）
	store, jobID := onceJobSingleTable(t, srcDSN, dstDSN, "old-schema-preflight", "no_such_table_old_schema")
	res, runErr := engine.Run(ctx, engine.Options{Store: store, JobID: jobID, SnapshotOnly: true})
	if runErr == nil {
		t.Fatal("源端没有这张表：必须失败")
	}
	msg := runErr.Error()
	if strings.Contains(msg, "paused_tables") {
		t.Fatalf("旧库首轮失败必须报真实原因，不许是列缺失（那会让人以为要手工改表）：%s", msg)
	}
	if !strings.Contains(msg, "no_such_table_old_schema") && !strings.Contains(msg, "不存在") {
		t.Fatalf("报错要点名真实原因（表不存在），得到：%s", msg)
	}
	// 迁移必须已发生（哪怕这一轮在 preflight 就失败）
	var hasCol bool
	if err := conn.QueryRow(ctx, `SELECT count(*) > 0 FROM information_schema.columns
		WHERE table_schema='public' AND table_name='_jl_meta' AND column_name='paused_tables'`).Scan(&hasCol); err != nil {
		t.Fatal(err)
	}
	if !hasCol {
		t.Fatal("即使这一轮在 preflight 失败，状态表迁移也该已经完成（paused_tables 列存在）")
	}
	_ = res
	t.Logf("旧 schema + preflight 失败：报的是真原因（表不存在），迁移已完成（列存在）")
}

var _ = os.Getenv
