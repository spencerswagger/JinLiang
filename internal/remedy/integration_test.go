//go:build integration

package remedy_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/remedy"
	"github.com/spencerswagger/jinliang/internal/runtime"
)

func openDSN(t *testing.T, env string) *runtime.DB {
	t.Helper()
	raw := os.Getenv(env)
	if raw == "" {
		t.Fatalf("%s 未设置：集成测试需要真实数据库；缺 DSN 必须失败，不允许跳过", env)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db, err := runtime.Open(ctx, raw)
	if err != nil {
		t.Fatalf("%s 连不上：%v", env, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func mustExec(t *testing.T, db *runtime.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(context.Background(), q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

const remedyTable = "jl_remedy_orders"

// Owl #10 item 4: replaying the same dead letter twice must not duplicate the
// row, the second attempt must say 「不存在」, and the authoritative position
// must not move.
func TestRetryDeadLetterIsIdempotentAndDoesNotMovePosition(t *testing.T) {
	db := openDSN(t, "JINLIANG_TEST_PG_DSN")
	ctx := context.Background()

	mustExec(t, db, `CREATE TABLE IF NOT EXISTS `+remedyTable+` (id bigint PRIMARY KEY, name text)`)
	mustExec(t, db, `CREATE TABLE IF NOT EXISTS _jl_dead_letter (
		id bigserial PRIMARY KEY, job_key text NOT NULL, table_name text NOT NULL,
		reason text NOT NULL, position text, raw_row jsonb,
		created_at timestamptz NOT NULL DEFAULT now())`)
	mustExec(t, db, `CREATE TABLE IF NOT EXISTS _jl_action (
		id bigserial PRIMARY KEY, job_key text NOT NULL, action_id text NOT NULL,
		what text NOT NULL, sql text, undo text, actor text NOT NULL,
		created_at timestamptz NOT NULL DEFAULT now())`)
	mustExec(t, db, `CREATE TABLE IF NOT EXISTS _jl_meta (
		job_key text PRIMARY KEY, name text NOT NULL, instance_id text NOT NULL DEFAULT '',
		schema_version int NOT NULL DEFAULT 1, job_hash text NOT NULL DEFAULT '',
		schema_hash text, phase text NOT NULL DEFAULT 'streaming', position text,
		watermark text, snapshot_id text,
		started_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now())`)
	t.Cleanup(func() {
		mustExec(t, db, `DROP TABLE IF EXISTS `+remedyTable)
		mustExec(t, db, `DELETE FROM _jl_dead_letter WHERE job_key = 'remedytest'`)
		mustExec(t, db, `DELETE FROM _jl_action WHERE job_key = 'remedytest'`)
		mustExec(t, db, `DELETE FROM _jl_meta WHERE job_key = 'remedytest'`)
	})

	mustExec(t, db, `DELETE FROM `+remedyTable)
	mustExec(t, db, `INSERT INTO _jl_meta (job_key, name, instance_id, position, phase)
		VALUES ('remedytest','remedy','integration-test','POS-1','streaming')
		ON CONFLICT (job_key) DO UPDATE SET position = 'POS-1'`)
	rows, _, err := db.QueryRows(ctx,
		`INSERT INTO _jl_dead_letter (job_key, table_name, reason, position, raw_row)
		 VALUES ('remedytest', $1, 'type conversion failed', 'POS-1', '{"id":7,"name":"retry-me"}'::jsonb)
		 RETURNING id`, remedyTable)
	if err != nil || len(rows) != 1 {
		t.Fatalf("insert dead letter: %v rows=%v", err, rows)
	}
	id := asInt64(t, rows[0][0])

	// 第一次：写入成功，死信被删掉。
	res, err := remedy.RetryDeadLetter(ctx, db, db.Schema(), id, false)
	if err != nil {
		t.Fatalf("first retry: %v", err)
	}
	if !res.Applied {
		t.Fatalf("first retry not applied: %+v", res)
	}
	if n := countRows(t, db); n != 1 {
		t.Fatalf("after first retry rows=%d, want 1", n)
	}
	if pos := metaPosition(t, db); pos != "POS-1" {
		t.Fatalf("position moved to %q, must stay POS-1", pos)
	}

	// 第二次：同一 id 已不存在，必须报错且不再写一行。
	if _, err := remedy.RetryDeadLetter(ctx, db, db.Schema(), id, false); err == nil {
		t.Fatal("second retry of the same id must fail")
	} else if !strings.Contains(err.Error(), "不存在") {
		t.Fatalf("second retry error should say 不存在, got: %v", err)
	}
	if n := countRows(t, db); n != 1 {
		t.Fatalf("after second retry rows=%d, want still 1", n)
	}
	if pos := metaPosition(t, db); pos != "POS-1" {
		t.Fatalf("position moved to %q, must stay POS-1", pos)
	}
}

func countRows(t *testing.T, db *runtime.DB) int64 {
	t.Helper()
	n, err := db.Count(context.Background(), db.Schema(), remedyTable)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func metaPosition(t *testing.T, db *runtime.DB) string {
	t.Helper()
	m, found, err := db.Meta(context.Background(), "remedytest")
	if err != nil || !found {
		t.Fatalf("read meta: %v found=%v", err, found)
	}
	return m.Position
}

func asInt64(t *testing.T, v any) int64 {
	t.Helper()
	switch x := v.(type) {
	case int64:
		return x
	case int32:
		return int64(x)
	case []byte:
		var n int64
		for _, b := range x {
			n = n*10 + int64(b-'0')
		}
		return n
	}
	t.Fatalf("unexpected id type %T", v)
	return 0
}
