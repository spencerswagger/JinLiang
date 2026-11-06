//go:build integration

package verify_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/runtime"
	"github.com/spencerswagger/jinliang/internal/verify"
)

// Integration tests talk to the databases named by the real DSN variables.
//
// 缺 DSN 必须红：t.Fatalf，不是 t.Skip。跳过会让 CI 在什么都没验的情况下变绿，
// 那正是这套校验想避免的事情。
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

func mustExec(t *testing.T, db *runtime.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(context.Background(), query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

const (
	verifyTable  = "jl_verify_orders"
	verifySchema = "jl_test_verify" // 每包独立 schema：不再依赖 go test -p 1
)

var pgVerifyTable = verifySchema + "." + verifyTable

func TestPGVerifyIntegration(t *testing.T) {
	src := openDSN(t, "JINLIANG_TEST_PG_SRC_DSN")
	tgt := openDSN(t, "JINLIANG_TEST_PG_DSN")
	ctx := context.Background()

	for _, db := range []*runtime.DB{src, tgt} {
		mustExec(t, db, `CREATE SCHEMA IF NOT EXISTS `+verifySchema)
	}
	setupPG(t, src, tgt, true)
	t.Cleanup(func() {
		mustExec(t, src, `DROP SCHEMA IF EXISTS `+verifySchema+` CASCADE`)
		mustExec(t, tgt, `DROP SCHEMA IF EXISTS `+verifySchema+` CASCADE`)
	})
	srcS, tgtS := src.WithSchema(verifySchema), tgt.WithSchema(verifySchema)

	// 位点：源端当前 WAL 与目标端 _jl_meta.position 对得上（正例）。
	var lsn string
	rows, _, err := src.QueryRows(ctx, `SELECT pg_current_wal_lsn()::text`)
	if err != nil || len(rows) != 1 {
		t.Fatalf("read pg_current_wal_lsn: %v", err)
	}
	lsn = fmt.Sprint(rows[0][0])
	setPGPosition(t, tgt, lsn)

	opts := verify.Options{
		Source: srcS, Target: tgtS,
		SourceSchema: verifySchema, TargetSchema: verifySchema,
		Tables: []string{verifyTable}, Level: verify.LevelAll, JobKey: "verifytest",
		SourcePosition: func(context.Context) (string, error) { return lsn, nil },
		TargetPosition: func(ctx context.Context) (string, error) {
			return verify.DBTargetPosition(ctx, tgtS, "verifytest")
		},
	}
	rep, err := verify.Run(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK {
		t.Fatalf("baseline should be green: %+v", rep.Levels)
	}

	// 数据级反例：改一行、删一行 → 必须报出来。
	mustExec(t, tgt, `UPDATE `+pgVerifyTable+` SET amount = amount + 1 WHERE id = 2`)
	mustExec(t, tgt, `DELETE FROM `+pgVerifyTable+` WHERE id = 5`)
	opts.Level = verify.LevelData
	rep, err = verify.Run(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK || !hasFinding(rep, "row_count_mismatch") || !hasFinding(rep, "row_mismatch") {
		t.Fatalf("data corruption not reported: %+v", rep.Levels)
	}

	// 恢复数据，再破坏结构（删一列）→ 结构级必须红。
	setupPG(t, src, tgt, false)
	mustExec(t, tgt, `ALTER TABLE `+pgVerifyTable+` DROP COLUMN name`)
	opts.Level = verify.LevelStructure
	rep, err = verify.Run(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK || !hasFinding(rep, "missing_column") {
		t.Fatalf("structure drift not reported: %+v", rep.Levels)
	}

	// 一致性反例：目标位点超前源端 → 必须红。
	setupPG(t, src, tgt, false)
	var cur uint64
	rows, _, _ = src.QueryRows(ctx, `SELECT pg_current_wal_lsn()::text`)
	var lsnErr error
	cur, lsnErr = runtime.ParseLSN(fmt.Sprint(rows[0][0]))
	if lsnErr != nil {
		t.Fatalf("parse current LSN: %v", lsnErr)
	}
	ahead := fmt.Sprintf("%X/%X", cur>>32+1, cur&0xffffffff)
	setPGPosition(t, tgt, ahead)
	opts.Level = verify.LevelConsistency
	rep, err = verify.Run(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK || !hasFinding(rep, "target_ahead") {
		t.Fatalf("target position ahead not reported: %+v", rep.Levels)
	}
}

func setupPG(t *testing.T, src, tgt *runtime.DB, createMeta bool) {
	t.Helper()
	mustExec(t, src, `DROP TABLE IF EXISTS `+pgVerifyTable)
	mustExec(t, tgt, `DROP TABLE IF EXISTS `+pgVerifyTable)
	for _, db := range []*runtime.DB{src, tgt} {
		mustExec(t, db, `CREATE TABLE `+pgVerifyTable+` (id bigint PRIMARY KEY, name text, amount numeric(12,2))`)
		mustExec(t, db, `INSERT INTO `+pgVerifyTable+` VALUES (1,'alice',10.00),(2,'bob',20.00),(3,NULL,30.00),(4,'dave',40.00),(5,'eve',50.00)`)
	}
	if createMeta {
		mustExec(t, tgt, `CREATE TABLE IF NOT EXISTS `+verifySchema+`._jl_meta (
			job_key text PRIMARY KEY, name text NOT NULL, instance_id text NOT NULL DEFAULT '',
			schema_version int NOT NULL DEFAULT 1, job_hash text NOT NULL DEFAULT '',
			schema_hash text, phase text NOT NULL DEFAULT 'streaming', position text,
			watermark text, snapshot_id text,
			started_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now())`)
	}
}

func setPGPosition(t *testing.T, tgt *runtime.DB, pos string) {
	t.Helper()
	mustExec(t, tgt, `INSERT INTO `+verifySchema+`._jl_meta (job_key, name, instance_id, position, phase)
		VALUES ('verifytest','verify','integration-test','`+pos+`','streaming')
		ON CONFLICT (job_key) DO UPDATE SET position = EXCLUDED.position`)
}

func hasFinding(rep verify.Report, code string) bool {
	for _, lr := range rep.Levels {
		for _, f := range lr.Findings {
			if f.Code == code {
				return true
			}
		}
	}
	return false
}

func TestMySQLVerifyIntegration(t *testing.T) {
	src := openDSN(t, "JINLIANG_TEST_MYSQL_SRC_DSN")
	tgt := openDSN(t, "JINLIANG_TEST_MYSQL_DSN")
	ctx := context.Background()

	setupMySQL(t, src, tgt, true)
	t.Cleanup(func() {
		mustExec(t, src, `DROP TABLE IF EXISTS `+verifyTable)
		mustExec(t, tgt, `DROP TABLE IF EXISTS `+verifyTable)
		mustExec(t, tgt, `DELETE FROM _jl_meta WHERE job_key = 'verifytest'`)
	})

	pos, err := verify.DBSourcePosition(ctx, src, "")
	if err != nil {
		t.Fatal(err)
	}
	if pos == "" {
		t.Fatal("MySQL source returned no binlog position")
	}
	setMySQLPosition(t, tgt, pos)

	opts := verify.Options{
		Source: src, Target: tgt,
		SourceSchema: src.Schema(), TargetSchema: tgt.Schema(),
		Tables: []string{verifyTable}, Level: verify.LevelAll, JobKey: "verifytest",
		SourcePosition: func(context.Context) (string, error) { return pos, nil },
		TargetPosition: func(ctx context.Context) (string, error) {
			return verify.DBTargetPosition(ctx, tgt, "verifytest")
		},
	}
	rep, err := verify.Run(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.OK {
		t.Fatalf("baseline should be green: %+v", rep.Levels)
	}

	mustExec(t, tgt, `UPDATE `+verifyTable+` SET amount = amount + 1 WHERE id = 2`)
	opts.Level = verify.LevelData
	rep, err = verify.Run(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK || !hasFinding(rep, "row_mismatch") {
		t.Fatalf("data corruption not reported: %+v", rep.Levels)
	}

	setupMySQL(t, src, tgt, false)
	mustExec(t, tgt, `ALTER TABLE `+verifyTable+` DROP COLUMN name`)
	opts.Level = verify.LevelStructure
	rep, err = verify.Run(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK || !hasFinding(rep, "missing_column") {
		t.Fatalf("structure drift not reported: %+v", rep.Levels)
	}

	// 一致性反例：目标位点超前。
	setupMySQL(t, src, tgt, false)
	file, off, ok := runtime.BinlogPos(pos)
	if !ok {
		t.Fatalf("unparseable binlog position %q", pos)
	}
	ahead := file + ":" + strconv.FormatInt(off+100000, 10)
	setMySQLPosition(t, tgt, ahead)
	opts.Level = verify.LevelConsistency
	rep, err = verify.Run(ctx, opts)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OK {
		t.Fatalf("target ahead not reported: %+v", rep.Levels)
	}
}

func setupMySQL(t *testing.T, src, tgt *runtime.DB, createMeta bool) {
	t.Helper()
	mustExec(t, src, `DROP TABLE IF EXISTS `+verifyTable)
	mustExec(t, tgt, `DROP TABLE IF EXISTS `+verifyTable)
	for _, db := range []*runtime.DB{src, tgt} {
		mustExec(t, db, `CREATE TABLE `+verifyTable+` (id bigint PRIMARY KEY, name text, amount decimal(12,2))`)
		mustExec(t, db, `INSERT INTO `+verifyTable+` VALUES (1,'alice',10.00),(2,'bob',20.00),(3,NULL,30.00),(4,'dave',40.00),(5,'eve',50.00)`)
	}
	if createMeta {
		mustExec(t, tgt, `CREATE TABLE IF NOT EXISTS _jl_meta (
			job_key varchar(16) PRIMARY KEY, name varchar(128) NOT NULL, instance_id varchar(32) NOT NULL DEFAULT '',
			schema_version int NOT NULL DEFAULT 1, job_hash varchar(64) NOT NULL DEFAULT '',
			schema_hash varchar(64), phase varchar(16) NOT NULL DEFAULT 'streaming', position text,
			watermark text, snapshot_id varchar(128),
			started_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
			updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6))`)
	}
}

func setMySQLPosition(t *testing.T, tgt *runtime.DB, pos string) {
	t.Helper()
	mustExec(t, tgt, `INSERT INTO _jl_meta (job_key, name, position, phase)
		VALUES ('verifytest','verify','`+pos+`','streaming')
		ON DUPLICATE KEY UPDATE position = VALUES(position)`)
}
