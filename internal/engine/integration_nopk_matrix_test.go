//go:build integration && nopkmatrix

package engine_test

// 矩阵格：无主键变体（docs/DESIGN-NO-PRIMARY-KEY.md §2.4 / §5 S4）。
//
// 为什么多一个 `nopkmatrix` 构建标签：这个文件里的用例**只能由格执行器带着参数调起**，
// 它没有「自己判断要不要跳过」的余地（缺参数就是红）。若它跟着普通 `-tags=integration`
// 一起编译，别的集成 job（b1-engine、nightly 的 integration）跑整包时会把它跑起来 →
// 缺参数直接红；改成 t.Skip 又会让「跳过」伪装成通过。所以：**只有执行器带这个标签**，
// 别的 job 连编译都不带它，用例不会凭空出现，也不会凭空消失。
//
// **1 格 = 1 条链路 × 1 个场景的一次可执行用例**。格由 `scripts/nopk_matrix.py`
// 按 `test/nopk-matrix.yaml` 逐格调起，用环境变量把「链路 / 场景 / 版本」传进来：
//
//	JINLIANG_NOPK_LINK        pg-pg | mysql-pg | pg-mysql | mysql-mysql
//	JINLIANG_NOPK_SCENARIO    snapshot | insert | pause | downgrade | unique_key_once | unique_key_continuous
//	JINLIANG_NOPK_SRC_VERSION 声明版本（如 10 / 5.7 / 8.4 / 9），连到的库必须对得上
//	JINLIANG_NOPK_DST_VERSION 同上（目标端）
//
// 缺 DSN / 缺 env 一律**失败**（不跳过）——与全仓集成测试同一纪律。
// 这里不新增任何产品逻辑：只把 S1–S3 已落地的写路径 / 预检 / 按表暂停串成可执行的格。
//
// 为什么要有这一层：单测钉的是函数分支；这一层钉的是「真库上这条链路真的会那样表现」。
// 版本标签还会被核对（连到的服务端版本对不上就是格子标错），防止「标着 PG 10→11、
// 实际跑了 16→16」这种假覆盖。

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/spencerswagger/jinliang/internal/connect"
	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/engine"
	"github.com/spencerswagger/jinliang/internal/pgsink"
	"github.com/spencerswagger/jinliang/internal/preflight"
	"github.com/spencerswagger/jinliang/internal/source"
)

func TestNoPKMatrixCell(t *testing.T) {
	link := strings.TrimSpace(os.Getenv("JINLIANG_NOPK_LINK"))
	scenario := strings.TrimSpace(os.Getenv("JINLIANG_NOPK_SCENARIO"))
	if link == "" || scenario == "" {
		t.Fatal("缺 JINLIANG_NOPK_LINK / JINLIANG_NOPK_SCENARIO：矩阵格由 scripts/nopk_matrix.py 调起（见 test/nopk-matrix.yaml）")
	}

	srcDSN, dstDSN := nopkDSNs(t, link)
	nopkAssertVersion(t, "源端", srcDSN, os.Getenv("JINLIANG_NOPK_SRC_VERSION"))
	nopkAssertVersion(t, "目标端", dstDSN, os.Getenv("JINLIANG_NOPK_DST_VERSION"))

	switch link {
	case "pg-pg":
		switch scenario {
		case "snapshot":
			nopkPGPGSnapshot(t, srcDSN, dstDSN)
		case "insert":
			nopkPGPGInsert(t, srcDSN, dstDSN)
		case "pause":
			nopkPGPGPause(t, srcDSN, dstDSN)
		case "downgrade":
			nopkPGPGDowngrade(t, srcDSN, dstDSN)
		case "unique_key_once":
			nopkPGPGUniqueKeyOnce(t, srcDSN, dstDSN)
		case "unique_key_continuous":
			nopkPGPGUniqueKeyContinuous(t, srcDSN, dstDSN)
		default:
			t.Fatalf("pg-pg 没有场景 %q", scenario)
		}
	case "mysql-pg":
		switch scenario {
		case "snapshot":
			nopkMySQLToPGSnapshot(t, srcDSN, dstDSN)
		case "insert":
			nopkMySQLToPGInsert(t, srcDSN, dstDSN)
		case "pause":
			nopkMySQLToPGPause(t, srcDSN, dstDSN)
		default:
			t.Fatalf("mysql-pg 没有场景 %q（该方向只铺 snapshot / insert / pause）", scenario)
		}
	case "pg-mysql":
		switch scenario {
		case "snapshot":
			nopkPGToMySQLSnapshot(t, srcDSN, dstDSN)
		case "unique_key_once":
			nopkPGToMySQLUniqueKeyOnce(t, srcDSN, dstDSN)
		default:
			t.Fatalf("pg-mysql 没有场景 %q：该方向只全量（internal/core/direction.go），"+
				"只有 snapshot / unique_key_once 可达", scenario)
		}
	case "mysql-mysql":
		t.Fatalf("MySQL→MySQL 方向未接通（internal/core/direction.go 的 pendingDirections，欠账 D6）：" +
			"test/nopk-matrix.yaml 里这 12 格是 pending，**不许**当已覆盖——能跑到这里说明选格逻辑坏了")
	default:
		t.Fatalf("不认识的链路 %q", link)
	}
}

// ── 链路 / 版本 ──────────────────────────────────────────────

func nopkDSNs(t *testing.T, link string) (src, dst string) {
	t.Helper()
	need := func(name string) string {
		v := strings.TrimSpace(os.Getenv(name))
		if v == "" {
			t.Fatalf("链路 %s 缺 %s：矩阵格必须跑在真库上，缺 DSN 就是失败（矩阵见 test/nopk-matrix.yaml）", link, name)
		}
		return v
	}
	switch link {
	case "pg-pg":
		return need("JINLIANG_TEST_PG_SRC_DSN"), need("JINLIANG_TEST_PG_DSN")
	case "mysql-pg":
		return need("JINLIANG_TEST_MYSQL_SRC_DSN"), need("JINLIANG_TEST_PG_DSN")
	case "pg-mysql":
		return need("JINLIANG_TEST_PG_SRC_DSN"), need("JINLIANG_TEST_MYSQL_DSN")
	case "mysql-mysql":
		return need("JINLIANG_TEST_MYSQL_SRC_DSN"), need("JINLIANG_TEST_MYSQL_DST_DSN")
	}
	t.Fatalf("不认识的链路 %q", link)
	return "", ""
}

// nopkAssertVersion 核对「格子标的版本」与「真连到的服务端版本」。
// 标错版本比不跑更糟：矩阵绿了但覆盖的是另一档。
func nopkAssertVersion(t *testing.T, side, raw, label string) {
	t.Helper()
	label = strings.TrimSpace(label)
	if label == "" {
		t.Fatal("缺 JINLIANG_NOPK_SRC_VERSION / JINLIANG_NOPK_DST_VERSION：格子必须标版本（脚本从 test/nopk-matrix.yaml 传）")
	}
	ep, err := dsn.Parse(raw)
	if err != nil {
		t.Fatalf("解析 %s DSN 失败: %v", side, err)
	}
	conn, err := connect.Open(ep)
	if err != nil {
		t.Fatalf("连不上%s: %v", side, err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got, err := conn.Version(ctx)
	if err != nil {
		t.Fatalf("读%s版本失败: %v", side, err)
	}
	if !nopkVersionMatches(label, got) {
		t.Fatalf("%s标的是 %s，实际连到 %s：格子标错版本比不跑更糟", side, label, got)
	}
}

func nopkVersionMatches(label, actual string) bool {
	label, actual = strings.TrimSpace(label), strings.TrimSpace(actual)
	if label == "" {
		return true
	}
	return actual == label || strings.HasPrefix(actual, label+".")
}

// ── 建任务（三条链路共用；只有连接类型与目标 schema 不同）──────

type nopkJobSpec struct {
	srcDSN, dstDSN   string
	srcKind, dstKind string
	name             string
	tables           []string
	uniqueKeys       map[string][]string
	freq             string
}

func nopkCreateJob(t *testing.T, s nopkJobSpec) (*control.Store, string) {
	t.Helper()
	ctx := context.Background()
	store, err := control.Open(control.Options{DataDir: t.TempDir(), Autogen: true})
	if err != nil {
		t.Fatalf("打开控制库失败: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	mk := func(nm, kind, raw string) (string, string) {
		ep, err := dsn.Parse(raw)
		if err != nil {
			t.Fatalf("解析 %s DSN 失败: %v", nm, err)
		}
		in := control.ConnectionInput{
			Name: nm, Kind: kind, Host: ep.Host, Port: ep.Port,
			Database: ep.Database, Username: ep.Username, Password: ep.Password,
		}
		if kind == dsn.KindPostgres {
			in.Params = map[string]string{"sslmode": "disable"}
		}
		c, err := store.CreateConnection(ctx, in)
		if err != nil {
			t.Fatalf("建 %s 连接失败: %v", nm, err)
		}
		return c.ID, ep.Database
	}
	srcID, srcDB := mk("src", s.srcKind, s.srcDSN)
	dstID, _ := mk("dst", s.dstKind, s.dstDSN)

	content := map[string]control.TableDoc{}
	for _, tbl := range s.tables {
		td := control.TableDoc{}
		if uk, ok := s.uniqueKeys[tbl]; ok {
			td.UniqueKey = uk
		}
		content[tbl] = td
	}
	srcSchema := "public"
	if s.srcKind == dsn.KindMySQL {
		srcSchema = srcDB
	}
	tgt := control.TargetDoc{Kind: s.dstKind, OnExists: "append"}
	if s.dstKind == dsn.KindPostgres {
		tgt.Schema = "public"
	}
	job, err := store.CreateJob(ctx, control.JobInput{
		Name: s.name, SourceID: srcID, TargetID: dstID,
		Spec: control.JobDoc{
			Version: 1, Job: s.name,
			Source:  control.SourceDoc{Kind: s.srcKind, Scope: &control.ScopeDoc{Schema: srcSchema}},
			Target:  tgt,
			Mode:    control.ModeDoc{Frequency: s.freq},
			Content: &control.ContentDoc{Tables: content},
		},
	})
	if err != nil {
		t.Fatalf("建任务失败: %v", err)
	}
	if s.srcKind == dsn.KindPostgres {
		dropJobSlot(t, s.srcDSN, job.JobKey)
	}
	return store, job.ID
}

func nopkJobKey(t *testing.T, store *control.Store, jobID string) string {
	t.Helper()
	job, err := store.GetJob(context.Background(), jobID)
	if err != nil {
		t.Fatalf("取任务失败: %v", err)
	}
	return job.JobKey
}

// ── 造数与清理 ────────────────────────────────────────────────

func nopkSeedPGNoPK(t *testing.T, src *pgx.Conn, table string, rows int) {
	t.Helper()
	exec(t, src, "DROP TABLE IF EXISTS "+table)
	exec(t, src, "CREATE TABLE "+table+" (id bigint, payload text, v bigint)")
	exec(t, src, "INSERT INTO "+table+" SELECT g, 'r'||g, g FROM generate_series(1,$1) g", rows)
	t.Cleanup(func() { exec(t, src, "DROP TABLE IF EXISTS "+table) })
}

func nopkSeedPGWithPK(t *testing.T, src *pgx.Conn, table string, rows int) {
	t.Helper()
	exec(t, src, "DROP TABLE IF EXISTS "+table)
	exec(t, src, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, payload text)")
	exec(t, src, "INSERT INTO "+table+" SELECT g, 'b'||g FROM generate_series(1,$1) g", rows)
	t.Cleanup(func() { exec(t, src, "DROP TABLE IF EXISTS "+table) })
}

func nopkResetPGTarget(t *testing.T, dst *pgx.Conn, table string) {
	t.Helper()
	exec(t, dst, "DROP TABLE IF EXISTS "+table)
	exec(t, dst, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")
}

func nopkSeedMySQLNoPK(t *testing.T, db *sql.DB, table string, rows int) {
	t.Helper()
	myExec(t, db, "DROP TABLE IF EXISTS `"+table+"`")
	myExec(t, db, "CREATE TABLE `"+table+"` (id BIGINT NOT NULL, payload VARCHAR(64) NOT NULL, v BIGINT NULL) ENGINE=InnoDB")
	nopkInsertMySQLRows(t, db, table, 1, rows, "r")
	t.Cleanup(func() { myExec(t, db, "DROP TABLE IF EXISTS `"+table+"`") })
}

func nopkSeedMySQLWithPK(t *testing.T, db *sql.DB, table string, rows int) {
	t.Helper()
	myExec(t, db, "DROP TABLE IF EXISTS `"+table+"`")
	myExec(t, db, "CREATE TABLE `"+table+"` (id BIGINT NOT NULL PRIMARY KEY, payload VARCHAR(64) NOT NULL) ENGINE=InnoDB")
	var b strings.Builder
	fmt.Fprintf(&b, "INSERT INTO `%s` (id,payload) VALUES ", table)
	for i := 1; i <= rows; i++ {
		if i > 1 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "(%d,'b%d')", i, i)
	}
	myExec(t, db, b.String())
	t.Cleanup(func() { myExec(t, db, "DROP TABLE IF EXISTS `"+table+"`") })
}

// nopkInsertMySQLRows 从 from..to 灌行（不用 CTE：5.7 没有 WITH RECURSIVE）。
func nopkInsertMySQLRows(t *testing.T, db *sql.DB, table string, from, to int, prefix string) {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "INSERT INTO `%s` (id,payload,v) VALUES ", table)
	for i := from; i <= to; i++ {
		if i > from {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "(%d,'%s%d',%d)", i, prefix, i, i)
	}
	myExec(t, db, b.String())
}

// ── 断言小件 ─────────────────────────────────────────────────

func nopkPausedPG(t *testing.T, dst *pgx.Conn, store *control.Store, jobID string) map[string]pgsink.PausedTable {
	t.Helper()
	paused, err := readPaused(t, dst, nopkJobKey(t, store, jobID))
	if err != nil {
		t.Fatalf("读暂停记录失败: %v", err)
	}
	return paused
}

func nopkPosPG(t *testing.T, dst *pgx.Conn, store *control.Store, jobID string) string {
	t.Helper()
	var pos string
	if err := dst.QueryRow(context.Background(),
		"SELECT COALESCE(position,'') FROM _jl_meta WHERE job_key=$1", nopkJobKey(t, store, jobID)).Scan(&pos); err != nil {
		t.Fatalf("读位点失败: %v", err)
	}
	return pos
}

func nopkPosMySQL(t *testing.T, db *sql.DB) string {
	t.Helper()
	var pos string
	if err := db.QueryRowContext(context.Background(),
		"SELECT COALESCE(position,'') FROM `_jl_meta` LIMIT 1").Scan(&pos); err != nil {
		t.Fatalf("读 MySQL 目标端位点失败: %v", err)
	}
	return pos
}

// ── 场景：PG → PG ────────────────────────────────────────────

func nopkPGPGSnapshot(t *testing.T, srcDSN, dstDSN string) {
	src, dst := mustConn(t, srcDSN), mustConn(t, dstDSN)
	const tbl, rows = "nopk_pp_snap", 50
	nopkSeedPGNoPK(t, src, tbl, rows)
	nopkResetPGTarget(t, dst, tbl)

	store, jobID := nopkCreateJob(t, nopkJobSpec{
		srcDSN: srcDSN, dstDSN: dstDSN, srcKind: dsn.KindPostgres, dstKind: dsn.KindPostgres,
		name: "nopk-pp-snap", tables: []string{tbl}, freq: "continuous+initial",
	})
	res, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, SnapshotOnly: true})
	if err != nil {
		t.Fatalf("无主键表全量失败（应 insert-only 照跑）: %v", err)
	}
	if res.ExitCode != 0 {
		t.Fatalf("无主键全量没有暂停表，应退 0，实际 %d", res.ExitCode)
	}
	if got := pgCount(t, dst, tbl); got != rows {
		t.Fatalf("目标端应有 %d 行，实际 %d", rows, got)
	}
	if paused := nopkPausedPG(t, dst, store, jobID); len(paused) != 0 {
		t.Fatalf("全量不该暂停任何表：%v", paused)
	}
}

func nopkPGPGInsert(t *testing.T, srcDSN, dstDSN string) {
	src, dst := mustConn(t, srcDSN), mustConn(t, dstDSN)
	const tbl, rows = "nopk_pp_ins", 40
	nopkSeedPGNoPK(t, src, tbl, rows)
	nopkResetPGTarget(t, dst, tbl)

	store, jobID := nopkCreateJob(t, nopkJobSpec{
		srcDSN: srcDSN, dstDSN: dstDSN, srcKind: dsn.KindPostgres, dstKind: dsn.KindPostgres,
		name: "nopk-pp-ins", tables: []string{tbl}, freq: "continuous+initial",
	})
	if _, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, SnapshotOnly: true}); err != nil {
		t.Fatalf("首搬失败: %v", err)
	}
	posBefore := nopkPosPG(t, dst, store, jobID)

	exec(t, src, "INSERT INTO "+tbl+" (id,payload,v) VALUES (1001,'new-1',1),(1002,'new-2',2),(1003,'new-3',3)")
	if _, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, IdleAfter: 700 * time.Millisecond}); err != nil {
		t.Fatalf("无主键表增量（纯 INSERT）不该失败: %v", err)
	}
	if got := pgCount(t, dst, tbl); got != rows+3 {
		t.Fatalf("追加 3 行后目标端应有 %d 行，实际 %d", rows+3, got)
	}
	if paused := nopkPausedPG(t, dst, store, jobID); len(paused) != 0 {
		t.Fatalf("纯 INSERT 不该暂停：%v", paused)
	}
	if pos := nopkPosPG(t, dst, store, jobID); pos == posBefore {
		t.Fatalf("位点必须前进（%q）", posBefore)
	}
}

func nopkPGPGPause(t *testing.T, srcDSN, dstDSN string) {
	src, dst := mustConn(t, srcDSN), mustConn(t, dstDSN)
	const ta, tb = "nopk_pp_pause", "nopk_pp_ref"
	nopkSeedPGNoPK(t, src, ta, 300)
	nopkSeedPGWithPK(t, src, tb, 200)
	nopkResetPGTarget(t, dst, ta)
	exec(t, dst, "DROP TABLE IF EXISTS "+tb)
	// PG 源的前置（R5 / PG-06）：不设 FULL，源端在第一条 UPDATE 上就断流，
	// 测的会是 PostgreSQL 而不是我们。这一步是 setup，不是被测行为。
	exec(t, src, "ALTER TABLE "+ta+" REPLICA IDENTITY FULL")

	store, jobID := nopkCreateJob(t, nopkJobSpec{
		srcDSN: srcDSN, dstDSN: dstDSN, srcKind: dsn.KindPostgres, dstKind: dsn.KindPostgres,
		name: "nopk-pp-pause", tables: []string{ta, tb}, freq: "continuous+initial",
	})
	if _, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, SnapshotOnly: true}); err != nil {
		t.Fatalf("首搬失败: %v", err)
	}
	posBefore := nopkPosPG(t, dst, store, jobID)

	exec(t, src, "UPDATE "+ta+" SET payload='changed', v=v+1000 WHERE id=42")
	exec(t, src, "INSERT INTO "+tb+" (id,payload) VALUES (1001,'after-nopk')")
	res, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, IdleAfter: 500 * time.Millisecond})
	if err != nil {
		t.Fatalf("无主键表的改只该停它自己，不该打死整轮: %v", err)
	}
	if res.ExitCode != engine.ExitNeedsAction {
		t.Fatalf("有暂停表必须退 %d（ExitNeedsAction），实际 %d", engine.ExitNeedsAction, res.ExitCode)
	}
	paused := nopkPausedPG(t, dst, store, jobID)
	rec, ok := paused[ta]
	if !ok || len(paused) != 1 {
		t.Fatalf("应只暂停 %s，实际 %v", ta, paused)
	}
	if rec.Reason != pgsink.PauseReasonNoPKWrite {
		t.Fatalf("原因码应是 %q，得到 %q（detail=%s）", pgsink.PauseReasonNoPKWrite, rec.Reason, rec.Detail)
	}
	if rec.SkippedRows < 1 {
		t.Fatalf("跳过窗口必须记账（skipped_rows ≥ 1），得到 %+v", rec)
	}
	if got := pgCount(t, dst, ta); got != 300 {
		t.Fatalf("跳过该表的改动不该多出一行，实际 %d", got)
	}
	var payload string
	if err := dst.QueryRow(context.Background(), "SELECT payload FROM "+ta+" WHERE id=42").Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if payload == "changed" {
		t.Fatal("没有身份的表，改动写不进去；写进去了说明 UPDATE 被当成 INSERT/upsert 硬写")
	}
	var n int
	if err := dst.QueryRow(context.Background(), "SELECT count(*) FROM "+tb+" WHERE id=1001").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("其它表必须照常推进（这就是「只停该表」），实际 %d", n)
	}
	if pos := nopkPosPG(t, dst, store, jobID); pos == posBefore {
		t.Fatalf("跳过这张表不等于停下同步：位点必须前进（%q）", posBefore)
	}

	// 修好身份后重跑：整表重搬补齐、暂停解除。
	exec(t, src, "ALTER TABLE "+ta+" ADD PRIMARY KEY (id)")
	exec(t, dst, "ALTER TABLE "+ta+" ADD PRIMARY KEY (id)")
	res2, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, IdleAfter: 700 * time.Millisecond})
	if err != nil {
		t.Fatalf("补上主键后重跑应成功: %v", err)
	}
	if res2.RowsCopied < 1 {
		t.Fatalf("暂停表必须整表重搬（RowsCopied=%d）", res2.RowsCopied)
	}
	if paused := nopkPausedPG(t, dst, store, jobID); len(paused) != 0 {
		t.Fatalf("重搬搬成 → 必须解除暂停，仍有：%v", paused)
	}
	if err := dst.QueryRow(context.Background(), "SELECT v FROM "+ta+" WHERE id=42").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1042 {
		t.Fatalf("重搬后应看到源端新值 v=1042，实际 %d", n)
	}
}

func nopkPGPGDowngrade(t *testing.T, srcDSN, dstDSN string) {
	src, dst := mustConn(t, srcDSN), mustConn(t, dstDSN)
	const tbl, rows = "nopk_pp_down", 20
	nopkSeedPGNoPK(t, src, tbl, rows)
	nopkResetPGTarget(t, dst, tbl)

	// 「提示并允许」：预检不许阻断，并且两张连接卡片把两侧版本写出来（版本可见）。
	srcVer := nopkServerVersion(t, srcDSN)
	dstVer := nopkServerVersion(t, dstDSN)
	rep := preflight.Run(preflight.Input{
		JobName: "nopk-pp-down", SourceKind: "postgres", TargetKind: "postgres",
		SourceVer: srcVer, TargetVer: dstVer,
		SourceCaps: source.CapSet{source.CapCDC: {Supported: true}},
		SourceCat:  &source.Catalog{Tables: []source.Table{{TableRef: source.TableRef{Schema: "public", Table: tbl}}}},
		Tables:     []string{tbl},
		Mode:       "continuous+initial",
		OnExists:   "append",
		BatchRows:  500,
	})
	if !rep.CanStart {
		t.Fatalf("降级方向只提示不阻断（允许继续），CanStart 却是 false：%+v", rep.Checks)
	}
	nopkRequireDetail(t, rep, "NET-01", srcVer)
	nopkRequireDetail(t, rep, "NET-02", dstVer)

	// 「允许继续」的另一半：引擎照跑，数据真搬过去。
	store, jobID := nopkCreateJob(t, nopkJobSpec{
		srcDSN: srcDSN, dstDSN: dstDSN, srcKind: dsn.KindPostgres, dstKind: dsn.KindPostgres,
		name: "nopk-pp-down", tables: []string{tbl}, freq: "continuous+initial",
	})
	if _, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, SnapshotOnly: true}); err != nil {
		t.Fatalf("降级方向（目标版本更低）应允许继续，实际失败: %v", err)
	}
	if got := pgCount(t, dst, tbl); got != rows {
		t.Fatalf("目标端应有 %d 行，实际 %d", rows, got)
	}
	t.Logf("降级格（%s → %s）：预检不阻断 + 引擎照跑。注意：产品今天**没有**降级专属提示卡片，"+
		"本格只证「允许继续」这一半（版本只在 NET-01/02 里可见），不冒充「已提示」", srcVer, dstVer)
}

func nopkServerVersion(t *testing.T, raw string) string {
	t.Helper()
	ep, err := dsn.Parse(raw)
	if err != nil {
		t.Fatalf("解析 DSN 失败: %v", err)
	}
	conn, err := connect.Open(ep)
	if err != nil {
		t.Fatalf("连库失败: %v", err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	v, err := conn.Version(ctx)
	if err != nil {
		t.Fatalf("读版本失败: %v", err)
	}
	return v
}

func nopkRequireDetail(t *testing.T, rep source.Report, id, want string) {
	t.Helper()
	for _, c := range rep.Checks {
		if c.ID == id {
			if !strings.Contains(c.Detail, want) {
				t.Fatalf("%s 的 Detail 应写出服务端版本 %q（版本可见才谈得上「提示」），实际 %q", id, want, c.Detail)
			}
			return
		}
	}
	t.Fatalf("预检报告里没有 %s", id)
}

func nopkPGPGUniqueKeyOnce(t *testing.T, srcDSN, dstDSN string) {
	src, dst := mustConn(t, srcDSN), mustConn(t, dstDSN)
	const tbl = "nopk_pp_uk_once"
	exec(t, src, "DROP TABLE IF EXISTS "+tbl)
	exec(t, src, "CREATE TABLE "+tbl+" (tenant_id int NOT NULL, event_no text NOT NULL, payload text)")
	exec(t, src, "INSERT INTO "+tbl+" SELECT 1, 'e'||g, 'row-'||g FROM generate_series(1,10) g")
	// 发布集建好之后，无主键表的改/删需要整行旧镜像（R5 / PG-06），否则源端自己报
	// "cannot update ... does not have a replica identity"。这是 setup，不是被测行为。
	exec(t, src, "ALTER TABLE "+tbl+" REPLICA IDENTITY FULL")
	t.Cleanup(func() { exec(t, src, "DROP TABLE IF EXISTS "+tbl) })
	nopkResetPGTarget(t, dst, tbl)
	exec(t, dst, "DROP TABLE IF EXISTS "+tbl)
	t.Cleanup(func() { exec(t, dst, "DROP TABLE IF EXISTS "+tbl) })

	store, jobID := nopkCreateJob(t, nopkJobSpec{
		srcDSN: srcDSN, dstDSN: dstDSN, srcKind: dsn.KindPostgres, dstKind: dsn.KindPostgres,
		name: "nopk-pp-uk-once", tables: []string{tbl}, freq: "once",
		uniqueKeys: map[string][]string{tbl: {"tenant_id", "event_no"}},
	})
	// once 的口径是**每轮整轮重建**（§4.2）：声明在 once 下不进写路径
	// （engine/target.go 的 sinkDeclaredUniqueKeys），幂等靠「清表 + 重搬」而不是 upsert。
	// 这里就按这个口径验：重放不翻倍，且结果等于源端当前状态。
	if _, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, SnapshotOnly: true}); err != nil {
		t.Fatalf("声明 unique_key 的 once 全量失败（once 不该因为声明而拒跑）: %v", err)
	}
	if got := pgCount(t, dst, tbl); got != 10 {
		t.Fatalf("首轮应有 10 行，实际 %d", got)
	}
	exec(t, src, "UPDATE "+tbl+" SET payload='changed' WHERE tenant_id=1 AND event_no='e3'")
	exec(t, src, "DELETE FROM "+tbl+" WHERE tenant_id=1 AND event_no='e5'")
	if _, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, SnapshotOnly: true}); err != nil {
		t.Fatalf("once 重放失败: %v", err)
	}
	if got := pgCount(t, dst, tbl); got != 9 {
		t.Fatalf("once 重放必须等于源端当前状态（9 行），实际 %d：不清表就会把删掉的行永远留在目标端", got)
	}
	var payload string
	if err := dst.QueryRow(context.Background(), "SELECT payload FROM "+tbl+" WHERE tenant_id=1 AND event_no='e3'").Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if payload != "changed" {
		t.Fatalf("重放后应看到源端新值，实际 %q", payload)
	}
}

func nopkPGPGUniqueKeyContinuous(t *testing.T, srcDSN, dstDSN string) {
	src, dst := mustConn(t, srcDSN), mustConn(t, dstDSN)
	const tbl = "nopk_pp_uk_cont"
	exec(t, src, "DROP TABLE IF EXISTS "+tbl)
	exec(t, src, "CREATE TABLE "+tbl+" (tenant_id int NOT NULL, event_no text NOT NULL, payload text)")
	exec(t, src, "INSERT INTO "+tbl+" SELECT 1, 'e'||g, 'row-'||g FROM generate_series(1,10) g")
	exec(t, src, "ALTER TABLE "+tbl+" REPLICA IDENTITY FULL") // 无主键表要拿到旧行标识（R5）
	t.Cleanup(func() { exec(t, src, "DROP TABLE IF EXISTS "+tbl) })

	exec(t, dst, "DROP TABLE IF EXISTS "+tbl)
	exec(t, dst, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")
	exec(t, dst, "CREATE TABLE "+tbl+" (tenant_id int NOT NULL, event_no text NOT NULL, payload text, UNIQUE (tenant_id, event_no))")
	t.Cleanup(func() { exec(t, dst, "DROP TABLE IF EXISTS "+tbl) })

	store, jobID := nopkCreateJob(t, nopkJobSpec{
		srcDSN: srcDSN, dstDSN: dstDSN, srcKind: dsn.KindPostgres, dstKind: dsn.KindPostgres,
		name: "nopk-pp-uk-cont", tables: []string{tbl}, freq: "continuous+initial",
		uniqueKeys: map[string][]string{tbl: {"tenant_id", "event_no"}},
	})
	if _, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, SnapshotOnly: true}); err != nil {
		t.Fatalf("首搬失败: %v", err)
	}
	exec(t, src, "UPDATE "+tbl+" SET payload='changed' WHERE tenant_id=1 AND event_no='e3'")
	exec(t, src, "DELETE FROM "+tbl+" WHERE tenant_id=1 AND event_no='e5'")
	if _, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, IdleAfter: 700 * time.Millisecond}); err != nil {
		t.Fatalf("声明 unique_key 后改/删应走得通: %v", err)
	}
	if paused := nopkPausedPG(t, dst, store, jobID); len(paused) != 0 {
		t.Fatalf("有身份就不该暂停：%v", paused)
	}
	if got := pgCount(t, dst, tbl); got != 9 {
		t.Fatalf("删一行后应剩 9 行，实际 %d", got)
	}
	var payload string
	if err := dst.QueryRow(context.Background(), "SELECT payload FROM "+tbl+" WHERE tenant_id=1 AND event_no='e3'").Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if payload != "changed" {
		t.Fatalf("改行应按声明列定位更新（不是插一版），实际 %q", payload)
	}
}

// ── 场景：MySQL → PG ────────────────────────────────────────

func nopkMySQLToPGSnapshot(t *testing.T, srcDSN, dstDSN string) {
	src, dst := mustMySQL(t, srcDSN), mustConn(t, dstDSN)
	const tbl, rows = "nopk_mp_snap", 50
	nopkSeedMySQLNoPK(t, src, tbl, rows)
	nopkResetPGTarget(t, dst, tbl)

	store, jobID := nopkCreateJob(t, nopkJobSpec{
		srcDSN: srcDSN, dstDSN: dstDSN, srcKind: dsn.KindMySQL, dstKind: dsn.KindPostgres,
		name: "nopk-mp-snap", tables: []string{tbl}, freq: "continuous+initial",
	})
	res, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, SnapshotOnly: true})
	if err != nil {
		t.Fatalf("MySQL 源无主键表全量失败: %v", err)
	}
	if res.ExitCode != 0 {
		t.Fatalf("全量应退 0，实际 %d", res.ExitCode)
	}
	if got := pgCount(t, dst, tbl); got != rows {
		t.Fatalf("目标端应有 %d 行，实际 %d", rows, got)
	}
}

func nopkMySQLToPGInsert(t *testing.T, srcDSN, dstDSN string) {
	src, dst := mustMySQL(t, srcDSN), mustConn(t, dstDSN)
	const tbl, rows = "nopk_mp_ins", 40
	nopkSeedMySQLNoPK(t, src, tbl, rows)
	nopkResetPGTarget(t, dst, tbl)

	store, jobID := nopkCreateJob(t, nopkJobSpec{
		srcDSN: srcDSN, dstDSN: dstDSN, srcKind: dsn.KindMySQL, dstKind: dsn.KindPostgres,
		name: "nopk-mp-ins", tables: []string{tbl}, freq: "continuous+initial",
	})
	if _, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, SnapshotOnly: true}); err != nil {
		t.Fatalf("首搬失败: %v", err)
	}
	posBefore := nopkPosPG(t, dst, store, jobID)
	nopkInsertMySQLRows(t, src, tbl, rows+1, rows+3, "new-")
	if _, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, IdleAfter: 900 * time.Millisecond}); err != nil {
		t.Fatalf("MySQL 源无主键表增量（纯 INSERT）不该失败: %v", err)
	}
	if got := pgCount(t, dst, tbl); got != rows+3 {
		t.Fatalf("追加 3 行后应有 %d 行，实际 %d", rows+3, got)
	}
	if paused := nopkPausedPG(t, dst, store, jobID); len(paused) != 0 {
		t.Fatalf("纯 INSERT 不该暂停：%v", paused)
	}
	if pos := nopkPosPG(t, dst, store, jobID); pos == posBefore {
		t.Fatalf("位点必须前进（%q）", posBefore)
	}
}

func nopkMySQLToPGPause(t *testing.T, srcDSN, dstDSN string) {
	src, dst := mustMySQL(t, srcDSN), mustConn(t, dstDSN)
	const ta, tb = "nopk_mp_pause", "nopk_mp_ref"

	// MySQL 源的前置：binlog_row_image=FULL（MY-03）。CI 的 docker run 已带上；
	// 这里断言一次，免得「配漏了」被读成「产品行为不对」。
	var img string
	if err := src.QueryRowContext(context.Background(), "SELECT @@binlog_row_image").Scan(&img); err != nil {
		t.Fatalf("读 binlog_row_image 失败: %v", err)
	}
	if !strings.EqualFold(img, "FULL") {
		t.Fatalf("MySQL 源的 binlog_row_image=%s（要 FULL）：无主键表的改/删要整行旧镜像才能解出来", img)
	}

	nopkSeedMySQLNoPK(t, src, ta, 300)
	nopkSeedMySQLWithPK(t, src, tb, 200)
	nopkResetPGTarget(t, dst, ta)
	exec(t, dst, "DROP TABLE IF EXISTS "+tb)
	t.Cleanup(func() { exec(t, dst, "DROP TABLE IF EXISTS "+tb) })

	store, jobID := nopkCreateJob(t, nopkJobSpec{
		srcDSN: srcDSN, dstDSN: dstDSN, srcKind: dsn.KindMySQL, dstKind: dsn.KindPostgres,
		name: "nopk-mp-pause", tables: []string{ta, tb}, freq: "continuous+initial",
	})
	if _, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, SnapshotOnly: true}); err != nil {
		t.Fatalf("首搬失败: %v", err)
	}
	posBefore := nopkPosPG(t, dst, store, jobID)

	myExec(t, src, "UPDATE `"+ta+"` SET payload='changed', v=v+1000 WHERE id=42")
	myExec(t, src, "INSERT INTO `"+tb+"` (id,payload) VALUES (1001,'after-nopk')")
	res, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, IdleAfter: 900 * time.Millisecond})
	if err != nil {
		t.Fatalf("无主键表的改只该停它自己: %v", err)
	}
	if res.ExitCode != engine.ExitNeedsAction {
		t.Fatalf("有暂停表必须退 %d，实际 %d", engine.ExitNeedsAction, res.ExitCode)
	}
	paused := nopkPausedPG(t, dst, store, jobID)
	rec, ok := paused[ta]
	if !ok || len(paused) != 1 {
		t.Fatalf("应只暂停 %s，实际 %v", ta, paused)
	}
	if rec.Reason != pgsink.PauseReasonNoPKWrite || rec.SkippedRows < 1 {
		t.Fatalf("暂停记录不对：%+v", rec)
	}
	if got := pgCount(t, dst, ta); got != 300 {
		t.Fatalf("跳过该表的改动不该多出一行，实际 %d", got)
	}
	var n int
	if err := dst.QueryRow(context.Background(), "SELECT count(*) FROM "+tb+" WHERE id=1001").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("其它表必须照常推进，实际 %d", n)
	}
	if pos := nopkPosPG(t, dst, store, jobID); pos == posBefore {
		t.Fatalf("位点必须前进（%q）", posBefore)
	}

	// 修好身份后重跑：整表重搬补齐。
	myExec(t, src, "ALTER TABLE `"+ta+"` ADD PRIMARY KEY (id)")
	exec(t, dst, "ALTER TABLE "+ta+" ADD PRIMARY KEY (id)")
	res2, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, IdleAfter: 900 * time.Millisecond})
	if err != nil {
		t.Fatalf("补上主键后重跑应成功: %v", err)
	}
	if res2.RowsCopied < 1 {
		t.Fatalf("暂停表必须整表重搬（RowsCopied=%d）", res2.RowsCopied)
	}
	if paused := nopkPausedPG(t, dst, store, jobID); len(paused) != 0 {
		t.Fatalf("重搬搬成 → 必须解除暂停，仍有：%v", paused)
	}
	var v int64
	if err := dst.QueryRow(context.Background(), "SELECT v FROM "+ta+" WHERE id=42").Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != 1042 {
		t.Fatalf("重搬后应看到源端新值 v=1042（暂停窗口里的改动只能靠整表重搬补上），实际 %d", v)
	}
}

// ── 场景：PG → MySQL（只全量）────────────────────────────────

func nopkPGToMySQLSnapshot(t *testing.T, srcDSN, dstDSN string) {
	src, dst := mustConn(t, srcDSN), mustMySQLTarget(t, dstDSN)
	const tbl, rows = "nopk_pm_snap", 50
	nopkSeedPGNoPK(t, src, tbl, rows)
	_, _ = dst.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+tbl+"`")
	dropMySQLState(t, dst)
	t.Cleanup(func() { _, _ = dst.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+tbl+"`") })

	store, jobID := nopkCreateJob(t, nopkJobSpec{
		srcDSN: srcDSN, dstDSN: dstDSN, srcKind: dsn.KindPostgres, dstKind: dsn.KindMySQL,
		name: "nopk-pm-snap", tables: []string{tbl}, freq: "continuous+initial",
	})
	res, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, SnapshotOnly: true})
	if err != nil {
		t.Fatalf("PG 源无主键表 → MySQL 目标端全量失败（应 insert-only 照跑）: %v", err)
	}
	if res.ExitCode != 0 {
		t.Fatalf("全量应退 0，实际 %d", res.ExitCode)
	}
	if got := myCount(t, dst, tbl); got != rows {
		t.Fatalf("目标端应有 %d 行，实际 %d", rows, got)
	}
}

func nopkPGToMySQLUniqueKeyOnce(t *testing.T, srcDSN, dstDSN string) {
	src, dst := mustConn(t, srcDSN), mustMySQLTarget(t, dstDSN)
	const tbl = "nopk_pm_uk"
	exec(t, src, "DROP TABLE IF EXISTS "+tbl)
	exec(t, src, "CREATE TABLE "+tbl+" (tenant_id int NOT NULL, event_no text NOT NULL, payload text)")
	exec(t, src, "INSERT INTO "+tbl+" SELECT 1, 'e'||g, 'row-'||g FROM generate_series(1,10) g")
	exec(t, src, "ALTER TABLE "+tbl+" REPLICA IDENTITY FULL") // 无主键表的改/删要整行旧镜像（R5）
	t.Cleanup(func() { exec(t, src, "DROP TABLE IF EXISTS "+tbl) })

	_, _ = dst.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+tbl+"`")
	dropMySQLState(t, dst)
	t.Cleanup(func() { _, _ = dst.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+tbl+"`") })

	store, jobID := nopkCreateJob(t, nopkJobSpec{
		srcDSN: srcDSN, dstDSN: dstDSN, srcKind: dsn.KindPostgres, dstKind: dsn.KindMySQL,
		name: "nopk-pm-uk", tables: []string{tbl}, freq: "once",
		uniqueKeys: map[string][]string{tbl: {"tenant_id", "event_no"}},
	})
	// 同 PG→PG 那一格：once 每轮整轮重建，声明不进写路径；验的是「重放不翻倍 + 跟上源端」。
	if _, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, SnapshotOnly: true}); err != nil {
		t.Fatalf("声明 unique_key 的 PG→MySQL once 全量失败: %v", err)
	}
	if got := myCount(t, dst, tbl); got != 10 {
		t.Fatalf("首轮应有 10 行，实际 %d", got)
	}
	exec(t, src, "UPDATE "+tbl+" SET payload='changed' WHERE tenant_id=1 AND event_no='e3'")
	exec(t, src, "DELETE FROM "+tbl+" WHERE tenant_id=1 AND event_no='e5'")
	if _, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID, SnapshotOnly: true}); err != nil {
		t.Fatalf("once 重放失败: %v", err)
	}
	if got := myCount(t, dst, tbl); got != 9 {
		t.Fatalf("once 重放必须等于源端当前状态（9 行），实际 %d", got)
	}
	var payload string
	if err := dst.QueryRowContext(context.Background(),
		"SELECT payload FROM `"+tbl+"` WHERE tenant_id=1 AND event_no='e3'").Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if payload != "changed" {
		t.Fatalf("重放后应看到源端新值，实际 %q", payload)
	}
}
