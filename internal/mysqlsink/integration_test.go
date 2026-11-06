//go:build integration

// 集成测试：需要真 MySQL 目标库。
// 缺 DSN 直接失败（不 skip）：静默跳过会让「矩阵全绿」变成假象。
package mysqlsink

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/catalog"
	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/source"
)

func openSink(t *testing.T, mutate func(*Options)) *Sink {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("JINLIANG_TEST_MYSQL_DSN"))
	if raw == "" {
		t.Fatal("缺少 JINLIANG_TEST_MYSQL_DSN：集成测试必须跑在真库上，没有 DSN 就是失败，不是跳过")
	}
	ep, err := dsn.Parse(raw)
	if err != nil {
		t.Fatalf("解析 DSN: %v", err)
	}
	opt := Options{
		DSN:        ep,
		JobKey:     "0123456789",
		Name:       "it",
		InstanceID: "it0000000000000000000000000",
		SourceKind: catalog.MySQL,
		OnExists:   OnExistsError,
	}
	if mutate != nil {
		mutate(&opt)
	}
	s, err := Open(context.Background(), opt)
	if err != nil {
		t.Fatalf("连接目标库: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func dropTable(t *testing.T, s *Sink, name string) {
	t.Helper()
	_, _ = s.db.ExecContext(context.Background(), "DROP TABLE IF EXISTS `"+name+"`")
}

func countRows(t *testing.T, s *Sink, table string) int {
	t.Helper()
	var n int
	if err := s.db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM `"+table+"`").Scan(&n); err != nil {
		t.Fatalf("数 %s 行数失败: %v", table, err)
	}
	return n
}

func ordersCatalog() source.Catalog {
	return source.Catalog{Tables: []source.Table{{
		TableRef: source.TableRef{Schema: "", Table: "jl_b3_orders"},
		Columns: []source.Column{
			{Name: "id", Type: "int", Nullable: false},
			{Name: "note", Type: "varchar(64)", Nullable: true},
		},
		PrimaryKey:  []string{"id"},
		HasStablePK: true,
	}}}
}

func posOf(t *testing.T, s string) source.Position {
	t.Helper()
	p, err := ParsePositionForTest(s)
	if err != nil {
		t.Fatalf("位点: %v", err)
	}
	return p
}

// 有唯一键：重放不产生重复行，更新会覆盖。
func TestIntegrationUpsertWithUniqueKey(t *testing.T) {
	s := openSink(t, nil)
	dropTable(t, s, "jl_b3_orders")
	if _, err := s.EnsureSchema(context.Background(), ordersCatalog()); err != nil {
		t.Fatalf("建表: %v", err)
	}
	defer dropTable(t, s, "jl_b3_orders")

	batch := insertBatch("jl_b3_orders", [][]any{{int64(1), "one"}, {int64(2), "two"}},
		"mysql:v1:file=binlog.000001;pos=100;coord=mysql_file_pos")
	if err := s.Apply(context.Background(), batch, posOf(t, "mysql:v1:file=binlog.000001;pos=100;coord=mysql_file_pos")); err != nil {
		t.Fatalf("写入: %v", err)
	}
	if n := countRows(t, s, "jl_b3_orders"); n != 2 {
		t.Fatalf("应写入 2 行，实际 %d", n)
	}

	// 重放同一批：ON DUPLICATE KEY UPDATE 必须吸收，不能变 4 行。
	replay := insertBatch("jl_b3_orders", [][]any{{int64(1), "one"}, {int64(2), "two"}},
		"mysql:v1:file=binlog.000001;pos=100;coord=mysql_file_pos")
	if err := s.Apply(context.Background(), replay, posOf(t, "mysql:v1:file=binlog.000001;pos=100;coord=mysql_file_pos")); err != nil {
		t.Fatalf("重放: %v", err)
	}
	if n := countRows(t, s, "jl_b3_orders"); n != 2 {
		t.Fatalf("重放后仍是 2 行，实际 %d（说明 upsert 退化了）", n)
	}

	// 更新同一主键：值要变，行数不变。
	upd := updateBatch("jl_b3_orders", [][]any{{int64(1), "changed"}}, [][]any{{int64(1), "one"}},
		"mysql:v1:file=binlog.000001;pos=200;coord=mysql_file_pos")
	if err := s.Apply(context.Background(), upd, posOf(t, "mysql:v1:file=binlog.000001;pos=200;coord=mysql_file_pos")); err != nil {
		t.Fatalf("更新: %v", err)
	}
	var note string
	if err := s.db.QueryRowContext(context.Background(), "SELECT note FROM `jl_b3_orders` WHERE id=1").Scan(&note); err != nil {
		t.Fatal(err)
	}
	if note != "changed" {
		t.Fatalf("更新没生效: note=%q", note)
	}

	// 位点与数据同事务：重放后应停在最后写入的那个位点。
	got, err := s.MetaPosition(context.Background())
	if err != nil {
		t.Fatalf("读位点: %v", err)
	}
	if !strings.Contains(got, "pos=200") {
		t.Fatalf("位点没落库或落错: %q", got)
	}
}

// 无唯一键：明确报错，且一行都不写（事务回滚）。
func TestIntegrationUpsertWithoutUniqueKeyFails(t *testing.T) {
	s := openSink(t, func(o *Options) { o.OnExists = OnExistsAppend })
	dropTable(t, s, "jl_b3_nopk")
	defer dropTable(t, s, "jl_b3_nopk")
	if _, err := s.db.ExecContext(context.Background(), "CREATE TABLE `jl_b3_nopk` (a int, b varchar(16))"); err != nil {
		t.Fatalf("建无主键表: %v", err)
	}

	cat := source.Catalog{Tables: []source.Table{{
		TableRef: source.TableRef{Table: "jl_b3_nopk"},
		Columns:  []source.Column{{Name: "a", Type: "int", Nullable: true}, {Name: "b", Type: "varchar(16)", Nullable: true}},
	}}}
	if _, err := s.EnsureSchema(context.Background(), cat); err != nil {
		t.Fatalf("append 策略应沿用已有表: %v", err)
	}

	batch := insertBatch("jl_b3_nopk", [][]any{{int64(1), "x"}},
		"mysql:v1:file=binlog.000001;pos=300;coord=mysql_file_pos")
	err := s.Apply(context.Background(), batch, posOf(t, "mysql:v1:file=binlog.000001;pos=300;coord=mysql_file_pos"))
	if err == nil {
		t.Fatal("没有唯一键必须报错，不能退化成纯 INSERT")
	}
	if !errors.Is(err, ErrNoUniqueKey) {
		t.Fatalf("错误应可判为 ErrNoUniqueKey: %v", err)
	}
	if n := countRows(t, s, "jl_b3_nopk"); n != 0 {
		t.Fatalf("报错时不能写进任何行，实际 %d 行", n)
	}
	if p, _ := s.MetaPosition(context.Background()); strings.Contains(p, "pos=300") {
		t.Fatalf("报错时位点不能前进: %q", p)
	}
}

// on_exists 三策略。
func TestIntegrationOnExistsStrategies(t *testing.T) {
	ctx := context.Background()

	// error：撞表就停
	s := openSink(t, nil)
	dropTable(t, s, "jl_b3_orders")
	defer dropTable(t, s, "jl_b3_orders")
	if _, err := s.EnsureSchema(ctx, ordersCatalog()); err != nil {
		t.Fatalf("首次建表: %v", err)
	}
	if _, err := s.EnsureSchema(ctx, ordersCatalog()); err == nil {
		t.Fatal("on_exists=error 撞到已有表必须报错")
	} else if !errors.Is(err, ErrTargetTableExists) {
		t.Fatalf("错误应可判为 ErrTargetTableExists: %v", err)
	}

	// recreate：清空重建
	if _, err := s.db.ExecContext(ctx, "INSERT INTO `jl_b3_orders` (id, note) VALUES (9,'old')"); err != nil {
		t.Fatal(err)
	}
	rs := openSink(t, func(o *Options) { o.OnExists = OnExistsRecreate })
	if _, err := rs.EnsureSchema(ctx, ordersCatalog()); err != nil {
		t.Fatalf("recreate: %v", err)
	}
	if n := countRows(t, rs, "jl_b3_orders"); n != 0 {
		t.Fatalf("recreate 应清空目标表，仍有 %d 行", n)
	}

	// append：不动已有行（源端删过的行会残留——这是语义选择，断言它确实残留）
	if _, err := rs.db.ExecContext(ctx, "INSERT INTO `jl_b3_orders` (id, note) VALUES (7,'user-row')"); err != nil {
		t.Fatal(err)
	}
	ap := openSink(t, func(o *Options) { o.OnExists = OnExistsAppend })
	acts, err := ap.EnsureSchema(ctx, ordersCatalog())
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if n := countRows(t, ap, "jl_b3_orders"); n != 1 {
		t.Fatalf("append 不清表：应保留用户那 1 行，实际 %d", n)
	}
	// 同步只写了 id=1；源端已删的 id=7 仍留在目标端。
	batch := insertBatch("jl_b3_orders", [][]any{{int64(1), "one"}},
		"mysql:v1:file=binlog.000001;pos=400;coord=mysql_file_pos")
	if err := ap.Apply(ctx, batch, posOf(t, "mysql:v1:file=binlog.000001;pos=400;coord=mysql_file_pos")); err != nil {
		t.Fatalf("append 写入: %v", err)
	}
	var residual int
	if err := ap.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM `jl_b3_orders` WHERE id=7").Scan(&residual); err != nil {
		t.Fatal(err)
	}
	if residual != 1 {
		t.Fatalf("append 语义：源端删过的行会在目标端残留（这里应看到 1 行），实际 %d", residual)
	}
	if len(acts) == 0 || !strings.Contains(acts[0].Reason, "残留") {
		t.Fatalf("append 的动作说明必须写清「源端删过的行会残留」: %#v", acts)
	}
}

// DDL 白名单：不在白名单 → 停，且整批不落库。
func TestIntegrationDDLOutsideWhitelistStops(t *testing.T) {
	ctx := context.Background()
	s := openSink(t, nil)
	dropTable(t, s, "jl_b3_orders")
	defer dropTable(t, s, "jl_b3_orders")
	if _, err := s.EnsureSchema(ctx, ordersCatalog()); err != nil {
		t.Fatalf("建表: %v", err)
	}

	batch := []source.Event{
		{Kind: source.EvBegin, TxID: "t1"},
		{Kind: source.EvInsert, Table: source.TableRef{Table: "jl_b3_orders"}, TxID: "t1",
			New:      source.Row{Table: source.TableRef{Table: "jl_b3_orders"}, Values: []source.Value{{Kind: source.KindInt, I: 1}, {Kind: source.KindString, S: "x"}}},
			Relation: relationPtr("jl_b3_orders")},
		{Kind: source.EvDDL, TxID: "t2", DDL: "DROP TABLE `jl_b3_orders`"},
		{Kind: source.EvCommit, TxID: "t2"},
	}
	err := s.Apply(ctx, batch, posOf(t, "mysql:v1:file=binlog.000001;pos=500;coord=mysql_file_pos"))
	if err == nil {
		t.Fatal("DROP TABLE 不在白名单，必须停下")
	}
	if !strings.Contains(err.Error(), "白名单") {
		t.Fatalf("错误要说清白名单: %v", err)
	}
	// 语义（Docs + Owl 裁决）：DDL 会隐式提交，所以「数据先落、位点后落」——
	// 数据可以已经写进去（重放靠 upsert 幂等），但位点绝不能前进，且 DDL 不能执行。
	var tableStillThere int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME='jl_b3_orders'`).Scan(&tableStillThere); err != nil {
		t.Fatal(err)
	}
	if tableStillThere != 1 {
		t.Fatal("被拒绝的 DROP TABLE 绝不能执行")
	}
	if p, _ := s.MetaPosition(ctx); strings.Contains(p, "pos=500") {
		t.Fatalf("DDL 被拒时位点不能前进: %q", p)
	}
}

// 加列白名单：跟随执行。
func TestIntegrationDDLAddColumnFollows(t *testing.T) {
	ctx := context.Background()
	s := openSink(t, nil)
	dropTable(t, s, "jl_b3_orders")
	defer dropTable(t, s, "jl_b3_orders")
	if _, err := s.EnsureSchema(ctx, ordersCatalog()); err != nil {
		t.Fatalf("建表: %v", err)
	}
	batch := []source.Event{
		{Kind: source.EvBegin, TxID: "t1"},
		{Kind: source.EvDDL, TxID: "t1", DDL: "ALTER TABLE `jl_b3_orders` ADD COLUMN extra VARCHAR(8) NULL"},
		{Kind: source.EvCommit, TxID: "t1"},
	}
	if err := s.Apply(ctx, batch, posOf(t, "mysql:v1:file=binlog.000001;pos=600;coord=mysql_file_pos")); err != nil {
		t.Fatalf("加列应跟随: %v", err)
	}
	var n int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME='jl_b3_orders' AND COLUMN_NAME='extra'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatal("加列没生效")
	}
}

// utf8mb4 行宽预检：真造一张会超限的表，断言警告里给出字节数与改法。
func TestIntegrationWidthPreflightOnRealTable(t *testing.T) {
	ctx := context.Background()
	s := openSink(t, nil)
	dropTable(t, s, "jl_b3_wide")
	defer dropTable(t, s, "jl_b3_wide")
	if _, err := s.db.ExecContext(ctx, "CREATE TABLE `jl_b3_wide` (id int primary key, big varchar(1000)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"); err != nil {
		t.Fatalf("建宽表: %v", err)
	}
	cat := source.Catalog{Tables: []source.Table{{
		TableRef: source.TableRef{Table: "jl_b3_wide"},
		Columns:  []source.Column{{Name: "id", Type: "int"}, {Name: "big", Type: "varchar(1000)"}},
	}}}
	rep, err := s.Preflight(ctx, cat)
	if err != nil {
		t.Fatalf("预检: %v", err)
	}
	if rep.CanStart != true {
		t.Fatal("行宽警告只提示，不阻断")
	}
	var found *source.Check
	for i := range rep.Checks {
		if strings.Contains(rep.Checks[i].Detail, "big") {
			found = &rep.Checks[i]
		}
	}
	if found == nil {
		t.Fatalf("varchar(1000) 在 utf8mb4 下应给出警告: %#v", rep.Checks)
	}
	for _, want := range []string{"4002", "3072", "text"} {
		if !strings.Contains(found.Detail, want) {
			t.Fatalf("警告里要有估算字节(4002)/上限(3072)/改法(text): %s", found.Detail)
		}
	}

	// 索引层：真实索引前缀没超限时不该误报（避免假警告）。
	issues, err := s.CheckIndexWidths(ctx, s.ep.Database)
	if err != nil {
		t.Fatalf("查索引: %v", err)
	}
	for _, i := range issues {
		if i.Table == "jl_b3_wide" {
			t.Fatalf("这张表的索引不该报警: %s", i.String())
		}
	}
}

// —— 事件构造 ——

func insertBatch(table string, rows [][]any, pos string) []source.Event {
	rel := relationFor(table)
	var evs []source.Event
	evs = append(evs, source.Event{Kind: source.EvBegin, TxID: "tx"})
	for _, r := range rows {
		evs = append(evs, source.Event{
			Kind: source.EvInsert, TxID: "tx", Table: source.TableRef{Table: table},
			New:      source.Row{Table: source.TableRef{Table: table}, Values: valuesOf(r)},
			Relation: rel,
		})
	}
	evs = append(evs, source.Event{Kind: source.EvCommit, TxID: "tx"})
	return evs
}

func updateBatch(table string, news, olds [][]any, pos string) []source.Event {
	rel := relationFor(table)
	var evs []source.Event
	evs = append(evs, source.Event{Kind: source.EvBegin, TxID: "tx"})
	for i := range news {
		evs = append(evs, source.Event{
			Kind: source.EvUpdate, TxID: "tx", Table: source.TableRef{Table: table},
			New:      source.Row{Table: source.TableRef{Table: table}, Values: valuesOf(news[i])},
			Old:      source.Row{Table: source.TableRef{Table: table}, Values: valuesOf(olds[i])},
			Relation: rel,
		})
	}
	evs = append(evs, source.Event{Kind: source.EvCommit, TxID: "tx"})
	return evs
}

func valuesOf(row []any) []source.Value {
	out := make([]source.Value, len(row))
	for i, v := range row {
		switch x := v.(type) {
		case int64:
			out[i] = source.Value{Kind: source.KindInt, I: x}
		case string:
			out[i] = source.Value{Kind: source.KindString, S: x}
		case nil:
			out[i] = source.Value{Kind: source.KindNull}
		}
	}
	return out
}

func relationFor(table string) *source.Table { return relationPtr(table) }

func relationPtr(table string) *source.Table {
	t := ordersCatalog().Tables[0]
	t.Table = table
	return &t
}

// ParsePositionForTest 用 mysqlsource 的位点格式（同一格式，两个包共用契约）。
func ParsePositionForTest(s string) (source.Position, error) {
	return testPosition{raw: s}, nil
}

type testPosition struct{ raw string }

func (p testPosition) String() string { return p.raw }
func (p testPosition) Kind() string   { return "mysql" }

var _ = time.Now

// 同一批 DDL 应用两次必须幂等（这是「DDL 隐式提交」那条必改的验收标准）。
// 旧实现里 DDL 塞在数据事务中：崩在「DDL 之后、位点写入之前」后重放 → 1061 Duplicate key name → 永久卡死。
//
// 注意强度：这里是**同一进程连着调两次 Apply**，不是 kill -9 后的新进程重放。
// 幂等判据全部落在库里（information_schema 的列/索引/类型），新进程只多一个空的
// uniqueCache，因此真实崩溃重放只会比这条更保守；但**真正的 kill -9 重放验证归
// B2 的崩溃恢复矩阵**（`feat/b2-*` 的真库用例），别把这条当成已经验过崩溃重放。
func TestIntegrationDDLReplayIsIdempotent(t *testing.T) {
	ctx := context.Background()
	s := openSink(t, nil)
	dropTable(t, s, "jl_b3_orders")
	defer dropTable(t, s, "jl_b3_orders")
	if _, err := s.EnsureSchema(ctx, ordersCatalog()); err != nil {
		t.Fatalf("建表: %v", err)
	}

	batch := []source.Event{
		{Kind: source.EvBegin, TxID: "t1"},
		{Kind: source.EvInsert, TxID: "t1", Table: source.TableRef{Table: "jl_b3_orders"},
			New:      source.Row{Table: source.TableRef{Table: "jl_b3_orders"}, Values: []source.Value{{Kind: source.KindInt, I: 1}, {Kind: source.KindString, S: "x"}}},
			Relation: relationPtr("jl_b3_orders")},
		{Kind: source.EvDDL, TxID: "t2", DDL: "CREATE INDEX jl_b3_idx ON `jl_b3_orders` (`note`)"},
		{Kind: source.EvCommit, TxID: "t2"},
	}
	for round := 1; round <= 2; round++ {
		if err := s.Apply(ctx, batch, posOf(t, "mysql:v1:file=binlog.000001;pos=700;coord=mysql_file_pos")); err != nil {
			t.Fatalf("第 %d 次应用失败（应幂等跳过已存在的索引）: %v", round, err)
		}
	}
	if n := countRows(t, s, "jl_b3_orders"); n != 1 {
		t.Fatalf("重放两次后仍应 1 行，实际 %d", n)
	}
	var idx int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT INDEX_NAME) FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME='jl_b3_orders' AND INDEX_NAME='jl_b3_idx'`).Scan(&idx); err != nil {
		t.Fatal(err)
	}
	if idx != 1 {
		t.Fatalf("索引应只有一份，实际 %d", idx)
	}

	// ADD COLUMN 同理（重复执行会 1060 Duplicate column name）。
	add := []source.Event{
		{Kind: source.EvBegin, TxID: "t3"},
		{Kind: source.EvDDL, TxID: "t3", DDL: "ALTER TABLE `jl_b3_orders` ADD COLUMN extra2 VARCHAR(8) NULL"},
		{Kind: source.EvCommit, TxID: "t3"},
	}
	for round := 1; round <= 2; round++ {
		if err := s.Apply(ctx, add, posOf(t, "mysql:v1:file=binlog.000001;pos=800;coord=mysql_file_pos")); err != nil {
			t.Fatalf("第 %d 次加列失败（应幂等跳过）: %v", round, err)
		}
	}
	var cols int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME='jl_b3_orders' AND COLUMN_NAME='extra2'`).Scan(&cols); err != nil {
		t.Fatal(err)
	}
	if cols != 1 {
		t.Fatalf("列应只有一份，实际 %d", cols)
	}
}

// 数据先落、位点后落：DDL 被白名单拒绝时，数据已提交但位点不动（重放靠 upsert 幂等）。
//
// 顺手记一个边界（未做，先留痕）：`TRUNCATE` 走的是「独立执行、不做幂等判定」那条路——
// 重复执行 TRUNCATE 本身无害，但如果 `TRUNCATE` 与它之后的数据被切到**不同批次**，
// 批次之间崩就会看到「截断了、新数据没到」的中间态。要收这个口子的话，做法是在
// `TRUNCATE` 前后各写一次位点（或把 TRUNCATE 与同表后续数据强制放在同一批），
// 现在按「先留痕、不扩范围」处理。
func TestIntegrationDataCommitsBeforeRejectedDDL(t *testing.T) {
	ctx := context.Background()
	s := openSink(t, nil)
	dropTable(t, s, "jl_b3_orders")
	defer dropTable(t, s, "jl_b3_orders")
	if _, err := s.EnsureSchema(ctx, ordersCatalog()); err != nil {
		t.Fatalf("建表: %v", err)
	}
	batch := []source.Event{
		{Kind: source.EvBegin, TxID: "t1"},
		{Kind: source.EvInsert, TxID: "t1", Table: source.TableRef{Table: "jl_b3_orders"},
			New:      source.Row{Table: source.TableRef{Table: "jl_b3_orders"}, Values: []source.Value{{Kind: source.KindInt, I: 3}, {Kind: source.KindString, S: "keep"}}},
			Relation: relationPtr("jl_b3_orders")},
		{Kind: source.EvCommit, TxID: "t1"},
		{Kind: source.EvDDL, TxID: "t2", DDL: "DROP TABLE `jl_b3_orders`"},
		{Kind: source.EvCommit, TxID: "t2"},
	}
	err := s.Apply(ctx, batch, posOf(t, "mysql:v1:file=binlog.000001;pos=900;coord=mysql_file_pos"))
	if err == nil {
		t.Fatal("DROP TABLE 不在白名单，必须停")
	}
	if n := countRows(t, s, "jl_b3_orders"); n != 1 {
		t.Fatalf("数据应先落库（1 行），实际 %d 行", n)
	}
	if p, _ := s.MetaPosition(ctx); strings.Contains(p, "pos=900") {
		t.Fatalf("DDL 失败时位点不能前进（否则重放会跳过这笔数据）: %q", p)
	}
}
