//go:build integration

// 集成测试：需要真 MySQL 目标库。
// 缺 DSN 直接失败（不 skip）：静默跳过会让「矩阵全绿」变成假象。
package mysqlsink

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/catalog"
	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/mysqlsource"
	"github.com/spencerswagger/jinliang/internal/pgsink"
	"github.com/spencerswagger/jinliang/internal/source"
)

func openSourceForTest(t *testing.T) (*mysqlsource.Source, error) {
	t.Helper()
	// 用**同一个库**：这个用例只验「同一份数据两侧指纹相等」，跨库比对会变成
	// 「表不存在」而不是算法问题。
	raw := strings.TrimSpace(os.Getenv("JINLIANG_TEST_MYSQL_DSN"))
	if raw == "" {
		return nil, fmt.Errorf("缺少 JINLIANG_TEST_MYSQL_DSN")
	}
	return mysqlsource.Open(context.Background(), raw)
}

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
// 这是「源端有主键、目标端缺键」那一半——源端身份明确，目标端却定位不了（设计 §4.3）。
// 源端本来就无主键的表不走这条，见 TestIntegrationInsertOnlyNoPKSource。
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
		t.Fatal("源端有主键、目标端没有唯一键时必须报错，不能退化成纯 INSERT")
	}
	if !errors.Is(err, ErrNoUniqueKey) {
		t.Fatalf("错误应可判为 ErrNoUniqueKey: %v", err)
	}
	// 这条路径是「源端有主键、目标端缺键」：此时声明 unique_key 不生效（§4.2 第 4 条），
	// 所以不能把它列为做法——用户照做也不会好转（Ark §7 D11，与 TARGET-02 的 warn 口径对不上）。
	if strings.Contains(err.Error(), "unique_key") {
		t.Fatalf("源端有主键时声明会被忽略，不该让用户去声明 unique_key：%v", err)
	}
	// 「换空 schema」单独一条是空动作：这条用例的目标库本来就是空的（schemaOf 回落 DSN 库）。
	// 能真救回来的前提是「那里还没有这张表」，或者配 on_exists: recreate 先 DROP 重建。
	if !strings.Contains(err.Error(), "target.schema") || !strings.Contains(err.Error(), "on_exists") {
		t.Fatalf("「换到空 schema + （已有表时）on_exists: recreate」才是可走的路，应写清前提：%v", err)
	}
	if n := countRows(t, s, "jl_b3_nopk"); n != 0 {
		t.Fatalf("报错时不能写进任何行，实际 %d 行", n)
	}
	if p, _ := s.MetaPosition(context.Background()); strings.Contains(p, "pos=300") {
		t.Fatalf("报错时位点不能前进: %q", p)
	}
}

// 源端无稳定主键 + 目标端无唯一索引：insert-only 照跑（设计 §1.2 路线 A）。
// 全量也走 Apply，所以这条同时是「无主键表全量能搬完」在写路径上的那一半。
func TestIntegrationInsertOnlyNoPKSource(t *testing.T) {
	s := openSink(t, func(o *Options) { o.OnExists = OnExistsAppend })
	dropTable(t, s, "jl_b4_nopk")
	defer dropTable(t, s, "jl_b4_nopk")
	if _, err := s.db.ExecContext(context.Background(), "CREATE TABLE `jl_b4_nopk` (a int, b varchar(16))"); err != nil {
		t.Fatalf("建无主键表: %v", err)
	}

	cat := source.Catalog{Tables: []source.Table{{
		TableRef: source.TableRef{Table: "jl_b4_nopk"},
		Columns:  []source.Column{{Name: "a", Type: "int", Nullable: true}, {Name: "b", Type: "varchar(16)", Nullable: true}},
	}}}
	if _, err := s.EnsureSchema(context.Background(), cat); err != nil {
		t.Fatalf("append 策略应沿用已有表: %v", err)
	}

	rel := relationNoPK("jl_b4_nopk")
	batch := insertBatchWithRel("jl_b4_nopk", rel, [][]any{{int64(1), "x"}, {int64(2), "y"}}, "")
	if err := s.Apply(context.Background(), batch, posOf(t, "mysql:v1:file=binlog.000001;pos=400;coord=mysql_file_pos")); err != nil {
		t.Fatalf("无主键表 insert-only 不应失败: %v", err)
	}
	if n := countRows(t, s, "jl_b4_nopk"); n != 2 {
		t.Fatalf("insert-only 应写进 2 行，实际 %d 行", n)
	}
	if p, _ := s.MetaPosition(context.Background()); !strings.Contains(p, "pos=400") {
		t.Fatalf("insert-only 成功后位点应前进: %q", p)
	}
}

// 源端无稳定主键时收到 UPDATE：必须明确失败——不许降级成 INSERT（设计 §1.5 已否），
// 也不许按目标端唯一索引猜身份（R1）。按表暂停是下一个切片，那之前这里整批失败。
func TestIntegrationInsertOnlyRejectsUpdateOnNoPKSource(t *testing.T) {
	s := openSink(t, func(o *Options) { o.OnExists = OnExistsAppend })
	dropTable(t, s, "jl_b4_nopk")
	defer dropTable(t, s, "jl_b4_nopk")
	if _, err := s.db.ExecContext(context.Background(), "CREATE TABLE `jl_b4_nopk` (a int, b varchar(16))"); err != nil {
		t.Fatalf("建无主键表: %v", err)
	}

	cat := source.Catalog{Tables: []source.Table{{
		TableRef: source.TableRef{Table: "jl_b4_nopk"},
		Columns:  []source.Column{{Name: "a", Type: "int", Nullable: true}, {Name: "b", Type: "varchar(16)", Nullable: true}},
	}}}
	if _, err := s.EnsureSchema(context.Background(), cat); err != nil {
		t.Fatalf("append 策略应沿用已有表: %v", err)
	}

	rel := relationNoPK("jl_b4_nopk")
	upd := updateBatchWithRel("jl_b4_nopk", rel, [][]any{{int64(1), "y"}}, [][]any{{int64(1), "x"}}, "")
	err := s.Apply(context.Background(), upd, posOf(t, "mysql:v1:file=binlog.000001;pos=500;coord=mysql_file_pos"))
	if err == nil {
		t.Fatal("无主键表收到 UPDATE 必须失败，不能降级成 INSERT")
	}
	if errors.Is(err, ErrNoUniqueKey) {
		t.Fatalf("这条不是 upsert 缺唯一键，不该报 ErrNoUniqueKey: %v", err)
	}
	if !strings.Contains(err.Error(), "没有主键") {
		t.Fatalf("错误应说清是无主键、无法定位改行: %v", err)
	}
	// 按表暂停（no_pk_write / E1017）：这张表停、其它表照写，不是整条任务死。
	var pe pgsink.PausableError
	if !errors.As(err, &pe) || pe.Reason() != pgsink.PauseReasonNoPKWrite {
		t.Fatalf("应是 PausableError(no_pk_write)，得到 %T：%v", err, err)
	}
	if n := countRows(t, s, "jl_b4_nopk"); n != 0 {
		t.Fatalf("失败时不能写进任何行，实际 %d 行", n)
	}
	if p, _ := s.MetaPosition(context.Background()); strings.Contains(p, "pos=500") {
		t.Fatalf("失败时位点不能前进: %q", p)
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
	return insertBatchWithRel(table, relationFor(table), rows, pos)
}

func insertBatchWithRel(table string, rel *source.Table, rows [][]any, pos string) []source.Event {
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
	return updateBatchWithRel(table, relationFor(table), news, olds, pos)
}

func updateBatchWithRel(table string, rel *source.Table, news, olds [][]any, pos string) []source.Event {
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

// relationNoPK 是「源端无稳定主键」的 Relation：列按 jl_b4_nopk 的 (a,b)，身份为空
// （设计 §1.1 的探测口径：HasStablePK=false 且 PrimaryKey 为空）。
func relationNoPK(table string) *source.Table {
	return &source.Table{
		TableRef: source.TableRef{Table: table},
		Columns: []source.Column{
			{Name: "a", Type: "int", Nullable: true},
			{Name: "b", Type: "varchar(16)", Nullable: true},
		},
		HasStablePK: false,
	}
}

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

// 租约：拿到 → 别人抢不走 → 释放 → 别人能拿；过期可抢占。
func TestIntegrationLeaseLifecycle(t *testing.T) {
	ctx := context.Background()
	a := openSink(t, nil)
	_, _ = a.db.ExecContext(ctx, "DELETE FROM `_jl_lease` WHERE job_key = ?", a.opt.JobKey)
	t.Cleanup(func() {
		_, _ = a.db.ExecContext(context.Background(), "DELETE FROM `_jl_lease` WHERE job_key = ?", a.opt.JobKey)
	})

	ok, why, err := a.AcquireLease(ctx, 30*time.Second)
	if err != nil || !ok {
		t.Fatalf("第一次应拿到租约: ok=%v why=%q err=%v", ok, why, err)
	}
	// 续租：自己续自己必须成功。
	if ok, err := a.RenewLease(ctx, 30*time.Second); err != nil || !ok {
		t.Fatalf("自己续租应成功: ok=%v err=%v", ok, err)
	}
	// 另一个实例：周期内必须被拒，且说清是谁在跑。
	b := openSink(t, func(o *Options) { o.InstanceID = "another-instance" })
	ok, why, err = b.AcquireLease(ctx, 30*time.Second)
	if err != nil {
		t.Fatalf("第二个实例: %v", err)
	}
	if ok {
		t.Fatal("同一 job 的租约期内，第二个实例不该拿到")
	}
	if !strings.Contains(why, a.opt.InstanceID) {
		t.Fatalf("拒绝原因要说清谁在跑: %q", why)
	}
	// 释放后别人能拿。
	if err := a.ReleaseLease(ctx); err != nil {
		t.Fatalf("释放: %v", err)
	}
	if ok, _, err = b.AcquireLease(ctx, 30*time.Second); err != nil || !ok {
		t.Fatalf("释放后应能拿到: ok=%v err=%v", ok, err)
	}

	// 过期可抢占：把到期时间改到过去。
	if _, err := b.db.ExecContext(ctx, "UPDATE `_jl_lease` SET expires_at = ? WHERE job_key = ?",
		time.Now().Add(-time.Minute), b.opt.JobKey); err != nil {
		t.Fatal(err)
	}
	if ok, why, err = a.AcquireLease(ctx, 30*time.Second); err != nil || !ok {
		t.Fatalf("过期租约应可抢占: ok=%v why=%q err=%v", ok, why, err)
	}
	if !strings.Contains(why, "过期") {
		t.Fatalf("抢占原因应说明是过期: %q", why)
	}
}

// 同机崩溃进程留下的租约可以立刻接管（不用等 30s），活着的进程则不行。
func TestIntegrationLeaseTakesOverDeadPID(t *testing.T) {
	ctx := context.Background()
	s := openSink(t, nil)
	_, _ = s.db.ExecContext(ctx, "DELETE FROM `_jl_lease` WHERE job_key = ?", s.opt.JobKey)
	t.Cleanup(func() {
		_, _ = s.db.ExecContext(context.Background(), "DELETE FROM `_jl_lease` WHERE job_key = ?", s.opt.JobKey)
	})

	// 直接写一条「同实例、pid 999999（几乎肯定不存在）」的有效期内租约。
	deadPID := 999999
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO _jl_lease (job_key, instance_id, label, expires_at) VALUES (?,?,?,?)
		ON DUPLICATE KEY UPDATE instance_id=VALUES(instance_id), label=VALUES(label), expires_at=VALUES(expires_at)`,
		s.opt.JobKey, s.opt.InstanceID, leaseOwnerLabel(s.opt.Name, deadPID, "x"),
		time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("造租约: %v", err)
	}
	ok, why, err := s.AcquireLease(ctx, 30*time.Second)
	if err != nil || !ok {
		t.Fatalf("崩溃进程的租约应可接管: ok=%v why=%q err=%v", ok, why, err)
	}
	if !strings.Contains(why, "崩溃") {
		t.Fatalf("接管原因应说明是崩溃进程: %q", why)
	}
}

// 元数据/位点/阶段/对象登记/块断点：增量要用的那套状态必须真能读写。
func TestIntegrationStateTables(t *testing.T) {
	ctx := context.Background()
	// 用独立 job_key：_jl_meta 的 position 只由 Apply 推进，复用同一个 key 会读到
	// 前面用例写进去的位点（这正是「位点不被 EnsureMeta 覆盖」的语义）。
	s := openSink(t, func(o *Options) { o.JobKey = "itstate01" })
	job := s.opt.JobKey
	t.Cleanup(func() {
		_, _ = s.db.ExecContext(context.Background(), "DELETE FROM `_jl_meta` WHERE job_key = ?", job)
		_, _ = s.db.ExecContext(context.Background(), "DELETE FROM `_jl_object` WHERE job_key = ?", job)
		_, _ = s.db.ExecContext(context.Background(), "DELETE FROM `_jl_chunk` WHERE job_key = ?", job)
	})

	if err := s.EnsureMeta(ctx, "hash123", "streaming", "postgres:v1:0/16B6C50", ""); err != nil {
		t.Fatalf("EnsureMeta: %v", err)
	}
	phase, pos, err := s.State(ctx)
	if err != nil || phase != "streaming" || pos != "postgres:v1:0/16B6C50" {
		t.Fatalf("State = (%q,%q,%v)", phase, pos, err)
	}
	if err := s.SetPhase(ctx, "done"); err != nil {
		t.Fatalf("SetPhase: %v", err)
	}
	if phase, _, _ = s.State(ctx); phase != "done" {
		t.Fatalf("阶段没更新: %q", phase)
	}
	// 位点只由 Apply 推进；这里直接读回 EnsureMeta 写的值。
	if p, err := s.Position(ctx); err != nil || p != "postgres:v1:0/16B6C50" {
		t.Fatalf("Position = (%q,%v)", p, err)
	}

	// 对象登记：建表 → DidCreateTable 为真 → 软删 → 为假。
	ref := source.TableRef{Table: "jl_b3_orders"}
	if err := s.RecordObject(ctx, s.tableObjectName(ref), "table"); err != nil {
		t.Fatalf("RecordObject: %v", err)
	}
	if ok, err := s.DidCreateTable(ctx, ref); err != nil || !ok {
		t.Fatalf("登记后应判为本任务建的: ok=%v err=%v", ok, err)
	}
	if err := s.MarkObjectDropped(ctx, s.tableObjectName(ref)); err != nil {
		t.Fatalf("MarkObjectDropped: %v", err)
	}
	if ok, _ := s.DidCreateTable(ctx, ref); ok {
		t.Fatal("软删后不该再判为本任务建的（recopy 的 fail-closed 靠这条）")
	}

	// 块断点：写两行 → 读回 → 重置后为空。
	if _, err := s.db.ExecContext(ctx, "INSERT INTO `_jl_chunk` (job_key, table_name, chunk_no, done, `rows`) VALUES (?,?,?,?,?)",
		job, "jl_b3_orders", 1, true, 100); err != nil {
		t.Fatal(err)
	}
	chunks, err := s.DoneChunks(ctx)
	if err != nil || !chunks["jl_b3_orders"][1] {
		t.Fatalf("DoneChunks = %v,%v", chunks, err)
	}
	if err := s.ResetChunks(ctx); err != nil {
		t.Fatalf("ResetChunks: %v", err)
	}
	if chunks, _ := s.DoneChunks(ctx); len(chunks) != 0 {
		t.Fatalf("重置后应为空: %v", chunks)
	}
}

// ReadLease 是崩溃重启时唯一的可信来源：进程没了但 runs 行还是「在跑」。
func TestIntegrationReadLease(t *testing.T) {
	ctx := context.Background()
	s := openSink(t, nil)
	_, _ = s.db.ExecContext(ctx, "DELETE FROM `_jl_lease` WHERE job_key = ?", s.opt.JobKey)
	t.Cleanup(func() {
		_, _ = s.db.ExecContext(context.Background(), "DELETE FROM `_jl_lease` WHERE job_key = ?", s.opt.JobKey)
	})

	h, err := ReadLease(ctx, s.ep, s.opt.JobKey)
	if err != nil || h.Exists {
		t.Fatalf("没有租约时应 Exists=false: %+v err=%v", h, err)
	}
	if ok, _, err := s.AcquireLease(ctx, 30*time.Second); err != nil || !ok {
		t.Fatalf("拿租约: ok=%v err=%v", ok, err)
	}
	h, err = ReadLease(ctx, s.ep, s.opt.JobKey)
	if err != nil || !h.Exists || h.InstanceID != s.opt.InstanceID {
		t.Fatalf("ReadLease = %+v err=%v", h, err)
	}
}

// staged_phases 的 ack 前提：位点没落盘就不能 ack（否则源端会清掉还能重放的流水）。
// 这条覆盖「含 DDL 的批次有三个提交点」里最危险的那种顺序。
func TestIntegrationPositionDurableGatesAck(t *testing.T) {
	ctx := context.Background()
	s := openSink(t, func(o *Options) { o.JobKey = "itdur00001" })
	t.Cleanup(func() {
		_, _ = s.db.ExecContext(context.Background(), "DELETE FROM `_jl_meta` WHERE job_key = ?", s.opt.JobKey)
	})

	if s.PositionDurability() != "staged_phases" {
		t.Fatalf("MySQL 目标端必须是 staged_phases，拿到 %q", s.PositionDurability())
	}
	// 还没写过任何位点：不是 durable（behind：再等等；不是 different：不用停）。
	if st, err := s.PositionDurable(ctx, "mysql:v1:file=b.000001;pos=1;coord=mysql_file_pos"); err != nil || st != PositionBehind {
		t.Fatalf("没有位点时应 behind，拿到 %v err=%v", st, err)
	}

	// 造一张表并写入一批，位点随数据落盘 → 该位点 durable。
	cat := source.Catalog{Tables: []source.Table{{
		TableRef: source.TableRef{Table: "jl_b3_orders"},
		Columns: []source.Column{
			{Name: "id", Type: "int", Nullable: false},
			{Name: "note", Type: "varchar(64)", Nullable: true},
		},
		PrimaryKey:  []string{"id"},
		HasStablePK: true,
	}}}
	dropTable(t, s, "jl_b3_orders")
	if _, err := s.EnsureSchema(ctx, cat); err != nil {
		t.Fatalf("建表: %v", err)
	}
	defer dropTable(t, s, "jl_b3_orders")

	want := "mysql:v1:file=binlog.000009;pos=900;coord=mysql_file_pos"
	batch := insertBatch("jl_b3_orders", [][]any{{int64(1), "x"}}, want)
	if err := s.Apply(ctx, batch, posOf(t, want)); err != nil {
		t.Fatalf("写入: %v", err)
	}
	if st, err := s.PositionDurable(ctx, want); err != nil || st != PositionDurable {
		t.Fatalf("位点应已落盘: %v err=%v", st, err)
	}
	// 期望一个「不是库里的值」→ different（位点被改过/回退，必须停）。
	if st, _ := s.PositionDurable(ctx, "mysql:v1:file=binlog.000009;pos=9999;coord=mysql_file_pos"); st != PositionDifferent {
		t.Fatalf("不同的位点应报 different，拿到 %v", st)
	}
}

// 含 DDL 的批次崩在「DDL 已执行、新位点未写」时：位点停在旧值 → ack 必须被拒。
func TestIntegrationDDLBatchKeepsAckAtOldPosition(t *testing.T) {
	ctx := context.Background()
	s := openSink(t, func(o *Options) { o.JobKey = "itdur00002" })
	t.Cleanup(func() {
		_, _ = s.db.ExecContext(context.Background(), "DELETE FROM `_jl_meta` WHERE job_key = ?", s.opt.JobKey)
	})
	dropTable(t, s, "jl_b3_orders")
	if _, err := s.EnsureSchema(ctx, ordersCatalog()); err != nil {
		t.Fatalf("建表: %v", err)
	}
	defer dropTable(t, s, "jl_b3_orders")

	old := "mysql:v1:file=binlog.000009;pos=100;coord=mysql_file_pos"
	if err := s.Apply(ctx, insertBatch("jl_b3_orders", [][]any{{int64(1), "x"}}, old), posOf(t, old)); err != nil {
		t.Fatalf("基线写入: %v", err)
	}

	// 含 DDL 且 DDL 被白名单拒绝 → 数据已落、DDL 没执行、新位点没写。
	newPos := "mysql:v1:file=binlog.000009;pos=200;coord=mysql_file_pos"
	batch := []source.Event{
		{Kind: source.EvBegin, TxID: "t1"},
		{Kind: source.EvInsert, TxID: "t1", Table: source.TableRef{Table: "jl_b3_orders"},
			New:      source.Row{Table: source.TableRef{Table: "jl_b3_orders"}, Values: []source.Value{{Kind: source.KindInt, I: 2}, {Kind: source.KindString, S: "y"}}},
			Relation: relationPtr("jl_b3_orders")},
		{Kind: source.EvDDL, TxID: "t2", DDL: "DROP TABLE `jl_b3_orders`"},
		{Kind: source.EvCommit, TxID: "t2"},
	}
	if err := s.Apply(ctx, batch, posOf(t, newPos)); err == nil {
		t.Fatal("DROP TABLE 不在白名单，必须报错")
	}
	if st, _ := s.PositionDurable(ctx, newPos); st == PositionDurable {
		t.Fatal("DDL 失败时新位点不能是 durable（否则引擎会 ack，源端随后清掉流水）")
	}
	if st, err := s.PositionDurable(ctx, old); err != nil || st != PositionDurable {
		t.Fatalf("旧位点仍应 durable（ack 只能停在这里）: %v err=%v", st, err)
	}
}

// 指纹必须**同端可比**：同一份数据，MySQL 源的指纹与 MySQL 目标的指纹要相等
// （共用 internal/mysqlcanon）。跨引擎则不可比——用 Scope 判定，绝不硬比。
func TestIntegrationFingerprintComparableWithinMySQL(t *testing.T) {
	ctx := context.Background()
	sink := openSink(t, func(o *Options) { o.JobKey = "itfp000001" })
	table := "jl_fp_same"
	dropTable(t, sink, table)
	cat := source.Catalog{Tables: []source.Table{{
		TableRef: source.TableRef{Table: table},
		Columns: []source.Column{
			{Name: "id", Type: "int", Nullable: false},
			{Name: "note", Type: "varchar(32)", Nullable: true},
		},
		PrimaryKey:  []string{"id"},
		HasStablePK: true,
	}}}
	if _, err := sink.EnsureSchema(ctx, cat); err != nil {
		t.Fatalf("建表: %v", err)
	}
	defer dropTable(t, sink, table)
	if err := sink.Apply(ctx, insertBatch(table, [][]any{{int64(1), "a"}, {int64(2), "b"}},
		"mysql:v1:file=b.000001;pos=10;coord=mysql_file_pos"), posOf(t, "mysql:v1:file=b.000001;pos=10;coord=mysql_file_pos")); err != nil {
		t.Fatalf("写入: %v", err)
	}
	dstFP, err := sink.Fingerprint(ctx, source.TableRef{Table: table})
	if err != nil {
		t.Fatalf("目标端指纹: %v", err)
	}
	if sink.FingerprintScope() != "mysql-canon-v1" {
		t.Fatalf("目标端 Scope = %q", sink.FingerprintScope())
	}

	// 源端：同一个库（同一个 MySQL 实例）上算同一张表的指纹，必须与目标端相等。
	src, err := openSourceForTest(t)
	if err != nil {
		// 同一份数据两侧指纹相等，是这套规范编码的核心断言；缺 DSN 就跳过等于
		// 让它在 CI 里永远没人验（本仓库不接受静默通过）。
		t.Fatalf("跨端指纹比对的前提缺失（需要 JINLIANG_TEST_MYSQL_DSN 指向真库）: %v", err)
	}
	srcFP, err := src.Fingerprint(ctx, source.TableRef{Table: table})
	if err != nil {
		t.Fatalf("源端指纹: %v", err)
	}
	if srcFP != dstFP {
		t.Fatalf("同一份数据的源/目标指纹必须相等（同一套规范编码）:\n 源 %s\n 目标 %s", srcFP, dstFP)
	}
	if src.FingerprintScope() != sink.FingerprintScope() {
		t.Fatalf("Scope 不一致就不能比: %q vs %q", src.FingerprintScope(), sink.FingerprintScope())
	}
}

// 声明了 unique_key 的无主键表：upsert 幂等 + 删除能定位（docs/DESIGN-NO-PRIMARY-KEY.md §5 S3 验收）。
func TestIntegrationDeclaredUniqueKeyUpsertAndDelete(t *testing.T) {
	s := openSink(t, func(o *Options) {
		o.OnExists = OnExistsAppend
		o.UniqueKeys = map[string][]string{"jl_b5_decl": {"tenant_id", "event_no"}}
	})
	dropTable(t, s, "jl_b5_decl")
	defer dropTable(t, s, "jl_b5_decl")
	if _, err := s.db.ExecContext(context.Background(),
		"CREATE TABLE `jl_b5_decl` (tenant_id int NOT NULL, event_no varchar(16) NOT NULL, payload varchar(32), UNIQUE KEY uk_event (tenant_id,event_no))"); err != nil {
		t.Fatalf("建带唯一索引的无主键表: %v", err)
	}
	cat := source.Catalog{Tables: []source.Table{{
		TableRef: source.TableRef{Table: "jl_b5_decl"},
		Columns: []source.Column{
			{Name: "tenant_id", Type: "int"},
			{Name: "event_no", Type: "varchar(16)"},
			{Name: "payload", Type: "varchar(32)", Nullable: true},
		},
	}}}
	if _, err := s.EnsureSchema(context.Background(), cat); err != nil {
		t.Fatalf("append 策略应沿用已有表: %v", err)
	}
	rel := &source.Table{
		TableRef: source.TableRef{Table: "jl_b5_decl"},
		Columns: []source.Column{
			{Name: "tenant_id", Type: "int"},
			{Name: "event_no", Type: "varchar(16)"},
			{Name: "payload", Type: "varchar(32)", Nullable: true},
		},
		HasStablePK: false,
	}
	pos := posOf(t, "mysql:v1:file=binlog.000001;pos=600;coord=mysql_file_pos")
	batch := insertBatchWithRel("jl_b5_decl", rel, [][]any{{int64(1), "e1", "x"}}, "")
	if err := s.Apply(context.Background(), batch, pos); err != nil {
		t.Fatalf("声明身份后的首写失败: %v", err)
	}
	// 重放同一批：必须靠声明唯一索引吸收，不能变两行。
	batch2 := insertBatchWithRel("jl_b5_decl", rel, [][]any{{int64(1), "e1", "y"}}, "")
	if err := s.Apply(context.Background(), batch2, posOf(t, "mysql:v1:file=binlog.000001;pos=700;coord=mysql_file_pos")); err != nil {
		t.Fatalf("重放失败: %v", err)
	}
	if n := countRows(t, s, "jl_b5_decl"); n != 1 {
		t.Fatalf("声明行身份后重放应幂等（1 行），实际 %d", n)
	}
	var payload string
	if err := s.db.QueryRowContext(context.Background(),
		"SELECT payload FROM `jl_b5_decl`").Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if payload != "y" {
		t.Fatalf("重放应更新 payload，实际 %q", payload)
	}
	// 删除：按声明列定位。
	del := []source.Event{
		{Kind: source.EvBegin, TxID: "tx"},
		{Kind: source.EvDelete, TxID: "tx", Table: source.TableRef{Table: "jl_b5_decl"},
			Relation: rel,
			Old: source.Row{Table: source.TableRef{Table: "jl_b5_decl"},
				Values: []source.Value{{Kind: source.KindInt, I: int64(1)}, {Kind: source.KindString, S: "e1"}, {Kind: source.KindString, S: "y"}}}},
		{Kind: source.EvCommit, TxID: "tx"},
	}
	if err := s.Apply(context.Background(), del, posOf(t, "mysql:v1:file=binlog.000001;pos=800;coord=mysql_file_pos")); err != nil {
		t.Fatalf("按声明列删除失败: %v", err)
	}
	if n := countRows(t, s, "jl_b5_decl"); n != 0 {
		t.Fatalf("删除后应 0 行，实际 %d", n)
	}
}

// 声明与目标端索引对不上：写之前就必须停（R3：ON DUPLICATE KEY 不会自己报错）。
func TestIntegrationDeclaredUniqueKeyMismatchFails(t *testing.T) {
	s := openSink(t, func(o *Options) {
		o.OnExists = OnExistsAppend
		o.UniqueKeys = map[string][]string{"jl_b5_decl": {"tenant_id"}}
	})
	dropTable(t, s, "jl_b5_decl")
	defer dropTable(t, s, "jl_b5_decl")
	if _, err := s.db.ExecContext(context.Background(),
		"CREATE TABLE `jl_b5_decl` (tenant_id int NOT NULL, event_no varchar(16) NOT NULL, payload varchar(32), UNIQUE KEY uk_event (tenant_id,event_no))"); err != nil {
		t.Fatalf("建带唯一索引的无主键表: %v", err)
	}
	cat := source.Catalog{Tables: []source.Table{{
		TableRef: source.TableRef{Table: "jl_b5_decl"},
		Columns: []source.Column{
			{Name: "tenant_id", Type: "int"},
			{Name: "event_no", Type: "varchar(16)"},
			{Name: "payload", Type: "varchar(32)", Nullable: true},
		},
	}}}
	if _, err := s.EnsureSchema(context.Background(), cat); err != nil {
		t.Fatalf("append 策略应沿用已有表: %v", err)
	}
	rel := &source.Table{
		TableRef: source.TableRef{Table: "jl_b5_decl"},
		Columns: []source.Column{
			{Name: "tenant_id", Type: "int"},
			{Name: "event_no", Type: "varchar(16)"},
			{Name: "payload", Type: "varchar(32)", Nullable: true},
		},
		HasStablePK: false,
	}
	err := s.Apply(context.Background(), insertBatchWithRel("jl_b5_decl", rel, [][]any{{int64(1), "e1", "x"}}, ""),
		posOf(t, "mysql:v1:file=binlog.000001;pos=900;coord=mysql_file_pos"))
	if err == nil {
		t.Fatal("声明与目标端索引不一致时必须失败")
	}
	if !errors.Is(err, ErrNoUniqueKey) {
		t.Fatalf("错误应可判为 ErrNoUniqueKey: %v", err)
	}
	if !strings.Contains(err.Error(), "unique_key") {
		t.Fatalf("错误应点名 unique_key 与核对结论: %v", err)
	}
	if n := countRows(t, s, "jl_b5_decl"); n != 0 {
		t.Fatalf("失败时不能写行，实际 %d", n)
	}
}

// 声明比真实索引「宽」时，一条 DELETE 会按声明列删掉整组行——所以核对必须在
// **Apply 入口、整批前置**，不能只挂在 upsert 分支（Owl PR #120 P0）。
func TestIntegrationDeclaredKeyMismatchBlocksDeleteToo(t *testing.T) {
	s := openSink(t, func(o *Options) {
		o.OnExists = OnExistsAppend
		// 目标端唯一索引是 (tenant_id,event_no)，声明只写了 tenant_id。
		o.UniqueKeys = map[string][]string{"jl_b5_del": {"tenant_id"}}
	})
	dropTable(t, s, "jl_b5_del")
	defer dropTable(t, s, "jl_b5_del")
	if _, err := s.db.ExecContext(context.Background(),
		"CREATE TABLE `jl_b5_del` (tenant_id int NOT NULL, event_no varchar(16) NOT NULL, payload varchar(32), UNIQUE KEY uk_event (tenant_id,event_no))"); err != nil {
		t.Fatalf("建表: %v", err)
	}
	if _, err := s.db.ExecContext(context.Background(),
		"INSERT INTO `jl_b5_del` VALUES (1,'e1','a'),(1,'e2','b')"); err != nil {
		t.Fatalf("灌数据: %v", err)
	}
	rel := &source.Table{
		TableRef: source.TableRef{Table: "jl_b5_del"},
		Columns: []source.Column{
			{Name: "tenant_id", Type: "int"},
			{Name: "event_no", Type: "varchar(16)"},
			{Name: "payload", Type: "varchar(32)", Nullable: true},
		},
		HasStablePK: false,
	}
	// 只有一条 DELETE 的批次：没有 upsert 分支可挂核对，正是漏洞所在。
	del := []source.Event{
		{Kind: source.EvBegin, TxID: "tx"},
		{Kind: source.EvDelete, TxID: "tx", Table: source.TableRef{Table: "jl_b5_del"},
			Relation: rel,
			Old: source.Row{Table: source.TableRef{Table: "jl_b5_del"}, Values: []source.Value{
				{Kind: source.KindInt, I: int64(1)}, {Kind: source.KindString, S: "e1"}, {Kind: source.KindString, S: "a"}}}},
		{Kind: source.EvCommit, TxID: "tx"},
	}
	err := s.Apply(context.Background(), del, posOf(t, "mysql:v1:file=binlog.000001;pos=1000;coord=mysql_file_pos"))
	if err == nil {
		t.Fatal("声明与目标端索引不一致时，DELETE 也必须被拦下（否则会删掉整组行还返回成功）")
	}
	if !errors.Is(err, ErrNoUniqueKey) {
		t.Fatalf("错误应可判为 ErrNoUniqueKey: %v", err)
	}
	if n := countRows(t, s, "jl_b5_del"); n != 2 {
		t.Fatalf("被拦下时一行都不能删，实际剩 %d 行", n)
	}
	if p, _ := s.MetaPosition(context.Background()); strings.Contains(p, "pos=1000") {
		t.Fatalf("失败时位点不能前进: %q", p)
	}
}
