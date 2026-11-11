//go:build integration

// 清表的所有权闸门（契约 §1.6 步 3 / §4.6 红线 4）：**只有本任务建的表才能清**。
//
// 为什么这些用例值得真库跑：判据在 `_jl_object` 里，它和 `TRUNCATE` 一样是 SQL 行为；
// 而「误清用户表」的后果是不可回滚的——mock 掉库等于把最该验的那一步跳过了。
package mysqlsink

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/source"
)

// mkUserTable 造一张「用户自己的表」：有行、**不登记**在 `_jl_object` 里。
func mkUserTable(t *testing.T, s *Sink, name string, rows int) {
	t.Helper()
	ctx := context.Background()
	dropTable(t, s, name)
	if _, err := s.db.ExecContext(ctx,
		"CREATE TABLE `"+name+"` (id bigint PRIMARY KEY, payload varchar(32)) ENGINE=InnoDB"); err != nil {
		t.Fatalf("建用户表失败: %v", err)
	}
	for i := 1; i <= rows; i++ {
		if _, err := s.db.ExecContext(ctx,
			"INSERT INTO `"+name+"` (id,payload) VALUES (?,?)", i, "user-row"); err != nil {
			t.Fatalf("灌用户行失败: %v", err)
		}
	}
	// 顺手把这名字的对象登记清掉：上一次跑同名的用例可能登记过。
	if _, err := s.db.ExecContext(ctx,
		"DELETE FROM `_jl_object` WHERE object_name = ?", s.tableObjectName(refOf(name))); err != nil {
		t.Fatalf("清对象登记失败: %v", err)
	}
}

// ① 误伤用例（要求的那一格）：用户表 + 重搬路径要清它 → **拒绝**，行一条不少。
func TestIntegrationTruncateRefusesUserTable(t *testing.T) {
	s := openSink(t, nil)
	ensureMetaRow(t, s)
	const tbl = "jl_b3_user_owned"
	mkUserTable(t, s, tbl, 7)
	t.Cleanup(func() { dropTable(t, s, tbl) })

	err := s.TruncateTable(context.Background(), refOf(tbl))
	if err == nil {
		t.Fatal("不是本任务建的表，绝不允许清空（这是删用户数据、不可回滚）")
	}
	if !errors.Is(err, ErrTruncateNotOwned) {
		t.Fatalf("错误要可判等（ErrTruncateNotOwned），实际 %v", err)
	}
	t.Logf("拒绝输出>>> %v", err)
	if n := countRows(t, s, tbl); n != 7 {
		t.Fatalf("用户表被清掉了：应有 7 行，实际 %d", n)
	}
	// 错误必须能带人走出去：三条出路里至少要有「改成 recreate」与「移出范围」。
	for _, want := range []string{"on_exists=recreate", "content.tables", "不可回滚"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("错误文案里应有 %q（用户要能照做）：%v", want, err)
		}
	}
}

// ② 源端 TRUNCATE 也要过同一道闸：append 语义下不许把用户表清掉。
func TestIntegrationTruncateEventRefusesUserTable(t *testing.T) {
	s := openSink(t, nil)
	ensureMetaRow(t, s)
	const tbl = "jl_b3_user_trunc_event"
	mkUserTable(t, s, tbl, 5)
	t.Cleanup(func() { dropTable(t, s, tbl) })

	err := s.Apply(context.Background(), []source.Event{
		{Kind: source.EvTruncate, Table: refOf(tbl)},
	}, posOf(t, "mysql:v1:file=binlog.000001;pos=120"))
	if err == nil {
		t.Fatal("源端 TRUNCATE 跟随到一张用户自己的表上必须明确失败")
	}
	if !errors.Is(err, ErrTruncateNotOwned) {
		t.Fatalf("错误要可判等（ErrTruncateNotOwned），实际 %v", err)
	}
	if n := countRows(t, s, tbl); n != 5 {
		t.Fatalf("用户表被 TRUNCATE 了：应有 5 行，实际 %d", n)
	}
}

// ③ 本任务建的表：照常清（别把闸门做成「什么都不许清」）。
func TestIntegrationTruncateOwnedTableWorks(t *testing.T) {
	s := openSink(t, nil)
	ensureMetaRow(t, s)
	const tbl = "jl_b3_our_table"
	mkUserTable(t, s, tbl, 4)
	t.Cleanup(func() { dropTable(t, s, tbl) })

	// 登记成「本任务建的」——这正是 EnsureSchema 建表之后做的事。
	if err := s.RecordObject(context.Background(), s.tableObjectName(refOf(tbl)), "table"); err != nil {
		t.Fatalf("登记所有权失败: %v", err)
	}
	if err := s.TruncateTable(context.Background(), refOf(tbl)); err != nil {
		t.Fatalf("本任务建的表应该能清: %v", err)
	}
	if n := countRows(t, s, tbl); n != 0 {
		t.Fatalf("清完之后应为 0 行，实际 %d", n)
	}
	// EvTruncate 走同一条路：也是能清的。
	if _, err := s.db.ExecContext(context.Background(),
		"INSERT INTO `"+tbl+"` (id,payload) VALUES (1,'x')"); err != nil {
		t.Fatal(err)
	}
	if err := s.Apply(context.Background(), []source.Event{
		{Kind: source.EvTruncate, Table: refOf(tbl)},
	}, posOf(t, "mysql:v1:file=binlog.000001;pos=240")); err != nil {
		t.Fatalf("本任务建的表，源端 TRUNCATE 应能跟随: %v", err)
	}
	if n := countRows(t, s, tbl); n != 0 {
		t.Fatalf("跟随 TRUNCATE 之后应为 0 行，实际 %d", n)
	}
}

// ④ 判断不了所有权（`_jl_object` 读不到）：**也失败**，而且要说清是「查不到」而不是「不是我们的」。
func TestIntegrationTruncateFailsClosedWhenOwnershipUnknown(t *testing.T) {
	s := openSink(t, nil)
	ensureMetaRow(t, s)
	const tbl = "jl_b3_unknown_owner"
	mkUserTable(t, s, tbl, 3)
	t.Cleanup(func() { dropTable(t, s, tbl) })

	if _, err := s.db.ExecContext(context.Background(), "DROP TABLE `_jl_object`"); err != nil {
		t.Fatalf("模拟读不到登记表失败: %v", err)
	}
	t.Cleanup(func() { _ = s.ensureStateTables(context.Background()) })

	err := s.TruncateTable(context.Background(), refOf(tbl))
	if err == nil {
		t.Fatal("查不到所有权时必须失败（判断不了就别动数据）")
	}
	if !strings.Contains(err.Error(), "_jl_object") {
		t.Fatalf("文案要点名是被哪张表挡住（`_jl_object`），实际: %v", err)
	}
	if errors.Is(err, ErrTruncateNotOwned) {
		t.Fatalf("「查不到」不是「不是我们的」，两者要能分开: %v", err)
	}
	if n := countRows(t, s, tbl); n != 3 {
		t.Fatalf("查不到所有权时不许清表：应有 3 行，实际 %d", n)
	}
}
