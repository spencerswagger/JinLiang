package mysqlsource

import "testing"

// TestDDLTargetTable：从语句原文里认出「这条 DDL 作用在哪张表」。
//
// 这条判定的唯一用途是「要不要给用户发一声提示 / 把暂停记到哪张表上」，
// 所以两件事比"多认几种写法"更重要：
//   - 认得出来就必须是**对的那张表**（认错表 = 把用户引去改错地方）；
//   - 认不出来就明确说认不出来（ok=false），不许瞎猜。
func TestDDLTargetTable(t *testing.T) {
	cases := []struct {
		stmt   string
		schema string
		table  string
		ok     bool
		why    string
	}{
		{"ALTER TABLE `orders` ADD COLUMN note VARCHAR(64) NULL", "", "orders", true,
			"反引号表名（最常见形态）"},
		{"ALTER TABLE `shop`.`orders` MODIFY COLUMN amount DECIMAL(14,2)", "shop", "orders", true,
			"`库`.`表` 要被拆成两段，不能把整串当表名"},
		{"ALTER TABLE shop.orders MODIFY COLUMN amount DECIMAL(14,2)", "shop", "orders", true,
			"不带反引号的 库.表"},
		{"ALTER TABLE orders MODIFY COLUMN v VARCHAR(64) NOT NULL", "", "orders", true,
			"改类型（第②步的触发形态）"},
		{"ALTER TABLE t ADD INDEX i (a), ADD INDEX j (b)", "", "t", true,
			"单表多个 ADD 子句：逗号在单表 ALTER 里是正常的，不能因此判成多表"},
		{"/* pt-osc */ ALTER TABLE `orders` DROP COLUMN old_col", "", "orders", true,
			"前导注释（pt-osc / ORM 生成的 DDL 常带）"},
		{"-- 维护窗口改的\nALTER TABLE orders ADD COLUMN c INT", "", "orders", true,
			"前导行注释"},
		{"CREATE TABLE IF NOT EXISTS `orders` (id BIGINT PRIMARY KEY)", "", "orders", true,
			"建表带 IF NOT EXISTS"},
		{"CREATE UNIQUE INDEX idx_amount ON `orders` (`amount`)", "", "orders", true,
			"建索引：表名在 ON 后面，不是索引名"},
		{"DROP INDEX idx_amount ON orders", "", "orders", true,
			"删索引同样是 ON 后面"},
		{"TRUNCATE TABLE orders", "", "orders", true, "TRUNCATE 带 TABLE"},
		{"TRUNCATE orders", "", "orders", true, "TRUNCATE 不带 TABLE（MySQL 允许）"},
		{"RENAME TABLE a TO b", "", "a", true, "单个重命名：认旧表名"},
		{"RENAME TABLE a TO b, c TO d", "", "", false,
			"多对重命名：说不清是哪张表，宁可不猜"},
		{"DROP TABLE a, b", "", "", false, "多表 DROP：同上"},
		{"DROP TABLE IF EXISTS `t`", "", "t", true, "DROP TABLE IF EXISTS"},
		{"", "", "", false, "空语句"},
		{"FLUSH PRIVILEGES", "", "", false, "与表无关的语句"},
		{"GRANT SELECT ON shop.* TO u", "", "", false, "授权语句不是表级 DDL"},
	}
	for _, c := range cases {
		schema, table, ok := ddlTargetTable(c.stmt)
		if ok != c.ok || schema != c.schema || table != c.table {
			t.Errorf("ddlTargetTable(%q) = (%q, %q, %v)，应为 (%q, %q, %v) —— %s",
				c.stmt, schema, table, ok, c.schema, c.table, c.ok, c.why)
		}
	}
}
