package mysqlsink

import (
	"context"
	"fmt"
	"strings"

	"github.com/spencerswagger/jinliang/internal/source"
)

// ErrNoUniqueKey 表示目标表没有唯一键/主键，upsert 无法幂等。
//
// 什么时候还会见到它：**源端有主键、目标端缺键**（源端身份明确了，目标端却定位不了），
// 或声明了 unique_key 但目标端索引对不上（设计 §4.3）。源端本来就无主键的表不走这条路：
// 那是 insert-only（只 INSERT，重放可能重复，口径见 docs/DESIGN-NO-PRIMARY-KEY.md §1.2）。
// 为什么不把有身份的表也退化成纯 INSERT：重放（断线重连、崩溃重启）会再插一遍，
// 产生重复行——那是**静默的数据损坏**，比一条明确的错误危险得多（docs/CONFLICT.md）。
var ErrNoUniqueKey = fmt.Errorf("目标表没有唯一键，无法做 upsert")

// hasUniqueKey 查目标表有没有唯一键（含主键）。结果按表缓存。
func (s *Sink) hasUniqueKey(ctx context.Context, table source.TableRef) (bool, error) {
	schema := s.schemaOf(table)
	key := schema + "." + table.Table
	if v, ok := s.uniqueCache[key]; ok {
		return v, nil
	}
	var n int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND NON_UNIQUE = 0`,
		schema, table.Table).Scan(&n)
	if err != nil {
		return false, fmt.Errorf("查 %s 的唯一键失败: %w", key, err)
	}
	ok := n > 0
	s.uniqueCache[key] = ok
	return ok, nil
}

// insertSQL 造一条多行纯 INSERT（无主键表的 insert-only 写路径）。
//
// 口径：`docs/DESIGN-NO-PRIMARY-KEY.md` §1.2 路线 A——表没有行身份时目标端只 INSERT，
// 从不 UPDATE/DELETE。**不拼 `ON DUPLICATE KEY`**：那要求唯一键，而无主键表没有；
// 重放窗口内可能插重复行，这是设计接受的代价（R1：写路径不拿目标端索引猜行身份）。
func insertSQL(table source.TableRef, cols []string, rows int) string {
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(qualified(table))
	b.WriteString(" (")
	for i, c := range cols {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(quoteIdent(c))
	}
	b.WriteString(") VALUES ")
	for r := 0; r < rows; r++ {
		if r > 0 {
			b.WriteString(",")
		}
		b.WriteString("(")
		for i := range cols {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString("?")
		}
		b.WriteString(")")
	}
	return b.String()
}

// upsertSQL 造一条多行 upsert：
//
//	INSERT INTO t (a,b) VALUES (?,?),(?,?)
//	  AS new ON DUPLICATE KEY UPDATE b=new.b      -- 8.0.19+
//	INSERT INTO t (a,b) VALUES (?,?),(?,?)
//	  ON DUPLICATE KEY UPDATE b=VALUES(b)         -- 5.7 / 8.0.18-
//
// 注意它**不是** `INSERT IGNORE`：IGNORE 遇到重复键是「丢掉这一行」，语义完全不同，
// 会把更新静默变成丢弃。
func (s *Sink) upsertSQL(table source.TableRef, cols []string, rows int, updateCols []string) string {
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(qualified(table))
	b.WriteString(" (")
	for i, c := range cols {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(quoteIdent(c))
	}
	b.WriteString(") VALUES ")
	for r := 0; r < rows; r++ {
		if r > 0 {
			b.WriteString(",")
		}
		b.WriteString("(")
		for i := range cols {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString("?")
		}
		b.WriteString(")")
	}
	if s.aliasUpsert {
		b.WriteString(" AS new")
	}
	if len(updateCols) == 0 {
		// 只有主键列：重复即等价，更新主键本身让语句合法且不产生副作用。
		updateCols = cols[:1]
	}
	b.WriteString(" ON DUPLICATE KEY UPDATE ")
	for i, c := range updateCols {
		if i > 0 {
			b.WriteString(",")
		}
		if s.aliasUpsert {
			fmt.Fprintf(&b, "%s=new.%s", quoteIdent(c), quoteIdent(c))
		} else {
			fmt.Fprintf(&b, "%s=VALUES(%s)", quoteIdent(c), quoteIdent(c))
		}
	}
	return b.String()
}

// deleteSQL 按主键删一行。没有主键就没法定位，明确报错。
func deleteSQL(table source.TableRef, pk []string) (string, error) {
	if len(pk) == 0 {
		return "", fmt.Errorf("%s 没有主键，无法按行删除（DELETE 若退化成全表条件会清空目标表）", qualified(table))
	}
	var b strings.Builder
	b.WriteString("DELETE FROM ")
	b.WriteString(qualified(table))
	b.WriteString(" WHERE ")
	for i, c := range pk {
		if i > 0 {
			b.WriteString(" AND ")
		}
		b.WriteString(quoteIdent(c))
		b.WriteString("=?")
	}
	return b.String(), nil
}

func qualified(t source.TableRef) string {
	if t.Schema == "" {
		return quoteIdent(t.Table)
	}
	return quoteIdent(t.Schema) + "." + quoteIdent(t.Table)
}

// qualifiedOn 明确带上库名：目标端的 DDL/DML 都写全名，免得跟着连接的默认库漂。
func (s *Sink) qualifiedOn(t source.TableRef) string {
	return quoteIdent(s.schemaOf(t)) + "." + quoteIdent(t.Table)
}

func quoteIdent(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }
