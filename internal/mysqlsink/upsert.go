package mysqlsink

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/spencerswagger/jinliang/internal/pgsink"
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

// declaredCheck 是「声明的 unique_key 与目标端实际索引」的核对结果（按表缓存）。
type declaredCheck struct {
	ok  bool
	why string
}

// declaredKeyFor 返回这张表生效的声明行身份；源端有主键时声明被忽略（§4.2 第 4 条：
// 主键就是身份，换一组列当身份会让同一条数据两次同步用两个身份）。
func (s *Sink) declaredKeyFor(table source.TableRef, rel *source.Table) []string {
	if rel != nil && rel.HasStablePK {
		return nil
	}
	return s.opt.UniqueKeys[table.Table]
}

// verifyDeclaredKey 实现 §4.3 的目标端核对判据（五条全过才算「唯一键可用」）：
// 存在非主键、非部分、非表达式、无前缀长度、列集合与声明完全一致、且列都 NOT NULL 的唯一索引。
// 目录层（connect.Catalog）已过滤掉部分/表达式/前缀索引，这里再按同一判据复核一遍：
// 从声明到写入之间目标端可能被改过（索引被删/换），运行时要响，而不是静默写重复（R3）。
func (s *Sink) verifyDeclaredKey(ctx context.Context, table source.TableRef, key []string) (bool, string, error) {
	cacheKey := s.schemaOf(table) + "." + table.Table + "\x00" + strings.Join(key, ",")
	if v, ok := s.declaredCache[cacheKey]; ok {
		return v.ok, v.why, nil
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT INDEX_NAME, COLUMN_NAME FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND NON_UNIQUE = 0
		  AND INDEX_NAME <> 'PRIMARY' AND SUB_PART IS NULL AND COLUMN_NAME IS NOT NULL
		ORDER BY INDEX_NAME, SEQ_IN_INDEX`,
		s.schemaOf(table), table.Table)
	if err != nil {
		return false, "", fmt.Errorf("读 %s 的唯一索引失败: %w", qualified(table), err)
	}
	defer rows.Close()
	var names []string
	byIndex := map[string][]string{}
	for rows.Next() {
		var idx, col string
		if err := rows.Scan(&idx, &col); err != nil {
			return false, "", err
		}
		if _, seen := byIndex[idx]; !seen {
			names = append(names, idx)
		}
		byIndex[idx] = append(byIndex[idx], col)
	}
	if err := rows.Err(); err != nil {
		return false, "", err
	}

	want := map[string]bool{}
	for _, c := range key {
		want[c] = true
	}
	var existing []string
	var match []string
	for _, name := range names {
		cols := byIndex[name]
		existing = append(existing, "("+strings.Join(cols, ", ")+")")
		if len(cols) != len(key) {
			continue
		}
		same := true
		for _, c := range cols {
			if !want[c] {
				same = false
				break
			}
		}
		if same {
			match = cols
			break
		}
	}
	if match == nil {
		res := declaredCheck{ok: false, why: "目标端这张表没有与声明逐列一致的可用唯一索引（要求：非主键、非部分、非表达式、无前缀长度）"}
		if len(existing) > 0 {
			res.why = fmt.Sprintf("目标端唯一索引是 %s，与声明 (%s) 的列集合不一致",
				strings.Join(existing, "、"), strings.Join(key, ", "))
		}
		s.declaredCache[cacheKey] = res
		return false, res.why, nil
	}
	// 列都 NOT NULL：NULL 互不相等，含 NULL 的列组合不构成唯一性（判据 5）。
	args := []any{s.schemaOf(table), table.Table}
	holders := make([]string, len(key))
	for i, c := range key {
		holders[i] = "?"
		args = append(args, c)
	}
	var nullable string
	err = s.db.QueryRowContext(ctx, `
		SELECT COLUMN_NAME FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND IS_NULLABLE = 'YES'
		  AND COLUMN_NAME IN (`+strings.Join(holders, ",")+`) LIMIT 1`,
		args...).Scan(&nullable)
	if err != nil && err != sql.ErrNoRows {
		return false, "", fmt.Errorf("读 %s 的列可空性失败: %w", qualified(table), err)
	}
	if err == nil {
		res := declaredCheck{ok: false, why: fmt.Sprintf("唯一索引 (%s) 的列 %s 允许为空：NULL 互不相等，这组列不构成唯一性",
			strings.Join(match, ", "), nullable)}
		s.declaredCache[cacheKey] = res
		return false, res.why, nil
	}
	s.declaredCache[cacheKey] = declaredCheck{ok: true}
	return true, "", nil
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

// deleteSQL 按定位列（主键或声明的 unique_key）删一行。没有定位列就没法删：
// 按表暂停这张表（no_pk_write / E1017），绝不退化成全表条件（§6 红线 2）。
func deleteSQL(table source.TableRef, pk []string) (string, error) {
	if len(pk) == 0 {
		return "", &pgsink.NoPKWriteError{TableName: table.Table,
			Detail: "收到一次删除，但这张表没有主键、也没声明 unique_key：目标端定位不到该删哪一行"}
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
