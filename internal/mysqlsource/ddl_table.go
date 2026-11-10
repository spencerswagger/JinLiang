package mysqlsource

import (
	"strings"
)

// ddlTargetTable 从一条 DDL / TRUNCATE 语句里取出**它作用在哪张表上**。
//
// 为什么必须有：binlog 的 Query 事件只带「默认库 + 语句原文」，不带表名；
// 而引擎按 `schema.表` 判断「这条 DDL 在不在本次同步范围里」。表名给不出来时，
// 判定必然落空 —— 结果就是**源端 DDL 被静默丢掉**（连「本版不搬 DDL」那声提示都发不出）。
//
// 取舍：
//   - 只认能**唯一**指向一张表的形态；拿不准就返回 ok=false，让上层保持「不知道是哪张表」，
//     宁可少提示一次，也不要把一条 DDL 记到错的表头上（错的表名会把用户引去改错地方）。
//   - 多表形态（`RENAME TABLE a TO b, c TO d`、`DROP TABLE a, b`）一律不猜。
//   - 名字保持原文大小写：MySQL 的表名在 Linux 上区分大小写，这里不做归一化。
func ddlTargetTable(stmt string) (schema, table string, ok bool) {
	toks := ddlTokens(stmt)
	if len(toks) == 0 {
		return "", "", false
	}
	up := strings.ToUpper
	switch up(toks[0]) {
	case "ALTER", "CREATE", "DROP", "OPTIMIZE", "REPAIR", "ANALYZE", "CHECK":
		i := 1
		// CREATE [UNIQUE|FULLTEXT|SPATIAL] INDEX name ON tbl
		if up(toks[0]) == "CREATE" || up(toks[0]) == "DROP" {
			for i < len(toks) && (up(toks[i]) == "UNIQUE" || up(toks[i]) == "FULLTEXT" ||
				up(toks[i]) == "SPATIAL" || up(toks[i]) == "TEMPORARY") {
				i++
			}
			if i < len(toks) && up(toks[i]) == "INDEX" {
				// 跳过索引名，找 ON
				for j := i + 1; j < len(toks); j++ {
					if up(toks[j]) == "ON" {
						return identPair(toks[j+1:])
					}
				}
				return "", "", false
			}
		}
		if i >= len(toks) {
			return "", "", false
		}
		switch up(toks[i]) {
		case "TABLE", "TABLES", "VIEW":
			i++
			// IF NOT EXISTS / IF EXISTS
			if i+1 < len(toks) && up(toks[i]) == "IF" {
				if up(toks[i+1]) == "NOT" {
					i += 3
				} else {
					i += 2
				}
			}
			// 多表形态：`DROP TABLE a, b`、`OPTIMIZE TABLE a, b`。
			// **ALTER / CREATE 不判逗号**：`ALTER TABLE t ADD INDEX i (a), ADD INDEX j (b)`
			// 与 `CREATE TABLE t (a INT, b INT)` 里的逗号都是单表的正常写法，
			// 拿逗号否掉它们等于把绝大多数 DDL 变成「不知道是哪张表」。
			if up(toks[0]) != "ALTER" && up(toks[0]) != "CREATE" && hasTopLevelComma(toks[i:]) {
				return "", "", false
			}
			return identPair(toks[i:])
		case "INDEX":
			// ALTER TABLE 之外的 ALTER INDEX 不是 MySQL 语法；不猜。
			return "", "", false
		}
		return "", "", false
	case "TRUNCATE":
		i := 1
		if i < len(toks) && up(toks[i]) == "TABLE" {
			i++
		}
		return identPair(toks[i:])
	case "RENAME":
		i := 1
		if i < len(toks) && (up(toks[i]) == "TABLE" || up(toks[i]) == "TABLES") {
			i++
		}
		if hasTopLevelComma(toks[i:]) {
			return "", "", false // 多对重命名：说不清是哪张表
		}
		// 认**目标**名字：`RENAME TABLE _t_gho TO t`（gh-ost / pt-osc 的收尾）之后，
		// 往后所有变更都属于 `t`；报旧名字等于让后面这条提示指着一张已经不存在的表。
		for j := i; j < len(toks); j++ {
			if up(toks[j]) == "TO" {
				return identPair(toks[j+1:])
			}
		}
		return identPair(toks[i:])
	}
	return "", "", false
}

// identPair 从待解析的词元里读出 `[库.]表`。
//
// 输入形如 ["`db`.`orders`", "ADD", …] 或 ["orders", "(", …]：
// 只看前一到两个词元，遇到别的形态就不猜。
func identPair(toks []string) (schema, table string, ok bool) {
	if len(toks) == 0 || !looksLikeIdent(toks[0]) {
		return "", "", false
	}
	first := toks[0]
	// `` `a`.`b` ``：两个被引号包住的标识符（分词时已合成一个词元）→ 按 `\`.\`` 拆。
	if i := strings.Index(first, "`.`"); i > 0 {
		return trimIdent(first[:i+1]), trimIdent(first[i+2:]), true
	}
	// 整词被一对引号包住（`` `a.b` ``）：这是**一个**标识符，里面的点是名字的一部分。
	if len(first) >= 2 && (first[0] == '`' || first[0] == '"') && first[len(first)-1] == first[0] {
		return "", trimIdent(first), true
	}
	if i := strings.IndexByte(first, '.'); i >= 0 {
		// `db`.`tbl` 在分词阶段已经合成一个词元
		schema, table = trimIdent(first[:i]), trimIdent(first[i+1:])
		return schema, table, schema != "" && table != ""
	}
	// 没有点：可能是 `db`.`x` 被拆成三个词元（`.` 单独成词元）
	if len(toks) >= 3 && toks[1] == "." && looksLikeIdent(toks[2]) {
		return trimIdent(toks[0]), trimIdent(toks[2]), true
	}
	table = trimIdent(first)
	return "", table, table != ""
}

// hasTopLevelComma 报告这一段里有没有「不属于函数调用/列定义」的逗号。
// 有就说明是多对象语句（多张表），不猜。
func hasTopLevelComma(toks []string) bool {
	depth := 0
	for _, t := range toks {
		switch t {
		case "(":
			depth++
		case ")":
			if depth > 0 {
				depth--
			}
		case ",":
			if depth == 0 {
				return true
			}
		}
	}
	return false
}

// ddlTokens 把语句切成词元：标识符（含 `db`.`tbl` 合并）、点、括号、逗号、关键字。
//
// 只做到「够用」：注释先剥掉，字符串字面量与反引号里的内容整体当一个词元，
// 其余按空白与 `(`, `)`, `,`, `.` 切分。
func ddlTokens(stmt string) []string {
	s := stripSQLComments(stmt)
	var toks []string
	var cur strings.Builder
	flush := func() {
		if cur.Len() > 0 {
			toks = append(toks, cur.String())
			cur.Reset()
		}
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		// 语句**中间**的注释也要跳：`ALTER TABLE /*gh-ost*/ t …` 是 gh-ost / pt-osc 的常见形态，
		// 不跳的话表名会被解析成 `/*` 这样的垃圾词元（Owl II 实测过）。
		if c == '/' && i+1 < len(s) && s[i+1] == '*' {
			if j := strings.Index(s[i+2:], "*/"); j >= 0 {
				flush()
				i += 2 + j + 1
				continue
			}
			break
		}
		if c == '-' && i+1 < len(s) && s[i+1] == '-' {
			flush()
			if j := strings.IndexByte(s[i+2:], '\n'); j >= 0 {
				i += 2 + j
				continue
			}
			break
		}
		switch {
		case c == '\'' || c == '"' || c == '`':
			// 引号内容整体进当前词元（反引号标识符里可以有空格与点）
			quote := c
			cur.WriteByte(c)
			i++
			for ; i < len(s); i++ {
				cur.WriteByte(s[i])
				if s[i] == quote {
					// MySQL 里连续两个引号是转义
					if i+1 < len(s) && s[i+1] == quote {
						cur.WriteByte(s[i+1])
						i++
						continue
					}
					break
				}
			}
		case c == '`':
			cur.WriteByte(c)
		case c == '(' || c == ')' || c == ',':
			flush()
			toks = append(toks, string(c))
		case c == '.':
			// `a`.`b` 里的点夹在反引号之间：合并成一个词元，交给 identPair
			if cur.Len() > 0 && strings.HasSuffix(cur.String(), "`") {
				cur.WriteByte('.')
				continue
			}
			flush()
			toks = append(toks, ".")
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == ';':
			flush()
		default:
			cur.WriteByte(c)
		}
	}
	flush()
	return toks
}

func looksLikeIdent(t string) bool {
	if t == "" || t == "." || t == "(" || t == ")" || t == "," {
		return false
	}
	// 纯标点/运算符不算标识符
	return strings.IndexFunc(t, func(r rune) bool {
		return r == '=' || r == '<' || r == '>'
	}) < 0
}

func trimIdent(s string) string {
	if len(s) >= 2 && (s[0] == '`' || s[0] == '"') && s[len(s)-1] == s[0] {
		return s[1 : len(s)-1]
	}
	return s
}

// stripSQLComments 剥掉 `/* … */` 与 `-- …` 注释（放行的判定用同一套，别两处口径不同）。
func stripSQLComments(s string) string {
	for {
		t := strings.TrimLeft(s, " \t\r\n")
		switch {
		case strings.HasPrefix(t, "/*"):
			if i := strings.Index(t, "*/"); i >= 0 {
				s = t[i+2:]
				continue
			}
			return t
		case strings.HasPrefix(t, "--"):
			if i := strings.IndexByte(t, '\n'); i >= 0 {
				s = t[i+1:]
				continue
			}
			return ""
		default:
			return s
		}
	}
}
