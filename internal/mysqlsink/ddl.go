package mysqlsink

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/spencerswagger/jinliang/internal/source"
)

// DDLClass 是白名单四类。
type DDLClass string

const (
	// DDLAllow：可安全跟随（加列、放宽长度、加普通索引）。
	DDLAllow DDLClass = "allow"
	// DDLSkip：与数据无关，跳过不照搬（索引删除、注释变更）。
	DDLSkip DDLClass = "skip"
	// DDLConfirm：会影响语义，必须人确认（删列、加唯一索引/改主键）。
	DDLConfirm DDLClass = "confirm"
	// DDLReject：白名单之外（drop/rename/改窄类型/未知语句）→ 停。
	DDLReject DDLClass = "reject"
)

// DDLDecision 是一条 DDL 的判定结果。
type DDLDecision struct {
	Class   DDLClass
	Table   source.TableRef
	Column  string // MODIFY/CHANGE 时的列名
	NewType string // MODIFY/CHANGE 时的新类型
	SQL     string // 要执行的语句（skip 时为空）
	Reason  string // 人话：为什么这么判
}

// Allowed 表示可以直接执行。
func (d DDLDecision) Allowed() bool { return d.Class == DDLAllow }

// ClassifyDDL 判断一条 DDL 属于白名单哪一类。
//
// 口径（docs/SPEC.md `content.schema_follow`）：只有四类跟随——加列、放宽长度、加索引、
// 可选删列（删列要确认）。其余一律 `reject`，由调用方停下并报错；
// **不许静默跳过、不许照搬**。索引删除判 `skip`：它不改变行数据，照搬没有意义。
func ClassifyDDL(stmt string) DDLDecision {
	raw := strings.TrimSpace(stmt)
	up := strings.ToUpper(raw)
	fields := strings.Fields(raw)
	if len(fields) < 2 {
		return DDLDecision{Class: DDLReject, Reason: "看不懂这条语句（不是一条完整的 DDL）"}
	}

	switch strings.ToUpper(fields[0]) {
	case "CREATE":
		if len(fields) > 1 && strings.EqualFold(fields[1], "INDEX") {
			tbl := tableFromCreateIndex(fields)
			return DDLDecision{Class: DDLAllow, Table: tbl, SQL: raw,
				Reason: "加索引属于白名单四类，跟随"}
		}
		return DDLDecision{Class: DDLReject,
			Reason: "建表/建视图不跟随：表结构由 EnsureSchema 按 catalog 建，DDL 只跟随加列/放宽/加索引/删列"}
	case "DROP":
		return DDLDecision{Class: DDLReject,
			Reason: "DROP 不在白名单：会丢数据，必须人工确认后在源/目标两侧分别处理"}
	case "TRUNCATE":
		return DDLDecision{Class: DDLReject,
			Reason: "TRUNCATE 不在白名单：行变更我们按 DELETE 事件跟随，DDL 形态的截断不照搬"}
	case "RENAME":
		return DDLDecision{Class: DDLReject,
			Reason: "重命名不在白名单：目标端改名会让位点与表名对不上"}
	case "ALTER":
		if len(fields) < 3 || !strings.EqualFold(fields[1], "TABLE") {
			return DDLDecision{Class: DDLReject, Reason: "只支持 ALTER TABLE 形态"}
		}
		tbl := parseTableRef(fields[2])
		rest := strings.Join(fields[3:], " ")
		action := strings.ToUpper(firstWord(fields[3:]))
		switch action {
		case "ADD":
			if len(fields) > 4 {
				next := strings.ToUpper(trimBackticks(fields[4]))
				switch next {
				case "UNIQUE":
					return DDLDecision{Class: DDLConfirm, Table: tbl, SQL: raw,
						Reason: "加唯一索引会改变 upsert 的「重复」判据，必须人确认（确认后再执行）"}
				case "PRIMARY":
					return DDLDecision{Class: DDLConfirm, Table: tbl, SQL: raw,
						Reason: "改主键会改变 upsert 的判据与 DELETE 的定位，必须人确认"}
				case "CONSTRAINT", "FOREIGN":
					return DDLDecision{Class: DDLReject, Table: tbl,
						Reason: "外键约束不在白名单：跨表约束在同步过程中必然短暂不成立"}
				case "COLUMN":
					return DDLDecision{Class: DDLAllow, Table: tbl, Column: addColumnName(fields), SQL: raw,
						Reason: "加列属于白名单四类，跟随（已存在则跳过）"}
				case "INDEX", "KEY", "FULLTEXT", "SPATIAL":
					return DDLDecision{Class: DDLAllow, Table: tbl, SQL: raw,
						Reason: "加索引属于白名单四类，跟随（已存在则跳过）"}
				}
			}
			return DDLDecision{Class: DDLAllow, Table: tbl, Column: addColumnName(fields), SQL: raw,
				Reason: "加列属于白名单四类，跟随（已存在则跳过）"}
		case "MODIFY", "CHANGE":
			col, newType, renamed, ok := parseModify(rest)
			if !ok {
				return DDLDecision{Class: DDLConfirm, Table: tbl, SQL: raw,
					Reason: "改列类型但读不出列名或新类型，必须人确认"}
			}
			if renamed {
				return DDLDecision{Class: DDLConfirm, Table: tbl, Column: col, NewType: newType, SQL: raw,
					Reason: "CHANGE 同时改了列名：目标端列名变了，位点/映射要对齐，必须人确认"}
			}
			// 是「放宽」还是「改窄」要看目标端**现在**的类型，光看语句判不了。
			return DDLDecision{Class: DDLConfirm, Table: tbl, Column: col, NewType: newType, SQL: raw,
				Reason: "改列类型：需要先读目标端现有类型，才能判断是放宽（可跟随）还是改窄（会截断）"}
		case "DROP":
			if len(fields) > 4 {
				switch strings.ToUpper(trimBackticks(fields[4])) {
				case "COLUMN":
					return DDLDecision{Class: DDLConfirm, Table: tbl, SQL: raw,
						Reason: "删列属于「可选删列」：删了目标端数据就回不来，必须人确认"}
				case "INDEX", "KEY":
					return DDLDecision{Class: DDLSkip, Table: tbl,
						Reason: "删索引不改变行数据，跳过不照搬（目标端索引由你自己管）"}
				}
			}
			return DDLDecision{Class: DDLReject, Table: tbl, Reason: "未能识别的 DROP 形态，停"}
		case "ALTER":
			// ALTER TABLE t ALTER COLUMN c SET DEFAULT ...
			return DDLDecision{Class: DDLSkip, Table: tbl,
				Reason: "改默认值不影响已同步的行，跳过"}
		case "ENGINE", "COMMENT", "CONVERT", "DEFAULT", "AUTO_INCREMENT", "ROW_FORMAT", "CHARACTER":
			return DDLDecision{Class: DDLSkip, Table: tbl, Reason: "表级属性变更与行数据无关，跳过"}
		}
		return DDLDecision{Class: DDLReject, Table: tbl,
			Reason: fmt.Sprintf("ALTER 的 %s 形态不在白名单（只跟随加列/放宽/加索引/删列）", action)}
	}
	_ = up
	return DDLDecision{Class: DDLReject,
		Reason: fmt.Sprintf("语句类型 %q 不在白名单（只跟随加列/放宽长度/加索引/删列）", fields[0])}
}

// addColumnName 从 "ALTER TABLE t ADD [COLUMN] c type…" 里取列名。
func addColumnName(fields []string) string {
	for i, f := range fields {
		if strings.EqualFold(f, "ADD") && i+1 < len(fields) {
			j := i + 1
			if strings.EqualFold(trimBackticks(fields[j]), "COLUMN") {
				j++
			}
			if j < len(fields) {
				return trimBackticks(fields[j])
			}
		}
	}
	return ""
}

func firstWord(fields []string) string {
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

func trimBackticks(s string) string { return strings.Trim(s, "`") }

// parseTableRef 解析 "`db`.`t`" / "t" 形态的表名。
func parseTableRef(tok string) source.TableRef {
	tok = strings.TrimSuffix(strings.TrimSuffix(tok, ","), ";")
	parts := strings.Split(tok, ".")
	switch len(parts) {
	case 2:
		return source.TableRef{Schema: trimBackticks(parts[0]), Table: trimBackticks(parts[1])}
	default:
		return source.TableRef{Table: trimBackticks(tok)}
	}
}

func tableFromCreateIndex(fields []string) source.TableRef {
	for i, f := range fields {
		if strings.EqualFold(f, "ON") && i+1 < len(fields) {
			return parseTableRef(fields[i+1])
		}
	}
	return source.TableRef{}
}

// parseModify 从 "MODIFY [COLUMN] c type…" / "CHANGE [COLUMN] old new type…" 里
// 取出列名、新类型，以及 CHANGE 里是否真的改了列名。
func parseModify(rest string) (column, newType string, renamed, ok bool) {
	fields := strings.Fields(rest)
	if len(fields) < 3 {
		return "", "", false, false
	}
	verb := strings.ToUpper(fields[0])
	i := 1
	if i < len(fields) && strings.EqualFold(fields[i], "COLUMN") {
		i++
	}
	if verb == "CHANGE" {
		if i+2 >= len(fields) {
			return "", "", false, false
		}
		oldName, newName := trimBackticks(fields[i]), trimBackticks(fields[i+1])
		typ := strings.Join(typeWords(fields[i+2:]), " ")
		if typ == "" {
			return "", "", false, false
		}
		return newName, typ, !strings.EqualFold(oldName, newName), true
	}
	if i+1 >= len(fields) {
		return "", "", false, false
	}
	col := trimBackticks(fields[i])
	typ := strings.Join(typeWords(fields[i+1:]), " ")
	if typ == "" {
		return "", "", false, false
	}
	return col, typ, false, true
}

// DecideColumnChange 用目标端现有类型判断「跟随还是拒绝」。
//
// 允许：同族变长（varchar(64)→varchar(128)）、整型家族往上（int→bigint）、
// 同类型只改可空性/默认值。拒绝：变短、换成别的家族（会截断或报错）。
func DecideColumnChange(oldType, newType string) DDLDecision {
	if isWidening(oldType, newType) {
		return DDLDecision{Class: DDLAllow,
			Reason: fmt.Sprintf("放宽长度（%s → %s）属于白名单四类，跟随", oldType, newType)}
	}
	return DDLDecision{Class: DDLReject,
		Reason: fmt.Sprintf("改窄/换类型（%s → %s）不在白名单：目标端已有数据会被截断或报错；"+
			"确实要改就手工在目标端执行，或换一张表重搬", oldType, newType)}
}

// isWidening 判断 new 是否「放宽」：同族且更长，或整型家族往上走。
func isWidening(oldType, newType string) bool {
	if oldType == "" || newType == "" {
		return false
	}
	ob, oa := splitType(oldType)
	nb, na := splitType(newType)
	if ob != nb {
		return intRank(nb) > intRank(ob) && intRank(ob) > 0
	}
	ol, err1 := strconv.Atoi(oa)
	nl, err2 := strconv.Atoi(na)
	if err1 == nil && err2 == nil {
		return nl >= ol
	}
	return true // 同类型只改了 NOT NULL / DEFAULT / COMMENT 之类
}

// typeWords 截到第一个约束关键字为止。
func typeWords(fields []string) []string {
	var out []string
	for _, f := range fields {
		switch strings.ToUpper(f) {
		case "NOT", "NULL", "DEFAULT", "COMMENT", "FIRST", "AFTER", "AUTO_INCREMENT", "COLLATE", "UNSIGNED", "SIGNED":
			return out
		}
		out = append(out, f)
	}
	return out
}

// intRank 给整数家族排个大小，用来判断 int → bigint 这种「放宽」。
func intRank(base string) int {
	switch base {
	case "tinyint":
		return 1
	case "smallint":
		return 2
	case "mediumint":
		return 3
	case "int", "integer":
		return 4
	case "bigint":
		return 5
	}
	return 0
}

func renamesColumn(rest string) bool {
	fields := strings.Fields(rest)
	if len(fields) < 3 {
		return false
	}
	i := 0
	if strings.EqualFold(fields[i], "COLUMN") {
		i++
	}
	return i+1 < len(fields) && !strings.EqualFold(trimBackticks(fields[i]), trimBackticks(fields[i+1]))
}
