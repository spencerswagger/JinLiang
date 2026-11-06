package mysqlsink

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/spencerswagger/jinliang/internal/source"
)

// InnoDB 的字节上限（utf8mb4 每字符最多 4 字节）：
//   - 单列索引 / 单列长度上限：3072 字节（DYNAMIC/COMPRESSED 行格式 + innodb_large_prefix=ON）
//   - 老行格式（REDUNDANT/COMPACT）或 large_prefix=OFF：767 字节
//
// 「超了」不阻断——用户可能就是要这个组合、也可能索引只取前缀；但必须说清
// 哪张表哪一列、估算多少字节、上限多少、怎么改，否则一条 1MB 的 varchar 会在
// 建索引那一步炸出一句英文驱动错误（docs/ROADMAP.md B3）。
const (
	indexLimitLargePrefix = 3072
	indexLimitLegacy      = 767
	utf8mb4MaxBytes       = 4
)

// WidthIssue 是一条「utf8mb4 下可能超限」的警告。
type WidthIssue struct {
	Schema string
	Table  string
	Column string
	Type   string // 列类型文本，如 varchar(1000)
	Where  string // column / index
	Bytes  int    // 估算字节数
	Limit  int    // 上限
	Advice string // 怎么改（人话 + 具体建议）
	Index  string // Where==index 时的索引名
}

// String 让这条警告能直接进日志/预检。
func (w WidthIssue) String() string {
	where := "列"
	if w.Where == "index" {
		where = "索引 " + w.Index
	}
	return fmt.Sprintf("%s.%s 的%s %s 在 utf8mb4 下估算 %d 字节，超过上限 %d 字节：%s",
		w.Schema, w.Table, where, w.Column, w.Bytes, w.Limit, w.Advice)
}

// WidthOptions 决定用哪套上限算。
type WidthOptions struct {
	// LargePrefix 对应 innodb_large_prefix + DYNAMIC/COMPRESSED 行格式（8.0 默认 true）。
	LargePrefix bool
	// CharsetBytes 是「一个字符最多几字节」：utf8mb4=4，utf8mb3=3，latin1=1。
	CharsetBytes int
}

// DefaultWidthOptions 是 8.0/8.4/9.x 的默认：DYNAMIC 行格式、utf8mb4。
func DefaultWidthOptions() WidthOptions {
	return WidthOptions{LargePrefix: true, CharsetBytes: utf8mb4MaxBytes}
}

func (o WidthOptions) limit() int {
	if o.LargePrefix {
		return indexLimitLargePrefix
	}
	return indexLimitLegacy
}

func (o WidthOptions) perChar() int {
	if o.CharsetBytes <= 0 {
		return utf8mb4MaxBytes
	}
	return o.CharsetBytes
}

// CheckRowWidth 只对 catalog 里的列做估算（不碰数据库），因此可以在 plan 阶段跑。
//
// 只报「字符类列按最大字符宽度可能超限」的情况：text/blob/json 在 InnoDB 里行内只有
// 12–20 字节指针，不该按声明长度算——把它们算进来会造成满屏假警告。
func CheckRowWidth(cat source.Catalog, opt WidthOptions) []WidthIssue {
	limit := opt.limit()
	var out []WidthIssue
	for _, t := range cat.Tables {
		for _, c := range t.Columns {
			n, ok := maxBytesForType(c.Type, opt.perChar())
			if !ok || n <= limit {
				continue
			}
			out = append(out, WidthIssue{
				Schema: t.Schema, Table: t.Table, Column: c.Name, Type: c.Type,
				Where: "column", Bytes: n, Limit: limit,
				Advice: fmt.Sprintf("把 %s 缩短到 %d 个字符以内，或改成 text（行内只存指针）；"+
					"如果只是要建索引，可以只取前缀：INDEX (%s(%d))",
					c.Type, limit/opt.perChar(), c.Name, limit/opt.perChar()),
			})
		}
	}
	return out
}

// CheckIndexWidths 查目标端的索引定义，算前缀长度是否超限（需要连库）。
func (s *Sink) CheckIndexWidths(ctx context.Context, schema string) ([]WidthIssue, error) {
	if s.db == nil {
		return nil, fmt.Errorf("没有目标库连接，无法检查索引长度")
	}
	// STATISTICS 没有 CHARACTER_SET_NAME：按 COLLATION_NAME join COLLATIONS 取字符集。
	rows, err := s.db.QueryContext(ctx, `
		SELECT st.TABLE_NAME, st.INDEX_NAME, st.COLUMN_NAME, COALESCE(st.SUB_PART, 0),
		       COALESCE(co.CHARACTER_SET_NAME, ''), COALESCE(st.COLLATION, ''),
		       COALESCE(c.DATA_TYPE, '')
		FROM information_schema.STATISTICS st
		LEFT JOIN information_schema.COLLATIONS co ON co.COLLATION_NAME = st.COLLATION
		LEFT JOIN information_schema.COLUMNS c
		       ON c.TABLE_SCHEMA = st.TABLE_SCHEMA AND c.TABLE_NAME = st.TABLE_NAME AND c.COLUMN_NAME = st.COLUMN_NAME
		WHERE st.TABLE_SCHEMA = ? AND st.TABLE_NAME NOT LIKE '\_jl\_%'
		ORDER BY st.TABLE_NAME, st.INDEX_NAME, st.SEQ_IN_INDEX`, schema)
	if err != nil {
		return nil, fmt.Errorf("读索引定义失败: %w", err)
	}
	defer rows.Close()

	limit := s.width.limit()
	var out []WidthIssue
	for rows.Next() {
		var table, index, column, charset, collation, dataType string
		var subPart int
		if err := rows.Scan(&table, &index, &column, &subPart, &charset, &collation, &dataType); err != nil {
			return nil, err
		}
		perChar := s.width.perChar()
		if charset != "" && !strings.HasPrefix(strings.ToLower(charset), "utf8mb4") {
			// 非 utf8mb4 的列：按它自己的字符集算（latin1=1，utf8mb3=3），别用 4 吓人。
			perChar = charsetMaxBytes(charset)
		}
		var bytes int
		if subPart > 0 {
			bytes = subPart * perChar
		} else if n, ok := maxBytesForType(dataType, perChar); ok {
			bytes = n
		} else {
			continue
		}
		if bytes <= limit {
			continue
		}
		out = append(out, WidthIssue{
			Schema: schema, Table: table, Column: column, Type: dataType,
			Where: "index", Index: index, Bytes: bytes, Limit: limit,
			Advice: fmt.Sprintf("该索引列估算 %d 字节超限：把索引改成前缀索引（%s(%d)），"+
				"或缩列宽 / 改 text + 前缀索引；行格式是 COMPACT/REDUNDANT 时上限只有 767 字节",
				bytes, column, limit/perChar),
		})
	}
	return out, rows.Err()
}

func charsetMaxBytes(charset string) int {
	switch strings.ToLower(charset) {
	case "latin1", "ascii", "binary":
		return 1
	case "utf8", "utf8mb3":
		return 3
	case "gbk", "gb2312", "gb18030":
		return 4 // gb18030 最坏 4 字节
	default:
		return utf8mb4MaxBytes
	}
}

// maxBytesForType 估算一个列在行内/索引里的最坏字节数。
//
// 只处理字符与定长类型：text/blob/json/geometry 在 InnoDB 里行内只有指针，
// 返回 ok=false 让调用方跳过（算进来只会产生假警告）。
func maxBytesForType(typ string, perChar int) (int, bool) {
	base, args := splitType(typ)
	switch base {
	case "varchar", "varbinary":
		if base == "varbinary" {
			if n, err := strconv.Atoi(args); err == nil {
				return n, true
			}
			return 0, false
		}
		if n, err := strconv.Atoi(args); err == nil {
			return n*perChar + 2, true // +2 长度前缀
		}
		return 0, false
	case "char", "binary":
		if base == "binary" {
			if n, err := strconv.Atoi(args); err == nil {
				return n, true
			}
			return 0, false
		}
		if n, err := strconv.Atoi(args); err == nil {
			return n * perChar, true
		}
		return 0, false
	case "tinytext":
		return 255, true
	case "text":
		return 0, false
	case "mediumtext", "longtext", "tinyblob", "blob", "mediumblob", "longblob",
		"json", "geometry", "point", "linestring", "polygon", "vector":
		return 0, false
	case "decimal", "numeric":
		return 0, false
	}
	return 0, false
}

// splitType 把 "varchar(64)" 拆成 ("varchar","64")；"int unsigned" → ("int","")。
func splitType(t string) (string, string) {
	t = strings.TrimSpace(strings.ToLower(t))
	if i := strings.IndexByte(t, '('); i >= 0 {
		base := strings.TrimSpace(t[:i])
		args := strings.TrimSpace(strings.TrimSuffix(t[i+1:], ")"))
		return base, args
	}
	if i := strings.IndexByte(t, ' '); i >= 0 {
		return strings.TrimSpace(t[:i]), ""
	}
	return t, ""
}

// tableWidthOptions 用目标表**实际的默认字符集**决定每字符几字节。
//
// 口径必须和 CheckIndexWidths 一致：那边按列的真实字符集算，这边要是统一按 4 字节，
// latin1 的表会被多报一堆假警告（假警告多了用户就不看了，等于没有警告）。
func (s *Sink) tableWidthOptions(ctx context.Context, t source.TableRef) WidthOptions {
	opt := s.width
	if s.db == nil {
		return opt
	}
	var charset sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT co.CHARACTER_SET_NAME FROM information_schema.TABLES t
		LEFT JOIN information_schema.COLLATIONS co ON co.COLLATION_NAME = t.TABLE_COLLATION
		WHERE t.TABLE_SCHEMA = ? AND t.TABLE_NAME = ?`, s.schemaOf(t), t.Table).Scan(&charset)
	if err != nil || !charset.Valid || charset.String == "" {
		return opt
	}
	opt.CharsetBytes = charsetMaxBytes(charset.String)
	return opt
}

// widthChecks 把警告变成预检项（StatusWarn：只提示，不阻断）。
func (s *Sink) widthChecks(ctx context.Context, cat source.Catalog) []source.Check {
	var checks []source.Check
	for _, t := range cat.Tables {
		opt := s.tableWidthOptions(ctx, t.TableRef)
		single := source.Catalog{Tables: []source.Table{t}}
		for _, w := range CheckRowWidth(single, opt) {
			checks = append(checks, source.Check{
				ID:     "MY-W01",
				Title:  "utf8mb4 下列宽可能超过索引/列上限",
				Status: source.StatusWarn,
				Detail: w.String(),
				SQL:    fmt.Sprintf("ALTER TABLE `%s`.`%s` MODIFY `%s` VARCHAR(%d);", w.Schema, w.Table, w.Column, w.Limit/opt.perChar()),
				Who:    "you",
				Code:   "E4001",
			})
		}
	}
	if s.db != nil {
		for _, schema := range catalogSchemas(cat) {
			issues, err := s.CheckIndexWidths(ctx, schema)
			if err != nil {
				continue // 预检不该因为读不到索引就把整个 run 打断
			}
			for _, w := range issues {
				checks = append(checks, source.Check{
					ID:     "MY-W02",
					Title:  "utf8mb4 下索引前缀超过上限",
					Status: source.StatusWarn,
					Detail: w.String(),
					SQL:    fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP INDEX `%s`, ADD INDEX `%s` (`%s`(%d));", w.Schema, w.Table, w.Index, w.Index, w.Column, w.Limit/s.width.perChar()),
					Who:    "dba",
					Code:   "E4001",
				})
			}
		}
	}
	return checks
}

func catalogSchemas(cat source.Catalog) []string {
	seen := map[string]bool{}
	var out []string
	for _, t := range cat.Tables {
		if t.Schema != "" && !seen[t.Schema] {
			seen[t.Schema] = true
			out = append(out, t.Schema)
		}
	}
	return out
}

// probeWidthOptions 读目标端的行格式与 large_prefix，决定用 3072 还是 767。
func (s *Sink) probeWidthOptions(ctx context.Context) WidthOptions {
	opt := DefaultWidthOptions()
	if s.db == nil {
		return opt
	}
	var rowFormat string
	if err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(ROW_FORMAT),'') FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = DATABASE()`).Scan(&rowFormat); err == nil {
		rf := strings.ToLower(rowFormat)
		if rf == "compact" || rf == "redundant" {
			opt.LargePrefix = false
		}
	}
	// 5.7 才有 innodb_large_prefix（8.0 起恒为 ON 且不可配）。
	var largePrefix sql.NullString
	if err := s.db.QueryRowContext(ctx, "SELECT @@innodb_large_prefix").Scan(&largePrefix); err == nil {
		if largePrefix.Valid && (largePrefix.String == "0" || strings.EqualFold(largePrefix.String, "OFF")) {
			opt.LargePrefix = false
		}
	}
	return opt
}
