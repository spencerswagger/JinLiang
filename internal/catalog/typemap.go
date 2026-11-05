package catalog

import (
	"strings"
)

// Engine names a database family this tool speaks.
type Engine string

// Supported engines (first release).
const (
	Postgres Engine = "postgres"
	MySQL    Engine = "mysql"
)

// Loss says how much fidelity a mapping costs. The engine never silently
// downgrades: anything above Lossless is surfaced in `plan` and must be
// acknowledged by the user (DESIGN.md §8).
type Loss uint8

// Loss levels, ordered by severity.
const (
	Lossless     Loss = iota // 完全等价
	Lossy                    // 精度/范围/时区可能变，但不报错
	NeedsConfirm             // 语义不同，必须用户确认
	Unsupported              // 字典未覆盖；上层按 text 兜底并提示，不拒绝
)

// String renders the loss level for reports.
func (l Loss) String() string {
	switch l {
	case Lossless:
		return "lossless"
	case Lossy:
		return "lossy"
	case NeedsConfirm:
		return "needs_confirm"
	case Unsupported:
		return "unsupported"
	}
	return "unknown"
}

// Mapping is the result of mapping one source column type.
type Mapping struct {
	Target string `json:"target"`
	Loss   Loss   `json:"loss"`
	Note   string `json:"note,omitempty"`
}

// normalizeType lowercases and splits a SQL type into base name and arguments.
// Examples: "VARCHAR(255)" -> ("varchar","255"); "bigint unsigned" ->
// ("bigint unsigned",""); "numeric(12, 2)" -> ("numeric","12, 2").
func normalizeType(t string) (base, args string) {
	s := strings.ToLower(strings.TrimSpace(t))
	if i := strings.IndexByte(s, '('); i >= 0 {
		if j := strings.LastIndexByte(s, ')'); j > i {
			return strings.TrimSpace(s[:i]), strings.TrimSpace(s[i+1 : j])
		}
		return strings.TrimSpace(s[:i]), ""
	}
	return strings.Join(strings.Fields(s), " "), ""
}

// Map returns the target type for sourceType when moving from one engine to
// another. ok is false when the pair is not covered by the dictionary and the
// caller must fall back to a probe (create a shadow column and see).
func Map(from, to Engine, sourceType string) (Mapping, bool) {
	if from == to {
		return Mapping{Target: sourceType, Loss: Lossless}, true
	}
	base, args := normalizeType(sourceType)

	// Array types (PostgreSQL only) carry their element type in args.
	if from == Postgres && strings.HasSuffix(base, "[]") {
		return Mapping{Target: "json", Loss: NeedsConfirm, Note: "数组落地为 JSON 数组，元素类型需人工确认"}, true
	}

	// MySQL tinyint(1) 在生态里被当作布尔用，但 2..127 会溢出：
	// 必须单独拦出来让用户确认，不能悄悄映射成 smallint。
	if from == MySQL && to == Postgres && base == "tinyint" && args == "1" {
		return Mapping{Target: "boolean", Loss: NeedsConfirm, Note: "tinyint(1) 常被当作布尔，但 2..127 会溢出"}, true
	}

	key := string(from) + "->" + string(to) + ":" + base
	rules := map[string]Mapping{
		// ---------- PostgreSQL -> MySQL ----------
		"postgres->mysql:boolean":                     {"tinyint(1)", Lossless, ""},
		"postgres->mysql:bool":                        {"tinyint(1)", Lossless, ""},
		"postgres->mysql:smallint":                    {"smallint", Lossless, ""},
		"postgres->mysql:int2":                        {"smallint", Lossless, ""},
		"postgres->mysql:integer":                     {"int", Lossless, ""},
		"postgres->mysql:int":                         {"int", Lossless, ""},
		"postgres->mysql:int4":                        {"int", Lossless, ""},
		"postgres->mysql:bigint":                      {"bigint", Lossless, ""},
		"postgres->mysql:int8":                        {"bigint", Lossless, ""},
		"postgres->mysql:smallserial":                 {"smallint", Lossless, ""},
		"postgres->mysql:serial":                      {"int", Lossless, ""},
		"postgres->mysql:bigserial":                   {"bigint", Lossless, ""},
		"postgres->mysql:real":                        {"float", Lossy, "MySQL FLOAT 为单精度，值域更窄"},
		"postgres->mysql:float4":                      {"float", Lossy, "MySQL FLOAT 为单精度，值域更窄"},
		"postgres->mysql:double precision":            {"double", Lossless, ""},
		"postgres->mysql:float8":                      {"double", Lossless, ""},
		"postgres->mysql:money":                       {"decimal(19,2)", NeedsConfirm, "货币符号与小数位需确认"},
		"postgres->mysql:date":                        {"date", Lossless, ""},
		"postgres->mysql:timestamp":                   {"datetime(6)", Lossy, "丢弃时区信息，按会话时区存储"},
		"postgres->mysql:timestamp without time zone": {"datetime(6)", Lossy, "丢弃时区信息"},
		"postgres->mysql:timestamptz":                 {"datetime(6)", NeedsConfirm, "建议统一按 UTC 存；读取方需知道"},
		"postgres->mysql:timestamp with time zone":    {"datetime(6)", NeedsConfirm, "建议统一按 UTC 存"},
		"postgres->mysql:time":                        {"time(6)", Lossless, ""},
		"postgres->mysql:timetz":                      {"time(6)", NeedsConfirm, "时区偏移被丢弃"},
		"postgres->mysql:interval":                    {"varchar(64)", NeedsConfirm, "间隔无等价类型，转文本"},
		"postgres->mysql:uuid":                        {"char(36)", Lossless, ""},
		"postgres->mysql:inet":                        {"varchar(45)", Lossless, ""},
		"postgres->mysql:cidr":                        {"varchar(45)", Lossless, ""},
		"postgres->mysql:macaddr":                     {"varchar(17)", Lossless, ""},
		"postgres->mysql:json":                        {"json", Lossy, "同 jsonb：MySQL 侧大整数与高精度小数按 double 失真；键序与重复键处理不同"},
		"postgres->mysql:jsonb":                       {"json", Lossy, "键序与重复键处理不同；MySQL 侧大整数与高精度小数会按 double 失真"},
		"postgres->mysql:text":                        {"text", Lossless, ""},
		"postgres->mysql:bytea":                       {"longblob", Lossless, ""},
		"postgres->mysql:xml":                         {"text", NeedsConfirm, "按文本存"},
		"postgres->mysql:tsvector":                    {"text", NeedsConfirm, "检索索引用途，落地为文本"},

		// ---------- MySQL -> PostgreSQL ----------
		"mysql->postgres:tinyint":            {"smallint", Lossless, ""},
		"mysql->postgres:tinyint(1)":         {"boolean", NeedsConfirm, "tinyint(1) 常被当作布尔，但 2..127 会溢出"},
		"mysql->postgres:mediumint":          {"integer", Lossless, ""},
		"mysql->postgres:smallint":           {"smallint", Lossless, ""},
		"mysql->postgres:int":                {"integer", Lossless, ""},
		"mysql->postgres:integer":            {"integer", Lossless, ""},
		"mysql->postgres:bigint":             {"bigint", Lossless, ""},
		"mysql->postgres:int unsigned":       {"bigint", Lossless, ""},
		"mysql->postgres:bigint unsigned":    {"numeric(20,0)", NeedsConfirm, "超出 bigint 上限，转 numeric"},
		"mysql->postgres:smallint unsigned":  {"integer", Lossless, ""},
		"mysql->postgres:tinyint unsigned":   {"smallint", Lossless, ""},
		"mysql->postgres:mediumint unsigned": {"integer", Lossless, ""},
		"mysql->postgres:float":              {"real", Lossy, "PG real 为单精度，语义接近"},
		"mysql->postgres:double":             {"double precision", Lossless, ""},
		"mysql->postgres:date":               {"date", Lossless, ""},
		"mysql->postgres:datetime":           {"timestamp", Lossy, "无时区，按会话时区解释"},
		"mysql->postgres:timestamp":          {"timestamptz", NeedsConfirm, "MySQL timestamp 自带会话时区语义"},
		"mysql->postgres:time":               {"time", Lossless, ""},
		"mysql->postgres:year":               {"integer", NeedsConfirm, "年份转整数"},
		"mysql->postgres:tinytext":           {"text", Lossless, ""},
		"mysql->postgres:text":               {"text", Lossless, ""},
		"mysql->postgres:mediumtext":         {"text", Lossless, ""},
		"mysql->postgres:longtext":           {"text", Lossless, ""},
		"mysql->postgres:binary":             {"bytea", Lossless, ""},
		"mysql->postgres:varbinary":          {"bytea", Lossless, ""},
		"mysql->postgres:tinyblob":           {"bytea", Lossless, ""},
		"mysql->postgres:blob":               {"bytea", Lossless, ""},
		"mysql->postgres:mediumblob":         {"bytea", Lossless, ""},
		"mysql->postgres:longblob":           {"bytea", Lossless, ""},
		"mysql->postgres:json":               {"jsonb", Lossy, "两边都会去重复键、键序不保证；MySQL 侧大整数与高精度小数已按 double 失真；字符串含 \\u0000 时 jsonb 拒绝，需该列改 text"},
		"mysql->postgres:enum":               {"text", NeedsConfirm, "落地为 text + CHECK，取值清单需人工确认"},
		"mysql->postgres:set":                {"text", NeedsConfirm, "落地为 text；如需数组语义需人工确认"},
		"mysql->postgres:geometry":           {"geometry", NeedsConfirm, "目标需 PostGIS 扩展"},
	}
	if m, ok := rules[key]; ok {
		return m, true
	}

	// 带长度的字符类型按长度决定是否溢出 MySQL 行宽（utf8mb4 单行 65535 字节）。
	switch base {
	case "varchar", "character varying":
		if to == MySQL && args != "" {
			if n := atoiSafe(args); n > 16383 {
				return Mapping{Target: "text", Loss: NeedsConfirm, Note: "长度超过 MySQL utf8mb4 行宽上限，改用 text"}, true
			}
			return Mapping{Target: "varchar(" + args + ")", Loss: Lossless}, true
		}
		return Mapping{Target: "text", Loss: NeedsConfirm, Note: "未给长度，目标端改为 text"}, true
	case "char", "character", "bpchar":
		if args != "" {
			return Mapping{Target: "char(" + args + ")", Loss: Lossless}, true
		}
		return Mapping{Target: "text", Loss: Lossy, Note: "未给长度"}, true
	case "numeric", "decimal", "dec":
		if args != "" {
			return Mapping{Target: "decimal(" + args + ")", Loss: Lossless}, true
		}
		return Mapping{Target: "decimal(65,30)", Loss: NeedsConfirm, Note: "未指定精度，按最大值落库，存储开销大"}, true
	case "bit":
		if args != "" {
			return Mapping{Target: "bit(" + args + ")", Loss: Lossless}, true
		}
		return Mapping{Target: "bit(1)", Loss: NeedsConfirm, Note: "未指定位数"}, true
	}

	return Mapping{Target: "text", Loss: Unsupported, Note: "字典未覆盖：默认按 text 落地，可人工指定类型"}, false
}

// atoiSafe parses a positive int prefix without importing strconv for one call.
func atoiSafe(s string) int {
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			break
		}
		n = n*10 + int(r-'0')
	}
	return n
}
