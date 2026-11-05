package mysqlsource

import "context"

// collation is one (charset, collation) pair.
type collation struct {
	Charset   string
	Collation string
}

// staticCollations covers the collations that show up as column defaults in
// practice (5.7 / 8.0 / 8.4 / 9.x). The authoritative source is
// information_schema.COLLATIONS, which the Source loads when it can; this table
// is the fallback so a stream never blocks on one extra query.
var staticCollations = map[uint64]collation{
	5:   {"latin1", "latin1_german1_ci"},
	8:   {"latin1", "latin1_swedish_ci"},
	11:  {"ascii", "ascii_general_ci"},
	28:  {"gbk", "gbk_chinese_ci"},
	33:  {"utf8mb3", "utf8mb3_general_ci"},
	45:  {"utf8mb4", "utf8mb4_general_ci"},
	46:  {"utf8mb4", "utf8mb4_bin"},
	47:  {"latin1", "latin1_bin"},
	63:  {"binary", "binary"},
	83:  {"utf8mb3", "utf8mb3_bin"},
	192: {"utf8mb3", "utf8mb3_unicode_ci"},
	224: {"utf8mb4", "utf8mb4_unicode_ci"},
	225: {"utf8mb4", "utf8mb4_icelandic_ci"},
	255: {"utf8mb4", "utf8mb4_0900_ai_ci"},
	256: {"utf8mb4", "utf8mb4_de_pb_0900_ai_ci"},
	303: {"utf8mb4", "utf8mb4_ja_0900_as_cs"},
	305: {"utf8mb4", "utf8mb4_0900_as_ci"},
	309: {"utf8mb4", "utf8mb4_0900_bin"},
}

// loadCollations reads the server's real collation table so Column.Charset /
// Column.Collation are the names the DBA sees, not ids.
func (s *Source) loadCollations(ctx context.Context) map[uint64]collation {
	out := make(map[uint64]collation, len(staticCollations))
	for id, c := range staticCollations {
		out[id] = c
	}
	if s.db == nil {
		return out
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT ID, COLLATION_NAME, CHARACTER_SET_NAME FROM information_schema.COLLATIONS`)
	if err != nil {
		return out
	}
	defer rows.Close()
	for rows.Next() {
		var id uint64
		var name, charset string
		if err := rows.Scan(&id, &name, &charset); err != nil {
			return out
		}
		out[id] = collation{Charset: charset, Collation: name}
	}
	return out
}

// lookupCollation resolves one collation id. 未知 id 返回空：契约里这两个字段
// 本来就允许为空（可空语义是「非字符类型或拿不到」），留空比编一个假名字诚实。
func lookupCollation(table map[uint64]collation, id uint64) (charset, name string) {
	if id == 0 {
		return "", ""
	}
	if c, ok := table[id]; ok {
		return c.Charset, c.Collation
	}
	if c, ok := staticCollations[id]; ok {
		return c.Charset, c.Collation
	}
	return "", ""
}
