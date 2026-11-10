package mysqlsink

// 目标端列类型对账（引擎侧「按表暂停」恢复路径要用）。
//
// 为什么需要它：`EnsureSchema` 在 `on_exists=append` 下只**补缺列**，不会改列类型
// （改类型等于动用户的数据，不能悄悄做）。而引擎要判断「这张表的结构现在对齐了吗」——
// 不对齐就清空重搬 = 把源端的数值写进一个类型不匹配的列，MySQL 会**静默收敛**
// （`1.25` 存进 bigint 变成 `1`），然后引擎发一句「已恢复：整表重搬完成」。那是假恢复。
//
// 所以这里只回答一个问题：「按这套类型字典，目标端这几列**现在**是不是源端那几列该有的类型」。
// 判据用本包自己的 `targetType`（唯一一份映射），不在引擎里另写一套。

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/spencerswagger/jinliang/internal/source"
)

// ColumnsMatch 报告目标表的列类型是否与「源端这些列按类型字典映射过来」一致。
// 第二个返回值是给人看的差异说明（不一致时非空）。
//
// 表不存在 → 返回 (false, "目标端还没有这张表", nil)：由调用方决定要不要建
// （调用方用 `TableExists` 区分「不存在」与「存在但类型不对」）。
func (s *Sink) ColumnsMatch(ctx context.Context, ref source.TableRef, cols []source.Column) (bool, string, error) {
	have, err := s.columnTypes(ctx, ref)
	if err != nil {
		return false, "", err
	}
	if len(have) == 0 {
		return false, "目标端还没有这张表（或读不到它的列）", nil
	}
	var diffs []string
	for _, c := range cols {
		want := canonType(s.targetType(c))
		got, ok := have[c.Name]
		if !ok {
			diffs = append(diffs, fmt.Sprintf("列 %s：目标端没有这一列", c.Name))
			continue
		}
		if canonType(got) != want {
			diffs = append(diffs, fmt.Sprintf("列 %s：目标端是 %s，源端映射过来应是 %s", c.Name, got, want))
		}
	}
	if len(diffs) > 0 {
		return false, strings.Join(diffs, "；"), nil
	}
	return true, "", nil
}

// TableExists 报告目标端有没有这张表（与 ColumnsMatch 配套：不存在 ≠ 类型不对）。
func (s *Sink) TableExists(ctx context.Context, ref source.TableRef) (bool, error) {
	have, err := s.columnTypes(ctx, ref)
	if err != nil {
		return false, err
	}
	return len(have) > 0, nil
}

// columnTypes 读目标表的「列名 → column_type」（`bigint`、`decimal(12,2)` 这种带精度的文本）。
func (s *Sink) columnTypes(ctx context.Context, ref source.TableRef) (map[string]string, error) {
	schema := ref.Schema
	if schema == "" && s.ep.Database != "" {
		schema = s.ep.Database
	}
	q := `SELECT column_name, column_type FROM information_schema.columns
	      WHERE table_schema = ? AND table_name = ?`
	rows, err := s.db.QueryContext(ctx, q, schema, ref.Table)
	if err != nil {
		if err == sql.ErrNoRows {
			return map[string]string{}, nil
		}
		return nil, fmt.Errorf("读目标表 %s 的列类型失败: %w", ref.Table, err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return nil, err
		}
		out[name] = typ
	}
	return out, rows.Err()
}

// canonType 归一化类型文本，供比较用：`DECIMAL(12, 2)` ≡ `decimal(12,2)`；
// `bigint(20) unsigned` 这种带显示宽度/符号的写法去掉后缀（MySQL 8 对 int 不再显示宽度，
// 5.7 会，二进制比较会把同一类型判成不一致）。
func canonType(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, " ", "")
	s = strings.TrimSuffix(s, "unsigned")
	s = strings.TrimSuffix(s, "zerofill")
	if i := strings.Index(s, "("); i > 0 && (strings.HasPrefix(s, "bigint") ||
		strings.HasPrefix(s, "int") || strings.HasPrefix(s, "smallint") || strings.HasPrefix(s, "tinyint") ||
		strings.HasPrefix(s, "mediumint")) {
		// int 系的显示宽度不改变类型身份（5.7 有、8.0 没有）。
		return s[:i] + s[strings.Index(s, ")"):]
	}
	return s
}
