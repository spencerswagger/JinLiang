// Package mysqlcanon 提供 MySQL 侧的「规范行编码 + 内容指纹」。
//
// 为什么单独一个包：**源端和目标端都要算同一个指纹**才能自比（MySQL→MySQL 的对账）。
// 放在任何一个连接器里都会让另一个反向依赖；两个连接器各自实现一遍，就会像今晚
// 那几次一样——两边算法悄悄分叉，比对永远假红。
//
// 重要边界（接线前必须知道）：
//   - 指纹**只在同一引擎族内可比**（MySQL↔MySQL）。PG 侧用的是服务端
//     `sum(hashtextextended(t::text,0))`，与这里的客户端 sha256 永远不可能相等；
//     跨引擎比对必须走 `internal/verify` 的逐行/分块比对，**不要拿指纹跨端比**。
//     调用方先比 Scope（见下），不同就不要比。
//   - 列顺序按**列名排序**，不按表定义序：源/目标建表顺序不同也不会假红。
//   - 必须有主键：没有稳定顺序就没有可复现的指纹（明确报错，不给你一个会变的数）。
package mysqlcanon

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"hash"
	"sort"
	"strings"
	"time"
)

// Scope 标识这套指纹算法的「可比范围」。
//
// 两端 Scope 相同才能比：MySQL 源与 MySQL 目标都是它；PG 侧是另一套（不可比）。
const Scope = "mysql-canon-v1"

// Querier 是 *sql.DB / *sql.Conn 都满足的最小面。
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Fingerprint 返回「行数:内容哈希」。
//
// 哈希输入是**按列名排序后的规范编码**，所以：列顺序不同不影响、行序由主键固定。
func Fingerprint(ctx context.Context, q Querier, schema, table string) (string, error) {
	cols, err := columns(ctx, q, schema, table)
	if err != nil {
		return "", err
	}
	if len(cols) == 0 {
		return "", fmt.Errorf("表 %s 没有任何列（或读不到列定义）", table)
	}
	pk, err := primaryKey(ctx, q, schema, table)
	if err != nil {
		return "", err
	}
	if len(pk) == 0 {
		return "", fmt.Errorf("表 %s 没有主键：没有稳定顺序就没有可复现的指纹。"+
			"你能做的：① 给表加主键/唯一键；② 或改用行数 + 逐块抽样比对。"+
			"风险：无主键表的指纹每次都可能不同，强行比对会给出假的「不一致」", table)
	}

	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = quoteIdent(c)
	}
	order := make([]string, len(pk))
	for i, c := range pk {
		order[i] = quoteIdent(c)
	}
	query := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s",
		strings.Join(quoted, ", "), qualified(schema, table), strings.Join(order, ", "))

	rows, err := q.QueryContext(ctx, query)
	if err != nil {
		return "", fmt.Errorf("读 %s 失败: %w", table, err)
	}
	defer rows.Close()

	h := sha256.New()
	var n int64
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return "", err
		}
		WriteRow(h, cols, vals)
		n++
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return fmt.Sprintf("%d:%s", n, hex.EncodeToString(h.Sum(nil))), nil
}

// WriteRow 把一行按「列名=规范值」写进哈希（列名参与哈希，列序因此无关）。
func WriteRow(h hash.Hash, cols []string, vals []any) {
	for i, c := range cols {
		var v any
		if i < len(vals) {
			v = vals[i]
		}
		fmt.Fprintf(h, "%s=%s\x00", c, Value(v))
	}
	h.Write([]byte{0x1e}) // 行分隔
}

// Value 把驱动值转成规范文本（与 internal/verify 的口径同源思路：
// NULL 与空串要能区分、浮点用定点十进制、时间用微秒）。
func Value(v any) string {
	switch x := v.(type) {
	case nil:
		return `\N`
	case []byte:
		return "x'" + hex.EncodeToString(x) + "'"
	case string:
		return "s" + x
	case time.Time:
		return fmt.Sprintf("t%d", x.UTC().UnixMicro())
	case bool:
		if x {
			return "b1"
		}
		return "b0"
	case int64:
		return fmt.Sprintf("i%d", x)
	case int32, int16, int8, int:
		return fmt.Sprintf("i%v", x)
	case uint64:
		return fmt.Sprintf("u%d", x)
	case uint32, uint16, uint8, uint:
		return fmt.Sprintf("u%v", x)
	case float64:
		return fmt.Sprintf("f%.15g", x)
	case float32:
		return fmt.Sprintf("f%.15g", float64(x))
	default:
		return fmt.Sprintf("?%v", x)
	}
}

func columns(ctx context.Context, q Querier, schema, table string) ([]string, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT COLUMN_NAME FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`, schema, table)
	if err != nil {
		return nil, fmt.Errorf("读 %s 的列定义失败: %w", table, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	sort.Strings(out) // 列序无关：按名字排序
	return out, rows.Err()
}

func primaryKey(ctx context.Context, q Querier, schema, table string) ([]string, error) {
	rows, err := q.QueryContext(ctx, `
		SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'
		ORDER BY ORDINAL_POSITION`, schema, table)
	if err != nil {
		return nil, fmt.Errorf("读 %s 的主键失败: %w", table, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func quoteIdent(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

func qualified(schema, table string) string {
	if schema == "" {
		return quoteIdent(table)
	}
	return quoteIdent(schema) + "." + quoteIdent(table)
}
