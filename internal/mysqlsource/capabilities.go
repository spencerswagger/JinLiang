package mysqlsource

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/spencerswagger/jinliang/internal/mysqlcanon"
	"github.com/spencerswagger/jinliang/internal/source"
)

// 引擎侧要的那几组能力，这里按「只读、不改行为」补齐。
//
// 为什么放在连接器而不是引擎：这些方法的签名里都有连接器的类型（位置、块边界、
// 指纹口径），引擎只要按能力接口断言即可（照 #51 的 stateCapable 写法），
// 契约（source.Source / sink.Sink）不用加方法。

// —— 1. LSN / ack 组：MySQL 没有服务端 ack ——
//
// PG 的 ack 是「源端可以丢掉这段 WAL」的强承诺；MySQL 的 binlog 由源端按保留策略
// 自己轮转，副本无话可说，所以这里只能是 no-op。用 no-op 而不是「假装成功」：
// 调用方看 SafeAckPoint 就知道「没有需要 ack 的东西」。

// Ack 是 no-op：MySQL 不需要向源端确认位点。
func (s *Source) Ack(pos source.Position) {}

// SafeAckPoint 返回「可以安全确认的位置」＝最后一次**已交付给下游**的事务位点。
// ok=false 表示还没有交付过任何事务（此时不该推进任何书签）。
//
// **全部安全性的依据（写死，改 ack 顺序前先读这段）**：
//
//	`Apply(nil)` ⇔ 目标端 `_jl_meta.position` 已等于该事务的提交位点
//
// 换句话说：只有「下游返回 nil」才允许把 ack 点推进到这个位置。
// 反过来，任何「Apply 还没返回/返回了错误」的时刻，ack 都必须停在上一个已确认点——
// 否则源端会清掉还能重放的变更流水，目标端一回滚就永久缺数据。
// MySQL 目标端是 `staged_phases`（含 DDL 的批次有三个提交点），所以那边多一道
// `PositionDurable` 校验；PG 目标端是 `same_tx`，Apply 返回即成立。
func (s *Source) SafeAckPoint() (source.Position, bool) {
	p, ok := s.lastCommit.Load().(Position)
	if !ok || p.IsZero() {
		return nil, false
	}
	return p, true
}

// LastCommit 与 SafeAckPoint 同值：MySQL 侧没有「已 ack 位置」与「已提交位置」之分。
func (s *Source) LastCommit() source.Position {
	p, ok := s.SafeAckPoint()
	if !ok {
		return nil
	}
	return p
}

// markCommitted 由 RunStream/Stream 在**下游成功处理完一笔事务**后调用。
func (s *Source) markCommitted(p Position) { s.lastCommit.Store(p) }

// —— 2. 分块快照组 ——

// Chunk 是一个快照块（与 pgsource.Chunk 同形状，方便引擎共用一套块记账）。
type Chunk struct {
	Table source.TableRef
	No    int
	Rows  [][]any
	From  string // 本块的 keyset 边界（供人核对 + 块进度标记）
	To    string
}

// ChunkHandler 消费一个块；返回错误即中止快照。
type ChunkHandler func(ctx context.Context, c Chunk) error

func (s *Source) scanRowsAny(ctx context.Context, conn *sql.Conn, q string, args []any, t source.Table) ([][]any, error) {
	rows, err := conn.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	names, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out [][]any
	for rows.Next() {
		row, err := scanRow(rows, names, t)
		if err != nil {
			return nil, err
		}
		out = append(out, toAnyRow(row))
	}
	return out, rows.Err()
}

// keyString 是块边界（_jl_chunk.range_to）的编码：JSON 数组，能原样还原成比较参数。
// 不用逗号拼接：含逗号的键会被切错，续跑就变成读重/读漏。
func keyString(keys []source.Value) string { return EncodeCursor(keys) }

// —— 3. 定时增量组 ——

// MaxWatermark 读水位列的当前最大值（interval 模式的水位起点）。
func (s *Source) MaxWatermark(ctx context.Context, ref source.TableRef, column string) (string, error) {
	if strings.TrimSpace(column) == "" {
		return "", capabilityError("没有配置水位列",
			"定时增量靠水位列判断「这一轮从哪开始」，没配就没法算",
			"在 job 里给这张表配 incremental_column，或把模式改成 continuous（需要打开变更流水）",
			"不配水位列时该表只能整表重搬")
	}
	var v sql.NullString
	q := fmt.Sprintf("SELECT MAX(%s) FROM %s", quoteIdent(column), s.qualifiedOn(ref))
	if err := s.db.QueryRowContext(ctx, q).Scan(&v); err != nil {
		return "", capabilityError("水位列读不出来（"+column+"）",
			"这一列不存在，或类型不能比大小",
			"确认列名（区分大小写按源库规则），或换一个单调递增的列",
			"水位列不对时定时增量会漏数据或重复搬")
	}
	if !v.Valid {
		return "", nil // 空表：没有水位
	}
	return v.String, nil
}

// ReadWatermarkPage 按水位列取一页（keyset，不用 OFFSET）。
func (s *Source) ReadWatermarkPage(ctx context.Context, ref source.TableRef, column, after string, limit int) ([]source.Row, string, error) {
	if limit <= 0 {
		limit = 1000
	}
	if strings.TrimSpace(column) == "" {
		return nil, "", capabilityError("没有配置水位列", "定时增量需要它算每轮起点", "配 incremental_column", "不配就只能整表重搬")
	}
	q := fmt.Sprintf("SELECT * FROM %s WHERE %s > ? ORDER BY %s LIMIT %d",
		s.qualifiedOn(ref), quoteIdent(column), quoteIdent(column), limit)
	rows, err := s.db.QueryContext(ctx, q, after)
	if err != nil {
		return nil, "", capabilityError("水位分页读不出来", "水位列不存在或不可比大小", "确认列名与类型", "分页失败会导致该轮同步不完整")
	}
	defer rows.Close()
	names, err := rows.Columns()
	if err != nil {
		return nil, "", err
	}
	var out []source.Row
	next := after
	for rows.Next() {
		row, err := scanRow(rows, names, ref2table(ref))
		if err != nil {
			return nil, "", err
		}
		out = append(out, row)
		for i, n := range names {
			if strings.EqualFold(n, column) {
				next = fmt.Sprintf("%v", driverValue(row.Values[i]))
			}
		}
	}
	return out, next, rows.Err()
}

// TableRowsEstimate 给规划用的行数估算（information_schema，不锁表）。
func (s *Source) TableRowsEstimate(ctx context.Context, ref source.TableRef) (int64, error) {
	var n sql.NullInt64
	err := s.db.QueryRowContext(ctx,
		"SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
		s.schemaOf(ref), ref.Table).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("读 %s 的行数估算失败: %w", ref.Table, err)
	}
	return n.Int64, nil
}

// —— 4. 校验组 ——

// Count 目标端/源端的行数（对账用）。
func (s *Source) Count(ctx context.Context, ref source.TableRef) (int64, error) {
	var n int64
	if err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+s.qualifiedOn(ref)).Scan(&n); err != nil {
		return 0, fmt.Errorf("数 %s 行数失败: %w", ref.Table, err)
	}
	return n, nil
}

// FingerprintScope 标识本连接器指纹的可比范围（"mysql-canon-v1"）。
//
// **只在 MySQL↔MySQL 之间可比**：PG 侧是服务端 `sum(hashtextextended(...))`，
// 与这里的客户端 sha256 永远不可能相等。跨引擎比对必须走 `internal/verify` 的
// 逐行/分块比对，别拿指纹跨端比——那会每条都假红（今晚「假红」家族第三次）。
func (s *Source) FingerprintScope() string { return mysqlcanon.Scope }

// Fingerprint 返回「行数:内容哈希」，与 mysqlsink 用同一套规范编码（可自比）。
// 列按**列名排序**参与哈希，所以源/目标建表顺序不同不会假红；无主键明确拒绝。
func (s *Source) Fingerprint(ctx context.Context, ref source.TableRef) (string, error) {
	return mysqlcanon.Fingerprint(ctx, s.db, s.schemaOf(ref), ref.Table)
}

// capabilityError 统一四段式：发生了什么 / 为什么 / 你能做什么 / 风险。
// 缺能力时给这个，而不是 panic 或静默返回空值。
func capabilityError(what, why, next, risk string) error {
	return fmt.Errorf("%s：%s。你能做的：%s。风险：%s", what, why, next, risk)
}

func ref2table(ref source.TableRef) source.Table { return source.Table{TableRef: ref} }

func toAnyRow(r source.Row) []any {
	out := make([]any, len(r.Values))
	for i, v := range r.Values {
		out[i] = driverValue(v)
	}
	return out
}

var _ = time.Now
var _ sync.Mutex

// schemaOf / qualifiedOn 是给「只带表名」的 TableRef 补上连接库名的两个小工具
// （MySQL 源没有 schema 层，scope.Schema 就是库名）。
func (s *Source) schemaOf(ref source.TableRef) string {
	if ref.Schema != "" {
		return ref.Schema
	}
	return s.ep.Database
}

func (s *Source) qualifiedOn(ref source.TableRef) string {
	return quoteIdent(s.schemaOf(ref)) + "." + quoteIdent(ref.Table)
}
