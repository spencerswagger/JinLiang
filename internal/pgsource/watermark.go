package pgsource

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/spencerswagger/jinliang/internal/pgwire"
	"github.com/spencerswagger/jinliang/internal/source"
)

// Watermark 是 interval 模式的水位：**(水位列值, 主键)**。
//
// 为什么要复合：单列水位会漏「同一时刻写入的多行」——上一轮读到 t=10:00:00 的第 3 行，
// 下一轮从 t > 10:00:00 开始，同秒的第 4 行就永远读不到。加上主键做次级比较才不漏。
// 两个值都按**文本**存：比较由数据库按列真实类型做（`$1::<type>`），Go 侧不做数值/时间比较。
type Watermark struct {
	Wm string `json:"wm"`
	PK string `json:"pk,omitempty"` // 复合主键时用 \x1f 连接（与 keyset 游标同一编码）
}

// String 序列化成能存进 `_jl_meta.watermark` 的形态（JSON，人可读、可手工修）。
func (w Watermark) String() string {
	b, _ := json.Marshal(w)
	return string(b)
}

// IsZero 表示「还没同步过」。
func (w Watermark) IsZero() bool { return w.Wm == "" }

// ParseWatermark 解析 `_jl_meta.watermark` 里的单个表的水位。
func ParseWatermark(s string) (Watermark, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Watermark{}, false
	}
	var w Watermark
	if err := json.Unmarshal([]byte(s), &w); err != nil {
		return Watermark{}, false
	}
	return w, !w.IsZero()
}

// ReadWatermarkPage 读一页「比水位新」的行（keyset 分页，永远不下沉到 OFFSET）。
//
// lookback 非空时（例如 "5m"），本轮起点回退这么多——用来吸收「提交晚于写入」的行
// （长事务、复制延迟）。重读的部分靠目标端 upsert 幂等吸收，不会产生重复。
// 回退只作用于本轮的**第一页**：之后按 (wm, pk) 前进，否则会在一页里反复读到旧行。
func (s *Source) ReadWatermarkPage(ctx context.Context, ref source.TableRef, wmColumn string,
	from Watermark, lookback string, limit int64) (rows [][]any, next Watermark, done bool, err error) {

	key := ref.Schema + "." + ref.Table
	ti := s.tables[key]
	if ti == nil {
		return nil, Watermark{}, false, fmt.Errorf("内部错误：任务范围里没有 %s", ref.Table)
	}
	wmIdx, ok := ti.colIndex[wmColumn]
	if !ok {
		return nil, Watermark{}, false, fmt.Errorf("表 %s 没有水位列 %s", ref.Table, wmColumn)
	}
	if limit <= 0 {
		limit = 10000
	}

	cols := make([]string, 0, len(ti.cols))
	for _, c := range ti.cols {
		cols = append(cols, pgwire.Quote(c))
	}
	pk := ti.pk
	if len(pk) == 0 {
		return nil, Watermark{}, false, fmt.Errorf("表 %s 没有主键，定时增量只能整表重搬", ref.Table)
	}
	var pkCols []string
	for _, p := range pk {
		pkCols = append(pkCols, pgwire.Quote(p))
	}

	sel := "SELECT " + strings.Join(cols, ", ") + " FROM " + pgwire.Qualified(ref.Schema, ref.Table)
	var args []any
	switch {
	case from.IsZero():
		// 没有水位（第一次跑这张表）：由调用方先做全量；这里返回空页，避免误读。
		return nil, Watermark{}, true, nil
	case lookback != "" && from.PK == "":
		// 本轮第一页：回退 lookback，不带主键比较（重复由 upsert 吸收）。
		sel += fmt.Sprintf(" WHERE %s >= $1::%s - $2::interval", pgwire.Quote(wmColumn), ti.types[wmIdx])
		args = append(args, from.Wm, lookback)
	default:
		// 后续页：按 (wm, pk) 前进。复合主键用行比较，单列主键等价于大于。
		var holders []string
		args = append(args, from.Wm)
		holders = append(holders, fmt.Sprintf("$1::%s", ti.types[wmIdx]))
		for i, p := range pk {
			idx := ti.colIndex[p]
			args = append(args, splitWatermarkPK(from.PK)[i])
			holders = append(holders, fmt.Sprintf("$%d::%s", i+2, ti.types[idx]))
		}
		sel += fmt.Sprintf(" WHERE (%s, %s) > (%s)",
			pgwire.Quote(wmColumn), strings.Join(pkCols, ", "), strings.Join(holders, ", "))
	}
	sel += fmt.Sprintf(" ORDER BY %s, %s LIMIT %d",
		pgwire.Quote(wmColumn), strings.Join(pkCols, ", "), limit)

	rs, err := s.conn.Query(ctx, sel, args...)
	if err != nil {
		return nil, Watermark{}, false, fmt.Errorf("读 %s 的水位增量失败: %w", ref.Table, err)
	}
	defer rs.Close()

	var out [][]any
	var lastVals []any
	for rs.Next() {
		vals, err := rs.Values()
		if err != nil {
			return nil, Watermark{}, false, err
		}
		out = append(out, vals)
		lastVals = vals
	}
	if err := rs.Err(); err != nil {
		return nil, Watermark{}, false, err
	}
	if len(lastVals) == 0 {
		return out, from, true, nil
	}
	parts := make([]string, 0, len(pk))
	for _, p := range pk {
		idx := ti.colIndex[p]
		parts = append(parts, cursorText(lastVals[idx]))
	}
	next = Watermark{Wm: cursorText(lastVals[wmIdx]), PK: joinCursor(parts)}
	return out, next, len(out) < int(limit), nil
}

// MaxWatermark 读这张表当前的「最大 (wm, pk)」，用于全量搬完之后把水位对齐到表尾，
// 避免下一轮从零重扫整表。
func (s *Source) MaxWatermark(ctx context.Context, ref source.TableRef, wmColumn string) (Watermark, error) {
	key := ref.Schema + "." + ref.Table
	ti := s.tables[key]
	if ti == nil {
		return Watermark{}, fmt.Errorf("内部错误：任务范围里没有 %s", ref.Table)
	}
	var pkCols []string
	for _, p := range ti.pk {
		pkCols = append(pkCols, pgwire.Quote(p))
	}
	q := fmt.Sprintf("SELECT %s, %s FROM %s ORDER BY %s DESC, %s DESC LIMIT 1",
		pgwire.Quote(wmColumn), strings.Join(pkCols, ", "),
		pgwire.Qualified(ref.Schema, ref.Table),
		pgwire.Quote(wmColumn), strings.Join(pkCols, ", "))
	rs, err := s.conn.Query(ctx, q)
	if err != nil {
		return Watermark{}, err
	}
	defer rs.Close()
	if !rs.Next() {
		return Watermark{}, nil // 空表
	}
	vals, err := rs.Values()
	if err != nil {
		return Watermark{}, err
	}
	if len(vals) < 2 {
		return Watermark{}, nil
	}
	parts := make([]string, 0, len(vals)-1)
	for _, v := range vals[1:] {
		parts = append(parts, cursorText(v))
	}
	return Watermark{Wm: cursorText(vals[0]), PK: joinCursor(parts)}, nil
}

func splitWatermarkPK(s string) []string {
	if s == "" {
		return []string{""}
	}
	return strings.Split(s, "\x1f")
}

// TableRowsEstimate 读源端表的行数估算，**并明确区分「未知」与「0 行」**。
//
// `pg_class.reltuples = -1` 表示「从没 ANALYZE 过」——新导入的大表长期就是 -1。
// 控制面 Catalog 把它折成 0（`CASE WHEN reltuples < 0 THEN 0`），于是下游会把
// 「不知道」当成「很小」，恰好放行最危险的表。这里保留 -1 作为 known=false，
// 让调用方能 fail-closed。
func (s *Source) TableRowsEstimate(ctx context.Context, ref source.TableRef) (rows int64, known bool, err error) {
	q := `SELECT c.reltuples::bigint FROM pg_class c
	      JOIN pg_namespace n ON n.oid = c.relnamespace
	      WHERE n.nspname = $1 AND c.relname = $2`
	var est int64
	if err := s.conn.QueryRow(ctx, q, ref.Schema, ref.Table).Scan(&est); err != nil {
		return 0, false, err
	}
	if est < 0 {
		return 0, false, nil // 未知：调用方必须按 fail-closed 处理
	}
	return est, true, nil
}

// TablePhysicalSize 返回表在磁盘上的实际大小（`pg_total_relation_size`，含索引与 TOAST）。
//
// 为什么需要它：`reltuples` 在「从没 ANALYZE 过」时是 -1，而 PG 的自动 analyze 阈值是
// `autovacuum_analyze_threshold = 50` 行——**几行的小维表永远不会被自动 analyze**，
// 于是「未知」会一直成立。若把「未知」一律拦死，小维表会把整条 interval 任务卡住。
// 物理大小读的是文件，不需要统计信息，正好用来区分「未知但很小」与「未知且很大」。
//
// **不要用 `relpages`**：它和 `reltuples` 一样只在 VACUUM/ANALYZE 后更新，
// 对新导入的大表同样是 0——用它放行会把最危险的表重新放回来。
func (s *Source) TablePhysicalSize(ctx context.Context, ref source.TableRef) (int64, error) {
	q := `SELECT pg_total_relation_size(c.oid) FROM pg_class c
	      JOIN pg_namespace n ON n.oid = c.relnamespace
	      WHERE n.nspname = $1 AND c.relname = $2`
	var size int64
	if err := s.conn.QueryRow(ctx, q, ref.Schema, ref.Table).Scan(&size); err != nil {
		return 0, err
	}
	return size, nil
}
