package verify

import (
	"context"
	"fmt"
	"hash/fnv"
	"math/big"
	"sort"
	"strconv"
	"strings"

	"github.com/spencerswagger/jinliang/internal/source"
)

// 数据级校验的判据顺序（Owl #44/P0）：
//   1. 聚合必须真比对（count / sum / min / max / 文本长度 / NULL 数）；
//   2. 全表按主键顺序逐行比对，块级 checksum 用于定位；
//   3. 抽样只是「聚合与全表都一致之后的额外确认」，绝不能单独当判据。
//
// 只比 count+sum 会漏掉「两行互换」——行数、sum、min、max 全不变；块级
// 顺序敏感 checksum 才能发现它，所以第四条反例必须存在。

const verifyBlockRows = 1000

// tableAgg is the additive evidence for one table. big.Rat keeps decimal
// precision exact across PG numeric / MySQL decimal / float encodings.
type tableAgg struct {
	rows    int64
	numeric map[string]bool
	textual map[string]bool
	sums    map[string]*big.Rat
	mins    map[string]*big.Rat
	maxs    map[string]*big.Rat
	textLen map[string]int64
	nulls   map[string]int64
}

func newTableAgg(srcT source.Table, cols []string) *tableAgg {
	a := &tableAgg{
		numeric: map[string]bool{}, textual: map[string]bool{}, sums: map[string]*big.Rat{},
		mins: map[string]*big.Rat{}, maxs: map[string]*big.Rat{},
		textLen: map[string]int64{}, nulls: map[string]int64{},
	}
	types := map[string]string{}
	for _, c := range srcT.Columns {
		types[strings.ToLower(c.Name)] = c.Type
	}
	for _, c := range cols {
		key := strings.ToLower(c)
		a.nulls[key] = 0
		if isNumericType(types[key]) {
			a.numeric[key] = true
			a.sums[key] = new(big.Rat)
		} else {
			a.textLen[key] = 0
			if isTextType(types[key]) {
				a.textual[key] = true
			}
		}
	}
	return a
}

func (a *tableAgg) add(row []any, cols []string) {
	a.rows++
	for i, c := range cols {
		key := strings.ToLower(c)
		var v any
		if i < len(row) {
			v = row[i]
		}
		if v == nil {
			a.nulls[key]++
			continue
		}
		if a.numeric[key] {
			if r, ok := toRat(v); ok {
				a.sums[key].Add(a.sums[key], r)
				if a.mins[key] == nil || r.Cmp(a.mins[key]) < 0 {
					a.mins[key] = new(big.Rat).Set(r)
				}
				if a.maxs[key] == nil || r.Cmp(a.maxs[key]) > 0 {
					a.maxs[key] = new(big.Rat).Set(r)
				}
			}
			continue
		}
		a.textLen[key] += int64(dataLength(v))
	}
}

// compare returns one finding per mismatching column, with both values.
func (a *tableAgg) compare(b *tableAgg, table string) []Finding {
	var out []Finding
	for c := range a.numeric {
		bs, ok := b.sums[c]
		if !ok {
			out = append(out, Finding{Code: "aggregate_mismatch", Severity: SeverityError, Table: table, Column: c,
				Detail: "两端列类型不一致（源端数值、目标端非数值），无法按数值聚合",
				Source: ratText(a.sums[c]), Target: "non-numeric",
				Hint: "先修类型映射；不要把数值当文本比"})
			continue
		}
		if a.sums[c].Cmp(bs) != 0 {
			out = append(out, aggFinding(table, c, "sum", ratText(a.sums[c]), ratText(bs)))
		}
		bm, ok := b.mins[c]
		switch {
		case !ok:
			out = append(out, aggFinding(table, c, "min", ratText(a.mins[c]), "non-numeric"))
		case a.mins[c] == nil && bm != nil:
			out = append(out, aggFinding(table, c, "min", "NULL", ratText(bm)))
		case a.mins[c] != nil && bm == nil:
			out = append(out, aggFinding(table, c, "min", ratText(a.mins[c]), "NULL"))
		case a.mins[c] != nil && bm != nil && a.mins[c].Cmp(bm) != 0:
			out = append(out, aggFinding(table, c, "min", ratText(a.mins[c]), ratText(bm)))
		}
		bx, ok := b.maxs[c]
		switch {
		case !ok:
			out = append(out, aggFinding(table, c, "max", ratText(a.maxs[c]), "non-numeric"))
		case a.maxs[c] == nil && bx != nil:
			out = append(out, aggFinding(table, c, "max", "NULL", ratText(bx)))
		case a.maxs[c] != nil && bx == nil:
			out = append(out, aggFinding(table, c, "max", ratText(a.maxs[c]), "NULL"))
		case a.maxs[c] != nil && bx != nil && a.maxs[c].Cmp(bx) != 0:
			out = append(out, aggFinding(table, c, "max", ratText(a.maxs[c]), ratText(bx)))
		}
	}
	for c := range a.textLen {
		bv, ok := b.textLen[c]
		if !ok {
			out = append(out, aggFinding(table, c, "text_len", fmt.Sprint(a.textLen[c]), "non-text"))
			continue
		}
		if a.textLen[c] != bv {
			out = append(out, aggFinding(table, c, "text_len", fmt.Sprint(a.textLen[c]), fmt.Sprint(bv)))
		}
	}
	for c := range a.nulls {
		bv, ok := b.nulls[c]
		if !ok {
			out = append(out, aggFinding(table, c, "nulls", fmt.Sprint(a.nulls[c]), "unknown"))
			continue
		}
		if a.nulls[c] != bv {
			out = append(out, aggFinding(table, c, "nulls", fmt.Sprint(a.nulls[c]), fmt.Sprint(bv)))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Column != out[j].Column {
			return out[i].Column < out[j].Column
		}
		return out[i].Detail < out[j].Detail
	})
	return out
}

func aggFinding(table, column, metric, src, tgt string) Finding {
	return Finding{
		Code: "aggregate_mismatch", Severity: SeverityError, Table: table, Column: column,
		Detail: fmt.Sprintf("聚合 %s 不一致", metric), Source: src, Target: tgt,
		Hint: "先看块级/行级差异定位具体行；聚合不一致但行级没差异说明样本漏了，按块 checksum 查",
	}
}

func ratText(r *big.Rat) string {
	if r == nil {
		return "NULL"
	}
	return r.FloatString(10)
}

func isTextType(t string) bool {
	s := strings.ToLower(strings.TrimSpace(t))
	if i := strings.IndexByte(s, '('); i >= 0 {
		s = strings.TrimSpace(s[:i])
	}
	switch {
	case strings.Contains(s, "char"), strings.Contains(s, "text"),
		strings.Contains(s, "json"), strings.Contains(s, "enum"),
		strings.Contains(s, "set"), strings.Contains(s, "uuid"),
		strings.Contains(s, "xml"), strings.Contains(s, "inet"),
		strings.Contains(s, "cidr"), strings.Contains(s, "macaddr"),
		strings.Contains(s, "interval"):
		return true
	}
	return false
}

// dataLength counts bytes for text/binary values. Looking at []byte avoids the
// cross-engine mismatch "s..." vs "x'hex'" doubling the length.
func dataLength(v any) int {
	switch x := v.(type) {
	case []byte:
		return len(x)
	case string:
		return len(x)
	}
	return len(canonicalValue(v))
}

func isNumericType(t string) bool {
	s := strings.ToLower(strings.TrimSpace(t))
	if i := strings.IndexByte(s, '('); i >= 0 {
		s = strings.TrimSpace(s[:i])
	}
	switch {
	case strings.Contains(s, "int"), strings.Contains(s, "serial"),
		strings.Contains(s, "numeric"), strings.Contains(s, "decimal"),
		strings.Contains(s, "float"), strings.Contains(s, "double"),
		strings.Contains(s, "real"), strings.Contains(s, "money"),
		strings.Contains(s, "year"):
		return true
	}
	return false
}

func toRat(v any) (*big.Rat, bool) {
	switch x := v.(type) {
	case int64:
		return new(big.Rat).SetInt64(x), true
	case int32:
		return new(big.Rat).SetInt64(int64(x)), true
	case int:
		return new(big.Rat).SetInt64(int64(x)), true
	case uint64:
		return new(big.Rat).SetUint64(x), true
	case float64:
		return ratFromString(strconv.FormatFloat(x, 'g', -1, 64))
	case float32:
		// bitSize=32 才能得到 "0.1"；先转 float64 会带出
		// 2500000037252903/25000000000000000 的二进制尾数。
		return ratFromString(strconv.FormatFloat(float64(x), 'g', -1, 32))
	case []byte:
		return ratFromString(string(x))
	case string:
		return ratFromString(x)
	}
	return nil, false
}

func ratFromString(s string) (*big.Rat, bool) {
	r := new(big.Rat)
	if _, ok := r.SetString(strings.TrimSpace(s)); ok {
		return r, true
	}
	return nil, false
}

// comparePK orders two PK tuples the way the database did: numeric when both
// sides parse as numbers, otherwise canonical string order.
func comparePK(a, b []any) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		ra, oka := toRat(a[i])
		rb, okb := toRat(b[i])
		if oka && okb {
			if c := ra.Cmp(rb); c != 0 {
				return c
			}
			continue
		}
		if c := strings.Compare(canonicalValue(a[i]), canonicalValue(b[i])); c != 0 {
			return c
		}
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	}
	return 0
}

// rowStream turns ScanOrdered's callback into a pull iterator so two sides can
// be merged in PK order without buffering whole tables.
type rowMsg struct {
	vals []any
	err  error
}

type rowStream struct {
	ch     chan rowMsg
	cancel context.CancelFunc
}

func startRowStream(ctx context.Context, side Side, schema, table string, cols, pk []string) *rowStream {
	cctx, cancel := context.WithCancel(ctx)
	s := &rowStream{ch: make(chan rowMsg, 64), cancel: cancel}
	go func() {
		defer close(s.ch)
		err := side.ScanOrdered(cctx, schema, table, cols, pk, func(vals []any) error {
			select {
			case s.ch <- rowMsg{vals: vals}:
				return nil
			case <-cctx.Done():
				return cctx.Err()
			}
		})
		if err != nil {
			s.ch <- rowMsg{err: err}
		}
	}()
	return s
}

func (s *rowStream) next(ctx context.Context) ([]any, bool, error) {
	select {
	case <-ctx.Done():
		return nil, false, ctx.Err()
	case m, ok := <-s.ch:
		if !ok {
			return nil, false, nil
		}
		if m.err != nil {
			return nil, false, m.err
		}
		return m.vals, true, nil
	}
}

// runDataFull compares one table end to end: ordered full scan, block checksum,
// row drill-down and additive aggregates. It returns the number of rows matched.
func runDataFull(ctx context.Context, lr *LevelReport, opts Options, name string,
	srcT, tgtT source.Table, commonCols []string) int64 {

	srcPK, tgtPK := srcT.PrimaryKey, tgtT.PrimaryKey
	srcIdx := indexesOf(commonCols, srcPK)
	tgtIdx := indexesOf(commonCols, tgtPK)
	if len(srcIdx) != len(srcPK) || len(tgtIdx) != len(tgtPK) || !sameSet(srcPK, tgtPK) {
		lr.add(SeverityError, "pk_mismatch", name, "",
			"主键列不在共同列里或两端主键不一致，无法做行级定位", join(srcPK), join(tgtPK),
			"先修结构级的主键差异")
		return 0
	}

	srcAgg := newTableAgg(srcT, commonCols)
	tgtAgg := newTableAgg(tgtT, commonCols)
	srcStream := startRowStream(ctx, opts.Source, opts.SourceSchema, name, commonCols, srcPK)
	tgtStream := startRowStream(ctx, opts.Target, opts.TargetSchema, name, commonCols, tgtPK)
	defer srcStream.cancel()
	defer tgtStream.cancel()

	srcRow, srcOK, err := srcStream.next(ctx)
	if err != nil {
		lr.add(SeverityError, "probe_failed", name, "", "全表扫描源端失败："+err.Error(), "", "", "")
		return 0
	}
	tgtRow, tgtOK, err := tgtStream.next(ctx)
	if err != nil {
		lr.add(SeverityError, "probe_failed", name, "", "全表扫描目标端失败："+err.Error(), "", "", "")
		return 0
	}

	var matched int64
	blockNo := 0
	blockRows := int64(0)
	blockStart := ""
	blocksChecked, blocksMismatched := 0, 0
	srcHash, tgtHash := fnv.New64a(), fnv.New64a()
	flushBlock := func() {
		if blockRows == 0 {
			return
		}
		blocksChecked++
		if srcHash.Sum64() != tgtHash.Sum64() {
			blocksMismatched++
			lr.add(SeverityError, "block_checksum_mismatch", name, "",
				fmt.Sprintf("第 %d 块（%d 行，起始主键 %s）校验和不一致", blockNo, blockRows, blockStart),
				fmt.Sprintf("%016x", srcHash.Sum64()), fmt.Sprintf("%016x", tgtHash.Sum64()),
				"块内逐行差异见同块的 row_mismatch/missing_row/extra_row")
		}
		blockNo++
		blockRows = 0
		srcHash.Reset()
		tgtHash.Reset()
	}

	for srcOK || tgtOK {
		switch {
		case !srcOK:
			pkDesc := pkDescription(tgtRow, tgtIdx)
			lr.add(SeverityError, "extra_row", name, "",
				"目标端多出一行（源端没有这个主键 "+pkDesc+"）", "missing", pkDesc, "确认这条是否该被删除")
			tgtAgg.add(tgtRow, commonCols)
			tgtRow, tgtOK, err = tgtStream.next(ctx)
		case !tgtOK:
			pkDesc := pkDescription(srcRow, srcIdx)
			lr.add(SeverityError, "missing_row", name, "",
				"源端有这一行，目标端没有（主键覆盖缺失，主键 "+pkDesc+"）", pkDesc, "missing", "确认这条变更是否进了死信")
			srcAgg.add(srcRow, commonCols)
			srcRow, srcOK, err = srcStream.next(ctx)
		default:
			cmp := comparePK(selectAt(srcRow, srcIdx), selectAt(tgtRow, tgtIdx))
			switch {
			case cmp < 0:
				pkDesc := pkDescription(srcRow, srcIdx)
				lr.add(SeverityError, "missing_row", name, "",
					"源端有这一行，目标端没有（主键覆盖缺失，主键 "+pkDesc+"）", pkDesc, "missing", "确认这条变更是否进了死信")
				srcAgg.add(srcRow, commonCols)
				srcRow, srcOK, err = srcStream.next(ctx)
			case cmp > 0:
				pkDesc := pkDescription(tgtRow, tgtIdx)
				lr.add(SeverityError, "extra_row", name, "",
					"目标端多出一行（源端没有这个主键 "+pkDesc+"）", "missing", pkDesc, "确认这条是否该被删除")
				tgtAgg.add(tgtRow, commonCols)
				tgtRow, tgtOK, err = tgtStream.next(ctx)
			default:
				srcAgg.add(srcRow, commonCols)
				tgtAgg.add(tgtRow, commonCols)
				pkDesc := pkDescription(srcRow, srcIdx)
				srcCanon := canonicalRowNumeric(commonCols, srcRow, srcAgg.numeric, srcAgg.textual)
				tgtCanon := canonicalRowNumeric(commonCols, tgtRow, tgtAgg.numeric, tgtAgg.textual)
				if srcCanon != tgtCanon {
					col, sv, tv := firstDiffNumeric(commonCols, srcRow, tgtRow, srcAgg.numeric, srcAgg.textual)
					lr.add(SeverityError, "row_mismatch", name, col,
						"行内容不一致（主键 "+pkDesc+"）", trunc(sv, 80), trunc(tv, 80),
						"按主键重放该行；若反复出现，检查转换规则")
				}
				if blockRows == 0 {
					blockStart = pkDesc
				}
				_, _ = srcHash.Write([]byte(pkDesc + "|" + srcCanon))
				_, _ = tgtHash.Write([]byte(pkDesc + "|" + tgtCanon))
				blockRows++
				matched++
				if blockRows >= verifyBlockRows {
					flushBlock()
				}
				srcRow, srcOK, err = srcStream.next(ctx)
				if err == nil {
					tgtRow, tgtOK, err = tgtStream.next(ctx)
				}
			}
		}
		if err != nil {
			lr.add(SeverityError, "probe_failed", name, "", "全表扫描中断："+err.Error(), "", "", "")
			break
		}
	}
	flushBlock()
	lr.stat("blocks_checked", blocksChecked)
	lr.stat("blocks_mismatched", blocksMismatched)
	lr.stat("full_scan_rows", matched)

	for _, f := range srcAgg.compare(tgtAgg, name) {
		lr.Findings = append(lr.Findings, f)
		lr.OK = false
	}
	return matched
}

// runAggregateOnly handles tables without a stable PK: no row alignment, but
// aggregates still must match (count/sum/min/max/text_len/nulls).
func runAggregateOnly(ctx context.Context, lr *LevelReport, opts Options, name string,
	srcT, tgtT source.Table, commonCols []string) {
	srcAgg := newTableAgg(srcT, commonCols)
	tgtAgg := newTableAgg(tgtT, commonCols)
	for _, side := range []struct {
		s  Side
		sc string
		a  *tableAgg
	}{{opts.Source, opts.SourceSchema, srcAgg}, {opts.Target, opts.TargetSchema, tgtAgg}} {
		err := side.s.ScanOrdered(ctx, side.sc, name, commonCols, nil, func(vals []any) error {
			side.a.add(vals, commonCols)
			return nil
		})
		if err != nil {
			lr.add(SeverityError, "probe_failed", name, "", "全表聚合扫描失败："+err.Error(), "", "", "")
			return
		}
	}
	for _, f := range srcAgg.compare(tgtAgg, name) {
		lr.Findings = append(lr.Findings, f)
		lr.OK = false
	}
	lr.stat("aggregate_only", true)
}

func indexesOf(cols, want []string) []int {
	idx := map[string]int{}
	for i, c := range cols {
		idx[strings.ToLower(c)] = i
	}
	out := make([]int, 0, len(want))
	for _, c := range want {
		i, ok := idx[strings.ToLower(c)]
		if !ok {
			return nil
		}
		out = append(out, i)
	}
	return out
}

func selectAt(row []any, idx []int) []any {
	out := make([]any, len(idx))
	for i, j := range idx {
		if j < len(row) {
			out[i] = row[j]
		}
	}
	return out
}

func pkDescription(row []any, idx []int) string {
	parts := make([]string, len(idx))
	for i, j := range idx {
		v := any(nil)
		if j < len(row) {
			v = row[j]
		}
		parts[i] = fmt.Sprint(v)
	}
	return strings.Join(parts, ",")
}

// SQLAggregator is the optional cheap L1 path: one aggregate query per side
// instead of a full row scan. runtime.DB implements it for PG/MySQL.
type SQLAggregator interface {
	AggregateSQL(ctx context.Context, schema, table string, columns []string, numeric map[string]bool) (int64, map[string]string, error)
}

// runAggregateSQL compares L1 evidence only (count/sum/min/max/text_len/nulls).
// It is the daily-check path; it cannot localize a row, so a mismatch tells the
// operator to run the full data level.
func runAggregateSQL(ctx context.Context, lr *LevelReport, opts Options, name string,
	srcT, tgtT source.Table, commonCols []string) {
	// 各端按自己的列类型决定是否按数值聚合：源 numeric → 目标 text 时，
	// 目标端应走文本长度/NULL 数，而不是被源端类型带着当数字算。
	srcNumeric := newTableAgg(srcT, commonCols).numeric
	tgtNumeric := newTableAgg(tgtT, commonCols).numeric
	srcAgg, err := sqlAggFor(ctx, opts.Source, opts.SourceSchema, name, commonCols, srcNumeric, srcT)
	if err != nil {
		lr.add(SeverityError, "probe_failed", name, "", "源端聚合查询失败："+err.Error(), "", "", "")
		return
	}
	tgtAgg, err := sqlAggFor(ctx, opts.Target, opts.TargetSchema, name, commonCols, tgtNumeric, tgtT)
	if err != nil {
		lr.add(SeverityError, "probe_failed", name, "", "目标端聚合查询失败："+err.Error(), "", "", "")
		return
	}
	for _, f := range srcAgg.compare(tgtAgg, name) {
		lr.Findings = append(lr.Findings, f)
		lr.OK = false
	}
	lr.stat("aggregate_only", true)
	lr.stat("rows_source", srcAgg.rows)
	lr.stat("rows_target", tgtAgg.rows)
}

func sqlAggFor(ctx context.Context, side Side, schema, table string, cols []string,
	numeric map[string]bool, srcT source.Table) (*tableAgg, error) {
	agg, ok := side.(SQLAggregator)
	if !ok {
		return nil, fmt.Errorf("该连接不支持 SQL 聚合下推（用完整 data 级）")
	}
	rows, vals, err := agg.AggregateSQL(ctx, schema, table, cols, numeric)
	if err != nil {
		return nil, err
	}
	return aggFromSQL(rows, vals, cols, numeric, srcT), nil
}

func aggFromSQL(rows int64, vals map[string]string, cols []string, numeric map[string]bool, srcT source.Table) *tableAgg {
	a := newTableAgg(srcT, cols)
	a.rows = rows
	for _, c := range cols {
		key := strings.ToLower(c)
		if numeric[key] {
			if r, ok := ratFromString(vals["sum:"+key]); ok {
				a.sums[key] = r
			}
			if r, ok := ratFromString(vals["min:"+key]); ok {
				a.mins[key] = r
			}
			if r, ok := ratFromString(vals["max:"+key]); ok {
				a.maxs[key] = r
			}
		}
		if n, ok := parseInt64(vals["len:"+key]); ok {
			a.textLen[key] = n
		}
		if n, ok := parseInt64(vals["nulls:"+key]); ok {
			a.nulls[key] = n
		}
	}
	return a
}

func parseInt64(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	return n, err == nil
}
