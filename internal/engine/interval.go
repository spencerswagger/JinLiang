package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/spencerswagger/jinliang/internal/pgsource"
	"github.com/spencerswagger/jinliang/internal/source"
	"github.com/spencerswagger/jinliang/internal/state"
)

// interval 模式的一轮：**没有变更流水，靠水位轮询**。
//
// 每张表：
//   - 有水位列、且已有水位 → 从「水位 − lookback」读一页页新行，upsert 落库，
//     每页把 (水位列, 主键) 写进 `_jl_meta.watermark`（与数据同事务）；
//   - 还没有水位（第一次跑这张表）→ 整表搬一遍，然后**把水位对齐到表尾**
//     （否则下一轮会把刚搬完的整表再扫一遍）；
//   - 没有水位列 → 每轮整表重搬，并且只 **warn**（不阻断：表小的时候这是完全合理的用法）。
//
// 视图只能走 interval（源端不记录视图的变更），这条在预检里已经是 blocked。
func (r *runner) runIntervalRound(ctx context.Context) error {
	wmCol := strings.TrimSpace(r.doc.Mode.IncrementalColumn)
	if wmCol == "" {
		return fmt.Errorf("interval 模式必须有 mode.incremental_column（水位列）；预检里这条是 blocked")
	}
	lookback := strings.TrimSpace(r.doc.Mode.Lookback)
	if lookback == "" {
		lookback = defaultLookback(r.intervalEvery())
	}

	stored, err := r.sink.Watermark(ctx)
	if err != nil {
		return fmt.Errorf("读水位失败: %w", err)
	}
	marks := map[string]string{}
	if strings.TrimSpace(stored) != "" {
		_ = json.Unmarshal([]byte(stored), &marks)
	}

	var total int64
	for _, ref := range r.tables {
		ti := r.sink.Table(ref)
		if ti == nil {
			continue
		}
		if !r.tableHasColumn(ref, wmCol) {
			// 没有水位列：整表重搬，只提示不阻断。
			r.note(ctx, fmt.Sprintf("%s 没有水位列 %s：本轮整表重搬（行数多的话建议给它加一个更新时间列）",
				ref.Table, wmCol))
			n, err := r.copyOneTable(ctx, ref)
			if err != nil {
				return err
			}
			total += n
			continue
		}

		from, has := pgsource.ParseWatermark(marks[ref.Table])
		if !has {
			// 第一次同步这张表：整表搬 + 水位对齐到表尾。
			n, err := r.copyOneTable(ctx, ref)
			if err != nil {
				return err
			}
			total += n
			maxWm, err := r.src.MaxWatermark(ctx, ref, wmCol)
			if err != nil {
				return err
			}
			if !maxWm.IsZero() {
				if err := r.saveWatermark(ctx, ref, maxWm); err != nil {
					return err
				}
			}
			continue
		}

		rowsRead, next, err := r.drainWatermark(ctx, ref, wmCol, from, lookback)
		if err != nil {
			return err
		}
		total += rowsRead
		if !next.IsZero() && next.String() != from.String() {
			if err := r.saveWatermark(ctx, ref, next); err != nil {
				return err
			}
		}
	}
	r.progress(ctx, fmt.Sprintf("定时增量：本轮搬了 %d 行（水位列 %s）", total, wmCol))
	return nil
}

// drainWatermark 把一张表的水位增量一页页读完并落库。
func (r *runner) drainWatermark(ctx context.Context, ref source.TableRef, wmCol string,
	from pgsource.Watermark, lookback string) (int64, pgsource.Watermark, error) {

	cursor := from
	// 第一页回退 lookback（吸收「提交晚于写入」的行），之后按 (wm, pk) 前进。
	first := pgsource.Watermark{Wm: from.Wm, PK: ""}
	var total int64
	for {
		rows, next, done, err := r.src.ReadWatermarkPage(ctx, ref, wmCol, cursor, lookback, int64(r.batchRows()))
		if err != nil {
			return total, cursor, err
		}
		_ = first
		if len(rows) > 0 {
			pos := r.consistentPoint.String()
			if pos == "" {
				pos = r.lastPos.String()
			}
			if err := r.sink.ApplyRowsRange(ctx, ref, rows, pos, ""); err != nil {
				return total, cursor, err
			}
			total += int64(len(rows))
			r.rowsStream += int64(len(rows))
		}
		if next.IsZero() || next.String() == cursor.String() {
			return total, cursor, nil
		}
		cursor = next
		if done {
			return total, cursor, nil
		}
	}
}

// saveWatermark 把这张表的水位写进 `_jl_meta.watermark`（一个事务、一个 JSON 对象）。
func (r *runner) saveWatermark(ctx context.Context, ref source.TableRef, w pgsource.Watermark) error {
	stored, err := r.sink.Watermark(ctx)
	if err != nil {
		return err
	}
	marks := map[string]string{}
	if strings.TrimSpace(stored) != "" {
		_ = json.Unmarshal([]byte(stored), &marks)
	}
	marks[ref.Table] = w.String()
	b, err := json.Marshal(marks)
	if err != nil {
		return err
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, b); err != nil {
		return err
	}
	return r.sink.ApplyRowsRange(ctx, ref, nil, "", buf.String())
}

// copyOneTable 整表搬一张表（interval 的「第一次」与「没有水位列」两条路都用它）。
// 与全量走同一个 pgsink 落地路径（临时表 + upsert），所以可重复执行。
func (r *runner) copyOneTable(ctx context.Context, ref source.TableRef) (int64, error) {
	var total int64
	_, err := r.src.SnapshotChunks(ctx, source.SnapshotOpt{ChunkRows: r.chunkRows(), Conns: 1},
		func(c context.Context, ch pgsource.Chunk) error {
			if ch.Table != ref {
				return nil
			}
			if err := r.sink.BulkApply(c, r.sink.Table(ch.Table), ch.No, ch.Rows,
				r.consistentPoint.String(), ch.From, ch.To); err != nil {
				return err
			}
			total += int64(len(ch.Rows))
			r.rowsCopied += int64(len(ch.Rows))
			return nil
		})
	return total, err
}

func (r *runner) tableHasColumn(ref source.TableRef, col string) bool {
	for _, t := range r.cat.Tables {
		if t.Schema == ref.Schema && t.Table == ref.Table {
			for _, c := range t.Columns {
				if strings.EqualFold(c.Name, col) {
					return true
				}
			}
			return false
		}
	}
	return false
}

// intervalEvery 解析 mode.interval（"5m" / "1h"；cron 形式暂不支持，返回 0）。
func (r *runner) intervalEvery() time.Duration {
	d, err := time.ParseDuration(strings.TrimSpace(r.doc.Mode.Interval))
	if err != nil || d <= 0 {
		return 0
	}
	return d
}

// defaultLookback：没配 lookback 时按「一个间隔」回看，最少 1 分钟。
// 理由：一个间隔刚好覆盖上一轮到现在这段，长事务晚提交的行也能被下一轮捞到。
func defaultLookback(every time.Duration) string {
	if every <= 0 {
		return "1m"
	}
	if every < time.Minute {
		return "1m"
	}
	return every.String()
}

// Schedule 是五档频次的调度骨架：按 mode.interval 一轮轮跑，直到 ctx 取消。
//
// 每轮一个独立的 run 行（kind=sync），所以 `idx_runs_one_active` 天然挡住重入；
// **跨轮不释放租约**（否则每轮都要抢一次，且两轮之间可能被别人插进来），
// 只在进程退出时由调用方 teardown 释放；丢租约仍然走 cancelRun()。
func Schedule(ctx context.Context, opt Options) error {
	store := opt.Store
	job, err := store.GetJob(ctx, opt.JobID)
	if err != nil {
		return err
	}
	doc, err := store.JobDoc(ctx, job)
	if err != nil {
		return err
	}
	every := parseInterval(doc.Mode.Interval)
	if every <= 0 {
		return fmt.Errorf("mode.interval=%q 不是一个可用的间隔（支持 5m / 1h 这类；cron 形式还没实现）",
			doc.Mode.Interval)
	}
	opt.SnapshotOnly = false
	// 第一轮立刻跑，之后按间隔。
	runOnce := func() error {
		res, err := Run(ctx, opt)
		if err != nil {
			// 单轮失败不停调度：定时同步的价值在于「下一轮还会试」。
			// 但用户按停（130）要停整个调度。
			if res.ExitCode == ExitStopped {
				return errStopSchedule
			}
			return nil
		}
		if res.ExitCode == ExitStopped {
			return errStopSchedule
		}
		return nil
	}
	if err := runOnce(); err != nil {
		if err == errStopSchedule {
			return nil
		}
		return err
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := runOnce(); err != nil {
				if err == errStopSchedule {
					return nil
				}
				return err
			}
		}
	}
}

var errStopSchedule = fmt.Errorf("调度停止")

func parseInterval(s string) time.Duration {
	d, err := time.ParseDuration(strings.TrimSpace(s))
	if err != nil {
		return 0
	}
	return d
}

// phaseForInterval 让 interval 轮次在状态机里有个合法入口（Prepare → Streaming）。
func (r *runner) phaseForInterval(ctx context.Context) error {
	if r.phase == state.Prepare {
		return r.phaseTo(ctx, state.Streaming)
	}
	return nil
}
