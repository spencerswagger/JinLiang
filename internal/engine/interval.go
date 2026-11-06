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
			// 没有水位列 → 只能每轮整表重搬。这个代价必须由用户显式承担，不能替他猜。
			//
			// 判据两条腿：
			//   1. 行数估算（reltuples）——**未知不等于很小**，`-1` 要单独处理；
			//   2. 物理大小（pg_total_relation_size）——不需要 ANALYZE，用来区分
			//      「未知但很小」与「未知且很大」。
			//
			// 为什么需要第 2 条：PG 的自动 analyze 阈值是 `autovacuum_analyze_threshold = 50`
			// 行，**几行的小维表永远不会被自动 analyze**，reltuples 长期是 -1；若把「未知」
			// 一律拦死，这种表会让整条 interval 任务起不来。
			// **不要用 relpages**：它和 reltuples 一样只在 VACUUM/ANALYZE 后更新，
			// 对新导入的大表同样是 0，用它放行会把最危险的表重新放回来。
			est, known, err := r.src.TableRowsEstimate(ctx, ref)
			if err != nil {
				return fmt.Errorf("读 %s 的行数估算失败: %w", ref.Table, err)
			}
			// 物理大小只在「统计信息未知」时才查：每轮每表少一次往返
			// （大表上 pg_total_relation_size 也不是免费的）。
			var size int64
			if !known {
				s, serr := r.src.TablePhysicalSize(ctx, ref)
				if serr != nil {
					return fmt.Errorf("读 %s 的物理大小失败（判断不了体量就不敢放行整表重搬）: %w", ref.Table, serr)
				}
				size = s
			}
			if allow, why := fullRecopyDecision(known, est, size); !allow {
				return &notImplementedError{fmt.Sprintf("表 %s 没有水位列 %s：%s", ref.Table, wmCol, why)}
			}
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
			// 第一次同步这张表：**先取表尾，再搬快照，水位用搬之前那个值**。
			// 反过来（先搬再取表尾）会留一个永久漏读窗口：快照之后插入的行不在快照里，
			// 而水位已经越过它们——如果水位列是业务时间、又有回填，`wm` 远早于
			// `maxWm − lookback`，那条行就永远捞不回来了。宁重不漏：重复由 upsert 吸收。
			maxWm, err := r.src.MaxWatermark(ctx, ref, wmCol)
			if err != nil {
				return err
			}
			n, err := r.copyOneTable(ctx, ref)
			if err != nil {
				return err
			}
			total += n
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

	// 第一页把主键清空 → 触发「水位 − lookback」的回退窗口（吸收提交晚于写入的行），
	// 之后按 (wm, pk) 前进。**不清空就等于 lookback 从未生效**（曾经就是这么错的）。
	cursor := pgsource.Watermark{Wm: from.Wm, PK: ""}
	var total int64
	for {
		rows, next, done, err := r.src.ReadWatermarkPage(ctx, ref, wmCol, cursor, lookback, int64(r.batchRows()))
		if err != nil {
			return total, cursor, err
		}
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

// fullRecopyDecision 判断「没有水位列的表能不能每轮整表重搬」。
//
// **纯函数**，把判据从 SQL/往返里拿出来：好测、好评审，也保证两条腿的先后关系固定——
// 先看已知行数（可靠且语义直接），未知才退到物理大小（可靠但只能表达「大概多大」）。
// 返回 (false, why) 时 why 是给用户看的一句话（调用方补上表名与水位列名）。
func fullRecopyDecision(known bool, rows, size int64) (allow bool, why string) {
	if known {
		if rows > maxFullCopyRowsWithoutWatermark {
			return false, fmt.Sprintf("它约 %d 行，定时增量只能每轮整表重搬，"+
				"这对大表等于每分钟一次全表拷贝。要么给它加一个更新时间列（推荐），"+
				"要么把 mode.frequency 改成 full_interval 明确接受整表周期重跑", rows)
		}
		return true, ""
	}
	if size >= maxFullCopyBytesWithoutWatermark {
		return false, fmt.Sprintf("源库里也没有它的统计信息（从没 ANALYZE 过），而它在磁盘上已经占 %s："+
			"判断不了行数就不敢放行「每轮整表重搬」。你现在能做什么："+
			"1) 在源库执行 ANALYZE 这张表后重跑；2) 给它加一个更新时间列（推荐，之后就走增量）；"+
			"3) 把 mode.frequency 改成 full_interval，明确接受整表周期重跑", humanLag(size))
	}
	// 未知但物理很小（典型：几行的小维表，永远到不了自动 analyze 的 50 行阈值）→ 放行。
	return true, ""
}

// maxFullCopyBytesWithoutWatermark：统计信息未知时用**物理大小**兜底判断体量。
// 8 MiB 以下的表（典型是小维表）每轮整表重搬完全可接受；再大就必须让人显式决定。
const maxFullCopyBytesWithoutWatermark = 8 << 20

// maxFullCopyRowsWithoutWatermark 是「没有水位列也允许每轮整表重搬」的行数上限。
// 超过就拦下来，让用户显式选：加水位列（推荐），或改用 full_interval 承认整表周期重跑。
const maxFullCopyRowsWithoutWatermark = 100_000

// ── 这段代码的判据：**宁重不漏** ───────────────────────────────
//
// 定时增量允许重复（重读靠目标端 upsert 吸收，代价是多一次写），但**绝不允许漏读**
// ——漏读是重跑也补不回来的错误。所以：
//   - 本轮第一页永远从「水位 − lookback」开始（宁可重读一窗，也不漏一条晚提交的）；
//   - 首次对齐水位要用**搬快照之前**的表尾（宁可从更早的位置再读一遍）；
//   - 拿不准的时候，选「多读一次」而不是「少读一次」。
//
// 关于「先取表尾」会不会变成每轮重扫（用户看到第二轮搬了几万行时最容易误会）：
// **不会**。水位 W0 = 搬快照**之前**的表尾，下一轮从 `W0 − lookback` 读，只重读
// 「快照期间新增的行 + 一窗回看」，而且**只发生一次**——那一轮结束 `saveWatermark(next)`
// 就把水位推到位了。
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
// 每轮一个独立的 run 行（kind=sync），所以 `idx_runs_one_active` 天然挡住重入。
//
// 租约的实话：**每轮 `Run()` 内部 acquire → defer release，所以轮次之间租约会释放**。
// 后果是两轮之间确实存在一个空隙，别的实例可以在这时插进来跑下一轮——
// 但同一时刻仍然只有一个写入者（`idx_runs_one_active` + 目标端租约），
// 且水位按 job 存，插进来的那一轮会从同一个水位继续，不会写乱。
// 要做到「跨轮持有」得把租约提到 Schedule 层（改 sink 生命周期），B6 再评估；
// 现在把注释改成实话，而不是留一句与实现相反的承诺。
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
