package engine

// 按表暂停的**写路径**（docs/CONTRACT-PAUSE-AND-POSITION.md §1.5）：
// 一张表的结构跟不上了，就只停这张表——其它表照写、位点照常前进，
// 跳过的那段窗口记在 `paused_tables` 里，结构对齐后**整表重搬**补齐。
//
// 为什么判定权从 sink 搬到引擎：sink 一批只有一个位点，它要么整批收、要么整批拒。
// 「整批拒」就是「一张表坏了、几十张无关表一起停」，与 DESIGN.md §4 直接矛盾。
// 引擎知道哪条事件属于哪张表，才有资格做「按表跳过」。
//
// 三条不许越的线（越一条就打回）：
//  1. **登记先行**：先让 `PauseTable` 落库成功，才允许这一批跳过该表；
//     登记失败 = 整批失败（位点不动、目标端 0 行）。「跳过了但没记下」是永久静默缺失。
//  2. **事务边界不属于任何表**：`EvBegin`/`EvCommit` 一律保留，否则整批的事务配对就断了。
//  3. **有界重试**：每一轮重试都必须新登记一张表；一轮下来没有新增记录 → 按真失败上抛，
//     不许原地打转（那会变成一个不前进也不报错的死循环）。

import (
	"context"
	"errors"

	"github.com/spencerswagger/jinliang/internal/pgsink"
	"github.com/spencerswagger/jinliang/internal/source"
)

// pauseRegistry 是「按表暂停的存储层」——PG 目标端是 pgsink，MySQL 目标端是 mysqlsink。
//
// 两边的 JSON 形状与语义完全同构（`PausedTable` 就是同一个结构体），所以引擎只认这六个方法：
// 具体是哪一种目标端、表建在哪儿，都不该漏到写路径里。
// **抽这个接口的原因**（维护者 2026-08-11 指出的验收缺口）：`r.sink` 原来是 pgsink 专属类型，
// 于是 `streamGeneric`（MySQL 目标端那条）根本没法包 `pauseSink` —— MySQL 目标端「只停一张表」
// 就一直做不出来，而存储层其实早就就绪了。
type pauseRegistry interface {
	LoadPausedTables(ctx context.Context) error
	PausedTables(ctx context.Context) (map[string]pgsink.PausedTable, error)
	PauseTable(ctx context.Context, ref source.TableRef, rec pgsink.PausedTable) error
	NoteSkipped(ctx context.Context, ref source.TableRef, rows int64, pos string) error
	ClearPausedTable(ctx context.Context, ref source.TableRef) error
	MarkTableRecopyPending(ctx context.Context, ref source.TableRef) error
}

// maxPausePerBatch 是一批里最多新登记几张表：正常情况下是 0–1 张（一次 DDL 只改一张表），
// 给到 8 是为了容纳「一批里恰好跨了几张表的 DDL」，同时把病态情况挡在死循环之外。
const maxPausePerBatch = 8

// pauseSink 在提交前按表过滤事件；命中「结构对不上」时**先登记暂停，再重试这一批**。
//
// 它包在 `refMappedSink` **外面**：事件里的表名还是源端的，而 `paused_tables` 的 key
// 是目标端裸表名，两者靠 `r.targetRef` 对齐（与 `_jl_chunk.table_name` 同源）。
type pauseSink struct {
	inner applier
	r     *runner
}

func newPauseSink(inner applier, r *runner) *pauseSink {
	return &pauseSink{inner: inner, r: r}
}

func (p *pauseSink) Apply(ctx context.Context, evs []source.Event, pos source.Position) error {
	if p.r.pauseReg == nil {
		// 目标端还没有暂停存储（例如没实现这六个方法的连接器）：原样透传。
		// **不是**「静默跳过」——真遇到结构不一致时，写路径自己会报错，上层按失败处理。
		return p.inner.Apply(ctx, evs, pos)
	}
	for attempt := 0; ; attempt++ {
		kept, skipped, rows := p.filter(evs)
		err := p.inner.Apply(ctx, kept, pos)
		if err == nil {
			p.noteSkipped(ctx, skipped, rows, pos)
			return nil
		}
		var pe pgsink.PausableError
		if !errors.As(err, &pe) || pe.Table() == "" {
			return err
		}
		if _, known := p.r.pausedRecord(pe.Table()); known {
			// 已经登记过、却还在报同一张表：那不是「新发现」，是这一批真的写不进去，
			// 原样上抛（别把它重试成一句无关的错）。
			return err
		}
		if attempt >= maxPausePerBatch {
			return err
		}
		// 登记先行：返回非 nil 就整批失败——宁可停下让人看，不可跳过不留痕。
		if rerr := p.r.registerPause(ctx, err); rerr != nil {
			return rerr
		}
	}
}

// filter 把暂停表的事件整条丢掉（不写、不计指纹），事务边界事件原样保留。
//
// 返回值：保留的事件、被跳过的表（目标端裸表名 → 该表这一批丢了几个行事件）。
// 一行 = 一个行事件：行式复制下 Insert/Update/Delete 都是一个事件一行，
// Truncate 记 1（它不带行，只代表「这张表被清空过」，重搬会覆盖）。
func (p *pauseSink) filter(evs []source.Event) (kept []source.Event, skipped map[string]int64, rows map[string]int64) {
	skipped = map[string]int64{}
	rows = map[string]int64{}
	if len(evs) == 0 {
		return evs, skipped, rows
	}
	out := make([]source.Event, 0, len(evs))
	for _, ev := range evs {
		switch ev.Kind {
		case source.EvBegin, source.EvCommit, source.EvHeartbeat:
			out = append(out, ev) // 事务边界不属于任何表
			continue
		}
		key := p.tableKey(ev.Table)
		if key == "" {
			out = append(out, ev)
			continue
		}
		if _, paused := p.r.pausedRecord(key); !paused {
			out = append(out, ev)
			continue
		}
		if skipped[key] == 0 {
			skipped[key] = 0
		}
		skipped[key]++
		if ev.Kind != source.EvDDL {
			rows[key]++
		}
	}
	return out, skipped, rows
}

// tableKey 把事件里的表引用换算成 `paused_tables` 的 key（目标端裸表名）。
// 空表名（没有行的事件）返回空串：拿空串去查暂停集合只会误判。
func (p *pauseSink) tableKey(ref source.TableRef) string {
	if ref.Table == "" {
		return ""
	}
	return p.r.targetRef(ref).Table
}

// noteSkipped 记一笔欠账。**记账失败不让这一批失败**：数据已经落库、位点已经前进，
// 这时把 run 判失败反而制造「写成功了却报失败」的假象；账错了顶多数字不准，不影响正确性。
func (p *pauseSink) noteSkipped(ctx context.Context, skipped map[string]int64, rows map[string]int64, pos source.Position) {
	for key, n := range skipped {
		if err := p.r.pauseReg.NoteSkipped(ctx, source.TableRef{Schema: p.r.targetSchema(), Table: key},
			rows[key], posString(pos)); err != nil {
			p.r.note(ctx, "记录表 "+key+" 的跳过窗口失败（不影响同步，只是数字不准）："+err.Error())
		}
		_ = n
	}
}

// ── 引擎持有的暂停集合 ───────────────────────────────────────────

// loadPausedSet 把目标端那份记录读进内存。prepare 读一次就够：写路径每批都要判，
// 不能每批查库；本轮里它只在「新登记一张表」和「某表重搬搬成」两处变化。
func (r *runner) loadPausedSet(ctx context.Context) error {
	rec, err := r.pauseReg.PausedTables(ctx)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.paused = rec
	r.mu.Unlock()
	return nil
}

func (r *runner) pausedRecord(key string) (pgsink.PausedTable, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rec, ok := r.paused[key]
	return rec, ok
}

func (r *runner) rememberPaused(key string, rec pgsink.PausedTable) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.paused == nil {
		r.paused = map[string]pgsink.PausedTable{}
	}
	r.paused[key] = rec
}

func (r *runner) forgetPaused(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.paused, key)
}
