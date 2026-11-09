package engine

import (
	"context"
	"fmt"

	"github.com/spencerswagger/jinliang/internal/pgsink"
	"github.com/spencerswagger/jinliang/internal/pgsource"
	"github.com/spencerswagger/jinliang/internal/source"
)

// 块级续跑（docs/SNAPSHOT.md §4、docs/RUNTIME.md §2）。
//
// 结论只有三种，写成一个纯函数（planCopy），因为它是这个能力里**唯一需要人反复看**的
// 判断：什么时候接着搬、什么时候必须整轮重搬、什么时候干脆不用搬。
//
//	① 续跑：上次搬到一半就断了 —— 从最后一块的右边界接着读，已搬完的表跳过；
//	② 重搬：书签追不回原来的位点（或本轮本来就该重搬）—— 清掉本任务建的表、清标记、从头来；
//	③ 不搬：存量已经在目标端了。
//
// 「搬到一半」不再靠 _jl_meta.phase 猜：块标记（含表完成标记）才是事实。
// phase 只在**升级兼容**里用一次——老状态没有块标记，只能按阶段判。
type copyKind int

const (
	copySkip   copyKind = iota // 存量已搬完，这次不用搬
	copyFresh                  // 从头搬（新书签 + 新导出的视图）
	copyResume                 // 从已提交的块边界续跑
)

func (k copyKind) String() string {
	switch k {
	case copySkip:
		return "skip"
	case copyFresh:
		return "fresh"
	case copyResume:
		return "resume"
	}
	return "unknown"
}

// chunkPlan is the decision plus everything the snapshot phase needs to execute it.
type chunkPlan struct {
	Kind copyKind
	// From 是每张表的续跑起点（只有 copyResume 的未完成表才有条目）。
	From map[string]pgsource.ChunkResume
	// Skip 是「上次已经搬完、这次不用碰」的表。
	Skip map[string]bool
	// Recopy 是不能块级续跑、必须整表重来的表（无主键表：keyset 游标不稳）。
	Recopy []source.TableRef
	// Interrupted 表示「上次的全量搬到一半就断了」：目标端里那些行既不全也可能过期，
	// 从头搬之前必须先清掉（只清本任务建的表）。上次已经搬完的那种「重搬」不清——
	// 那属于 once 的语义，清不清结果都一样，不清少一次删数据的窗口。
	Interrupted bool
	// Why 是给人看的一句话：为什么续跑 / 为什么重搬。
	Why string
}

// copyInputs 是 planCopy 的全部输入：目标端状态 + 源端书签 + 任务口径。
// 全部显式传入（不读全局、不连库），这样这张判定表能被穷举测。
type copyInputs struct {
	// Mode 是 mode.frequency 的原文（once / full_interval / continuous / interval / 空）。
	Mode string
	// AlwaysCopy 表示「每跑一轮就该重搬一轮」（once / full_interval）。
	AlwaysCopy bool
	// Tables 是本次选中的表。
	Tables []source.Table
	// Progress 是目标端 _jl_chunk 折出来的每表进度。
	Progress map[string]pgsink.ChunkProgress
	// StoredCP 是 _jl_meta.position（上次定下的一致性起点）。
	StoredCP string
	// SlotOK 表示源端书签还在（不在就追不回旧位点之后的变更）。
	SlotOK bool
	// SlotAhead 表示书签已确认的位点**越过了** StoredCP：中间那段变更已被丢弃。
	SlotAhead bool
	// SlotCP 是书签已确认位点的原文（只用于把「哪一条不成立」写清楚）。
	SlotCP string
	// PriorPhase 是 _jl_meta.phase，只给「没有块标记的老状态」兜底用。
	PriorPhase string
	// Capable 表示目标端会记块进度（PG 目标端；MySQL 全量路径本版还没有块记账）。
	Capable bool
}

// planCopy decides how this run copies the initial data.
func planCopy(in copyInputs) chunkPlan {
	p := chunkPlan{From: map[string]pgsource.ChunkResume{}, Skip: map[string]bool{}}
	if len(in.Tables) == 0 {
		p.Kind = copySkip
		return p
	}
	if in.Mode == "interval" || in.Mode == "continuous" {
		// 这两种模式本来就不做初始全量（interval 靠水位列追，continuous 只接增量）。
		p.Kind = copySkip
		return p
	}

	anyMarker, allDone := false, true
	for _, t := range in.Tables {
		pr := in.Progress[t.Table]
		if pr.Done {
			p.Skip[t.Table] = true
		} else {
			allDone = false
		}
		if pr.Done || pr.LastNo > 0 {
			anyMarker = true
		}
	}

	interrupted := !allDone && anyMarker

	if !in.Capable {
		// 目标端不记块进度：只能整轮重搬（与接入块记账之前的行为一致）。
		switch {
		case allDone && !in.AlwaysCopy:
			p.Kind = copySkip
		case !anyMarker && initialCopyDone(in.PriorPhase, in.StoredCP) && !in.AlwaysCopy:
			p.Kind = copySkip
		default:
			p.Kind = copyFresh
			p.Why = "目标端不记块进度：这一轮从头搬"
		}
		p.Interrupted = interrupted
		return p
	}

	if allDone {
		if in.AlwaysCopy {
			p.Kind = copyFresh
			p.Why = "这一轮按模式重搬全量（上次已经搬完）"
			return p
		}
		p.Kind = copySkip
		return p
	}

	// 到这儿：至少有一张表没搬完。
	if !anyMarker && initialCopyDone(in.PriorPhase, in.StoredCP) {
		// 老状态（升级前跑的）：没有块标记，只能按阶段认「存量已经搬完」。
		if !in.AlwaysCopy {
			p.Kind = copySkip
			p.Why = "旧版本没有块进度记录，按阶段（" + in.PriorPhase + "）认定存量已搬完"
			return p
		}
		p.Kind = copyFresh
		p.Interrupted = interrupted
		p.Why = "旧版本没有块进度记录，这一轮整轮重搬"
		return p
	}

	if in.StoredCP == "" || !in.SlotOK || in.SlotAhead {
		p.Kind = copyFresh
		p.Interrupted = interrupted
		switch {
		case in.StoredCP == "":
			p.Why = "目标端没有可用的一致性位点（_jl_meta.position 为空），这一轮从头搬"
		case !in.SlotOK:
			p.Why = "源端书签已经不在了（被清理，或 WAL 已被 max_slot_wal_keep_size 丢掉）：" +
				"从目标位点 " + in.StoredCP + " 往后那段变更重放不出来，这一轮从头搬"
		default:
			p.Why = "源端书签已经越过目标端的位点（书签确认到 " + in.SlotCP +
				"，目标位点是 " + in.StoredCP + "，中间那段变更已被清理）：这一轮从头搬"
		}
		return p
	}

	// 续跑：有主键的表从最后一块的右边界接着读；无主键表只能整表重来。
	// 遍历顺序就是 in.Tables 的顺序（调用方给的是稳定的表顺序）。
	for _, t := range in.Tables {
		pr := in.Progress[t.Table]
		if pr.Done {
			continue
		}
		ref := source.TableRef{Schema: t.Schema, Table: t.Table}
		if !t.HasStablePK {
			// 无主键：游标是 ctid，行被更新/整理过位置就变，续跑可能漏读或读重。
			// 只重来这一张表，不拖累别的表（docs/SNAPSHOT.md §1）。
			if pr.LastNo > 0 {
				p.Recopy = append(p.Recopy, ref)
			}
			continue
		}
		if pr.LastNo > 0 {
			p.From[t.Table] = pgsource.ChunkResume{Cursor: pr.Cursor, LastNo: pr.LastNo}
		}
	}
	p.Kind = copyResume
	p.Interrupted = interrupted
	p.Why = fmt.Sprintf("上次的全量没搬完：%d 张表从已提交的块边界续跑，%d 张表已经搬完",
		len(p.From), len(p.Skip))
	if len(p.Recopy) > 0 {
		p.Why += fmt.Sprintf("，%d 张无主键表整表重来", len(p.Recopy))
	}
	return p
}

// decideCopyPlan reads the two facts a decision needs from outside: the target's
// chunk progress (our own truth) and the source slot's confirmed position (can
// the old checkpoint still be replayed?). Everything else comes from the job doc.
func (r *runner) decideCopyPlan(ctx context.Context, src *pgsource.Source,
	phase, pos string) (chunkPlan, error) {

	prog, err := r.sink.ChunkProgress(ctx)
	if err != nil {
		return chunkPlan{}, fmt.Errorf("读目标端的块进度失败: %w", err)
	}
	in := copyInputs{
		Mode:       r.mode(),
		AlwaysCopy: r.mode() == "once" || r.mode() == "full_interval",
		Tables:     r.selectedTables(),
		Progress:   prog,
		StoredCP:   pos,
		PriorPhase: phase,
		// prepare() 只服务 PG 目标端（MySQL 目标端走 executeGenericFullCopy），
		// 而 PG 目标端就是记块进度的那个。
		Capable: true,
	}
	if in.Mode == "interval" || in.Mode == "continuous" || pos == "" {
		return planCopy(in), nil
	}
	cp, err := src.ParsePosition(pos)
	if err != nil {
		// 位点串读不懂（换过源类型 / 手改过）：不当成可续跑，也不在这里失败——
		// 后面按「从头搬」走，用户在动作清单里能看到原因。
		r.note(ctx, "目标端的一致性位点读不懂（"+pos+"）：这一轮不会续跑。原因："+err.Error())
		return planCopy(in), nil
	}
	pgPos, ok := cp.(pgsource.Position)
	if !ok {
		r.note(ctx, fmt.Sprintf("目标端位点的类型是 %T，不是 PG 位点：这一轮不会续跑", cp))
		return planCopy(in), nil
	}
	slot, ok, err := src.SlotConfirmed(ctx)
	if err != nil {
		return chunkPlan{}, fmt.Errorf("读源端书签的确认位点失败: %w", err)
	}
	in.SlotOK = ok
	in.SlotCP = slot.String()
	// 书签确认位点**越过**目标位点：中间那段变更已经被源端清理，追不回来了。
	in.SlotAhead = ok && !slot.IsZero() && slot.Compare(pgPos) > 0
	return planCopy(in), nil
}

// parsedCP is the stored checkpoint as a source position (used by the resume
// path, which deliberately keeps the old checkpoint instead of taking a new one).
func parsedCP(src *pgsource.Source, raw string) pgsource.Position {
	cp, err := src.ParsePosition(raw)
	if err != nil {
		return pgsource.Position{}
	}
	if p, ok := cp.(pgsource.Position); ok {
		return p
	}
	return pgsource.Position{}
}
