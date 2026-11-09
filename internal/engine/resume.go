package engine

import (
	"context"
	"fmt"

	"github.com/spencerswagger/jinliang/internal/pgsink"
	"github.com/spencerswagger/jinliang/internal/pgsource"
	"github.com/spencerswagger/jinliang/internal/source"
	"github.com/spencerswagger/jinliang/internal/state"
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

// cleanScope says which tables must be emptied before this run copies again.
type cleanScope int

const (
	cleanNone cleanScope = iota // 不搬，或这次搬的是空表（没有我们写过的行）
	// cleanAll：**凡是从头搬就清**（Ark 口径 2026-11-10）。理由不是「位点坏了」，
	// 而是「BulkApply 只有 upsert、没有 delete」：要让本轮结果等于源端当前状态，
	// 清空重建是唯一实现。清哪些表由 CleanNames 决定（只清我们写过的表）。
	cleanAll
)

// chunkPlan is the decision plus everything the snapshot phase needs to execute it.
type chunkPlan struct {
	Kind copyKind
	// From 是每张表的续跑起点（只有 copyResume 的未完成表才有条目）。
	From map[string]pgsource.ChunkResume
	// Skip 是「上次已经搬完、这次不用碰」的表。
	Skip map[string]bool
	// Recopy 是不能块级续跑、必须整表重来的表（无主键表：keyset 游标不稳）。
	Recopy []source.TableRef
	// Clean 表示「从头搬之前要先清掉目标端里我们上次写进去的行」。
	//
	// Ark 口径（2026-11-10）：判据是**这一轮是不是从头搬**，不是「位点坏没坏」——
	// BulkApply 只有 upsert、没有 delete，要让「本轮结果 = 源端当前状态」，只能清空重建。
	// 绑在位点状态上会让同一模式因位点不同而对 delete 表现不同（第 4 轮那个病）。
	//
	// 清哪些表看 CleanNames：只清**我们写过的**表（有块标记）。没写过的表没有脏行，
	// 也不能清——append 模式复用的用户表里全是用户自己的行。
	Clean      cleanScope
	CleanNames []string
	// CleanWhy 决定「为什么必须清」的说法（用户照这句话决定下一步）。
	CleanWhy recopyReason
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
	// SlotReplayable 表示源端书签还能从旧位点重放（书签在、且没被
	// max_slot_wal_keep_size 丢掉 WAL）。注意它**不等于**「书签存在」：
	// wal_status=lost 的书签还在，但一段变更都放不出来。
	SlotReplayable bool
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
//
// 判据是什么、从哪来，比结论本身重要：
//   - 「可重算的事实」放在最前面（位点、书签能不能重放），它们只看库里的状态，
//     和我们这一轮写过什么无关，所以崩多少次、重跑多少轮，答案都一样；
//   - 「块进度」决定续跑还是重搬，证据是 _jl_chunk，也是库里的状态；
//   - 唯一靠 _jl_meta.phase 的，是没有块标记的老状态（升级兼容），而且这一条
//     **只在没有可重算事实可用时**才轮到它。
//
// 反过来做（先按 phase 判、再补可重放性）已经踩过一次：snapshot() 一开始就
// phaseTo(Snapshot)，我们自己把 phase 改写了，于是「清表清到一半崩了」再重跑时
// 阶段变成了 snapshot → initialCopyDone 假 → 退化成「不清表重搬」，空洞期里源端
// 删掉的行就永远留在目标端（退出码 0）。所以 cleanAll 的判据必须是可重算的。
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

	doneCount, anyMarker, allDone := 0, false, true
	doneTables := map[string]bool{}
	for _, t := range in.Tables {
		pr := in.Progress[t.Table]
		if pr.Done {
			doneCount++
			doneTables[t.Table] = true
		} else {
			allDone = false
		}
		if pr.Done || pr.LastNo > 0 {
			anyMarker = true
		}
	}
	interrupted := !allDone && anyMarker

	// ① 可重算的事实：位点还在，但书签追不回它 —— 中间那段变更永远补不回来。
	// 这时无论上次是搬完、搬到一半、还是升级前根本没记块，结论都一样：
	// 整轮重搬，**并且先把上次搬进去的行清掉**（BulkApply 只有 upsert，没有 delete，
	// 不清就会把源端在这段空洞里删掉的行永远留在目标端）。
	unreplayable := in.StoredCP != "" && (!in.SlotReplayable || in.SlotAhead)
	if in.Capable && unreplayable {
		p.Kind = copyFresh
		p.markClean(in, recopyGap)
		p.Why = unreplayableWhy(in)
		return p
	}

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
		return p
	}

	// ② 存量与位点对得上：看要不要搬。
	if allDone {
		if in.AlwaysCopy {
			// once / full_interval：每轮都是整轮重搬 → 每轮都清（否则源端删掉的行永远补不掉，
			// 同一模式还会因位点状态不同而对 delete 表现不同）。
			p.Kind = copyFresh
			p.markClean(in, recopyOnce)
			p.Why = "这一轮按模式整轮重搬（once / full_interval 每轮重建，目标端结果等于源端当前状态）"
			return p
		}
		p.Kind = copySkip
		p.Skip = allSkip(in.Tables)
		return p
	}

	// ③ 没有块标记的老状态（升级兼容）：只按阶段认「存量已经搬完」。
	//    可重放的判断已经在 ① 做过了，这里不用再看一遍。
	if !anyMarker && initialCopyDone(in.PriorPhase, in.StoredCP) {
		if !in.AlwaysCopy {
			p.Kind = copySkip
			p.Skip = allSkip(in.Tables)
			p.Why = "旧版本没有块进度记录，按阶段（" + in.PriorPhase + "）认定存量已搬完"
			return p
		}
		// once / full_interval：这一轮本来就要重搬，别让它落到「续跑」上——
		// 那样既没有新视图也没有新书签，语义和「从头搬」不一样。
		p.Kind = copyFresh
		p.markClean(in, recopyOnce)
		p.Why = "旧版本没有块进度记录，这一轮整轮重搬"
		return p
	}

	// ④ 位点缺失（书签那两种情况已在 ① 处理）：只能从头搬。
	if in.StoredCP == "" {
		p.Kind = copyFresh
		why := recopyHalfCopy
		if !interrupted {
			// 搬过但一块都没落地（所以没有位点）：既不是「字签追不回」，也不是
			// once/full_interval 的模式语义 —— 单独说清，别让 continuous+initial 的
			// 用户看到「once / full_interval」这种不相干的话。
			why = recopyNoPos
		}
		p.markClean(in, why)
		p.Why = "目标端没有可用的一致性位点（_jl_meta.position 为空），这一轮从头搬"
		return p
	}

	// ⑤ 位点在、书签也能重放：从最后一块的右边界续跑。
	// 无主键表的游标是 ctid，行被更新/整理过位置就变，续跑可能漏读或读重 →
	// 只重来这一张表，不拖累别的表（docs/SNAPSHOT.md §1）。
	for _, t := range in.Tables {
		pr := in.Progress[t.Table]
		if pr.Done {
			continue
		}
		ref := source.TableRef{Schema: t.Schema, Table: t.Table}
		if !t.HasStablePK {
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
	// Skip 只影响「收尾时要不要给这张表盖完成标记」：续跑时已经搬完的表不必再盖一遍。
	// 注意它**不影响清表**——Clean 只对「从头搬」有意义，续跑保持 cleanNone。
	p.Skip = doneTables
	p.Why = fmt.Sprintf("上次的全量没搬完：%d 张表从已提交的块边界续跑，%d 张表已经搬完",
		len(p.From), doneCount)
	if len(p.Recopy) > 0 {
		p.Why += fmt.Sprintf("，%d 张无主键表整表重来", len(p.Recopy))
	}
	return p
}

// markClean 给「从头搬」这一轮挂上清表信息：清哪些表（我们写过的）与为什么清。
// 没有标记的表格子不进去——那些表里没有我们的行，清了就是删用户数据。
func (p *chunkPlan) markClean(in copyInputs, why recopyReason) {
	p.Clean = cleanAll
	p.CleanWhy = why
	p.CleanNames = nil
	for _, t := range in.Tables {
		pr := in.Progress[t.Table]
		if pr.Done || pr.LastNo > 0 {
			p.CleanNames = append(p.CleanNames, t.Table)
		}
	}
	if len(p.CleanNames) == 0 && copiedBefore(in) {
		// 老状态（升级前跑过）：整本块账都没有，但目标端留下了「搬过」的证据。
		// 这时无从分辨哪张表有我们的行，只能整批清——不清就补不掉这段时间的删除。
		for _, t := range in.Tables {
			p.CleanNames = append(p.CleanNames, t.Table)
		}
	}
	if len(p.CleanNames) == 0 {
		// 这个任务从没往目标端写过东西（首次搬）：没有可清的行。
		// 也不能清——append 模式复用的用户表里全是用户自己的行。
		p.Clean = cleanNone
	}
}

// copiedBefore 判断「目标端有没有『这个任务搬过』的证据」——只用于兜底那格
// （块账整本缺失的老状态）。判据刻意窄：
//
//   - 有位点：搬过（位点随块/批次提交写入）。注意 prepare 也会先写一个起点位点，
//     所以它的含义是「可能搬过」，真正能不能删仍由 DidCreateTable 把关；
//   - 没有位点但阶段已经是 snapshot：说明至少进过全量的读阶段。
//
// **不能用「phase 非空」**：EnsureMeta 在读状态之前就写了 phase='prepare'，首次跑
// phase 也非空 —— 拿它当证据会把第一次搬判成「要清表」，append 复用用户表时首个
// 轮次就被拒（踩过一次，别再退回去）。
func copiedBefore(in copyInputs) bool {
	return in.StoredCP != "" || in.PriorPhase == string(state.Snapshot)
}

// allSkip marks every selected table as "already done" (used only when this run
// copies nothing at all).
func allSkip(tables []source.Table) map[string]bool {
	out := make(map[string]bool, len(tables))
	for _, t := range tables {
		out[t.Table] = true
	}
	return out
}

// unreplayableWhy 说清「哪一条不成立」——它是用户唯一能拿去做决定的线索。
func unreplayableWhy(in copyInputs) string {
	switch {
	case !in.SlotReplayable:
		return "源端书签追不回目标端的位点（书签不在了，或 WAL 已被 max_slot_wal_keep_size " +
			"丢掉）：位点 " + in.StoredCP + " 之后那段变更重放不出来，只能整轮重搬"
	default:
		return "源端书签已经越过目标端的位点（书签确认到 " + in.SlotCP +
			"，目标位点是 " + in.StoredCP + "，中间那段变更被清理了）：只能整轮重搬"
	}
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
	in.SlotReplayable = ok
	in.SlotCP = slot.String()
	// 书签确认位点**越过**目标位点：中间那段变更已经被源端清理，追不回来了。
	in.SlotAhead = ok && !slot.IsZero() && slot.Compare(pgPos) > 0
	plan := planCopy(in)
	// 只剩一种组合会落到「不搬 + 重放不了」：continuous 模式（它本来就不搬存量）。
	// 这时硬着头皮从新书签开始流，等于把中间那段变更静默扔掉——不如说清楚停下。
	if plan.Kind == copySkip && (!in.SlotReplayable || in.SlotAhead) && in.Mode == "continuous" {
		return plan, &unreplayableSlotError{stored: pos}
	}
	return plan, nil
}

// unreplayableSlotError 是「已经同步过，但源端那段变更再也拿不到了」。
//
// 它不是我们能自己修好的事：要么整轮重搬，要么接受一段空洞。所以它必须停下来
// 让用户选，而不是把位点悄悄往前挪——那是最坏的一种「看起来成功」。
type unreplayableSlotError struct{ stored string }

func (e *unreplayableSlotError) Error() string {
	return "源端书签追不回目标端的位点了（书签不在了，或 WAL 已被 max_slot_wal_keep_size 丢掉）：" +
		"位点 " + e.stored + " 之后那段变更重放不出来，而这个任务只接增量、不搬存量。\n" +
		"你现在能做什么：1) 让它整轮重搬一次（把 mode.frequency 改成 once 跑一轮，再改回 continuous）；" +
		"2) 确认这段空洞可以接受后重建任务。\n" +
		"风险：整轮重搬期间目标端会短暂与源端不一致；放着不管则会永久缺这一段变更。"
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
