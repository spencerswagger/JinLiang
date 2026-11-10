package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/source"
)

// errResyncWithoutSnapshot 是「说了要整轮重搬却没搬」的兜底。
//
// 这类错误必须响亮：`--resync` 承诺的是「重建 + 整轮重搬」，如果代码路径没走到
// 快照（例如以后有人改了 needSnapshot / plan 的判据），用户会拿到一个「成功」退出码，
// 而数据一行都没重搬——那比失败更糟。断言放在 execute() 的收尾，任何一处漏改都用例必红。
var errResyncWithoutSnapshot = errors.New(
	"内部错误：--resync 没有走到整轮重搬（位点没清掉或快照被跳过）——已停下，不会假装成功")

// `--resync` = 显式整轮重搬（口径 2026-11-10 定死）：
//
//	重建书签/位点 + 按 copyFresh 口径先清「本任务建的表」再整轮重搬。
//
// 三条不许动的边界：
//  1. **不换 job_key、不删任务**。「删任务重建」会丢审计与运行历史，还会换身份；
//     job_key 变化是 --accept-change 那条路的事，不是自救动作。
//  2. **只清本任务写过的表**（`_jl_chunk` 里有标记的表）。复用用户已有表时，
//     表里的行不全是我们的——清掉就是删用户数据，所以 recopyAction 仍然 fail-closed。
//  3. **清表不清暂停记录**：`paused_tables` 是「后果记录」，与本轮「清表」维度不同；
//     某表重搬成功后只清该表那一条，失败则原样留着（清表只动数据 + 块标记）。
//
// 为什么这个能力必须存在：E3001（位点与源库的变更流水接不上）没有第二条出路——
// 那段变更已经被源库清掉，只能整轮重搬。文案里写着 `--resync` 而 CLI 没有这个旗标，
// 等于把唯一的自救路径写成了空头承诺（用户照着敲只会撞 unknown flag）。

// ResyncReport 是 `--resync` 实际做了什么。CLI 打印、审计落库、用例断言都用这一份，
// 不让任何一方自己再拼一遍「我猜它清了哪几张表」。
type ResyncReport struct {
	// OldPhase / OldPosition 是清之前的进度（审计要看「从哪重建」）。
	OldPhase    string
	OldPosition string
	// Slot 是本次重建的源端同步书签（continuous 模式的「重建」也落在它上面）。
	Slot string
	// Tables 是**真的清空过**的表（清成一张填一张，按 catalog 顺序）。
	// 计划清单不在这里——它只进审计的 planned_tables，别把计划当已完成报告。
	Tables []string
}

// checkResync 在**动任何数据之前**判断这个任务能不能 --resync。
//
// 失败得很早很响：不清表、不建书签、不加油门，用户拿到的是四段式而不是
// 「跑了一半才告诉你这旗标不管用」。
func (r *runner) checkResync() error {
	if !r.opt.Resync {
		return nil
	}
	if r.tgtEP.Kind != dsn.KindPostgres {
		return fmt.Errorf("目标端是 %s：本版它没有同步书签/位点可重建（这条路径本来就是每轮整表全量，"+
			"而且目标端所有权还没登记，分不清哪些表是本任务建的）。"+
			"你现在能做什么：去掉 --resync 直接跑（它就是全量）。"+
			"风险：没有（什么都没动）", r.tgtEP.Kind)
	}
	if r.mode() == "interval" {
		return fmt.Errorf("interval 模式没有同步书签：「重建位点」对它没有意义（它的进度是每张表的水位），" +
			"而按表清空重搬还没落地。" +
			"你现在能做什么：把 mode.frequency 改成 full_interval 跑一轮（每轮整表重搬；源端已删的行不会因此消失）；" +
			"或者手动清空目标表后再跑 interval。" +
			"风险：没有（什么都没动）")
	}
	if r.mode() == "continuous" && r.srcEP.Kind != dsn.KindPostgres {
		// continuous 只接增量，--resync 的全部动作就是「把位点挪到源端当前位置」。
		// PG 源靠书签的 confirmed_flush_lsn 回答这个问题；别的源还没有这个入口，
		// 硬做只会写出「审计说清了位点、位点其实被原样写回」的假动作。
		return fmt.Errorf("continuous 模式只接增量，「重新开始」要靠源端书签的当前位置；"+
			"%s 源没有书签，本版还不能替你决定从哪个位点重新开始。"+
			"你现在能做什么：把 mode.frequency 改成 continuous+initial 跑一次 --resync（整轮重搬），再改回 continuous。"+
			"风险：没有（什么都没动）", r.srcEP.Kind)
	}
	return nil
}

// applyResync 把「常规续跑判定」改成「显式整轮重搬」。
//
// 刻意**复用** planCopy 的 clean 机制（cleanAll + CleanNames + cleanBeforeRecopy）而不是
// 另写一套清表：清表前的「这张表是不是本任务建的」判定（fail-closed）、清数据与清块标记
// 同事务、崩溃后按标记续判——这些都已经在那一处写对了，第二套迟早分叉。
//
// 唯一改的是**判据**：常规判定看「位点还追不追得回、上次搬到哪」，而 --resync 是
// 用户显式要求「不管你上次到哪，整轮重来」。所以 From/Skip 清空、Kind 直接落 copyFresh。
func (r *runner) applyResync(p *chunkPlan, priorPhase, priorPos string) error {
	// checkResync 是幂等的：execute() 已经在动数据之前判过一次（那里判是为了「不支持
	// 就一个字节都不动」），这里再判一次是为了让这个函数单独被调用时也安全（用例这么用）。
	if err := r.checkResync(); err != nil {
		return err
	}
	rep := &ResyncReport{OldPhase: priorPhase, OldPosition: priorPos, Slot: r.slot()}
	r.resyncReport = rep

	if r.mode() == "continuous" {
		// 只接增量：没有存量可重搬。--resync 在这里的含义是「清空位点、按书签
		// 当前位置重新开始」——这正是 unreplayableSlotError 给的第二个选项
		//（接受中间那段空洞），只是这次由用户显式选择，而不是我们替他挪位点。
		p.Kind = copySkip
		p.Clean = cleanNone
		p.CleanNames = nil
		p.From = map[string]chunkFrom{}
		p.Skip = map[string]bool{}
		p.Recopy = nil
		p.Why = "--resync：continuous 模式不搬存量，本次只清空位点、按书签当前位置重新开始" +
			"（位点与当前书签之间那段变更接受丢弃）"
		return nil
	}

	p.Kind = copyFresh
	p.From = map[string]chunkFrom{}
	p.Skip = map[string]bool{}
	p.Recopy = nil
	if len(p.Written) == 0 && p.CopiedBefore {
		// 老状态（升级前跑过、没有块账）：目标端确实有我们的行，但分不清哪几张表。
		// 与 markClean 同一口径：只能整批清，不清就补不掉这段时间的删除。
		p.Written = allNames(r.selectedTables())
	}
	p.setClean(recopyResync)
	p.Why = "--resync：重建同步书签并从零整轮重搬（目标端结果等于源端当前状态）"
	return nil
}

// precheckResyncTables 在**任何破坏性动作之前**把「这几张表能不能清」问一遍。
//
// 为什么必须前置：清位点、重建书签、清表是三步不可逆的动作。把「这是你自己的表、
// 清不清由你定」这种需要人来决定的失败留到第三步，前面的位点和旧书签就已经没了；
// 用户「再跑一次」会被判 copySkip（块标记还在、Done 齐）直接进流，把
// (旧位点, 新起点] 永久跳过且不报错——他要的是整轮重搬，代码却替他接受了空洞。
//
// 这里复用 recopyAction（同一个 fail-closed 判定），失败时位点与书签都还没动，
// 用户看到的是原本那三条可选路，而不是一个被弄坏的现场。
func (r *runner) precheckResyncTables(ctx context.Context, p chunkPlan) error {
	if p.Clean == cleanNone || len(p.CleanNames) == 0 {
		return nil
	}
	for _, name := range p.CleanNames {
		ref, ok := r.tableByName(name)
		if !ok {
			continue // 与 cleanBeforeRecopy 同口径：不在本轮范围内的名字跳过
		}
		if err := r.recopyAction(ctx, ref, p.CleanWhy); err != nil {
			return err
		}
	}
	return nil
}

// auditResync 记录「谁触发、重建哪个书签、**计划**清哪些表」。
//
// 放在清表之前（prepare 里，清位点之后）：审计失败必须响亮——再往后就是删数据，
// 而用户有权知道这一步是谁按的。位点已清 + 计划是 copyFresh，所以就算这里失败，
// 下一次 run 也会整轮重搬，不会停在「位点空了、表还是旧的」。
//
// 字段叫 planned_tables 而不是 cleared_tables：此刻一张表都还没清。真正清完哪些表
// 由 auditResyncCleared 另记一条，两段各说事实，不拿计划冒充结果。
func (r *runner) auditResync(ctx context.Context, planned []string) error {
	rep := r.resyncReport
	if rep == nil {
		return nil
	}
	actor := strings.TrimSpace(r.opt.Actor)
	if actor == "" {
		actor = control.ActorSystem
	}
	if r.opt.Store != nil {
		if err := r.opt.Store.Audit(ctx, actor, "job.resync", map[string]any{
			"job_id": r.job.ID, "job_key": r.job.JobKey, "job_name": r.job.Name,
			"slot": rep.Slot, "old_phase": rep.OldPhase, "old_position": rep.OldPosition,
			"planned_tables": planned,
		}); err != nil {
			return fmt.Errorf("--resync 已清空位点，但写审计失败：%w（目标表一行都没动）", err)
		}
	}
	r.note(ctx, fmt.Sprintf("--resync：重建同步书签 %s，位点从 %q 清空；接下来会清空 %d 张本任务写过的表再整轮重搬：%s",
		rep.Slot, rep.OldPosition, len(planned), strings.Join(planned, ", ")))
	return nil
}

// auditResyncPartial 记录「清到一半失败时**已经**清掉哪些表」。
//
// 为什么必须有这一条：`job.resync.cleared` 只在全部清完时写；清到第三张时权限没了
// 或连接断了，审计里就只剩「计划清 5 张」——用户回头查「到底清了几张」只能去翻
// 事件流，而事件流进程一重启就没了。这条把「已经发生的破坏」钉在库里。
func (r *runner) auditResyncPartial(ctx context.Context, cause error) error {
	rep := r.resyncReport
	if rep == nil || r.opt.Store == nil {
		return nil
	}
	actor := strings.TrimSpace(r.opt.Actor)
	if actor == "" {
		actor = control.ActorSystem
	}
	return r.opt.Store.Audit(ctx, actor, "job.resync.partial", map[string]any{
		"job_id": r.job.ID, "job_key": r.job.JobKey, "job_name": r.job.Name,
		"slot": rep.Slot, "cleared_tables": rep.Tables, "error": cause.Error(),
	})
}

// auditResyncCleared 记录「**真的**清了哪些表」——在 cleanBeforeRecopy 跑完之后写。
//
// 为什么要两条：计划可以中途失败（清到第三张时权限没了），一条审计既说「谁按的」
// 又说「清了哪些」就只能二选一，另一个必是假话。
func (r *runner) auditResyncCleared(ctx context.Context) error {
	rep := r.resyncReport
	if rep == nil || r.opt.Store == nil {
		return nil
	}
	actor := strings.TrimSpace(r.opt.Actor)
	if actor == "" {
		actor = control.ActorSystem
	}
	if err := r.opt.Store.Audit(ctx, actor, "job.resync.cleared", map[string]any{
		"job_id": r.job.ID, "job_key": r.job.JobKey, "job_name": r.job.Name,
		"slot": rep.Slot, "cleared_tables": rep.Tables,
	}); err != nil {
		return fmt.Errorf("清表已完成（%d 张），但写审计失败：%w", len(rep.Tables), err)
	}
	return nil
}

// slotRebuilder 是「能强制重建同步书签」的源端能力（continuous 模式的 --resync 用它）。
//
// 这条路径没有存量可搬，--resync 的全部意义就是「把变更流挪到源端当前位置」：
// 先删掉旧书签，下一次 Prepare 会按当前位置建一条新的。没有这个能力的源端在
// checkResync 里就被挡住了，不会走到这里。
type slotRebuilder interface {
	RebuildSlot(ctx context.Context) error
}

// pauseClearer 是「按表暂停记录」（`_jl_meta.paused_tables`）的可选能力。
//
// 为什么用可选接口而不是直接调 pgsink：按表暂停的记录还没在本分支的 main 上，
// 直接用会编译不过；用接口可以现在就把「搬成才解除、失败保留」的时序写对，
// 等能力落地（同名同签名）自动接上，不必再改引擎一次——也就不会出现
// 「能力上线了、引擎忘了清」的第二种状态。
type pauseClearer interface {
	ClearPausedTable(ctx context.Context, ref source.TableRef) error
}

// clearPauseRecords 在整轮重搬**成功之后**只清每张表自己的暂停记录。
//
// 时序是契约的一部分（「清表不清记录、搬成清该表、搬败留记录」）：
//   - 清表时保留记录（表可能还是不一致的，清完照样会失败）；
//   - 搬成后才解除（结构核对通过 **且** 存量搬完，才等于既对齐又有数据）。
func (r *runner) clearPauseRecords(ctx context.Context) error {
	pc, ok := any(r.sink).(pauseClearer)
	if !ok {
		// 本版目标端还没有暂停记录：没有要清的，但要说出来（别让用户以为我们漏了）。
		r.note(ctx, "--resync：目标端本版还没有「按表暂停」记录，这一步没有要清的")
		return nil
	}
	var cleared []string
	for _, ref := range r.tables {
		if err := pc.ClearPausedTable(ctx, ref); err != nil {
			return fmt.Errorf("重搬已成功，但清不掉 %s 的暂停记录（它还会显示为暂停）: %w", ref.Table, err)
		}
		cleared = append(cleared, ref.Table)
	}
	if len(cleared) > 0 {
		r.note(ctx, "整轮重搬成功，解除这些表的暂停记录："+strings.Join(cleared, ", "))
	}
	return nil
}
