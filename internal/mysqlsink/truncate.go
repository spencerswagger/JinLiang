package mysqlsink

import (
	"context"
	"fmt"

	"github.com/spencerswagger/jinliang/internal/source"
)

// 清表的所有权闸门（docs/CONTRACT-PAUSE-AND-POSITION.md §1.6 步 3 / §4.6 红线 4）。
//
// 裁决 A 的恢复路径第一步是「清空**本任务建的表**，再整表重搬」。这句话的重音在
// 「本任务建的」：`on_exists=append/keep` 复用的用户表里，行可能根本不是我们写的
// （用户自己的存量、别的工具灌的数据），清它就是删用户数据，而且**不可回滚**。
//
// 所以这一层不靠调用方自觉：凡是「清空一张目标表」的动作，都在这里问一次
// `DidCreateTable`（`_jl_object` 登记 + 同一 job_key）。三种结果三种处置：
//
//	是本任务建的 → 清；
//	不是本任务建的 → **明确失败**，四段式说清「谁的表、为什么会想清、你能做什么」；
//	查不出来（`_jl_object` 读失败）→ 也失败：判断不了就别动，别猜。
//
// 为什么不「跳过 + 一条 note」：跳过之后重搬会把存量 upsert 进去，而这只是让
// 「本该清掉的行」继续留着——用户看到的是「同步成功」与一张内容错的目标表。
// 静默跳过比停下来更坏，这正是契约 §4.6 红线 4 要挡的东西。

// ErrTruncateNotOwned 表示「要清的表不是本任务建的」，调用方据此给 E1014 家族的错误卡。
var ErrTruncateNotOwned = fmt.Errorf("目标表不是本任务建的，拒绝清空")

// truncateOwned 是**唯一**的清表入口：先证明所有权，再清。
//
// `reason` 只用于错误文案里说明「为什么现在要清」（重搬 / 源端 TRUNCATE 跟随），
// 不参与判定——判定只有 `DidCreateTable` 一条。
func (s *Sink) truncateOwned(ctx context.Context, ref source.TableRef, reason string) error {
	ours, err := s.DidCreateTable(ctx, ref)
	if err != nil {
		// 查不出来 ≠ 不是我们的。两条都不能猜，但这一条更不能放行：
		// 读 `_jl_object` 失败时清表，等于在不知道所有权的情况下删数据。
		return fmt.Errorf("清空 %s 之前查不到它是不是本任务建的（读 `_jl_object` 失败）：%v。"+
			"你能做什么：确认目标端账号对 `_jl_object` 有 SELECT 权限（一般都有），或稍后重跑；"+
			"风险：没有（本次没有清空任何表，也没有推进位点）", qualified(ref), err)
	}
	if !ours {
		return fmt.Errorf("%w：%s（%s）。"+
			"为什么会想清它：%s。"+
			"你能做什么：① 想让这张表归本任务管 → 在任务里把它改成可重建（on_exists=recreate），"+
			"或先手工清空它再重跑（`TRUNCATE TABLE %s`，只在你确认里面没有自己的数据时做）；"+
			"② 不想让本任务碰它 → 把它从 content.tables 里去掉后重新 import；"+
			"③ 只是想让源端这次 TRUNCATE 跟过去 → 手工在目标端执行同一条 TRUNCATE。"+
			"风险：手工清空是**不可回滚**的，表里的行找不回来；本实现不会替你猜",
			ErrTruncateNotOwned, qualified(ref), s.opt.JobKey, reason, qualified(ref))
	}
	if _, err := s.db.ExecContext(ctx, "TRUNCATE TABLE "+s.qualifiedOn(ref)); err != nil {
		return fmt.Errorf("清空 %s 失败: %w", qualified(ref), err)
	}
	return nil
}

// TruncateTable 实现引擎的 recopy 清表能力（重搬前清掉本任务写过的行）。
//
// **不是**裸 TRUNCATE：所有权由 truncateOwned 判定，判不了/不是我们的就明确失败。
// 引擎侧（cleanBeforeGenericCopy）也做过一次同样的判定——这里是第二道，
// 因为「清表」是不可回滚的动作，多问一次比少问一次便宜得多。
func (s *Sink) TruncateTable(ctx context.Context, ref source.TableRef) error {
	return s.truncateOwned(ctx, ref, "结构对齐后要整表重搬，先清掉上次写进去的行")
}
