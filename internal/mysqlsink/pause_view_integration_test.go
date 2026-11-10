//go:build integration

// 片 C 的 Data 侧两件事：
//  1. `StoredJobHash`（不在契约 §4.2 的六方法里，但引擎的 reconcilePaused 需要它）；
//  2. 未绑 `pauseReg` 时，`status`/`doctor` 读到的暂停状态必须**说得清**——
//     记录在、但写路径不认，绝不能返回空集合假装「没有暂停」。
package mysqlsink

import (
	"context"
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/pgsink"
)

// StoredJobHash 必须跨重启读得回、且与 EnsureMeta 写进去的一致。
// 没有状态行时返回空串（引擎把空当「没有历史」）。
func TestIntegrationStoredJobHashSurvivesRestart(t *testing.T) {
	ctx := context.Background()

	// 先问一次「没有状态行」的形态（新 job_key，且把可能残留的行清掉）。
	fresh := openSink(t, func(o *Options) { o.JobKey = "brandnew01" })
	if _, err := fresh.db.ExecContext(ctx,
		"DELETE FROM `_jl_meta` WHERE job_key = ?", fresh.opt.JobKey); err != nil {
		t.Fatalf("清状态行失败: %v", err)
	}
	h, err := fresh.StoredJobHash(ctx)
	if err != nil {
		t.Fatalf("没有状态行时读 hash 不该报错: %v", err)
	}
	if h != "" {
		t.Fatalf("没有状态行时应返回空串（引擎按「没有历史」处理），实际 %q", h)
	}

	// 写一个 hash（prepare 就是这么写的），再「重启」读回。
	const want = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	b := openSink(t, nil)
	if err := b.EnsureMeta(ctx, want, "streaming", "", ""); err != nil {
		t.Fatalf("写状态行失败: %v", err)
	}
	b.Close()

	c := openSink(t, nil) // 重启：新实例、同一个 job_key
	got, err := c.StoredJobHash(ctx)
	if err != nil {
		t.Fatalf("重启后读 hash 失败: %v", err)
	}
	if got != want {
		t.Fatalf("重启后读回的 hash 不对：%q，应为 %q（reconcile 的跨轮存活判据靠它）", got, want)
	}

	// 登记暂停（单独事务）**不许**顺手改掉 hash：两个写互不干扰。
	if err := c.PauseTable(ctx, refOf("jl_b3_hash_tbl"), pgsink.PausedTable{
		Reason: pgsink.PauseReasonTypeChanged, Detail: "x"}); err != nil {
		t.Fatalf("登记暂停失败: %v", err)
	}
	if got2, _ := c.StoredJobHash(ctx); got2 != want {
		t.Fatalf("登记暂停后 hash 变了：%q（两个写必须互不干扰）", got2)
	}
	_ = c.ClearPausedTable(ctx, refOf("jl_b3_hash_tbl"))
}

// 未绑 pauseReg 的形态：记录在、写路径不认——展示层必须把这句话说出来。
func TestIntegrationPauseViewWhenEngineNotBound(t *testing.T) {
	ctx := context.Background()
	s := openSink(t, nil)
	ensureMetaRow(t, s)
	ref := refOf("jl_b3_view_tbl")
	_ = s.ClearPausedTable(ctx, ref)
	t.Cleanup(func() { _ = s.ClearPausedTable(ctx, ref) })

	// 空记录：不该输出任何行（也不该假装有个「暂停 0 张」）。
	if v := s.PauseSummary(ctx); len(v.Lines()) != 0 {
		t.Fatalf("没有记录时不该有输出行：%v", v.Lines())
	}

	if err := s.PauseTable(ctx, ref, pgsink.PausedTable{
		Reason: pgsink.PauseReasonTypeChanged, Detail: "列 v：源端 bigint → varchar(64)",
		Resume: pgsink.ResumeRecopy,
	}); err != nil {
		t.Fatalf("登记暂停失败: %v", err)
	}

	v := s.PauseSummary(ctx)
	if v.ReadErr != nil {
		t.Fatalf("读暂停状态失败: %v", v.ReadErr)
	}
	if v.Enforcement != PauseEnforcementStorageOnly {
		t.Fatalf("MySQL 目标端今天必须自报 storage-only（片 C 绑上才改），实际 %q", v.Enforcement)
	}
	if len(v.Tables) != 1 || v.Tables[0] != ref.Table {
		t.Fatalf("记录里应有这张表：%v", v.Tables)
	}
	lines := v.Lines()
	joined := strings.Join(lines, "\n")
	for _, want := range []string{ref.Table, "storage-only", "仍在被写入", "整表重搬补齐"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("展示文案里应有 %q（用户必须知道「记录在但写路径不认」）：\n%s", want, joined)
		}
	}
	t.Logf("status/doctor 在「MySQL 目标端未绑 pauseReg」时的输出样例：\n%s", joined)
}

// 读失败时**不许**返回空集合假装没有暂停：错误必须原样带出来。
func TestIntegrationPauseViewReadFailureIsNotSilent(t *testing.T) {
	ctx := context.Background()
	s := openSink(t, nil)
	ensureMetaRow(t, s)

	// 把状态表删掉，制造读失败（真实形态：权限被收、库被别人清过）。
	if _, err := s.db.ExecContext(ctx, "DROP TABLE `_jl_meta`"); err != nil {
		t.Fatalf("模拟读失败失败: %v", err)
	}
	t.Cleanup(func() { _ = s.ensureStateTables(ctx) })
	// 重新建一张**没有这一列**的表，并让补列也失败（没有 ALTER 权限的场景不好造，
	// 这里退而求其次：把表建成只读视图会太绕，直接测「表不存在」这条路径）。
	v := s.PauseSummary(ctx)
	if v.ReadErr == nil && len(v.Tables) == 0 {
		t.Fatal("读不到状态时绝不能安静返回「没有暂停」")
	}
	lines := strings.Join(v.Lines(), "\n")
	if v.ReadErr != nil && !strings.Contains(lines, "不要当成「没有暂停」") {
		t.Fatalf("读失败的输出要明确写「别当成没有暂停」：\n%s", lines)
	}
}
