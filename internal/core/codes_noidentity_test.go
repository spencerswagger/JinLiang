package core

import (
	"os"
	"strings"
	"testing"
)

// uniqueKeyConfigLanded 是 S3（`content.tables.<表>.unique_key`）是否已落地的人工开关。
//
// 为什么要有它：E1017 的四段式里「还能怎么办」这一节必须**只写现在真能走通的做法**。
// 定稿（docs/DESIGN-NO-PRIMARY-KEY.md §2.3）里的第三条是「声明 unique_key」，但 jobspec 是
// strict 解析——配置项还没落地时照着我们的话写，用户会直接撞「未知字段」，比不说更糟。
// 所以 S3 合入时必须一次改三处：① 码表 Next 补第三条；② docs/ERRORS.md 那一行同步；
// ③ 把这个常量改成 true（漏了第 ③ 步，本用例会红，正好当提醒）。
const uniqueKeyConfigLanded = false

// TestNoIdentityFourParts：E1017 是**族码**（2026-11-13 裁决）——两个场景共用：
//   - 运行中：无主键表收到改/删 → 按表暂停（原因码 no_pk_write，#117 接线）；
//   - 预检：`interval` + 无主键 → blocked（每轮按水位重读，没有身份必然重复）。
//
// 正因为是族码，四段式既要能回答「为什么改删写不进去」，也要能回答「为什么每轮重复」。
// 只写其中一半，另一个场景的卡片就会挂着一个说不通的码（#117 之前 Core 先挂 E1006 被拦下，
// 挂错码会把用户引到「开变更流水」上去）。
func TestNoIdentityFourParts(t *testing.T) {
	spec, ok := Lookup(CodeNoIdentity)
	if !ok {
		t.Fatal("E1017 必须在码表里（Lookup 不命中 = 四段式注入被跳过，用户只拿到一句话）")
	}
	if spec.Class != UserAction {
		t.Fatalf("E1017 的 Class 应为 user_action，得到 %s", spec.Class)
	}
	if !strings.HasPrefix(string(spec.Code), "E1") {
		t.Fatalf("E1017 的编号前缀应落在 E1xxx（需要你处理），得到 %s", spec.Code)
	}
	// 三个「做不到」都要在 Message 里点名：定位（改/删）与判重（interval）。
	for _, want := range []string{"主键", "去重", "删"} {
		if !strings.Contains(spec.Message, want) {
			t.Errorf("Message 应提到 %q（族码要同时说得通 runtime 改/删与 interval 重复）：%s", want, spec.Message)
		}
	}
	// Why 要写清根因 + PG 源那道前置（不然用户会以为是我们的 bug）。
	for _, want := range []string{"唯一索引", "判重", "定时增量", "REPLICA IDENTITY"} {
		if !strings.Contains(spec.Why, want) {
			t.Errorf("Why 应提到 %q：%s", want, spec.Why)
		}
	}
	// Risk 必须说清「只停这张表」的边界与重搬代价（与 #117 的按表暂停语义一致）。
	for _, want := range []string{"其它表照常", "整表重搬", "清空"} {
		if !strings.Contains(spec.Risk, want) {
			t.Errorf("Risk 应提到 %q：%s", want, spec.Risk)
		}
	}
	// Risk 里不许出现「不会重搬」「位点不动」这类安抚性表述（E1013/E1015 已立过规矩）：
	// 这张表的恢复代价就是一次整表重搬，说轻了用户就不处理了。
	for _, bad := range []string{"不会重搬", "位点不动", "无需处理"} {
		if strings.Contains(spec.Risk, bad) {
			t.Errorf("Risk 里不该出现安抚性表述 %q：%s", bad, spec.Risk)
		}
	}
}

// TestNoIdentityNextOnlyNamesRealActions：「你现在能做什么」里的每一条都必须是**真能走通**的。
//
// 三条判据：① 加主键/唯一索引；② 换 once / full_interval（每轮整表重搬）；③ 从 content.tables 去掉后重新 import。
// 第四条「声明 unique_key」在 S3 落地前不许出现（见 uniqueKeyConfigLanded）。
func TestNoIdentityNextOnlyNamesRealActions(t *testing.T) {
	spec, ok := Lookup(CodeNoIdentity)
	if !ok {
		t.Fatal("E1017 必须在码表里")
	}
	if len(spec.Next) != 3 {
		t.Fatalf("E1017 的 Next 应为 3 步（三条真实做法），实际 %d 步", len(spec.Next))
	}
	steps := strings.Join(spec.Next, "\n")
	for _, want := range []string{"主键或唯一索引", "once", "full_interval", "content.tables", "重新 import"} {
		if !strings.Contains(steps, want) {
			t.Errorf("Next 应包含可执行做法 %q：\n%s", want, steps)
		}
	}
	// 配置项没落地就不许写进「你能做什么」：strict 解析会把它当未知字段直接报错。
	mentionUniqueKey := strings.Contains(steps, "unique_key")
	if uniqueKeyConfigLanded && !mentionUniqueKey {
		t.Fatal("S3 已落地（uniqueKeyConfigLanded=true）但 Next 里没有 unique_key 这条路——" +
			"请补第三条并同步 docs/ERRORS.md")
	}
	if !uniqueKeyConfigLanded && mentionUniqueKey {
		t.Fatal("content.tables.<表>.unique_key 还没落地（S3 未合入），Next 里不许写它：" +
			"jobspec 是 strict 解析，用户照着写会撞「未知字段」。S3 合入时改 uniqueKeyConfigLanded 并补第三条")
	}
}

// TestErrorsDocE1017MatchesCatalogue：`docs/ERRORS.md` 那一行是**手抄**的（`.md` 不参与编译），
// 抄漏了用户就拿到一个只有编号的码。与 E1014 的用例同源：文档与码表对不上就是假说明书。
func TestErrorsDocE1017MatchesCatalogue(t *testing.T) {
	spec, ok := Lookup(CodeNoIdentity)
	if !ok {
		t.Fatal("E1017 必须在码表里")
	}
	raw, err := os.ReadFile("../../docs/ERRORS.md")
	if err != nil {
		t.Fatalf("读 docs/ERRORS.md 失败: %v", err)
	}
	var row string
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "| E1017 |") {
			row = line
			break
		}
	}
	if row == "" {
		t.Fatal("docs/ERRORS.md 里找不到 E1017 那一行")
	}
	if !strings.Contains(row, spec.Message) {
		t.Errorf("ERRORS.md 的 E1017 行与码表 Message 不一致：\n码表：%s\n文档：%s", spec.Message, row)
	}
	for _, want := range []string{"主键或唯一索引", "once", "full_interval", "content.tables", "REPLICA IDENTITY"} {
		if !strings.Contains(row, want) {
			t.Errorf("ERRORS.md 的 E1017 行缺 %q（四段式里能做的事必须与码表同源）：%s", want, row)
		}
	}
}
