package core

import (
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestSpecs(t *testing.T) {
	re := regexp.MustCompile(`^E[1-4][0-9]{3}$`)
	seen := map[Code]bool{}
	for _, s := range Catalogue() {
		if !re.MatchString(string(s.Code)) {
			t.Errorf("%s: code must look like E1xxx..E4xxx", s.Code)
		}
		if seen[s.Code] {
			t.Errorf("%s: duplicate code", s.Code)
		}
		seen[s.Code] = true
		if got := ClassOf(s.Code); got != s.Class {
			t.Errorf("%s: class %s does not match code prefix (%s)", s.Code, s.Class, got)
		}
		if n := utf8.RuneCountInString(s.Message); n == 0 || n > 40 {
			t.Errorf("%s: message should be short and human (%d runes)", s.Code, n)
		}
		if s.Why == "" || s.Risk == "" {
			t.Errorf("%s: four-part prompt needs why and risk", s.Code)
		}
		if len(s.Next) == 0 || len(s.Next) > 3 {
			t.Errorf("%s: needs 1-3 concrete next steps, got %d", s.Code, len(s.Next))
		}
		for _, step := range s.Next {
			if utf8.RuneCountInString(step) == 0 {
				t.Errorf("%s: empty next step", s.Code)
			}
		}
	}
	if len(seen) < 10 {
		t.Fatalf("expected a meaningful catalogue, got %d codes", len(seen))
	}
}

func TestSpecLookup(t *testing.T) {
	if _, ok := Lookup(CodeWalLevel); !ok {
		t.Fatal("CodeWalLevel must be in the catalogue")
	}
	if _, ok := Lookup("E9999"); ok {
		t.Fatal("unknown code must not resolve")
	}
}

// TestSchemaMismatchCoversBothReasons：E1015 的两条原因码（type_changed / schema_drift）
// 与「哪张表哪一列看 last_error」这句都必须在用户可见文案里，
// 否则用户只拿到一个码、不知道到底哪种情况。
func TestSchemaMismatchCoversBothReasons(t *testing.T) {
	spec, ok := Lookup(CodeSchemaMismatch)
	if !ok {
		t.Fatal("E1015 必须在码表里")
	}
	for _, want := range []string{"type_changed", "schema_drift", "last_error"} {
		if !strings.Contains(spec.Why+strings.Join(spec.Next, " "), want) {
			t.Errorf("E1015 的四段式里没提到 %q：%s / %v", want, spec.Why, spec.Next)
		}
	}
	if strings.Contains(spec.Risk, "数据会不一致") {
		t.Error("E1015 的 Risk 不该写「数据会不一致」——它是「这张表停止更新」，不是数据错了")
	}
}

// TestObjectMissingNextIsLayeredByCase：E1013 同时承担四种情形，Next 必须一一对应。
//
// 判据来自一次真实的误判：第一版把「3F000 建 schema」顶在最前面，于是**表名拼错**的人
// 会先去建一个已经存在的 schema——方向正好相反。这条用例把四种情形钉在各自的 step 上。
func TestObjectMissingNextIsLayeredByCase(t *testing.T) {
	spec, ok := Lookup(CodeObjectMissing)
	if !ok {
		t.Fatal("E1013 必须在码表里")
	}
	if len(spec.Next) != 3 {
		t.Fatalf("E1013 的 Next 应为 3 步（按情形分层），实际 %d 步", len(spec.Next))
	}
	cases := []struct {
		step int
		want []string
		desc string
	}{
		{0, []string{"3F000", "schema"}, "缺 schema 的动作"},
		{1, []string{"42P01", "别去建 schema"}, "缺表 / 表名写错的动作用"},
		{2, []string{"42703", "预检"}, "缺列的动作"},
	}
	for _, c := range cases {
		for _, w := range c.want {
			if !strings.Contains(spec.Next[c.step], w) {
				t.Errorf("%s：Next[%d] 应含 %q，实际 %q", c.desc, c.step, w, spec.Next[c.step])
			}
		}
	}
	// 名字写错这一情形：Why 与 Next 都要提到，否则用户看到「缺表」只会去改库
	if !strings.Contains(spec.Why, "写错") {
		t.Error("Why 必须提到「名字写错」这种情形")
	}
	if !strings.Contains(spec.Next[1], "写错") {
		t.Error("Next[1] 必须把「表名写错」并进去（动作与缺表相同）")
	}
	// 「位点不动、不会重搬」是**陈述事实**，放 Why 而不是 Risk——
	// Risk 里出现让人安心的话，会让「风险」这个词贬值（Owl 在 #66 上提的）。
	if !strings.Contains(spec.Why, "位点没有推进") {
		t.Error("Why 必须陈述「位点没有推进」这个事实")
	}
	if strings.Contains(spec.Risk, "不会重搬") || strings.Contains(spec.Risk, "位点不动") {
		t.Error("Risk 里不该出现安抚性表述（「位点不动/不会重搬」应放在 Why）")
	}
	// Risk 只放真风险：改错方向的后果 + 换 schema 的代价
	if !strings.Contains(spec.Risk, "继续失败") {
		t.Error("Risk 必须写清「改错方向会继续失败」")
	}
	if !strings.Contains(spec.Risk, "不会自动搬过去") {
		t.Error("Risk 必须保留「换 schema = 另一套表」的代价")
	}
}

// TestSchemaMismatchSaysOnlyThatTableStops：E1015 的三句话必须与实现一致。
//
// 这条用例翻过一次面：**第一次**错在「文案说其它表不受影响、实现却是整轮失败」，于是把
// 「其它表也不会继续推进」grep 进 Risk；**第二次**是按表暂停真落地之后（契约
// docs/CONTRACT-PAUSE-AND-POSITION.md §1.5/§1.6）——命中暂停表的行整条跳过、其它表照写、
// 位点照常前进、恢复=整表重搬。旧短语变成**假话**，必须删掉并钉住新事实：
// 「其它表照常继续」+「会清空这张表再整表重搬」。
//
// 谁再改文案，两条 grep 短语都必须还在——它们是用户判断「要不要现在处理」的唯一依据。
func TestSchemaMismatchSaysOnlyThatTableStops(t *testing.T) {
	spec, ok := Lookup(CodeSchemaMismatch)
	if !ok {
		t.Fatal("E1015 必须在码表里")
	}
	if spec.Message != "表结构不一致" {
		t.Errorf("Message 不该改，得到 %q", spec.Message)
	}
	const caveat = "若当前版本已支持按表暂停"
	if strings.Contains(spec.Why, caveat) || strings.Contains(spec.Risk, caveat) {
		t.Fatal("三处括注必须全删（Why / Next / Risk 都不许再出现）")
	}
	for _, n := range spec.Next {
		if strings.Contains(n, caveat) {
			t.Fatal("Next 里还留着括注")
		}
		if strings.Contains(n, "--resync") {
			t.Fatalf("Next 不许出现 --resync（位点没推进，重搬全量是错的下一步）：%q", n)
		}
	}
	// Why 要覆盖两个时机，并说清「具体看 last_error」。
	// #72 把 Why 改准了：type_changed 两个时机都会出现（prepare 的类型不一致 /
	// 运行时把列类型改了），schema_drift 只在 streaming。断言按这个事实写。
	for _, want := range []string{"prepare", "type_changed", "schema_drift", "last_error"} {
		if !strings.Contains(spec.Why, want) {
			t.Errorf("Why 应含 %q（两个时机都要覆盖，具体到表列看 last_error）", want)
		}
	}
	if len(spec.Next) < 2 || len(spec.Next) > 3 {
		t.Errorf("Next 要 1~3 步，得到 %d 步", len(spec.Next))
	}
	// **Next 里不许出现未实现的开关**：`--accept-change` 全仓没有实现过（cmdRun 的 flag
	// 里没有它），写进去用户会拿到 `flag provided but not defined`——这正是 #78 清掉的那类假命令。
	for _, n := range spec.Next {
		if strings.Contains(n, "--accept-change") {
			t.Fatalf("Next 里出现了未实现的开关：%q", n)
		}
	}
	if !strings.Contains(spec.Next[2], "content.tables") {
		t.Errorf("「不想跟这张表」要给一条真能走的动作（从 content.tables 去掉后重新 import），得到 %q", spec.Next[2])
	}
	// 漂移判定的两个时机必须在 Why 里写清：prepare 只判本任务建的表、streaming 判所有表。
	// 防的是有人把它改回「schema_drift 只属 streaming」。
	if !strings.Contains(spec.Why, "跑起来后所有表都判") {
		t.Fatalf("E1015 的 Why 还要写清「跑起来后所有表都判」（两时机都得钉住），得到：%s", spec.Why)
	}
	if !strings.Contains(spec.Why, "prepare") || !strings.Contains(spec.Why, "本任务建的表") {
		t.Fatalf("E1015 的 Why 必须写清 prepare 期只在本任务建的表上判漂移，得到：%s", spec.Why)
	}
	if strings.Contains(spec.Risk, "其它表也不会继续推进") || strings.Contains(spec.Risk, "位点不推进") {
		t.Error("旧口径（整轮停 / 位点不推进）已经是假话：按表暂停落地后其它表照常继续、位点照常前进")
	}
	if !strings.Contains(spec.Risk, "其它表照常继续") {
		t.Error("Risk 必须写明「其它表照常继续」——这是用户决定「现在处理还是等等」的依据")
	}
	if !strings.Contains(spec.Risk, "整表重搬") {
		t.Error("Risk 必须写明恢复代价：结构对齐后会**清空这张表再整表重搬**（用户要知道这是一次全量）")
	}
	for _, n := range spec.Next {
		if strings.Contains(n, "不会重搬历史") {
			t.Errorf("旧口径「不会重搬历史」已不成立（恢复=整表重搬）：%q", n)
		}
	}
	if !strings.Contains(spec.Next[1], "其它表继续") {
		t.Errorf("Next 必须说明「只停这一张表、其它表继续」，得到 %q", spec.Next[1])
	}
	// **「看哪里」必须真实存在**（Owl II 必改 3）：`jinliang status` 这一版没有暂停这一节
	// （片 D2 未落地，契约 §1.8），文案却让人「看 status 里的 skipped_rows」——用户照着找，
	// 找不到只会以为是我们没记。第一手记录只有目标库 `_jl_meta.paused_tables` 那一列。
	// 等 D2 落地（status 里真有 paused[]）再把这条换成「status 里能看到」也不迟。
	if strings.Contains(spec.Risk, "status 里") {
		t.Errorf("Risk 不许把用户指去 `jinliang status` 里找待办/行数（那里没有这一节）：%q", spec.Risk)
	}
	if !strings.Contains(spec.Risk, "paused_tables") {
		t.Errorf("Risk 必须给出一个真实存在的「看哪里」：目标库 _jl_meta 的 paused_tables 列，得到 %q", spec.Risk)
	}
}

// TestSchemaMismatchReasonTimingNotOneToOne：原因码与发现时机**不是一一对应**。
//
// 真实错例：第一版写「prepare = type_changed，跑起来后改类型/改名/删列 = schema_drift」，
// 而实现里 streaming 的**改类型**也返回 TypeChangedError。于是「跑起来后改了类型」
// 被码表错标成 schema_drift，用户按它去查「列是不是被改名了」，方向反了。
func TestSchemaMismatchReasonTimingNotOneToOne(t *testing.T) {
	spec, ok := Lookup(CodeSchemaMismatch)
	if !ok {
		t.Fatal("E1015 必须在码表里")
	}
	// type_changed 必须同时挂在「prepare」与「跑起来后改类型」两处语境上
	if !strings.Contains(spec.Why, "prepare") {
		t.Error("Why 必须提到 prepare 这条时机（EnsureSchema 补列时就会撞）")
	}
	if !strings.Contains(spec.Why, "跑起来后源端把列类型改了") {
		t.Error("Why 必须说明 streaming 的改类型也是 type_changed —— 第一版正是把这里错标成 schema_drift")
	}
	// schema_drift 只对应改名/删列
	if !strings.Contains(spec.Why, "改名") || !strings.Contains(spec.Why, "删掉") {
		t.Error("Why 必须说明 schema_drift 是改名/删列")
	}
	// 不许再出现「跑起来后改类型 / 改名 / 删列」被绑成一组的旧表述
	if strings.Contains(spec.Why, "改类型 / 改名 / 删列") {
		t.Error("Why 里不能再把「改类型」与「改名/删列」绑成同一原因码")
	}
}
