package engine

import (
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/pgsink"
	"github.com/spencerswagger/jinliang/internal/pgsource"
	"github.com/spencerswagger/jinliang/internal/source"
)

// planCopy 是块级续跑的判定表，穷举它比在集成里撞运气便宜得多。
//
// 每条用例都写「为什么」，因为这张表里最容易犯的错不是写错分支，而是
// 把一个分支的理由记错了（那会让下一次改动把它删掉）。
func TestPlanCopy(t *testing.T) {
	tbl := func(name string, pk bool) source.Table {
		return source.Table{TableRef: source.TableRef{Schema: "public", Table: name}, HasStablePK: pk}
	}
	pkA, pkB, noPK := tbl("a", true), tbl("b", true), tbl("c", false)

	cases := []struct {
		name  string
		in    copyInputs
		kind  copyKind
		from  map[string]pgsource.ChunkResume
		skip  []string
		recpy []string
		why   string // Why 里应出现的片段（空 = 不检查）
		// clean：重搬之前必须清哪些表（决定会不会留下源端已删除的脏行）
		clean cleanScope
	}{
		{
			name: "首次全量：什么都没有，从头搬",
			in: copyInputs{Mode: "once", AlwaysCopy: true, Capable: true,
				Tables: []source.Table{pkA}, Progress: map[string]pgsink.ChunkProgress{}},
			kind: copyFresh,
		},
		{
			name: "continuous+initial 首次全量：也要搬",
			in: copyInputs{Mode: "", Tables: []source.Table{pkA}, Capable: true,
				Progress: map[string]pgsink.ChunkProgress{}},
			kind: copyFresh,
		},
		{
			// 核心用例：块 3 已提交、位点还在、书签没越过 → 从第 3 块的右边界接着读。
			name: "中断后续跑：从最后一块的右边界接着读",
			in: copyInputs{Mode: "once", AlwaysCopy: true, Capable: true,
				Tables:   []source.Table{pkA},
				StoredCP: "postgres:v1:0/1A2B3C", SlotReplayable: true,
				Progress: map[string]pgsink.ChunkProgress{
					"a": {LastNo: 3, Cursor: "3000", Rows: 3000},
				}},
			kind:  copyResume,
			from:  map[string]pgsource.ChunkResume{"a": {Cursor: "3000", LastNo: 3}},
			why:   "续跑",
			clean: cleanNone,
		},
		{
			// 一张表搬完了（有完成标记），另一张半截：搬完的不许重读。
			name: "续跑：上次搬完的表跳过",
			in: copyInputs{Mode: "", Capable: true,
				Tables:   []source.Table{pkA, pkB},
				StoredCP: "postgres:v1:0/1A2B3C", SlotReplayable: true,
				Progress: map[string]pgsink.ChunkProgress{
					"a": {Done: true, LastNo: 7, Cursor: "7000"},
					"b": {LastNo: 2, Cursor: "2000"},
				}},
			kind:  copyResume,
			from:  map[string]pgsource.ChunkResume{"b": {Cursor: "2000", LastNo: 2}},
			skip:  []string{"a"},
			why:   "1 张表已经搬完",
			clean: cleanNone,
		},
		{
			// 书签没了 = 中间那段变更源端已经不保留，续跑会漏行，只能整轮重搬。
			name: "书签不在：整轮重搬",
			in: copyInputs{Mode: "", Capable: true,
				Tables:   []source.Table{pkA},
				StoredCP: "postgres:v1:0/1A2B3C", SlotReplayable: false,
				Progress: map[string]pgsink.ChunkProgress{"a": {LastNo: 1, Cursor: "10"}}},
			kind:  copyFresh,
			why:   "书签追不回",
			clean: cleanAll, // 断在半截 → 清表之后再搬
		},
		{
			name: "书签越过位点：整轮重搬",
			in: copyInputs{Mode: "", Capable: true,
				Tables:   []source.Table{pkA},
				StoredCP: "postgres:v1:0/1A2B3C", SlotReplayable: true, SlotAhead: true,
				Progress: map[string]pgsink.ChunkProgress{"a": {LastNo: 1, Cursor: "10"}}},
			kind:  copyFresh,
			why:   "越过目标端的位点",
			clean: cleanAll,
		},
		{
			name: "没有位点：整轮重搬",
			in: copyInputs{Mode: "", Capable: true,
				Tables: []source.Table{pkA}, SlotReplayable: true,
				Progress: map[string]pgsink.ChunkProgress{"a": {LastNo: 1, Cursor: "10"}}},
			kind:  copyFresh,
			why:   "没有可用的一致性位点",
			clean: cleanAll,
		},
		{
			// 「存量都在目标端」不等于「已经同步到位」：书签追不回位点，中间那段变更
			// 永远补不回来 → 不能报「不用搬」，只能整轮重搬重新对齐。
			name: "全部搬完 + 书签追不回位点：整轮重搬",
			in: copyInputs{Mode: "", Capable: true,
				Tables:   []source.Table{pkA},
				StoredCP: "postgres:v1:0/1A2B3C", SlotReplayable: false, PriorPhase: "streaming",
				Progress: map[string]pgsink.ChunkProgress{"a": {Done: true, LastNo: 9, Cursor: "90"}}},
			kind:  copyFresh,
			why:   "重放不出来",
			clean: cleanAll, // 上次「搬完」了，但那段变更补不回来 → 整批清干净再搬
		},
		{
			name: "全部搬完 + 书签越过位点：整轮重搬",
			in: copyInputs{Mode: "", Capable: true,
				Tables:   []source.Table{pkA},
				StoredCP: "postgres:v1:0/1A2B3C", SlotReplayable: true, SlotAhead: true,
				SlotCP: "postgres:v1:0/2FFFFFF", PriorPhase: "streaming",
				Progress: map[string]pgsink.ChunkProgress{"a": {Done: true, LastNo: 9, Cursor: "90"}}},
			kind:  copyFresh,
			why:   "越过目标端的位点",
			clean: cleanAll,
		},
		{
			// 这条是「清表清到一半崩了再重跑」的判据：snapshot() 一开始就把 phase 写成
			// snapshot，所以下一轮 initialCopyDone 必假、块标记也可能已经清掉。只要
			// 「位点追不回来」这个事实还成立，就必须仍然整轮重搬 + 整批清，不能退化成
			// 「不清表重搬」（那会把空洞期删掉的行永远留在目标端）。
			name: "重跑退化陷阱：phase 已被改写成 snapshot + 无块标记 + 书签越过位点",
			in: copyInputs{Mode: "", Capable: true,
				Tables: []source.Table{pkA}, StoredCP: "postgres:v1:0/1A2B3C",
				SlotReplayable: true, SlotAhead: true, SlotCP: "postgres:v1:0/2FFFFFF",
				PriorPhase: "snapshot", Progress: map[string]pgsink.ChunkProgress{}},
			kind:  copyFresh,
			why:   "越过目标端的位点",
			clean: cleanAll,
		},
		{
			// 老状态（升级前没有块账）但位点/阶段说明「确实搬过」：无从分辨哪张表有我们的行，
			// 只能整批清，否则补不掉这段时间源端的删除。
			name: "老状态（无块标记、有位点）追不回位点：整批清",
			in: copyInputs{Mode: "", Capable: true,
				Tables: []source.Table{pkA, pkB}, StoredCP: "postgres:v1:0/1A2B3C",
				SlotReplayable: true, SlotAhead: true, SlotCP: "postgres:v1:0/2FFFFFF",
				PriorPhase: "streaming", Progress: map[string]pgsink.ChunkProgress{}},
			kind:  copyFresh,
			why:   "越过目标端的位点",
			clean: cleanAll,
		},
		{
			// once 也一样：模式要求重搬，而位点追不回来时那批行整批过期 → 整批清。
			name: "once + 书签追不回位点：整轮重搬且整批清",
			in: copyInputs{Mode: "once", AlwaysCopy: true, Capable: true,
				Tables:   []source.Table{pkA, pkB},
				StoredCP: "postgres:v1:0/1A2B3C", SlotReplayable: false, PriorPhase: "done",
				Progress: map[string]pgsink.ChunkProgress{"a": {Done: true, LastNo: 9}, "b": {Done: true, LastNo: 9}}},
			kind:  copyFresh,
			why:   "重放不出来",
			clean: cleanAll,
		},
		{
			// 无主键表的游标是 ctid，续跑可能漏读或读重 → 只重来这一张表。
			name: "无主键表半截：只整表重来它，别的表照续",
			in: copyInputs{Mode: "", Capable: true,
				Tables:   []source.Table{pkA, noPK},
				StoredCP: "postgres:v1:0/1A2B3C", SlotReplayable: true,
				Progress: map[string]pgsink.ChunkProgress{
					"a":    {LastNo: 1, Cursor: "10"},
					"c":    {LastNo: 4, Cursor: "(0,9)"},
					"b":    {},
					"pkA2": {},
				}},
			kind:  copyResume,
			from:  map[string]pgsource.ChunkResume{"a": {Cursor: "10", LastNo: 1}},
			recpy: []string{"c"},
			why:   "无主键表整表重来",
			clean: cleanNone,
		},
		{
			// once 的语义没变：上一次**搬完了**，这一轮就该重搬一轮。
			name: "once + 上次搬完：重搬（模式语义）",
			in: copyInputs{Mode: "once", AlwaysCopy: true, Capable: true,
				Tables:   []source.Table{pkA},
				StoredCP: "postgres:v1:0/1A2B3C", SlotReplayable: true, PriorPhase: "done",
				Progress: map[string]pgsink.ChunkProgress{"a": {Done: true, LastNo: 9, Cursor: "90"}}},
			kind: copyFresh,
			why:  "按模式整轮重搬",
			// Ark 口径（2026-11-10）：once / full_interval 每轮整轮重搬 → 每轮都清。
			// 不清的话，源端在这两轮之间删掉的行永远补不掉（BulkApply 只有 upsert）。
			clean: cleanAll,
		},
		{
			name: "continuous+initial + 上次搬完：不搬",
			in: copyInputs{Mode: "", Capable: true,
				Tables:   []source.Table{pkA},
				StoredCP: "postgres:v1:0/1A2B3C", SlotReplayable: true, PriorPhase: "streaming",
				Progress: map[string]pgsink.ChunkProgress{"a": {Done: true, LastNo: 9, Cursor: "90"}}},
			kind: copySkip,
		},
		{
			// 升级兼容：老状态没有块标记，只有阶段。不能因此把已经同步好的任务重搬一遍。
			name: "老状态（无块标记，phase=streaming）：不搬",
			in: copyInputs{Mode: "", Capable: true,
				Tables: []source.Table{pkA}, StoredCP: "postgres:v1:0/1A2B3C",
				SlotReplayable: true, PriorPhase: "streaming"},
			kind: copySkip,
			why:  "旧版本没有块进度记录",
		},
		{
			// 同一个洞的另一个出口：升级路径（没有块标记、只有 phase）也要看可重放性，
			// 否则 Prepare 会重建书签、位点跳到今天，那段变更永不重放。
			name: "老状态（无块标记）+ 书签追不回位点：整轮重搬且清表",
			in: copyInputs{Mode: "", Capable: true,
				Tables: []source.Table{pkA}, StoredCP: "postgres:v1:0/1A2B3C",
				SlotReplayable: false, PriorPhase: "streaming"},
			kind:  copyFresh,
			why:   "重放不出来",
			clean: cleanAll,
		},
		{
			name: "老状态（无块标记）+ 书签越过位点：整轮重搬且清表",
			in: copyInputs{Mode: "", Capable: true,
				Tables: []source.Table{pkA}, StoredCP: "postgres:v1:0/1A2B3C",
				SlotReplayable: true, SlotAhead: true, SlotCP: "postgres:v1:0/2FFFFFF",
				PriorPhase: "streaming"},
			kind:  copyFresh,
			why:   "越过目标端的位点",
			clean: cleanAll,
		},
		{
			name: "老状态（无块标记，phase=streaming）+ once：重搬且整批清",
			in: copyInputs{Mode: "once", AlwaysCopy: true, Capable: true,
				Tables: []source.Table{pkA}, StoredCP: "postgres:v1:0/1A2B3C",
				SlotReplayable: true, PriorPhase: "streaming"},
			kind:  copyFresh,
			clean: cleanAll, // 块账没有，但位点说明搬过 → 整批清
		},
		{
			// 目标端不记块进度（MySQL 全量路径）：回到「整轮重搬」，不假装能续。
			name: "目标端不记块进度：整轮重搬",
			in: copyInputs{Mode: "", Capable: false,
				Tables:   []source.Table{pkA},
				StoredCP: "postgres:v1:0/1A2B3C", SlotReplayable: true, PriorPhase: "snapshot",
				Progress: map[string]pgsink.ChunkProgress{"a": {LastNo: 1, Cursor: "10"}}},
			kind: copyFresh,
			why:  "不记块进度",
			// 目标端不记块进度（MySQL 全量路径），这一版不消费 Clean。
			clean: cleanNone,
		},
		{
			name: "interval / continuous 模式不做初始全量",
			in: copyInputs{Mode: "interval", Capable: true,
				Tables: []source.Table{pkA}, Progress: map[string]pgsink.ChunkProgress{}},
			kind: copySkip,
		},
		{
			// 首次搬：目标端的行全是用户自己的（append 复用用户表）→ 一行都不能清。
			name: "首次搬（无标记、无位点、无阶段）：搬但不许清表",
			in: copyInputs{Mode: "", Capable: true,
				Tables: []source.Table{pkA}, Progress: map[string]pgsink.ChunkProgress{}},
			kind:  copyFresh,
			clean: cleanNone,
		},
		{
			// 真实首次跑的样子：EnsureMeta 在读状态之前就写了 phase='prepare'。
			// 拿「phase 非空」当「搬过」会把第一次搬也判成要清表 → append 用户表被拒。
			name: "首次搬（phase=prepare 是 EnsureMeta 刚写的）：搬但不许清表",
			in: copyInputs{Mode: "", Capable: true,
				Tables: []source.Table{pkA}, PriorPhase: "prepare",
				Progress: map[string]pgsink.ChunkProgress{}},
			kind:  copyFresh,
			clean: cleanNone,
		},
		{
			name: "没有选中表",
			in:   copyInputs{Mode: "once", AlwaysCopy: true, Capable: true},
			kind: copySkip,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := planCopy(tc.in)
			if got.Kind != tc.kind {
				t.Fatalf("结论错：想要 %s，得到 %s（Why=%q）", tc.kind, got.Kind, got.Why)
			}
			if tc.why != "" && !strings.Contains(got.Why, tc.why) {
				t.Fatalf("Why 里要说清理由（应含 %q），得到 %q", tc.why, got.Why)
			}
			if len(tc.from) != len(got.From) {
				t.Fatalf("续跑起点数量错：想要 %v，得到 %v", tc.from, got.From)
			}
			for name, want := range tc.from {
				if g := got.From[name]; g != want {
					t.Fatalf("%s 的续跑起点错：想要 %+v，得到 %+v", name, want, g)
				}
			}
			for _, name := range tc.skip {
				if !got.Skip[name] {
					t.Fatalf("%s 应该被跳过（上次已搬完），Skip=%v", name, got.Skip)
				}
			}
			if len(tc.recpy) != len(got.Recopy) {
				t.Fatalf("整表重来的表数量错：想要 %v，得到 %v", tc.recpy, got.Recopy)
			}
			for i, name := range tc.recpy {
				if got.Recopy[i].Table != name {
					t.Fatalf("整表重来的表错：想要 %v，得到 %v", tc.recpy, got.Recopy)
				}
			}
			for _, ref := range got.Recopy {
				if _, bad := got.From[ref.Table]; bad {
					t.Fatalf("%s 既要整表重来又给了续跑起点：两者不能同时成立", ref.Table)
				}
			}
			if got.Clean != tc.clean {
				t.Fatalf("Clean 错：想要 %v，得到 %v", tc.clean, got.Clean)
			}
			if got.Clean == cleanNone && len(got.CleanNames) > 0 {
				t.Fatalf("Clean=cleanNone 却点了名要清：%v（首次搬一行都不许清）", got.CleanNames)
			}
			if got.Clean == cleanAll && len(got.CleanNames) == 0 {
				t.Fatalf("Clean=cleanAll 却没点名任何表：前后矛盾")
			}
			if got.Kind == copyFresh && len(got.Skip) > 0 {
				// 从头搬却把表标成「已搬完」→ 收尾不会给它盖完成标记 → 下一轮空跑补章。
				t.Fatalf("从头搬不该带 Skip（会漏盖完成标记）：%v", got.Skip)
			}
			if got.Kind != copyResume && len(got.From) > 0 {
				t.Fatalf("非续跑却给了续跑起点：%v", got.From)
			}
		})
	}
}

// 续跑必须**保留**原来的位点：挪了位点就等于放弃 CP 之后那一段变更。
func TestResumeKeepsStoredCheckpoint(t *testing.T) {
	in := copyInputs{Mode: "", Capable: true, StoredCP: "postgres:v1:0/1A2B3C", SlotReplayable: true,
		Tables:   []source.Table{{TableRef: source.TableRef{Table: "a"}, HasStablePK: true}},
		Progress: map[string]pgsink.ChunkProgress{"a": {LastNo: 2, Cursor: "20"}}}
	got := planCopy(in)
	if got.Kind != copyResume {
		t.Fatalf("应该续跑，得到 %s", got.Kind)
	}
	if got.From["a"].Cursor != "20" || got.From["a"].LastNo != 2 {
		t.Fatalf("续跑起点必须是最后一块的右边界与编号，得到 %+v", got.From["a"])
	}
}

// 「只接增量、但那段变更再也拿不到」必须四段齐全地把选择权交回用户，
// 而不是悄悄把位点往前挪（那是最坏的一种「看起来成功」）。
func TestUnreplayableSlotErrorSaysWhatToDo(t *testing.T) {
	e := &unreplayableSlotError{stored: "postgres:v1:0/1A2B3C"}
	msg := e.Error()
	for _, want := range []string{
		"书签追不回",                  // 发生了什么
		"max_slot_wal_keep_size", // 为什么
		"改成 once",                // 你能做什么
		"风险",                     // 风险
	} {
		if !strings.Contains(msg, want) {
			t.Fatalf("文案缺 %q：\n%s", want, msg)
		}
	}
}

// 拒绝清表的三句理由必须各不相同、且各自说对：用户照着这句决定下一步做什么，
// 一律说「上次搬到一半就断了」在「位点追不回来」的场景里是错的（会把他引向「再跑一次」）。
func TestRecopyReasonSentences(t *testing.T) {
	cases := []struct {
		why  recopyReason
		want string
	}{
		{recopyHalfCopy, "上次全量搬到一半就断了"},
		{recopyGap, "追不回目标端的位点"},
		{recopyNoPK, "没有稳定的主键"},
	}
	seen := map[string]bool{}
	for _, c := range cases {
		got := c.why.sentence()
		if !strings.Contains(got, c.want) {
			t.Fatalf("%d 的理由应含 %q，得到 %q", c.why, c.want, got)
		}
		if seen[got] {
			t.Fatalf("不同原因必须给不同理由，%q 重复了", got)
		}
		seen[got] = true
	}
}
