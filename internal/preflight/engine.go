package preflight

import (
	"fmt"
	"sort"
	"strings"

	"github.com/spencerswagger/jinliang/internal/source"
)

// 这个文件是 SPEC MY-08「表的存储引擎」的**唯一判据**。
//
// 为什么单独抽出来：同一个「MyISAM 能不能跑」的问题，昨晚有三处各写一份
// （预检卡片里 `strings.Contains(engine,"MYISAM")`、连接器的检查、文档措辞），
// 只要有一处改了口径，另外两处就会互相打脸。现在判定只有 JudgeEngines 一个入口：
// 预检卡片（engineChecks）和引擎收尾的「降级标注」都只读它的结论。
//
// 结论只有三态，且是**明确降级**（不是拒绝）：MyISAM 物理上读得到、也搬得动，
// 只是没有事务 —— docs/DESIGN.md 的「不拒绝，只提示」要求照跑并标注代价。
type EngineVerdict string

const (
	// EngineTransactional：选中的表都是有事务的引擎（InnoDB 等）→ ok，不发 MY-08。
	EngineTransactional EngineVerdict = "transactional"
	// EngineNonTransactional：选中的表里有 MyISAM（含 MRG_MyISAM）→ **降级**
	//（warn，照跑；收尾必须标注「全量不是一致性切面」）。不是拒绝。
	EngineNonTransactional EngineVerdict = "non_transactional"
	// EngineNotJudged：不判定（非 MySQL 源 / 没读到表清单）→ 不发 MY-08。
	// 这一态**不冒充 ok**：调用方拿到它只能说「没判」，不许把「没判」当「没问题」。
	EngineNotJudged EngineVerdict = "not_judged"
)

// EngineJudgement 是判据的输出。
type EngineJudgement struct {
	Verdict EngineVerdict
	// Tables 是被告名的表，形如 "orders(MyISAM)"，已排序；只有
	// EngineNonTransactional 时非空。
	Tables []string
	// Names 是同一批表的裸表名（Affected 字段要裸名）。
	Names []string
	// SQL 是把这些表改成 InnoDB 的语句（用户能照做的那条）；判据自己给，
	// 免得每处调用各造一句。
	SQL string
	// Reason 是这个结论的人话解释（进 Detail 的尾巴）。
	Reason string
}

// JudgeEngines 判断「选中的表里有没有无事务引擎」。
//
// 引擎名来自 Catalog（mysqlsource 从 information_schema.TABLES 填
// source.Table.Engine），这里不做任何新查询。PG 源的 Engine 是 relkind 名
// （table / partitioned table…），不能拿来判存储引擎，所以只认 mysql。
//
// 口径边界（刻意收窄，改它要先改 docs/SPEC.md MY-08）：
//   - 算「无事务」：MyISAM / MRG_MyISAM（引擎名里含 MYISAM 的都算，覆盖将来变体）；
//   - 不算：InnoDB 等有事务引擎；
//   - 还没定口径、因此**暂不判**：MEMORY / CSV / ARCHIVE（同样没有事务，但 SPEC
//     只写了 MyISAM；放宽要 Ark 拍板，见 notes/myisam-preflight-degrade.md）。
func JudgeEngines(sourceKind string, cat *source.Catalog, selected []string) EngineJudgement {
	if sourceKind != "mysql" {
		return EngineJudgement{
			Verdict: EngineNotJudged,
			Reason:  "不是 MySQL 源：非 MySQL 源的 Engine 不是存储引擎（PG 是 relkind），没有这一项可判",
		}
	}
	if cat == nil {
		// 不阻断，与 SCOPE-01 同口径；但结论是「没判」而不是「没问题」。
		return EngineJudgement{
			Verdict: EngineNotJudged,
			Reason:  "没读到源端表清单，没有判存储引擎（不阻断，跑起来再按真实引擎处理）",
		}
	}

	want := make(map[string]bool, len(selected))
	for _, t := range selected {
		want[t] = true
	}

	var names, display, alters []string
	for _, t := range cat.Tables {
		if len(want) > 0 && !want[t.Table] {
			continue
		}
		if !strings.Contains(strings.ToUpper(t.Engine), "MYISAM") {
			continue
		}
		names = append(names, t.Table)
		display = append(display, t.Table+"("+t.Engine+")")
		alters = append(alters, fmt.Sprintf("ALTER TABLE %s.%s ENGINE=InnoDB;",
			backquote(t.Schema), backquote(t.Table)))
	}
	if len(names) == 0 {
		return EngineJudgement{
			Verdict: EngineTransactional,
			Reason:  "选中的表都是有事务的引擎",
		}
	}
	sort.Strings(names)
	sort.Strings(display)
	sort.Strings(alters)
	return EngineJudgement{
		Verdict: EngineNonTransactional,
		Tables:  display,
		Names:   names,
		SQL:     strings.Join(alters, "\n"),
		Reason: "全量读取和位点不在同一个时间点，崩溃或并发写入时目标端可能与源端不一致；" +
			"verify 全绿也不代表崩溃后仍然一致",
	}
}

// EngineDegradeNote 是「无事务表已降级」的**唯一文案**（判据 + 为什么都在一句里）。
//
// 预检卡片的 Detail 和引擎收尾的「说明」都从这里取：两处措辞各写一套，
// 用户就会拿到两种说法（昨晚 PAUSE-01 已经踩过一次）。四段齐：
// 发生了什么（降级，照跑）/ 为什么（无事务，不是同一时间点）/ 风险（可能不一致，
// verify 也不算数）/ 能做什么（改成 InnoDB，SQL 见 MY-08 卡片）。
func EngineDegradeNote(j EngineJudgement) string {
	return fmt.Sprintf("降级：%s 用的是无事务引擎（MyISAM）——本轮照跑、不拒绝，但**不是一致性切面**：%s；"+
		"要消除这个代价就把这几张表改成 InnoDB（见预检 MY-08）",
		strings.Join(j.Tables, "、"), j.Reason)
}

// backquote 给 MySQL 标识符加反引号（反引号翻倍）。
func backquote(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }
