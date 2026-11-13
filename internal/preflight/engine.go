package preflight

import (
	"fmt"
	"sort"
	"strings"

	"github.com/spencerswagger/jinliang/internal/source"
)

// 这个文件是 SPEC MY-08「表的存储引擎」的**唯一判据**。
//
// 为什么单独抽出来：同一个「这张表能不能给出一致性切面」的问题，昨晚有三处各写一份
// （预检卡片里 `strings.Contains(engine,"MYISAM")`、连接器的检查、文档措辞），
// 只要有一处改了口径，另外两处就会互相打脸。现在判定只有 JudgeEngines 一个入口：
// 预检卡片（engineChecks）和引擎收尾的「降级标注」都只读它的结论。
//
// 判据是**能力探测，不是引擎名猜测**（硬规则 5 的同一条道理）：取值来自服务器自报的
// `information_schema.ENGINES.TRANSACTIONS`（`source.Catalog.Engines`），
// `YES` 才有事务，`NO` / `NULL` / 查不到一律没有。不维护引擎名黑名单——
// 按名字判会把 MEMORY / CSV / ARCHIVE 当成「有事务」（#113 那条判据漏掉的假绿），
// 将来加的引擎（MyRocks、插件引擎）也不该等我们改代码才被发现。
//
// 结论只有三态，且无事务是**明确降级**（不是拒绝）：这些表读得到、也搬得动，
// 缺的只是「同一时间点」；docs/DESIGN.md 的「不拒绝，只提示」要求照跑并标注代价。
type EngineVerdict string

const (
	// EngineTransactional：选中的表全部有事务（TRANSACTIONS='YES'）→ ok，不发 MY-08。
	EngineTransactional EngineVerdict = "transactional"
	// EngineNonTransactional：至少一张表没有事务，**或**它的引擎名在
	// information_schema.ENGINES 里查不到 → **降级**（warn 照跑 + 收尾标注）。
	// 不是拒绝，也不是「暂不判」。
	EngineNonTransactional EngineVerdict = "non_transactional"
	// EngineNotJudged：**没判**（非 MySQL 源 / 没读到表清单）。这一态**不冒充 ok**：
	// 调用方拿到它只能说「没判」，不许把「没判」当「没问题」。
	EngineNotJudged EngineVerdict = "not_judged"
)

// EngineJudgement 是判据的输出。
type EngineJudgement struct {
	Verdict EngineVerdict
	// Tables 是被告名的表，形如 "orders(MyISAM)"（真实引擎名，不是写死的 MyISAM），
	// 已排序；只有 EngineNonTransactional 时非空。
	Tables []string
	// Names 是同一批表的裸表名（Affected 字段要裸名），已排序。
	Names []string
	// Engines 是命中表的引擎名（大写，去重，已排序）：EngineDegradeNote 用它追加
	// 「引擎特有的代价」（MEMORY 重启即空 / BLACKHOLE 读 0 行…）。
	Engines []string
	// SQL 是把这些表改成 InnoDB 的语句（用户能照做的那条）。
	SQL string
	// Reason 是这个结论的人话解释：「<引擎> 没有事务」或「服务器没告诉我们 …」。
	Reason string
}

// JudgeEngines 判断「选中的表里有没有无事务引擎」。
//
// 引擎名与事务能力都来自 Catalog（connect/mysql.go 与 mysqlsource/snapshot.go 两处
// Catalog() 都填 Engines），这里不做任何新查询。PG 源的 Engine 是 relkind 名
// （table / partitioned table…），不是存储引擎，所以只认 mysql。
//
// 四条口径（SPEC MY-08 已写死，改它先改文档）：
//  1. 查不到 = 降级，不是 ok（读不到能力 ≠ 有事务）；
//  2. 档位与 mode 无关（once 也不是一致快照）；
//  3. 只判源端（目标端由 mysqlsink 一律建 InnoDB）；
//  4. not_judged 只在「非 MySQL 源」和「没读到表清单」出现。
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

	var names, display, engines, alters []string
	noTxSet, unknownSet := map[string]bool{}, map[string]bool{}
	seenEngine := map[string]bool{}
	for _, t := range cat.Tables {
		if len(want) > 0 && !want[t.Table] {
			continue
		}
		engine := strings.TrimSpace(t.Engine)
		key := strings.ToUpper(engine)
		tx, known := cat.Engines[key]
		if known && tx {
			continue // 有事务：这一张不用管
		}

		name := engine
		if name == "" {
			name = "未知引擎"
		}
		names = append(names, t.Table)
		display = append(display, t.Table+"("+name+")")
		alters = append(alters, fmt.Sprintf("ALTER TABLE %s.%s ENGINE=InnoDB;",
			backquote(t.Schema), backquote(t.Table)))
		if !seenEngine[key] {
			seenEngine[key] = true
			engines = append(engines, name)
		}
		if known {
			noTxSet[name] = true
		} else {
			unknownSet[name] = true
		}
	}
	if len(names) == 0 {
		return EngineJudgement{
			Verdict: EngineTransactional,
			Reason:  "选中的表都有事务（服务器自报 TRANSACTIONS=YES）",
		}
	}
	noTx := sortedKeys(noTxSet)
	unknown := sortedKeys(unknownSet)
	sort.Strings(names)
	sort.Strings(display)
	sort.Strings(engines)
	sort.Strings(alters)

	var why []string
	if len(noTx) > 0 {
		why = append(why, strings.Join(noTx, "、")+" 没有事务（服务器自报 TRANSACTIONS=NO/NULL）")
	}
	if len(unknown) > 0 {
		// 关键的一句：读不到能力 ≠ 有事务。不许在这里退化成「当成 ok」。
		why = append(why, "服务器没告诉我们 "+strings.Join(unknown, "、")+
			" 有没有事务（读不到能力 ≠ 有事务，按没有处理）")
	}
	return EngineJudgement{
		Verdict: EngineNonTransactional,
		Tables:  display,
		Names:   names,
		Engines: engines,
		SQL:     strings.Join(alters, "\n"),
		Reason:  strings.Join(why, "；"),
	}
}

// engineCosts 是「这个引擎比 MyISAM 还多一句什么」。
//
// 命中才追加（InnoDB 一个字不加）：把它们并成一句「无事务」会丢掉用户真正要做的判断
// （MEMORY 重启即空、BLACKHOLE 读 0 行、CSV/ARCHIVE 恒无主键…）。键用大写归一化。
var engineCosts = map[string][]string{
	"MEMORY": {
		"整表在内存，实例重启/崩溃即空；源端空了目标端还留着旧行，要重搬才收敛（增量里没有 delete 事件承载它）",
	},
	"CSV": {
		"不支持任何索引（8.0.28 实测 ADD PRIMARY KEY 报 1069），所以这类表恒无主键——" +
			"身份与改删归 SCOPE-02 / TARGET-02，本项只说「有没有事务」这一半；interval 下以 SCOPE-02 的 blocked 为准",
	},
	"ARCHIVE": {
		"不支持任何索引（实测 ADD PRIMARY KEY 报 1030），所以这类表恒无主键——" +
			"身份与改删归 SCOPE-02 / TARGET-02，本项只说「有没有事务」这一半；interval 下以 SCOPE-02 的 blocked 为准",
		"只能 INSERT/SELECT，也不支持可空列 ⇒ 源端不会发生改删",
	},
	"BLACKHOLE": {"写入即丢、读永远 0 行：「全量完成 0 行」对它成立，但没有意义"},
	"FEDERATED": {"表在远端；位点是本机变更流的位置，两者不保证同一时间点"},
}

// EngineDegradeNote 是「无事务表已降级」的**唯一文案**（预检卡片 Detail 与收尾「说明」共用）。
//
// 四段齐（SPEC MY-08 已定稿，别在这里另写一套）：发生了什么（降级、不拒绝、不是一致性切面）/
// 为什么（读取没有 read view，读取与位点不在同一时间点）/ 风险（崩溃或并发写入可能不一致，
// verify 只说明校验那一刻）/ 能做什么（ALTER … ENGINE=InnoDB，代价说清）。最后追加命中的
// 引擎特有代价。
func EngineDegradeNote(j EngineJudgement) string {
	var b strings.Builder
	b.WriteString("降级：")
	b.WriteString(strings.Join(j.Tables, "、"))
	b.WriteString(" 用的是无事务引擎——本轮照跑、不拒绝，但**不是一致性切面**：")
	b.WriteString(j.Reason)
	b.WriteString("；全量读取和位点不在同一个时间点（读取没有 read view），" +
		"崩溃或并发写入时目标端可能与源端不一致；verify 全绿只说明「校验那一刻两边一样」，不说明崩溃后仍然一致")
	for _, e := range j.Engines {
		for _, cost := range engineCosts[strings.ToUpper(e)] {
			b.WriteString("；")
			b.WriteString(e)
			b.WriteString("：")
			b.WriteString(cost)
		}
	}
	b.WriteString(engineFixHint(j))
	return b.String()
}

// engineFixHint 是「能做什么」那一段（四段的第四段）。
//
// 单表时直接把那条 ALTER 抄给用户；多表时只给一句说明 + 指向卡片的 SQL ——
// 一长串多行 SQL 塞进「说明」会把单行日志撕成好几行（CLI 与容器日志都按行读）。
func engineFixHint(j EngineJudgement) string {
	head := "；要消除这个代价就把这几张表改成 InnoDB（"
	tail := "——要重建表、要磁盘余量、要主键），改完下一轮 MY-08 自动消失；或者就这样接受"
	if len(j.Names) <= 1 {
		return head + j.SQL + tail
	}
	return fmt.Sprintf("%s逐表一条 ALTER TABLE <库>.<表> ENGINE=InnoDB;（共 %d 张，完整语句见 MY-08 卡片的 SQL）%s",
		head, len(j.Names), tail)
}

// sortedKeys 把 set 变成排好序的切片（点名要稳定，测试与文案都靠它）。
func sortedKeys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// backquote 给 MySQL 标识符加反引号（反引号翻倍）。
func backquote(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }
