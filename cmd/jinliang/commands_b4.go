package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spencerswagger/jinliang/internal/gc"
	"github.com/spencerswagger/jinliang/internal/remedy"
	"github.com/spencerswagger/jinliang/internal/runtime"
	"github.com/spencerswagger/jinliang/internal/verify"
)

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func openRuntime(raw, what string) (*runtime.DB, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("缺 %s DSN（例如 --%s postgres://user:pass@host:5432/db）", what, what)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db, err := runtime.Open(ctx, raw)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// cmdVerify runs the three-tier consistency check.
func cmdVerify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	source := fs.String("source", "", "源端 DSN（只读）")
	target := fs.String("target", "", "目标端 DSN（只读；一致性级要读 _jl_meta）")
	srcSchema := fs.String("source-schema", "", "源端 schema（默认：PG public / MySQL 库名）")
	tgtSchema := fs.String("target-schema", "", "目标端 schema（默认：PG public / MySQL 库名）")
	tables := fs.String("tables", "", "要校验的表，逗号分隔（必填）")
	level := fs.String("level", "all", "structure / data / consistency / all")
	sample := fs.Int("sample", 100, "每张表抽样比对的主键数")
	jobKey := fs.String("job-key", "", "任务指纹（一致性级需要：读 slot/binlog 与 _jl_meta.position）")
	slotPrefix := fs.String("slot-prefix", "", "同步书签前缀（默认 jl_<job-key>）")
	maxGap := fs.Int64("max-gap-bytes", 64<<20, "允许目标端落后的字节数")
	jsonOut := fs.Bool("json", false, "JSON 输出（给 CI / AI 助手）")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*tables) == "" {
		return errors.New("缺 --tables：至少指定一张表（逗号分隔）")
	}
	src, err := openRuntime(*source, "source")
	if err != nil {
		return err
	}
	defer src.Close()
	tgt, err := openRuntime(*target, "target")
	if err != nil {
		return err
	}
	defer tgt.Close()

	opts := verify.Options{
		Source: src, Target: tgt,
		SourceSchema: *srcSchema, TargetSchema: *tgtSchema,
		Tables: splitList(*tables), Level: verify.Level(strings.ToLower(*level)),
		SampleRows: *sample, JobKey: strings.TrimSpace(*jobKey), MaxGapBytes: *maxGap,
	}
	if opts.SourceSchema == "" {
		opts.SourceSchema = src.Schema()
	}
	if opts.TargetSchema == "" {
		opts.TargetSchema = tgt.Schema()
	}
	if opts.JobKey != "" {
		prefix := *slotPrefix
		if prefix == "" {
			prefix = "jl_" + opts.JobKey
		}
		opts.SourcePosition = func(ctx context.Context) (string, error) {
			return verify.DBSourcePosition(ctx, src, prefix)
		}
		opts.TargetPosition = func(ctx context.Context) (string, error) {
			return verify.DBTargetPosition(ctx, tgt, opts.JobKey)
		}
	}
	rep, err := verify.Run(context.Background(), opts)
	if err != nil {
		return err
	}
	if *jsonOut {
		if err := printJSON(rep); err != nil {
			return err
		}
	} else {
		printVerify(rep)
	}
	if !rep.OK {
		return errors.New("校验未通过：看上面每级的 code / detail（这不是「永远绿」的空跑）")
	}
	return nil
}

func printVerify(rep verify.Report) {
	fmt.Printf("校验 %s（job_key=%s）\n", rep.GeneratedAt, firstOr(rep.JobKey, "-"))
	for _, lr := range rep.Levels {
		status := "OK"
		if !lr.OK {
			status = "不通过"
		}
		fmt.Printf("\n[%s] %s（%d 表，%d ms）\n", lr.Level, status, lr.Tables, lr.DurationMS)
		for _, f := range lr.Findings {
			loc := f.Table
			if f.Column != "" {
				loc += "." + f.Column
			}
			fmt.Printf("  - %s %s %s：%s\n", strings.ToUpper(string(f.Severity)), f.Code, loc, f.Detail)
			if f.Source != "" || f.Target != "" {
				fmt.Printf("      源端 %s / 目标端 %s\n", f.Source, f.Target)
			}
			if f.Hint != "" {
				fmt.Printf("      能做什么：%s\n", f.Hint)
			}
		}
		if len(lr.Stats) > 0 {
			fmt.Printf("  统计：%v\n", lr.Stats)
		}
	}
	if rep.OK {
		fmt.Println("\n结论：三级校验通过")
	} else {
		fmt.Println("\n结论：有级别未通过（详见上面的 code）")
	}
}

func firstOr(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}

// cmdGC lists/removes objects we created. Default is dry-run.
func cmdGC(args []string) error {
	fs := flag.NewFlagSet("gc", flag.ExitOnError)
	source := fs.String("source", "", "源端 DSN（slot / publication 所在；MySQL 可省略）")
	target := fs.String("target", "", "目标端 DSN（读 _jl_object / _jl_lease）")
	instance := fs.String("instance", "", "本实例 ID（默认取 JINLIANG_INSTANCE 或控制库）")
	dir := fs.String("data-dir", "", "数据目录（用来查实例 ID）")
	apply := fs.Bool("apply", false, "真的删除（默认 dry-run，只列不删）")
	force := fs.Bool("force-cross-instance", false, "允许清理其他实例创建且空闲的对象（未知的永不删）")
	jsonOut := fs.Bool("json", false, "JSON 输出")
	if err := fs.Parse(args); err != nil {
		return err
	}
	tgt, err := openRuntime(*target, "target")
	if err != nil {
		return err
	}
	defer tgt.Close()
	var src *runtime.DB
	if strings.TrimSpace(*source) != "" {
		src, err = openRuntime(*source, "source")
		if err != nil {
			return err
		}
		defer src.Close()
	}

	instanceID := strings.TrimSpace(*instance)
	if instanceID == "" {
		instanceID = strings.TrimSpace(os.Getenv("JINLIANG_INSTANCE"))
	}
	if instanceID == "" {
		// 控制库里有实例身份；读不到就让用户显式给，绝不猜。
		if store, serr := openStore(dataDir(*dir), "", false); serr == nil {
			instanceID, _ = store.InstanceID(context.Background())
			_ = store.Close()
		}
	}
	if instanceID == "" {
		return errors.New("不知道本实例 ID：用 --instance 指定（或先跑一次 jinliang serve 生成）")
	}

	plan, err := gc.Load(context.Background(), src, tgt, instanceID, gc.Options{
		DryRun: !*apply, AllowOtherInstances: *force,
	})
	if err != nil {
		return err
	}
	if *jsonOut {
		if err := printJSON(plan); err != nil {
			return err
		}
	} else {
		printGC(plan)
	}
	if plan.DryRun {
		return nil
	}
	res, err := gc.Execute(context.Background(), plan, compositeDropper{source: src, target: tgt})
	if err != nil {
		if !*jsonOut {
			fmt.Printf("删除失败：%v\n", err)
		}
		return err
	}
	if !*jsonOut {
		fmt.Printf("已删除 %d 个对象", len(res.Deleted))
		if len(res.Failed) > 0 {
			fmt.Printf("，失败 %d 个：%s", len(res.Failed), strings.Join(res.Failed, "；"))
		}
		fmt.Println()
	}
	return nil
}

func printGC(plan gc.Plan) {
	mode := "dry-run（只列不删）"
	if !plan.DryRun {
		mode = "apply（会真的删除）"
	}
	fmt.Printf("垃圾清理 %s：实例 %s\n", mode, plan.InstanceID)
	if len(plan.Decisions) == 0 {
		fmt.Println("  没有发现 jl_ 前缀的对象")
		return
	}
	for _, d := range plan.Decisions {
		mark := "  "
		if d.Delete {
			if plan.DryRun {
				mark = "删?"
			} else {
				mark = "删 "
			}
		}
		fmt.Printf("  %s %-10s %-22s %s\n", mark, d.Kind, d.Name, d.Verdict)
		fmt.Printf("       %s\n", d.Reason)
		if d.Owner != "" {
			fmt.Printf("       归属 %s / 任务 %s\n", d.Owner, d.JobKey)
		}
	}
	fmt.Printf("  可清理 %d 个（本实例创建且空闲）\n", plan.Deletable)
}

type compositeDropper struct {
	source *runtime.DB
	target *runtime.DB
}

func (c compositeDropper) DropObject(ctx context.Context, name, kind string) error {
	if c.source == nil {
		return errors.New("没有源端连接，无法删除源端对象")
	}
	return c.source.DropObject(ctx, name, kind)
}

func (c compositeDropper) MarkObjectDropped(ctx context.Context, name string) error {
	if c.target == nil {
		return nil
	}
	return c.target.MarkObjectDropped(ctx, name)
}

// cmdConflicts lists recorded conflicts. Resolve is not implemented yet and
// says so instead of half-writing.
func cmdConflicts(args []string) error {
	if len(args) == 0 || args[0] != "list" {
		return errors.New("用法：jinliang conflicts list --target DSN [--job-key KEY] [--limit N] [--json]" +
			"（resolve 未实现：先看清单，人工决定后再改目标行）")
	}
	fs := flag.NewFlagSet("conflicts list", flag.ExitOnError)
	target := fs.String("target", "", "目标端 DSN")
	jobKey := fs.String("job-key", "", "只看这个任务")
	limit := fs.Int("limit", 200, "最多列出多少条")
	jsonOut := fs.Bool("json", false, "JSON 输出")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	tgt, err := openRuntime(*target, "target")
	if err != nil {
		return err
	}
	defer tgt.Close()
	cs, err := remedy.Conflicts(context.Background(), tgt, *jobKey, *limit)
	if err != nil {
		return err
	}
	if *jsonOut {
		return printJSON(cs)
	}
	if len(cs) == 0 {
		fmt.Println("没有冲突记录")
		return nil
	}
	fmt.Printf("冲突 %d 条（unresolved 优先看）\n", len(cs))
	for _, c := range cs {
		state := "未解决"
		if c.Resolved {
			state = "已解决"
		}
		fmt.Printf("  #%d [%s] %s pk=%s policy=%s 源=%s 目标=%s\n",
			c.ID, state, c.Table, c.PK, c.Policy, trimJSON(c.SourceRow), trimJSON(c.TargetRow))
	}
	return nil
}

func trimJSON(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > 120 {
		return s[:120] + "…"
	}
	if s == "" {
		return "-"
	}
	return s
}

// cmdDeadLetter lists and retries dead letters.
func cmdDeadLetter(args []string) error {
	if len(args) == 0 {
		return errors.New("用法：jinliang dead-letter list|retry --target DSN [--job-key KEY] [--id N] [--dry-run]")
	}
	sub := args[0]
	fs := flag.NewFlagSet("dead-letter "+sub, flag.ExitOnError)
	target := fs.String("target", "", "目标端 DSN")
	jobKey := fs.String("job-key", "", "只看这个任务")
	limit := fs.Int("limit", 500, "list 最多列出多少条")
	id := fs.Int64("id", 0, "retry 的死信 id")
	dryRun := fs.Bool("dry-run", false, "retry 只校验，不写目标表")
	jsonOut := fs.Bool("json", false, "JSON 输出")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	tgt, err := openRuntime(*target, "target")
	if err != nil {
		return err
	}
	defer tgt.Close()
	ctx := context.Background()

	switch sub {
	case "list":
		tables, err := tgt.Tables(ctx)
		if err != nil {
			return err
		}
		if !tables[runtime.TableDeadLetter] {
			return errors.New("目标库没有 _jl_dead_letter（任务还没跑过）")
		}
		dls, err := tgt.DeadLetters(ctx, *jobKey, *limit)
		if err != nil {
			return err
		}
		if *jsonOut {
			return printJSON(dls)
		}
		if len(dls) == 0 {
			fmt.Println("没有死信")
			return nil
		}
		fmt.Printf("死信 %d 条\n", len(dls))
		for _, dl := range dls {
			fmt.Printf("  #%d %s 原因=%s 位点=%s 行=%s\n",
				dl.ID, dl.Table, dl.Reason, firstOr(dl.Position, "-"), trimJSON(dl.RawRow))
		}
		return nil
	case "retry":
		if *id <= 0 {
			return errors.New("retry 需要 --id（先跑 dead-letter list 看 id）")
		}
		res, err := remedy.RetryDeadLetter(ctx, tgt, tgt.Schema(), *id, *dryRun)
		if err != nil {
			return err
		}
		if *jsonOut {
			return printJSON(res)
		}
		if res.DryRun {
			fmt.Printf("#%d %s pk=%s：dry-run 校验通过，未写入\n", res.ID, res.Table, res.PK)
			return nil
		}
		fmt.Printf("#%d %s pk=%s：已重放（审计 %s）\n", res.ID, res.Table, res.PK, res.ActionID)
		return nil
	default:
		return fmt.Errorf("不认识的子命令 %q（list / retry）", sub)
	}
}

// cmdUndo is deliberately explicit: no half-working undo.
func cmdUndo(args []string) error {
	return remedy.Undo(context.Background(), nil, "", true)
}
