package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/engine"
	"github.com/spencerswagger/jinliang/internal/httpapi"
)

// dataDir resolves --data-dir / JINLIANG_DATA_DIR / default.
func dataDir(flagValue string) string {
	if strings.TrimSpace(flagValue) != "" {
		return flagValue
	}
	if v := strings.TrimSpace(os.Getenv("JINLIANG_DATA_DIR")); v != "" {
		return v
	}
	return control.DefaultDataDir()
}

// openStore opens the control database with the key resolution rules from
// docs/CONTROL-PLANE.md §5.
func openStore(dir, keyFile string, autogen bool) (*control.Store, error) {
	return openStoreWithHeartbeat(dir, keyFile, autogen, 0)
}

// openStoreWithHeartbeat lets `serve` pin the heartbeat interval (everything else
// uses the 30s default; only serve actually runs jobs).
func openStoreWithHeartbeat(dir, keyFile string, autogen bool, heartbeat time.Duration) (*control.Store, error) {
	return control.Open(control.Options{
		DataDir:   dir,
		KeyFile:   keyFile,
		EnvKey:    os.Getenv("JINLIANG_KEY"),
		Autogen:   autogen,
		Heartbeat: heartbeat,
	})
}

// heartbeatInterval resolves --heartbeat seconds / JINLIANG_HEARTBEAT_SECONDS.
// 30s 默认、上限 10 分钟：再长就不是心跳了（B1 的长事务靠它续命，不能太懒）。
func heartbeatInterval(flagSeconds int) (time.Duration, error) {
	if flagSeconds <= 0 {
		if v := strings.TrimSpace(os.Getenv("JINLIANG_HEARTBEAT_SECONDS")); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil {
				return 0, fmt.Errorf("JINLIANG_HEARTBEAT_SECONDS=%q 不是整数", v)
			}
			flagSeconds = n
		}
	}
	if flagSeconds == 0 {
		return control.DefaultHeartbeatInterval, nil
	}
	if flagSeconds < 5 || flagSeconds > 600 {
		return 0, fmt.Errorf("心跳间隔要在 5–600 秒之间（收到 %d）", flagSeconds)
	}
	return time.Duration(flagSeconds) * time.Second, nil
}

func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:8080", "监听地址；改成 0.0.0.0 会让同网络的人也能访问")
	dir := fs.String("data-dir", "", "数据目录（默认 ~/.config/jinliang）")
	keyFile := fs.String("key-file", "", "主密钥文件（必须 0600）")
	apiToken := fs.String("api-token", os.Getenv("JINLIANG_API_TOKEN"), "固定 bearer token（自动化用）")
	hbSeconds := fs.Int("heartbeat", 0, "运行心跳间隔（秒，默认 30；也可用 JINLIANG_HEARTBEAT_SECONDS）")
	if err := fs.Parse(args); err != nil {
		return err
	}

	hb, err := heartbeatInterval(*hbSeconds)
	if err != nil {
		return err
	}
	stale := control.StaleAfter(hb)

	store, err := openStoreWithHeartbeat(dataDir(*dir), *keyFile, true, hb)
	if err != nil {
		return err
	}
	defer store.Close()

	// SSE 的 id 是 <boot_epoch>-<seq>：只有 serve 写 epoch（doctor/export 不写，
	// 否则设置页的 boot_epoch 会一直跳）。
	store.StartBootEpoch(context.Background())

	// 上一次进程可能被 kill -9：心跳过期的 run 永远 ended_at IS NULL，
	// 历史页会永久显示「在跑」并且挡住下一次运行。启动时替它们收尾。
	if n, err := store.RecoverStaleRuns(context.Background(), stale); err != nil {
		return fmt.Errorf("清点上次遗留的运行失败: %w", err)
	} else if n > 0 {
		fmt.Printf("  上次有 %d 条运行没有正常收尾（进程被终止），已标记为 interrupted\n", n)
	}

	if !isLoopbackListen(*listen) {
		fmt.Fprintf(os.Stderr, `jinliang serve: 注意 —— 你把控制台暴露在 %s 上了。

  风险：控制台能改连接与任务、能测连你的数据库。局域网里任何人都能打开这个地址。
  建议：1) 只在需要时临时这样起；2) 前面加一层 HTTPS 反向代理；
        3) 至少设一个长口令，并在不使用时降回 127.0.0.1。

  反代注意：走反向代理时，首次设置口令必须在**跑控制台的那台机器上**完成
  （反代会给请求带上 X-Forwarded-For 这类头，我们一律按「非本机」fail-closed
  拦掉），设好口令后再从外部访问。

`, *listen)
	}

	srv, err := httpapi.New(httpapi.Options{
		Store: store, Listen: *listen, Version: version, APIToken: strings.TrimSpace(*apiToken),
	})
	if err != nil {
		return err
	}

	configured, err := store.AdminConfigured(context.Background())
	if err != nil {
		return err
	}
	url := "http://" + *listen + "/"
	fmt.Printf("津梁控制台 %s\n", version)
	fmt.Printf("  地址      %s\n", url)
	fmt.Printf("  数据目录  %s\n", store.Dir())
	fmt.Printf("  控制库    %s\n", store.Path())
	fmt.Printf("  运行心跳  每 %s 一次；超过 %s 没有心跳视为进程已死（可 --heartbeat 调）\n", hb, stale)
	fmt.Printf("  密钥来源  %s", store.Cipher().KeySource())
	if p := store.Cipher().KeyPath(); p != "" {
		fmt.Printf("（%s）", p)
	}
	fmt.Println()
	if !configured {
		fmt.Println("  首次启动：打开上面的地址，先设一个管理员口令")
	} else if strings.TrimSpace(*apiToken) != "" {
		fmt.Println("  已启用固定 API token（--api-token）：curl 可直接带 Authorization: Bearer <token>")
	}
	fmt.Println("  Ctrl-C 退出（正在跑的骨架运行会收尾后退出）")

	httpSrv := &http.Server{
		Addr:              *listen,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		// SSE 需要长连接，所以不给 WriteTimeout
		IdleTimeout: 120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		fmt.Println("\n正在关闭……")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	return nil
}

// cmdImport turns job.yaml files into control-database rows.
func cmdImport(args []string) error {
	fs := flag.NewFlagSet("import", flag.ExitOnError)
	file := fs.String("file", "", "job.yaml 路径（- 表示从 stdin 读）")
	dry := fs.Bool("dry-run", false, "只校验，不写控制库")
	dir := fs.String("data-dir", "", "数据目录")
	keyFile := fs.String("key-file", "", "主密钥文件")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*file) == "" {
		return errors.New("缺 --file（或 --file - 从标准输入读）")
	}
	var raw []byte
	var err error
	if *file == "-" {
		raw, err = io.ReadAll(os.Stdin)
	} else {
		raw, err = os.ReadFile(*file)
	}
	if err != nil {
		return err
	}
	store, err := openStore(dataDir(*dir), *keyFile, !*dry)
	if err != nil {
		return err
	}
	defer store.Close()

	report, err := store.ImportYAMLDocs(context.Background(), raw, control.ImportOptions{Actor: control.ActorCLI, DryRun: *dry})
	if err != nil && report.Total == 0 {
		return err
	}
	if *dry {
		fmt.Printf("校验：%s\n", report.Summary())
	} else {
		for _, job := range report.Imported {
			fmt.Printf("已导入任务 %s（id=%s，指纹=%s）\n", job.Name, job.ID, job.JobKey)
		}
		fmt.Printf("%s\n", report.Summary())
	}
	for _, skip := range report.Skipped {
		fmt.Fprintf(os.Stderr, "跳过第 %d 份（%s）：%s\n", skip.Index, skip.Name, skip.Reason)
	}
	if len(report.Skipped) > 0 {
		// 有跳过的导入绝不能返回 0：脚本/CI 会把它当成「全部恢复成功」。
		return fmt.Errorf("%s —— 备份没有完整恢复，见上面每份的原因", report.Summary())
	}
	if *dry {
		return nil
	}
	fmt.Println("下一步：jinliang serve → 打开任务页跑一次预检")
	return nil
}

// cmdExport writes job.yaml from the control database.
func cmdExport(args []string) error {
	fs := flag.NewFlagSet("export", flag.ExitOnError)
	jobRef := fs.String("job", "", "只导出这一个任务（id 或名字）")
	all := fs.Bool("all", false, "导出全部任务（每份之间用 --- 分隔）")
	out := fs.String("out", "-", "输出文件（- 表示标准输出）")
	dir := fs.String("data-dir", "", "数据目录")
	keyFile := fs.String("key-file", "", "主密钥文件")
	if err := fs.Parse(args); err != nil {
		return err
	}
	store, err := openStore(dataDir(*dir), *keyFile, false)
	if err != nil {
		return err
	}
	defer store.Close()
	ctx := context.Background()

	var buf strings.Builder
	switch {
	case *all || *jobRef == "":
		jobs, err := store.ListJobs(ctx)
		if err != nil {
			return err
		}
		if len(jobs) == 0 {
			return errors.New("控制库里还没有任务")
		}
		for _, j := range jobs {
			data, err := store.ExportYAML(ctx, j.Job)
			if err != nil {
				return err
			}
			buf.WriteString("---\n")
			buf.Write(data)
		}
	default:
		job, err := store.ResolveJob(ctx, *jobRef)
		if err != nil {
			return fmt.Errorf("找不到任务 %q: %w", *jobRef, err)
		}
		data, err := store.ExportYAML(ctx, job)
		if err != nil {
			return err
		}
		buf.Write(data)
	}

	if *out == "-" {
		_, err := os.Stdout.WriteString(buf.String())
		return err
	}
	return os.WriteFile(*out, []byte(buf.String()), 0o600)
}

// cmdRun is the headless execution path: no browser, no server. It runs the
// real engine (B1) and prints the same events the UI would show.
func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	jobRef := fs.String("job", "", "任务 id 或名字（必填）")
	jsonOut := fs.Bool("json", false, "事件以 JSON Lines 输出（给自动化用）")
	dir := fs.String("data-dir", "", "数据目录")
	keyFile := fs.String("key-file", "", "主密钥文件")
	snapshotOnly := fs.Bool("snapshot-only", false, "只搬存量数据，不进增量")
	stopAfter := fs.Duration("stop-after", 0, "跑这么久之后收尾（冒烟测试用；0 = 一直跑）")
	idleAfter := fs.Duration("stop-when-idle", 0, "追平后安静这么久就收尾（一次性增量同步用；0 = 一直跑）")
	maxLag := fs.Int64("max-lag", 0, "配 --stop-when-idle：源端积压小于这么多字节才算追平（默认 8MB）")
	verify := fs.Bool("verify", false, "跑完对比源端与目标端的行数与指纹")
	batchRows := fs.Int("batch-rows", 0, "每批行数（默认取任务的 reliability.batch_rows）")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*jobRef) == "" {
		return errors.New("缺 --job（可用 id 或名字，jinliang export --all 能看到）")
	}
	store, err := openStore(dataDir(*dir), *keyFile, true)
	if err != nil {
		return err
	}
	defer store.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	job, err := store.ResolveJob(ctx, *jobRef)
	if err != nil {
		return fmt.Errorf("找不到任务 %q: %w", *jobRef, err)
	}

	enc := newEventPrinter(*jsonOut, os.Stdout)
	ch, cancel := store.Bus().Subscribe(job.ID)
	defer cancel()
	printDone := make(chan struct{})
	go func() {
		defer close(printDone)
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-ch:
				if !ok {
					return
				}
				enc.print(ev)
			}
		}
	}()

	res, runErr := engine.Run(ctx, engine.Options{
		Store:        store,
		JobID:        job.ID,
		Actor:        control.ActorCLI,
		SnapshotOnly: *snapshotOnly,
		StopAfter:    *stopAfter,
		IdleAfter:    *idleAfter,
		LagThreshold: *maxLag,
		Verify:       *verify,
		BatchRows:    *batchRows,
	})
	time.Sleep(30 * time.Millisecond) // 让最后几条事件先打印出来
	cancel()
	<-printDone

	if !*jsonOut {
		fmt.Printf("\n结果：阶段 %s，复制 %d 行，增量 %d 行，用时 %s，退出码 %d\n",
			res.Phase, res.RowsCopied, res.RowsStream, res.Elapsed.Truncate(time.Millisecond), res.ExitCode)
		if res.Position != "" {
			fmt.Printf("位点：%s（目标端已确认\n", res.Position)
		}
		for _, d := range res.Mismatches {
			fmt.Println("差异：" + d)
		}
		if res.Note != "" {
			fmt.Println("说明：" + res.Note)
		}
		if res.ExitCode == engine.ExitStopped {
			fmt.Println("（手动停止用 Ctrl-C 也可以：会在提交完手上这个事务后收尾）")
		}
	}
	if res.ExitCode == engine.ExitStopped {
		return &exitError{code: engine.ExitStopped, msg: "已手动停止"}
	}
	if runErr != nil {
		return runErr
	}
	return nil
}

// cmdStatus prints where a job is now, without touching anything.
func cmdStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	jobRef := fs.String("job", "", "任务 id 或名字（空 = 列出全部任务）")
	jsonOut := fs.Bool("json", false, "JSON 输出")
	dir := fs.String("data-dir", "", "数据目录")
	keyFile := fs.String("key-file", "", "主密钥文件")
	verify := fs.Bool("verify", false, "顺便比对源端与目标端（只读，会扫表）")
	if err := fs.Parse(args); err != nil {
		return err
	}
	store, err := openStore(dataDir(*dir), *keyFile, false)
	if err != nil {
		return err
	}
	defer store.Close()
	ctx := context.Background()

	if strings.TrimSpace(*jobRef) == "" {
		jobs, err := store.ListJobs(ctx)
		if err != nil {
			return err
		}
		if len(jobs) == 0 {
			return errors.New("控制库里还没有任务：先 jinliang import --file job.yaml 或打开 jinliang serve")
		}
		for _, j := range jobs {
			line := fmt.Sprintf("%s  %s", j.ID, j.Name)
			if j.LastRun != nil {
				line += fmt.Sprintf("  [%s", j.LastRun.Phase)
				if j.LastRun.RowsDone > 0 {
					line += fmt.Sprintf(" %d 行", j.LastRun.RowsDone)
				}
				line += "]"
			} else {
				line += "  [还没跑过]"
			}
			fmt.Println(line)
		}
		return nil
	}

	st, err := engine.StatusFor(ctx, engine.Options{Store: store, JobID: *jobRef})
	if err != nil {
		return err
	}
	if *jsonOut {
		return printJSON(st)
	}
	fmt.Printf("任务      %s（%s）\n", st.Name, st.JobID)
	fmt.Printf("指纹      %s\n", st.JobKey)
	fmt.Printf("源 → 目标 %s → %s\n", st.Source, st.Target)
	fmt.Printf("模式      %s，%d 张表\n", st.Mode, len(st.Tables))
	fmt.Printf("阶段      %s\n", orDash(st.Phase))
	fmt.Printf("位点      %s（更新于 %s）\n", orDash(st.Position), orDash(st.UpdatedAt))
	fmt.Printf("落后      %s（书签占用 %s）\n", humanBytes(st.LagBytes), humanBytes(st.SlotBytes))
	if st.LastRun != nil {
		fmt.Printf("最近一次  %s 开始，阶段 %s，%d 行", st.LastRun.StartedAt, st.LastRun.Phase, st.LastRun.RowsDone)
		if st.LastRun.EndedAt != "" {
			fmt.Printf("，结束于 %s", st.LastRun.EndedAt)
		}
		if st.LastRun.ExitCode != nil {
			fmt.Printf("，退出码 %d", *st.LastRun.ExitCode)
		}
		fmt.Println()
		if st.LastRun.LastError != "" {
			fmt.Printf("上次错误  %s\n", st.LastRun.LastError)
		}
	}
	if st.Note != "" {
		fmt.Printf("说明      %s\n", st.Note)
	}
	if *verify {
		// 校验是两次独立查询（源一次、目标一次）：任务还在写的时候两次查询不是同一时点，
		// 差异可能只是「中间又搬了几行」。先把话说清楚，别让人白惊慌。
		if st.LastRun != nil && st.LastRun.EndedAt == "" {
			fmt.Println("注意      这个任务还有一次运行没结束（进行中）：此刻的比对是两个不同时点的快照，")
			fmt.Println("          差异可能只是中间又搬了几行。校验只在停写之后才有意义。")
		}
		diffs, err := engine.VerifyJob(ctx, engine.Options{Store: store, JobID: st.JobID})
		if err != nil {
			return err
		}
		if len(diffs) == 0 {
			fmt.Println("校验      一致")
		} else {
			fmt.Println("校验      有差异：")
			for _, d := range diffs {
				fmt.Println("  - " + d)
			}
			return &exitError{code: 5, msg: "校验发现不一致"}
		}
	}
	return nil
}

func orDash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "—"
	}
	return s
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	units := []string{"KB", "MB", "GB", "TB"}
	v := float64(n)
	i := -1
	for v >= unit && i < len(units)-1 {
		v /= unit
		i++
	}
	return fmt.Sprintf("%.1f %s", v, units[i])
}

// exitError carries a specific exit code up to main (0 完成 / 1 失败 / 130 手动停止).
type exitError struct {
	code int
	msg  string
}

func (e *exitError) Error() string { return e.msg }

// cmdDoctor prints a health report. Exit code 1 when something needs attention,
// so a container healthcheck can just run it.
func cmdDoctor(args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ExitOnError)
	jsonOut := fs.Bool("json", false, "JSON 输出")
	dir := fs.String("data-dir", "", "数据目录")
	keyFile := fs.String("key-file", "", "主密钥文件")
	if err := fs.Parse(args); err != nil {
		return err
	}
	store, err := openStore(dataDir(*dir), *keyFile, false)
	if err != nil {
		return err
	}
	defer store.Close()
	ctx := context.Background()

	instance, _ := store.InstanceID(ctx)
	integrity, ierr := store.IntegrityCheck(ctx)
	stats, _ := store.DBStats(ctx)
	configured, _ := store.AdminConfigured(ctx)
	schemaV, _ := store.SchemaVersion(ctx)
	active, _ := store.ActiveRuns(ctx)
	plan := control.MigrationPlan(schemaV)

	var problems []string
	if ierr != nil || integrity != "ok" {
		problems = append(problems, "控制库完整性检查没通过："+integrity)
	}
	if !configured {
		problems = append(problems, "还没有管理员口令：跑一次 jinliang serve 打开页面设置")
	}
	if len(plan) > 0 {
		problems = append(problems, "有未应用的迁移："+strings.Join(plan, ", "))
	}

	report := map[string]any{
		"version":          version,
		"instance_id":      instance,
		"data_dir":         store.Dir(),
		"db_path":          store.Path(),
		"db_bytes":         stats["db_bytes"],
		"counts":           stats,
		"schema_version":   store.SchemaVersionText(ctx),
		"key_source":       string(store.Cipher().KeySource()),
		"key_path":         store.Cipher().KeyPath(),
		"admin_configured": configured,
		"integrity":        integrity,
		"active_runs":      len(active),
		"problems":         problems,
		"healthy":          len(problems) == 0,
	}
	if *jsonOut {
		return printJSON(report)
	}
	fmt.Printf("津梁 %s\n", version)
	fmt.Printf("  实例      %s\n", instance)
	fmt.Printf("  数据目录  %s\n", store.Dir())
	fmt.Printf("  控制库    %s（schema %s）\n", store.Path(), store.SchemaVersionText(ctx))
	fmt.Printf("  密钥      %s", store.Cipher().KeySource())
	if p := store.Cipher().KeyPath(); p != "" {
		fmt.Printf("（%s）", p)
	}
	fmt.Println()
	fmt.Printf("  行数      连接 %d / 任务 %d / 运行 %d / 审计 %d\n",
		stats["connections"], stats["jobs"], stats["runs"], stats["audit"])
	fmt.Printf("  完整性    %s\n", integrity)
	fmt.Printf("  管理员    %v\n", configured)
	if len(problems) == 0 {
		fmt.Println("  结论      正常")
		return nil
	}
	fmt.Println("  需要处理：")
	for _, p := range problems {
		fmt.Printf("    - %s\n", p)
	}
	return errors.New("体检发现问题")
}
