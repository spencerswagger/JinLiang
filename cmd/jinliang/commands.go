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
	"strings"
	"syscall"
	"time"

	"github.com/spencerswagger/jinliang/internal/control"
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
	return control.Open(control.Options{
		DataDir: dir,
		KeyFile: keyFile,
		EnvKey:  os.Getenv("JINLIANG_KEY"),
		Autogen: autogen,
	})
}

func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	listen := fs.String("listen", "127.0.0.1:8080", "监听地址；改成 0.0.0.0 会让同网络的人也能访问")
	dir := fs.String("data-dir", "", "数据目录（默认 ~/.config/jinliang）")
	keyFile := fs.String("key-file", "", "主密钥文件（必须 0600）")
	apiToken := fs.String("api-token", os.Getenv("JINLIANG_API_TOKEN"), "固定 bearer token（自动化用）")
	if err := fs.Parse(args); err != nil {
		return err
	}

	store, err := openStore(dataDir(*dir), *keyFile, true)
	if err != nil {
		return err
	}
	defer store.Close()

	// 上一次进程可能被 kill -9：心跳过期的 run 永远 ended_at IS NULL，
	// 历史页会永久显示「在跑」并且挡住下一次运行。启动时替它们收尾。
	if n, err := store.RecoverStaleRuns(context.Background(), 2*time.Minute); err != nil {
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

// cmdRun is the headless execution path: no browser, no server.
func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	jobRef := fs.String("job", "", "任务 id 或名字（必填）")
	jsonOut := fs.Bool("json", false, "事件以 JSON Lines 输出（给自动化用）")
	dir := fs.String("data-dir", "", "数据目录")
	keyFile := fs.String("key-file", "", "主密钥文件")
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
	if _, err := store.Runner().Start(ctx, job.ID, control.ActorCLI); err != nil {
		return err
	}

	ch, cancel := store.Bus().Subscribe(job.ID)
	defer cancel()

	enc := newEventPrinter(*jsonOut, os.Stdout)
	for {
		select {
		case <-ctx.Done():
			return nil
		case ev, ok := <-ch:
			if !ok {
				return nil
			}
			enc.print(ev)
			if ev.Type == "done" || ev.Type == "error" {
				if ev.Type == "error" {
					return errors.New(ev.Message)
				}
				return nil
			}
		}
	}
}

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
