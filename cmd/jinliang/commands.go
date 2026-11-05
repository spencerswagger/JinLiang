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
	"github.com/spencerswagger/jinliang/internal/doctor"
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

	if !isLoopbackListen(*listen) {
		fmt.Fprintf(os.Stderr, `jinliang serve: 注意 —— 你把控制台暴露在 %s 上了。

  风险：控制台能改连接与任务、能测连你的数据库。局域网里任何人都能打开这个地址。
  建议：1) 只在需要时临时这样起；2) 前面加一层 HTTPS 反向代理；
        3) 至少设一个长口令，并在不使用时降回 127.0.0.1。

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

	job, err := store.ImportYAML(context.Background(), raw, control.ImportOptions{Actor: control.ActorCLI, DryRun: *dry})
	if err != nil {
		return err
	}
	if *dry {
		fmt.Printf("校验通过：%s（dry-run，没有写入）\n", job.Name)
		return nil
	}
	fmt.Printf("已导入任务 %s（id=%s，指纹=%s）\n", job.Name, job.ID, job.JobKey)
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
//
// 体检分三块：控制库（完整性/版本/幽灵运行）、源库（变更流水/书签）、
// 目标库（运行时表/租约/位点/死信/冲突）。控制库之外的检查只有给了 DSN 才做，
// 没给会明确写「跳过」而不是假装正常。
func cmdDoctor(args []string) error {
	fs := flag.NewFlagSet("doctor", flag.ExitOnError)
	jsonOut := fs.Bool("json", false, "JSON 输出")
	dir := fs.String("data-dir", "", "数据目录")
	keyFile := fs.String("key-file", "", "主密钥文件")
	source := fs.String("source", "", "源端 DSN（可省略：省略则跳过源端体检）")
	target := fs.String("target", "", "目标端 DSN（可省略）")
	jobKey := fs.String("job-key", "", "任务指纹（读目标端 _jl_meta / 死信 / 冲突）")
	slotPrefix := fs.String("slot-prefix", "", "同步书签前缀（默认 jl_<job-key>）")
	heartbeat := fs.Duration("heartbeat-interval", doctor.DefaultHeartbeatInterval,
		"运行心跳间隔；过期阈值是 3×（默认 30s → 90s，与控制面 runs-liveness 一致）")
	if err := fs.Parse(args); err != nil {
		return err
	}
	store, err := openStore(dataDir(*dir), *keyFile, false)
	if err != nil {
		return err
	}
	defer store.Close()
	ctx := context.Background()

	opts := doctor.Options{
		Control:             storeControl{s: store},
		LatestSchemaVersion: control.LatestSchemaVersion(),
		HeartbeatInterval:   *heartbeat,
		JobKey:              strings.TrimSpace(*jobKey),
		SlotPrefix:          strings.TrimSpace(*slotPrefix),
	}
	if opts.JobKey != "" && opts.SlotPrefix == "" {
		opts.SlotPrefix = "jl_" + opts.JobKey
	}
	if strings.TrimSpace(*source) != "" {
		db, err := openRuntime(*source, "source")
		if err != nil {
			return err
		}
		defer db.Close()
		opts.Source = db
	}
	if strings.TrimSpace(*target) != "" {
		db, err := openRuntime(*target, "target")
		if err != nil {
			return err
		}
		defer db.Close()
		opts.Target = db
	}
	rep := doctor.Run(ctx, opts)

	stats, _ := store.DBStats(ctx)
	if *jsonOut {
		return printJSON(map[string]any{
			"version":    version,
			"data_dir":   store.Dir(),
			"db_path":    store.Path(),
			"db_bytes":   stats["db_bytes"],
			"counts":     stats,
			"key_source": string(store.Cipher().KeySource()),
			"key_path":   store.Cipher().KeyPath(),
			"report":     rep,
		})
	}

	fmt.Printf("津梁 %s\n", version)
	fmt.Printf("  实例      %s\n", rep.InstanceID)
	fmt.Printf("  数据目录  %s\n", store.Dir())
	fmt.Printf("  控制库    %s\n", store.Path())
	fmt.Printf("  密钥      %s", store.Cipher().KeySource())
	if p := store.Cipher().KeyPath(); p != "" {
		fmt.Printf("（%s）", p)
	}
	fmt.Println()
	fmt.Printf("  行数      连接 %d / 任务 %d / 运行 %d / 审计 %d\n",
		stats["connections"], stats["jobs"], stats["runs"], stats["audit"])
	fmt.Println("  体检：")
	for _, c := range rep.Checks {
		mark := "OK  "
		switch c.Status {
		case doctor.StatusWarn:
			mark = "注意"
		case doctor.StatusProblem:
			mark = "问题"
		}
		fmt.Printf("    [%s] %-7s %s\n", mark, c.Area, c.Title)
		if c.Detail != "" {
			fmt.Printf("           %s\n", c.Detail)
		}
		if c.Hint != "" {
			fmt.Printf("           能做什么：%s\n", c.Hint)
		}
	}
	if rep.Healthy {
		fmt.Println("  结论      正常")
		return nil
	}
	fmt.Printf("  结论      有 %d 个问题、%d 个提醒\n", rep.Problems, rep.Warnings)
	return errors.New("体检发现问题")
}
