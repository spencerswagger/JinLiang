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
	"github.com/spencerswagger/jinliang/internal/doctor"
	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/engine"
	"github.com/spencerswagger/jinliang/internal/httpapi"
	"github.com/spencerswagger/jinliang/internal/pgsink"
	"github.com/spencerswagger/jinliang/internal/state"
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
	watch := fs.Bool("watch", false, "按 mode.interval 一轮轮跑（interval / full_interval 用）；Ctrl-C 停止")
	verify := fs.Bool("verify", false, "跑完对比源端与目标端的行数与指纹")
	batchRows := fs.Int("batch-rows", 0, "每批行数（默认取任务的 reliability.batch_rows）")
	resync, yes := registerResyncFlags(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*jobRef) == "" {
		return errors.New("缺 --job（可用 id 或名字，jinliang export --all 能看到）")
	}
	if *resync && *watch {
		// --resync 是「这一轮从零重来」，--watch 是「一轮轮跑」：把两者放一起，
		// 要么每一轮都清表（等于持续删目标端数据），要么只有第一轮生效却没写在文档里。
		return errors.New("--resync 不能和 --watch 一起用：先跑一次 --resync（单轮），再起 --watch。" +
			"风险：没有（什么都没动）")
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
	// 确认放在解析之后：先把「哪个任务」查清楚再问，避免用户对着一个拼错的
	// --job 回答 yes，然后在下一步才看到「找不到任务」。
	if *resync && !*yes {
		if err := confirmResync(job.Name); err != nil {
			return err
		}
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

	if *watch {
		// 调度模式：一个人一轮轮跑，每轮一条 kind=sync 的 run 行。
		err := engine.Schedule(ctx, engine.Options{
			Store: store, JobID: job.ID, Actor: control.ActorCLI, BatchRows: *batchRows,
		})
		if err != nil {
			return err
		}
		if !*jsonOut {
			fmt.Println("调度已停止（Ctrl-C）")
		}
		return nil
	}
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
		Resync:       *resync,
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
		if rep := res.Resync; rep != nil && res.ExitCode == engine.ExitOK {
			// 「谁触发了、重建了哪个位点、清了哪些表」——用户回头查的正是这三件事。
			// 引擎已把同一份写进 audit（action=job.resync）；这里让当场的输出也对得上。
			fmt.Printf("--resync：书签 %s，旧位点 %q 已清空，清了 %d 张表：%s\n",
				rep.Slot, rep.OldPosition, len(rep.Tables), strings.Join(rep.Tables, ", "))
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
	// 退出码是引擎**单点派生**的（engine.DecideExit：失败 1 > 停止 130 > 有暂停表 3 > 0），
	// CLI 只负责把它交给进程，**不在这里重新推导**。
	//
	// 为什么必须显式写这一段：#109 之前这里只认 ExitStopped，于是「有表被暂停」那一轮
	// 打印出「退出码 3」却 `return nil` → 进程退 0。日志说 3、`$?` 是 0，容器与 CI 读的是
	// `$?`，用户看到的是「一切正常」，欠账被静默吞掉——比报错更糟。
	if res.ExitCode != engine.ExitOK {
		return &exitError{code: res.ExitCode, msg: needsActionNote(job.Name, res.Paused)}
	}
	return nil
}

// needsActionNote 是「跑完了但有欠账」（退出码 3）那一行要讲清的话。
//
// 四段齐（提示即文档）：发生了什么 / 为什么 / 你能做什么（含命令）/ 风险。
// 退出码本身是粗信号，权威是 status --json 的 paused_tables —— 所以这里必须给出那条命令，
// 否则用户只拿到一个 3，无从下手。
func needsActionNote(jobRef string, paused []engine.PausedTableStat) string {
	if len(paused) == 0 {
		// 将来 blocked/「需要确认」也复用 3，那时未必有暂停表。
		return fmt.Sprintf("跑完了，但有需要你处理的事（退出码 %d）。看 `jinliang status --job %s --json`",
			engine.ExitNeedsAction, jobRef)
	}
	names := make([]string, 0, len(paused))
	for _, p := range paused {
		names = append(names, p.Table)
	}
	return fmt.Sprintf("跑完了，但有 %d 张表被暂停（%s）：这几张表的数据还不全，其它表已经同步完。"+
		"这不是失败——看 `jinliang status --job %s --json` 的 paused_tables 拿到表名与欠的行数；"+
		"把目标端结构对齐（例如按 detail 里给的源端类型改列）后重跑，那几张表会整表重搬补齐"+
		"（风险：重搬会先清空这几张表在目标端的现有内容）",
		len(paused), strings.Join(names, "、"), jobRef)
}

// registerResyncFlags 是 --resync / --yes 的**唯一注册点**。
//
// 单独抽出来是为了让「文案承诺」能被机器验证：E3001 的 Next 里写着
// `jinliang run --job <任务名> --resync`，用例拿着这句话来这个函数上问
// 「这个旗标真的注册了吗」——而不是靠人记得在两处同步。
func registerResyncFlags(fs *flag.FlagSet) (resync, yes *bool) {
	resync = fs.Bool("resync", false, "重建同步书签/位点，先清空本任务建的表再整轮重搬（不可撤销）")
	yes = fs.Bool("yes", false, "危险动作（--resync）不再交互确认；容器/CI 里必须显式带上")
	return resync, yes
}

// confirmResync 是 --resync 的二次确认。
//
// 为什么非交互环境**必须**显式给 --yes，而不是读到 EOF 就当取消：容器/CI 里
// stdin 常常是空的，静默取消会让「脚本以为跑了、其实什么都没做」——那和
// 「跑了但没重建」是同一类错。所以这里直接拒绝，把决定权交回给人。
func confirmResync(jobRef string) error {
	info, err := os.Stdin.Stat()
	if err != nil {
		return fmt.Errorf("读不到标准输入，无法确认 --resync；非交互环境请显式加 --yes（风险：不会重建、也不会清表）: %w", err)
	}
	if info.Mode()&os.ModeCharDevice == 0 {
		return errors.New("--resync 会清空本任务建的目标表，非交互环境（stdin 不是终端）必须显式加 --yes。" +
			"风险：没有（什么都没动）")
	}
	fmt.Printf(`即将整轮重搬任务 %q：
  - 重建源端同步书签：旧的会被删掉，变更流从新起点开始
  - 清空本任务在目标端建的每一张表，然后从零重搬（搬到一半期间目标端是部分数据，不是旧但完整）
  - 不动任务身份（job_key），也不删运行历史与审计
这一步不可撤销。确认请输入 yes：`, jobRef)
	var answer string
	if _, err := fmt.Scanln(&answer); err != nil {
		// EOF（容器/CI 的 /dev/null、或者终端里 Ctrl-D）**不能**当成「用户取消」：
		// 静默取消会让脚本以为已经重搬过。这里一律按「没确认」拒绝，并指名 --yes。
		return fmt.Errorf("没读到确认（--resync 已取消，什么都没动）：非交互环境请显式加 --yes 再跑: %w", err)
	}
	if strings.ToLower(strings.TrimSpace(answer)) != "yes" {
		return errors.New("已取消：什么都没动")
	}
	return nil
}

// cmdStatus prints where a job is now, without touching anything.
func cmdDelete(args []string) error {
	fs := flag.NewFlagSet("delete", flag.ExitOnError)
	dir := fs.String("data-dir", "", "数据目录（默认 ~/.config/jinliang）")
	jobRef := fs.String("job", "", "任务 ID 或名字")
	wait := fs.Duration("wait", 30*time.Second, "等引擎退出多久；等不到就留一条 audit 说明原因")
	force := fs.Bool("yes", false, "不再交互确认")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*jobRef) == "" {
		return errors.New("delete 需要 --job <ID 或名字>")
	}
	store, err := openStore(dataDir(*dir), "", false)
	if err != nil {
		return err
	}
	defer store.Close()
	ctx := context.Background()
	job, err := store.ResolveJob(ctx, *jobRef)
	if err != nil {
		return err
	}
	if !*force {
		fmt.Printf("即将删除任务 %s（%s）：runs 记录会一起删掉，目标端已同步的数据不受影响。\n", job.Name, job.JobKey)
		fmt.Print("确认删除请输入 yes：")
		var answer string
		fmt.Scanln(&answer)
		if strings.ToLower(strings.TrimSpace(answer)) != "yes" {
			fmt.Println("已取消，什么都没删。")
			return nil
		}
	}

	// **先停引擎，再删记录**：反过来会留下「记录没了、goroutine 还在写、租约还在续期」，
	// 同目标新建任务直接撞 E1005，而用户没有任何办法让那个进程停下来。
	canceled, stopped, serr := engine.StopJob(ctx, store, job.ID, job.JobKey, *wait)
	switch {
	case !canceled:
		fmt.Println("本进程里没有这个任务在跑（可能在另一个进程里，或已经结束）——直接删记录。")
	case stopped:
		fmt.Println("已让正在跑的引擎停下并等它退出（退出时已释放目标端租约）。")
	default:
		fmt.Println("已经在跑的那个引擎没有在等待时间内退出。已记一条 audit 说明原因；" +
			"目标端租约会在 TTL 后过期并可被抢占，期间的 run 会报 E1005——再跑一次即可接管。")
	}
	// **退出码必须反映事实**：等不到退出时，以前这里用的是「错误非空**且**已确认停下」
	// 的双条件——而「已确认停下」在超时那一刻恰恰为假，于是条件恒假、超时错误被吞、
	// delete 退出码 0：用户看到「删除成功」而引擎其实没停。
	// 现在只看错误本身：超时 = 非 0 退出，并说清楚为什么没停干净。
	//
	// 注：这里刻意**不复述旧表达式的字面量**——注释里写旧代码，会让后来的 grep / review
	// 误以为代码里还有它（今晚已经发生过一次：有人 grep 到注释就判「带病合并」）。
	// 注释只描述语义，代码才出现符号。
	//
	// 回归见 TestStopJobTimeoutIsNotSilent（**指向测试，不指向 PR 号**：
	// 测试会被维护、PR 号会失效——本仓库已有 PR 号被分支删除连坐的先例）。
	var stopTimeout error
	if serr != nil {
		stopTimeout = serr
		fmt.Println("⚠️ 引擎没有在等待时间内退出：删除会继续，但**这一轮不算干净停下**。")
		fmt.Printf("   原因：%v\n", serr)
		fmt.Println("   影响：目标端租约删掉后，那个进程最多约 10 秒才会自己发现并退出；" +
			"期间请勿在同一目标新建/重跑任务。")
	}
	// 跨进程的情况：那个引擎不在本进程里，cancel 不到它。做法是**删掉目标端租约行**——
	// 它下一次续租会失败两次 → loseLease → cancelRun 自己停下（#30 的语义）。
	// 不这么做，用户删了任务后租约还会续期，同目标新建任务一直撞 E1005。
	holder, alive, hasLease, lerr := releaseTargetLease(ctx, store, job)
	switch {
	case !hasLease:
		// 目标端没有租约能力（例如 MySQL 侧还没接状态表租约）：什么都不用做，
		// 也不能打印一句假的「清租约失败」。
		fmt.Println("该目标端没有租约（不支持租约能力），跳过清租约这一步。")
	case lerr != nil:
		fmt.Printf("清目标端租约失败（%v）：租约会在 TTL 后过期并可被抢占。\n", lerr)
	case holder != "":
		// 窗口要说出来：`watchLease` 对「行没了」(ok==false) 是**立刻** loseLease，
		// 所以最坏 = leaseTTL/3 ≈ 10 秒，不是等 TTL。这 10 秒里新任务确实能起来 → 双写，
		// 同一 job 同主键 upsert 多数幂等，但「删了重建时改了 content.tables/on_exists」不幂等。
		if alive {
			fmt.Println("（原持有者的进程还活着——这次是强制踢掉一个正在写的进程。）")
		} else {
			fmt.Println("（原持有者的进程已经不在了——这次清掉的是一条尸体租约。）")
		}
		fmt.Printf("已通知目标端那个还在跑的进程停下（原持有者 %s：租约行已删除，最坏约 10 秒内自行退出）。\n", holder)
		fmt.Println("这 10 秒内请勿在同一目标新建/重跑任务——两个写入者会同时写（同主键 upsert 多数幂等，" +
			"但如果你改了 content.tables 或 on_exists，就可能不幂等）。")
		_ = store.Audit(ctx, control.ActorSystem, "job.run.lease_forced", map[string]any{
			"job_id": job.ID, "job_key": job.JobKey, "job_name": job.Name,
			"holder": holder, "holder_alive": alive,
		})
	}
	// 守卫会拒删「还有未结束 run」的任务，而我们刚刚才把那个写入者踢下线——
	// 所以这里等它自己收尾（最多 15 秒），等不到就**由我们替它收尾**（标 interrupted，
	// 原因写清是删除时强制的），再删。不能让用户卡在「踢了引擎却删不掉」。
	if err := settleActiveRuns(ctx, store, job.ID, 15*time.Second); err != nil {
		return err
	}
	if err := store.DeleteJob(ctx, job.ID); err != nil {
		return err
	}
	// detail 里带上 job_name：删完之后靠 key 认人，名字比 id 好用。
	_ = store.Audit(ctx, control.ActorSystem, "job.delete", map[string]any{
		"job_id": job.ID, "job_key": job.JobKey, "job_name": job.Name, "engine_stopped": stopped,
	})
	fmt.Printf("已删除任务 %s。\n", job.Name)
	if stopTimeout != nil {
		// 记录已删（用户要的就是这个），但进程没确认停下 → 非 0，让脚本/CI 看得见。
		return fmt.Errorf("任务已删除，但引擎没有在等待时间内确认退出：%w", stopTimeout)
	}
	return nil
}

// releaseTargetLease 删掉这个任务在目标端的租约行（删除任务专用）。
// 目标端连不上时返回错误，由调用方如实说明「租约会在 TTL 后过期」，不静默。
func releaseTargetLease(ctx context.Context, store *control.Store, job control.Job) (holder string, alive, hasLease bool, err error) {
	conn, err := store.GetConnection(ctx, job.TargetID)
	if err != nil {
		return "", false, false, err
	}
	// 能力探测（不按 kind 硬编码）：只有 PG 目标端实现了目标端租约表。
	// MySQL 侧的 _jl_lease 还没有接入引擎的租约路径，这里跳过并如实说明——
	// 以前无条件调 ForceReleaseLease，每次删除都会打印一句假的「清租约失败」。
	if conn.Kind != dsn.KindPostgres {
		return "", false, false, nil
	}
	secret, err := store.Secret(ctx, job.TargetID)
	if err != nil {
		return "", false, true, err
	}
	doc, err := store.JobDoc(ctx, job)
	if err != nil {
		return "", false, true, err
	}
	ep := dsn.Endpoint{
		Kind: conn.Kind, Host: conn.Host, Port: conn.Port, Database: conn.Database,
		Username: conn.Username, Password: secret, Params: conn.Params(),
	}
	schema := strings.TrimSpace(doc.Target.Schema)
	if schema == "" {
		if conn.Kind == dsn.KindMySQL {
			schema = conn.Database
		} else {
			schema = "public"
		}
	}
	h, a, err := pgsink.ForceReleaseLease(ctx, ep, schema, job.JobKey)
	return h, a, true, err
}

// settleActiveRuns 等这个任务的未结束 run 自己收尾；等不到就替它收尾。
//
// 为什么要这一步：DeleteJob 现在有「有未结束 run 就拒删」的守卫，而删除路径刚刚
// 才把目标端租约删掉、那个进程要最多约 10 秒才自己发现并退出。
// 不在这里等/收尾，用户就会看到「引擎被踢了、任务却删不掉」——比原来的问题更难懂。
func settleActiveRuns(ctx context.Context, store *control.Store, jobID string, wait time.Duration) error {
	deadline := time.Now().Add(wait)
	for {
		active, err := store.ActiveRuns(ctx)
		if err != nil {
			return err
		}
		var mine []control.Run
		for _, r := range active {
			if r.JobID == jobID {
				mine = append(mine, r)
			}
		}
		if len(mine) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			for _, r := range mine {
				_, ferr := store.FinishRun(ctx, r.ID, state.Failed,
					"删除任务时强制收尾：目标端租约已删除，原进程会在续租失败后自行退出",
					1, control.FinishInterrupted)
				if ferr != nil {
					return ferr
				}
			}
			fmt.Printf("那个运行没有在 %s 内自己收尾，已由本次删除标记为 interrupted（审计里可查）。\n", wait)
			return nil
		}
		time.Sleep(300 * time.Millisecond)
	}
}

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
	// 暂停表必须看见：这是「要不要处理」的唯一信号。空清单 + 空说明 = 确实没有暂停；
	// 只有说明 = 读不到（未知），两者结论相反，所以**不合并成一句**。
	if len(st.PausedTables) > 0 {
		fmt.Printf("暂停表      %d 张（这些表的事件被跳过、其它表照写，位点照常前进）：\n", len(st.PausedTables))
		for _, p := range st.PausedTables {
			line := fmt.Sprintf("  - %s  原因码 %s", p.Table, p.Reason)
			if p.SkippedRows > 0 {
				line += fmt.Sprintf("，已跳过 %d 行", p.SkippedRows)
			}
			if p.Detail != "" {
				line += "；" + p.Detail
			}
			fmt.Println(line)
		}
		fmt.Println("          结构对齐后要整表重搬一次才会解除（清空 + 从新视图搬）；在那之前这张表不完整。")
	}
	if st.PausedNote != "" {
		fmt.Printf("暂停说明  %s\n", st.PausedNote)
	}
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
		case doctor.StatusSkipped:
			mark = "跳过" // 「跑不了/没意义」不能显示成 OK
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
		// 跳过不算不健康，但**必须在结论里说清有几项没做**：
		// 逐行写 [跳过]、页脚说「正常」，人只读页脚就会以为全查过了。
		if rep.Skipped > 0 {
			fmt.Printf("  结论      正常（%d 项跳过，见上面的 [跳过]；那几项不代表通过）\n", rep.Skipped)
		} else {
			fmt.Println("  结论      正常")
		}
		return nil
	}
	if rep.Skipped > 0 {
		fmt.Printf("  结论      有 %d 个问题、%d 个提醒（另有 %d 项跳过，见上面的 [跳过]）\n",
			rep.Problems, rep.Warnings, rep.Skipped)
	} else {
		fmt.Printf("  结论      有 %d 个问题、%d 个提醒\n", rep.Problems, rep.Warnings)
	}
	return errors.New("体检发现问题")
}
