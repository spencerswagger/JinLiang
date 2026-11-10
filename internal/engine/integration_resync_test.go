//go:build integration

package engine_test

// `--resync` 的验收：真库上「位点被弄坏 → E3001 → --resync → 恢复并追平」。
//
// 为什么要在真库上跑：这条能力的全部风险都在数据库那边——清表顺序、书签重建、
// 位点与数据同事务、以及「清空之后真的把源端当前状态搬回来了」。假实现（只改
// 一个 bool）在单测里也能绿，只有真库能证伪。
//
// 破坏方式选的是**真实现场里会出现的那一种**：同步跑着的时候书签被删掉
//（DBA 手滑 / 清理脚本 / max_slot_wal_keep_size）。此时 `_jl_meta` 里仍有位点，
// 但 StartReplication 找不到书签 → PostgreSQL 报 "replication slot ... does not exist"
// → 引擎按位点专属信号判 E3001（不是我们另造的错误码）。
//
// 缺 DSN 时**直接失败**而不是跳过（与同目录其它集成用例一致：跳过＝静默通过）。

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/engine"
	"github.com/spencerswagger/jinliang/internal/state"
)

func TestResyncRebuildsBrokenBookmarkAndCatchesUp(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	srcDB := mustConn(t, srcDSN)
	dstDB := mustConn(t, dstDSN)
	ctx := context.Background()
	const table = "resync_orders"
	const rows = 120000

	// 数据量要够大：破坏动作必须落在「书签已建好、还没开始流」的快照窗口里，
	// 太小的话一拍就过去了（那时用例会红着告诉你「没测到」，而不是假绿）。
	exec(t, srcDB, "DROP TABLE IF EXISTS "+table)
	exec(t, srcDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, payload text NOT NULL)")
	exec(t, srcDB, fmt.Sprintf("INSERT INTO %s SELECT g, 'r'||g FROM generate_series(1,%d) g", table, rows))
	exec(t, dstDB, "DROP TABLE IF EXISTS "+table)
	exec(t, dstDB, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")

	store, jobID := resyncJob(t, srcDSN, dstDSN, "resync-e2e", table, 10000)
	job, err := store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	slot, err := state.ObjectName("slot", job.JobKey, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		// 书签属于**源库**，测完必须收走：留着会一直攒 WAL，还会污染下一个用例。
		_, _ = srcDB.Exec(context.Background(), `SELECT pg_drop_replication_slot($1) WHERE EXISTS
			(SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)`, slot)
	})

	events := collectEvents(store, jobID)

	// ── 第一轮：正常开跑，快照窗口里把书签删掉 → 必须红着停下，且是 E3001。
	stopKill := make(chan struct{})
	killerDone := make(chan bool, 1)
	go killSlotWhileCopying(t, srcDB, slot, stopKill, killerDone)

	res1, err1 := engine.Run(ctx, engine.Options{
		Store: store, JobID: jobID, Actor: control.ActorCLI,
		IdleAfter: 3 * time.Second, // 破坏失败时这一轮会正常跑完 → 下面按「没测到」处理
	})
	close(stopKill)
	// **必须等破坏 goroutine 真的退出**再开第二轮：它最后一拍可能删掉第二轮刚建的书签，
	// 那样第二轮会以「can no longer get changes from replication slot」失败——用例
	// 变成在测自己制造的竞态（本地已经踩过一次）。
	if !<-killerDone {
		t.Fatalf("没能在快照窗口里删掉书签：用例没测到 E3001（引擎错误：%v）", err1)
	}
	if err1 == nil {
		t.Fatalf("书签被删掉之后这一轮必须失败，却成功收尾了（阶段 %s）", res1.Phase)
	}
	if ev := events.wait(5*time.Second, func(e control.Event) bool {
		return e.Type == "error" && e.Code == "E3001"
	}); ev == nil {
		t.Fatalf("书签被删掉应报 E3001，事件流里没有；引擎返回的错误：%v", err1)
	}
	t.Logf("第一轮按预期失败（E3001）：%v", err1)

	// 目标表此刻是半截数据：这正是「必须整轮重搬」的现场。
	var half int
	if err := dstDB.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&half); err != nil {
		t.Fatalf("读目标端行数失败（快照阶段应该已经建好表）：%v", err)
	}
	t.Logf("破坏后：目标端 %d 行 / 源端 %d 行（半截数据）", half, rows)

	// ── 源端继续变化：新增 10 行、删掉 5 行、改 1 行。
	// 「--resync 之后目标端等于源端当前状态」只有这张表被清空重建才可能成立
	//（否则源端删掉的那 5 行会永远留在目标端）。
	exec(t, srcDB, fmt.Sprintf("INSERT INTO %s SELECT g, 'r'||g FROM generate_series(%d,%d) g", table, rows+1, rows+10))
	exec(t, srcDB, fmt.Sprintf("DELETE FROM %s WHERE id IN (1,2,3,4,5)", table))
	exec(t, srcDB, fmt.Sprintf("UPDATE %s SET payload='updated' WHERE id=6", table))

	// ── 第二轮：--resync。必须成功、必须整轮重搬、必须追平。
	t0 := time.Now()
	res2, err2 := engine.Run(ctx, engine.Options{
		Store: store, JobID: jobID, Actor: control.ActorCLI, Resync: true,
		IdleAfter: 2 * time.Second,
	})
	if err2 != nil {
		// 失败时把诊断打出来：位点与书签是这条能力的全部依据。
		t.Logf("_jl_meta: phase=%q position=%q", metaScalar(t, dstDB, "phase", job.JobKey), metaScalar(t, dstDB, "position", job.JobKey))
		var restart, confirmed, status string
		var active bool
		if qerr := srcDB.QueryRow(ctx, `SELECT COALESCE(restart_lsn::text,''), COALESCE(confirmed_flush_lsn::text,''),
			COALESCE(wal_status,''), active FROM pg_replication_slots WHERE slot_name=$1`, slot).
			Scan(&restart, &confirmed, &status, &active); qerr != nil {
			t.Logf("读书签 %s 状态失败：%v", slot, qerr)
		} else {
			t.Logf("书签 %s: restart=%s confirmed=%s status=%s active=%v", slot, restart, confirmed, status, active)
		}
		t.Fatalf("--resync 应该能自己修好这一轮，得到错误：%v（阶段 %s）", err2, res2.Phase)
	}
	if res2.ExitCode != engine.ExitOK {
		t.Fatalf("--resync 这一轮应成功，退出码 %d", res2.ExitCode)
	}
	if res2.Resync == nil {
		t.Fatal("结果里没有 ResyncReport：CLI 就没法告诉用户「清了哪些表、旧位点是什么」")
	}
	if !containsStr(res2.Resync.Tables, table) {
		t.Fatalf("清表名单里应有 %s，得到 %v", table, res2.Resync.Tables)
	}
	if res2.Resync.OldPosition == "" {
		t.Fatal("报告里要留住旧位点（审计要回答「从哪重建的」）")
	}
	if res2.RowsCopied < rows {
		t.Fatalf("整轮重搬应该搬回 ≥%d 行，只搬了 %d 行", rows, res2.RowsCopied)
	}

	// **禁止把它实现成「删任务再重建」**：身份必须原样（id 与 job_key 都不变）。
	// 换了 job_key 就是另一个任务——审计对不上、历史断掉，用户以为在修问题，
	// 实际换了个任务；这条断言让任何「删了重建」的实现直接红。
	after, err := store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatalf("--resync 之后任务应该还在（不是删了重建）：%v", err)
	}
	if after.ID != job.ID || after.JobKey != job.JobKey {
		t.Fatalf("--resync 不许换任务身份：id %s → %s，job_key %s → %s",
			job.ID, after.ID, job.JobKey, after.JobKey)
	}

	// 数据到齐：行数、指纹、以及「源端删掉的行不许留在目标端」。
	srcRows, srcFP := tableShape(t, srcDB, table)
	dstRows, dstFP := tableShape(t, dstDB, table)
	t.Logf("源端 %d 行 fp=%s；目标端 %d 行 fp=%s", srcRows, srcFP, dstRows, dstFP)
	if srcRows != dstRows || srcFP != dstFP {
		t.Fatalf("整轮重搬后两边不一致：源 %d/%s，目标 %d/%s", srcRows, srcFP, dstRows, dstFP)
	}
	if srcRows != rows+5 { // 120000 + 10 - 5
		t.Fatalf("源端应有 %d 行（用例前提变了？），得到 %d", rows+5, srcRows)
	}
	var leftovers int
	if err := dstDB.QueryRow(ctx, fmt.Sprintf("SELECT count(*) FROM %s WHERE id <= 5", table)).Scan(&leftovers); err != nil {
		t.Fatal(err)
	}
	if leftovers != 0 {
		t.Fatalf("源端已删的 5 行还留在目标端（%d 行）：说明这一轮没有清空重建", leftovers)
	}

	// 追平时延：从 --resync 开跑到进入 catchup（存量已搬完，开始追这段变更）。
	// 注意 since=t0：事件流里还留着**第一轮**的 catchup（它是在建书签之后才失败的）。
	// 只看 --resync 开跑之后的事件，否则会把上一轮的进度当成这一轮的。
	caughtUp := events.waitAfter(t0, 30*time.Second, func(e control.Event) bool {
		return e.Type == "phase" && e.Phase == state.Catchup
	})
	if caughtUp == nil {
		events.mu.Lock()
		var dump []string
		for _, e := range events.all {
			dump = append(dump, fmt.Sprintf("%s phase=%q %s", e.Type, e.Phase, e.Message))
		}
		events.mu.Unlock()
		t.Fatalf("--resync 这一轮没有进入 catchup（存量搬完），事件流里没有 phase=catchup；收到 %d 条：\n%s",
			len(dump), strings.Join(dump, "\n"))
	}
	doneEv := events.waitAfter(t0, 30*time.Second, func(e control.Event) bool {
		return e.Type == "done" || (e.Type == "phase" && e.Phase == state.Done)
	})
	doneAt := time.Now()
	if doneEv != nil {
		doneAt = doneEv.At
	}
	t.Logf("存量重搬完成 %s（--resync 开跑 → 120005 行搬完、进入追增量）；"+
		"整轮收尾 %s（含 2s 静默窗口）；引擎自报用时 %s",
		caughtUp.At.Sub(t0).Truncate(time.Millisecond),
		doneAt.Sub(t0).Truncate(time.Millisecond),
		res2.Elapsed.Truncate(time.Millisecond))

	// 留痕分两段，各说事实：
	//   job.resync         —— 谁触发、哪个书签、旧位点、**计划**清哪些表（planned_tables）；
	//   job.resync.cleared —— **实际**清了哪些表（清完才写）。
	intent, err := store.ListAuditByAction(ctx, "job.resync", 10)
	if err != nil {
		t.Fatalf("读审计失败：%v", err)
	}
	if len(intent) != 1 {
		t.Fatalf("应有一条 job.resync（意图）审计，得到 %d 条", len(intent))
	}
	if intent[0].Actor != control.ActorCLI {
		t.Fatalf("审计的 actor 应是 %q（谁触发的），得到 %q", control.ActorCLI, intent[0].Actor)
	}
	for _, want := range []string{slot, table, "postgres:", "planned_tables"} {
		if !strings.Contains(intent[0].Detail, want) {
			t.Fatalf("意图审计 detail 里缺 %q（哪个书签、计划清哪些表、旧位点都要有）：%s", want, intent[0].Detail)
		}
	}
	cleared, err := store.ListAuditByAction(ctx, "job.resync.cleared", 10)
	if err != nil {
		t.Fatalf("读审计失败：%v", err)
	}
	if len(cleared) != 1 {
		t.Fatalf("应有一条 job.resync.cleared（实清）审计，得到 %d 条", len(cleared))
	}
	for _, want := range []string{slot, table, "cleared_tables"} {
		if !strings.Contains(cleared[0].Detail, want) {
			t.Fatalf("实清审计 detail 里缺 %q：%s", want, cleared[0].Detail)
		}
	}
}

// killSlotWhileCopying 每 20ms 看一眼书签，一出现就删掉它。
//
// 为什么用轮询而不是「在某个钩子里删」：我们要复现的是**外部**把一个正在用的书签
// 删掉（DBA/清理脚本），让它从外面发生才作数。整个快照期间会反复尝试：
// 删成功一次就够，之后删不到（书签已不存在）也无所谓。
func killSlotWhileCopying(t *testing.T, conn *pgx.Conn, slot string, stop <-chan struct{}, done chan<- bool) {
	tick := time.NewTicker(2 * time.Millisecond)
	defer tick.Stop()
	reported := false
	// 无论从哪条路返回，都必须告诉调用方「我已经不再删了」。
	defer func() {
		select {
		case done <- reported:
		default:
		}
	}()
	for {
		select {
		case <-stop:
			return
		case <-tick.C:
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			// 不带 active=false：快照期间书签可能仍被导出会话「占着」，这时删会报
			// "is active for PID"（无害，继续重试）；一旦导出会话结束（EndSnapshot），
			// 下一次尝试就会成功——这个窗口只有几毫秒，所以间隔取 2ms。
			rows, err := conn.Query(ctx,
				`SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots
				 WHERE slot_name=$1`, slot)
			if err == nil {
				for rows.Next() {
					reported = true
				}
				rows.Close()
			} else if !reported {
				t.Logf("删书签尝试失败（继续重试）：%v", err)
			}
			cancel()
			if reported {
				// 已经删掉了：不再重试（后面 Prepare 不会再建，直到下一轮 --resync）。
				return
			}
		}
	}
}

// resyncJob 建一条 continuous+initial 的任务，并把块切小：破坏动作要落在快照窗口里。
func resyncJob(t *testing.T, srcDSN, dstDSN, name, table string, chunkRows int) (*control.Store, string) {
	t.Helper()
	return resyncJobMode(t, srcDSN, dstDSN, name, table, "continuous+initial", chunkRows)
}

func resyncJobMode(t *testing.T, srcDSN, dstDSN, name, table, frequency string, chunkRows ...int) (*control.Store, string) {
	t.Helper()
	ctx := context.Background()
	store, err := control.Open(control.Options{DataDir: t.TempDir(), Autogen: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	mk := func(nm string, dsn string) string {
		c, err := store.CreateConnection(ctx, control.ConnectionInput{
			Name: nm, Kind: "postgres", Host: "127.0.0.1", Port: portOf(t, dsn),
			Database: dbOf(t, dsn), Username: userOf(t, dsn), Password: passOf(t, dsn),
			Params: map[string]string{"sslmode": "disable"},
		})
		if err != nil {
			t.Fatal(err)
		}
		return c.ID
	}
	srcID := mk("src", srcDSN)
	dstID := mk("dst", dstDSN)
	snap := &control.SnapshotDoc{}
	if len(chunkRows) > 0 {
		n := chunkRows[0]
		snap.ChunkRows = &n
	}
	job, err := store.CreateJob(ctx, control.JobInput{
		Name: name, SourceID: srcID, TargetID: dstID,
		Spec: control.JobDoc{
			Version: 1, Job: name,
			Source: control.SourceDoc{
				Kind: "postgres", Scope: &control.ScopeDoc{Schema: "public"},
				Snapshot: snap,
			},
			Target:  control.TargetDoc{Kind: "postgres", Schema: "public", OnExists: "append"},
			Mode:    control.ModeDoc{Frequency: frequency},
			Content: &control.ContentDoc{Tables: map[string]control.TableDoc{table: {}}},
		},
	})
	if err != nil {
		t.Fatalf("建任务失败: %v", err)
	}
	return store, job.ID
}

// tableShape 是「行数 + 内容指纹」：光比行数看不出改行丢没丢。
func tableShape(t *testing.T, conn *pgx.Conn, table string) (int64, string) {
	t.Helper()
	var n int64
	var fp string
	q := fmt.Sprintf(`SELECT count(*), COALESCE(md5(string_agg(id::text||':'||payload, ',' ORDER BY id)), '') FROM %s`, table)
	if err := conn.QueryRow(context.Background(), q).Scan(&n, &fp); err != nil {
		t.Fatalf("算 %s 的行数与指纹失败: %v", table, err)
	}
	return n, fp
}

// eventLog 收集事件流（带到达时间），供「必须看到某个事件」的判据用。
type eventLog struct {
	mu  sync.Mutex
	all []stampedEvent
}

type stampedEvent struct {
	control.Event
	At time.Time
}

func collectEvents(store *control.Store, jobID string) *eventLog {
	log := &eventLog{}
	ch, cancel := store.Bus().Subscribe(jobID)
	go func() {
		defer cancel()
		for ev := range ch {
			log.mu.Lock()
			log.all = append(log.all, stampedEvent{Event: ev, At: time.Now()})
			log.mu.Unlock()
		}
	}()
	return log
}

// wait 等到第一条满足条件的事件，超时返回 nil。
func (l *eventLog) wait(d time.Duration, match func(control.Event) bool) *stampedEvent {
	return l.waitAfter(time.Time{}, d, match)
}

// waitAfter 只认 since 之后到达的事件（同一个日志会横跨好几轮运行）。
func (l *eventLog) waitAfter(since time.Time, d time.Duration, match func(control.Event) bool) *stampedEvent {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		l.mu.Lock()
		snapshot := append([]stampedEvent(nil), l.all...)
		l.mu.Unlock()
		for i := range snapshot {
			if snapshot[i].At.Before(since) {
				continue
			}
			if match(snapshot[i].Event) {
				return &snapshot[i]
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nil
}

func containsStr(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

// continuous（只接增量）的 --resync：没有存量可搬，动作只有「重建书签、从源端当前位置重新开始」。
//
// 这条用例守两件事：
//  1. 书签追不回位点时，不给 --resync 必须**停下**（`unreplayableSlotError`），
//     而不是默默把位点往前挪——那是最坏的一种「看起来成功」；
//  2. 给了 --resync 就必须真的换一个新位点，并且**一张表都不清**
//     （continuous 没有存量；清表就是删数据）。
func TestResyncContinuousReanchorsBookmarkOnly(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	srcDB := mustConn(t, srcDSN)
	dstDB := mustConn(t, dstDSN)
	ctx := context.Background()
	const table = "resync_cont"

	exec(t, srcDB, "DROP TABLE IF EXISTS "+table)
	exec(t, srcDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, payload text NOT NULL)")
	exec(t, srcDB, "INSERT INTO "+table+" SELECT g, 'r'||g FROM generate_series(1,50) g")
	exec(t, dstDB, "DROP TABLE IF EXISTS "+table)
	exec(t, dstDB, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")

	store, jobID := resyncJobMode(t, srcDSN, dstDSN, "resync-cont", table, "continuous")
	job, err := store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	slot, err := state.ObjectName("slot", job.JobKey, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = srcDB.Exec(context.Background(), `SELECT pg_drop_replication_slot($1) WHERE EXISTS
			(SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)`, slot)
	})

	// 第一轮：只接增量，很快追平并收尾。
	if _, err := engine.Run(ctx, engine.Options{
		Store: store, JobID: jobID, Actor: control.ActorCLI, IdleAfter: 1 * time.Second,
	}); err != nil {
		t.Fatalf("第一轮应该能跑起来：%v", err)
	}
	var oldPos string
	if err := dstDB.QueryRow(ctx, "SELECT COALESCE(position,'') FROM _jl_meta WHERE job_key=$1", job.JobKey).Scan(&oldPos); err != nil {
		t.Fatal(err)
	}
	if oldPos == "" || !strings.HasPrefix(oldPos, "postgres:") {
		t.Fatalf("第一轮之后应有一个 PG 位点，得到 %q", oldPos)
	}

	// 破坏：删书签 + 把位点改成一个老位置。这一步模拟「变更流水被清理」的现场。
	exec(t, srcDB, "SELECT pg_drop_replication_slot('"+slot+"')")
	exec(t, dstDB, "UPDATE _jl_meta SET position='postgres:v1:0/1' WHERE job_key=$1", job.JobKey)

	// --resync：continuous 只接增量，所以动作只有「删旧书签 → 按源端当前位置建新的」。
	// 不做这一步的话 Prepare 会复用旧书签、位点根本不会动（审计说清了、其实没清）。
	res, err := engine.Run(ctx, engine.Options{
		Store: store, JobID: jobID, Actor: control.ActorCLI, Resync: true, IdleAfter: 1 * time.Second,
	})
	if err != nil {
		t.Fatalf("--resync 应该能把位点重新锚到源端当前位置，得到：%v", err)
	}
	if res.ExitCode != engine.ExitOK {
		t.Fatalf("--resync 这一轮应成功，退出码 %d", res.ExitCode)
	}
	if res.Resync == nil || len(res.Resync.Tables) != 0 {
		t.Fatalf("continuous 的 --resync 不许清任何表，得到 %+v", res.Resync)
	}
	if res.Resync.Slot != slot {
		t.Fatalf("报告里的书签名应是 %q，得到 %q", slot, res.Resync.Slot)
	}
	var newPos string
	if err := dstDB.QueryRow(ctx, "SELECT COALESCE(position,'') FROM _jl_meta WHERE job_key=$1", job.JobKey).Scan(&newPos); err != nil {
		t.Fatal(err)
	}
	if newPos == "" || newPos == "postgres:v1:0/1" || newPos == oldPos {
		t.Fatalf("--resync 之后位点必须是源端当前位置（既不是坏位点也不是旧位点），得到 %q（旧 %q）", newPos, oldPos)
	}
	t.Logf("continuous：位点 %q → %q（书签已重建，未清任何表）", oldPos, newPos)

	// 目标表一行都不该有：continuous 不搬存量，--resync 也不许顺手搬/清。
	var n int64
	if err := dstDB.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("目标表应该被建出来（EnsureSchema），读不到：%v", err)
	}
	if n != 0 {
		t.Fatalf("continuous 不该搬存量，目标端却有 %d 行", n)
	}
}

// Owl II 必改 1 的判据：`--resync` 必须**先问「这张表能不能清」，再动位点与书签**。
//
// 复现路径（`on_exists: append` + 目标表是用户自己的 + 上次全量已搬完）：
// 旧实现里 `ClearProgress` + `Prepare(Snapshot)` 先把位点和旧书签毁掉，直到
// `snapshot() → cleanBeforeRecopy → recopyAction` 才因为「不是本任务建的表」停下。
// 留下的是「位点没了 + 旧书签没了 + 表一行没清」——用户再跑一次会被判 `copySkip`
// 直接进流，(旧位点, 新起点] 永久跳过且不报错。
//
// 判据：失败发生在清位点之前 —— position 与书签 confirmed_flush_lsn **逐字节相等**，
// 且这条失败是响亮的（runs 里 last_error 点名表），现场可直接重试。
func TestResyncRefusesUserTableBeforeTouchingPosition(t *testing.T) {
	srcDSN, dstDSN := dsns(t)
	srcDB := mustConn(t, srcDSN)
	dstDB := mustConn(t, dstDSN)
	ctx := context.Background()
	const table = "resync_user_table"

	exec(t, srcDB, "DROP TABLE IF EXISTS "+table)
	exec(t, srcDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, payload text NOT NULL)")
	exec(t, srcDB, fmt.Sprintf("INSERT INTO %s SELECT g, 'r'||g FROM generate_series(1,2000) g", table))
	exec(t, dstDB, "DROP TABLE IF EXISTS "+table)
	exec(t, dstDB, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")
	// **用户自己建的表**（不是本任务建的）：append 模式会复用它，`_jl_object` 里没有登记。
	exec(t, dstDB, "CREATE TABLE "+table+" (id bigint PRIMARY KEY, payload text NOT NULL)")

	store, jobID := resyncJob(t, srcDSN, dstDSN, "resync-user-table", table, 1000)
	job, err := store.GetJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	slot, err := state.ObjectName("slot", job.JobKey, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = srcDB.Exec(context.Background(), `SELECT pg_drop_replication_slot($1) WHERE EXISTS
			(SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)`, slot)
	})

	// 先正常搬一轮：块标记 Done 齐（所以 --resync 的 CleanNames 里有这张表）。
	if _, err := engine.Run(ctx, engine.Options{
		Store: store, JobID: jobID, Actor: control.ActorCLI, IdleAfter: 1 * time.Second,
	}); err != nil {
		t.Fatalf("先正常搬一轮应该成功：%v", err)
	}
	if ours, err := tableOwnedByJob(ctx, store, jobID, dstDB, table); err != nil || ours {
		t.Fatalf("前提：目标表必须是用户自己的（_jl_object 无登记），得到 ours=%v err=%v", ours, err)
	}

	posBefore := metaScalar(t, dstDB, "position", job.JobKey)
	phaseBefore := metaScalar(t, dstDB, "phase", job.JobKey)
	slotBefore := slotConfirmed(t, srcDB, slot)
	if posBefore == "" || slotBefore == "" {
		t.Fatalf("前提：应已有位点与书签 confirmed（pos=%q slot=%q）", posBefore, slotBefore)
	}

	// --resync 必须在这里停下，且**位点与书签都没动**。
	_, errResync := engine.Run(ctx, engine.Options{
		Store: store, JobID: jobID, Actor: control.ActorCLI, Resync: true, IdleAfter: 1 * time.Second,
	})
	if errResync == nil {
		t.Fatal("目标表是用户自己的，--resync 必须停下（不能先毁位点再问）")
	}
	if !strings.Contains(errResync.Error(), table) {
		t.Fatalf("失败文案要点名这张表，得到：%v", errResync)
	}

	if got := metaScalar(t, dstDB, "position", job.JobKey); got != posBefore {
		t.Fatalf("失败发生在清位点之前，position 必须逐字节不变：%q → %q", posBefore, got)
	}
	if got := metaScalar(t, dstDB, "phase", job.JobKey); got != phaseBefore {
		t.Fatalf("失败发生在改阶段之前，phase 必须不变：%q → %q", phaseBefore, got)
	}
	if got := slotConfirmed(t, srcDB, slot); got != slotBefore {
		t.Fatalf("旧书签必须原样（没被 drop/重建）：confirmed %q → %q", slotBefore, got)
	}
	// 目标表一行都不能少（没被清过）。
	var n int64
	if err := dstDB.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2000 {
		t.Fatalf("表不该被动过：应仍是 2000 行，得到 %d", n)
	}

	// 失败必须落在 runs 里（历史页要看得到），并点名表。
	runs, err := store.ListRuns(ctx, jobID, 5)
	if err != nil {
		t.Fatalf("读运行历史失败：%v", err)
	}
	if len(runs) == 0 {
		t.Fatal("应有一条失败的 run")
	}
	last := runs[0]
	if last.EndedAt == "" {
		t.Fatalf("这条 run 应该已经收尾：%+v", last)
	}
	if !strings.Contains(last.LastError, table) {
		t.Fatalf("runs.last_error 要点名这张表，得到 %q", last.LastError)
	}

	// 现场可以直接重试：位点还在，普通 run 必须能接着跑（不会把一段变更静默跳过）。
	if _, err := engine.Run(ctx, engine.Options{
		Store: store, JobID: jobID, Actor: control.ActorCLI, IdleAfter: 1 * time.Second,
	}); err != nil {
		t.Fatalf("失败之后现场应可重试（位点与书签都还在），得到：%v", err)
	}
	t.Logf("append + 用户表：--resync 在清位点前停下，position %q 与书签 %q 原样", posBefore, slotBefore)
}

// tableOwnedByJob 查目标表是不是本任务建的（读 _jl_object，与 DidCreateTable 同源）。
func tableOwnedByJob(ctx context.Context, store *control.Store, jobID string,
	dstDB *pgx.Conn, table string) (bool, error) {
	job, err := store.GetJob(ctx, jobID)
	if err != nil {
		return false, err
	}
	var n int
	err = dstDB.QueryRow(ctx,
		`SELECT count(*) FROM _jl_object WHERE object_name=$1 AND job_key=$2 AND dropped_at IS NULL`,
		"table:public."+table, job.JobKey).Scan(&n)
	return n > 0, err
}

// metaScalar 读 _jl_meta 某一列（position/phase），空值返回 ""。
func metaScalar(t *testing.T, conn *pgx.Conn, column, jobKey string) string {
	t.Helper()
	var v *string
	q := fmt.Sprintf(`SELECT %s::text FROM _jl_meta WHERE job_key=$1`, column)
	if err := conn.QueryRow(context.Background(), q, jobKey).Scan(&v); err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return ""
		}
		t.Fatalf("读 _jl_meta.%s 失败: %v", column, err)
	}
	if v == nil {
		return ""
	}
	return *v
}

// slotConfirmed 读源端书签的 confirmed_flush_lsn（缺失/空返回 ""）。
func slotConfirmed(t *testing.T, conn *pgx.Conn, slot string) string {
	t.Helper()
	var v *string
	if err := conn.QueryRow(context.Background(),
		`SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=$1`, slot).Scan(&v); err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return ""
		}
		t.Fatalf("读书签 %s 失败: %v", slot, err)
	}
	if v == nil {
		return ""
	}
	return *v
}
