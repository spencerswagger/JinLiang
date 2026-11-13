//go:build integration

package engine_test

// P1（Owl II 在 #128 审查里提的）：MY-08 的**收尾标注接线**没有自动化保护。
//
// 变异实测（没这条用例之前）：
//   - 删掉 `runner.preflight` 里 `r.degradeNote = preflight.EngineDegradeNote(j)`；
//   - 或者删掉 `drain()` 里那次 `r.emit(... r.degradeNote ...)`；
//   `go test ./internal/engine/` 仍然全绿——现有用例都是「手工喂一个 degradeNote，
//   再验打印函数」，从来没走过「预检 → 判定 → 收尾」这条真接线。
//
// 这条用例走真库（MySQL 源 MyISAM + MEMORY → PG），断言的是**接线**而不是打印：
//   ① `Result.Note`（CLI 的「说明」）必须带降级 —— 杀掉「删赋值」；
//   ② 事件流里「降级」出现两次（预检 MY-08 + 收尾）—— 杀掉「删 emit」的第一次；
//   ③ 其中一次必须排在「全量完成：…」**之后** —— 只可能来自 drain()，
//      杀掉「删 emit」（删了就只剩预检那一次，且在「全量完成」之前）。
//
// CI：b4-mysql-source 用 `-run 'TestMySQLSource'` 跑本包，名字前缀保证被选中；
// b1-engine 用 `-skip 'TestMySQLSource'` 排除（那个 job 没有 MySQL 源库）。

import (
	"context"
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/engine"
)

// 全量完成那一帧的前缀（snapshot() 里的原话）：降级标注必须排在它之后。
const fullCopyDonePrefix = "全量完成："

func TestMySQLSourceMY08DegradeAnnotatedInStreamAndResult(t *testing.T) {
	srcDSN, dstDSN := mysqlDSNs(t)
	my := mustMySQL(t, srcDSN)
	dst := mustConn(t, dstDSN)

	const (
		myisam = "engine_mysql_my08_myisam"
		memory = "engine_mysql_my08_memory"
	)
	myExec(t, my, "DROP TABLE IF EXISTS "+myisam)
	myExec(t, my, "DROP TABLE IF EXISTS "+memory)
	myExec(t, my, "CREATE TABLE "+myisam+" (id BIGINT NOT NULL PRIMARY KEY, payload VARCHAR(32)) ENGINE=MyISAM")
	myExec(t, my, "CREATE TABLE "+memory+" (id BIGINT NOT NULL PRIMARY KEY, payload VARCHAR(32)) ENGINE=MEMORY")
	myExec(t, my, "INSERT INTO "+myisam+" VALUES (1,'a'),(2,'b')")
	myExec(t, my, "INSERT INTO "+memory+" VALUES (1,'m')")
	t.Cleanup(func() {
		myExec(t, my, "DROP TABLE IF EXISTS "+myisam)
		myExec(t, my, "DROP TABLE IF EXISTS "+memory)
	})

	for _, tbl := range []string{myisam, memory} {
		exec(t, dst, "DROP TABLE IF EXISTS "+tbl)
	}
	exec(t, dst, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance")

	store, jobID := newMySQLJobTables(t, srcDSN, dstDSN, "mysql-my08", "once", myisam, memory)
	ensureStateTables(t, store, dstDSN, jobID)

	res, err := engine.Run(context.Background(), engine.Options{Store: store, JobID: jobID})
	if err != nil {
		t.Fatalf("降级路径必须照跑（不是拒绝）：%v", err)
	}
	if res.ExitCode != engine.ExitOK {
		t.Fatalf("降级不改变退出码：期望 %d，实际 %d（note=%q）", engine.ExitOK, res.ExitCode, res.Note)
	}
	if res.RowsCopied != 3 {
		t.Fatalf("两张表共 3 行应搬完（降级不是「不搬」），实际 %d", res.RowsCopied)
	}

	// ① Result.Note：删掉 runner.preflight 里的 degradeNote 赋值，这里立刻红。
	if !strings.Contains(res.Note, "降级") {
		t.Fatalf("Result.Note（CLI 的「说明」）必须带降级标注——否则收尾又是假绿：%q", res.Note)
	}
	for _, want := range []string{myisam + "(MyISAM)", memory + "(MEMORY)"} {
		if !strings.Contains(res.Note, want) {
			t.Errorf("Result.Note 要点名 %s（真实引擎名）：%q", want, res.Note)
		}
	}

	// ②③ 事件流：预检一次 + 收尾一次，且收尾那次在「全量完成」之后。
	evs := store.Bus().Since(jobID, 0)
	if len(evs) == 0 {
		t.Fatal("事件流为空：读不到就没法判「预检说了、收尾再说一次」")
	}
	idxFullDone := -1
	var degradeAt []int
	for i, ev := range evs {
		if ev.Type != "note" {
			continue
		}
		if strings.HasPrefix(ev.Message, fullCopyDonePrefix) && idxFullDone < 0 {
			idxFullDone = i
		}
		if strings.Contains(ev.Message, "降级") {
			degradeAt = append(degradeAt, i)
		}
	}
	if idxFullDone < 0 {
		t.Fatalf("事件流里没有「%s」那一帧：%d 帧分别是 %s", fullCopyDonePrefix, len(evs), eventDigest(evs))
	}
	if len(degradeAt) < 2 {
		// 删掉 drain 里那次 emit 之后，只剩预检 MY-08 那一次。
		t.Fatalf("降级标注应出现两次（预检 MY-08 + 收尾），实际 %d 次（帧序：%s）",
			len(degradeAt), eventDigest(evs))
	}
	afterDone := false
	for _, i := range degradeAt {
		if i > idxFullDone {
			afterDone = true
		}
	}
	if !afterDone {
		// 只可能来自 drain()：删掉那次 emit，这一条就红。
		t.Fatalf("「全量完成」之后必须还有一次降级标注（drain 里那次），实际帧序：%s", eventDigest(evs))
	}
}

// newMySQLJobTables 与 newMySQLJob 同款，但 content.tables 收多张表
// （MY-08 要一次点名多个引擎，单表版不够用）。
func newMySQLJobTables(t *testing.T, srcDSN, dstDSN, jobName, freq string, tables ...string) (*control.Store, string) {
	t.Helper()
	dir := t.TempDir()
	store, err := control.Open(control.Options{DataDir: dir, Autogen: true})
	if err != nil {
		t.Fatalf("打开控制库失败: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	ctx := context.Background()

	my := myConfigOf(t, srcDSN)
	sc, err := store.CreateConnection(ctx, control.ConnectionInput{
		Name: "src", Kind: "mysql", Host: my.host, Port: my.port,
		Database: my.db, Username: my.user, Password: my.pass,
	})
	if err != nil {
		t.Fatalf("建 MySQL 源连接失败: %v", err)
	}
	tc, err := store.CreateConnection(ctx, control.ConnectionInput{
		Name: "dst", Kind: "postgres", Host: "127.0.0.1", Port: portOf(t, dstDSN),
		Database: dbOf(t, dstDSN), Username: userOf(t, dstDSN), Password: passOf(t, dstDSN),
		Params: map[string]string{"sslmode": "disable"},
	})
	if err != nil {
		t.Fatalf("建 PG 目标连接失败: %v", err)
	}
	content := map[string]control.TableDoc{}
	for _, tbl := range tables {
		content[tbl] = control.TableDoc{}
	}
	job, err := store.CreateJob(ctx, control.JobInput{
		Name: jobName, SourceID: sc.ID, TargetID: tc.ID,
		Spec: control.JobDoc{
			Version: 1, Job: jobName,
			Source:  control.SourceDoc{Kind: "mysql", Scope: &control.ScopeDoc{Schema: my.db}},
			Target:  control.TargetDoc{Kind: "postgres", Schema: "public", OnExists: "append"},
			Mode:    control.ModeDoc{Frequency: freq},
			Content: &control.ContentDoc{Tables: content},
		},
	})
	if err != nil {
		t.Fatalf("建任务失败: %v", err)
	}
	return store, job.ID
}

// eventDigest 把事件流压成「类型:消息前缀」一行，失败信息里能看清顺序（不出全文，太长）。
func eventDigest(evs []control.Event) string {
	var b strings.Builder
	for i, ev := range evs {
		if i > 0 {
			b.WriteString(" → ")
		}
		msg := ev.Message
		if r := []rune(msg); len(r) > 18 {
			msg = string(r[:18]) + "…"
		}
		b.WriteString(ev.Type)
		b.WriteString(":")
		b.WriteString(msg)
	}
	return b.String()
}
