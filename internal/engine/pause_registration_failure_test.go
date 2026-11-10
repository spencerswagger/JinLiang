//go:build integration

package engine

// 「暂停登记落库失败 ⇒ 整批失败」——`pauseSink.Apply` 里那条 `return rerr` 的看门用例。
//
// 为什么需要它：裁决 A（只停该表 + 位点前进）落地后，原集成用例改成断言「只登记该表 /
// 位点前进 / 对齐后整表重搬」，**「登记失败」这条分支没人盯**。把
// `engine.registerPause` 置空（不写暂停记录）后，原用例仍然绿——门禁的
// `register-pause-neutered` 注入因此抓不到，等于这条纪律没有守卫。
//
// 契约（`internal/engine/pause_skip.go` 开头第 1 条红线）：
//   - 登记先行：先让 `PauseTable` 落库成功，才允许这一批跳过该表；
//   - 登记失败 = **整批失败**（位点不动、目标端 0 行）。
//     「跳过了但没记下」是永久静默缺失：用户看不到哪张表坏了，数据也真的少了。
//
// 为什么直接测 `pauseSink.Apply` 而不是跑整轮：
//   - prepare 阶段的结构核对是**另一条路径**（`prepare` 里直接调 `sink.PauseTable`），
//     不经过 `registerPause`；整轮跑很容易被它抢走，用例就变成了「测 prepare」。
//   - 这里用一个「Apply 永远返回 TypeChangedError」的假目标端，把写路径单独拎出来，
//     时间上完全确定（不靠 sleep 猜有没有进 streaming）。
//
// 怎么造「登记落库失败」：目标端 `_jl_meta` 上挂触发器——`paused_tables` 被更新就 RAISE。
// 这不是改引擎，是在目标端模拟真实故障（列被锁、库只读、磁盘满都会让那次 UPDATE 失败）。
//
// 断言（任一不成立就红）：
//   1. Apply 返回错误，且带着 `registerPause` 的包裹语（「另外：登记这张表的暂停状态也失败了」）；
//      **置空 registerPause 时这句不会出现**——这是与「重试 N 次后放弃」的分水岭；
//   2. 假目标端只被调用 **1 次**：登记失败要立刻整批失败，不许原地重试（有界重试那条红线）；
//      置空 registerPause 时会重试到 maxPausePerBatch（9 次），这里必红；
//   3. `_jl_meta.paused_tables` 里没有半条记录（登记失败＝没登记）。

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/dsn"
	"github.com/spencerswagger/jinliang/internal/pgsink"
	"github.com/spencerswagger/jinliang/internal/source"
)

// failingApplier 永远返回给定的错误，并记下被调用了几次。
type failingApplier struct {
	calls int
	err   error
}

func (f *failingApplier) Apply(ctx context.Context, evs []source.Event, pos source.Position) error {
	f.calls++
	return f.err
}

func TestPauseRegistrationFailureFailsWholeBatch(t *testing.T) {
	raw := strings.TrimSpace(os.Getenv("JINLIANG_TEST_PG_DSN"))
	if raw == "" {
		t.Fatal("缺 JINLIANG_TEST_PG_DSN：这条用例要真 PostgreSQL（缺了就是失败，不跳过）")
	}
	ctx := context.Background()
	ep, err := dsn.Parse(raw)
	if err != nil {
		t.Fatalf("解析 DSN: %v", err)
	}
	conn, err := pgx.Connect(ctx, raw)
	if err != nil {
		t.Fatalf("连目标库: %v", err)
	}
	defer conn.Close(ctx)

	if _, err := conn.Exec(ctx, "DROP TABLE IF EXISTS _jl_meta, _jl_chunk, _jl_lease, _jl_object, _jl_instance"); err != nil {
		t.Fatalf("清状态表: %v", err)
	}
	s, err := pgsink.Open(ctx, pgsink.Options{
		Target: ep, JobKey: "pausefail01", JobName: "pause-fail", InstanceID: "test",
		Schema: "public", OnExists: "append",
	})
	if err != nil {
		t.Fatalf("打开目标端: %v", err)
	}
	defer s.Close()
	if err := s.EnsureMeta(ctx, "hash", "streaming", "", ""); err != nil {
		t.Fatalf("建状态行: %v", err)
	}

	// 目标端：任何「写 paused_tables」的更新都失败
	if _, err := conn.Exec(ctx, `CREATE OR REPLACE FUNCTION jl_fail_pause() RETURNS trigger AS $$
BEGIN
  IF NEW.paused_tables IS DISTINCT FROM OLD.paused_tables THEN
    RAISE EXCEPTION '注入：暂停登记落库失败（jl_fail_pause 触发器）';
  END IF;
  RETURN NEW;
END $$ LANGUAGE plpgsql;`); err != nil {
		t.Fatalf("建触发器函数: %v", err)
	}
	if _, err := conn.Exec(ctx, `CREATE TRIGGER jl_fail_pause BEFORE UPDATE ON _jl_meta
FOR EACH ROW EXECUTE FUNCTION jl_fail_pause()`); err != nil {
		t.Fatalf("建触发器: %v", err)
	}
	defer func() {
		_, _ = conn.Exec(ctx, "DROP TRIGGER IF EXISTS jl_fail_pause ON _jl_meta")
		_, _ = conn.Exec(ctx, "DROP FUNCTION IF EXISTS jl_fail_pause()")
	}()

	r := &runner{
		doc:   control.JobDoc{Target: control.TargetDoc{Schema: "public"}},
		job:   control.Job{JobKey: "pausefail01"},
		srcEP: ep,
		tgtEP: ep,
		sink:  s,
	}
	inner := &failingApplier{err: &pgsink.TypeChangedError{
		TableName: "pausefail_a", Detail: "v: 目标 bigint / 源 numeric(12,2)",
	}}
	ps := newPauseSink(inner, r)

	ref := source.TableRef{Schema: "public", Table: "pausefail_a"}
	evs := []source.Event{
		{Kind: source.EvBegin, TxID: "tx1"},
		{Kind: source.EvInsert, TxID: "tx1", Table: ref,
			New: source.Row{Table: ref, Values: []source.Value{
				{Kind: source.KindString, S: "301"}, {Kind: source.KindString, S: "1.25"}}}},
		{Kind: source.EvCommit, TxID: "tx1"},
	}
	err = ps.Apply(ctx, evs, nil)
	if err == nil {
		t.Fatal("暂停登记落库失败时这一批**必须失败**：吞掉它等于「跳过了但没记下」")
	}
	if !strings.Contains(err.Error(), "另外：登记这张表的暂停状态也失败了") {
		t.Fatalf("错误里没有 registerPause 的包裹语——写路径的登记被跳过/吞掉了：%v", err)
	}
	if !strings.Contains(err.Error(), "注入：暂停登记落库失败") {
		t.Fatalf("错误里没有触发器原话（PauseTable 的 UPDATE 没真的执行）：%v", err)
	}
	// 登记失败要立刻整批失败：不许原地重试（`maxPausePerBatch` 那条有界重试红线）。
	// 置空 registerPause 时这里会变成 9 次（8 次重试 + 首次），用例必红。
	if inner.calls != 1 {
		t.Fatalf("假目标端被调用了 %d 次，应为 1 次（登记失败立刻整批失败，不重试）", inner.calls)
	}

	// 状态表里不许留半条记录（登记没成功＝没登记）
	var paused string
	if err := conn.QueryRow(ctx, `SELECT COALESCE(paused_tables,'') FROM _jl_meta WHERE job_key='pausefail01'`).Scan(&paused); err != nil {
		t.Fatalf("读 paused_tables: %v", err)
	}
	if strings.Contains(paused, "pausefail_a") {
		t.Fatalf("登记落库失败却留下了暂停记录：%q", paused)
	}
	t.Logf("写路径登记失败：整批失败、只尝试 1 次、paused_tables 无记录；错误含 registerPause 包裹语与触发器原话")
}
