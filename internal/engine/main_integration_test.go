//go:build integration

package engine_test

// 集成测试包的启动/收尾。
//
// 为什么需要 TestMain：逻辑复制书签是**实例级**资源（`max_replication_slots`，PG 默认 10），
// 而一轮 engine 集成用例会建 6~10 个书签。用例进程被 kill -9、或 teardown 因为控制库被
// SIGKILL 留下锁而放弃清理时，书签就留在源库里；下一轮从这里起步，跑到一半就撞
// `all replication slots are in use (SQLSTATE 53400)`——这才是「整包跑红、单条单跑绿」
// 的真相（不是用例之间有顺序依赖，是资源没还）。
//
// 所以：包开跑前把**上一轮泄漏**的 inactive 书签收回来，包跑完后也再收一次。
// 只在源库、只动 `jl_%`（本产品自己的书签名前缀），active 的不碰（那是别人正在用）。

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// reclaimEnabled：回收只在**显式开启**时做（JINLIANG_TEST_RECLAIM_SLOTS=1）。
//
// 默认关的理由：reclaimSlots 删的是源库里**所有空闲的 `jl_%` 槽**。CI 里每个 job 自己
// `docker run` 一套 PG、独占，所以没问题；但只要有人把 JINLIANG_TEST_PG_SRC_DSN 指到有真实
// 任务的库，或者本地用 `go test ./...`（包级并行）跟别人共用一套 PG，就会把别人的空闲槽一起
// 收走——对方下一次连流直接报「槽不存在」，甚至在流上被 pg_terminate_backend 打断成 57P01。
// CI 的集成步骤里显式设 JINLIANG_TEST_RECLAIM_SLOTS=1（那里源库是 job 独占的）。
// 只认 1/true/on/yes（大小写不敏感、容忍空格）；写成别的值当关闭，但会**喊一声**——
// 静默失效最坑：本地写 =true 或带空格，开关没开、过一会儿撞 53400，还不知道为什么。
func reclaimEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("JINLIANG_TEST_RECLAIM_SLOTS"))) {
	case "1", "true", "on", "yes":
		return true
	default:
		return false
	}
}

func TestMain(m *testing.M) {
	dsn := os.Getenv("JINLIANG_TEST_PG_SRC_DSN")
	if raw := strings.TrimSpace(os.Getenv("JINLIANG_TEST_RECLAIM_SLOTS")); raw != "" && !reclaimEnabled() {
		fmt.Printf("[engine-it] JINLIANG_TEST_RECLAIM_SLOTS=%q 不被识别（只认 1/true/on/yes），按关闭处理\n", raw)
	}
	beforeReclaim(dsn)
	code := m.Run()
	afterRun(dsn)
	os.Exit(code)
}

// beforeReclaim：开着就回收；关着就**只数不删**，把「忘开开关 / 新 job 漏注入」从
// 神秘 53400 变成一眼可见的提示。
func beforeReclaim(dsn string) {
	if dsn == "" {
		return
	}
	if reclaimEnabled() {
		reclaimSlots(dsn)
		return
	}
	n, names := idleSlots(dsn)
	if n > 0 {
		fmt.Printf("[engine-it] 发现 %d 个未回收的空闲书签（%s）：本轮不回收（默认关）。\n"+
			"[engine-it] 若这套库是测试独占的，设 JINLIANG_TEST_RECLAIM_SLOTS=1 让包首尾自动回收，否则容易撞 "+
			"\"all replication slots are in use (SQLSTATE 53400)\"；也可以手动 SELECT pg_drop_replication_slot(name)。\n",
			n, strings.Join(names, ", "))
	}
}

// afterRun：包跑完同样只在开着时回收。
func afterRun(dsn string) {
	if dsn == "" || !reclaimEnabled() {
		return
	}
	reclaimSlots(dsn)
}

// idleSlots 数一下源库里空闲的 `jl_%` 书签（不删），给上面的提示用。
func idleSlots(dsn string) (int, []string) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return 0, nil
	}
	defer conn.Close(context.Background())
	rows, err := conn.Query(ctx,
		`SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'jl\_%' AND NOT active ORDER BY slot_name`)
	if err != nil {
		return 0, nil
	}
	var names []string
	for rows.Next() {
		var n string
		if rows.Scan(&n) == nil {
			names = append(names, n)
		}
	}
	rows.Close()
	return len(names), names
}

// reclaimSlots 删掉源库里**空闲**的 `jl_%` 书签（上一轮留下来的）。
func reclaimSlots(dsn string) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return // 连不上就让用例自己报（缺 DSN/缺库是失败，但这里不抢它的错）
	}
	defer conn.Close(context.Background())
	rows, err := conn.Query(ctx,
		`SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'jl\_%' AND NOT active`)
	if err != nil {
		return
	}
	var names []string
	for rows.Next() {
		var n string
		if rows.Scan(&n) == nil {
			names = append(names, n)
		}
	}
	rows.Close()
	for _, n := range names {
		_, _ = conn.Exec(ctx, `SELECT pg_drop_replication_slot($1)`, n)
	}
}
