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
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestMain(m *testing.M) {
	if dsn := os.Getenv("JINLIANG_TEST_PG_SRC_DSN"); dsn != "" {
		reclaimSlots(dsn)
	}
	code := m.Run()
	if dsn := os.Getenv("JINLIANG_TEST_PG_SRC_DSN"); dsn != "" {
		reclaimSlots(dsn)
	}
	os.Exit(code)
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
