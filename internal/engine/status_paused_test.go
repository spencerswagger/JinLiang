package engine

// `status` 的暂停态**不需要库**的用例（Owl II 复核 #109 必改 1：`paused_tables` 永远为空）。
//
// 这条 bug 的成因不在「读得对不对」，而在「根本没读」：`StatusFor` 造的 runner 没有
// sink / pauseReg（那是执行路径才构造的），`pauseRegistryOf()` 恒 nil，于是
// `status --json` 的 `paused_tables` 永远为空、PG 端的 `paused_note` 从不出现，
// 而 MySQL 端还在打印一句「按表暂停在 MySQL 目标端暂不支持」（#102 早就支持了）。
//
// 这里钉的是**三种结论不能合并**：
//   - 读不到（连不上 / 权限 / 脏 JSON）→ `nil` + note；
//   - 读到了、没有暂停 → `[]`（空数组）+ 没有 note；
//   - 读到了、有暂停 → 非空 + 没有 note。
// 前两者在「要不要处理」上是相反的，折叠成同一个形状就是这条 bug 的另一种形态。
// 「读到了、有暂停」需要真库，落在 `integration_status_pause_test.go`（-tags=integration）。

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/spencerswagger/jinliang/internal/dsn"
)

// deadEndpoint 指向一个**一定连不上**的端口（1），用来造「读失败」这个真实分支。
// 不用 mock：要证明的是「连不上时落到 note」，mock 掉连接反而证明不了。
func deadEndpoint(kind string) dsn.Endpoint {
	return dsn.Endpoint{Kind: kind, Host: "127.0.0.1", Port: 1,
		Database: "jinliang_test", Username: "u", Password: "p"}
}

func TestStatusPausedUnreadableTargetFallsBackToNote(t *testing.T) {
	for _, kind := range []string{dsn.KindPostgres, dsn.KindMySQL} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			stats, note := statusPaused(ctx, deadEndpoint(kind), "public", "abcdef1234")
			if stats != nil {
				t.Fatalf("读不到时必须返回 nil（JSON 里的 null），得到 %#v —— 空数组的意思是「没有暂停」，两者相反", stats)
			}
			if !strings.Contains(note, "读不到暂停记录") || !strings.Contains(note, "不是「没有暂停」") {
				t.Fatalf("读不到时必须明说「未知，不是没有暂停」，得到 note=%q", note)
			}
		})
	}
}

// 目标端 kind 不认识时也不许静默返回空：那会让「这段代码还没学会读」看起来像「没事」。
func TestStatusPausedUnknownTargetKindIsNotSilentEmpty(t *testing.T) {
	stats, note := statusPaused(context.Background(), dsn.Endpoint{Kind: "sqlite"}, "public", "abcdef1234")
	if stats != nil {
		t.Fatalf("不认识的 kind 不许给出清单，得到 %#v", stats)
	}
	if !strings.Contains(note, "sqlite") {
		t.Fatalf("note 里要指明是哪个目标端 kind，得到 %q", note)
	}
	if !strings.Contains(note, "别把空当成「没有暂停」") {
		t.Fatalf("note 要挡住「空 = 没有暂停」这个误读，得到 %q", note)
	}
}

// 「读到了、没有暂停」要序列化成 `[]`，「读不到」要序列化成 `null`。
// 这两个形状是外部契约（Panel / 运维脚本按它判「要不要处理」），不能被 omitempty 吞掉。
func TestStatusPausedJSONDistinguishesNullAndEmpty(t *testing.T) {
	unknown := Status{PausedNote: "读不到暂停记录（不是「没有暂停」）：boom"}
	blob, err := json.Marshal(unknown)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(blob), `"paused_tables":null`) {
		t.Fatalf("读不到时 JSON 里必须是 null（未知），得到 %s", blob)
	}
	if !strings.Contains(string(blob), "paused_note") {
		t.Fatalf("读不到时必须带 paused_note，得到 %s", blob)
	}

	got := Status{PausedTables: emptyInsteadOfNil(nil)}
	blob, err = json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(blob), `"paused_tables":[]`) {
		t.Fatalf("读到、没有暂停时 JSON 里必须是空数组，得到 %s", blob)
	}
	if strings.Contains(string(blob), "paused_note") {
		t.Fatalf("没有暂停时不该有 paused_note，得到 %s", blob)
	}
}
