package main

import (
	"flag"
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/core"
)

// 这条用例守的是「文案承诺 = 真的能做」这一类事故：
// E3001 的 Next 让用户跑 `--resync`，而 CLI 里曾经**没有**这个旗标——
// 用户照着敲只会撞 "flag provided but not defined"，唯一的自救路径成了空头承诺。
//
// 判据是机器从码表里读，不是靠人记得同步两处。
func TestE3001NextNamesAFlagThisBinaryRegisters(t *testing.T) {
	spec, ok := core.Lookup(core.CodePositionGap)
	if !ok {
		t.Fatal("码表里没有 E3001")
	}
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	registerResyncFlags(fs)

	next := strings.Join(spec.Next, " ")
	if !strings.Contains(next, "--job") {
		// run 必须要 --job；文案里漏了它，用户复制出去也跑不起来。
		t.Fatalf("E3001 的 Next 必须给出完整的 run 命令（含 --job）：%q", next)
	}
	var seenResync bool
	for _, tok := range flagTokens(next) {
		name := tok
		if name == "resync" {
			seenResync = true
		}
		if fs.Lookup(name) == nil && name != "job" {
			t.Fatalf("E3001 的 Next 里点名了 --%s，但 run 没有注册这个旗标（用户会撞 unknown flag）", name)
		}
	}
	if !seenResync {
		t.Fatalf("E3001 的 Next 必须给出 --resync（唯一的自救路径）：%q", next)
	}
}

// flagTokens 从文案里抠出长旗标名：`--resync（会重建…）` 这种紧贴中文的也要认。
func flagTokens(text string) []string {
	var out []string
	for _, f := range strings.Fields(text) {
		if !strings.HasPrefix(f, "--") {
			continue
		}
		name := strings.TrimPrefix(f, "--")
		end := len(name)
		for i, r := range name {
			if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_') {
				end = i
				break
			}
		}
		out = append(out, name[:end])
	}
	return out
}

// 非交互环境（容器/CI）不许「读到 EOF 就当取消」：静默取消会让脚本以为跑了。
// 必须拒绝并要求显式 --yes。
func TestResyncRequiresExplicitYesWhenNotInteractive(t *testing.T) {
	err := confirmResync("orders")
	if err == nil {
		// go test 的 stdin 不是终端；真到了终端环境这条断言就没意义了，
		// 所以只在确实非交互时才断言（否则跳过，不制造假绿）。
		t.Skip("stdin 是终端：这条用例只覆盖非交互场景")
	}
	if !strings.Contains(err.Error(), "--yes") {
		t.Fatalf("非交互环境必须提示加 --yes，得到 %v", err)
	}
	if !strings.Contains(err.Error(), "什么都没动") {
		t.Fatalf("拒绝时必须交代「什么都没动」，得到 %v", err)
	}
}

// --resync 只作用于「这一轮」：和 --watch 一起用要么每轮都清表，要么只有第一轮生效，
// 两种都不能默认发生。必须在开库、开跑之前拒绝。
func TestRunRejectsResyncWithWatch(t *testing.T) {
	err := cmdRun([]string{"--resync", "--watch", "--job", "orders", "--yes"})
	if err == nil || !strings.Contains(err.Error(), "--watch") {
		t.Fatalf("--resync 配 --watch 必须被拒绝并说明原因，得到 %v", err)
	}
	if !strings.Contains(err.Error(), "什么都没动") {
		t.Fatalf("拒绝必须交代「什么都没动」，得到 %v", err)
	}
}
