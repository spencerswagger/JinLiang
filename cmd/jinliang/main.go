// Command jinliang runs database synchronization jobs.
//
// 津梁 (jinliang) 意为渡口与桥 —— 在异构数据库之间摆渡数据。
//
// 当前处于设计阶段，本命令只提供版本信息与最小骨架，功能实现见 docs/DESIGN.md。
package main

import (
	"flag"
	"fmt"
	"os"
)

var version = "dev"

func main() {
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("jinliang %s\n", version)
		return
	}

	fmt.Fprintln(os.Stderr, "jinliang: 设计阶段，尚无可用命令。见 https://github.com/spencerswagger/JinLiang/blob/main/docs/DESIGN.md")
	os.Exit(1)
}
