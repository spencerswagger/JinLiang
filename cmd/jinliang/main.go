// Command jinliang runs database synchronization jobs.
//
// 津梁 (jinliang) 意为渡口与桥 —— 在异构数据库之间摆渡数据。
//
// 命令面（docs/CONTROL-PLANE.md §7）：
//
//	jinliang serve    起控制面（WebUI + REST + SSE），默认只监听 127.0.0.1
//	jinliang import   把 job.yaml 导进控制库（GitOps / 迁移 / 备份恢复）
//	jinliang export   把控制库里的任务导出成 job.yaml
//	jinliang run      无 UI 的 headless 执行（容器 / CI）
//	jinliang doctor   体检：控制库完整性、密钥来源、监听地址、待迁移
//	jinliang version  版本
package main

import (
	"fmt"
	"os"
)

var version = "dev"

func main() {
	usage := func() {
		fmt.Fprintf(os.Stderr, `jinliang %s — 异构数据库同步

用法:
  jinliang serve    [--listen 127.0.0.1:8080] [--data-dir DIR] [--key-file FILE] [--api-token TOKEN]
  jinliang import   --file job.yaml [--dry-run] [--data-dir DIR]
  jinliang export   [--job ID|NAME] [--out FILE] [--all] [--data-dir DIR]
  jinliang run      --job ID|NAME [--json] [--data-dir DIR]
  jinliang doctor   [--json] [--data-dir DIR]
  jinliang version

环境变量:
  JINLIANG_DATA_DIR   数据目录（默认 ~/.config/jinliang）
  JINLIANG_KEY        主密钥（hex/base64；优先于自动生成的 data-dir/key）
  JINLIANG_API_TOKEN  固定 bearer token（临时调试用）
`, version)
	}

	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	args := os.Args[2:]
	var err error
	switch cmd {
	case "version", "-version", "--version":
		fmt.Printf("jinliang %s\n", version)
		return
	case "serve":
		err = cmdServe(args)
	case "import":
		err = cmdImport(args)
	case "export":
		err = cmdExport(args)
	case "run":
		err = cmdRun(args)
	case "doctor":
		err = cmdDoctor(args)
	case "help", "-h", "--help":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "jinliang: 不认识的命令 %q\n\n", cmd)
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "jinliang %s: %v\n", cmd, err)
		os.Exit(1)
	}
}
