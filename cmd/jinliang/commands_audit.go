package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/spencerswagger/jinliang/internal/control"
)

// cmdAuditPrune 清理过期的审计记录（默认 dry-run）。
//
// hugo 定的口径：默认保留 180 天；**登录、凭证、连接变更类动作永久保留**；
// 清理动作本身要留痕。所以这里的默认是「只统计不删」——清理不可逆，
// 默认就该安全；要真删必须显式 --apply。
func cmdAuditPrune(args []string) error {
	fs := flag.NewFlagSet("audit-prune", flag.ExitOnError)
	dir := fs.String("data-dir", "", "数据目录（默认 $JINLIANG_DATA_DIR 或 ~/.jinliang）")
	days := fs.Int("days", int(control.AuditRetention.Hours()/24), "保留天数（默认 180）")
	apply := fs.Bool("apply", false, "真的删除（默认 dry-run）")
	jsonOut := fs.Bool("json", false, "JSON 输出")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *days <= 0 {
		return fmt.Errorf("--days 必须大于 0（想全清就不是清理，是删库）")
	}
	store, err := openStore(dataDir(*dir), "", false)
	if err != nil {
		return err
	}
	defer store.Close()

	before := time.Now().Add(-time.Duration(*days) * 24 * time.Hour)
	res, err := store.PruneAudit(context.Background(), before, *apply)
	if err != nil {
		return err
	}
	if *jsonOut {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(res)
	}
	mode := "dry-run（只统计，不删）"
	if res.Apply {
		mode = "apply（已真的删除）"
	}
	fmt.Printf("审计清理 %s：保留 %d 天，截止 %s\n", mode, *days, res.Cutoff)
	fmt.Printf("  可删除 %d 条；永久保留（登录/凭证/连接变更/清理动作本身）%d 条\n", res.Deleted, res.Kept)
	if len(res.Candidates) > 0 {
		fmt.Printf("  涉及动作：%v\n", res.Candidates)
	}
	if !res.Apply && res.Deleted > 0 {
		fmt.Println("  要真的删除，加 --apply")
	}
	return nil
}
