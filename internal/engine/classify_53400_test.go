package engine

// SQLSTATE 53400（all replication slots are in use）曾经落到「未识别的数据库错误」——
// 而我们的提示里恰好写着「书签数已满」是常见原因之一，用户被自相矛盾地告知「没分类」。
// 现在它挂 E1016：动作三条都是真能跑的命令，且位点不动、一条数据都不写。
// 变异：删掉 classifyError 里那条 53400 分支 → 本用例红。

import (
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/spencerswagger/jinliang/internal/core"
)

func TestSlotExhaustedClassifiesAsE1016(t *testing.T) {
	// 包一层：真实路径上 53400 会从 pgwire/pgsink 一路被 wrap 上来，classifyError 必须
	// 用 errors.As 找到里面的 *pgconn.PgError（而不是只认最外层）。
	drv := &pgconn.PgError{Code: "53400", Message: "all replication slots are in use"}
	err := fmt.Errorf("准备源端对象失败: %w", drv)
	code, ok := classifyError(err)
	if !ok {
		t.Fatal("53400 必须被识别（不许落到「未识别」）")
	}
	if code != core.CodeSourceResourceLimit {
		t.Fatalf("53400 应挂 %s，得到 %q", core.CodeSourceResourceLimit, code)
	}
	spec, found := core.Lookup(core.CodeSourceResourceLimit)
	if !found {
		t.Fatal("E1016 必须在码表里")
	}
	// 文案要能照着做：三条 Next 都要出现真命令；Risk 要说清「一条数据都不会写入」。
	if len(spec.Next) != 3 {
		t.Fatalf("Next 要 3 条可执行动作，得到 %d 条", len(spec.Next))
	}
	if !strings.Contains(spec.Why, "53400") {
		t.Fatalf("Why 要点名 SQLSTATE，得到 %q", spec.Why)
	}
	if !strings.Contains(spec.Risk, "一条数据都不会写入") {
		t.Fatalf("Risk 要写明不会写入任何数据，得到 %q", spec.Risk)
	}
}
