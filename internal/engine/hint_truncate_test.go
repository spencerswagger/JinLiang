package engine

import (
	"strings"
	"testing"

	"github.com/spencerswagger/jinliang/internal/mysqlsink"
)

// 清表闸门（mysqlsink.ErrTruncateNotOwned）不能落到「未识别的数据库错误 → 找维护者」。
//
// 为什么值得一格用例：这类错误**用户自己就能解决**（改 on_exists 或移出范围），
// 一旦被兜底文案接管，用户拿到的是「把日志发给维护者」——而三行之后就是我们自己
// 写好的那三条出路。文案层的回归没有编译期保护，只能靠用例钉。
func TestHintTruncateNotOwnedGivesRealOptions(t *testing.T) {
	why, next := hint(mysqlsink.ErrTruncateNotOwned)
	for _, bad := range []string{"未识别", "发给维护者"} {
		if strings.Contains(why, bad) || containsAny(next, bad) {
			t.Fatalf("这类错误用户能自己解决，不许落到兜底文案（含 %q）：why=%q next=%v", bad, why, next)
		}
	}
	joined := why + strings.Join(next, " ")
	for _, want := range []string{"不是本任务建", "recreate", "content.tables"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("文案里应有 %q（三条出路），实际 why=%q next=%v", want, why, next)
		}
	}
}

func containsAny(list []string, sub string) bool {
	for _, s := range list {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}
