package control

import "testing"

// 迁移版本号必须唯一且严格递增。
//
// 为什么单独立一条测试：`migrate()` 用启动时读到的 schema_version 做「跳过还是执行」
// 的唯一判据。两条迁移撞同一个版本号时，行为不是报错而是**静默跳过**——库已经是
// 那个版本，两条都 skip；`latest` 也仍等于那个版本，`CodeSchemaTooNew` 那条检查同样
// 发现不了。结果就是「后来的那条迁移带来的列永远不存在」，直到某条查询炸掉。
// （PR #5 的 runs-liveness 与本分支的 runs-kind 就撞过一次，两边都是 2。）
func TestMigrationVersionsAreUniqueAndOrdered(t *testing.T) {
	seen := map[int]string{}
	prev := 0
	for _, m := range migrations {
		if m.Version <= 0 {
			t.Fatalf("迁移 %q 的版本号必须是正数，实际 %d", m.Name, m.Version)
		}
		if dup, ok := seen[m.Version]; ok {
			t.Fatalf("迁移版本号撞了：%d 同时被 %q 与 %q 使用", m.Version, dup, m.Name)
		}
		seen[m.Version] = m.Name
		if m.Version <= prev {
			t.Fatalf("迁移必须严格递增：%d 出现在 %d 之后", m.Version, prev)
		}
		prev = m.Version
		if m.Name == "" {
			t.Fatalf("迁移 %d 必须有名字", m.Version)
		}
		if len(m.Stmts) == 0 && m.Fn == nil {
			t.Fatalf("迁移 %d(%s) 既没有 SQL 也没有 Fn，等于空转", m.Version, m.Name)
		}
	}
	if got := LatestSchemaVersion(); got != prev {
		t.Fatalf("LatestSchemaVersion() = %d，但最后一条迁移是 %d", got, prev)
	}
	if len(migrations) < 2 {
		t.Fatalf("至少应该有两条迁移（初始 + runs-kind）")
	}
}
