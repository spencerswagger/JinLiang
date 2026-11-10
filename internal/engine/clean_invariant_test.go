package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/spencerswagger/jinliang/internal/control"
	"github.com/spencerswagger/jinliang/internal/source"
)

// fakeState 是 `cleanBeforeGenericCopy` 用到的那几个状态能力的假实现。
//
// 只实现 Count / DidCreateTable / TruncateTable（判据真正会调的三个）；其余方法由嵌入
// 的接口占位——本用例走不到它们，真走到了会 panic，正好提醒「判据又多了依赖」。
type fakeState struct {
	stateCapable

	rows      int64
	ours      bool
	truncated []string
}

func (f *fakeState) Count(context.Context, source.TableRef) (int64, error) { return f.rows, nil }

func (f *fakeState) DidCreateTable(context.Context, source.TableRef) (bool, error) {
	return f.ours, nil
}

func (f *fakeState) TruncateTable(_ context.Context, ref source.TableRef) error {
	f.truncated = append(f.truncated, ref.Table)
	return nil
}

func (f *fakeState) State(context.Context) (string, string, error) { return "snapshot", "", nil }

func newCleanRunner(table string) *runner {
	return &runner{
		doc:    control.JobDoc{Target: control.TargetDoc{Schema: "public"}},
		job:    control.Job{JobKey: "cleaninv"},
		tables: []source.TableRef{{Schema: "public", Table: table}},
	}
}

// 不变量（不连库版）：**空表永远不该被硬停**，哪怕 phase 还停在 'snapshot'。
//
// 为什么这格必须存在：这条不变量原来只靠真库 e2e（TestMySQLTargetEmptyTableWithSnapshotPhaseStillCopies）
// 守着，而 e2e 要 MySQL；将来谁把 `if n == 0 { continue }` 挪到所有权/phase 判定之后，
// `go test ./...` 里应该立刻红，而不是等到集成矩阵才发现。
//
// 两组对照：空表 → 不报错、不 TRUNCATE；有行的用户表 + phase=snapshot → 必须硬停。
// 后者是前者「不是因为它没判」的证明——两格一起才说明判据确实按 Count 分叉。
func TestCleanBeforeGenericCopySkipsEmptyTable(t *testing.T) {
	// ① 空表（且不是我们建的）+ phase=snapshot：跳过，不许硬停
	empty := &fakeState{rows: 0, ours: false}
	if err := newCleanRunner("t_empty").cleanBeforeGenericCopy(context.Background(), empty, true, "snapshot"); err != nil {
		var reuse *reusedTableRecopyError
		if errors.As(err, &reuse) {
			t.Fatalf("空表不该硬停（Count==0 必须排在所有权/phase 判定之前），实际: %v", err)
		}
		t.Fatalf("空表不该报错: %v", err)
	}
	if len(empty.truncated) != 0 {
		t.Fatalf("空表不该被 TRUNCATE，实际清了 %v", empty.truncated)
	}

	// ② 对照组：同样的 phase，但有 3 行且不是我们建的 → 必须硬停
	filled := &fakeState{rows: 3, ours: false}
	err := newCleanRunner("t_filled").cleanBeforeGenericCopy(context.Background(), filled, true, "snapshot")
	var reuse *reusedTableRecopyError
	if !errors.As(err, &reuse) {
		t.Fatalf("有行的用户表 + phase=snapshot 必须硬停（reusedTableRecopyError），实际: %v", err)
	}
	if len(filled.truncated) != 0 {
		t.Fatalf("不是我们建的表不许 TRUNCATE，实际清了 %v", filled.truncated)
	}

	// ③ 本任务建的表（有行）→ 清；这条与 ① 一起说明「清谁」只由所有权决定
	ours := &fakeState{rows: 5, ours: true}
	if err := newCleanRunner("t_ours").cleanBeforeGenericCopy(context.Background(), ours, true, "snapshot"); err != nil {
		t.Fatalf("本任务建的表应允许清: %v", err)
	}
	if len(ours.truncated) != 1 || ours.truncated[0] != "t_ours" {
		t.Fatalf("本任务建的表应被 TRUNCATE 一次，实际 %v", ours.truncated)
	}
}
