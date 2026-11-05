// Package sink abstracts the write side: batched transactions, idempotent
// upserts, and committing the position in the same transaction as the data.
package sink

import (
	"context"

	"github.com/spencerswagger/jinliang/internal/source"
)

// Action mirrors source.Action so sinks can report what they created.
type Action = source.Action

// Sink is the write side. Apply must be atomic: rows and position commit
// together, which is what makes replays idempotent without any external
// coordinator (DESIGN.md §7.3).
type Sink interface {
	Kind() string
	// EnsureSchema creates missing tables/columns per the chosen policy.
	EnsureSchema(ctx context.Context, catalog source.Catalog) ([]Action, error)
	// Apply writes one batch and the resulting position in a single transaction.
	//
	// 不变量（2026-11-05 契约裁决，三轨并行前定下来）：
	//   - 事务边界由 source.Event 的 EvBegin / EvCommit 表达。**EvCommit 是「一个事务已提交」
	//     的唯一可见信号**；连接器只上报已提交的事务（PG 逻辑解码与 MySQL binlog 都只在
	//     提交后可读），所以不存在 rollback 事件，引擎也不该等它。
	//   - pos 必须落在事务边界上：一个批次可以包含多个完整事务，也可以切开一个大事务，
	//     但**半个事务的批次必须给上一个已提交的位点**，不能给事务中间的位点——
	//     否则崩溃重放的起点会漂，catchup 会从半截事务开始。
	//   - 数据与位点同事务提交（DESIGN.md §1）；重放的幂等性靠 upsert 兜住，不靠位点精确。
	Apply(ctx context.Context, evs []source.Event, pos source.Position) error
	// Verify compares source and target for a table and returns mismatch count.
	Verify(ctx context.Context, table source.TableRef) (int64, error)
	Cleanup(ctx context.Context) error
}
