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
	Apply(ctx context.Context, evs []source.Event, pos source.Position) error
	// Verify compares source and target for a table and returns mismatch count.
	Verify(ctx context.Context, table source.TableRef) (int64, error)
	Cleanup(ctx context.Context) error
}
