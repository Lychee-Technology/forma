package model

import (
	"github.com/google/uuid"

	"github.com/lychee-technology/forma"
)

// HotKeysQuery says what the pgx hot leg's hot-keys query reads
// (design.md §6.1.1, step 2 of a pass; pgx-hot-leg-plan.md step 4): the
// entity_main rows of one schema that have a change_log entry in the dirty
// window and match Condition, filtered as the PostgreSQL route filters.
type HotKeysQuery struct {
	Tables    StorageTables
	SchemaID  int16
	Condition forma.Condition
	// GraceCutoffMs bounds the dirty window: an entry is in it when it is
	// unflushed, or was marked at or after the cutoff. It is the cutoff of
	// the dirty set the pass stages, so that the hot keys and the anti-join
	// cover the same rows.
	GraceCutoffMs int64
	// SortColumns names the page order's columns, key for key (the Name of
	// each sortkey.Column): a system column of the visible CTE or an
	// attribute. Each hot key holds one derived value per name, in order.
	SortColumns []string
}

// HotKey is one row the hot-keys query matched: its identity, the version
// the dirty window holds for it, and its sort key.
type HotKey struct {
	RowID uuid.UUID
	// VerTS is the changed_at of the row's newest change_log entry in the
	// window.
	VerTS int64
	// Key holds one value per HotKeysQuery.SortColumns name, derived from
	// the stored value by the CDC export's rules (internal/sortkey), so it
	// compares as the row's next export will.
	Key []any
}
