//go:build e2e

package production

import (
	"context"
	"testing"
)

// redirtyRows writes each of creates again with the attributes it was
// created with, so the rows are back in the dirty set at unchanged values,
// and the next flush exports them to a delta object. A test that lays rows
// out in base and delta at once needs it under Protocol V2: the cdc-init
// handoff marks every row the init exported, so rows exported to base reach delta only through a later write. It
// answers the update events, in creates' order, with their read-back
// timestamps.
func redirtyRows(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, creates []*Event) []*Event {
	t.Helper()
	updates := make([]*Event, 0, len(creates))
	for _, ev := range creates {
		updates = append(updates, UpdateEvent(schema, ev.RowID, ev.Attrs))
	}
	mustApplyEvents(ctx, t, env, "re-write rows at unchanged values", updates...)
	return updates
}
