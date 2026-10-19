package coordinationtest

import (
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// LTFlow's deduplicated history: an event recorded again within the window
// counts on the first one's row; outside the window nothing changes.

func TestHistoryDedup(t *testing.T) {
	const window = time.Hour
	// Each key carries the env's name: the row ID derives from the key, and
	// the row-ID locks of one database are shared by every run on it.
	dedup := func(e *env, inst *Instance, key string) error {
		return e.inTx(func(uow UnitOfWork) error {
			return uow.History().UpsertDeduped(e.ctx, HistoryEvent{InstanceID: inst.ID, Type: "stuck", Details: key}, key, window)
		})
	}

	t.Run("within the window, dedup_count increments on one row", func(t *testing.T) {
		e := newEnv(t)
		inst := e.seedInstance(t, 1)
		row := DedupRowID(e.app + "/stuck-1")
		for want := int64(0); want < 3; want++ {
			before := e.stamps(t, HistorySchema, row)
			require.NoError(t, dedup(e, inst, e.app+"/stuck-1"))
			require.Equal(t, map[string]any{"dedup_count": want, "type": "stuck"}, e.stored(t, HistorySchema, row, "dedup_count", "type"))
			e.requireWritten(t, HistorySchema, before, row)
		}
		require.Equal(t, 1, e.liveRows(t, HistorySchema))
	})

	t.Run("outside the window, a silent no-op", func(t *testing.T) {
		e := newEnv(t)
		inst := e.seedInstance(t, 1)
		row := DedupRowID(e.app + "/stuck-2")
		old := []HistoryEvent{{ID: row, InstanceID: inst.ID, Type: "stuck", RecordedAt: time.Now().Add(-2 * window)}}
		require.NoError(t, e.inTx(func(uow UnitOfWork) error { return uow.History().Insert(e.ctx, old) }))
		entries := e.changeLogEntries(t, HistorySchema, row)
		require.NoError(t, dedup(e, inst, e.app+"/stuck-2"), "a deduplication outside the window is no error")
		require.Equal(t, map[string]any{"dedup_count": int64(0)}, e.stored(t, HistorySchema, row, "dedup_count"))
		require.Equal(t, entries, e.changeLogEntries(t, HistorySchema, row), "nothing was written")
		require.Equal(t, 1, e.liveRows(t, HistorySchema))
	})

	t.Run("concurrent deduplications lose no count", func(t *testing.T) {
		e := newEnv(t)
		inst := e.seedInstance(t, 1)
		const writers = 8
		errs := race(writers, func(int) error { return dedup(e, inst, e.app+"/stuck-3") })
		winners := succeeded(errs)
		t.Run("contract: one row, counting every commit after the first", func(t *testing.T) {
			require.NotEmpty(t, winners)
			for _, err := range errs {
				if err != nil {
					require.ErrorIs(t, err, forma.ErrRetryableConflict)
				}
			}
			require.Equal(t, 1, e.liveRows(t, HistorySchema))
			require.Equal(t, map[string]any{"dedup_count": int64(len(winners) - 1)}, e.stored(t, HistorySchema, DedupRowID(e.app+"/stuck-3"), "dedup_count"))
		})
		t.Run("mechanism: all commit", func(t *testing.T) {
			requirePostgresMechanisms(t)
			require.Len(t, winners, writers, "errors %v", errs)
		})
	})
}
