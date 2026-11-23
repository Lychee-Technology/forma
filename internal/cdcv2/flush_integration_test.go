package cdcv2

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// TestFlushPublishesAndMarksInOneCommit runs the flush of an unpublished
// schema and a flush over it: each commits one generation and marks its
// entries in the same transaction, the first starts a lineage at parent 0,
// the second keeps it and lists its parent's files.
func TestFlushPublishesAndMarksInOneCommit(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.register(t)
	w := f.writer(t)
	st := &stager{f: f}

	first := f.insert(t, 3)
	rep, err := w.Flush(ctx, schemaID, 1, st)
	require.NoError(t, err)
	require.Len(t, rep.Generations, 1)
	require.Equal(t, 3, rep.Marked)
	g1 := f.record(t)
	require.Equal(t, rep.Generations[0].Token, g1.Token)
	require.Equal(t, int64(1), g1.Generation)
	require.Zero(t, g1.ParentGeneration)
	require.Empty(t, f.dirty(t))
	f.checkInvariants(t)

	f.put(t, first[0], 2000, false)
	f.put(t, first[1], 2001, true)
	second := f.insert(t, 2)
	rep, err = w.Flush(ctx, schemaID, 1, st)
	require.NoError(t, err)
	require.Equal(t, 4, rep.Marked)
	g2 := f.record(t)
	require.Equal(t, int64(2), g2.Generation)
	require.Equal(t, int64(1), g2.ParentGeneration)
	require.Equal(t, g1.Lineage, g2.Lineage, "a flush keeps the record's lineage")
	require.Empty(t, f.dirty(t))
	held := f.held(t)
	require.Len(t, held, 5)
	require.Equal(t, rowCopy{Row: first[1], Version: 2001, Deleted: true}, held[first[1]], "a deletion flushes as a tombstone")
	require.Contains(t, held, second[1])
	f.checkInvariants(t)

	rep, err = w.Flush(ctx, schemaID, 1, st)
	require.NoError(t, err)
	require.Empty(t, rep.Generations, "nothing unflushed, nothing published")
	require.Equal(t, g2, f.record(t))
}

// TestFlushGenerationSize pins the transaction budget: on Aurora DSQL a
// generation marks at most DSQLGenerationLimit entries, so the default
// 10,000-entry batch publishes as ten generations; on PostgreSQL the batch
// is one generation.
func TestFlushGenerationSize(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.register(t)
	f.insert(t, DefaultBatchSize)

	w := f.writer(t, func(c *Config) { c.Engine = forma.EngineAuroraDSQL })
	rep, err := w.Flush(ctx, schemaID, 1, &stager{f: f})
	require.NoError(t, err)
	require.Len(t, rep.Generations, 10)
	require.Equal(t, DefaultBatchSize, rep.Marked)
	require.Empty(t, f.dirty(t))
	prev := 0
	for _, g := range f.history(t) {
		m, err := manifestv2.Fetch(ctx, f.store, manifestv2.GenerationRef(schemaID, g))
		require.NoError(t, err)
		require.Len(t, m.Files, prev+1, "each generation lists its parent's files and one delta")
		require.LessOrEqual(t, m.Files[len(m.Files)-1].RowCount, int64(DSQLGenerationLimit))
		prev = len(m.Files)
	}
	f.checkInvariants(t)

	if f.engine != forma.EnginePostgreSQL {
		return // the PostgreSQL mark takes row locks the DSQL target refuses
	}
	f.insert(t, 2500)
	rep, err = f.writer(t).Flush(ctx, schemaID, 1, &stager{f: f})
	require.NoError(t, err)
	require.Len(t, rep.Generations, 1, "a PostgreSQL generation keeps the whole batch")
	require.Equal(t, 2500, rep.Marked)
	f.checkInvariants(t)
}

// TestFlushRefusesWhatOnlyAnInitOrNothingMayPublish pins the states a flush
// fails closed on: no record, AWAITING_INIT, DROPPED, and an epoch other
// than the one the process loaded the schema at.
func TestFlushRefusesWhatOnlyAnInitOrNothingMayPublish(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	w := f.writer(t)
	st := &stager{f: f}
	f.insert(t, 2)

	_, err := w.Flush(ctx, schemaID, 0, st)
	require.ErrorIs(t, err, publication.ErrNoRecord)

	f.inTx(t, func(tx *sql.Tx) error {
		return f.initializeAwaitingInit(ctx, tx)
	})
	_, err = w.Flush(ctx, schemaID, 1, st)
	require.ErrorIs(t, err, publication.ErrAwaitingInit)
	_, err = w.Flush(ctx, schemaID, 2, st)
	require.ErrorIs(t, err, ErrEpochChanged)

	f.drop(t)
	_, err = w.Flush(ctx, schemaID, 0, st)
	require.ErrorIs(t, err, publication.ErrDropped)
	require.Len(t, f.dirty(t), 2, "nothing marked")
	require.Empty(t, f.history(t))
	require.Zero(t, st.calls, "nothing staged")
}

// TestFlushLeavesARowThatMovedDirty pins the exact-version mark: a row
// written again after the flush selected it is exported at its new state,
// not marked, and stays dirty for the next flush.
func TestFlushLeavesARowThatMovedDirty(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	f.register(t)
	rows := f.insert(t, 3)
	st := &stager{f: f, during: func(context.Context, []uuid.UUID) error {
		f.put(t, rows[0], 5000, false)
		return nil
	}}
	rep, err := f.writer(t).Flush(ctx, schemaID, 1, st)
	require.NoError(t, err)
	require.Equal(t, 2, rep.Marked)
	require.Equal(t, map[uuid.UUID]int64{rows[0]: 5000}, f.dirty(t))
	f.checkInvariants(t)

	st.during = nil
	rep, err = f.writer(t).Flush(ctx, schemaID, 1, st)
	require.NoError(t, err)
	require.Equal(t, 1, rep.Marked)
	require.Empty(t, f.dirty(t))
	f.checkInvariants(t)
}

// drop resets the schema's record, as the drop transaction does.
func (f *fixture) drop(t *testing.T) publication.Record {
	t.Helper()
	cur := f.record(t)
	f.inTx(t, func(tx *sql.Tx) error {
		_, err := publication.Reset(context.Background(), publication.SQLTx(tx), f.tables, cur)
		return err
	})
	return f.record(t)
}

// recreate purges the dropped schema's rows and activates its id again.
func (f *fixture) recreate(t *testing.T) publication.Record {
	t.Helper()
	cur := f.record(t)
	_, err := f.db.Exec(fmt.Sprintf("DELETE FROM %s WHERE schema_id = $1", f.changeLog), schemaID)
	require.NoError(t, err)
	_, err = f.db.Exec(fmt.Sprintf("DELETE FROM %s WHERE ltbase_schema_id = $1", f.entityMain), schemaID)
	require.NoError(t, err)
	f.inTx(t, func(tx *sql.Tx) error {
		_, err := publication.Activate(context.Background(), publication.SQLTx(tx), f.tables, cur, []publication.Residue{{Table: f.changeLog}})
		return err
	})
	return f.record(t)
}

// TestMarkBindsMaxMarkEntries runs the mark at its bound: one entry more
// than MaxMarkEntries is refused before it is sent, on either engine, and on
// PostgreSQL, whose flush generation is the whole batch, a statement of
// MaxMarkEntries entries binds on the server, so a publication never stages
// a batch it cannot mark with its pointer. A DSQL generation stays at
// DSQLGenerationLimit, far below the bound.
func TestMarkBindsMaxMarkEntries(t *testing.T) {
	f := newFixture(t)
	w := f.writer(t)
	entries := make([]Entry, MaxMarkEntries+1)
	for i := range entries {
		entries[i] = Entry{RowID: uuid.New(), Version: 1000}
	}
	ctx := context.Background()
	tx, err := f.db.Begin()
	require.NoError(t, err)
	_, err = w.mark(ctx, tx, schemaID, entries, 1)
	require.ErrorContains(t, err, "at most 32766 (MaxMarkEntries)")
	require.NoError(t, tx.Rollback())
	if f.engine != forma.EnginePostgreSQL {
		return
	}
	f.inTx(t, func(tx *sql.Tx) error {
		n, err := w.mark(ctx, tx, schemaID, entries[:MaxMarkEntries], 1)
		if err == nil && n != 0 {
			err = fmt.Errorf("marked %d entries change_log does not hold", n)
		}
		return err
	})
}
