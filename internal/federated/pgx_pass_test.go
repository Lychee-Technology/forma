package federated

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sortkey"
	"github.com/stretchr/testify/require"
)

// tieRunRows is the mixed-tier tiebreak fixture (pgx-hot-leg-plan.md step
// 5): three runs of four rows that tie on created_at, and two runs that tie
// on age, each holding hot and parquet rows with their row_ids interleaved.
// Row 3 was updated after its flush (its parquet version is stale), and row
// 13 deleted after its flush.
func tieRunRows() []pgxRow {
	hot := map[int]bool{2: true, 3: true, 4: true, 7: true, 9: true, 12: true}
	var rows []pgxRow
	for i := 1; i <= 12; i++ {
		rows = append(rows, pgxRow{
			i:         i,
			createdAt: int64(100 * ((i - 1) / 4)),
			age:       ageOf(int64(30 + i%2)),
			hot:       hot[i],
			parquet:   !hot[i] || i == 3,
		})
	}
	return append(rows, pgxRow{i: 13, createdAt: 0, age: ageOf(30), parquet: true, deleted: true})
}

var tieRunTables = model.StorageTables{EntityMain: "entity_main", EAVData: "eav_data", ChangeLog: "change_log"}

func ageOrders(desc bool) []model.AttributeOrder {
	dir := forma.SortOrderAsc
	if desc {
		dir = forma.SortOrderDesc
	}
	return []model.AttributeOrder{{AttrID: 2, AttrName: "age", ValueType: forma.ValueTypeInteger,
		StorageLocation: forma.AttributeStorageLocationMain, ColumnName: "integer_01", SortOrder: dir}}
}

// Offset pages of every size tile the merged order, and every page, the
// empty ones past the end included (#181), reports the full count.
func TestPgxPassOffsetPagesTileTheMergedOrder(t *testing.T) {
	ctx := context.Background()
	f := newPgxFixture(t, tieRunRows())
	e := f.engine(t, []string{f.dir + "/base.parquet"})
	for _, orders := range [][]model.AttributeOrder{nil, ageOrders(false), ageOrders(true)} {
		order, err := sortkey.OffsetOrder(orders)
		require.NoError(t, err)
		want := f.visibleOrder(t, order)
		require.Len(t, want, 12)
		for limit := 1; limit <= 4; limit++ {
			for offset := 0; offset <= len(want)+1; offset++ {
				records, total, err := e.ExecuteDuckDBFederatedQuery(ctx, tieRunTables, pgxQuery(), limit, offset, orders, nil)
				require.NoError(t, err)
				require.Equal(t, want[min(offset, len(want)):min(offset+limit, len(want))], idsOrEmpty(records),
					"orders %v, offset %d, limit %d", orders, offset, limit)
				require.Equal(t, int64(len(want)), total, "orders %v, offset %d, limit %d", orders, offset, limit)
			}
		}
	}
	// Row 3 reads as the snapshot holds it, not as its stale parquet copy.
	records, _, err := e.ExecuteDuckDBFederatedQuery(ctx, tieRunTables, pgxQuery(), 12, 0, nil, nil)
	require.NoError(t, err)
	for _, r := range records {
		if r.RowID == parquetOnlyRowID(3) {
			require.Equal(t, map[string]int32{"integer_01": 31}, r.Int32Items)
		}
	}
	require.Equal(t, f.leg.begins, f.leg.commits, "every pass ends its snapshot")
}

func idsOrEmpty(records []*model.PersistentRecord) []uuid.UUID {
	ids := recordIDs(records)
	if len(ids) == 0 {
		return []uuid.UUID{}
	}
	return ids
}

// The mixed-tier tiebreak walk: one or two rows per page, so page boundaries
// fall inside the tie runs and between a hot and a parquet row, under
// created_at DESC with row_id DESC and ASC, and under an attribute sort with
// its row_id ASC cursor. Each walk returns every row once, in the order the
// rows have once all are flushed (the parquet leg's order).
func TestPgxPassKeysetWalksTieRunsAcrossTiers(t *testing.T) {
	// Each walk starts at a cursor before every row, so every page, the
	// first included, renders the cursor's order: a first page without a
	// cursor renders the offset order instead (row_id ASC).
	type walk struct {
		columns []model.KeysetColumn
		orders  []model.AttributeOrder
		start   []any
	}
	createdAt := func(rowID forma.SortOrder) walk {
		return walk{
			columns: []model.KeysetColumn{{Attribute: "created_at", Direction: forma.SortOrderDesc}, {Attribute: "row_id", Direction: rowID}},
			start:   []any{int64(math.MaxInt64), uuid.Nil},
		}
	}
	cases := map[string]walk{
		"created_at desc, row_id desc": createdAt(forma.SortOrderDesc),
		"created_at desc, row_id asc":  createdAt(forma.SortOrderAsc),
		"age asc, row_id asc": {
			columns: []model.KeysetColumn{{Attribute: "age", Direction: forma.SortOrderAsc}, {Attribute: "row_id", Direction: forma.SortOrderAsc}},
			orders:  ageOrders(false),
			start:   []any{int64(-1), uuid.Nil},
		},
	}
	for name, w := range cases {
		for _, limit := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s/limit=%d", name, limit), func(t *testing.T) {
				f := newPgxFixture(t, tieRunRows())
				e := f.engine(t, []string{f.dir + "/base.parquet"})
				order, err := sortkey.KeysetOrder(normalizedCursor(t, e, w.columns, w.orders))
				require.NoError(t, err)
				require.Equal(t, f.visibleOrder(t, order), walkPgxKeyset(t, e, f, w.columns, w.orders, w.start, limit))
			})
		}
	}
}

// normalizedCursor types a cursor's columns as the engine will, so the test
// can build the Go order the walk must follow.
func normalizedCursor(t *testing.T, e *DBFederatedQueryEngine, columns []model.KeysetColumn, orders []model.AttributeOrder) *model.KeysetCursor {
	t.Helper()
	values := make([]any, len(columns))
	for i, c := range columns {
		if c.Attribute == "row_id" {
			values[i] = uuid.Nil
		} else {
			values[i] = int64(0)
		}
	}
	q := pgxQuery()
	q.KeysetCursor = &model.KeysetCursor{Columns: columns, Values: values, Mode: model.KeysetCursorModeAfter}
	normalized, err := e.keysetQuery(q, orders)
	require.NoError(t, err)
	return normalized.KeysetCursor
}

func walkPgxKeyset(t *testing.T, e *DBFederatedQueryEngine, f *pgxFixture, columns []model.KeysetColumn, orders []model.AttributeOrder, start []any, limit int) []uuid.UUID {
	t.Helper()
	byID := map[uuid.UUID]pgxRow{}
	for _, r := range f.rows {
		byID[r.id()] = r
	}
	var walked []uuid.UUID
	cursor := &model.KeysetCursor{Columns: columns, Values: start, Mode: model.KeysetCursorModeAfter}
	for page := 0; page <= len(f.rows); page++ {
		q := pgxQuery()
		q.KeysetCursor = cursor
		records, _, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, q, limit, 0, orders, nil)
		require.NoError(t, err)
		if len(records) == 0 {
			return walked
		}
		walked = append(walked, recordIDs(records)...)
		last := byID[records[len(records)-1].RowID]
		values := make([]any, len(columns))
		for i, c := range columns {
			values[i] = pgxRowValue(t, last, c.Attribute)
		}
		cursor = &model.KeysetCursor{Columns: columns, Values: values, Mode: model.KeysetCursorModeAfter}
	}
	t.Fatalf("the walk did not end after %d pages", len(f.rows))
	return nil
}

// A keyset page through Query reports the matches after the cursor: the hot
// keys after it plus the parquet count.
func TestPgxPassKeysetTotalCountsTheMatchesAfterTheCursor(t *testing.T) {
	f := newPgxFixture(t, tieRunRows())
	e := f.engine(t, []string{f.dir + "/base.parquet"})
	q := pgxQuery()
	q.KeysetCursor = &model.KeysetCursor{
		Columns: []model.KeysetColumn{{Attribute: "created_at", Direction: forma.SortOrderDesc}, {Attribute: "row_id", Direction: forma.SortOrderAsc}},
		Values:  []any{int64(100), parquetOnlyRowID(6)},
		Mode:    model.KeysetCursorModeAfter,
	}
	records, total, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, q, 2, 0, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{parquetOnlyRowID(7), parquetOnlyRowID(8)}, recordIDs(records))
	require.Equal(t, int64(6), total, "rows 7 and 8, then 1 to 4")
}

// The hot-excluded form reads the strict dirty set, runs no hot-keys query,
// and answers the parquet rows the anti-join keeps.
func TestPgxPassHotExcludedFormReadsTheStrictSet(t *testing.T) {
	f := newPgxFixture(t, tieRunRows())
	e := f.engine(t, []string{f.dir + "/base.parquet"})
	q := pgxQuery()
	q.PreferredTiers = []model.DataTier{model.DataTierWarm, model.DataTierCold}
	records, total, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, q, 20, 0, nil, nil)
	require.NoError(t, err)
	require.Equal(t, DirtySetHotExcluded, f.leg.lastChangeLog.Form)
	require.Nil(t, f.leg.lastHotKeys, "a hot-excluded pass has no hot leg")
	want := []uuid.UUID{parquetOnlyRowID(10), parquetOnlyRowID(11), parquetOnlyRowID(5), parquetOnlyRowID(6), parquetOnlyRowID(8), parquetOnlyRowID(1)}
	require.Equal(t, want, recordIDs(records))
	require.Equal(t, int64(len(want)), total)
}

// The cutoff (#252, design.md §6.1): a negative grace, from the config or
// from WithFlushVisibilityGrace, reads as 0, so the cutoff is the
// path-resolution instant and the widening is never disabled.
func TestPgxPassCutoffNeverDisablesTheWidening(t *testing.T) {
	const resolvedAt = int64(1_700_000_000_000)
	build := func(grace int64) *DBFederatedQueryEngine {
		cfg := hybridDuckConfig()
		cfg.FlushVisibilityGraceMs = grace
		return NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, nil, nil, cfg, nil)
	}
	require.Equal(t, resolvedAt, build(-1).passCutoffMs(resolvedAt))
	require.Equal(t, resolvedAt-250, build(250).passCutoffMs(resolvedAt), "a positive margin applies")
	withOption := NewDBFederatedQueryEngine(&fakePostgresFederatedSource{}, nil, nil,
		forma.DuckDBConfig{}, nil, WithFlushVisibilityGrace(-time.Second))
	require.Equal(t, resolvedAt, withOption.passCutoffMs(resolvedAt))

	// The pass's snapshot reads the widened set at that cutoff.
	f := newPgxFixture(t, tieRunRows())
	e := f.engine(t, []string{f.dir + "/base.parquet"})
	e.flushVisibilityGraceMs = -1
	clock := &fakeClock{t: time.UnixMilli(resolvedAt)}
	e.now = clock.now
	_, _, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 5, 0, nil, nil)
	require.NoError(t, err)
	require.Equal(t, DirtySetHot, f.leg.lastChangeLog.Form)
	require.Equal(t, resolvedAt, f.leg.lastChangeLog.GraceCutoffMs)
}

// Past HotLegMaxDirtyRows the pass fails as a degradable read failure before
// DuckDB runs its statement, and hands the breaker's probe back.
func TestPgxPassRefusesADirtySetPastTheCap(t *testing.T) {
	f := newPgxFixture(t, tieRunRows())
	breaker := NewCircuitBreaker(1, time.Minute, time.Minute)
	e := f.engine(t, []string{f.dir + "/base.parquet"})
	e.breaker = breaker
	e.cfg.HotLegMaxDirtyRows = len(f.leg.dirty) - 1
	_, _, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 5, 0, nil, nil)
	require.ErrorIs(t, err, ErrFederatedReadFailed)
	require.Contains(t, err.Error(), "hotLegMaxDirtyRows")
	require.True(t, degradableFederatedError(err))
	require.Nil(t, f.leg.lastHotKeys, "the pass stops before the hot keys")
	require.False(t, breaker.IsOpen(), "the refusal is not a DuckDB failure")

	e.cfg.HotLegMaxDirtyRows = len(f.leg.dirty)
	records, _, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 5, 0, nil, nil)
	require.NoError(t, err, "a set at the cap is staged")
	require.Len(t, records, 5)
}

// A failed snapshot read is ErrPostgresReadFailed and stays out of the
// breaker, at every read of the snapshot.
func TestPgxPassSnapshotFailuresStayOutOfTheBreaker(t *testing.T) {
	outage := errors.New("connection refused")
	for name, arm := range map[string]func(*fakeHotLeg){
		"begin":     func(l *fakeHotLeg) { l.beginErr = outage },
		"dirty set": func(l *fakeHotLeg) { l.changeLogErr = outage },
		"hot keys":  func(l *fakeHotLeg) { l.hotKeysErr = outage },
	} {
		t.Run(name, func(t *testing.T) {
			f := newPgxFixture(t, tieRunRows())
			breaker := NewCircuitBreaker(1, time.Minute, time.Minute)
			e := f.engine(t, []string{f.dir + "/base.parquet"})
			e.breaker = breaker
			arm(f.leg)
			_, _, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 5, 0, nil, nil)
			require.ErrorIs(t, err, ErrPostgresReadFailed)
			require.ErrorIs(t, err, outage)
			require.False(t, breaker.IsOpen())
			if f.leg.beginErr == nil {
				require.Equal(t, f.leg.begins, f.leg.rollbacks, "a failed pass releases its snapshot")
			}
		})
	}
}

// A pass on an engine built without WithPgxHotLeg is a configuration error,
// not a degradable failure.
func TestPgxPassWithoutASnapshotPoolIsMisconfigured(t *testing.T) {
	f := newPgxFixture(t, tieRunRows())
	e := f.engine(t, []string{f.dir + "/base.parquet"})
	e.hotLeg = nil
	_, _, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 5, 0, nil, nil)
	require.ErrorIs(t, err, errPgxHotLegNotConfigured)
	require.False(t, degradableFederatedError(err))
}
