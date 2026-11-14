package federated

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sortkey"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/stretchr/testify/require"
)

// The merge's placement arithmetic (design.md §6.1, Pagination and totals),
// checked against a full sort of both legs: for every offset and limit, the
// page mergeWindow cuts from the window OffsetPageParquetWindow chooses is
// the same slice of the full merged order, and the count h + parquet total
// is its length.

// mergeTestOrder is the default offset order, created_at DESC, row_id ASC.
var mergeTestOrder = sortkey.Order{Columns: []sortkey.Column{
	{Name: sortkey.ColumnCreatedAt, Type: sortkey.BigInt, Desc: true},
	{Name: sortkey.ColumnRowID, Type: sortkey.UUID},
}}

type mergeTestRow struct {
	id  uuid.UUID
	key []any
	hot bool
}

// mergeTestRows draws n rows whose created_at values collide often, so runs
// that tie on every key before row_id cross the two legs.
func mergeTestRows(r *rand.Rand, n int) []mergeTestRow {
	rows := make([]mergeTestRow, n)
	for i := range rows {
		id := uuid.New()
		rows[i] = mergeTestRow{id: id, key: []any{int64(r.Intn(n/3 + 1)), id}, hot: r.Intn(3) == 0}
	}
	return rows
}

// split sorts rows and returns the full order, the hot keys and the parquet
// leg in its own order.
func splitMergeTestRows(order sortkey.Order, rows []mergeTestRow) (all []uuid.UUID, hot []model.HotKey, parquet []parquetRow) {
	slices.SortFunc(rows, func(a, b mergeTestRow) int { return order.Compare(a.key, b.key) })
	for _, row := range rows {
		all = append(all, row.id)
		if row.hot {
			hot = append(hot, model.HotKey{RowID: row.id, Key: row.key})
			continue
		}
		parquet = append(parquet, parquetRow{record: &model.PersistentRecord{RowID: row.id}, key: row.key})
	}
	return all, hot, parquet
}

func pageIDs(entries []pageEntry) []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(entries))
	for _, e := range entries {
		if e.hot != nil {
			ids = append(ids, e.hot.RowID)
			continue
		}
		ids = append(ids, e.parquet.RowID)
	}
	return ids
}

// parquetWindowRows is what the parquet leg answers for window: its ranks
// [Offset, Offset+Limit).
func parquetWindowRows(parquet []parquetRow, w sqlgen.ParquetWindow) []parquetRow {
	start := min(int(w.Offset), len(parquet))
	end := min(int(w.Offset+w.Limit), len(parquet))
	return parquet[start:end]
}

func TestMergeWindowCutsTheFullOrdersPage(t *testing.T) {
	r := rand.New(rand.NewSource(856))
	for trial := 0; trial < 60; trial++ {
		rows := mergeTestRows(r, 1+r.Intn(30))
		all, hot, parquet := splitMergeTestRows(mergeTestOrder, rows)
		for limit := 0; limit <= 6; limit++ {
			for offset := 0; offset <= len(all)+2; offset++ {
				window, err := sqlgen.OffsetPageParquetWindow(offset, limit, len(hot))
				require.NoError(t, err)
				got := pageIDs(mergeWindow(mergeTestOrder, hot, parquetWindowRows(parquet, window), window, offset, limit))
				want := all[min(offset, len(all)):min(offset+limit, len(all))]
				if len(want) == 0 {
					want = nil
				}
				if len(got) == 0 {
					got = nil
				}
				require.Equal(t, want, got, "trial %d: %d rows (%d hot), offset %d, limit %d", trial, len(all), len(hot), offset, limit)
			}
		}
	}
}

// A keyset page applies the cursor to both legs and takes limit rows of the
// merge, in either row_id direction.
func TestMergeWindowWalksKeysetPagesInBothRowIDDirections(t *testing.T) {
	r := rand.New(rand.NewSource(8560))
	for _, rowIDDesc := range []bool{false, true} {
		order := sortkey.Order{Columns: []sortkey.Column{
			{Name: sortkey.ColumnCreatedAt, Type: sortkey.BigInt, Desc: true},
			{Name: sortkey.ColumnRowID, Type: sortkey.UUID, Desc: rowIDDesc},
		}}
		for trial := 0; trial < 30; trial++ {
			rows := mergeTestRows(r, 1+r.Intn(25))
			all, hot, parquet := splitMergeTestRows(order, rows)
			for limit := 1; limit <= 3; limit++ {
				t.Run(fmt.Sprintf("desc=%t/trial=%d/limit=%d", rowIDDesc, trial, limit), func(t *testing.T) {
					walked := walkKeyset(t, order, hot, parquet, limit)
					require.Equal(t, all, walked)
				})
			}
		}
	}
}

// walkKeyset pages through the merge with a cursor taken from each page's
// last row, as a caller does.
func walkKeyset(t *testing.T, order sortkey.Order, hot []model.HotKey, parquet []parquetRow, limit int) []uuid.UUID {
	t.Helper()
	keys := map[uuid.UUID][]any{}
	for _, h := range hot {
		keys[h.RowID] = h.Key
	}
	for _, p := range parquet {
		keys[p.record.RowID] = p.key
	}
	var walked []uuid.UUID
	var bounds []any
	for page := 0; page <= len(keys); page++ {
		admittedHot, admittedParquet := hot, parquet
		if bounds != nil {
			admittedHot = nil
			for _, h := range hot {
				if order.Admits(h.Key, bounds) {
					admittedHot = append(admittedHot, h)
				}
			}
			admittedParquet = nil
			for _, p := range parquet {
				if order.Admits(p.key, bounds) {
					admittedParquet = append(admittedParquet, p)
				}
			}
		}
		window, err := sqlgen.KeysetPageParquetWindow(limit)
		require.NoError(t, err)
		ids := pageIDs(mergeWindow(order, admittedHot, parquetWindowRows(admittedParquet, window), window, 0, limit))
		if len(ids) == 0 {
			return walked
		}
		walked = append(walked, ids...)
		bounds = keys[ids[len(ids)-1]]
	}
	t.Fatalf("the keyset walk did not end after %d pages", len(keys))
	return nil
}

func TestParquetCountKnown(t *testing.T) {
	require.True(t, parquetCountKnown(sqlgen.ParquetWindow{Offset: 4, Limit: 2}, 1), "a row carries the count")
	require.True(t, parquetCountKnown(sqlgen.ParquetWindow{Offset: 0, Limit: 2}, 0), "a window from rank 0 that found nothing counts 0")
	require.False(t, parquetCountKnown(sqlgen.ParquetWindow{Offset: 4, Limit: 2}, 0), "a window past the last match carries no count (#181)")
	require.False(t, parquetCountKnown(sqlgen.ParquetWindow{Offset: 0, Limit: 0}, 0), "a window that asked for no row carries no count")
}

func TestCheckTiersDisjointRefusesADirtyParquetRow(t *testing.T) {
	dirty := uuid.New()
	rows := []parquetRow{{record: &model.PersistentRecord{RowID: uuid.New()}}, {record: &model.PersistentRecord{RowID: dirty}}}
	require.NoError(t, checkTiersDisjoint(3, []uuid.UUID{uuid.New()}, rows))
	err := checkTiersDisjoint(3, []uuid.UUID{dirty}, rows)
	require.ErrorIs(t, err, ErrHotLegNotDisjoint)
	require.Contains(t, err.Error(), dirty.String())
	require.False(t, degradableFederatedError(err), "an overlap is an internal inconsistency, not a degradable failure")
}

func TestAdmitHotKeysAppliesTheCursorPredicate(t *testing.T) {
	a, b, c := uuid.MustParse("00000000-0000-0000-0000-00000000000a"), uuid.MustParse("00000000-0000-0000-0000-00000000000b"), uuid.MustParse("00000000-0000-0000-0000-00000000000c")
	keys := []model.HotKey{{RowID: a, Key: []any{int64(5), a}}, {RowID: b, Key: []any{int64(5), b}}, {RowID: c, Key: []any{int64(4), c}}}
	cursor := &model.KeysetCursor{Columns: []model.KeysetColumn{{Attribute: "created_at"}, {Attribute: "row_id"}}, Values: []any{int64(5), a}}
	admitted, err := admitHotKeys(mergeTestOrder, keys, cursor)
	require.NoError(t, err)
	require.Equal(t, []uuid.UUID{b, c}, pageIDs(hotEntries(admitted)))

	all, err := admitHotKeys(mergeTestOrder, keys, nil)
	require.NoError(t, err)
	require.Len(t, all, 3, "no cursor admits every key")

	_, err = admitHotKeys(mergeTestOrder, keys, &model.KeysetCursor{Columns: cursor.Columns, Values: []any{nil, a}})
	require.Error(t, err, "a nil bound cannot reach the predicate")
}

func hotEntries(keys []model.HotKey) []pageEntry {
	out := make([]pageEntry, len(keys))
	for i := range keys {
		out[i] = pageEntry{hot: &keys[i]}
	}
	return out
}
