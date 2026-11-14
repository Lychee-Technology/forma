package federated

import (
	"fmt"
	"slices"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sortkey"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// The Go merge of a pgx pass (design.md §6.1.1, steps 3 and 5): the hot keys
// sorted by the page's order and filtered by its cursor, the parquet window
// placed among them, and the page cut from the result. Everything here is
// pure, so the placement arithmetic is tested without either store.

// parquetRow is one row of the parquet leg's window: the record DuckDB
// answered, and its sort key as the statement ordered it
// (sqlgen.SortKeyColumn), one value per column of the page's order.
type parquetRow struct {
	record *model.PersistentRecord
	key    []any
}

// pageEntry is one row of the merged page: a hot key, whose record the pass
// reads after the merge, or a parquet record.
type pageEntry struct {
	hot     *model.HotKey
	parquet *model.PersistentRecord
}

// sortHotKeys orders keys as the parquet leg orders its rows. The order ends
// in row_id, so no two keys compare equal and the sort is total.
func sortHotKeys(order sortkey.Order, keys []model.HotKey) {
	slices.SortFunc(keys, func(a, b model.HotKey) int { return order.Compare(a.Key, b.Key) })
}

// admitHotKeys keeps the keys the keyset predicate admits, the predicate the
// parquet leg renders (sortkey.Order.Admits), in their order. A request
// without an active cursor admits every key.
func admitHotKeys(order sortkey.Order, keys []model.HotKey, cursor *model.KeysetCursor) ([]model.HotKey, error) {
	if !cursor.IsActive() {
		return keys, nil
	}
	if err := order.CheckBounds(cursor.Values); err != nil {
		return nil, fmt.Errorf("apply the keyset cursor to the hot keys: %w", err)
	}
	admitted := keys[:0:0]
	for _, k := range keys {
		if order.Admits(k.Key, cursor.Values) {
			admitted = append(admitted, k)
		}
	}
	return admitted, nil
}

// mergeWindow places the parquet window among the sorted hot keys and cuts
// the page [pageOffset, pageOffset+limit) of the merged order.
//
// A parquet row of rank p lands at merged position p plus the number of hot
// keys before it. The window holds the ranks [w, w+n), w = window.Offset, so
// the merge of the window with the hot keys at or after its first row starts
// at merged position w plus the hot keys before that row. Two kinds of hot
// key fall outside what the window can place, and neither can be on the page:
//
//   - When w > 0, a hot key before the window's first row has fewer than w
//     parquet rows and fewer than h hot keys before it, so it lands before
//     w + h = pageOffset (OffsetPageParquetWindow chose w = pageOffset - h).
//     It is dropped, and only counts toward the start position.
//   - A hot key after the window's last row, when the window is full, has at
//     least w + window.Limit = pageOffset + limit rows before it, so the cut
//     drops it. Its position in the merge is a lower bound, which is enough.
//
// A keyset page's window starts at its cursor (w = 0, pageOffset = 0).
func mergeWindow(order sortkey.Order, hot []model.HotKey, rows []parquetRow, window sqlgen.ParquetWindow, pageOffset, limit int) []pageEntry {
	base := 0
	if window.Offset > 0 {
		if len(rows) == 0 {
			// The parquet leg holds at most w rows, so the merged order
			// holds at most w + h = pageOffset rows: the page is empty.
			return nil
		}
		before := 0
		for before < len(hot) && order.Compare(hot[before].Key, rows[0].key) < 0 {
			before++
		}
		hot = hot[before:]
		base = int(window.Offset) + before
	}
	merged := make([]pageEntry, 0, len(hot)+len(rows))
	i, j := 0, 0
	for i < len(hot) || j < len(rows) {
		takeHot := j == len(rows) || (i < len(hot) && order.Compare(hot[i].Key, rows[j].key) < 0)
		if takeHot {
			merged = append(merged, pageEntry{hot: &hot[i]})
			i++
			continue
		}
		merged = append(merged, pageEntry{parquet: rows[j].record})
		j++
	}
	start := pageOffset - base
	if start < 0 {
		// Unreachable: base never exceeds pageOffset (see above). Clamping
		// keeps a broken window from slicing out of range.
		start = 0
	}
	if start >= len(merged) || limit <= 0 {
		return nil
	}
	return merged[start:min(start+limit, len(merged))]
}

// parquetCountKnown reports whether the window's rows carry the parquet
// leg's count. COUNT(*) OVER() rides on every row, so an empty window carries
// none. It is still known to be 0 when the window started at rank 0 and asked
// for rows: the leg had none to give. A window past the last match, or one
// that asked for no row, needs the recount (#181).
func parquetCountKnown(window sqlgen.ParquetWindow, rows int) bool {
	return rows > 0 || (window.Offset == 0 && window.Limit > 0)
}

// checkTiersDisjoint is the merge's guard on the staging (design.md §6.1.1,
// Frozen inventory): the anti-join drops every parquet version of a dirty
// row, so a parquet row whose row_id is dirty means the parquet leg read
// another dirty set than the snapshot's. Answering would hold the row twice.
func checkTiersDisjoint(schemaID int16, dirty []uuid.UUID, rows []parquetRow) error {
	if len(dirty) == 0 || len(rows) == 0 {
		return nil
	}
	set := make(map[uuid.UUID]struct{}, len(dirty))
	for _, id := range dirty {
		set[id] = struct{}{}
	}
	for _, r := range rows {
		if _, ok := set[r.record.RowID]; ok {
			return fmt.Errorf("schema %d: the parquet leg answered row %s, which the pass's snapshot holds as dirty, so the anti-join did not read the snapshot's dirty set: %w", schemaID, r.record.RowID, ErrHotLegNotDisjoint)
		}
	}
	return nil
}
