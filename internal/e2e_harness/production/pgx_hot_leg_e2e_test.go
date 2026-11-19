//go:build e2e

package production

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	forma "github.com/lychee-technology/forma"
	fedengine "github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/model"
)

// The pgx hot leg's end-to-end regressions (pgx-hot-leg-plan.md step 5,
// #856).

// recordTitle reads a record's title as the oracle normalizes it.
func recordTitle(t *testing.T, env *Env, schema SchemaRef, rec *model.PersistentRecord) string {
	t.Helper()
	cache, ok := env.Metadata.GetSchemaCacheByID(schema.ID)
	if !ok {
		t.Fatalf("no metadata for schema %s", schema.Name)
	}
	v, err := actualAttrValue(rec, cache["title"])
	if err != nil {
		t.Fatalf("read title of row %s: %v", rec.RowID, err)
	}
	s, _ := v.(string)
	return s
}

// hookedHotLegReader runs hook once, inside the first pass that reads hot
// keys, after its snapshot is taken and before DuckDB scans.
type hookedHotLegReader struct {
	fedengine.HotLegReader
	once sync.Once
	hook func(ctx context.Context)
}

func (r *hookedHotLegReader) QueryHotKeys(ctx context.Context, tx pgx.Tx, req model.HotKeysQuery) ([]model.HotKey, error) {
	keys, err := r.HotLegReader.QueryHotKeys(ctx, tx, req)
	r.once.Do(func() { r.hook(ctx) })
	return keys, err
}

// A publication after the snapshot (design.md §6.1): between the pass's
// snapshot and its DuckDB scan a writer updates one clean row and flushes it,
// and deletes and flushes another. Each flush publishes a new generation and
// marks its entries in one transaction, after the snapshot read the record,
// so the pass scans the generation its snapshot named and sees neither new
// object nor mark: the page holds the first row at its snapshot version and
// still holds the second.
func TestPgxHotLegPublicationAfterTheSnapshot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1]
	var updated, deleted *Event
	reader := &hookedHotLegReader{hook: func(hctx context.Context) {
		mustApplyEvents(hctx, t, env, "update after the snapshot", UpdateEvent(wide, updated.RowID, map[string]any{"title": "after-snapshot"}))
		mustFlush(hctx, t, env)
		mustApplyEvents(hctx, t, env, "delete after the snapshot", DeleteEvent(wide, deleted.RowID))
		mustFlush(hctx, t, env)
	}}
	env.HotLegReaderWrap = func(inner fedengine.HotLegReader) fedengine.HotLegReader {
		reader.HotLegReader = inner
		return reader
	}

	creates := buildOpenCreates(wide, 4)
	mustApplyEvents(ctx, t, env, "seed", creates...)
	mustFlush(ctx, t, env)
	// One hot row, so the pass reads hot keys and the hook runs.
	mustApplyEvents(ctx, t, env, "hot row", CreateEvent(wide, map[string]any{"title": "hot", "count": float64(1)}))
	updated, deleted = creates[0], creates[1]

	res, err := env.Query(ctx, Query{Schema: wide, Limit: 20, PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold}})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	byID := map[uuid.UUID]*model.PersistentRecord{}
	for _, rec := range res.Records {
		byID[rec.RowID] = rec
	}
	if len(res.Records) != 5 {
		t.Fatalf("page holds %d rows, want the 4 flushed rows and the hot one as of the snapshot", len(res.Records))
	}
	if rec := byID[updated.RowID]; rec == nil || recordTitle(t, env, wide, rec) != "open-00" {
		t.Errorf("the row updated after the snapshot must read at its snapshot version open-00, got %v", rec)
	}
	if byID[deleted.RowID] == nil {
		t.Errorf("the row deleted after the snapshot must still be on the page")
	}
}

// tieRun builds the mixed-tier tiebreak fixture: n rows that tie on
// created_at and count, flushed, then every other row by row_id updated, so
// the run's hot and parquet rows interleave by row_id.
func tieRun(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, n int) {
	t.Helper()
	creates := make([]*Event, n)
	for i := range creates {
		creates[i] = CreateEvent(wide, map[string]any{"title": fmt.Sprintf("tie-%02d", i), "count": float64(7)})
	}
	mustApplyEvents(ctx, t, env, "seed the tie run", creates...)
	env.ExecSQL(ctx, "UPDATE entity_main SET ltbase_created_at = 1700000000000 WHERE ltbase_schema_id = $1", wide.ID)
	mustFlush(ctx, t, env)
	slices.SortFunc(creates, func(a, b *Event) int { return slices.Compare(a.RowID[:], b.RowID[:]) })
	var updates []*Event
	for i := 0; i < n; i += 2 {
		updates = append(updates, UpdateEvent(wide, creates[i].RowID, map[string]any{"title": fmt.Sprintf("tie-%02d-hot", i)}))
	}
	mustApplyEvents(ctx, t, env, "update every other row", updates...)
}

// tieWalk is one way to page through the run.
type tieWalk struct {
	name  string
	sorts []Sort
	// cursor is the keyset cursor's columns; nil walks offset pages.
	cursor []model.KeysetColumn
	start  []any
}

func tieWalks() []tieWalk {
	createdAt := func(rowID forma.SortOrder) []model.KeysetColumn {
		return []model.KeysetColumn{{Attribute: "created_at", Direction: forma.SortOrderDesc}, {Attribute: "row_id", Direction: rowID}}
	}
	count := []model.KeysetColumn{{Attribute: "count", Direction: forma.SortOrderAsc}, {Attribute: "row_id", Direction: forma.SortOrderAsc}}
	return []tieWalk{
		{name: "keyset created_at desc, row_id desc", cursor: createdAt(forma.SortOrderDesc), start: []any{int64(math.MaxInt64), uuid.Nil.String()}},
		{name: "keyset created_at desc, row_id asc", cursor: createdAt(forma.SortOrderAsc), start: []any{int64(math.MaxInt64), uuid.Nil.String()}},
		{name: "keyset count asc, row_id asc", sorts: []Sort{{Attr: "count"}}, cursor: count, start: []any{int64(-1), uuid.Nil.String()}},
		{name: "offset default order"},
		{name: "offset count asc", sorts: []Sort{{Attr: "count"}}},
	}
}

// pageOf walks w with limit rows per page and returns the row IDs in order.
func (w tieWalk) pageOf(ctx context.Context, t *testing.T, env *Env, wide SchemaRef, limit, rows int) []uuid.UUID {
	t.Helper()
	var walked []uuid.UUID
	var cursor *model.KeysetCursor
	if w.cursor != nil {
		cursor = &model.KeysetCursor{Columns: w.cursor, Values: w.start, Mode: model.KeysetCursorModeAfter}
	}
	for page := 0; page <= rows+1; page++ {
		q := Query{Schema: wide, Sorts: w.sorts, Limit: limit, PreferredTiers: allTiers, Keyset: cursor}
		if cursor == nil {
			q.Offset = page * limit
		}
		res := mustQuery(ctx, t, env, q)
		if !res.Plan.Routing.UseDuckDB {
			t.Fatalf("%s: page %d did not route to the federated path: %+v", w.name, page, res.Plan.Routing)
		}
		if len(res.Records) == 0 {
			return walked
		}
		for _, rec := range res.Records {
			walked = append(walked, rec.RowID)
		}
		if w.cursor != nil {
			cursor = tieCursor(w.cursor, res.Records[len(res.Records)-1])
		}
	}
	t.Fatalf("%s: the walk did not end", w.name)
	return nil
}

func tieCursor(cols []model.KeysetColumn, rec *model.PersistentRecord) *model.KeysetCursor {
	vals := make([]any, len(cols))
	for i, c := range cols {
		switch c.Attribute {
		case "created_at":
			vals[i] = rec.CreatedAt
		case "row_id":
			vals[i] = rec.RowID.String()
		case "count":
			vals[i] = int64(7)
		}
	}
	return &model.KeysetCursor{Columns: cols, Values: vals, Mode: model.KeysetCursorModeAfter}
}

// The mixed-tier tiebreak walk (pgx-hot-leg-plan.md step 5): one run of rows
// that tie on every column before row_id, some hot and some in parquet with
// their row_ids interleaved, walked one or two rows per page so that page
// boundaries fall inside the run and between a hot and a parquet row. Each
// walk returns every row exactly once, in the order DuckDB gives the same
// rows once all are flushed.
func TestPgxHotLegMixedTierTiebreakWalk(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1]
	const rows = 7
	tieRun(ctx, t, env, wide, rows)

	walked := map[string][]uuid.UUID{}
	for _, w := range tieWalks() {
		for _, limit := range []int{1, 2} {
			walked[fmt.Sprintf("%s/limit=%d", w.name, limit)] = w.pageOf(ctx, t, env, wide, limit, rows)
		}
	}

	mustFlush(ctx, t, env)
	for _, w := range tieWalks() {
		want := w.pageOf(ctx, t, env, wide, rows+1, rows)
		if len(want) != rows {
			t.Fatalf("%s: the flushed run answers %d rows, want %d", w.name, len(want), rows)
		}
		for _, limit := range []int{1, 2} {
			name := fmt.Sprintf("%s/limit=%d", w.name, limit)
			if !slices.Equal(walked[name], want) {
				t.Errorf("%s: walked %v, want the flushed order %v", name, walked[name], want)
			}
		}
	}
}
