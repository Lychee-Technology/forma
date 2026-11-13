//go:build e2e

package production

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sortkey"
)

// deletedTSCreatedAt is the creation stamp every fixture row shares, so the
// rows tie on created_at across their three encodings.
const deletedTSCreatedAt = int64(1_700_000_000_000)

// deletedTSFixture holds live rows in every encoding of deleted_ts: a
// pre-#274 delta row (NULL deleted_at), a row exported since #274 (0), and a
// hot row (NULL, read from change_log). encoding maps a row_id to its kind.
type deletedTSFixture struct {
	schema   SchemaRef
	encoding map[uuid.UUID]string
	hot      []uuid.UUID
}

// ids returns the fixture's row_ids, ascending, or descending when desc.
func (f *deletedTSFixture) ids(desc bool) []string {
	out := make([]string, 0, len(f.encoding))
	for id := range f.encoding {
		out = append(out, id.String())
	}
	sort.Strings(out)
	if desc {
		for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
			out[i], out[j] = out[j], out[i]
		}
	}
	return out
}

// seedDeletedTSFixture writes three rows of each encoding. The pre-#274 rows
// are flushed alone and their delta object rewritten in place with a NULL
// deleted_at, which is what an exporter before #274 wrote for a live row.
func seedDeletedTSFixture(ctx context.Context, t *testing.T, env *Env) *deletedTSFixture {
	t.Helper()
	f := &deletedTSFixture{schema: DefaultSchemaFixtures()[0], encoding: map[uuid.UUID]string{}}
	create := func(kind string) []*Event {
		events := make([]*Event, 3)
		for i := range events {
			events[i] = CreateEvent(f.schema, map[string]any{"name": fmt.Sprintf("%s-%d", kind, i), "value": float64(i)})
		}
		mustApplyEvents(ctx, t, env, kind+" creates", events...)
		env.ExecSQL(ctx, "UPDATE "+env.Tables.EntityMain+" SET ltbase_created_at = $1 WHERE ltbase_row_id = ANY($2)",
			deletedTSCreatedAt, rowIDs(events))
		for _, id := range rowIDs(events) {
			f.encoding[id] = kind
		}
		return events
	}

	create("pre274")
	mustFlush(ctx, t, env)
	keys := schemaParquetKeys(ctx, t, env, f.schema)
	if len(keys) != 1 {
		t.Fatalf("expected one delta object after the first flush, got %v", keys)
	}
	rewriteDeletedAtAsNull(ctx, t, env, keys[0])

	create("exported")
	mustFlush(ctx, t, env)
	f.hot = rowIDs(create("hot"))
	return f
}

// rewriteDeletedAtAsNull replaces a published delta object's bytes with the
// same rows holding a NULL deleted_at. The manifest's column stamp still
// holds: the column stays BIGINT.
func rewriteDeletedAtAsNull(ctx context.Context, t *testing.T, env *Env, key string) {
	t.Helper()
	tmp := strings.TrimSuffix(key, ".parquet") + "-pre274.parquet"
	writeParquetViaDuck(ctx, t, env, fmt.Sprintf(
		"SELECT * REPLACE (CAST(NULL AS BIGINT) AS deleted_at) FROM read_parquet('s3://%s/%s')", env.Cluster.Bucket, key), tmp)
	putObjectBytes(ctx, t, env, key, fetchObjectBytes(ctx, t, env, tmp))
	deleteObject(ctx, t, env, tmp)
	var nulls int
	q := fmt.Sprintf("SELECT count(*) FROM read_parquet('s3://%s/%s') WHERE deleted_at IS NULL", env.Cluster.Bucket, key)
	if err := env.Duck.DB.QueryRowContext(ctx, q).Scan(&nulls); err != nil || nulls != 3 {
		t.Fatalf("rewritten delta %s: %d NULL deleted_at rows (err %v), want 3", key, nulls, err)
	}
}

func deletedTSCursor(dir forma.SortOrder, boundary int64, rowID uuid.UUID) *model.KeysetCursor {
	return &model.KeysetCursor{
		Columns: []model.KeysetColumn{{Attribute: "deleted_ts", Direction: dir}, {Attribute: "row_id", Direction: dir}},
		Values:  []any{boundary, rowID},
		Mode:    model.KeysetCursorModeAfter,
	}
}

// TestDeletedTSKeysetWalk walks the fixture with cursors on deleted_ts,
// row_id at the boundary 0, one row per page, and flushes the hot rows
// between the second and third pages. The walk returns every row exactly
// once, in row_id order. Before #853 a NULL deleted_ts made the cursor's
// equality arm unknown: the descending walk dropped every hot and every
// pre-#274 row, returning only the exported ones. The ascending walk dropped
// the pre-#274 rows but returned the hot ones, because the mid-walk flush had
// rewritten them as 0 before the cursor reached them.
func TestDeletedTSKeysetWalk(t *testing.T) {
	for _, dir := range []forma.SortOrder{forma.SortOrderAsc, forma.SortOrderDesc} {
		t.Run(string(dir), func(t *testing.T) {
			ctx := context.Background()
			env := NewEnv(t, SharedCluster(t))
			f := seedDeletedTSFixture(ctx, t, env)

			start := uuid.Nil
			if dir == forma.SortOrderDesc {
				start = uuid.Max
			}
			cursor := deletedTSCursor(dir, 0, start)
			var seen []string
			for page := 0; page < 2*len(f.encoding); page++ {
				if page == 2 {
					mustFlush(ctx, t, env)
				}
				res := mustQuery(ctx, t, env, Query{Schema: f.schema, Limit: 1, Keyset: cursor})
				if !res.Plan.Routing.UseDuckDB {
					t.Fatalf("page %d: a keyset page must route to DuckDB, got %+v", page, res.Plan.Routing)
				}
				if len(res.Records) == 0 {
					break
				}
				last := res.Records[0]
				seen = append(seen, last.RowID.String())
				cursor = deletedTSCursor(dir, 0, last.RowID)
			}
			if want := f.ids(dir == forma.SortOrderDesc); !equalStrings(seen, want) {
				t.Fatalf("walk %s = %v, want every row once in row_id order %v (encodings %v)", dir, seen, want, f.encoding)
			}
		})
	}
}

// TestDeletedTSKeysetBoundaries pins design.md §6.1.1's table on the attach
// path, one page per direction and boundary with deleted_ts leading: below 0
// ascending and above 0 descending admit every row, 0 admits the rows past
// the cursor's row_id, and the other side admits none, whatever the row's
// encoding. Each case also holds the page to the Go predicate the hot leg
// will apply. A cursor on created_at, deleted_ts, row_id then returns the
// rows sharing a created_at in row_id order, where the raw column put the
// NULL rows last. Last, the offset form: a sort on deleted_ts is refused
// before either leg runs, and an offset walk with a flush between two pages
// returns every row once.
func TestDeletedTSKeysetBoundaries(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	f := seedDeletedTSFixture(ctx, t, env)
	all := f.ids(false)
	pivot := uuid.MustParse(all[len(all)/2])

	for _, tc := range []struct {
		dir      forma.SortOrder
		boundary int64
		want     func(id string) bool
	}{
		{forma.SortOrderAsc, -1, func(string) bool { return true }},
		{forma.SortOrderAsc, 0, func(id string) bool { return id > pivot.String() }},
		{forma.SortOrderAsc, 1, func(string) bool { return false }},
		{forma.SortOrderDesc, 1, func(string) bool { return true }},
		{forma.SortOrderDesc, 0, func(id string) bool { return id < pivot.String() }},
		{forma.SortOrderDesc, -1, func(string) bool { return false }},
	} {
		label := fmt.Sprintf("%s at %d", tc.dir, tc.boundary)
		cursor := deletedTSCursor(tc.dir, tc.boundary, pivot)
		res := mustQuery(ctx, t, env, Query{Schema: f.schema, Limit: 100, Keyset: cursor})
		got := pageRowIDs(res)
		var want []string
		for _, id := range f.ids(tc.dir == forma.SortOrderDesc) {
			if tc.want(id) {
				want = append(want, id)
			}
		}
		if !equalStrings(got, want) {
			t.Fatalf("%s: page %v, want %v (encodings %v)", label, got, want, f.encoding)
		}
		requireEncodingsAdmitted(t, label, f, got)
		requireGoPredicateAgrees(t, label, f, cursor, got)
	}

	ordered := &model.KeysetCursor{
		Columns: []model.KeysetColumn{
			{Attribute: "created_at", Direction: forma.SortOrderAsc},
			{Attribute: "deleted_ts", Direction: forma.SortOrderAsc},
			{Attribute: "row_id", Direction: forma.SortOrderAsc},
		},
		Values: []any{deletedTSCreatedAt - 1, int64(0), uuid.Nil},
		Mode:   model.KeysetCursorModeAfter,
	}
	if got := pageRowIDs(mustQuery(ctx, t, env, Query{Schema: f.schema, Limit: 100, Keyset: ordered})); !equalStrings(got, all) {
		t.Fatalf("created_at, deleted_ts, row_id: page %v, want the shared-created_at rows in row_id order %v", got, all)
	}

	assertDeletedTSOffsetForm(ctx, t, env, f)
}

// requireEncodingsAdmitted names the NULL-encoded rows: a page that admits
// every row holds the pre-#274 and hot rows beside the exported ones, and a
// page at the boundary 0 holds at least one of them, so the case reaches
// the rows the rule changes.
func requireEncodingsAdmitted(t *testing.T, label string, f *deletedTSFixture, got []string) {
	t.Helper()
	if len(got) == 0 {
		return
	}
	kinds := map[string]int{}
	for _, id := range got {
		kinds[f.encoding[uuid.MustParse(id)]]++
	}
	if len(got) == len(f.encoding) {
		for _, kind := range []string{"pre274", "exported", "hot"} {
			if kinds[kind] == 0 {
				t.Fatalf("%s: no %s row on the page %v", label, kind, got)
			}
		}
		return
	}
	if kinds["pre274"]+kinds["hot"] == 0 {
		t.Fatalf("%s: the page %v holds no NULL-encoded row, so it does not test the rule (encodings %v)", label, got, f.encoding)
	}
}

// requireGoPredicateAgrees applies the hot leg's predicate to every fixture
// row, each keyed as the hot leg derives it: deleted_ts 0, then row_id.
func requireGoPredicateAgrees(t *testing.T, label string, f *deletedTSFixture, cursor *model.KeysetCursor, got []string) {
	t.Helper()
	normalized, err := sortkey.NormalizeCursor(cursor, nil, nil)
	if err != nil {
		t.Fatalf("%s: normalize: %v", label, err)
	}
	order, err := sortkey.KeysetOrder(normalized)
	if err != nil {
		t.Fatalf("%s: order: %v", label, err)
	}
	admitted := map[string]bool{}
	for id := range f.encoding {
		deleted, err := sortkey.DeriveSystem("deleted_ts", nil)
		if err != nil {
			t.Fatalf("%s: derive: %v", label, err)
		}
		if order.Admits([]any{deleted, id}, normalized.Values) {
			admitted[id.String()] = true
		}
	}
	if len(admitted) != len(got) {
		t.Fatalf("%s: the Go predicate admits %d rows, the page holds %d (%v)", label, len(admitted), len(got), got)
	}
	for _, id := range got {
		if !admitted[id] {
			t.Fatalf("%s: the page holds %s, which the Go predicate refuses", label, id)
		}
	}
}

// assertDeletedTSOffsetForm: offset pages cannot sort on deleted_ts, and an
// offset walk with a flush between two pages returns every row once. The
// rows tie on created_at, so the default order is row_id ASC.
func assertDeletedTSOffsetForm(ctx context.Context, t *testing.T, env *Env, f *deletedTSFixture) {
	t.Helper()
	_, err := env.EntityManager().Query(ctx, &forma.QueryRequest{
		SchemaName: f.schema.Name,
		Sort:       []forma.OrderBy{{Attribute: "deleted_ts"}},
		Federated:  &forma.FederatedQueryRequest{Enabled: true},
	})
	if !errors.Is(err, forma.ErrInvalidInput) {
		t.Fatalf("a federated sort on deleted_ts: err %v, want an invalid-input refusal", err)
	}

	var seen []string
	for offset := 0; offset < 2*len(f.encoding); offset += 2 {
		if offset == 4 {
			mustFlush(ctx, t, env)
		}
		res := mustQuery(ctx, t, env, Query{Schema: f.schema, Limit: 2, Offset: offset, PreferredTiers: []model.DataTier{
			model.DataTierHot, model.DataTierWarm, model.DataTierCold}})
		if len(res.Records) == 0 {
			break
		}
		seen = append(seen, pageRowIDs(res)...)
	}
	if want := f.ids(false); !equalStrings(seen, want) {
		t.Fatalf("offset walk = %v, want every row once %v", seen, want)
	}
}
