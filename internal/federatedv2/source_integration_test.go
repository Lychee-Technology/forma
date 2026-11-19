package federatedv2

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/publication"
)

// The V2 reader over PostgreSQL publication rows and manifests seeded as a V2
// writer commits them (docs/publication-protocol-v2.md, "Readers"). The
// engine's classification of every outcome is pinned against fakes in
// internal/federated (pgx_publication*_test.go); these tests pin the snapshot
// and the record and manifest semantics underneath it.

var allRows = map[string]row{
	"a": {i: 1, name: "a", age: 31},
	"b": {i: 2, name: "b", age: 32},
	"c": {i: 3, name: "c", age: 33},
	"d": {i: 4, name: "d", age: 34},
	"e": {i: 5, name: "e", age: 35},
}

func query() *model.FederatedAttributeQuery {
	return &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{SchemaID: schemaID}}
}

func ids(records []*model.PersistentRecord) []uuid.UUID {
	out := make([]uuid.UUID, len(records))
	for i, r := range records {
		out[i] = r.RowID
	}
	return out
}

func rowIDs(names ...string) []uuid.UUID {
	out := make([]uuid.UUID, len(names))
	for i, n := range names {
		out[i] = allRows[n].id()
	}
	return out
}

// closedDuckDB is a DuckDB executor any statement fails on: a pass that
// answers through it ran none.
func closedDuckDB(t *testing.T) federated.DuckDBQueryExecutor {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	require.NoError(t, db.Close())
	return federated.NewDuckDBClientQueryExecutor(&federated.DuckDBClient{DB: db})
}

// A schema that was never published answers its live rows, offset and cursor
// pages alike, from PostgreSQL alone: no DuckDB statement runs (the engine's
// DuckDB is closed), and nothing is fetched or probed in storage.
func TestNeverPublishedSchemaIsAnsweredFromPostgreSQL(t *testing.T) {
	f := newFixture(t)
	f.initialize(t, publication.Unpublished)
	for _, r := range allRows {
		f.write(t, r, 100)
	}
	e := f.engine(t, f.source(t, 1), closedDuckDB(t), nil)

	records, total, err := e.ExecuteDuckDBFederatedQuery(f.ctx, f.tables, query(), 3, 1, nil, nil)
	require.NoError(t, err)
	require.Equal(t, rowIDs("d", "c", "b"), ids(records), "created_at DESC, offset 1")
	require.Equal(t, int64(5), total)

	columns := []model.KeysetColumn{{Attribute: "created_at", Direction: forma.SortOrderDesc}, {Attribute: "row_id", Direction: forma.SortOrderAsc}}
	cursor := &model.KeysetCursor{Columns: columns, Values: []any{int64(math.MaxInt64), uuid.Nil}, Mode: model.KeysetCursorModeAfter}
	var walked []uuid.UUID
	for range len(allRows) + 1 {
		q := query()
		q.KeysetCursor = cursor
		page, _, err := e.ExecuteDuckDBFederatedQuery(f.ctx, f.tables, q, 2, 0, nil, nil)
		require.NoError(t, err)
		if len(page) == 0 {
			break
		}
		walked = append(walked, ids(page)...)
		last := page[len(page)-1]
		cursor = &model.KeysetCursor{Columns: columns, Values: []any{last.CreatedAt, last.RowID}, Mode: model.KeysetCursorModeAfter}
	}
	require.Equal(t, rowIDs("e", "d", "c", "b", "a"), walked, "created_at DESC")
	require.Zero(t, f.store.count(), "no manifest fetched")
	require.Zero(t, f.probes, "nothing probed in storage")
}

// hookedSource runs a hook once, right after the pass read the record in its
// snapshot, and remembers which generation each pass resolved.
type hookedSource struct {
	*Source
	afterRead func()
	resolved  []int64
}

func (h *hookedSource) ReadRecord(ctx context.Context, tx pgx.Tx, id int16) (federated.PublicationRecord, error) {
	rec, err := h.Source.ReadRecord(ctx, tx, id)
	if hook := h.afterRead; hook != nil {
		h.afterRead = nil
		hook()
	}
	return rec, err
}

func (h *hookedSource) ResolveGeneration(ctx context.Context, rec federated.PublicationRecord) (federated.PublishedGeneration, error) {
	h.resolved = append(h.resolved, rec.Generation)
	return h.Source.ResolveGeneration(ctx, rec)
}

// hotOnPage is how many of the page's rows the pass read from PostgreSQL, from
// its execution plan.
func hotOnPage(t *testing.T, opts *model.FederatedQueryOptions) int64 {
	t.Helper()
	for _, s := range opts.ExecutionPlan.Sources {
		if s.Tier == model.DataTierHot && s.PredicatePushdown {
			return s.ActualRows
		}
	}
	t.Fatalf("the plan has no hot-leg source: %+v", opts.ExecutionPlan.Sources)
	return 0
}

// A generation committed while a pass is in its snapshot is seen by the next
// pass only. The pass that began first scans the generation its snapshot
// read and serves the newly flushed row from the hot leg, where its snapshot
// still holds it dirty; it neither loses the row nor reads it twice. The next
// pass reads the new generation and the row's mark together, and serves the
// row from parquet.
func TestAGenerationCommittedDuringAPassIsSeenByTheNextPassOnly(t *testing.T) {
	f := newFixture(t)
	f.initialize(t, publication.Unpublished)
	first := map[row]int64{allRows["a"]: 100, allRows["b"]: 100, allRows["c"]: 100}
	for r, v := range first {
		f.write(t, r, v)
	}
	g1 := f.flush(t, first, f.deltaFile(t, 1, first))
	d := map[row]int64{allRows["d"]: 200}
	f.write(t, allRows["d"], 200)

	src := &hookedSource{Source: f.source(t, 1)}
	src.afterRead = func() { f.flush(t, d, f.deltaFile(t, 1, d)) }
	e := f.engine(t, src, nil, nil)
	want := rowIDs("d", "c", "b", "a") // created_at DESC

	opts := &model.FederatedQueryOptions{IncludeExecutionPlan: true}
	records, total, err := e.ExecuteDuckDBFederatedQuery(f.ctx, f.tables, query(), 10, 0, nil, opts)
	require.NoError(t, err)
	require.Equal(t, want, ids(records))
	require.Equal(t, int64(4), total)
	require.Equal(t, []int64{g1.Generation}, src.resolved, "the pass scans the generation its snapshot read")
	require.Equal(t, int64(1), hotOnPage(t, opts), "d is dirty in the pass's snapshot, and served hot")
	require.Equal(t, g1.Generation+1, f.record(t).Generation, "the next generation committed during the pass")

	opts = &model.FederatedQueryOptions{IncludeExecutionPlan: true}
	records, total, err = e.ExecuteDuckDBFederatedQuery(f.ctx, f.tables, query(), 10, 0, nil, opts)
	require.NoError(t, err)
	require.Equal(t, want, ids(records))
	require.Equal(t, int64(4), total)
	require.Equal(t, []int64{g1.Generation, g1.Generation + 1}, src.resolved)
	require.Zero(t, hotOnPage(t, opts), "the next pass reads d's mark and its generation together")
}

// #759's loss, with the manifest present: a generation whose listed objects
// are all gone (the zero match), or one of whose objects is gone (the
// partial match), fails the read with the typed error naming the lost keys,
// never a 200 short of their rows, even under AllowPartialDegradedMode.
func TestAGenerationWithLostObjectsFailsTheRead(t *testing.T) {
	for name, lose := range map[string]func(kept, other manifestv2.File) []string{
		"zero match":    func(kept, other manifestv2.File) []string { return []string{kept.Key, other.Key} },
		"partial match": func(_, other manifestv2.File) []string { return []string{other.Key} },
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			f.initialize(t, publication.Unpublished)
			ab := map[row]int64{allRows["a"]: 100, allRows["b"]: 100}
			c := map[row]int64{allRows["c"]: 100}
			for _, r := range allRows {
				if r.name <= "c" {
					f.write(t, r, 100)
				}
			}
			kept := f.deltaFile(t, 1, ab)
			f.flush(t, ab, kept)
			other := f.deltaFile(t, 1, c)
			g := f.flush(t, c, other)
			lost := lose(kept, other)
			for _, key := range lost {
				require.NoError(t, os.Remove(key))
			}
			breaker := federated.NewCircuitBreaker(1, time.Minute, time.Hour)
			e := f.engine(t, f.source(t, 1), nil, breaker)

			_, err := e.Query(f.ctx, f.tables, query(), &model.FederatedQueryOptions{AllowPartialDegradedMode: true})
			require.ErrorIs(t, err, federated.ErrPublicationInconsistent)
			require.NotErrorIs(t, err, federated.ErrFederatedReadFailed)
			var typed *federated.PublicationConsistencyError
			require.ErrorAs(t, err, &typed)
			require.Equal(t, federated.PublicationConsistencyError{SchemaID: schemaID, Epoch: 1, Generation: g.Generation, MissingKeys: lost},
				federated.PublicationConsistencyError{SchemaID: typed.SchemaID, Epoch: typed.Epoch, Generation: typed.Generation, MissingKeys: typed.MissingKeys})
			require.False(t, breaker.IsOpen(), "a lost object is not DuckDB's failure")
		})
	}
}

// Every record or manifest a reader must fail closed on fails the read with
// the typed error, wrapping the fault, before any object is scanned, and
// neither degrades nor trips a breaker that opens on its first failure.
func TestAReadFailsClosedOnItsRecordOrManifest(t *testing.T) {
	cases := map[string]struct {
		// arrange answers the epoch the process pinned.
		arrange func(t *testing.T, f *fixture) int64
		fault   error
	}{
		"no record": {fault: publication.ErrNoRecord, arrange: func(*testing.T, *fixture) int64 { return 1 }},
		"awaiting init": {fault: publication.ErrAwaitingInit, arrange: func(t *testing.T, f *fixture) int64 {
			f.initialize(t, publication.AwaitingInit)
			return 1
		}},
		"dropped": {fault: publication.ErrDropped, arrange: func(t *testing.T, f *fixture) int64 {
			published(t, f)
			tx, err := f.pool.Begin(f.ctx)
			require.NoError(t, err)
			defer func() { _ = tx.Rollback(f.ctx) }()
			_, err = publication.Reset(f.ctx, publication.PgxTx(tx), f.pub, f.record(t))
			require.NoError(t, err)
			require.NoError(t, tx.Commit(f.ctx))
			return 1
		}},
		"epoch changed": {fault: ErrEpochChanged, arrange: func(t *testing.T, f *fixture) int64 {
			published(t, f)
			return 2
		}},
		"manifest missing": {fault: manifestv2.ErrManifestMissing, arrange: func(t *testing.T, f *fixture) int64 {
			f.store.lost = map[string]bool{published(t, f).ManifestKey: true}
			return 1
		}},
		"manifest corrupt": {fault: publication.ErrManifestInvalid, arrange: func(t *testing.T, f *fixture) int64 {
			f.store.corrupt = map[string]bool{published(t, f).ManifestKey: true}
			return 1
		}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			epoch := c.arrange(t, f)
			breaker := federated.NewCircuitBreaker(1, time.Minute, time.Hour)
			e := f.engine(t, f.source(t, epoch), nil, breaker)

			_, err := e.Query(f.ctx, f.tables, query(), &model.FederatedQueryOptions{AllowPartialDegradedMode: true})
			require.ErrorIs(t, err, federated.ErrPublicationInconsistent)
			require.ErrorIs(t, err, c.fault)
			require.NotErrorIs(t, err, federated.ErrFederatedReadFailed)
			var typed *federated.PublicationConsistencyError
			require.ErrorAs(t, err, &typed)
			require.Equal(t, schemaID, typed.SchemaID)
			require.False(t, breaker.IsOpen())
			require.Zero(t, f.probes, "no object was scanned, so none is probed")
		})
	}
}

// published flushes one row into a first generation and answers the record.
func published(t *testing.T, f *fixture) publication.Record {
	t.Helper()
	f.initialize(t, publication.Unpublished)
	a := map[row]int64{allRows["a"]: 100}
	f.write(t, allRows["a"], 100)
	return f.flush(t, a, f.deltaFile(t, 1, a))
}

// The source refuses a record it did not read, and probes only the objects
// the generation lists.
func TestSourceResolvesOnlyItsOwnRecords(t *testing.T) {
	f := newFixture(t)
	src := f.source(t, 1)
	_, err := src.ResolveGeneration(f.ctx, federated.PublicationRecord{SchemaID: schemaID, Epoch: 1, Generation: 1, Published: true})
	require.ErrorContains(t, err, "not a published record this source read")
	require.False(t, errors.Is(err, federated.ErrPublicationInconsistent))

	gen := federated.PublishedGeneration{SchemaID: schemaID, Epoch: 1, Generation: 1, Paths: []string{"/nowhere/a"}, Keys: []string{"/nowhere/a"}}
	missing, err := src.MissingIn(f.ctx, gen, []string{"/nowhere/a", "/nowhere/unlisted"})
	require.NoError(t, err)
	require.Equal(t, []string{"/nowhere/a"}, missing)
	require.Equal(t, 1, f.probes)
}
