package federated

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sortkey"
	"github.com/stretchr/testify/require"
)

// The published pass (pgx_publication.go) over the engine-seam fixture: a
// real DuckDB over real parquet, the in-memory snapshot, and a publication
// reader that answers what each test seeds. internal/federatedv2 runs the
// same pass over PostgreSQL publication rows and manifests.

// fakePublication is a PublicationReader answering a seeded record and
// generation.
type fakePublication struct {
	rec    PublicationRecord
	recErr error
	gen    PublishedGeneration
	genErr error
	// missing are the keys MissingIn reports lost, beside probeErr, the
	// probes that failed. probeBlocks makes MissingIn wait for its context
	// to end, and answer its error beside missing: the keys found lost
	// before a probe blocked on another object.
	missing     []string
	probeErr    error
	probeBlocks bool

	reads, resolves, probes int
}

func (f *fakePublication) ReadRecord(_ context.Context, _ pgx.Tx, _ int16) (PublicationRecord, error) {
	f.reads++
	return f.rec, f.recErr
}

func (f *fakePublication) ResolveGeneration(_ context.Context, rec PublicationRecord) (PublishedGeneration, error) {
	f.resolves++
	if rec != f.rec {
		return PublishedGeneration{}, errors.New("resolved a record the reader did not read")
	}
	return f.gen, f.genErr
}

func (f *fakePublication) MissingIn(ctx context.Context, _ PublishedGeneration, _ []string) ([]string, error) {
	f.probes++
	if f.probeBlocks {
		<-ctx.Done()
		return f.missing, ctx.Err()
	}
	return f.missing, f.probeErr
}

// unpublished is an UNPUBLISHED record of the fixture's schema.
func unpublished() *fakePublication {
	return &fakePublication{rec: PublicationRecord{SchemaID: parquetOnlySchemaID, Epoch: 1}}
}

// publishedOver is a PUBLISHED record whose generation lists paths, each
// keyed by its file name.
func publishedOver(paths ...string) *fakePublication {
	gen := PublishedGeneration{SchemaID: parquetOnlySchemaID, Epoch: 1, Generation: 3, Paths: paths, Keys: []string{}}
	for _, p := range paths {
		gen.Keys = append(gen.Keys, filepath.Base(p))
	}
	return &fakePublication{rec: PublicationRecord{SchemaID: parquetOnlySchemaID, Epoch: 1, Generation: 3, Published: true}, gen: gen}
}

// countingDuck counts the DuckDB statements a pass sends, staged or not.
type countingDuck struct {
	inner DuckDBQueryExecutor
	mu    sync.Mutex
	n     int
}

func (c *countingDuck) count() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
}

func (c *countingDuck) statements() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

func (c *countingDuck) Query(ctx context.Context, sqlStr string, args ...any) (duckDBRowsIterator, error) {
	c.count()
	return c.inner.Query(ctx, sqlStr, args...)
}

func (c *countingDuck) queryStaged(ctx context.Context, dirty []uuid.UUID, sqlStr string, args ...any) (duckDBRowsIterator, error) {
	c.count()
	return c.inner.(stagedQuerier).queryStaged(ctx, dirty, sqlStr, args...)
}

// publishedEngine is the fixture's engine with pub as its publication reader.
// Its V1 source answers a glob over every file of the fixture, which a
// published pass never asks for.
func publishedEngine(t *testing.T, f *pgxFixture, pub PublicationReader, opts ...EngineOption) (*DBFederatedQueryEngine, *countingDuck, *fakeParquetSource) {
	t.Helper()
	duck := &countingDuck{inner: NewDuckDBClientQueryExecutor(&DuckDBClient{DB: f.db})}
	v1 := &fakeParquetSource{paths: []string{filepath.Join(f.dir, "*.parquet")}}
	all := append([]EngineOption{WithParquetSource(v1), WithPublicationReader(pub, 0)}, opts...)
	return f.engineOver(t, duck, nil, all...), duck, v1
}

func hotRows(n int) []pgxRow {
	rows := make([]pgxRow, n)
	for i := range rows {
		rows[i] = pgxRow{i: i + 1, createdAt: int64(100 * (i % 3)), age: ageOf(int64(30 + i%4)), hot: true}
	}
	return rows
}

// A never-published schema answers its live rows and its cursor pages from
// PostgreSQL. No DuckDB statement runs, nothing is listed or fetched, and the
// record is the snapshot's first read. The dirty-set cap, which bounds what a
// pass stages in DuckDB, does not apply: every live row of an unpublished
// schema is dirty, and nothing is staged.
func TestPublishedPassOfAnUnpublishedSchemaAnswersFromPostgreSQL(t *testing.T) {
	ctx := context.Background()
	f := newPgxFixture(t, hotRows(7))
	pub := unpublished()
	e, duck, v1 := publishedEngine(t, f, pub)
	e.cfg.HotLegMaxDirtyRows = 2

	order, err := sortkey.OffsetOrder(ageOrders(false))
	require.NoError(t, err)
	want := f.visibleOrder(t, order)
	records, total, err := e.ExecuteDuckDBFederatedQuery(ctx, tieRunTables, pgxQuery(), 4, 2, ageOrders(false), nil)
	require.NoError(t, err)
	require.Equal(t, want[2:6], recordIDs(records))
	require.Equal(t, int64(7), total)
	require.Equal(t, []string{"publication", "change_log", "hot_keys", "hot_rows"}, f.leg.statements)
	require.Equal(t, DirtySetHotExcluded, f.leg.lastChangeLog.Form, "nothing widens a published pass's barrier")
	require.False(t, f.leg.lastChangeLog.ParquetHistory, "no parquet-history probe")
	require.Equal(t, int64(noGraceCutoffMs), f.leg.lastHotKeys.GraceCutoffMs, "the hot keys are the unflushed entries'")

	columns := []model.KeysetColumn{{Attribute: "age", Direction: forma.SortOrderAsc}, {Attribute: "row_id", Direction: forma.SortOrderAsc}}
	keysetOrder, err := sortkey.KeysetOrder(normalizedCursor(t, e, columns, ageOrders(false)))
	require.NoError(t, err)
	require.Equal(t, f.visibleOrder(t, keysetOrder), walkPgxKeyset(t, e, f, columns, ageOrders(false), []any{int64(-1), uuid.Nil}, 3))

	require.Zero(t, duck.statements(), "no DuckDB statement, no glob listing")
	require.Zero(t, v1.pathsCalls, "the V1 path set is never resolved")
	require.Zero(t, pub.resolves+pub.probes, "an unpublished record has no manifest")
	require.Equal(t, f.leg.begins, pub.reads, "one record read per pass")
	require.Equal(t, f.leg.begins, f.leg.commits)
}

// A published generation that lists no file says the parquet tiers hold
// nothing: the hot leg answers, and no DuckDB statement runs.
func TestPublishedPassOfAnEmptyGenerationRunsNoDuckDB(t *testing.T) {
	f := newPgxFixture(t, hotRows(3))
	pub := publishedOver()
	e, duck, _ := publishedEngine(t, f, pub)
	records, total, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 10, 0, nil, nil)
	require.NoError(t, err)
	require.Len(t, records, 3)
	require.Equal(t, int64(3), total)
	require.Equal(t, 1, pub.resolves)
	require.Zero(t, duck.statements())
}

// A published pass scans exactly the objects its generation lists: an object
// beside them, which a glob would have read, stays out of the answer.
func TestPublishedPassScansExactlyTheListedObjects(t *testing.T) {
	ctx := context.Background()
	f := newPgxFixture(t, tieRunRows())
	f.writeParquet(t, "unlisted.parquet", []pgxRow{{i: 40, createdAt: 900, age: ageOf(99), parquet: true}})
	pub := publishedOver(filepath.Join(f.dir, "base.parquet"))
	e, duck, v1 := publishedEngine(t, f, pub)

	order, err := sortkey.OffsetOrder(nil)
	require.NoError(t, err)
	want := f.visibleOrder(t, order)
	records, total, err := e.ExecuteDuckDBFederatedQuery(ctx, tieRunTables, pgxQuery(), 20, 0, nil, nil)
	require.NoError(t, err)
	require.Equal(t, want, recordIDs(records), "the listed object's rows and the hot rows, the unlisted object's never")
	require.Equal(t, int64(len(want)), total)
	require.Positive(t, duck.statements())
	require.Zero(t, v1.pathsCalls)
	require.Equal(t, "publication", f.leg.statements[0], "the record is the snapshot's first read")
	require.Zero(t, pub.probes, "nothing failed, so nothing is probed")
}

// blockingScan lets every statement through but the parquet leg's, which
// blocks until its context ends, and reports the cause it ended with.
type blockingScan struct {
	*countingDuck
	causes chan error
}

func (b *blockingScan) queryStaged(ctx context.Context, _ []uuid.UUID, _ string, _ ...any) (duckDBRowsIterator, error) {
	<-ctx.Done()
	b.causes <- context.Cause(ctx)
	return nil, ctx.Err()
}

// A published pass that runs past R_max is aborted: its running DuckDB scan
// is cancelled, and the engine retries the pass once, on a fresh snapshot. A
// retry that runs out too fails as an expired deadline, which no degraded
// fallback absorbs.
func TestPublishedPassPastItsDeadlineIsAbortedAndRetried(t *testing.T) {
	f := newPgxFixture(t, tieRunRows())
	pub := publishedOver(filepath.Join(f.dir, "base.parquet"))
	e, duck, _ := publishedEngine(t, f, pub, WithPublicationReader(pub, 50*time.Millisecond))
	scan := &blockingScan{countingDuck: duck, causes: make(chan error, 2)}
	e.duck = scan

	start := time.Now()
	_, _, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 5, 0, nil, nil)
	require.ErrorIs(t, err, ErrPassDeadline)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, "retry after the published pass ran past its deadline")
	require.False(t, degradableFederatedError(err))
	require.Less(t, time.Since(start), 10*time.Second)
	require.Equal(t, 2, f.leg.begins, "the retry reads a fresh snapshot")
	require.Equal(t, 2, pub.reads)
	for range 2 {
		require.ErrorIs(t, <-scan.causes, ErrPassDeadline, "the deadline cancelled the running scan")
	}
}

// The request's own deadline is not the pass's: a pass the caller's budget
// ends is not retried.
func TestPublishedPassUnderTheRequestDeadlineIsNotRetried(t *testing.T) {
	f := newPgxFixture(t, tieRunRows())
	pub := publishedOver(filepath.Join(f.dir, "base.parquet"))
	e, duck, _ := publishedEngine(t, f, pub)
	scan := &blockingScan{countingDuck: duck, causes: make(chan error, 2)}
	e.duck = scan

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _, err := e.ExecuteDuckDBFederatedQuery(ctx, tieRunTables, pgxQuery(), 5, 0, nil, nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotErrorIs(t, err, ErrPassDeadline)
	require.Equal(t, 1, f.leg.begins)
}
