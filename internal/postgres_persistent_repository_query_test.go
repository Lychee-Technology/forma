package internal

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/queryplan"
	"github.com/lychee-technology/forma/internal/schemameta"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRunOptimizedQueryEmptyPageRecountsTotal pins the #181 fallback: a page
// at or beyond the last match returns zero rows (the window count travels on
// data rows), so the repository must recount at limit 1, offset 0 and report
// the true total instead of 0.
func TestRunOptimizedQueryEmptyPageRecountsTotal(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()
	mock.MatchExpectationsInOrder(true)

	repo := NewDBPersistentRecordRepository(mock, nil)
	tables := model.StorageTables{EntityMain: "main_table", EAVData: "eav_table"}

	columns, values := optimizedQueryFixtureColumnsAndValues(
		uuid.MustParse("11111111-1111-1111-1111-111111111111"), int64(25))

	// Deep page: offset past the 25 matches returns no rows.
	mock.ExpectQuery("WITH anchor").
		WithArgs(int16(1), 10, 30).
		WillReturnRows(pgxmock.NewRows(columns))
	// Fallback recount: same shape at limit 1, offset 0 carries the count back.
	mock.ExpectQuery("WITH anchor").
		WithArgs(int16(1), 1, 0).
		WillReturnRows(pgxmock.NewRows(columns).AddRow(values...))

	records, total, err := repo.runOptimizedQuery(ctx, tables, 1, "1=1", nil, 10, 30, nil, true)
	require.NoError(t, err)
	assert.Empty(t, records)
	assert.Equal(t, int64(25), total)

	require.NoError(t, mock.ExpectationsWereMet())
}

// TestRunOptimizedQueryEmptyResultAtOffsetZeroSkipsRecount guards the other
// half of the #181 condition: with offset 0 an empty result is a genuine
// total of 0, and the fallback must not burn a second query.
func TestRunOptimizedQueryEmptyResultAtOffsetZeroSkipsRecount(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	repo := NewDBPersistentRecordRepository(mock, nil)
	tables := model.StorageTables{EntityMain: "main_table", EAVData: "eav_table"}

	columns, _ := optimizedQueryFixtureColumnsAndValues(uuid.New(), 0)
	mock.ExpectQuery("WITH anchor").
		WithArgs(int16(1), 10, 0).
		WillReturnRows(pgxmock.NewRows(columns))

	records, total, err := repo.runOptimizedQuery(ctx, tables, 1, "1=1", nil, 10, 0, nil, true)
	require.NoError(t, err)
	assert.Empty(t, records)
	assert.Equal(t, int64(0), total)

	require.NoError(t, mock.ExpectationsWereMet())
}

// renderKeyFixture is one StreamOptimizedQuery call whose render-key inputs
// are all off the defaults the older render-cache tests use (schema 1, main
// anchor, no sort keys, no metadata cache). A key that drops or hard-codes
// any input then misses the entry these tests seed under the expected key
// (#441).
type renderKeyFixture struct {
	mc       *schemameta.MetadataCache
	tables   model.StorageTables
	schemaID int16
	clause   string
	args     []any
	orders   []model.AttributeOrder
	anchor   bool
}

func newRenderKeyFixture(t *testing.T) renderKeyFixture {
	t.Helper()
	mc := schemameta.NewMetadataCache()
	require.NoError(t, mc.RegisterSchema("orders", 7, forma.SchemaAttributeCache{
		"total": {AttributeID: 5, ValueType: forma.ValueTypeInteger,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("integer_01")}},
	}))
	return renderKeyFixture{
		mc:       mc,
		tables:   model.StorageTables{EntityMain: "main_t", EAVData: "eav_t", ChangeLog: "cl_t"},
		schemaID: 7,
		clause:   `m."integer_01" > $2`,
		args:     []any{int64(5)},
		orders: []model.AttributeOrder{{AttrID: 5, AttrName: "total", ColumnName: "integer_01",
			StorageLocation: forma.AttributeStorageLocationMain, SortOrder: forma.SortOrderDesc, ValueType: forma.ValueTypeInteger}},
		anchor: false,
	}
}

// expectedKey builds the key the call must look up without going through
// renderOptimizedQuerySQL.
func (f renderKeyFixture) expectedKey(t *testing.T) queryplan.Key {
	t.Helper()
	fingerprint, ok := f.mc.SchemaFingerprint(f.schemaID)
	require.True(t, ok)
	require.NotEmpty(t, fingerprint, "an empty fingerprint would leave SchemaVersion unpinned")
	return queryplan.Key{
		Kind:          "postgres_optimized_template",
		SchemaVersion: fingerprint,
		SchemaID:      f.schemaID,
		ShapeHash:     strconv.FormatUint(optimizedQueryShapeKey(f.tables, f.anchor, f.clause, len(f.args), f.orders), 16),
	}
}

func (f renderKeyFixture) stream(ctx context.Context, repo *DBPersistentRecordRepository) (int64, error) {
	return repo.StreamOptimizedQuery(ctx, f.tables, f.schemaID, f.clause, f.args, 10, 0, f.orders, f.anchor, nil)
}

// TestStreamOptimizedQueryRenderKeyPin pins #441: every field of the render
// key is derived from the call's own inputs. The cache is seeded with a
// sentinel query under the expected key, and the call must execute that
// sentinel. The count-based render-cache tests pass under any injective key
// transform, so they cannot catch a key that ignores the anchor choice, the
// sort keys or the schema fingerprint; two shapes would then share one SQL.
func TestStreamOptimizedQueryRenderKeyPin(t *testing.T) {
	ctx := context.Background()
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	f := newRenderKeyFixture(t)
	repo := NewDBPersistentRecordRepository(mock, f.mc)
	const sentinel = "SELECT 'render key sentinel' AS sentinel"
	_, _, err = repo.planCache.GetOrBuild(ctx, f.expectedKey(t), func() (any, error) { return sentinel, nil })
	require.NoError(t, err)

	mock.ExpectQuery(regexp.QuoteMeta(sentinel)).
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnRows(pgxmock.NewRows([]string{"sentinel"}))
	_, err = f.stream(ctx, repo)
	require.NoError(t, err, "the call must look up the key built from its own inputs")

	hits, _ := repo.planCache.Stats()
	require.Equal(t, int64(1), hits)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestStreamOptimizedQueryBuildErrorWrappedOnce pins #441's error path: a
// failed render-cache lookup reaches the caller prefixed "build optimized
// query: " exactly once, with the cause still matchable. The template render
// cannot fail on the repository's own parameters, so the lookup is failed
// the way production can fail it: the call waits on another request's
// in-flight build of the same key and its context is already cancelled.
func TestStreamOptimizedQueryBuildErrorWrappedOnce(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	f := newRenderKeyFixture(t)
	repo := NewDBPersistentRecordRepository(mock, f.mc)
	key := f.expectedKey(t)

	started := make(chan struct{})
	release := make(chan struct{})
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		_, _, _ = repo.planCache.GetOrBuild(context.Background(), key, func() (any, error) {
			close(started)
			<-release
			return nil, errors.New("synthetic leader build")
		})
	}()
	<-started
	defer func() {
		close(release)
		select {
		case <-leaderDone:
		case <-time.After(5 * time.Second):
			t.Fatal("leader did not finish after release")
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = f.stream(ctx, repo)
	require.ErrorIs(t, err, context.Canceled)
	msg := err.Error()
	require.True(t, strings.HasPrefix(msg, "build optimized query: "), "unexpected error text: %s", msg)
	require.Equal(t, 1, strings.Count(msg, "build optimized query"), "context must be added exactly once: %s", msg)
	require.NoError(t, mock.ExpectationsWereMet(), "no query may reach the database")
}
