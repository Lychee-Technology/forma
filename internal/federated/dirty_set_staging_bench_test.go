package federated

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/duckdbinit"
	"github.com/stretchr/testify/require"
)

// The two transports the parquet-only template (plan step 3) can receive the
// dirty set through, measured by plan step 1 (#849): staged into a temp
// table through the Appender, or bound as one UUID[] parameter. Each pass
// stages the set and runs the template's anti-join against it, over a fixed
// stand-in for the parquet scan. Half of the dirty IDs have a scanned copy,
// as a dirty row with flushed history does; the other half are new rows.
//
//	go test ./internal/federated -run '^$' -bench BenchmarkDirtySetStaging -benchmem
//
// Neither transport changes the statement text with the set's size, so both
// keep one compiled plan per shape.

// stagingScanRows is the size of the stand-in parquet scan.
const stagingScanRows = 200_000

// stagingFixture is one DuckDB connection holding the scan stand-in. The
// temp table and the query share it, as they would in a pass.
type stagingFixture struct {
	conn *sql.Conn
}

func newStagingFixture(tb testing.TB) *stagingFixture {
	tb.Helper()
	ctx := context.Background()
	db, err := sql.Open("duckdb", "")
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = db.Close() })
	conn, err := db.Conn(ctx)
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = conn.Close() })
	_, err = conn.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE scan_rows AS SELECT gen_random_uuid() AS row_id FROM range(%d)`, stagingScanRows))
	require.NoError(tb, err)
	return &stagingFixture{conn: conn}
}

// dirtySet answers n dirty IDs, n/2 of them with a row in the scan.
func (f *stagingFixture) dirtySet(tb testing.TB, n int) []uuid.UUID {
	tb.Helper()
	rows, err := f.conn.QueryContext(context.Background(), `SELECT row_id FROM scan_rows LIMIT ?`, n/2)
	require.NoError(tb, err)
	defer func() { _ = rows.Close() }()
	ids := make([]uuid.UUID, 0, n)
	for rows.Next() {
		var id duckdb.UUID
		require.NoError(tb, rows.Scan(&id))
		ids = append(ids, uuid.UUID(id))
	}
	require.NoError(tb, rows.Err())
	for len(ids) < n {
		ids = append(ids, uuid.New())
	}
	return ids
}

// antiJoinAppender stages ids into a temp table through the Appender and
// counts the scan rows the anti-join keeps.
func (f *stagingFixture) antiJoinAppender(ctx context.Context, ids []uuid.UUID) (int64, error) {
	if _, err := f.conn.ExecContext(ctx, `CREATE OR REPLACE TEMP TABLE dirty_ids (row_id UUID)`); err != nil {
		return 0, fmt.Errorf("create dirty_ids: %w", err)
	}
	err := f.conn.Raw(func(driverConn any) error {
		app, err := duckdbinit.NewAppender(driverConn, "temp", "main", "dirty_ids")
		if err != nil {
			return err
		}
		for _, id := range ids {
			if err := app.AppendRow(duckdb.UUID(id)); err != nil {
				_ = app.Close()
				return fmt.Errorf("append dirty id: %w", err)
			}
		}
		return app.Close()
	})
	if err != nil {
		return 0, fmt.Errorf("stage dirty ids: %w", err)
	}
	var kept int64
	err = f.conn.QueryRowContext(ctx, `SELECT count(*) FROM scan_rows WHERE row_id NOT IN (SELECT row_id FROM dirty_ids)`).Scan(&kept)
	if err != nil {
		return 0, fmt.Errorf("anti-join staged dirty ids: %w", err)
	}
	return kept, nil
}

// antiJoinListParam binds ids as one UUID[] parameter and counts the scan
// rows the anti-join keeps. duckdb-go binds a []duckdb.UUID as a UUID list;
// it refuses []uuid.UUID and []string, so the conversion is part of the cost.
func (f *stagingFixture) antiJoinListParam(ctx context.Context, ids []uuid.UUID) (int64, error) {
	list := make([]duckdb.UUID, len(ids))
	for i, id := range ids {
		list[i] = duckdb.UUID(id)
	}
	var kept int64
	err := f.conn.QueryRowContext(ctx,
		`SELECT count(*) FROM scan_rows WHERE row_id NOT IN (SELECT unnest(CAST(? AS UUID[])))`, list).Scan(&kept)
	if err != nil {
		return 0, fmt.Errorf("anti-join bound dirty ids: %w", err)
	}
	return kept, nil
}

// Both transports keep the same rows, so the benchmark compares like with like.
func TestDirtySetStagingTransportsAgree(t *testing.T) {
	ctx := context.Background()
	f := newStagingFixture(t)
	for _, n := range []int{0, 1, 1_000} {
		ids := f.dirtySet(t, n)
		want := int64(stagingScanRows - n/2)
		viaAppender, err := f.antiJoinAppender(ctx, ids)
		require.NoError(t, err)
		require.Equal(t, want, viaAppender, "appender, n=%d", n)
		viaParam, err := f.antiJoinListParam(ctx, ids)
		require.NoError(t, err)
		require.Equal(t, want, viaParam, "UUID[] parameter, n=%d", n)
	}
}

func BenchmarkDirtySetStaging(b *testing.B) {
	ctx := context.Background()
	f := newStagingFixture(b)
	for _, n := range []int{1_000, 10_000, 100_000} {
		ids := f.dirtySet(b, n)
		want := int64(stagingScanRows - n/2)
		for name, run := range map[string]func(context.Context, []uuid.UUID) (int64, error){
			"appender":  f.antiJoinAppender,
			"uuid_list": f.antiJoinListParam,
		} {
			b.Run(fmt.Sprintf("%s/n=%d", name, n), func(b *testing.B) {
				for b.Loop() {
					kept, err := run(ctx, ids)
					if err != nil {
						b.Fatal(err)
					}
					if kept != want {
						b.Fatalf("kept %d rows, want %d", kept, want)
					}
				}
			})
		}
	}
}
