package federated

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sortkey"
	"github.com/stretchr/testify/require"
)

// What a published pass asks of DuckDB, and when: its admission waits for a
// generation that lists objects, every listed object must reach DuckDB as
// one concrete object, and the plan reports DuckDB work only when there was
// some.

// openBreaker is a breaker that one failure opens, opened.
func openBreaker() *CircuitBreaker {
	b := NewCircuitBreaker(1, time.Minute, time.Hour)
	b.RecordFailure()
	return b
}

// A pass without a parquet leg needs no DuckDB, so neither an open breaker
// nor a missing DuckDB client refuses it: an unpublished schema and an empty
// generation answer offset and cursor pages from PostgreSQL, through Query
// too, with no degraded fallback, and the breaker stays as it was.
func TestPublishedPassWithoutAParquetLegNeedsNoDuckDBAdmission(t *testing.T) {
	for name, seed := range map[string]func() *fakePublication{
		"unpublished":      unpublished,
		"empty generation": func() *fakePublication { return publishedOver() },
	} {
		for _, noClient := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s, no duckdb client %v", name, noClient), func(t *testing.T) {
				ctx := context.Background()
				f := newPgxFixture(t, hotRows(5))
				pub := seed()
				e, duck := publishedEngine(t, f, pub)
				e.breaker = openBreaker()
				if noClient {
					e.duck = nil
				}
				rec := recordDegradedFallbacks(e)

				order, err := sortkey.OffsetOrder(ageOrders(false))
				require.NoError(t, err)
				want := f.visibleOrder(t, order)
				records, total, err := e.ExecuteDuckDBFederatedQuery(ctx, tieRunTables, pgxQuery(), 2, 1, ageOrders(false), nil)
				require.NoError(t, err)
				require.Equal(t, want[1:3], recordIDs(records))
				require.Equal(t, int64(5), total)

				columns := []model.KeysetColumn{{Attribute: "age", Direction: forma.SortOrderAsc}, {Attribute: "row_id", Direction: forma.SortOrderAsc}}
				keysetOrder, err := sortkey.KeysetOrder(normalizedCursor(t, e, columns, ageOrders(false)))
				require.NoError(t, err)
				require.Equal(t, f.visibleOrder(t, keysetOrder), walkPgxKeyset(t, e, f, columns, ageOrders(false), []any{int64(-1), uuid.Nil}, 2))

				page, err := e.Query(ctx, tieRunTables, pgxQuery(), &model.FederatedQueryOptions{AllowPartialDegradedMode: true, IncludeExecutionPlan: true})
				require.NoError(t, err)
				require.True(t, page.ExecutionPlan.Routing.UseDuckDB, "routed to the federated pass")
				require.Equal(t, int64(5), page.TotalRecords)
				require.Empty(t, rec.take(), "answered by the pass, not by the degraded fallback")
				require.Zero(t, duck.statements())
				require.True(t, e.breaker.IsOpen(), "a pass without DuckDB work tells the breaker nothing")
			})
		}
	}
}

// Under an open breaker a publication fault is still read, and reported, as
// what it is: the record is read before any admission could answer for it
// with a degradable rejection. A generation that lists objects is refused
// once its record is read, with the degradable rejection a V1 pass meets on
// entry.
func TestPublishedPassUnderAnOpenBreaker(t *testing.T) {
	t.Run("a publication fault", func(t *testing.T) {
		f := newPgxFixture(t, tieRunRows())
		pub := &fakePublication{recErr: &PublicationConsistencyError{SchemaID: parquetOnlySchemaID, Epoch: 2, Generation: 5, Err: errors.New("schema dropped")}}
		e, duck := publishedEngine(t, f, pub)
		e.breaker = openBreaker()
		rec := recordDegradedFallbacks(e)

		_, err := e.Query(context.Background(), tieRunTables, pgxQuery(), &model.FederatedQueryOptions{AllowPartialDegradedMode: true})
		require.ErrorIs(t, err, ErrPublicationInconsistent)
		require.NotErrorIs(t, err, ErrDuckDBUnavailable)
		require.Empty(t, rec.take(), "no degraded fallback")
		require.Zero(t, duck.statements())
	})
	t.Run("a generation that lists objects", func(t *testing.T) {
		f := newPgxFixture(t, tieRunRows())
		pub := publishedOver(filepath.Join(f.dir, "base.parquet"))
		e, duck := publishedEngine(t, f, pub)
		e.breaker = openBreaker()

		_, _, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 5, 0, nil, nil)
		require.ErrorIs(t, err, ErrDuckDBUnavailable)
		require.ErrorIs(t, err, errBreakerRejected)
		require.True(t, degradableFederatedError(err))
		require.Equal(t, 1, pub.reads, "the record is read first")
		require.Equal(t, 1, pub.resolves)
		require.Zero(t, duck.statements(), "nothing reaches DuckDB")
	})
}

// A listed address that holds a glob metacharacter would let DuckDB list
// the store and read objects the generation does not list. The pass refuses
// the generation with the typed error before any DuckDB statement, the
// listing included, so the unlisted object the pattern matches is never
// read; nothing is probed, nothing degrades, and the breaker hears nothing.
func TestPublishedPassRefusesAListedAddressDuckDBWouldExpand(t *testing.T) {
	for name, listed := range map[string]string{
		"star":          "*.parquet",
		"question mark": "unliste?.parquet",
		"bracket":       "[u]nlisted.parquet",
	} {
		t.Run(name, func(t *testing.T) {
			f := newPgxFixture(t, tieRunRows())
			f.writeParquet(t, "unlisted.parquet", []pgxRow{{i: 40, createdAt: 900, age: ageOf(99), parquet: true}})
			pub := publishedOver(filepath.Join(f.dir, "base.parquet"), filepath.Join(f.dir, listed))
			e, duck := publishedEngine(t, f, pub)
			e.breaker = NewCircuitBreaker(1, time.Minute, time.Hour)
			rec := recordDegradedFallbacks(e)

			_, err := e.Query(context.Background(), tieRunTables, pgxQuery(), &model.FederatedQueryOptions{AllowPartialDegradedMode: true})
			require.ErrorIs(t, err, ErrPublicationInconsistent)
			require.ErrorIs(t, err, ErrPublishedPathNotConcrete)
			require.ErrorContains(t, err, listed)
			var typed *PublicationConsistencyError
			require.ErrorAs(t, err, &typed)
			require.Equal(t, int64(3), typed.Generation)
			require.Zero(t, duck.statements(), "no listing and no read")
			require.Zero(t, pub.probes)
			require.Empty(t, rec.take())
			require.Empty(t, rec.transitions)
		})
	}
}

// The plan reports the dirty set of a pass without a parquet leg as read,
// never as staged: such a pass runs no DuckDB statement. A pass that scans
// objects reports it staged.
func TestPublishedPassPlanReportsStagingOnlyWithAParquetLeg(t *testing.T) {
	for name, c := range map[string]struct {
		seed   func(f *pgxFixture) *fakePublication
		staged bool
	}{
		"unpublished":      {seed: func(*pgxFixture) *fakePublication { return unpublished() }},
		"empty generation": {seed: func(*pgxFixture) *fakePublication { return publishedOver() }},
		"listed objects": {staged: true, seed: func(f *pgxFixture) *fakePublication {
			return publishedOver(filepath.Join(f.dir, "base.parquet"))
		}},
	} {
		t.Run(name, func(t *testing.T) {
			f := newPgxFixture(t, tieRunRows())
			e, _ := publishedEngine(t, f, c.seed(f))
			opts := &model.FederatedQueryOptions{IncludeExecutionPlan: true}
			_, _, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 5, 0, nil, opts)
			require.NoError(t, err)

			var reasons []string
			for _, s := range opts.ExecutionPlan.Sources {
				if strings.Contains(s.Reason, "dirty set") {
					reasons = append(reasons, s.Reason)
				}
			}
			require.Len(t, reasons, 1, "one dirty-set source: %+v", opts.ExecutionPlan.Sources)
			if c.staged {
				require.Contains(t, reasons[0], "staged for the anti-join")
				return
			}
			require.Equal(t, dirtySetUnstagedReason, reasons[0])
		})
	}
}
