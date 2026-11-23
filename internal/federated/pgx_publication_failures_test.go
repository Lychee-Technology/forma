package federated

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// How a published pass classifies its failures. A publication fault is a
// read-path consistency error: never ErrFederatedReadFailed, never absorbed
// by the degraded fallback, never charged to the circuit breaker.

// publicationFault is one way a published read can be inconsistent.
type publicationFault struct {
	name string
	// seed arranges the fault over the fixture and answers the reader.
	seed func(t *testing.T, f *pgxFixture) *fakePublication
	// lost are the keys the error must name as missing.
	lost []string
}

func publicationFaults() []publicationFault {
	recordFault := func(err error) func(*testing.T, *pgxFixture) *fakePublication {
		return func(*testing.T, *pgxFixture) *fakePublication {
			return &fakePublication{recErr: &PublicationConsistencyError{SchemaID: parquetOnlySchemaID, Epoch: 2, Generation: 5, Err: err}}
		}
	}
	return []publicationFault{
		{name: "no record", seed: recordFault(errors.New("publication record missing"))},
		{name: "awaiting init", seed: recordFault(errors.New("schema awaits a cdc-init"))},
		{name: "dropped", seed: recordFault(errors.New("schema dropped"))},
		{name: "epoch changed", seed: recordFault(errors.New("publication epoch differs from the process's pin"))},
		{name: "manifest missing or invalid", seed: func(t *testing.T, f *pgxFixture) *fakePublication {
			pub := publishedOver(filepath.Join(f.dir, "base.parquet"))
			pub.genErr = &PublicationConsistencyError{SchemaID: parquetOnlySchemaID, Epoch: 1, Generation: 3, Err: errors.New("manifest object missing")}
			return pub
		}},
		// #759's zero-match loss: the manifest is there, and none of the
		// objects it lists is.
		{name: "every listed object missing", lost: []string{"base.parquet"}, seed: func(t *testing.T, f *pgxFixture) *fakePublication {
			path := filepath.Join(f.dir, "base.parquet")
			require.NoError(t, os.Remove(path))
			pub := publishedOver(path)
			pub.missing = []string{"base.parquet"}
			return pub
		}},
		// #759's partial-match loss: one listed object of two is gone, and
		// the other would answer a short page.
		{name: "a listed object missing", lost: []string{"gone.parquet"}, seed: partialLoss},
		// A lost object stays lost when the probe of another failed: the
		// read fails with the typed error, not as a failed scan the
		// breaker would be charged for and a fallback would absorb.
		{name: "a listed object missing beside a failed probe", lost: []string{"gone.parquet"}, seed: func(t *testing.T, f *pgxFixture) *fakePublication {
			pub := partialLoss(t, f)
			pub.probeErr = errors.New("probe object base.parquet: HeadObject answered 503")
			return pub
		}},
	}
}

// partialLoss is a generation of two listed objects, one of which the store
// lost after the manifest stamped it.
func partialLoss(t *testing.T, f *pgxFixture) *fakePublication {
	gone := f.writeParquet(t, "gone.parquet", []pgxRow{{i: 40, createdAt: 900, age: ageOf(40), parquet: true}})
	pub := publishedOver(filepath.Join(f.dir, "base.parquet"), gone)
	// The stamps let the pre-read checks pass without the object, so the
	// scan itself finds it gone.
	pub.gen.Columns = map[string]map[string]string{}
	for _, p := range pub.gen.Paths {
		pub.gen.Columns[p] = parquetFooterColumns(t, f, p)
	}
	require.NoError(t, os.Remove(gone))
	pub.missing = []string{"gone.parquet"}
	return pub
}

// parquetFooterColumns reads a fixture object's columns as a manifest stamps
// them.
func parquetFooterColumns(t *testing.T, f *pgxFixture, path string) map[string]string {
	t.Helper()
	rows, err := f.db.Query(`SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet('` + path + `'))`)
	require.NoError(t, err)
	defer func() { require.NoError(t, rows.Close()) }()
	cols := map[string]string{}
	for rows.Next() {
		var name, typ string
		require.NoError(t, rows.Scan(&name, &typ))
		cols[name] = typ
	}
	require.NoError(t, rows.Err())
	return cols
}

// Each fault fails the read with the typed error, which names the schema,
// the epoch and the generation, and the lost keys when objects are lost. The
// read fails even under AllowPartialDegradedMode: no fallback runs, the
// degraded-fallback counter stays at zero, and a breaker that opens on its
// first failure stays closed.
func TestPublishedPassFailsClosedOnAPublicationFault(t *testing.T) {
	for _, fault := range publicationFaults() {
		t.Run(fault.name, func(t *testing.T) {
			f := newPgxFixture(t, tieRunRows())
			pub := fault.seed(t, f)
			e, _ := publishedEngine(t, f, pub)
			e.breaker = NewCircuitBreaker(1, time.Minute, time.Hour)
			rec := recordDegradedFallbacks(e)

			_, err := e.Query(context.Background(), tieRunTables, pgxQuery(), &model.FederatedQueryOptions{AllowPartialDegradedMode: true})
			require.Error(t, err, "never an answer without the lost rows")
			require.ErrorIs(t, err, ErrPublicationInconsistent)
			require.NotErrorIs(t, err, ErrFederatedReadFailed)
			require.False(t, degradableFederatedError(err))
			var typed *PublicationConsistencyError
			require.ErrorAs(t, err, &typed)
			require.Equal(t, parquetOnlySchemaID, typed.SchemaID)
			require.Positive(t, typed.Epoch)
			require.Positive(t, typed.Generation)
			require.Equal(t, fault.lost, typed.MissingKeys)
			require.ErrorContains(t, err, fmt.Sprintf("schema %d epoch %d generation %d", typed.SchemaID, typed.Epoch, typed.Generation))

			require.Empty(t, rec.take(), "no degraded fallback")
			require.Empty(t, rec.transitions, "no breaker transition")
			require.False(t, e.breaker.IsOpen(), "a publication fault is not DuckDB's")
			require.Equal(t, 1, f.leg.begins, "a consistency error is not retried")
			require.Equal(t, f.leg.begins, f.leg.rollbacks)
		})
	}
}

// A publication fault is told apart from a failed read: a record the
// snapshot could not read is a PostgreSQL failure, and a manifest the store
// could not answer is a federated read failure. Both stay degradable, and neither is charged to the breaker, since
// neither reached DuckDB.
func TestPublishedPassTellsAFailedReadFromAPublicationFault(t *testing.T) {
	for name, c := range map[string]struct {
		seed  func(f *pgxFixture) *fakePublication
		class error
	}{
		"record read": {class: ErrPostgresReadFailed, seed: func(*pgxFixture) *fakePublication {
			return &fakePublication{recErr: errors.New("connection reset")}
		}},
		"manifest fetch": {class: ErrFederatedReadFailed, seed: func(f *pgxFixture) *fakePublication {
			pub := publishedOver(filepath.Join(f.dir, "base.parquet"))
			pub.genErr = errors.New("s3 unavailable")
			return pub
		}},
	} {
		t.Run(name, func(t *testing.T) {
			f := newPgxFixture(t, tieRunRows())
			e, duck := publishedEngine(t, f, c.seed(f))
			e.breaker = NewCircuitBreaker(1, time.Minute, time.Hour)
			rec := recordDegradedFallbacks(e)

			_, _, err := e.ExecuteDuckDBFederatedQuery(context.Background(), tieRunTables, pgxQuery(), 5, 0, nil, nil)
			require.ErrorIs(t, err, c.class)
			require.NotErrorIs(t, err, ErrPublicationInconsistent)
			require.True(t, degradableFederatedError(err))
			require.Zero(t, duck.statements())

			page, err := e.Query(context.Background(), tieRunTables, pgxQuery(), &model.FederatedQueryOptions{AllowPartialDegradedMode: true})
			require.NoError(t, err)
			require.NotNil(t, page)
			require.Len(t, rec.take(), 1, "the degraded fallback answered")
			require.False(t, e.breaker.IsOpen())
		})
	}
}
