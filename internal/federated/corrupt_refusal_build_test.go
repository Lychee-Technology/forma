package federated

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// buildRefusalCase is one SQL-build refusal the cached corrupt verdict must
// not mask.
type buildRefusalCase struct {
	emptyMetadata bool
	condition     forma.Condition
	wantIs        error
}

// TestCachedCorruptRefusalKeepsTheBuildRefusals pins the #722 review finding
// that a cached corrupt verdict must not mask the refusals the SQL build
// raises. A fresh read reaches the build before it scans, so a schema whose
// active metadata went empty fails with ErrSchemaMetadataCacheRequired and a
// malformed filter literal fails as invalid input; both are non-degradable.
// The cached run must match the uncached one with degraded mode off and on:
// no CorruptParquetError, no PostgreSQL fallback, no scan, and the breaker
// probe handed back as for any pre-flight refusal.
func TestCachedCorruptRefusalKeepsTheBuildRefusals(t *testing.T) {
	cases := map[string]buildRefusalCase{
		"schema metadata emptied": {
			emptyMetadata: true,
			wantIs:        ErrSchemaMetadataCacheRequired,
		},
		"malformed filter literal": {
			condition: &forma.KvCondition{Attr: "age", Value: "gt:not-a-number"},
			wantIs:    forma.ErrInvalidInput,
		},
	}
	for name, tc := range cases {
		for _, cached := range []bool{false, true} {
			for _, degraded := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/cached=%v/degraded=%v", name, cached, degraded), func(t *testing.T) {
					runBuildRefusalCase(t, tc, cached, degraded)
				})
			}
		}
	}
}

func runBuildRefusalCase(t *testing.T, tc buildRefusalCase, cached, degraded bool) {
	t.Helper()
	restore := initTestDescriptors()
	defer restore()

	duck := &retryFakeDuck{passes: []retryPass{{}}}
	pg := &fakePostgresFederatedSource{}
	breaker := NewCircuitBreaker(1, time.Minute, time.Minute)
	mc := testMetadataCacheSchema7(t)
	src := &fakeParquetSource{paths: []string{retryKeptPathA, retryCorruptPath1}}
	e := NewDBFederatedQueryEngine(pg, &fakeDirtyIDFetcher{}, duck, breaker,
		hybridDuckConfig(), mc, "host=x", WithParquetSource(src))
	if cached {
		e.corruptPaths.Add([]string{retryCorruptPath1})
	}
	if tc.emptyMetadata {
		// RegisterSchema replaces the active cache in place, as a schema
		// re-registered with only retired attributes would.
		require.NoError(t, mc.RegisterSchema("test", 7, forma.SchemaAttributeCache{}))
	}

	q := coldTierQuery()
	q.Condition = tc.condition
	opts := &model.FederatedQueryOptions{AllowPartialDegradedMode: degraded}
	_, err := e.Query(context.Background(), corruptTables, q, opts)

	require.ErrorIs(t, err, tc.wantIs)
	require.False(t, degradableFederatedError(err))
	var corrupt *CorruptParquetError
	require.False(t, errors.As(err, &corrupt), "the cached verdict must not mask the build refusal")
	require.Zero(t, pg.queryCalls, "the build refusal must not degrade to postgres-only")
	require.Empty(t, duck.mainSQL, "nothing may be scanned")
	require.Zero(t, src.missingCalls, "the build refusal precedes the missing-object probe, as it precedes the scan")
	require.False(t, breaker.IsOpen(), "threshold 1: a pre-flight refusal hands the probe back")
}
