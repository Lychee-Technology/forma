//go:build e2e

package production

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	fedengine "github.com/lychee-technology/forma/internal/federated"
)

// TestCorruptDeltaWithNewestVersionsIsNotAnsweredStale pins #722: a delta
// holds the newest version of two rows, a tombstone and an update, and a
// readable base object holds their older live versions. Both rows are past
// the flush grace, so neither the dirty set nor the hot leg holds them. When
// the delta is confirmed corrupt, answering from the readable remainder would
// rank the base versions first: the deleted row would come back live and the
// updated row would carry its old title. The read must fail instead, and
// degraded mode must answer from PostgreSQL, which holds the current state of
// every live row.
func TestCorruptDeltaWithNewestVersionsIsNotAnsweredStale(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	wide := DefaultSchemaFixtures()[1]

	creates := buildOpenCreates(wide, 4)
	mustApplyEvents(ctx, t, env, "apply base creates", creates...)
	if _, err := env.RunInit(ctx, wide); err != nil {
		t.Fatalf("run init (base export): %v", err)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", wide.ID)

	deleted, updated := creates[0], creates[1]
	mustApplyEvents(ctx, t, env, "apply the newest versions",
		DeleteEvent(wide, deleted.RowID),
		UpdateEvent(wide, updated.RowID, map[string]any{"title": "closed-01"}))
	mustFlush(ctx, t, env)

	m := loadSchemaManifest(ctx, t, env, wide)
	soleTierEntry(t, m, "base")
	deltaKey := mustManifestKey(t, env, soleTierEntry(t, m, "delta").Path)

	// Control: with the delta intact the newest versions win on DuckDB.
	probes := newestVersionProbes(wide)
	healthy := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 20})
	if healthy == nil || !healthy.Plan.Routing.UseDuckDB {
		t.Fatalf("precondition: healthy query did not route to duckdb")
	}
	for _, p := range probes {
		assertRowCount(ctx, t, env, "healthy "+p.name, p.query, p.want)
	}

	overwriteObjectBytes(ctx, t, env, deltaKey, truncateHalf)

	// Degraded off: every probe fails rather than answering from the base
	// object alone. The first confirms the corruption; the rest refuse from
	// the cached verdict.
	for _, p := range probes {
		failCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		res, err := env.Query(failCtx, p.query)
		cancel()
		if err == nil {
			t.Fatalf("%s: answered %d rows from the readable remainder, want a corrupt-object failure: %v",
				p.name, len(res.Records), resultRowIDSet(t, res))
		}
		assertCorruptRefusal(t, err, deltaKey)
	}

	// Degraded on: the PostgreSQL-only answer matches the oracle, so the
	// deleted row is absent and the updated row carries its new title.
	for _, p := range probes {
		q := p.query
		q.AllowPartialDegradedMode = true
		res := env.AssertQueryMatches(ctx, q)
		if res == nil {
			continue
		}
		if len(res.Records) != p.want {
			t.Errorf("degraded %s returned %d rows, want %d", p.name, len(res.Records), p.want)
		}
		assertDegradedFallbackPlan(t, res)
		if res.Partial == nil || len(res.Partial.UnconsultedTiers) == 0 {
			t.Errorf("degraded %s must carry the hot_tier_only coverage marker, got %+v", p.name, res.Partial)
		}
	}
}

type newestVersionProbe struct {
	name  string
	query Query
	want  int
}

// newestVersionProbes covers the whole schema plus a filter on each
// superseded value. A remainder answer returns the deleted row for
// title=open-00 and the updated row for title=open-01; the current state
// matches neither.
func newestVersionProbes(wide SchemaRef) []newestVersionProbe {
	return []newestVersionProbe{
		{name: "all rows", query: Query{Schema: wide, Limit: 20}, want: 3},
		{name: "deleted row's title", query: Query{Schema: wide, Filters: []Filter{{Attr: "title", Value: "open-00"}}, Limit: 20}},
		{name: "updated row's old title", query: Query{Schema: wide, Filters: []Filter{{Attr: "title", Value: "open-01"}}, Limit: 20}},
		{name: "updated row's new title", query: Query{Schema: wide, Filters: []Filter{{Attr: "title", Value: "closed-01"}}, Limit: 20}, want: 1},
	}
}

// assertCorruptRefusal requires err to be the #722 corrupt-object failure
// naming key: degradable (ErrFederatedReadFailed), never manifest
// inconsistency, and never a breaker rejection, which would mean the
// corruption fed the breaker.
func assertCorruptRefusal(t *testing.T, err error, key string) {
	t.Helper()
	var corrupt *fedengine.CorruptParquetError
	if !errors.As(err, &corrupt) {
		t.Fatalf("want a CorruptParquetError naming %s, got: %v", key, err)
	}
	named := false
	for _, p := range corrupt.Paths {
		named = named || strings.HasSuffix(p, key)
	}
	if !named {
		t.Errorf("corrupt-object failure names %v, want %s", corrupt.Paths, key)
	}
	if !errors.Is(err, fedengine.ErrFederatedReadFailed) {
		t.Errorf("a corrupt-object failure must stay ErrFederatedReadFailed (degradable): %v", err)
	}
	if errors.Is(err, fedengine.ErrParquetSetInconsistent) || errors.Is(err, fedengine.ErrDuckDBUnavailable) {
		t.Errorf("a corrupt-object failure must not classify as inconsistency or a breaker rejection: %v", err)
	}
}
