//go:build e2e

package production

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	fedengine "github.com/lychee-technology/forma/internal/federated"
)

// TestManifestStamp_RogueOverwriteFailsLoudly closes the one silent-loss
// channel the #256 manifest stamps opened.
//
// A stamp that satisfies the parquetcheck invariant spares its path the footer
// probe — that is the whole cold-start win. The cost is that the read path
// stops looking at the bytes: if the object behind a stamped key is replaced
// by one that does not carry row_id (a rogue overwrite, a tampered manifest,
// an operator restoring the wrong file), `union_by_name` NULL-fills row_id
// from the sibling objects' schema, every one of those rows falls out of the
// dirty anti-join, and the query SUCCEEDS while quietly ignoring the file.
// That is the exact inversion of #187's contract, and it is invisible: no
// error, no plan note, just fewer rows.
//
// The scenario reproduces it end to end against the real stack: flush two
// delta objects (both stamped by the flusher), then overwrite ONE of them
// with a parquet carrying every column except row_id, leaving its manifest
// entry — and its now-false stamp — untouched. The second object is what
// keeps row_id in the union so the scan binds at all; with a single object
// the scan would fail on its own and prove nothing.
//
// The read must fail loudly. It classifies as ErrFederatedReadFailed, the
// same degradable class as today's schema-violation failures — so the
// degraded-mode caller still gets an answer, marked.
//
// Note on #251: verification drains SELECT * without the guard, so this
// schema-wrong (not byte-corrupt) object is never confirmed corrupt and never
// excluded. Correct — exclusion is for unreadable bytes; a file that violates
// the export schema is an operator-visible consistency fault, not something
// to route around.
func TestManifestStamp_RogueOverwriteFailsLoudly(t *testing.T) {
	ctx := context.Background()
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	wide := DefaultSchemaFixtures()[1]

	keys := seedMultiParquet(ctx, t, env, wide)
	victim, sibling := keys[0], keys[1]

	// Precondition 1: the flusher stamped both entries, so the read path will
	// trust the stamp instead of probing. Without this the scenario would
	// degrade into the plain #189 probe path and prove nothing about stamps.
	stamped := stampedEntryFor(ctx, t, env, wide, victim)
	if _, ok := stamped["row_id"]; !ok {
		t.Fatalf("precondition: manifest stamp for %s does not claim row_id: %#v", victim, stamped)
	}

	// Precondition 2: the query answers from the cold tier today, and both
	// objects contribute. This also warms the validator cache under the
	// current stamp — the state a long-lived server is actually in.
	healthy, err := env.Query(ctx, Query{Schema: wide, Limit: 50})
	if err != nil {
		t.Fatalf("precondition query failed: %v", err)
	}
	if !healthy.Plan.Routing.UseDuckDB {
		t.Fatalf("precondition: healthy query did not route to duckdb: %+v", healthy.Plan.Routing)
	}
	victimIDs := readParquetRowIDs(ctx, t, env, victim)
	before := resultRowIDSet(t, healthy)
	for id := range victimIDs {
		if _, ok := before[id]; !ok {
			t.Fatalf("precondition: row %s from %s is already absent from the healthy result", id, victim)
		}
	}

	// The rogue overwrite: same key, same manifest entry, same stamp — bytes
	// that no longer carry row_id. Derived from the object itself so every
	// OTHER column still binds and the guard is the only thing under test.
	overwriteWithoutRowID(ctx, t, env, victim)
	if cols := describeParquetCols(ctx, t, env, victim); len(cols) == 0 {
		t.Fatalf("rogue overwrite produced an unreadable object at %s", victim)
	} else if _, ok := cols["row_id"]; ok {
		t.Fatalf("rogue overwrite did not drop row_id from %s: %#v", victim, cols)
	}
	if still := stampedEntryFor(ctx, t, env, wide, victim); len(still) == 0 {
		t.Fatalf("manifest entry for %s lost its stamp; the scenario needs the stale stamp intact", victim)
	}

	failCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	res, err := env.Query(failCtx, Query{Schema: wide, Limit: 50})
	if err == nil {
		lost := setMinus(before, resultRowIDSet(t, res))
		t.Fatalf("a stamp-trusted rogue object was scanned silently: query returned %d records, "+
			"losing %d row(s) that the sibling object %s cannot supply (#256 silent-loss channel)",
			len(res.Records), len(lost), sibling)
	}
	if !errors.Is(err, fedengine.ErrFederatedReadFailed) {
		t.Fatalf("a scanned object violating the export schema must classify as ErrFederatedReadFailed, got: %v", err)
	}
	if !strings.Contains(err.Error(), "NULL row_id") {
		t.Errorf("the failure must name the violated invariant so an operator can act on it, got: %v", err)
	}

	// Degradable, like every other read-side schema violation: the caller that
	// asked for a marked partial answer still gets one.
	degraded := env.AssertQueryMatches(ctx, Query{Schema: wide, Limit: 50, AllowPartialDegradedMode: true})
	assertDegradedFallbackPlan(t, degraded)
}

// stampedEntryFor returns the Columns stamp the schema's manifest currently
// records for key, or nil if the entry is unstamped or absent.
func stampedEntryFor(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, key string) map[string]string {
	t.Helper()
	manifests, err := env.loadManifests(ctx)
	if err != nil {
		t.Fatalf("load manifests: %v", err)
	}
	m := manifests[schema.ID]
	if m == nil {
		t.Fatalf("schema %s has no manifest", schema.Name)
	}
	for _, entry := range m.Files {
		if strings.TrimPrefix(entry.Path, "/") == strings.TrimPrefix(key, "/") {
			return entry.Columns
		}
	}
	t.Fatalf("manifest for %s lists no entry for %s: %+v", schema.Name, key, m.Files)
	return nil
}

// overwriteWithoutRowID republishes key with every column it already carries
// except row_id. Going through a staging table is deliberate: COPY cannot read
// and rewrite the same object in one statement.
func overwriteWithoutRowID(ctx context.Context, t *testing.T, env *Env, key string) {
	t.Helper()
	const staging = "rogue_missing_row_id"
	stage := fmt.Sprintf(
		"CREATE OR REPLACE TABLE %s AS SELECT * EXCLUDE (row_id) FROM read_parquet('s3://%s/%s')",
		staging, env.Cluster.Bucket, strings.TrimPrefix(key, "/"))
	if _, err := env.Duck.DB.ExecContext(ctx, stage); err != nil {
		t.Fatalf("stage rogue rewrite of %s: %v", key, err)
	}
	writeParquetViaDuck(ctx, t, env, "SELECT * FROM "+staging, key)
}
