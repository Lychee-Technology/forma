//go:build e2e

package production

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/manifest"
)

// resultSnapshot is a canonical fold of one engine query result, used for the
// literal before/after-compaction comparison (#188): unlike the oracle diff it
// compares the engine against itself across the compaction boundary, so any
// drift the oracle happened to tolerate still fails.
type resultSnapshot struct {
	Query Query
	Total int64
	Order []string                  // row IDs in engine order
	Rows  map[string]map[string]any // row ID -> attr name -> normalized value
}

// snapshotQueryResult runs the query through the real engine and folds the
// records into the oracle's canonical value forms (normalizeValue via
// actualAttrValue), so two snapshots compare bit-for-bit.
func snapshotQueryResult(ctx context.Context, t *testing.T, env *Env, q Query) *resultSnapshot {
	t.Helper()
	result, err := env.Query(ctx, q)
	if err != nil {
		t.Fatalf("snapshot query: %v", err)
	}
	cache, ok := env.Metadata.GetSchemaCacheByID(q.Schema.ID)
	if !ok {
		t.Fatalf("no metadata cache for schema %d", q.Schema.ID)
	}

	snap := &resultSnapshot{
		Query: q,
		Total: result.Total,
		Rows:  make(map[string]map[string]any, len(result.Records)),
	}
	for _, rec := range result.Records {
		id := rec.RowID.String()
		snap.Order = append(snap.Order, id)
		attrs := make(map[string]any, len(cache))
		for name, meta := range cache {
			val, err := actualAttrValue(rec, meta)
			if err != nil {
				t.Fatalf("extract attr %q of row %s: %v", name, id, err)
			}
			attrs[name] = val
		}
		snap.Rows[id] = attrs
	}
	return snap
}

// assertResultsIdentical is the bit-for-bit before/after criterion (#188):
// identical totals, identical row-ID sets, identical per-attribute values,
// and identical order when the query sorts.
func assertResultsIdentical(t *testing.T, label string, before, after *resultSnapshot) {
	t.Helper()
	if before.Total != after.Total {
		t.Errorf("%s: total changed across compaction: %d -> %d", label, before.Total, after.Total)
	}
	for id := range before.Rows {
		if _, ok := after.Rows[id]; !ok {
			t.Errorf("%s: row %s missing after compaction", label, id)
		}
	}
	for id := range after.Rows {
		if _, ok := before.Rows[id]; !ok {
			t.Errorf("%s: row %s appeared after compaction", label, id)
		}
	}
	if len(before.Query.Sorts) > 0 && !reflect.DeepEqual(before.Order, after.Order) {
		t.Errorf("%s: sorted order changed across compaction:\n before: %v\n after:  %v",
			label, before.Order, after.Order)
	}
	for id, beforeAttrs := range before.Rows {
		afterAttrs, ok := after.Rows[id]
		if !ok {
			continue
		}
		for name, b := range beforeAttrs {
			if a := afterAttrs[name]; !valuesEqual(b, a) {
				t.Errorf("%s: row %s attr %q changed across compaction: %v -> %v",
					label, id, name, b, a)
			}
		}
	}
}

// seedCompactionBase creates n rows and exports them as the base tier via a
// real cdc-init run, then clears their change_log entries (the seedAllTiers
// recipe) so they are neither hot nor dirty. Returns the create events; their
// resolved RowIDs are the targets for follow-up updates/deletes.
func seedCompactionBase(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, n int) []*Event {
	t.Helper()
	creates := env.GenerateScript(ScriptSpec{Schema: schema, Creates: n})
	if err := env.ApplyEvents(ctx, creates...); err != nil {
		t.Fatalf("apply base events: %v", err)
	}
	if _, err := env.RunInit(ctx, schema); err != nil {
		t.Fatalf("run init (base export): %v", err)
	}
	env.ExecSQL(ctx, "DELETE FROM change_log WHERE schema_id = $1", schema.ID)
	return creates
}

// loadSchemaManifest loads and parses the schema's current manifest, failing
// the test when it does not exist.
func loadSchemaManifest(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) *manifest.Manifest {
	t.Helper()
	manifests, err := env.loadManifests(ctx)
	if err != nil {
		t.Fatalf("load manifests: %v", err)
	}
	m := manifests[schema.ID]
	if m == nil {
		t.Fatalf("schema %d has no manifest", schema.ID)
	}
	return m
}

// assertNoDuplicateManifestEntries pins the "no duplicate manifest entries"
// success criterion (#188).
func assertNoDuplicateManifestEntries(t *testing.T, m *manifest.Manifest) {
	t.Helper()
	seen := make(map[string]bool, len(m.Files))
	for _, f := range m.Files {
		if seen[f.Path] {
			t.Errorf("manifest lists %s more than once", f.Path)
		}
		seen[f.Path] = true
	}
}

// assertDeltaSizesPopulated is the positive control that the SizeBytes fix is
// active: without it the promotion scenarios would pass vacuously (a zero
// threshold override could promote empty tiers).
func assertDeltaSizesPopulated(t *testing.T, m *manifest.Manifest) {
	t.Helper()
	deltas := 0
	for _, f := range m.Files {
		if !strings.EqualFold(f.Tier, "delta") {
			continue
		}
		deltas++
		if f.SizeBytes <= 0 {
			t.Fatalf("delta entry %s has SizeBytes=%d, want >0 (SizeBytes fix not active?)", f.Path, f.SizeBytes)
		}
	}
	if deltas == 0 {
		t.Fatal("manifest has no delta entries; seed did not flush a delta")
	}
}

// parquetInventory filters an S3 inventory snapshot down to parquet objects,
// excluding the manifest JSON (which legitimately changes when compaction
// commits) and _tmp staging keys (swallowed-delete residue is possible by
// design and reclaimed by manifest-reconcile --gc, #226).
func parquetInventory(inv map[string]s3ObjectStat) map[string]s3ObjectStat {
	out := make(map[string]s3ObjectStat, len(inv))
	for key, stat := range inv {
		if !strings.HasSuffix(key, ".parquet") || strings.Contains(key, "/_tmp/") {
			continue
		}
		out[key] = stat
	}
	return out
}

// assertParquetInventoryUnchanged asserts compaction created, deleted and
// modified no parquet objects (promotion is a manifest-only relabel).
func assertParquetInventoryUnchanged(t *testing.T, label string, before, after map[string]s3ObjectStat) {
	t.Helper()
	if !reflect.DeepEqual(parquetInventory(before), parquetInventory(after)) {
		t.Errorf("%s: parquet inventory changed:\n before: %v\n after:  %v",
			label, parquetInventory(before), parquetInventory(after))
	}
}
