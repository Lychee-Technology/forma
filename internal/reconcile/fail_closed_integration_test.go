package reconcile

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifestv2"
)

// collectable builds a schema with something to collect: an init (g1) and
// a flush (g2) a compaction (g3) replaced, retired by a pass and past their
// grace, and an orphan staged an hour ago. A pass now would delete g1's and
// g2's manifests and the objects only they list, and the orphan.
func collectable(t *testing.T, f *fixture) generation {
	t.Helper()
	g1 := f.publishInit(t, f.initialized(t), 2)
	g2 := f.publish(t, f.stageFlush(t, g1, 1))
	g3 := f.publishCompact(t, g2)
	f.store.set(f.layout.ParquetKey(testSchema, 1, manifestv2.TierDelta, uuid.Must(uuid.NewV7())), []byte("orphan"),
		f.serverMillis(t)-time.Hour.Milliseconds())
	rep := f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Equal(t, []int64{1, 2}, rep.Collected.Retired)
	waitOutGrace()
	return g3
}

// TestCollectorFailsClosed: a pass that cannot prove what readers may still
// scan deletes nothing and forgets nothing, and its finding names the schema,
// epoch and generation it concerns and the operator's action.
func TestCollectorFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fault func(t *testing.T, f *fixture, g3 generation) *Reconciler
		want  FindingKind
		// generation is the generation the finding names, 0 for none.
		generation int64
		current    bool
	}{
		{"the record is missing", func(t *testing.T, f *fixture, _ generation) *Reconciler {
			f.exec(t, "DELETE FROM %s", f.tables.Record)
			return nil
		}, NoRecord, 0, false},
		{"the record is one no transition writes", func(t *testing.T, f *fixture, _ generation) *Reconciler {
			f.exec(t, "UPDATE %s SET generation = -1", f.tables.Record)
			return nil
		}, RecordInvalid, 0, false},
		{"the current generation's manifest is missing", func(t *testing.T, f *fixture, g3 generation) *Reconciler {
			f.store.remove(g3.rec.ManifestKey)
			return nil
		}, MissingGeneration, 3, true},
		{"the current generation's history row is missing", func(t *testing.T, f *fixture, _ generation) *Reconciler {
			f.exec(t, "DELETE FROM %s WHERE generation = 3", f.tables.Generation)
			return nil
		}, MissingGeneration, 3, true},
		{"the current generation's manifest is corrupt", func(t *testing.T, f *fixture, g3 generation) *Reconciler {
			f.corrupt(t, g3.rec.ManifestKey)
			return nil
		}, InvalidManifest, 3, true},
		{"an expired generation's manifest is corrupt", func(t *testing.T, f *fixture, _ generation) *Reconciler {
			f.corrupt(t, f.manifestKeyOf(t, 1))
			return nil
		}, InvalidManifest, 1, false},
		{"a retained superseded generation's manifest is missing", func(t *testing.T, f *fixture, g3 generation) *Reconciler {
			f.publish(t, f.stageFlush(t, g3, 1))
			f.store.remove(g3.rec.ManifestKey)
			return nil
		}, MissingRetainedManifest, 3, false},
		{"the pass runs under another data prefix than the writers'", func(t *testing.T, f *fixture, _ generation) *Reconciler {
			other := manifestv2.Layout{Prefix: "other"}
			f.store.set(other.ParquetKey(testSchema, 1, manifestv2.TierBase, uuid.Must(uuid.NewV7())), []byte("x"),
				f.serverMillis(t)-time.Hour.Milliseconds())
			r := f.reconciler(testOptions(true))
			r.Layout = other
			return r
		}, ForeignManifest, 3, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			g3 := collectable(t, f)
			r := tc.fault(t, f, g3)
			if r == nil {
				r = f.reconciler(testOptions(true))
			}
			before := f.store.keys()
			report, err := r.Run(context.Background())
			require.NoError(t, err)
			rep := report.Schemas[0]
			require.NoError(t, rep.Err)
			require.NotEmpty(t, rep.Refused, "the pass refuses to collect")
			require.Empty(t, rep.Plan.Objects)
			require.Empty(t, rep.Collected.Objects)
			require.Empty(t, rep.Collected.Manifests)
			require.Empty(t, rep.Collected.Forgotten)
			require.Equal(t, before, f.store.keys(), "nothing is deleted")
			require.True(t, rep.Residual())
			finding := rep.Findings[0]
			require.Equal(t, tc.want, finding.Kind)
			require.Equal(t, tc.generation, finding.Generation)
			require.Equal(t, tc.current, finding.Current)
			if tc.generation != 0 {
				require.Equal(t, int64(1), finding.Epoch)
			}
			require.NotEmpty(t, finding.Action)
		})
	}
}

// TestCollectorFailsOnAManifestReadItCannotTrust: a refused read proves
// nothing about the manifest, so the pass fails as a tool failure and
// deletes nothing.
func TestCollectorFailsOnAManifestReadItCannotTrust(t *testing.T) {
	f := newFixture(t)
	g3 := collectable(t, f)
	f.store.getErr[g3.rec.ManifestKey] = fmt.Errorf("get: %w", manifestv2.ErrAccessDenied)
	before := f.store.keys()
	rep := f.pass(t, testOptions(true))
	require.ErrorIs(t, rep.Err, manifestv2.ErrAccessDenied)
	require.Equal(t, before, f.store.keys())
}

// TestCollectionCrashAndRetryAreIdempotent fails a delete at each step and
// runs the pass again: the retry finishes what the failed pass left, and the
// end state is the one an uninterrupted pass reaches.
func TestCollectionCrashAndRetryAreIdempotent(t *testing.T) {
	for _, tc := range []struct {
		name string
		// fails answers the key whose delete fails.
		fails func(t *testing.T, f *fixture, g3 generation) string
		// manifestsLeft is how many expired manifests the failed pass
		// leaves.
		manifestsLeft int
	}{
		{"an object delete fails before any manifest goes", func(t *testing.T, f *fixture, _ generation) string {
			return f.manifestOf(t, 2).Keys()[2] // a delta only g2 lists
		}, 2},
		{"a manifest delete fails after every object went", func(t *testing.T, f *fixture, _ generation) string {
			return f.manifestKeyOf(t, 1)
		}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			g3 := collectable(t, f)
			failing := tc.fails(t, f, g3)
			f.store.failDelete = func(key string) error {
				if key == failing {
					return fmt.Errorf("injected delete failure of %s", key)
				}
				return nil
			}
			rep := f.pass(t, testOptions(true))
			require.ErrorContains(t, rep.Err, "injected delete failure")
			require.Empty(t, rep.Collected.Forgotten, "no row is forgotten before its manifest is gone")
			require.Len(t, rep.Collected.Manifests, 2-tc.manifestsLeft)
			require.Equal(t, 2, f.pass(t, testOptions(false)).Expired, "both rows outlive the failed pass")

			f.store.failDelete = nil
			rep = f.pass(t, testOptions(true))
			require.NoError(t, rep.Err)
			require.Equal(t, []int64{1, 2}, rep.Collected.Forgotten)
			require.Equal(t, f.keysOf(g3), f.store.keys())

			rep = f.pass(t, testOptions(true))
			require.NoError(t, rep.Err)
			require.Empty(t, rep.Plan.Objects, "a third pass has nothing left")
			require.Empty(t, rep.Plan.Manifests)
			require.Empty(t, rep.Plan.Forget)
		})
	}
}

// exec runs a statement over one of the fixture's tables, formatted with
// it.
func (f *fixture) exec(t *testing.T, format string, table string) {
	t.Helper()
	_, err := f.db.Exec(fmt.Sprintf(format, table))
	require.NoError(t, err)
}

// corrupt flips a byte of the object, keeping its length.
func (f *fixture) corrupt(t *testing.T, key string) {
	t.Helper()
	raw, err := f.store.Get(context.Background(), key)
	require.NoError(t, err)
	raw[len(raw)/2] ^= 0xff
	f.store.set(key, raw, f.serverMillis(t))
}

// manifestKeyOf answers the manifest key the history names for generation.
func (f *fixture) manifestKeyOf(t *testing.T, generation int64) string {
	t.Helper()
	var key string
	require.NoError(t, f.db.QueryRow(fmt.Sprintf("SELECT manifest_key FROM %s WHERE generation = $1", f.tables.Generation), generation).Scan(&key))
	return key
}

// manifestOf answers generation's manifest as the store holds it.
func (f *fixture) manifestOf(t *testing.T, generation int64) *manifestv2.Manifest {
	t.Helper()
	raw, err := f.store.Get(context.Background(), f.manifestKeyOf(t, generation))
	require.NoError(t, err)
	var m manifestv2.Manifest
	require.NoError(t, json.Unmarshal(raw, &m))
	return &m
}
