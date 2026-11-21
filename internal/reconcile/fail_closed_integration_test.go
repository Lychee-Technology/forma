package reconcile

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifestv2"
)

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

// TestCollectorFailsClosed verifies that every blocking metadata or manifest
// discrepancy stops GC from deleting or forgetting anything.
func TestCollectorFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name       string
		fault      func(t *testing.T, f *fixture, g3 generation) *Reconciler
		want       FindingKind
		generation int64
		current    bool
	}{
		{"record missing", func(t *testing.T, f *fixture, _ generation) *Reconciler {
			f.exec(t, "DELETE FROM %s", f.tables.Record)
			return nil
		}, NoRecord, 0, false},
		{"record invalid", func(t *testing.T, f *fixture, _ generation) *Reconciler {
			f.exec(t, "UPDATE %s SET generation = -1", f.tables.Record)
			return nil
		}, RecordInvalid, 0, false},
		{"current manifest missing", func(t *testing.T, f *fixture, g3 generation) *Reconciler {
			f.store.remove(g3.rec.ManifestKey)
			return nil
		}, MissingGeneration, 3, true},
		{"current history row missing", func(t *testing.T, f *fixture, _ generation) *Reconciler {
			f.exec(t, "DELETE FROM %s WHERE generation = 3", f.tables.Generation)
			return nil
		}, MissingGeneration, 3, true},
		{"current manifest corrupt", func(t *testing.T, f *fixture, g3 generation) *Reconciler {
			f.corrupt(t, g3.rec.ManifestKey)
			return nil
		}, InvalidManifest, 3, true},
		{"record parent disagrees with history row", func(t *testing.T, f *fixture, _ generation) *Reconciler {
			f.exec(t, "UPDATE %s SET parent_generation = 1", f.tables.Record)
			return nil
		}, HistoryMismatch, 3, true},
		{"current history row digest disagrees", func(t *testing.T, f *fixture, _ generation) *Reconciler {
			f.exec(t, "UPDATE %s SET manifest_sha256 = '"+strings.Repeat("0", 64)+"' WHERE generation = 3", f.tables.Generation)
			return nil
		}, HistoryMismatch, 3, true},
		{"current history row lineage disagrees", func(t *testing.T, f *fixture, _ generation) *Reconciler {
			f.exec(t, "UPDATE %s SET lineage = '"+uuid.NewString()+"' WHERE generation = 3", f.tables.Generation)
			return nil
		}, HistoryMismatch, 3, true},
		{"current history row stamped superseded", func(t *testing.T, f *fixture, _ generation) *Reconciler {
			f.exec(t, "UPDATE %s SET superseded_at = 1 WHERE generation = 3", f.tables.Generation)
			return nil
		}, HistoryMismatch, 3, true},
		{"record lineage disagrees with history and manifest", func(t *testing.T, f *fixture, _ generation) *Reconciler {
			f.exec(t, "UPDATE %s SET lineage = '"+uuid.NewString()+"'", f.tables.Record)
			return nil
		}, HistoryMismatch, 3, true},
		{"expired manifest corrupt", func(t *testing.T, f *fixture, _ generation) *Reconciler {
			f.corrupt(t, f.manifestKeyOf(t, 1))
			return nil
		}, InvalidManifest, 1, false},
		{"retained superseded manifest missing", func(t *testing.T, f *fixture, g3 generation) *Reconciler {
			f.publish(t, f.stageFlush(t, g3, 1))
			f.store.remove(g3.rec.ManifestKey)
			return nil
		}, MissingRetainedManifest, 3, false},
		{"foreign data prefix", func(t *testing.T, f *fixture, _ generation) *Reconciler {
			other := manifestv2.Layout{Prefix: "other"}
			f.store.set(other.ParquetKey(testSchema, 1, manifestv2.TierBase, uuid.Must(uuid.NewV7())), []byte("x"),
				f.serverMillis(t)-time.Hour.Milliseconds())
			f.exec(t, "UPDATE %s SET data_prefix = 'other'", f.tables.Storage)
			r := f.reconciler(testOptions(true))
			r.Layout, r.Storage.Prefix = other, other.Prefix
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
			require.NotEmpty(t, rep.Refused)
			require.Empty(t, rep.Plan.Objects)
			require.Empty(t, rep.Collected.Objects)
			require.Empty(t, rep.Collected.Manifests)
			require.Empty(t, rep.Collected.Forgotten)
			require.Equal(t, before, f.store.keys())
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
// verifies that a retry converges cleanly to the same final state.
func TestCollectionCrashAndRetryAreIdempotent(t *testing.T) {
	for _, tc := range []struct {
		name          string
		fails         func(t *testing.T, f *fixture, g3 generation) string
		manifestsLeft int
	}{
		{"object delete fails before any manifest goes", func(t *testing.T, f *fixture, _ generation) string {
			return f.manifestOf(t, 2).Keys()[2]
		}, 2},
		{"manifest delete fails after objects went", func(t *testing.T, f *fixture, _ generation) string {
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
			require.Empty(t, rep.Collected.Forgotten, "no row forgotten before its manifest is deleted")
			require.Len(t, rep.Collected.Manifests, 2-tc.manifestsLeft)
			require.Equal(t, 2, f.pass(t, testOptions(false)).Expired)

			f.store.failDelete = nil
			rep = f.pass(t, testOptions(true))
			require.NoError(t, rep.Err)
			require.Equal(t, []int64{1, 2}, rep.Collected.Forgotten)
			require.Equal(t, f.keysOf(g3), f.store.keys())

			rep = f.pass(t, testOptions(true))
			require.NoError(t, rep.Err)
			require.Empty(t, rep.Plan.Objects)
			require.Empty(t, rep.Plan.Manifests)
			require.Empty(t, rep.Plan.Forget)
		})
	}
}

func (f *fixture) exec(t *testing.T, format string, table string) {
	t.Helper()
	_, err := f.db.Exec(fmt.Sprintf(format, table))
	require.NoError(t, err)
}

func (f *fixture) corrupt(t *testing.T, key string) {
	t.Helper()
	raw, err := f.store.Get(context.Background(), key)
	require.NoError(t, err)
	raw[len(raw)/2] ^= 0xff
	f.store.set(key, raw, f.serverMillis(t))
}

func (f *fixture) manifestKeyOf(t *testing.T, generation int64) string {
	t.Helper()
	var key string
	require.NoError(t, f.db.QueryRow(fmt.Sprintf("SELECT manifest_key FROM %s WHERE generation = $1", f.tables.Generation), generation).Scan(&key))
	return key
}

func (f *fixture) manifestOf(t *testing.T, generation int64) *manifestv2.Manifest {
	t.Helper()
	raw, err := f.store.Get(context.Background(), f.manifestKeyOf(t, generation))
	require.NoError(t, err)
	var m manifestv2.Manifest
	require.NoError(t, json.Unmarshal(raw, &m))
	return &m
}
