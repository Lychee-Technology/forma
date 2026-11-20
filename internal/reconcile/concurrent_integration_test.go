package reconcile

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/sqlgen"
)

// TestDiagnosisLeavesOutACurrentGenerationCollectedBesideIt verifies that when
// a generation seen current in S1 is superseded, retired, and collected while
// a lock-free diagnosis reads S3, S2 proves the generation expired/was
// forgotten and leaves those absent findings out, marking the schema
// Inconclusive (exit 2) without recommending re-init.
func TestDiagnosisLeavesOutACurrentGenerationCollectedBesideIt(t *testing.T) {
	for _, tc := range []struct {
		name      string
		checksums bool
		left      int
	}{
		{"before its manifest is read", false, 1},
		{"before its objects are hashed", true, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			g1 := f.publishInit(t, f.initialized(t), 2)
			fired := false
			var collected SchemaReport
			expire := func(string) {
				if fired {
					return
				}
				fired = true
				f.publishCompact(t, g1)
				require.Equal(t, []int64{1}, f.pass(t, testOptions(true)).Collected.Retired)
				waitOutGrace()
				collected = f.pass(t, testOptions(true))
			}
			opts := testOptions(false)
			opts.VerifyChecksums = tc.checksums
			if tc.checksums {
				f.store.beforeHash = expire
			} else {
				f.store.beforeGet = expire
			}

			rep := f.pass(t, opts)
			require.True(t, fired)
			require.NoError(t, collected.Err)
			require.Equal(t, []string{g1.rec.ManifestKey}, collected.Collected.Manifests)
			require.ElementsMatch(t, g1.manifest.Keys(), collected.Collected.Objects)
			require.Equal(t, []int64{1}, collected.Collected.Forgotten)
			require.NoError(t, rep.Err)
			require.Equal(t, g1.rec.Generation, rep.Generation)
			require.Empty(t, rep.Findings)
			require.Equal(t, tc.left, rep.ConcurrentlyExpired)
			require.Contains(t, rep.Refused, "run diagnosis again")
			require.True(t, rep.Inconclusive())
			require.True(t, rep.Residual())

			var out bytes.Buffer
			Report{Schemas: []SchemaReport{rep}}.Render(&out)
			require.NotContains(t, out.String(), "cdc-init")
			require.Contains(t, out.String(), "finding(s) left out: generation expired while diagnosis ran")
			require.Contains(t, out.String(), "inconclusive (run the diagnosis again)")
			require.NotContains(t, out.String(), ": clean (")
		})
	}
}

// TestDiagnosisReportsACurrentLossNoCollectionCaused verifies that permanent
// loss of the current manifest or current history row is never suppressed.
func TestDiagnosisReportsACurrentLossNoCollectionCaused(t *testing.T) {
	f := newFixture(t)
	g1 := f.publishInit(t, f.initialized(t), 1)
	f.store.remove(g1.rec.ManifestKey)
	rep := f.pass(t, testOptions(false))
	require.NoError(t, rep.Err)
	require.Equal(t, []FindingKind{MissingGeneration}, kinds(rep.Findings))
	require.Equal(t, actionMissingGeneration, rep.Findings[0].Action)
	require.Zero(t, rep.ConcurrentlyExpired)

	f = newFixture(t)
	g1 = f.publishInit(t, f.initialized(t), 1)
	f.exec(t, "DELETE FROM %s WHERE generation = 1", f.tables.Generation)
	rep = f.pass(t, testOptions(false))
	require.NoError(t, rep.Err)
	require.Equal(t, []FindingKind{MissingGeneration}, kinds(rep.Findings))
	require.Equal(t, actionHistoryMismatch, rep.Findings[0].Action)
	require.True(t, rep.Findings[0].Current)
	require.Equal(t, g1.rec.Generation, rep.Findings[0].Generation)
	require.Zero(t, rep.ConcurrentlyExpired)
}

// TestDiagnosisReportsCurrentLossWhateverItsHistoryRow verifies that even when
// the current generation's history row is missing or prematurely retired,
// diagnosis still verifies the record's manifest (as readers do) and reports
// any lost or corrupt object or missing manifest on the current generation.
func TestDiagnosisReportsCurrentLossWhateverItsHistoryRow(t *testing.T) {
	for _, tc := range []struct {
		name    string
		damage  string
		rowKind FindingKind
	}{
		{"row stamped retired", "UPDATE %s SET retired_at = " + sqlgen.ServerTimeMillisSQL + " WHERE generation = 1", HistoryMismatch},
		{"row deleted", "DELETE FROM %s WHERE generation = 1", MissingGeneration},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			g1 := f.publishInit(t, f.initialized(t), 2)
			f.exec(t, tc.damage, f.tables.Generation)
			f.ageStamps(t, 2*time.Hour)
			lost := g1.manifest.Keys()[0]
			f.store.remove(lost)

			rep := f.pass(t, testOptions(false))
			require.NoError(t, rep.Err)
			require.Zero(t, rep.ConcurrentlyExpired)
			require.Equal(t, []FindingKind{tc.rowKind, LostObject}, kinds(rep.Findings))
			require.Equal(t, actionHistoryMismatch, rep.Findings[0].Action)
			require.Contains(t, rep.Findings[0].Detail, readsManifestVerified)
			require.Equal(t, lost, rep.Findings[1].Key)
			require.True(t, rep.Findings[1].Current)
			require.Equal(t, actionLostCurrent, rep.Findings[1].Action)
			var out bytes.Buffer
			Report{Schemas: []SchemaReport{rep}}.Render(&out)
			require.NotContains(t, out.String(), "reads succeed")

			corrupt := g1.manifest.Keys()[1]
			f.corrupt(t, corrupt)
			opts := testOptions(false)
			opts.VerifyChecksums = true
			rep = f.pass(t, opts)
			require.NoError(t, rep.Err)
			require.Equal(t, []FindingKind{tc.rowKind, LostObject, CorruptObject}, kinds(rep.Findings))
			bad := rep.Findings[2]
			require.Equal(t, corrupt, bad.Key)
			require.True(t, bad.Current)
			require.Equal(t, actionCorrupt, bad.Action)
			require.Contains(t, bad.Detail, "incorrect results")
			require.Contains(t, bad.Detail, "scan error")
			require.NotContains(t, bad.Detail, "fail closed")

			f.store.remove(g1.rec.ManifestKey)
			rep = f.pass(t, testOptions(false))
			require.NoError(t, rep.Err)
			require.Zero(t, rep.ConcurrentlyExpired)
			require.Equal(t, []FindingKind{tc.rowKind, MissingGeneration}, kinds(rep.Findings))
			require.Contains(t, rep.Findings[0].Detail, readsUnverified)
			require.Equal(t, g1.rec.ManifestKey, rep.Findings[1].Key)
			require.Equal(t, actionMissingGeneration, rep.Findings[1].Action)
		})
	}
}

// TestDiagnosisReportsALossAReplacementCannotHaveCollected verifies that if
// generation N is replaced by N+1 while diagnosis runs without N expiring, N's
// missing manifest is still reported (not suppressed as concurrent expiry).
func TestDiagnosisReportsALossAReplacementCannotHaveCollected(t *testing.T) {
	f := newFixture(t)
	g1 := f.publishInit(t, f.initialized(t), 1)
	f.store.remove(g1.rec.ManifestKey)
	fired := false
	f.store.beforeGet = func(string) {
		if !fired {
			fired = true
			f.publishCompact(t, g1)
		}
	}

	rep := f.pass(t, testOptions(false))
	require.True(t, fired)
	require.NoError(t, rep.Err)
	require.Zero(t, rep.ConcurrentlyExpired)
	require.Equal(t, []FindingKind{MissingGeneration}, kinds(rep.Findings))
	require.Equal(t, g1.rec.ManifestKey, rep.Findings[0].Key)
	require.False(t, rep.Inconclusive())
}

// TestDiagnosisKeepsItsUncertaintyWhenNoCollectionRan verifies R12-F2: when a
// superseded retired generation expires while diagnosis runs without any GC
// running, the finding is left out as inconclusive without claiming a
// collection was observed running or deleting anything.
func TestDiagnosisKeepsItsUncertaintyWhenNoCollectionRan(t *testing.T) {
	f := newFixture(t)
	g1 := f.publishInit(t, f.initialized(t), 1)
	f.publishCompact(t, g1)
	retire := f.pass(t, agedOptions(true))
	require.NoError(t, retire.Err)
	require.Equal(t, []int64{g1.rec.Generation}, retire.Collected.Retired)
	f.store.remove(g1.rec.ManifestKey)
	fired := false
	f.store.beforeGet = func(string) {
		if !fired {
			fired = true
			f.ageStamps(t, time.Hour)
		}
	}

	rep := f.pass(t, agedOptions(false))
	require.True(t, fired)
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Findings)
	require.Equal(t, 1, rep.ConcurrentlyExpired)
	require.True(t, rep.Inconclusive())
	require.Contains(t, rep.Refused, "a collection may have deleted what they listed, or none ran")
	require.Contains(t, rep.Refused, "run diagnosis again")
	var out bytes.Buffer
	Report{Schemas: []SchemaReport{rep}}.Render(&out)
	text := out.String()
	require.Contains(t, text, "inconclusive (run the diagnosis again)")
	require.Contains(t, text, "a collection may have deleted what it listed, or none ran and it was missing before")
	require.NotContains(t, text, "collection ran beside")
}
