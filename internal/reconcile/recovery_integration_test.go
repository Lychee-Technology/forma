package reconcile

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMissingCurrentHistoryRecoveryTakesTwoPublications runs the documented
// recovery of a lost current history row without a backup: the rendered
// action says one publication and a grace are not enough, and they are not.
// A re-init supersedes the unrecorded generation, which becomes a history
// gap; a grace after the re-init the collection is still refused. Only once
// a later publication supersedes the re-init's generation, a pass retires
// it and it expires does the collection resume, and then it collects the
// unrecorded generation's manifest and the objects only it listed.
func TestMissingCurrentHistoryRecoveryTakesTwoPublications(t *testing.T) {
	f := newFixture(t)
	g1 := f.publishInit(t, f.initialized(t), 1)
	f.exec(t, "DELETE FROM %s WHERE generation = 1", f.tables.Generation)

	rep := f.pass(t, testOptions(false))
	require.NoError(t, rep.Err)
	require.Equal(t, []FindingKind{MissingGeneration}, kinds(rep.Findings))
	require.Equal(t, actionMissingHistory, rep.Findings[0].Action)
	var out bytes.Buffer
	Report{Schemas: []SchemaReport{rep}}.Render(&out)
	for _, step := range []string{
		"restore the generation history row from a backup",
		"takes two publications",
		"then reported as missing_history",
		"a later one must supersede that publication's generation in turn, a pass retire it, and the retention grace pass",
		"Waiting the retention grace after a single publication or re-init is not enough",
	} {
		require.Contains(t, out.String(), step)
	}

	reinit := f.publishInit(t, g1.rec, 1)
	refused := func(retired []int64) {
		t.Helper()
		rep := f.pass(t, testOptions(true))
		require.NoError(t, rep.Err)
		require.Equal(t, []FindingKind{HistoryGap}, kinds(rep.Findings))
		require.Equal(t, g1.rec.Generation, rep.Findings[0].Generation, "the unrecorded generation is the gap")
		require.Equal(t, actionHistoryGap, rep.Findings[0].Action)
		require.NotEmpty(t, rep.Refused)
		require.Equal(t, retired, rep.Collected.Retired)
		require.Empty(t, rep.Collected.Objects)
		require.Empty(t, rep.Collected.Manifests)
		require.Subset(t, f.store.keys(), f.keysOf(g1), "the unrecorded generation is kept")
	}
	refused(nil)
	waitOutGrace()
	refused(nil) // a grace after the re-init: still refused

	f.publish(t, f.stageFlush(t, reinit, 1))
	refused([]int64{reinit.rec.Generation}) // the re-init's generation is retired, not expired
	waitOutGrace()

	rep = f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Findings, "the re-init's generation expired above the gap")
	require.Empty(t, rep.Refused)
	require.Subset(t, rep.Collected.Objects, f.keysOf(g1), "the unrecorded generation's manifest and objects are collected")
	require.Equal(t, []string{reinit.rec.ManifestKey}, rep.Collected.Manifests)
	require.Empty(t, rep.Collected.Forgotten, "the expired re-init row stays as the history's floor")
}
