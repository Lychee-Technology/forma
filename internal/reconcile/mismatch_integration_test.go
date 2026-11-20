package reconcile

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// TestAHistoryRowThatDisagreesWithTheRecordIsNotAMissingRow is R8-F3: only
// the current history row's digest is changed. The row is still there, so
// it is reported as history_mismatch, not as the missing row whose recovery
// takes two publications and an expiry, and the intact manifest is not to
// be restored to match the row: it verifies against the record, and reads
// succeed. The publication that replaces the generation writes the row back
// from the record (R9-F3), so one publication resumes collection, and the
// generation is kept for the grace after its retirement like any other.
func TestAHistoryRowThatDisagreesWithTheRecordIsNotAMissingRow(t *testing.T) {
	f := newFixture(t)
	g1 := f.publishInit(t, f.initialized(t), 1)
	f.exec(t, "UPDATE %s SET manifest_sha256 = '"+strings.Repeat("0", 64)+"' WHERE generation = 1", f.tables.Generation)

	rep := f.pass(t, testOptions(false))
	require.NoError(t, rep.Err)
	require.Equal(t, []FindingKind{HistoryMismatch}, kinds(rep.Findings), "the record's manifest verifies against the record")
	require.Equal(t, actionHistoryMismatch, rep.Findings[0].Action)
	require.Contains(t, rep.Findings[0].Detail, "present but disagrees with the record")
	require.Contains(t, rep.Findings[0].Detail, "verifies against it, as readers verify it, so reads succeed")
	var out bytes.Buffer
	Report{Schemas: []SchemaReport{rep}}.Render(&out)
	for _, step := range []string{
		"history_mismatch",
		"Do not rewrite either to make them agree, do not restore the row from a backup",
		"re-initializing with cdc-init restores them",
		"writes the row back from the record it replaces and clears its stamps",
	} {
		require.Contains(t, out.String(), step)
	}
	require.NotContains(t, out.String(), "takes two publications", "the missing row's recovery is not promised")

	reinit := f.publishInit(t, g1.rec, 1)
	rep = f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Findings, "the replacement wrote the row back from the record")
	require.Equal(t, []int64{g1.rec.Generation}, rep.Collected.Retired)
	require.Empty(t, rep.Collected.Objects)
	require.Subset(t, f.store.keys(), f.keysOf(g1), "kept for the grace after its retirement")

	waitOutGrace()
	rep = f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Findings)
	require.Equal(t, []string{g1.rec.ManifestKey}, rep.Collected.Manifests, "collection resumed")
	require.Equal(t, f.keysOf(reinit), f.store.keys())
}

// TestAHistoryRowStampedSupersededWhileCurrent is R8-F3's other side: the
// current row agrees with the record and its manifest but carries a
// superseded_at no transition wrote. The collector never retires the
// current generation's row, whose grace would otherwise run while readers
// still choose it; once a publication replaced it the row is retired then
// and expires like any other.
func TestAHistoryRowStampedSupersededWhileCurrent(t *testing.T) {
	f := newFixture(t)
	g1 := f.publishInit(t, f.initialized(t), 1)
	f.exec(t, "UPDATE %s SET superseded_at = 1 WHERE generation = 1", f.tables.Generation)

	rep := f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Equal(t, []FindingKind{HistoryMismatch}, kinds(rep.Findings))
	require.Equal(t, actionHistoryMismatch, rep.Findings[0].Action)
	require.Empty(t, rep.Collected.Retired, "the current generation is not retired")
	waitOutGrace()
	require.Empty(t, f.pass(t, testOptions(true)).Collected.Retired)

	f.publish(t, f.stageFlush(t, g1, 1))
	rep = f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Findings)
	require.Equal(t, []int64{g1.rec.Generation}, rep.Collected.Retired, "retired once a publication replaced it")
	require.Empty(t, rep.Collected.Manifests, "kept for the grace after its retirement")
	require.Subset(t, f.store.keys(), f.keysOf(g1))

	waitOutGrace()
	rep = f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Findings)
	require.Equal(t, []string{g1.rec.ManifestKey}, rep.Collected.Manifests, "collection resumed")
}

// pin opens a reader's snapshot and answers the record it chose, after it
// fetched and verified the record's manifest as a reader does.
func (f *fixture) pin(t *testing.T) (*sql.Tx, publication.Record) {
	t.Helper()
	ctx := context.Background()
	reader, err := f.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	require.NoError(t, err)
	t.Cleanup(func() { _ = reader.Rollback() })
	pinned, err := publication.Read(ctx, publication.SQLTx(reader), f.tables, testSchema)
	require.NoError(t, err)
	f.requireReadable(t, pinned.Record)
	return reader, pinned.Record
}

// requireReadable asserts that a reader pinned to rec still fetches and
// verifies its manifest and every object it lists.
func (f *fixture) requireReadable(t *testing.T, rec publication.Record) {
	t.Helper()
	ctx := context.Background()
	ref, err := manifestv2.RecordRef(rec)
	require.NoError(t, err)
	raw, err := f.store.Get(ctx, rec.ManifestKey)
	require.NoError(t, err, "the pinned generation's manifest is there")
	m, err := manifestv2.Verify(raw, ref)
	require.NoError(t, err)
	for _, key := range m.Keys() {
		ok, err := f.store.Exists(ctx, key)
		require.NoError(t, err)
		require.True(t, ok, "the pinned generation's object %s is there", key)
	}
}

// TestAnEarlyRetirementStampDoesNotSurviveTheReplacement is R9-F2: the
// current generation N carries a retired_at no collector wrote, already
// past the grace, so its row is among the expired ones. The diagnosis
// reports the present row as history_mismatch, not as a missing one. A
// reader pins N, and N+1 replaces every file N lists. The replacement
// clears the stamp, so the collections that follow, inside the reader's
// deadline, keep N's manifest and every object it lists, although they
// are all old enough to be orphans: N is retired by the pass that saw it
// replaced, and expires the grace after that.
func TestAnEarlyRetirementStampDoesNotSurviveTheReplacement(t *testing.T) {
	f := newFixture(t)
	opts := agedOptions(true)
	g1 := f.publishInit(t, f.initialized(t), 1)
	g2 := f.publish(t, f.stageFlush(t, g1, 1))
	_, err := f.db.Exec(fmt.Sprintf("UPDATE %s SET retired_at = $1 WHERE generation = $2", f.tables.Generation),
		f.serverMillis(t)-2*time.Hour.Milliseconds(), g2.rec.Generation)
	require.NoError(t, err)

	rep := f.pass(t, agedOptions(false))
	require.NoError(t, rep.Err)
	require.Equal(t, []FindingKind{HistoryMismatch}, kinds(rep.Findings), "the present row, not a missing one")
	require.Equal(t, g2.rec.Generation, rep.Findings[0].Generation)
	require.Contains(t, rep.Findings[0].Detail, "so reads succeed")

	reader, pinned := f.pin(t)
	require.Equal(t, g2.rec.Generation, pinned.Generation, "the reader chose N")
	g3 := f.publishCompact(t, g2)
	f.ageStamps(t, time.Hour) // only N's row keeps its objects, not their age
	for pass := range 2 {
		rep := f.pass(t, opts)
		require.NoError(t, rep.Err)
		require.Empty(t, rep.Findings, "pass %d", pass)
		require.Empty(t, rep.Collected.Objects, "pass %d deletes nothing N lists", pass)
		require.Empty(t, rep.Collected.Manifests, "pass %d keeps N's manifest", pass)
		f.requireOrphansEligible(t, rep, opts, f.keysOf(g2))
		require.Empty(t, rep.Collected.Forgotten, "pass %d", pass)
		if pass == 0 {
			require.Equal(t, []int64{g1.rec.Generation, g2.rec.Generation}, rep.Collected.Retired, "N is retired after its replacement")
		}
	}
	f.requireReadable(t, pinned)
	require.Subset(t, f.store.keys(), append(f.keysOf(g2), f.keysOf(g3)...))
	require.NoError(t, reader.Commit())
}

// TestAHistoryRowNamingAnotherManifestKeepsTheRecordsInventory is R9-F3:
// the current generation N's history row names another manifest, a valid
// one a losing publication staged for the same generation, while the
// record names the manifest that committed. The diagnosis checks the
// record's manifest against the record, as readers do, and not the row's
// bytes: intact, reads succeed; corrupted, the invalid_manifest names the
// record's key. A reader pins N through the record, and N+1 replaces every
// file it lists. The replacement writes N's row back from the record, so
// the collections that follow keep the manifest and the objects the reader
// scans, and collect the losing publication's staging, which no reader
// ever chose.
func TestAHistoryRowNamingAnotherManifestKeepsTheRecordsInventory(t *testing.T) {
	f := newFixture(t)
	opts := agedOptions(true)
	g1 := f.publishInit(t, f.initialized(t), 1)
	won, lost := f.stageFlush(t, g1, 1), f.stageFlush(t, g1, 1)
	g2 := f.publish(t, won)
	_, err := f.db.Exec(fmt.Sprintf("UPDATE %s SET manifest_key = $1, manifest_sha256 = $2, cas_token = $3 WHERE generation = $4",
		f.tables.Generation), lost.next.ManifestKey, lost.next.ManifestSHA256, lost.next.Token, g2.rec.Generation)
	require.NoError(t, err)

	rep := f.pass(t, agedOptions(false))
	require.NoError(t, rep.Err)
	require.Equal(t, []FindingKind{HistoryMismatch}, kinds(rep.Findings))
	require.Contains(t, rep.Findings[0].Detail, "so reads succeed")
	intact, err := f.store.Get(context.Background(), g2.rec.ManifestKey)
	require.NoError(t, err)
	f.corrupt(t, g2.rec.ManifestKey)
	rep = f.pass(t, agedOptions(false))
	require.NoError(t, rep.Err)
	require.Equal(t, []FindingKind{HistoryMismatch, InvalidManifest}, kinds(rep.Findings))
	require.Equal(t, g2.rec.ManifestKey, rep.Findings[1].Key, "the record's manifest, which readers read, not the row's")
	require.True(t, rep.Findings[1].Current)
	require.Contains(t, rep.Findings[0].Detail, "could not verify the manifest the record names")
	f.store.set(g2.rec.ManifestKey, intact, f.serverMillis(t))

	reader, pinned := f.pin(t)
	require.Equal(t, g2.rec.ManifestKey, pinned.ManifestKey, "the reader chose N through the record")
	g3 := f.publishCompact(t, g2)
	f.ageStamps(t, time.Hour)
	for pass := range 2 {
		rep := f.pass(t, opts)
		require.NoError(t, rep.Err)
		require.Empty(t, rep.Findings, "pass %d", pass)
		if pass == 0 {
			require.ElementsMatch(t, f.stagedKeys(lost, g1), rep.Collected.Objects, "only the losing staging is collected")
		} else {
			require.Empty(t, rep.Collected.Objects, "pass %d", pass)
		}
		require.Empty(t, rep.Collected.Manifests, "pass %d", pass)
		f.requireOrphansEligible(t, rep, opts, f.keysOf(g2))
	}
	f.requireReadable(t, pinned)
	require.Subset(t, f.store.keys(), append(f.keysOf(g2), f.keysOf(g3)...))
	require.NoError(t, reader.Commit())
}
