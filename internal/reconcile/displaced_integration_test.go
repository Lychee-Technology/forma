package reconcile

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// TestAChangedRecordKeepsTheInventoryAReaderPinned is R10-F1, the side of
// R9-F3 where the record, not the history row, is what changed. A reader
// pins generation N through the record and verifies its manifest; then only
// the record is changed, to name a valid manifest a losing publication
// staged for N. The diagnosis reports history_mismatch. A normal
// publication, or a drop, replaces N and writes N's row back from the record
// it replaced, which names the losing manifest; the manifest the reader
// verified is then in no history row. The replacement kept it as a displaced
// inventory, so the collections that follow inside the reader's deadline
// delete nothing for the schema, although every key of N is old enough to be
// an orphan, and the reader still fetches and verifies what it pinned. Only
// once N's row expired, a full grace after a pass retired it, does a pass
// collect both of N's inventories.
func TestAChangedRecordKeepsTheInventoryAReaderPinned(t *testing.T) {
	for name, replace := range map[string]func(*testing.T, *fixture, publication.Record) []string{
		"publication": func(t *testing.T, f *fixture, cur publication.Record) []string {
			return f.keysOf(f.publishInit(t, cur, 1))
		},
		"drop": func(t *testing.T, f *fixture, cur publication.Record) []string {
			require.NoError(t, f.inTx(t, func(tx publication.Tx) error {
				_, err := publication.Reset(context.Background(), tx, f.tables, cur)
				return err
			}))
			f.ageLifecycle(t, time.Hour) // the drop's own lifecycle fence is not what keeps N
			return nil
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			opts := agedOptions(true)
			g1 := f.publishInit(t, f.initialized(t), 1)
			won, lost := f.stageFlush(t, g1, 1), f.stageFlush(t, g1, 1)
			g2 := f.publish(t, won)
			reader, pinned := f.pin(t)
			require.Equal(t, g2.rec.ManifestKey, pinned.ManifestKey, "the reader chose N's committed manifest")
			_, err := f.db.Exec(fmt.Sprintf("UPDATE %s SET manifest_key = $1, manifest_sha256 = $2, cas_token = $3 WHERE schema_id = $4",
				f.tables.Record), lost.next.ManifestKey, lost.next.ManifestSHA256, lost.next.Token, testSchema)
			require.NoError(t, err)

			rep := f.pass(t, agedOptions(false))
			require.NoError(t, rep.Err)
			require.Equal(t, []FindingKind{HistoryMismatch}, kinds(rep.Findings))
			require.Contains(t, rep.Findings[0].Detail, readsManifestVerified, "the changed record names a valid manifest of N")

			remaining := replace(t, f, f.record(t))
			f.ageStamps(t, time.Hour) // only the displacement keeps N's objects, not their age
			for pass := range 2 {
				rep := f.pass(t, opts)
				require.NoError(t, rep.Err)
				require.Empty(t, rep.Findings, "pass %d", pass)
				require.Contains(t, rep.Refused, "disagreed with the record when the generation was replaced", "pass %d", pass)
				require.Contains(t, rep.Refused, g2.rec.ManifestKey, "pass %d names the displaced inventory", pass)
				require.Empty(t, rep.Collected.Objects, "pass %d", pass)
				require.Empty(t, rep.Collected.Manifests, "pass %d", pass)
				require.Empty(t, rep.Collected.Forgotten, "pass %d", pass)
				f.requireOrphansEligible(t, rep, opts, f.keysOf(g2))
				if pass == 0 {
					require.Contains(t, rep.Collected.Retired, g2.rec.Generation, "N is retired after its replacement")
				}
			}
			f.requireReadable(t, pinned)
			require.Subset(t, f.store.keys(), append(f.keysOf(g2), f.stagedKeys(lost, g1)...))
			require.NoError(t, reader.Commit())

			f.ageStamps(t, time.Hour) // N's row expired: no reader of either inventory is left
			rep = f.pass(t, opts)
			require.NoError(t, rep.Err)
			require.Empty(t, rep.Refused)
			require.Contains(t, rep.Collected.Manifests, g2.rec.ManifestKey, "the displaced manifest is collected as an expired one")
			require.Contains(t, rep.Collected.Manifests, lost.next.ManifestKey, "the row's manifest expired with N")
			slices.Sort(remaining)
			require.Equal(t, remaining, f.store.keys())
			displaced, err := publication.Displaced(context.Background(), publication.DB(f.db), f.tables, testSchema)
			require.NoError(t, err)
			require.Empty(t, displaced, "a displacement no longer live is forgotten")
		})
	}
}

// TestADroppedSchemaFinishesCollectingItsDisplacedInventory is R11-F1 and
// the drop of R13-F1: the retention grace is shorter than the orphan age,
// as with the CLI's defaults (15 minutes and 2 hours). A drop replaces
// generation N, whose record had been changed to name another manifest, and
// keeps the row's inventory as displaced. Once N's row expired, nothing
// else of the schema is left to verify in the bucket, and the displaced
// manifest and the object only it lists are still younger than the orphan
// cutoff. A verified displaced manifest is no proof that no publication can
// still commit what it lists, so the passes keep both (Plan.Deferred) and
// keep the displacement, the evidence that names them, rather than delete
// the manifest and leave the object unnamed. Once both are below the
// cutoff, a pass collects them and forgets the displacement only after they
// are gone: a pass whose delete of the displaced manifest fails leaves the
// displacement, and the next pass, with no verified history manifest left,
// finishes the collection.
func TestADroppedSchemaFinishesCollectingItsDisplacedInventory(t *testing.T) {
	f := newFixture(t)
	opts := Options{GC: true, RetentionGrace: time.Minute, ReaderDeadline: 50 * time.Second,
		ClockMargin: 50 * time.Millisecond, OrphanAge: 2 * time.Hour}
	g1 := f.publishInit(t, f.initialized(t), 1)
	won, lost := f.stageFlush(t, g1, 1), f.stageFlush(t, g1, 1)
	g2 := f.publish(t, won)
	_, err := f.db.Exec(fmt.Sprintf("UPDATE %s SET manifest_key = $1, manifest_sha256 = $2, cas_token = $3 WHERE schema_id = $4",
		f.tables.Record), lost.next.ManifestKey, lost.next.ManifestSHA256, lost.next.Token, testSchema)
	require.NoError(t, err)
	require.NoError(t, f.inTx(t, func(tx publication.Tx) error {
		_, err := publication.Reset(context.Background(), tx, f.tables, f.record(t))
		return err
	}))
	f.ageLifecycle(t, time.Hour)

	rep := f.pass(t, opts)
	require.NoError(t, rep.Err)
	require.Contains(t, rep.Refused, g2.rec.ManifestKey, "the displaced inventory is live inside the grace")
	require.ElementsMatch(t, []int64{g1.rec.Generation, g2.rec.Generation}, rep.Collected.Retired)

	f.ageRetirements(t, time.Hour) // past the grace, while every object is younger than the orphan age
	displacedOnly := f.stagedKeys(won, g1)
	objectsOnly := slices.Sorted(slices.Values(displacedOnly[1:])) // stagedKeys answers the manifest first
	slices.Sort(displacedOnly)
	for pass := range 2 {
		rep = f.pass(t, opts)
		require.NoError(t, rep.Err, "pass %d", pass)
		require.Empty(t, rep.Refused, "pass %d: the displacement's verified manifest is the schema's evidence in the bucket", pass)
		require.Equal(t, displacedOnly, rep.Plan.Deferred, "pass %d: what only the displaced inventory names is no orphan yet", pass)
		require.Equal(t, displacedOnly, f.store.keys(), "pass %d: the expired generations are collected, the displaced inventory kept", pass)
		displaced, err := publication.Displaced(context.Background(), publication.DB(f.db), f.tables, testSchema)
		require.NoError(t, err)
		require.Len(t, displaced, 1, "pass %d: the displacement is kept with the keys it names", pass)
	}

	f.ageStamps(t, 3*time.Hour) // the displaced inventory's keys are below the orphan cutoff now
	f.store.failDelete = func(key string) error {
		if key == g2.rec.ManifestKey {
			return errors.New("injected delete failure")
		}
		return nil
	}
	rep = f.pass(t, opts)
	require.ErrorContains(t, rep.Err, "delete the expired manifests")
	require.Empty(t, rep.Plan.Deferred)
	require.Equal(t, objectsOnly, rep.Collected.Objects, "the object only the displaced inventory lists, once below the cutoff")
	require.Equal(t, []string{g2.rec.ManifestKey}, f.store.keys(), "only the displaced manifest whose delete failed is left")
	displaced, err := publication.Displaced(context.Background(), publication.DB(f.db), f.tables, testSchema)
	require.NoError(t, err)
	require.Len(t, displaced, 1, "a displacement is not forgotten before its manifest is deleted")

	f.store.failDelete = nil
	rep = f.pass(t, opts)
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Refused, "the displacement's verified manifest is the schema's evidence in the bucket")
	require.Equal(t, []string{g2.rec.ManifestKey}, rep.Collected.Manifests)
	require.Empty(t, f.store.keys(), "the drop's collection finished")
	displaced, err = publication.Displaced(context.Background(), publication.DB(f.db), f.tables, testSchema)
	require.NoError(t, err)
	require.Empty(t, displaced, "forgotten once its manifest is gone")

	rep = f.pass(t, opts)
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Findings)
	require.Empty(t, rep.Refused, "nothing is left to delete, and nothing is refused")
	require.Empty(t, rep.Plan.Objects)
	require.Empty(t, rep.Plan.Manifests)
}

// ageRetirements moves every retirement stamp of the schema's history back
// by d, leaving the objects' LastModified as they are.
func (f *fixture) ageRetirements(t *testing.T, d time.Duration) {
	t.Helper()
	_, err := f.db.Exec(fmt.Sprintf("UPDATE %s SET retired_at = retired_at - $1 WHERE schema_id = $2 AND retired_at <> 0",
		f.tables.Generation), d.Milliseconds(), testSchema)
	require.NoError(t, err)
}

// TestADisplacedManifestNamingAnotherEpochDeletesNothing is R12-F1: the
// R11-F1 drop, but the displaced manifest, whose digest the displacement
// row was rewritten to match, also lists an object of the epoch the drop
// moved to, staged by a publication that may still commit and far above
// the orphan cutoff. Its digest and identity pass the collector's own check
// (publication.ManifestFiles); the reader's full check refuses a listed key
// of another epoch. Once N's row expired, every pass reports the displaced
// manifest as invalid_manifest, deletes nothing, forgets nothing and is not
// clean, so the staged object survives.
func TestADisplacedManifestNamingAnotherEpochDeletesNothing(t *testing.T) {
	f := newFixture(t)
	opts := Options{GC: true, RetentionGrace: time.Minute, ReaderDeadline: 50 * time.Second,
		ClockMargin: 50 * time.Millisecond, OrphanAge: 2 * time.Hour}
	g1 := f.publishInit(t, f.initialized(t), 1)
	won, lost := f.stageFlush(t, g1, 1), f.stageFlush(t, g1, 1)
	g2 := f.publish(t, won)
	_, err := f.db.Exec(fmt.Sprintf("UPDATE %s SET manifest_key = $1, manifest_sha256 = $2, cas_token = $3 WHERE schema_id = $4",
		f.tables.Record), lost.next.ManifestKey, lost.next.ManifestSHA256, lost.next.Token, testSchema)
	require.NoError(t, err)
	require.NoError(t, f.inTx(t, func(tx publication.Tx) error {
		_, err := publication.Reset(context.Background(), tx, f.tables, f.record(t))
		return err
	}))
	f.ageLifecycle(t, time.Hour)
	dropped := f.record(t)
	require.Greater(t, dropped.Epoch, g2.rec.Epoch)
	staged := f.file(t, dropped.Epoch, manifestv2.TierBase) // a publication at the new epoch, in flight

	raw, err := f.store.Get(context.Background(), g2.rec.ManifestKey)
	require.NoError(t, err)
	var doc map[string]any
	require.NoError(t, json.Unmarshal(raw, &doc))
	doc["files"] = append(doc["files"].([]any), map[string]any{"key": staged.Key})
	forged, err := json.Marshal(doc)
	require.NoError(t, err)
	f.store.set(g2.rec.ManifestKey, forged, f.serverMillis(t))
	_, err = f.db.Exec(fmt.Sprintf("UPDATE %s SET manifest_sha256 = $1 WHERE schema_id = $2 AND manifest_key = $3", f.tables.Displaced),
		manifestv2.Digest(forged), testSchema, g2.rec.ManifestKey)
	require.NoError(t, err)
	displaced, err := publication.Displaced(context.Background(), publication.DB(f.db), f.tables, testSchema)
	require.NoError(t, err)
	require.Len(t, displaced, 1)
	files, err := publication.ManifestFiles(forged, testSchema, displaced[0].Inventory())
	require.NoError(t, err, "the digest and identity pass the collector's own check")
	require.Contains(t, files, staged.Key)

	rep := f.pass(t, opts)
	require.NoError(t, rep.Err)
	require.Contains(t, rep.Refused, g2.rec.ManifestKey, "the displaced inventory is live inside the grace")
	f.ageRetirements(t, time.Hour) // N's row expired; the staged object is still young
	before := f.store.keys()
	for pass := range 2 {
		rep := f.pass(t, opts)
		require.NoError(t, rep.Err, "pass %d", pass)
		require.Equal(t, []FindingKind{InvalidManifest}, kinds(rep.Findings), "pass %d", pass)
		require.Equal(t, g2.rec.ManifestKey, rep.Findings[0].Key)
		require.Equal(t, actionInvalidDisplaced, rep.Findings[0].Action)
		require.Contains(t, rep.Findings[0].Detail, "does not verify against the displacement")
		require.Contains(t, rep.Refused, string(InvalidManifest))
		require.Empty(t, rep.Collected.Objects, "pass %d", pass)
		require.Empty(t, rep.Collected.Manifests, "pass %d", pass)
		require.Empty(t, rep.Collected.Forgotten, "pass %d", pass)
		require.True(t, rep.Residual(), "invalid displaced evidence is not clean")
		var out bytes.Buffer
		Report{Schemas: []SchemaReport{rep}}.Render(&out)
		require.Contains(t, out.String(), "needs attention")
		require.NotContains(t, out.String(), ": clean (")
	}
	require.Equal(t, before, f.store.keys(), "nothing is deleted")
	require.Contains(t, f.store.keys(), staged.Key, "the other epoch's staged object survives")
	displaced, err = publication.Displaced(context.Background(), publication.DB(f.db), f.tables, testSchema)
	require.NoError(t, err)
	require.Len(t, displaced, 1, "the displacement is not forgotten")
}

// TestADisplacedManifestDoesNotDeleteSameEpochStaging is R13-F1: the
// R12-F1 forgery, but in the same epoch, where the reader's full check
// cannot catch it. A normal publication replaces generation N, whose record
// had been changed to name another manifest, and keeps the row's inventory
// as displaced. A compaction of the new head N+1 then stages a base object
// and has not committed yet. The displaced manifest, whose digest the
// displacement row was rewritten to match, also lists that object as a
// fully formed entry of the same epoch: a Parquet key spells the schema,
// epoch and tier, not the attempt that staged it, so the manifest passes
// manifestv2.VerifyDisplaced. Once N's row expired, the pass keeps the
// staged object, which is not yet below the orphan cutoff, and keeps the
// displaced manifest and its displacement with it (Plan.Deferred). The
// compaction then commits, and its generation is readable. The next pass
// finds the object protected, deletes the displaced manifest and forgets
// the displacement.
func TestADisplacedManifestDoesNotDeleteSameEpochStaging(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	opts := Options{GC: true, RetentionGrace: time.Minute, ReaderDeadline: 50 * time.Second,
		ClockMargin: 50 * time.Millisecond, OrphanAge: 2 * time.Hour}
	g1 := f.publishInit(t, f.initialized(t), 1)
	won, lost := f.stageFlush(t, g1, 1), f.stageFlush(t, g1, 1)
	g2 := f.publish(t, won)
	_, err := f.db.Exec(fmt.Sprintf("UPDATE %s SET manifest_key = $1, manifest_sha256 = $2, cas_token = $3 WHERE schema_id = $4",
		f.tables.Record), lost.next.ManifestKey, lost.next.ManifestSHA256, lost.next.Token, testSchema)
	require.NoError(t, err)
	g3 := f.publishCompact(t, generation{rec: f.record(t), manifest: lost.manifest})
	require.Equal(t, g2.rec.Epoch, g3.rec.Epoch, "no drop: one epoch throughout")
	f.ageStamps(t, 3*time.Hour) // every object so far is old enough to be an orphan

	inFlight := f.stage(t, g3.rec, publication.KindCompact, func(h manifestv2.Header) (manifestv2.Manifest, error) {
		h.InputGeneration = g3.rec.Generation
		return manifestv2.NewCompact(&g3.manifest, h, g3.manifest.Keys(), []manifestv2.File{f.file(t, h.Epoch, manifestv2.TierBase)})
	})
	staged := inFlight.manifest.Files[0]
	require.Equal(t, g3.rec.Epoch, inFlight.manifest.Epoch)

	forged := g2.manifest
	forged.Files = append(slices.Clone(forged.Files), staged)
	raw, digest, err := forged.Encode()
	require.NoError(t, err)
	f.store.set(g2.rec.ManifestKey, raw, f.serverMillis(t)-(3*time.Hour).Milliseconds())
	_, err = f.db.Exec(fmt.Sprintf("UPDATE %s SET manifest_sha256 = $1 WHERE schema_id = $2 AND manifest_key = $3", f.tables.Displaced),
		digest, testSchema, g2.rec.ManifestKey)
	require.NoError(t, err)
	displaced, err := publication.Displaced(ctx, publication.DB(f.db), f.tables, testSchema)
	require.NoError(t, err)
	require.Len(t, displaced, 1)
	m, err := manifestv2.VerifyDisplaced(raw, testSchema, displaced[0].Inventory())
	require.NoError(t, err, "a fully formed same-epoch entry passes the reader's full check")
	require.Contains(t, m.Keys(), staged.Key)

	rep := f.pass(t, opts)
	require.NoError(t, rep.Err)
	require.Contains(t, rep.Refused, g2.rec.ManifestKey, "the displaced inventory is live inside the grace")
	f.ageRetirements(t, time.Hour) // N's row expired; the staged object is still young
	for pass := range 2 {
		rep := f.pass(t, opts)
		require.NoError(t, rep.Err, "pass %d", pass)
		require.Empty(t, rep.Findings, "pass %d", pass)
		require.Empty(t, rep.Refused, "pass %d", pass)
		require.Equal(t, []string{g2.rec.ManifestKey, staged.Key}, rep.Plan.Deferred,
			"pass %d: a verified displaced manifest is no proof the staging can no longer commit", pass)
		require.NotContains(t, rep.Collected.Objects, staged.Key, "pass %d", pass)
		require.NotContains(t, rep.Collected.Manifests, g2.rec.ManifestKey, "pass %d", pass)
		require.Contains(t, f.store.keys(), staged.Key, "pass %d: the in-flight compaction's object survives", pass)
		require.Positive(t, rep.PendingOrphans, "pass %d", pass)
		var out bytes.Buffer
		Report{Schemas: []SchemaReport{rep}}.Render(&out)
		require.Contains(t, out.String(), "kept: "+staged.Key)
		displaced, err := publication.Displaced(ctx, publication.DB(f.db), f.tables, testSchema)
		require.NoError(t, err)
		require.Len(t, displaced, 1, "pass %d: the displacement is kept with the keys it names", pass)
	}

	g4 := f.publish(t, inFlight) // its compare-and-set still commits
	f.requireReadable(t, g4.rec)

	rep = f.pass(t, opts)
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Refused)
	require.Empty(t, rep.Plan.Deferred, "the object is the new head's now")
	require.Contains(t, rep.Collected.Manifests, g2.rec.ManifestKey)
	f.requireReadable(t, g4.rec)
	displaced, err = publication.Displaced(ctx, publication.DB(f.db), f.tables, testSchema)
	require.NoError(t, err)
	require.Empty(t, displaced, "forgotten once its manifest is gone")
}
