package reconcile

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/testdb"
)

// TestCollectorRefusesAHistoryGapAPinnedReaderCouldHold: a reader pins
// generation N, N's history row is lost, and N+1, a compaction, still
// publishes (its supersede of N matches no row). N's manifest and its delta
// are named by no row and older than the horizon, and the base N shares
// with generation 1 is listed only by generation 1 once that expires, yet
// no pass deletes any of them: the history has no row for a generation a
// reader could hold. Once a generation above N expires, N is past every
// reader, and the collection resumes and deletes them.
func TestCollectorRefusesAHistoryGapAPinnedReaderCouldHold(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	g1 := f.publishInit(t, f.initialized(t), 1)
	sN := f.stageFlush(t, g1, 1)
	gN := f.publish(t, sN)
	reader, err := f.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	require.NoError(t, err)
	defer func() { _ = reader.Rollback() }()
	pinned, err := publication.Read(ctx, publication.SQLTx(reader), f.tables, testSchema)
	require.NoError(t, err)
	require.Equal(t, gN.rec.Generation, pinned.Generation)

	_, err = f.db.Exec(fmt.Sprintf("DELETE FROM %s WHERE generation = $1", f.tables.Generation), gN.rec.Generation)
	require.NoError(t, err)
	gNext := f.publishCompact(t, gN)
	onlyN := f.stagedKeys(sN, g1)
	old := f.serverMillis(t) - time.Hour.Milliseconds()
	for _, key := range onlyN {
		raw, err := f.store.Get(ctx, key)
		require.NoError(t, err)
		f.store.set(key, raw, old) // below any horizon: only the proof protects them
	}

	for range 2 {
		rep := f.pass(t, testOptions(true))
		require.NoError(t, rep.Err)
		require.NotEmpty(t, rep.Refused)
		require.Equal(t, HistoryGap, rep.Findings[0].Kind)
		require.Equal(t, gN.rec.Generation, rep.Findings[0].Generation)
		require.Equal(t, actionHistoryGap, rep.Findings[0].Action)
		require.Empty(t, rep.Collected.Objects)
		require.Empty(t, rep.Collected.Manifests)
		require.Empty(t, rep.Collected.Forgotten)
		waitOutGrace() // generation 1 expires: below N, it proves nothing about N
	}
	ref, err := manifestv2.RecordRef(pinned.Record)
	require.NoError(t, err)
	raw, err := f.store.Get(ctx, pinned.ManifestKey)
	require.NoError(t, err, "the pinned generation's manifest is still there")
	m, err := manifestv2.Verify(raw, ref)
	require.NoError(t, err)
	for _, key := range m.Keys() {
		ok, err := f.store.Exists(ctx, key)
		require.NoError(t, err)
		require.True(t, ok, "the pinned generation's object %s is still there", key)
	}
	require.NoError(t, reader.Commit())

	f.publish(t, f.stageFlush(t, gNext, 1))
	f.pass(t, testOptions(true)) // retires N+1
	waitOutGrace()
	rep := f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Refused, "N+1 expired above the gap, so N is past every reader")
	require.Empty(t, rep.Findings)
	require.Subset(t, rep.Collected.Objects, onlyN, "N's manifest and delta, which no row names, are collected")
	require.Subset(t, rep.Collected.Objects, g1.manifest.Keys(), "and so is the base N shared with generation 1")
}

// TestCollectorKeepsAnObjectOfUnknownAge: a listing that answers a V2
// staging key without a usable LastModified fails the pass, which deletes
// nothing; the staging's publication still commits.
func TestCollectorKeepsAnObjectOfUnknownAge(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	g1 := f.publishInit(t, f.initialized(t), 1)
	s := f.stageFlush(t, g1, 1)
	for _, key := range f.stagedKeys(s, g1) {
		raw, err := f.store.Get(ctx, key)
		require.NoError(t, err)
		f.store.set(key, raw, 0)
	}
	before := f.store.keys()
	for _, gc := range []bool{false, true} {
		opts := testOptions(gc)
		opts.OrphanAge = time.Hour // the writer is above the horizon and may still commit
		rep := f.pass(t, opts)
		require.ErrorIs(t, rep.Err, ErrUnknownAge)
		require.Equal(t, before, f.store.keys(), "nothing is deleted")
	}
	_, err := f.commit(t, s)
	require.NoError(t, err, "the staging's publication still commits")
}

// TestDiagnosisRunsAsAReadOnlyPrincipal runs the diagnosis on a database
// principal that may only SELECT the publication tables, with the Aurora
// DSQL schema lock wired, whose lease is a row write: the diagnosis
// succeeds, and the same principal's collection fails on its first write.
func TestDiagnosisRunsAsAReadOnlyPrincipal(t *testing.T) {
	f := newFixture(t)
	collectable(t, f)
	ro := f.readOnlyDB(t)
	r := f.reconciler(testOptions(false))
	r.DB = ro
	locker, err := cdc.NewSchemaLocker(ro, forma.EngineAuroraDSQL, "gc_ro_lock")
	require.NoError(t, err)
	r.Locker = locker
	before := f.store.keys()
	report, err := r.Run(context.Background())
	require.NoError(t, err)
	rep := report.Schemas[0]
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Findings)
	require.Equal(t, []int64{1}, rep.Plan.Forget)
	require.Equal(t, before, f.store.keys())

	r.Opts.GC = true
	report, err = r.Run(context.Background())
	require.NoError(t, err)
	require.Error(t, report.Schemas[0].Err, "the principal cannot write, so a collection fails")
	require.Equal(t, before, f.store.keys())
}

// TestDiagnosisLeavesOutWhatAConcurrentCollectionDeleted: a collection that
// runs while the diagnosis fetches manifests deletes the generations that
// expired since the diagnosis's snapshot. The diagnosis reports no missing
// manifest for them, and says how many findings it left out.
func TestDiagnosisLeavesOutWhatAConcurrentCollectionDeleted(t *testing.T) {
	f := newFixture(t)
	g1 := f.publishInit(t, f.initialized(t), 2)
	g2 := f.publish(t, f.stageFlush(t, g1, 1))
	f.publishCompact(t, g2)
	require.Equal(t, []int64{1, 2}, f.pass(t, testOptions(true)).Collected.Retired)

	fired := false
	var concurrent SchemaReport
	f.store.beforeGet = func(string) {
		if fired {
			return
		}
		fired = true
		waitOutGrace()
		concurrent = f.pass(t, testOptions(true))
	}
	rep := f.pass(t, testOptions(false))
	require.True(t, fired)
	require.NoError(t, concurrent.Err)
	require.Len(t, concurrent.Collected.Manifests, 2, "the concurrent collection deleted both expired manifests")
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Findings, "the diagnosis does not report what the collection deleted as loss")
	require.Equal(t, 2, rep.ConcurrentlyExpired)
	require.Contains(t, rep.Refused, "run the diagnosis again", "the snapshot's plan is stale, and says so")
	require.True(t, rep.Inconclusive())
	require.True(t, rep.Residual(), "the diagnosis established neither loss nor collection: it is not clean")
	var out bytes.Buffer
	Report{Schemas: []SchemaReport{rep}}.Render(&out)
	require.Contains(t, out.String(), "2 finding(s) left out")
	require.Contains(t, out.String(), "inconclusive (run the diagnosis again)")
	require.NotContains(t, out.String(), ": clean (")
}

// TestCollectionAfterALostRecordRetiresTheFormerHead runs the documented
// recovery of a lost record, init-db (InitializeAfterLoss) then cdc-init,
// and collects: the generation the lost record named is superseded by the
// recovery, retired by the first pass after it, kept for the grace, and
// then collected with every object only it listed.
func TestCollectionAfterALostRecordRetiresTheFormerHead(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	g1 := f.publishInit(t, f.initialized(t), 1)
	head := f.publish(t, f.stageFlush(t, g1, 1))
	f.exec(t, "DELETE FROM %s", f.tables.Record)
	require.NoError(t, f.inTx(t, func(tx publication.Tx) error {
		h, err := publication.ReadHistory(ctx, tx, f.tables, testSchema)
		if err != nil {
			return err
		}
		_, wrote, err := publication.InitializeAfterLoss(ctx, tx, f.tables, testSchema, h, publication.AwaitingInit)
		require.True(t, wrote)
		return err
	}))
	recovered := f.record(t)
	require.Equal(t, head.rec.Generation, recovered.Generation, "the counter resumes after the lost record's generation")
	reinit := f.publishInit(t, recovered, 1)

	rep := f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Findings)
	require.Equal(t, []int64{1, 2}, rep.Collected.Retired, "the former head is retired with the generation before it")
	require.Empty(t, rep.Collected.Objects, "nothing is deleted inside the grace")
	require.Subset(t, f.store.keys(), f.keysOf(head))

	waitOutGrace()
	rep = f.pass(t, testOptions(true))
	require.NoError(t, rep.Err)
	require.Empty(t, rep.Findings)
	require.Equal(t, []int64{1}, rep.Collected.Forgotten)
	require.Equal(t, f.keysOf(reinit), f.store.keys(), "only the re-init's generation remains")
}

// TestVerifyChecksumsChecksEveryEntryThatNamesAKey: a base object the
// current generation still lists and that went bad is reported against the
// current generation as well as the superseded one; and an entry only the
// current generation committed with another digest is reported against it,
// although the superseded generation's entry for the same key matches.
func TestVerifyChecksumsChecksEveryEntryThatNamesAKey(t *testing.T) {
	opts := testOptions(false)
	opts.VerifyChecksums = true

	f := newFixture(t)
	g1 := f.publishInit(t, f.initialized(t), 1)
	f.publish(t, f.stageFlush(t, g1, 1))
	shared := g1.manifest.Files[0].Key
	f.corrupt(t, shared)
	rep := f.pass(t, opts)
	require.NoError(t, rep.Err)
	require.Equal(t, []FindingKind{CorruptObject, CorruptObject}, kinds(rep.Findings))
	require.Equal(t, [2]any{int64(1), false}, [2]any{rep.Findings[0].Generation, rep.Findings[0].Current})
	require.Equal(t, [2]any{int64(2), true}, [2]any{rep.Findings[1].Generation, rep.Findings[1].Current})
	require.Equal(t, shared, rep.Findings[1].Key)

	f = newFixture(t)
	g1 = f.publishInit(t, f.initialized(t), 1)
	shared = g1.manifest.Files[0].Key
	f.publish(t, f.stage(t, g1.rec, publication.KindFlush, func(h manifestv2.Header) (manifestv2.Manifest, error) {
		m, err := manifestv2.NewFlush(&g1.manifest, h, []manifestv2.File{f.file(t, h.Epoch, manifestv2.TierDelta)})
		if err != nil {
			return m, err
		}
		for i := range m.Files {
			if m.Files[i].Key == shared {
				m.Files[i].SHA256 = manifestv2.Digest([]byte("other bytes"))
			}
		}
		return m, nil
	}))
	rep = f.pass(t, opts)
	require.NoError(t, rep.Err)
	require.Equal(t, []FindingKind{CorruptObject}, kinds(rep.Findings))
	require.Equal(t, [3]any{int64(2), true, shared}, [3]any{rep.Findings[0].Generation, rep.Findings[0].Current, rep.Findings[0].Key})
}

// readOnlyDB answers a handle on the test database as a fresh login role
// that may only SELECT the fixture's publication tables.
func (f *fixture) readOnlyDB(t *testing.T) *sql.DB {
	t.Helper()
	role := fmt.Sprintf("gc_ro_%d", time.Now().UnixNano())
	password := uuid.NewString()
	_, err := f.db.Exec(fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", role, password))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = f.db.Exec("DROP OWNED BY " + role)
		_, _ = f.db.Exec("DROP ROLE IF EXISTS " + role)
	})
	for _, table := range []string{f.tables.Record, f.tables.Generation, f.tables.Epoch, f.tables.Storage} {
		_, err := f.db.Exec(fmt.Sprintf("GRANT SELECT ON %s TO %s", table, role))
		require.NoError(t, err)
	}
	dsn, err := url.Parse(testdb.ResolveDSN())
	require.NoError(t, err)
	dsn.User = url.UserPassword(role, password)
	db, err := sql.Open("pgx", dsn.String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}
