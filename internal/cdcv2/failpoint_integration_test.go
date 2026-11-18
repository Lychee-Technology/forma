package cdcv2

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

var errInjected = errors.New("injected fault")

// once answers a failpoint that runs fn at the first hit of at, and passes
// every other hit.
func once(at Point, fn func(context.Context) error) func(context.Context, Point) error {
	var fired atomic.Bool
	return func(ctx context.Context, p Point) error {
		if p != at || !fired.CompareAndSwap(false, true) {
			return nil
		}
		return fn(ctx)
	}
}

func failWith(err error) func(context.Context) error {
	return func(context.Context) error { return err }
}

// published sets up a published schema with two dirty entries on top: one
// flushed generation, then two new rows.
func published(t *testing.T, f *fixture) {
	t.Helper()
	f.register(t)
	f.insert(t, 2)
	_, err := f.writer(t).Flush(context.Background(), schemaID, 1, &stager{f: f})
	require.NoError(t, err)
	f.insert(t, 2)
}

// TestFlushFaultAtEveryBoundaryLeavesOnlyOrphans fails a flush at each
// boundary before COMMIT: object staging, manifest upload, BEGIN, the
// compare-and-set and the marks. Each leaves the record, the history and
// every entry as they were (no mark without its pointer), at most orphan
// objects, and a later flush publishes the entries.
func TestFlushFaultAtEveryBoundaryLeavesOnlyOrphans(t *testing.T) {
	for _, at := range []Point{PointStageObjects, PointStageManifest, PointBegin, PointPublish, PointMarks, PointCommit} {
		t.Run(string(at), func(t *testing.T) {
			f := newFixture(t)
			ctx := context.Background()
			published(t, f)
			before, dirty, history := f.record(t), f.dirty(t), f.history(t)

			_, err := f.writer(t, func(c *Config) { c.Failpoint = once(at, failWith(errInjected)) }).Flush(ctx, schemaID, 1, &stager{f: f})
			require.ErrorIs(t, err, errInjected)
			require.Equal(t, before, f.record(t))
			require.Equal(t, dirty, f.dirty(t))
			require.Equal(t, history, f.history(t))
			f.checkInvariants(t)

			rep, err := f.writer(t).Flush(ctx, schemaID, 1, &stager{f: f})
			require.NoError(t, err)
			require.Equal(t, 2, rep.Marked)
			require.Empty(t, f.dirty(t))
			f.checkInvariants(t)
		})
	}
}

// TestFlushResolvesACommitWithoutAnAnswer covers both lost answers: a
// COMMIT that did not land is resent with the same token (NotCommitted), and
// one that landed is taken as committed (Committed). Neither stages a second
// manifest or commits a second generation.
func TestFlushResolvesACommitWithoutAnAnswer(t *testing.T) {
	for _, tc := range []struct {
		at      Point
		resends int
	}{{PointCommit, 1}, {PointCommitAnswer, 0}} {
		t.Run(string(tc.at), func(t *testing.T) {
			f := newFixture(t)
			ctx := context.Background()
			published(t, f)
			fp := once(tc.at, failWith(fmt.Errorf("connection reset: %w", ErrCommitAnswerLost)))
			if tc.at == PointCommitAnswer {
				fp = once(tc.at, failWith(errors.New("connection reset")))
			}
			rep, err := f.writer(t, func(c *Config) { c.Failpoint = fp }).Flush(ctx, schemaID, 1, &stager{f: f})
			require.NoError(t, err)
			require.Equal(t, tc.resends, rep.Resends)
			require.Equal(t, 2, rep.Marked, "one transaction's COMMIT went without an answer: its marks committed")
			require.Zero(t, rep.Unconfirmed)
			require.Len(t, rep.Generations, 1)
			head := f.record(t)
			require.Equal(t, int64(2), head.Generation)
			require.Equal(t, rep.Generations[0].Token, head.Token)
			require.Empty(t, f.dirty(t))
			manifests, _ := f.objects(t)
			require.Equal(t, len(f.history(t)), manifests, "the resend reused its staged manifest")
			f.checkInvariants(t)
		})
	}

	for _, tc := range []struct {
		name   string
		at     Point
		answer error
	}{
		{"the lost commit lands after its resend began", PointBegin, nil},
		{"the lost commit lands after the final resend's snapshot", PointPublish, serializationConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			published(t, f)
			dirty := f.dirty(t)
			w := f.writer(t, func(c *Config) { c.Attempts = 2; c.Failpoint = f.lateCommit(t, tc.at, tc.answer) })
			rep, err := w.Flush(context.Background(), schemaID, 1, &stager{f: f})
			require.NoError(t, err, "the resend's failure was its own commit landing, within the attempt cap")
			require.Equal(t, 1, rep.Resends)
			require.Zero(t, rep.Rebases, "the resend's miss was its own commit, not a rival's")
			require.Zero(t, rep.Restarts)
			require.Equal(t, 2, rep.Marked, "the lost COMMIT's marks, not the resend's")
			require.Zero(t, rep.Unconfirmed)
			require.Len(t, f.history(t), 2)
			head := f.record(t)
			require.Equal(t, rep.Generations[0].Token, head.Token)
			require.Empty(t, f.dirty(t))
			f.markedOnce(t, dirty, head.UpdatedAt)
			f.checkInvariants(t)
		})
	}
}

// TestFlushReportsTheMarksOfTheCommitThatLanded: the flush's COMMIT goes
// without an answer, the resend's marks fail (T_pub runs out in them), and the
// lost COMMIT lands before the next resend, which misses and finds the flush
// committed. The report counts the marks of the transaction that committed,
// not the failed resend's zero, and the database agrees on its own.
func TestFlushReportsTheMarksOfTheCommitThatLanded(t *testing.T) {
	f := newFixture(t)
	published(t, f)
	dirty := f.dirty(t)
	var commits, marks, begins atomic.Int32
	fp := func(ctx context.Context, p Point) error {
		switch {
		case p == PointCommit && commits.Add(1) == 1:
			return ErrCommitAnswerLost
		case p == PointMarks && marks.Add(1) == 2: // the resend
			<-ctx.Done() // T_pub runs out before its marks
		case p == PointBegin && begins.Add(1) == 3:
			f.landStaged(t) // the lost COMMIT lands
		}
		return nil
	}
	w := f.writer(t, func(c *Config) { c.PublishTimeout = 500 * time.Millisecond; c.Failpoint = fp })
	rep, err := w.Flush(context.Background(), schemaID, 1, &stager{f: f})
	require.NoError(t, err)
	require.Equal(t, int32(3), begins.Load())
	require.Equal(t, 2, rep.Resends)
	require.Zero(t, rep.Rebases)
	require.Equal(t, 2, rep.Marked, "the report counts the committed transaction's marks")
	require.Zero(t, rep.Unconfirmed)

	head := f.record(t)
	require.Equal(t, rep.Generations[0].Token, head.Token)
	require.Empty(t, f.dirty(t))
	f.markedOnce(t, dirty, head.UpdatedAt)
	f.checkInvariants(t)
}

// serializationConflict is the 40001 a server answers a transaction that
// lost a write conflict: an ERROR, which ends the transaction, not the
// session.
var serializationConflict = &pgconn.PgError{Severity: "ERROR", SeverityUnlocalized: "ERROR", Code: "40001", Message: "could not serialize access due to concurrent update"}

// sessionEndedWith answers the FATAL error with code a pooler answers a COMMIT
// it forwarded before it lost the backend: the session ended, and whether
// the COMMIT landed is unknown.
func sessionEndedWith(code string) error {
	return &pgconn.PgError{Severity: "FATAL", SeverityUnlocalized: "FATAL", Code: code, Message: "terminating connection"}
}

// TestFlushResolvesACommitWhoseSessionEnded: a COMMIT that landed and was
// answered with a FATAL error, even one carrying 40001, is resolved, found
// committed, and not sent again.
func TestFlushResolvesACommitWhoseSessionEnded(t *testing.T) {
	for _, code := range []string{"40001", "57P01"} {
		t.Run(code, func(t *testing.T) {
			f := newFixture(t)
			published(t, f)
			fp := once(PointCommitAnswer, failWith(sessionEndedWith(code)))
			rep, err := f.writer(t, func(c *Config) { c.Failpoint = fp }).Flush(context.Background(), schemaID, 1, &stager{f: f})
			require.NoError(t, err)
			require.Zero(t, rep.Resends)
			require.Zero(t, rep.Rebases)
			require.Len(t, rep.Generations, 1)
			require.Equal(t, rep.Generations[0].Token, f.record(t).Token)
			require.Empty(t, f.dirty(t))
			f.checkInvariants(t)
		})
	}
}

// TestFlushResendsASerializationFailureWithTheSameToken is DSQL's 40001 on a
// mark: the whole transaction aborts, and the same publication is sent
// again.
func TestFlushResendsASerializationFailureWithTheSameToken(t *testing.T) {
	f := newFixture(t)
	published(t, f)
	rep, err := f.writer(t, func(c *Config) { c.Failpoint = once(PointMarks, failWith(serializationConflict)) }).Flush(context.Background(), schemaID, 1, &stager{f: f})
	require.NoError(t, err)
	require.Equal(t, 1, rep.Resends)
	require.Equal(t, 2, rep.Marked)
	manifests, _ := f.objects(t)
	require.Equal(t, len(f.history(t)), manifests)
	f.checkInvariants(t)
}

// TestFlushAbandonsAnExpiredStaging pins the staging budget: a publication
// whose staging is older than W never commits, so the flush abandons it,
// selects its entries again, and gives up after Config.Restarts.
func TestFlushAbandonsAnExpiredStaging(t *testing.T) {
	f := newFixture(t)
	published(t, f)
	before, dirty := f.record(t), f.dirty(t)
	slow := func(_ context.Context, p Point) error {
		if p == PointBegin {
			time.Sleep(20 * time.Millisecond)
		}
		return nil
	}
	rep, err := f.writer(t, func(c *Config) { c.StagingBudget = time.Millisecond; c.Failpoint = slow }).Flush(context.Background(), schemaID, 1, &stager{f: f})
	require.ErrorIs(t, err, ErrAbandoned)
	require.ErrorIs(t, err, publication.ErrStagingExpired)
	require.Equal(t, DefaultRestarts, rep.Restarts)
	require.Equal(t, before, f.record(t))
	require.Equal(t, dirty, f.dirty(t))
	f.checkInvariants(t)
}

// TestFlushAbandonsAnOutcomeItCannotRecover stages Resolve's Unknown: the
// flush's COMMIT landed without an answer, and before it asks, a rival
// published over it and the collector forgot its generation. The flush
// abandons the attempt and starts over from the head, where its entries are
// already marked.
func TestFlushAbandonsAnOutcomeItCannotRecover(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	published(t, f)
	forgetOurs := func(ctx context.Context) error {
		ours := f.record(t)
		f.insert(t, 1)
		if _, err := f.writer(t).Flush(ctx, schemaID, 1, &stager{f: f}); err != nil {
			return err
		}
		f.forget(t, ours.Generation)
		return errors.New("connection reset")
	}
	rep, err := f.writer(t, func(c *Config) { c.Failpoint = once(PointCommitAnswer, forgetOurs) }).Flush(ctx, schemaID, 1, &stager{f: f})
	require.NoError(t, err)
	require.Equal(t, 1, rep.Restarts)
	require.Empty(t, f.dirty(t))
	require.Equal(t, int64(3), f.record(t).Generation)
}

// TestFlushResendsAfterThePublishDeadline pins T_pub: a compare-and-set
// still waiting on the record row when the deadline passes is rolled back
// and sent again, and commits once the row is free.
func TestFlushResendsAfterThePublishDeadline(t *testing.T) {
	f := newFixture(t)
	if f.cluster {
		t.Skip("nothing waits on a row lock on an Aurora DSQL cluster")
	}
	ctx := context.Background()
	published(t, f)
	holder, err := f.db.Begin()
	require.NoError(t, err)
	t.Cleanup(func() { _ = holder.Rollback() })
	_, err = holder.Exec(fmt.Sprintf("SELECT generation FROM %s WHERE schema_id = $1 FOR UPDATE", f.tables.Record), schemaID)
	require.NoError(t, err)
	var begins atomic.Int32
	release := func(_ context.Context, p Point) error {
		if p == PointBegin && begins.Add(1) == 2 {
			return holder.Rollback()
		}
		return nil
	}
	rep, err := f.writer(t, func(c *Config) { c.PublishTimeout = 300 * time.Millisecond; c.Failpoint = release }).Flush(ctx, schemaID, 1, &stager{f: f})
	require.NoError(t, err)
	require.Equal(t, 1, rep.Resends)
	require.Equal(t, 2, rep.Marked)
	f.checkInvariants(t)
}

// lateCommit answers a failpoint that loses the first COMMIT's answer, the
// COMMIT not landing, and then, at the resend's first hit of at, lands that
// very publication, as a COMMIT delayed in the network would, and answers
// answer there. At PointBegin with no answer the late COMMIT lands before
// the resend's snapshot, so the resend loses its compare-and-set to it. At
// PointPublish with a 40001 it lands after the resend's BEGIN, and the
// resend fails as a snapshot engine fails a transaction whose snapshot
// predates a conflicting commit.
func (f *fixture) lateCommit(t *testing.T, at Point, answer error) func(context.Context, Point) error {
	var commits, landed atomic.Int32
	return func(ctx context.Context, p Point) error {
		switch {
		case p == PointCommit && commits.Add(1) == 1:
			return ErrCommitAnswerLost
		case p == at && commits.Load() == 1 && landed.Add(1) == 1:
			f.landStaged(t)
			return answer
		}
		return nil
	}
}

// landStaged commits the one staged manifest of the next generation that no
// history row names, with its own token, key and digest, as the writer's
// transaction would: for a flush, the compare-and-set and the exact-version
// marks of the entries its new delta objects hold, in one transaction.
func (f *fixture) landStaged(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	cur := f.record(t)
	m, raw := f.stagedNext(t, cur)
	entries := f.newEntries(t, cur, m)
	f.inTx(t, func(tx *sql.Tx) error {
		var now int64
		if err := tx.QueryRow("SELECT floor(extract(epoch from now()) * 1000)::bigint").Scan(&now); err != nil {
			return err
		}
		out, err := publication.Publish(ctx, publication.SQLTx(tx), f.tables, cur, publication.Next{
			Kind: m.Kind, Token: m.Token, ManifestKey: f.layout.ManifestKey(m.SchemaID, m.Epoch, m.Generation, m.Token),
			ManifestSHA256: manifestv2.Digest(raw), Lineage: m.Lineage, StagedAt: now, StagingBudget: time.Minute,
		})
		if err != nil || m.Kind != publication.KindFlush {
			return err
		}
		n, err := f.writer(t).mark(ctx, tx, schemaID, entries, out.UpdatedAt)
		if err == nil && n != len(entries) {
			err = fmt.Errorf("the late commit marked %d of its %d entries", n, len(entries))
		}
		return err
	})
}

// stagedNext answers the one staged manifest of the generation after cur
// that no history row names, and its bytes.
func (f *fixture) stagedNext(t *testing.T, cur publication.Record) (manifestv2.Manifest, []byte) {
	t.Helper()
	committed := map[string]bool{}
	for _, g := range f.history(t) {
		committed[g.ManifestKey] = true
	}
	var staged []manifestv2.Manifest
	var raws [][]byte
	require.NoError(t, filepath.WalkDir(f.store.Dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.Contains(p, "_manifests") {
			return err
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		var m manifestv2.Manifest
		if err := json.Unmarshal(raw, &m); err != nil {
			return err
		}
		if m.Generation == cur.Generation+1 && !committed[f.layout.ManifestKey(m.SchemaID, m.Epoch, m.Generation, m.Token)] {
			staged, raws = append(staged, m), append(raws, raw)
		}
		return nil
	}))
	require.Len(t, staged, 1, "one staged publication waits for its commit")
	return staged[0], raws[0]
}

// newEntries answers the entries m's objects hold that cur's manifest does
// not list: what a flush generation of m marks.
func (f *fixture) newEntries(t *testing.T, cur publication.Record, m manifestv2.Manifest) []Entry {
	t.Helper()
	listed := map[string]bool{}
	if cur.State == publication.Published {
		ref, err := manifestv2.RecordRef(cur)
		require.NoError(t, err)
		parent, err := manifestv2.Fetch(context.Background(), f.store, ref)
		require.NoError(t, err)
		for _, file := range parent.Files {
			listed[file.Key] = true
		}
	}
	var entries []Entry
	for _, file := range m.Files {
		if listed[file.Key] {
			continue
		}
		raw, err := f.store.Get(context.Background(), file.Key)
		require.NoError(t, err)
		var copies []rowCopy
		require.NoError(t, json.Unmarshal(raw, &copies))
		for _, c := range copies {
			entries = append(entries, Entry{RowID: c.Row, Version: c.Version})
		}
	}
	return entries
}

// markedOnce checks that each of entries, a row and its version, is
// change_log's only entry of its row, flushed at flushedAt.
func (f *fixture) markedOnce(t *testing.T, entries map[uuid.UUID]int64, flushedAt int64) {
	t.Helper()
	for row, version := range entries {
		rows, err := f.db.Query(fmt.Sprintf("SELECT changed_at, flushed_at FROM %s WHERE schema_id = $1 AND row_id = $2", f.changeLog), schemaID, row)
		require.NoError(t, err)
		var got [][2]int64
		for rows.Next() {
			var e [2]int64
			require.NoError(t, rows.Scan(&e[0], &e[1]))
			got = append(got, e)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		require.Equal(t, [][2]int64{{version, flushedAt}}, got, "row %s is marked once, by the generation that committed", row)
	}
}

// forget collects generation as a collector would with a zero grace.
func (f *fixture) forget(t *testing.T, generation int64) {
	t.Helper()
	ctx := context.Background()
	ok, err := publication.Retire(ctx, publication.DB(f.db), f.tables, schemaID, generation)
	require.NoError(t, err)
	require.True(t, ok, "generation %d is superseded", generation)
	ok, err = publication.Forget(ctx, publication.DB(f.db), f.tables, schemaID, generation, 0)
	require.NoError(t, err)
	require.True(t, ok, "generation %d is retired", generation)
}
