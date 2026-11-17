package publication

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
)

// TestInitializeWritesOneUnpublishedRecord pins that a missing record is an
// error, never "unpublished", and that registration writes the record once.
func TestInitializeWritesOneUnpublishedRecord(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	_, err := Read(ctx, DB(f.db), f.t, 100)
	require.ErrorIs(t, err, ErrNoRecord)

	rec := f.initialized(t, 100)
	require.Equal(t, Record{SchemaID: 100, Epoch: 1, State: Unpublished, Token: rec.Token, UpdatedAt: rec.UpdatedAt}, rec)
	require.NotEqual(t, uuid.Nil, rec.Token)
	require.NoError(t, rec.Readable())

	again, err := f.initialize(t, 100)
	require.NoError(t, err)
	require.False(t, again, "registering the schema again must not rewrite its record")
	require.Equal(t, rec, f.read(t, 100))
}

// TestPublishCommitsPointerAndMarksAtOneBoundary is the commit-boundary
// proof's test: no snapshot sees the marks without the generation that holds
// their rows, or the generation without its marks.
func TestPublishCommitsPointerAndMarksAtOneBoundary(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	cur := f.initialized(t, 100)
	rows := f.write(t, 100, 3)

	_, before := f.snapshot(t, 100)
	require.Equal(t, view{State: Unpublished, Dirty: 3}, before())

	tx, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	out, err := Publish(ctx, SQLTx(tx), f.t, cur, f.next(t, cur, KindFlush))
	require.NoError(t, err)
	require.NoError(t, f.mark(ctx, tx, 100, rows))

	_, during := f.snapshot(t, 100)
	require.Equal(t, view{State: Unpublished, Dirty: 3}, during(), "an uncommitted publication is invisible, pointer and marks alike")

	require.NoError(t, tx.Commit())
	require.Equal(t, view{State: Unpublished, Dirty: 3}, before(), "a snapshot taken before the commit keeps the old generation and its dirty set")
	require.Equal(t, view{State: Unpublished, Dirty: 3}, during())

	_, after := f.snapshot(t, 100)
	require.Equal(t, view{State: Published, Generation: 1, Dirty: 0}, after())
	require.Equal(t, out, f.read(t, 100))
	require.Equal(t, int64(0), out.ParentGeneration)
}

// TestPublishRolledBackLeavesNeitherPointerNorMarks pins the failure half of
// the boundary: a publication that fails after marking (a lost COMMIT, a
// crash, a 40001 at COMMIT on DSQL) leaves the rows dirty and the record as
// it was.
func TestPublishRolledBackLeavesNeitherPointerNorMarks(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	cur := f.initialized(t, 100)
	rows := f.write(t, 100, 2)

	tx, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	next := f.next(t, cur, KindFlush)
	_, err = Publish(ctx, SQLTx(tx), f.t, cur, next)
	require.NoError(t, err)
	require.NoError(t, f.mark(ctx, tx, 100, rows))
	require.NoError(t, tx.Rollback())

	_, now := f.snapshot(t, 100)
	require.Equal(t, view{State: Unpublished, Dirty: 2}, now())
	require.Equal(t, cur, f.read(t, 100))
	outcome, err := Resolve(ctx, DB(f.db), f.t, cur, next)
	require.NoError(t, err)
	require.Equal(t, NotCommitted, outcome)
}

// TestPublishLosesToACommittedRival pins the compare-and-set: a writer whose
// expectation is stale learns it at its first statement, before it marks.
func TestPublishLosesToACommittedRival(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	cur := f.initialized(t, 100)
	rows := f.write(t, 100, 2)

	won, err := f.publish(ctx, cur, f.next(t, cur, KindFlush), nil)
	require.NoError(t, err)

	_, err = f.publish(ctx, cur, f.next(t, cur, KindFlush), rows)
	require.ErrorIs(t, err, ErrRecordMoved)
	require.True(t, Conflict(err))

	require.Equal(t, won, f.read(t, 100))
	_, now := f.snapshot(t, 100)
	require.Equal(t, view{State: Published, Generation: 1, Dirty: 2}, now(), "the loser marked nothing")
}

// TestPublishRacingRivalsCommitOne races two publications from one record. On
// PostgreSQL the second waits on the first's row lock and then misses; at
// REPEATABLE READ, on MiniStack and on DSQL, one of the two gets 40001. Either
// way exactly one commits.
func TestPublishRacingRivalsCommitOne(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	cur := f.initialized(t, 100)

	tx, err := f.db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, errA := Publish(ctx, SQLTx(tx), f.t, cur, f.next(t, cur, KindFlush))

	rival := f.next(t, cur, KindFlush)
	done := make(chan error, 1)
	go func() {
		_, err := f.publish(ctx, cur, rival, nil)
		done <- err
	}()
	time.Sleep(200 * time.Millisecond) // let the rival reach the record
	if errA == nil {
		errA = tx.Commit()
	}
	errB := <-done

	require.True(t, (errA == nil) != (errB == nil), "exactly one publication commits: first %v, second %v", errA, errB)
	lost := errA
	if lost == nil {
		lost = errB
	}
	require.True(t, Conflict(lost), "the other loses the compare-and-set: %v", lost)
	require.Equal(t, int64(1), f.read(t, 100).Generation)
}

// TestPublishRefusesAnExpiredStaging pins the staging budget: it is judged on
// the database clock inside the compare-and-set, so garbage collection's
// orphan rule can rely on it.
func TestPublishRefusesAnExpiredStaging(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	cur := f.initialized(t, 100)

	next := f.next(t, cur, KindFlush)
	next.StagedAt -= 10_000
	next.StagingBudget = time.Second
	_, err := f.publish(ctx, cur, next, nil)
	require.ErrorIs(t, err, ErrStagingExpired)
	require.False(t, Conflict(err))
	require.Equal(t, cur, f.read(t, 100))
}

// TestPublishRefusesTransitionsTheProtocolLacks pins Next.validate, which runs
// before any statement.
func TestPublishRefusesTransitionsTheProtocolLacks(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	cur := f.initialized(t, 100)

	compact := f.next(t, cur, KindCompact)
	_, err := f.publish(ctx, cur, compact, nil)
	require.ErrorContains(t, err, "nothing to compact")

	upper := f.next(t, cur, KindFlush)
	upper.ManifestSHA256 = strings.ToUpper(fmt.Sprintf("%064x", 0xabc))
	_, err = f.publish(ctx, cur, upper, nil)
	require.ErrorContains(t, err, "lowercase hex", "a digest the collector would refuse is refused at commit")

	pub, err := f.publish(ctx, cur, f.next(t, cur, KindFlush), nil)
	require.NoError(t, err)
	drift := f.next(t, pub, KindFlush)
	drift.Lineage = uuid.New()
	_, err = f.publish(ctx, pub, drift, nil)
	require.ErrorContains(t, err, "keeps the record's lineage")

	sameLineage := f.next(t, pub, KindInit)
	sameLineage.Lineage = pub.Lineage
	_, err = f.publish(ctx, pub, sameLineage, nil)
	require.ErrorContains(t, err, "new lineage")
	require.False(t, errors.Is(err, ErrRecordMoved))
	require.Equal(t, pub, f.read(t, 100))
}

// TestPublishThroughAPgxTransaction pins the path the pgx-based callers
// (init-db's registration among them) take: their own pgx transaction, which
// they commit, carries the transition and their statements together.
func TestPublishThroughAPgxTransaction(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	conn, err := f.db.Conn(ctx)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	rows := f.write(t, 100, 2)

	require.NoError(t, conn.Raw(func(driverConn any) error {
		pc := driverConn.(*stdlib.Conn).Conn()
		tx, err := pc.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := Initialize(ctx, PgxTx(tx), f.t, 100); err != nil {
			return err
		}
		cur, err := Read(ctx, PgxTx(tx), f.t, 100)
		if err != nil {
			return err
		}
		if _, err := Publish(ctx, PgxTx(tx), f.t, cur.Record, f.next(t, cur.Record, KindFlush)); err != nil {
			return err
		}
		for id, version := range rows {
			if _, err := tx.Exec(ctx, "UPDATE "+f.changeLog+" SET flushed_at = 1 WHERE schema_id = 100 AND row_id = $1 AND changed_at = $2 AND flushed_at = 0", id, version); err != nil {
				return err
			}
		}
		_, err = Read(ctx, PgxTx(tx), f.t, 101)
		if !errors.Is(err, ErrNoRecord) {
			return fmt.Errorf("a missing record through pgx reads as %v, want ErrNoRecord", err)
		}
		return tx.Commit(ctx)
	}))
	_, now := f.snapshot(t, 100)
	require.Equal(t, view{State: Published, Generation: 1, Dirty: 0}, now())
}
