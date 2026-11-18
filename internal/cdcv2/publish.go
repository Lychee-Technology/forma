package cdcv2

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lychee-technology/forma/internal/publication"
)

// resendBackoff is the pause before the n-th resend of a publication that
// lost a serialization conflict (40001).
const resendBackoff = 20 * time.Millisecond

// markFunc marks a generation's entries on the publication transaction tx,
// after the compare-and-set moved the record to out.
type markFunc func(ctx context.Context, tx *sql.Tx, out publication.Record) error

// buildFunc stages the generation's manifest for cur under a fresh token and
// answers what Publish commits: the first staging, and every rebase's.
type buildFunc func(ctx context.Context, cur publication.Record) (publication.Next, error)

// pub is one generation in flight: the expectation it was staged on, what
// is staged, and how to restage and mark it.
type pub struct {
	kind  publication.Kind
	cur   publication.Record
	next  publication.Next
	build buildFunc
	mark  markFunc // nil for an init, whose publication carries no marks
	// resends and rebases count the transactions sent again with the same
	// token, and the restagings onto a new head.
	resends, rebases int
	// uncertain says a COMMIT of this staging went without an answer. Its
	// resend may then lose the compare-and-set to that very COMMIT landing
	// late, so a miss is resolved before it is taken for a rival's.
	uncertain bool
}

// verdict is what a failed publication transaction calls for.
type verdict int

const (
	fail    verdict = iota // a fault the protocol does not retry
	resend                 // nothing committed: send the same publication again
	rebase                 // the record moved: restage onto the head, if the rules allow
	resolve                // the COMMIT's answer was lost: ask Resolve
	abandon                // the staging can never commit
	done                   // Resolve found the publication committed
)

// commitError is a failed COMMIT, or a lost answer after one that landed.
type commitError struct{ err error }

func (e *commitError) Error() string { return "commit the publication: " + e.err.Error() }
func (e *commitError) Unwrap() error { return e.err }

// publish sends p until it commits, is abandoned or fails, in at most
// Config.Attempts transactions, and answers the record it committed.
func (w *Writer) publish(ctx context.Context, p *pub) (publication.Record, error) {
	var last error
	for attempt := 1; attempt <= w.cfg.Attempts; attempt++ {
		out, err := w.commit(ctx, p)
		if err == nil {
			return out, nil
		}
		last = err
		v := w.classify(ctx, err)
		if v == resolve || (v == rebase && p.uncertain) {
			p.uncertain = true
			v, out, err = w.resolve(ctx, p, err)
			last = err
		}
		switch v {
		case done:
			return out, nil
		case resend:
			p.resends++
			if err := pause(ctx, time.Duration(attempt)*resendBackoff); err != nil {
				return publication.Record{}, err
			}
		case rebase:
			if err := w.rebase(ctx, p); err != nil {
				return publication.Record{}, err
			}
			p.rebases, p.uncertain = p.rebases+1, false
		case abandon:
			return publication.Record{}, fmt.Errorf("%s of schema %d staged on generation %d: %w: %w", p.kind, p.cur.SchemaID, p.cur.Generation, ErrAbandoned, err)
		default:
			return publication.Record{}, fmt.Errorf("%s of schema %d staged on generation %d: %w", p.kind, p.cur.SchemaID, p.cur.Generation, err)
		}
	}
	return publication.Record{}, fmt.Errorf("%s of schema %d after %d transactions: %w: %w", p.kind, p.cur.SchemaID, w.cfg.Attempts, ErrAttemptsExhausted, last)
}

// commit sends p once: BEGIN, the compare-and-set, the marks, COMMIT, under
// the T_pub deadline. Every S3 write happened before, so the transaction is
// its statements alone.
func (w *Writer) commit(ctx context.Context, p *pub) (publication.Record, error) {
	if err := w.fail(ctx, PointBegin); err != nil {
		return publication.Record{}, err
	}
	tctx, cancel := context.WithTimeout(ctx, w.cfg.PublishTimeout)
	defer cancel()
	tx, err := w.cfg.DB.BeginTx(tctx, nil)
	if err != nil {
		return publication.Record{}, fmt.Errorf("begin the publication transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := w.fail(tctx, PointPublish); err != nil {
		return publication.Record{}, err
	}
	out, err := publication.Publish(tctx, publication.SQLTx(tx), w.tables, p.cur, p.next)
	if err != nil {
		return publication.Record{}, err
	}
	if err := w.fail(tctx, PointMarks); err != nil {
		return publication.Record{}, err
	}
	if p.mark != nil {
		if err := p.mark(tctx, tx, out); err != nil {
			return publication.Record{}, err
		}
	}
	if err := w.fail(tctx, PointCommit); err != nil {
		return publication.Record{}, err
	}
	if err := tx.Commit(); err != nil {
		return publication.Record{}, &commitError{err}
	}
	if err := w.fail(ctx, PointCommitAnswer); err != nil {
		return publication.Record{}, &commitError{fmt.Errorf("%w: %w", ErrCommitAnswerLost, err)}
	}
	return out, nil
}

// classify answers what a failed transaction of a publication calls for.
// Only a COMMIT the server did not answer has an unknown outcome: every
// failure before it rolled the transaction back, and a COMMIT the server
// refused did not land.
func (w *Writer) classify(ctx context.Context, err error) verdict {
	var ce *commitError
	if errors.As(err, &ce) {
		if serializationFailure(err) {
			return resend
		}
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			return fail
		}
		return resolve
	}
	switch {
	case errors.Is(err, ErrCommitAnswerLost):
		return resolve
	case errors.Is(err, publication.ErrStagingExpired):
		return abandon
	case errors.Is(err, publication.ErrRecordMoved):
		return rebase
	case serializationFailure(err):
		return resend
	case errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil:
		return resend // T_pub ran out before COMMIT was sent
	}
	return fail
}

// resolve asks Resolve what became of p after a COMMIT without an answer,
// and answers the verdict, with the committed record when it is done.
func (w *Writer) resolve(ctx context.Context, p *pub, cause error) (verdict, publication.Record, error) {
	outcome, err := publication.Resolve(ctx, publication.DB(w.cfg.DB), w.tables, p.cur, p.next)
	if err != nil {
		return fail, publication.Record{}, fmt.Errorf("resolve the outcome of a commit without an answer (%w): %w", cause, err)
	}
	switch outcome {
	case publication.Committed:
		return done, committed(p.cur, p.next), nil
	case publication.Lost:
		return rebase, publication.Record{}, fmt.Errorf("another generation took number %d: %w", p.cur.Generation+1, publication.ErrRecordMoved)
	case publication.NotCommitted:
		return resend, publication.Record{}, cause
	}
	return abandon, publication.Record{}, fmt.Errorf("the record moved past generation %d and its history row is gone, so whether it committed cannot be recovered: %w", p.cur.Generation+1, cause)
}

// committed is the record next committed on cur, as Publish answers it, but
// for UpdatedAt, which only the lost answer held.
func committed(cur publication.Record, next publication.Next) publication.Record {
	out := cur
	out.State, out.Generation, out.ParentGeneration = publication.Published, cur.Generation+1, 0
	if cur.State == publication.Published {
		out.ParentGeneration = cur.Generation
	}
	out.ManifestKey, out.ManifestSHA256 = next.ManifestKey, next.ManifestSHA256
	out.Lineage, out.Token, out.UpdatedAt = next.Lineage, next.Token, 0
	return out
}

// rebase moves p onto the record's head when the rules allow it
// (publication.Rebase): it restages p's manifest from the head under a fresh
// token, and keeps its parquet objects, marks and staged_at. A refusal
// abandons p.
func (w *Writer) rebase(ctx context.Context, p *pub) error {
	head, err := publication.Read(ctx, publication.DB(w.cfg.DB), w.tables, p.cur.SchemaID)
	if err != nil {
		return fmt.Errorf("read the head of schema %d to rebase a %s: %w", p.cur.SchemaID, p.kind, err)
	}
	var since []publication.Kind
	if head.Epoch == p.cur.Epoch {
		if since, err = publication.KindsSince(ctx, publication.DB(w.cfg.DB), w.tables, p.cur, head.Record); err != nil {
			return w.refused(p, err)
		}
	}
	if err := publication.Rebase(p.kind, p.cur, head.Record, since); err != nil {
		return w.refused(p, err)
	}
	next, err := p.build(ctx, head.Record)
	if err != nil {
		return fmt.Errorf("restage the %s of schema %d onto generation %d: %w", p.kind, p.cur.SchemaID, head.Generation, err)
	}
	p.cur, p.next = head.Record, next
	return nil
}

func (w *Writer) refused(p *pub, err error) error {
	if errors.Is(err, publication.ErrRebaseRefused) {
		return fmt.Errorf("%s of schema %d staged on generation %d: %w: %w", p.kind, p.cur.SchemaID, p.cur.Generation, ErrAbandoned, err)
	}
	return fmt.Errorf("decide whether the %s of schema %d may rebase: %w", p.kind, p.cur.SchemaID, err)
}

// serializationFailure reports SQLSTATE 40001: DSQL's answer to a lost write
// conflict, at a statement or at COMMIT, and PostgreSQL's at REPEATABLE READ.
func serializationFailure(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "40001"
}

func pause(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("wait to resend a publication: %w", ctx.Err())
	case <-time.After(d):
		return nil
	}
}
