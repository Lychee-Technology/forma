package cdcv2

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lychee-technology/forma/internal/publication"
)

// resendBackoff is the pause before the n-th resend of a publication that
// lost a serialization conflict (40001).
const resendBackoff = 20 * time.Millisecond

// markFunc marks a generation's entries on the publication transaction tx,
// after the compare-and-set moved the record to out, and answers how many it
// marked.
type markFunc func(ctx context.Context, tx *sql.Tx, out publication.Record) (int, error)

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
	// handoff is an init's exported entries at their export versions, what
	// its handoff marks once the init committed: an UnresolvedError keeps
	// them, so that ResumeInit can still hand them off.
	handoff []Entry
	// resends and rebases count the transactions sent again with the same
	// token, and the restagings onto a new head.
	resends, rebases int
	// marked is what the marks of the transaction sent last counted, and
	// unanswered what those of each transaction of this staging whose
	// COMMIT went without an answer counted. resolved says the record
	// publish answered came from Resolve, not from an answered COMMIT: then
	// one of the unanswered transactions is the one that committed, and the
	// report cannot say which (markedRange).
	marked     int
	unanswered []int
	resolved   bool
	// uncertain says a COMMIT of this staging went without an answer. That
	// COMMIT may still land while the staging is sent again: before the
	// resend's snapshot, and the resend's compare-and-set misses
	// (ErrRecordMoved), or after it, and the resend fails with 40001 on a
	// snapshot engine. So while it is set, a resend's miss or conflict is
	// resolved before it is taken for a rival's or sent once more, and
	// publish settles it before it gives the staging up. Only a read that
	// proves the COMMIT can no longer land clears it.
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
// Config.Attempts transactions, and answers the record it committed. When it
// would give p up while a COMMIT of p's staging went without an answer and
// may still land, it settles that COMMIT first (settle): it answers the
// record if the COMMIT landed, the failure only once the COMMIT can no
// longer land, and ErrUnresolved when it could tell neither.
func (w *Writer) publish(ctx context.Context, p *pub) (publication.Record, error) {
	out, err := w.send(ctx, p)
	if err == nil || !p.uncertain {
		return out, err
	}
	return w.settle(ctx, p, err)
}

// send is publish's loop: it answers the record p committed, or why it
// stopped, with p.uncertain still set when a COMMIT of the staging it stopped
// on went without an answer that no later read settled.
func (w *Writer) send(ctx context.Context, p *pub) (publication.Record, error) {
	var last error
	for attempt := 1; attempt <= w.cfg.Attempts; attempt++ {
		out, err := w.commit(ctx, p)
		if err == nil {
			return out, nil
		}
		last = err
		v := w.classify(ctx, err)
		if v == resolve {
			p.unanswered = append(p.unanswered, p.marked)
		}
		if v == resolve || (p.uncertain && (v == rebase || v == resend)) {
			p.uncertain = true
			v, out, err = w.resolve(ctx, p, err)
			last = err
		}
		switch v {
		case done:
			p.resolved = true
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
			// No COMMIT of the staging given up can land any more.
			p.rebases, p.uncertain, p.unanswered = p.rebases+1, false, nil
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
	p.marked = 0
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
		n, err := p.mark(tctx, tx, out)
		if err != nil {
			return publication.Record{}, err
		}
		p.marked = n
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
// refused did not land. A COMMIT is told refused by severity first, as
// internal/storage_outcome.go's classifyCommitError does: an ERROR rejected
// it and aborted the transaction, so its SQLSTATE decides (40001 is sent
// again, anything else fails), while a FATAL or PANIC ended the session,
// which a pooler also answers for a backend lost after it forwarded the
// COMMIT. That proves nothing about the commit, whatever its code, so it is
// resolved like a COMMIT without any answer.
func (w *Writer) classify(ctx context.Context, err error) verdict {
	var ce *commitError
	if errors.As(err, &ce) {
		var pgErr *pgconn.PgError
		switch {
		case !errors.As(err, &pgErr), sessionEnded(pgErr):
			return resolve
		case pgErr.Code == "40001":
			return resend
		}
		return fail
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
		// The record moved from p.cur to another generation, so no COMMIT of
		// p's staging can land any more.
		p.uncertain = false
		return rebase, publication.Record{}, fmt.Errorf("another generation took number %d: %w", p.cur.Generation+1, publication.ErrRecordMoved)
	case publication.NotCommitted:
		return resend, publication.Record{}, cause
	}
	p.uncertain = false // the record moved past p.cur: nothing of p can land
	return abandon, publication.Record{}, fmt.Errorf("the record moved past generation %d and its history row is gone, so whether it committed cannot be recovered: %w", p.cur.Generation+1, cause)
}

// markedRange answers how many entries the transaction of p that committed
// marked, at least lo and at most hi. After an answered COMMIT that is
// exactly what its marks counted. After Resolve found the staging committed,
// the transaction that committed is one of those whose COMMIT went without an
// answer, so the count is exact when they all counted alike, and a range
// otherwise: an entry a writer held locked, or a row that moved, between
// them.
func (p *pub) markedRange() (lo, hi int) {
	if !p.resolved || len(p.unanswered) == 0 {
		return p.marked, p.marked
	}
	return slices.Min(p.unanswered), slices.Max(p.unanswered)
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

// sessionEnded reports whether a server error ended the session rather than
// only the statement or the transaction. It reads the unlocalized severity, a
// protocol enumeration rather than message text, and falls back to the
// localized one for a server that predates the field (PostgreSQL < 9.6).
func sessionEnded(pgErr *pgconn.PgError) bool {
	severity := pgErr.SeverityUnlocalized
	if severity == "" {
		severity = pgErr.Severity
	}
	return severity == "FATAL" || severity == "PANIC"
}

func pause(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("wait to resend a publication: %w", ctx.Err())
	case <-time.After(d):
		return nil
	}
}
