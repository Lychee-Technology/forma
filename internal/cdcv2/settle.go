package cdcv2

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/lychee-technology/forma/internal/publication"
)

// ErrUnresolved reports a publication the writer stopped while a COMMIT of
// its staging, sent without an answer, may still land, and whose outcome it
// could not settle (the fence failed or the context ended). It is neither
// committed nor abandoned: the UnresolvedError it comes in names the
// expectation and the staging, which publication.Resolve, after a fence
// (publication.Fence) has committed, answers for good. Until then the staged
// objects must stay, and the entries the publication would have marked are
// flushed or dirty as change_log says. A flush does not start over on it, and
// an init's handoff does not run: the entries it would have marked stay dirty
// for the next flush, as after a replaced lineage.
var ErrUnresolved = errors.New("publication outcome unresolved")

// UnresolvedError is an ErrUnresolved publication: Cur is the record it was
// staged on, Next what it sent, Cause why the writer stopped, and Settle why
// the settling failed. It matches ErrUnresolved and Settle's errors, never
// Cause's: a staging that expired or ran out of attempts while its COMMIT may
// still land is not ErrAbandoned.
type UnresolvedError struct {
	Kind          publication.Kind
	Cur           publication.Record
	Next          publication.Next
	Cause, Settle error
}

func (e *UnresolvedError) Error() string {
	return fmt.Sprintf("%s of schema %d staged on generation %d with token %s: %v (%v), settling failed: %v",
		e.Kind, e.Cur.SchemaID, e.Cur.Generation, e.Next.Token, ErrUnresolved, e.Cause, e.Settle)
}

func (e *UnresolvedError) Unwrap() []error { return []error{ErrUnresolved, e.Settle} }

// settle decides p before publish gives it up with cause while a COMMIT of
// p's staging may still land: Resolve last answered NotCommitted, and the
// attempts ran out, the resend found the staging expired, or it failed. It
// fences the record at p.cur (publication.Fence), in at most fenceAttempts
// transactions of at most T_pub each. A fence that held the record and
// committed proves that COMMIT can never land, and publish fails with cause.
// One that found the record moved hands the outcome to Resolve, which is
// final then: Committed answers the record, and Lost or Unknown fail with
// cause. Anything else answers an UnresolvedError.
func (w *Writer) settle(ctx context.Context, p *pub, cause error) (publication.Record, error) {
	var last error
	attempts := w.fenceAttempts()
	for attempt := 1; attempt <= attempts; attempt++ {
		held, err := w.fence(ctx, p)
		if err == nil && held {
			return publication.Record{}, cause
		}
		if err == nil {
			outcome, rerr := publication.Resolve(ctx, publication.DB(w.cfg.DB), w.tables, p.cur, p.next)
			switch {
			case rerr != nil:
				return publication.Record{}, unresolved(p, cause, fmt.Errorf("resolve after the fence found the record moved: %w", rerr))
			case outcome == publication.Committed:
				return committed(p.cur, p.next), nil
			case outcome != publication.NotCommitted:
				return publication.Record{}, cause
			}
			err = errors.New("the fence found the record moved, and Resolve still found it at the publication's expectation")
		}
		last = err
		if v := w.classify(ctx, err); v != resend && v != resolve {
			return publication.Record{}, unresolved(p, cause, err)
		}
		if err := pause(ctx, time.Duration(attempt)*resendBackoff); err != nil {
			return publication.Record{}, unresolved(p, cause, err)
		}
	}
	return publication.Record{}, unresolved(p, cause, fmt.Errorf("after %d fence transactions: %w", attempts, last))
}

// fenceAttempts bounds the fence transactions of one settling: Config.Attempts,
// but never fewer than DefaultAttempts. A fence that loses to the COMMIT it
// waits for fails with 40001 on a snapshot engine, and only the next one finds
// the record moved, so a publication sent once must still be fenced again.
func (w *Writer) fenceAttempts() int {
	return max(w.cfg.Attempts, DefaultAttempts)
}

// fence sends one fence transaction for p and answers whether it held the
// record at p.cur and committed. A fence whose COMMIT went without an answer
// is sent again: it changes nothing, so a second one that holds the record
// proves what the first would have.
func (w *Writer) fence(ctx context.Context, p *pub) (bool, error) {
	if err := w.fail(ctx, PointFence); err != nil {
		return false, err
	}
	tctx, cancel := context.WithTimeout(ctx, w.cfg.PublishTimeout)
	defer cancel()
	tx, err := w.cfg.DB.BeginTx(tctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin a fence transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	held, err := publication.Fence(tctx, publication.SQLTx(tx), w.tables, p.cur)
	if err != nil || !held {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, &commitError{err}
	}
	return true, nil
}

func unresolved(p *pub, cause, err error) error {
	return &UnresolvedError{Kind: p.kind, Cur: p.cur, Next: p.next, Cause: cause, Settle: err}
}
