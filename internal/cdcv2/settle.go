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
// could not settle (the fence failed, the database was out, or the context
// ended). It is neither committed nor abandoned: the UnresolvedError it comes
// in holds what settling it later takes, and Writer.Settle (a flush) or
// Writer.ResumeInit (an init) settles it with a fresh context. Until then the
// staged objects must stay, and the entries the publication would have marked
// are flushed or dirty as change_log says. A flush does not start over on it:
// its marks commit with its publication, so a flush found committed later is
// complete. An init's handoff has not run: ResumeInit runs it, from the
// entries the error keeps, once it finds the init committed. A process that
// exits first leaves those entries dirty for the next flush, as a crash
// between an init's commit and its handoff does.
var ErrUnresolved = errors.New("publication outcome unresolved")

// UnresolvedError is an ErrUnresolved publication: Cur is the record it was
// staged on, Next what it sent, Cause why the writer stopped, and Settle why
// the settling failed. Handoff is an init's exported entries at the versions
// its export snapshots listed, what its handoff marks; it is nil for a flush.
// Marked and Unconfirmed bound what a flush's marks flushed should the
// publication be found committed: at least Marked entries and at most
// Marked + Unconfirmed, as FlushReport counts them. The transaction that
// would have committed is one of those whose COMMIT went without an answer,
// and each counted its own marks. Both are zero for an init, whose
// publication carries no marks.
// It matches ErrUnresolved and Settle's errors, never Cause's: a staging that
// expired or ran out of attempts while its COMMIT may still land is not
// ErrAbandoned.
type UnresolvedError struct {
	Kind                publication.Kind
	Cur                 publication.Record
	Next                publication.Next
	Handoff             []Entry
	Marked, Unconfirmed int
	Cause, Settle       error
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
				p.resolved = true
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
	lo, hi := p.unansweredRange()
	return &UnresolvedError{Kind: p.kind, Cur: p.cur, Next: p.next, Handoff: p.handoff,
		Marked: lo, Unconfirmed: hi - lo, Cause: cause, Settle: err}
}

// Settle settles u, an ErrUnresolved publication, as the writer would have
// before it gave the publication up: it fences the record at u.Cur, then asks
// Resolve once the fence found the record moved. It answers the record u's
// publication committed, however late its COMMIT landed; ErrAbandoned once the
// fence proves it can never land, or another generation took its number, so
// that a flush or an init can start over; and a fresh UnresolvedError when it
// still cannot tell. A flush found committed is complete: its marks committed
// with it. An init needs its handoff too, which ResumeInit runs.
func (w *Writer) Settle(ctx context.Context, u *UnresolvedError) (publication.Record, error) {
	// The settling keeps the mark range u carries: a COMMIT it finds landed is
	// one of those the range was counted from, and a fresh UnresolvedError
	// carries it on.
	p := &pub{kind: u.Kind, cur: u.Cur, next: u.Next, handoff: u.Handoff,
		unanswered: []int{u.Marked, u.Marked + u.Unconfirmed}}
	out, err := w.settle(ctx, p, u.Cause)
	var again *UnresolvedError
	if err != nil && !errors.As(err, &again) {
		return publication.Record{}, fmt.Errorf("%s of schema %d staged on generation %d: %w: %w", u.Kind, u.Cur.SchemaID, u.Cur.Generation, ErrAbandoned, err)
	}
	return out, err
}

// ResumeInit finishes an init that returned ErrUnresolved: it settles u
// (Settle) and, when the init committed, hands u.Handoff off at exactly their
// export versions, fenced by the init's lineage, as Init would have. An entry
// whose row moved past its listed version stays dirty, and a lineage replaced
// in between stops the handoff (HandoffStopped). It answers the report of what
// it handed off; ErrAbandoned when the init can never commit, so the caller
// runs Init again; and a fresh UnresolvedError, to resume later, when it still
// cannot tell.
func (w *Writer) ResumeInit(ctx context.Context, u *UnresolvedError) (InitReport, error) {
	rep := InitReport{Entries: len(u.Handoff)}
	if u.Kind != publication.KindInit {
		return rep, fmt.Errorf("resume an init of schema %d: the unresolved publication is a %s", u.Cur.SchemaID, u.Kind)
	}
	gen, err := w.Settle(ctx, u)
	if err != nil {
		return rep, fmt.Errorf("resume an init: %w", err)
	}
	rep.Generation = gen
	if err := w.handoff(ctx, gen, u.Handoff, &rep); err != nil {
		return rep, fmt.Errorf("resume an init of schema %d generation %d: %w", gen.SchemaID, gen.Generation, err)
	}
	return rep, nil
}
