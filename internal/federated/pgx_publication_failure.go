package federated

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// How a published pass (pgx_publication.go) classifies its failures. A
// publication fault is a read-path consistency error, never degradable and
// never charged to the breaker. A failed read keeps its V1 class. A pass its
// own deadline, R_max, aborted is retried once on a fresh snapshot, and stays
// non-degradable whatever the retry meets, unless it had already confirmed a
// publication fault: that fault is final.

// errPublishedObjectMissing is the fault of a generation that lists an object
// the store does not hold: loss, which garbage collection never causes
// within its grace.
var errPublishedObjectMissing = errors.New("the generation lists objects the store does not hold")

// ErrPassDeadline marks a published pass that ran past its deadline, R_max,
// and was aborted, its DuckDB statement cancelled. The engine retries the
// pass once, on a fresh snapshot. Like any expired deadline it is not
// degradable (#465), on its own and through the retry: a retry that cannot
// answer keeps it in its error. It is not charged to the breaker either.
// R_max bounds how long a pass may hold its generation against garbage
// collection, not how fast DuckDB answers; DuckDBConfig.QueryTimeout is that
// budget, and its expiry is charged as in V1.
var ErrPassDeadline = errors.New("published pass ran past its deadline")

// passDeadlineError wraps the failure of a pass its deadline aborted.
type passDeadlineError struct {
	SchemaID int16
	Deadline time.Duration
	cause    error
}

func (e *passDeadlineError) Error() string {
	return fmt.Sprintf("schema %d: the pass ran past its %s deadline and was aborted: %v", e.SchemaID, e.Deadline, e.cause)
}

func (e *passDeadlineError) Unwrap() []error { return []error{ErrPassDeadline, e.cause} }

// passDeadlineExpired reports whether ctx ended because its published pass
// ran past R_max (armDeadline).
func passDeadlineExpired(ctx context.Context) bool {
	return errors.Is(context.Cause(ctx), ErrPassDeadline)
}

// deadlineFailure turns the failure of a pass its own deadline aborted into a
// retryable passDeadlineError. A failure the request's own deadline or
// cancellation caused is left as it is, and so is a consistency error: a
// confirmed fault, such as a listed object the missing-object probe found
// lost before R_max stopped its other probes, is the pass's final answer,
// and a retry on a fresh snapshot must not get the chance to hide it.
func (p *pgxPass) deadlineFailure(passCtx context.Context, err error) error {
	if err == nil || !p.published() || !passDeadlineExpired(passCtx) {
		return err
	}
	if errors.Is(err, ErrPublicationInconsistent) {
		return err
	}
	return &passDeadlineError{SchemaID: p.q.SchemaID, Deadline: p.e.publishedPassDeadline, cause: err}
}

// retryFailure is the error of a request whose retry failed after first
// failed. A retry after a pass deadline keeps the first pass's failure in
// its chain: the request already spent R_max, so the retry's failure, a
// breaker rejection included, must not turn the deadline into a degradable
// error that a Postgres-only fallback would answer.
func retryFailure(reason string, first, retry error) error {
	if errors.Is(first, ErrPassDeadline) && !errors.Is(retry, ErrPassDeadline) {
		return fmt.Errorf("retry after %s: %w; the first pass: %w", reason, retry, first)
	}
	return fmt.Errorf("retry after %s: %w", reason, retry)
}

// publicationFailure keeps a consistency error as it is, and labels any other
// failure with class.
func (p *pgxPass) publicationFailure(err, class error, op string) error {
	if errors.Is(err, ErrPublicationInconsistent) {
		return fmt.Errorf("%s: %w", op, err)
	}
	return fmt.Errorf("%s: %w: %w", op, class, err)
}

// refusePublishedCorrupt fails a generation that lists objects cached as
// corrupt without scanning it, as refuseCachedCorrupt does for a V1 set: the
// type guard over the rest first, then the missing-object probe, so a listed
// object the store lost is reported as the consistency error it is.
func (p *pgxPass) refusePublishedCorrupt(ctx context.Context, corrupt []string) error {
	e := p.e
	if err := e.typeGuardReadableRemainder(ctx, p.q.SchemaID, p.gen.Paths, corrupt, p.gen.Columns); err != nil {
		return fmt.Errorf("type-check the objects listed beside ones cached as corrupt: %w", err)
	}
	cause := errors.New("verified corrupt by an earlier read and still cached")
	sc := scan{parquetPaths: p.gen.Paths, pathsFromSource: true, probe: p.probe, published: p.gen}
	final, classified := e.classifyPublishedReadError(ctx, sc, cause, "probe a generation listing objects cached as corrupt")
	if final != nil {
		p.resolved = true // the probe went back with the error
		return final
	}
	return &CorruptParquetError{SchemaID: p.q.SchemaID, Paths: corrupt, cause: fmt.Errorf("%w: %w", cause, classified)}
}

// classifyPublishedReadError classifies a published pass's failed read
// before the V1 classification runs (failDuckDBScan), and answers either the
// final error, with the breaker's probe handed back, or the class the V1
// classification goes on with:
//
//   - A read R_max ended is final: it proves no loss and says nothing of
//     DuckDB's health, and the pass is retried. Nothing is probed.
//   - Otherwise the listed objects are probed through the reader first. A
//     confirmed loss is final, a PublicationConsistencyError, even when
//     other probes failed: a lost object says nothing about DuckDB, and a
//     failed probe cannot take back a key another found missing.
//   - A probe that R_max ended is final as well, as a read it ended.
//   - A probe the request's deadline or cancellation ended keeps that
//     context error in the class, so the read stays non-degradable (#465),
//     as classifyDuckDBReadError keeps it for V1.
//   - Anything else is ErrFederatedReadFailed.
func (e *DBFederatedQueryEngine) classifyPublishedReadError(ctx context.Context, sc scan, cause error, op string) (final, classified error) {
	if passDeadlineExpired(ctx) {
		e.breaker.ReleaseProbe(sc.probe)
		return fmt.Errorf("%s: %w: %w", op, ErrFederatedReadFailed, cause), nil
	}
	missing, probeErr := e.publication.MissingIn(ctx, *sc.published, sc.parquetPaths)
	if len(missing) > 0 {
		e.breaker.ReleaseProbe(sc.probe)
		return objectsLost(*sc.published, missing, cause, probeErr, op), nil
	}
	if passDeadlineExpired(ctx) {
		e.breaker.ReleaseProbe(sc.probe)
		return fmt.Errorf("%s: %w: missing-object probe stopped: %w: %w", op, ErrFederatedReadFailed, ctx.Err(), cause), nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, fmt.Errorf("%w: missing-object probe stopped: %w", ErrFederatedReadFailed, ctxErr)
	}
	return nil, ErrFederatedReadFailed
}

// objectsLost is the consistency error of a generation whose listed objects
// missing the store does not hold.
func objectsLost(gen PublishedGeneration, missing []string, cause, probeErr error, op string) error {
	// The cause and the failed probes are text only: they may carry
	// ErrFederatedReadFailed or an expired deadline, and neither may
	// reclassify a lost object.
	fault := fmt.Sprintf("%s: %s", op, cause.Error())
	if probeErr != nil {
		fault += fmt.Sprintf("; other objects could not be probed: %s", probeErr.Error())
	}
	return &PublicationConsistencyError{
		SchemaID: gen.SchemaID, Epoch: gen.Epoch, Generation: gen.Generation, MissingKeys: missing,
		Err: fmt.Errorf("%w: %s", errPublishedObjectMissing, fault),
	}
}
