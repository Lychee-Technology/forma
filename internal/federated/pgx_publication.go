package federated

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// The Protocol V2 source of a pgx pass (docs/publication-protocol-v2.md,
// "Readers"). An engine built WithPublicationReader resolves what its parquet
// leg scans inside the pass's snapshot: the publication record is the
// snapshot's first statement, so the generation the pass scans and the dirty
// set it discards come from one snapshot (Invariant R). No glob is listed, no
// manifest.json is read, and the flush-grace widening (#252) and the
// parquet-history probe (#720) have nothing left to make up for.
//
// The seam is dormant until #896: no production caller passes
// WithPublicationReader, so every engine resolves its path set as before
// (pgx_scan_sources.go). Its implementation lives in internal/federatedv2,
// which the guard in internal/publication keeps out of every binary.

// DefaultPublishedPassDeadline is R_max, the pass deadline of a published read
// when WithPublicationReader is given none. Garbage collection keeps a
// superseded generation for a grace G >= R_max + C (the clock margin), so a
// pass must end by then whatever DuckDBConfig.QueryTimeout says, and that
// budget may be zero (no bound). It is twice the default query budget, so
// under the default configuration QueryTimeout expires first and R_max only
// bounds a deployment that raised or disabled it.
const DefaultPublishedPassDeadline = 60 * time.Second

// PublicationReader resolves a published pass's parquet source. Its
// implementation is internal/federatedv2's Source.
type PublicationReader interface {
	// ReadRecord reads schemaID's publication record on tx, the pass's
	// REPEATABLE READ snapshot, as the snapshot's first statement. A record
	// that is absent, AWAITING_INIT, DROPPED or at an epoch other than the
	// one the process pinned fails with a *PublicationConsistencyError; any
	// other error is a failed PostgreSQL read.
	ReadRecord(ctx context.Context, tx pgx.Tx, schemaID int16) (PublicationRecord, error)
	// ResolveGeneration answers the objects a published record's generation
	// lists, from its verified manifest. A missing or invalid manifest is a
	// *PublicationConsistencyError; any other error is a failed fetch.
	ResolveGeneration(ctx context.Context, rec PublicationRecord) (PublishedGeneration, error)
	// MissingIn answers the keys of gen's objects among scanned that the
	// store does not hold. It runs only after a failed read.
	MissingIn(ctx context.Context, gen PublishedGeneration, scanned []string) ([]string, error)
}

// PublicationRecord is a readable record as a pass's snapshot read it.
type PublicationRecord struct {
	SchemaID int16
	Epoch    int64
	// Generation is the current generation when Published, otherwise the
	// allocation counter.
	Generation int64
	// Published is false for an UNPUBLISHED record: the hot leg alone
	// answers, and no DuckDB statement runs.
	Published bool
	// Detail is the reader's own reading of the record, handed back to
	// ResolveGeneration. The engine does not look at it.
	Detail any
}

// PublishedGeneration is what a published pass scans: exactly the objects its
// generation's manifest lists, and nothing else.
type PublishedGeneration struct {
	SchemaID   int16
	Epoch      int64
	Generation int64
	// Paths are the listed objects as DuckDB reads them, in manifest order.
	// Empty means the parquet tiers hold nothing, and no DuckDB statement
	// runs.
	Paths []string
	// Keys are the objects' storage keys, Keys[i] for Paths[i].
	Keys []string
	// Columns are the manifest's footer columns of each path, which the
	// pre-read checks take in place of a footer probe. Shared with the
	// reader's cache: never modified.
	Columns map[string]map[string]string
}

// WithPublicationReader makes every pgx pass of the engine resolve its
// parquet source from the publication record in its snapshot (Protocol V2),
// under a pass deadline of deadline, or DefaultPublishedPassDeadline when it
// is not positive. Nothing passes it before #896.
func WithPublicationReader(r PublicationReader, deadline time.Duration) EngineOption {
	return func(e *DBFederatedQueryEngine) {
		if r == nil {
			return
		}
		if deadline <= 0 {
			deadline = DefaultPublishedPassDeadline
		}
		e.publication, e.publishedPassDeadline = r, deadline
	}
}

// ErrPublicationInconsistent marks a published read whose publication state
// cannot be answered from: no record, a record that is AWAITING_INIT or
// DROPPED or at an epoch the process did not pin, a manifest that is missing
// or is not the one PostgreSQL committed, or a listed object the store does
// not hold. It is a read-path consistency error, never a 4xx: it is not
// degradable, since a PostgreSQL-only answer would drop every row the parquet
// tiers hold, it is never charged to the circuit breaker, which measures
// DuckDB's health, and it is never ErrFederatedReadFailed.
var ErrPublicationInconsistent = errors.New("publication state inconsistent")

// PublicationConsistencyError names the schema, the epoch and the generation
// of a published read's consistency failure. MissingKeys are storage keys,
// operator detail that the HTTP boundary keeps out of response bodies with
// every internal-class error.
type PublicationConsistencyError struct {
	SchemaID   int16
	Epoch      int64
	Generation int64
	// MissingKeys lists the generation's objects the store does not hold,
	// when that is the fault.
	MissingKeys []string
	// Err is the fault.
	Err error
}

func (e *PublicationConsistencyError) Error() string {
	msg := fmt.Sprintf("schema %d epoch %d generation %d: %v", e.SchemaID, e.Epoch, e.Generation, e.Err)
	if len(e.MissingKeys) > 0 {
		msg += fmt.Sprintf(": %d listed object(s) missing from storage: %s", len(e.MissingKeys), strings.Join(e.MissingKeys, ", "))
	}
	return msg
}

func (e *PublicationConsistencyError) Unwrap() []error {
	return []error{ErrPublicationInconsistent, e.Err}
}

// errPublishedObjectMissing is the fault of a generation that lists an object
// the store does not hold: loss, which garbage collection never causes
// within its grace.
var errPublishedObjectMissing = errors.New("the generation lists objects the store does not hold")

// ErrPassDeadline marks a published pass that ran past its deadline, R_max,
// and was aborted, its DuckDB statement cancelled. The engine retries the
// pass once, on a fresh snapshot. Like any expired deadline it is not
// degradable (#465).
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

// noGraceCutoffMs is a published pass's dirty-window cutoff: no entry is
// marked at or after it, so the hot keys' window is the unflushed entries
// alone. The generation is read in the snapshot, so a row marked flushed in it
// is in the generation, and nothing widens the barrier.
const noGraceCutoffMs = math.MaxInt64

// published reports whether the pass resolves its source from the record.
func (p *pgxPass) published() bool { return p.e.publication != nil }

// graceCutoffMs is the cutoff of the pass's dirty window.
func (p *pgxPass) graceCutoffMs() int64 {
	if p.published() {
		return noGraceCutoffMs
	}
	return p.src.graceCutoffMs
}

// armDeadline bounds a published pass by R_max from t0, before its snapshot
// begins. The cancel is the caller's to defer.
func (p *pgxPass) armDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	if !p.published() {
		return ctx, func() {}
	}
	return context.WithTimeoutCause(ctx, p.e.publishedPassDeadline, ErrPassDeadline)
}

// deadlineFailure turns the failure of a pass its own deadline aborted into a
// retryable passDeadlineError. A failure the request's own deadline or
// cancellation caused is left as it is.
func (p *pgxPass) deadlineFailure(passCtx context.Context, err error) error {
	if err == nil || !p.published() || !errors.Is(context.Cause(passCtx), ErrPassDeadline) {
		return err
	}
	return &passDeadlineError{SchemaID: p.q.SchemaID, Deadline: p.e.publishedPassDeadline, cause: err}
}

// readPublication reads the record as the snapshot's first statement. An
// unpublished record leaves the pass without a parquet leg.
func (p *pgxPass) readPublication(ctx context.Context, snap hotLegSnapshot) error {
	rec, err := snap.readPublication(ctx, p.e.publication, p.q.SchemaID)
	if err != nil {
		return p.publicationFailure(err, ErrPostgresReadFailed, "read the publication record in the pass's snapshot")
	}
	if rec.SchemaID != p.q.SchemaID {
		return fmt.Errorf("the publication reader answered schema %d's record for schema %d: %w", rec.SchemaID, p.q.SchemaID, errPgxHotLegNotConfigured)
	}
	p.record = rec
	p.noObjects = !rec.Published
	return nil
}

// publicationFailure keeps a consistency error as it is, and labels any other
// failure with class.
func (p *pgxPass) publicationFailure(err, class error, op string) error {
	if errors.Is(err, ErrPublicationInconsistent) {
		return fmt.Errorf("%s: %w", op, err)
	}
	return fmt.Errorf("%s: %w: %w", op, class, err)
}

// resolveGeneration fetches the generation's manifest and runs the pre-read
// checks over the objects it lists. A generation that lists none leaves the
// pass without a parquet leg.
func (p *pgxPass) resolveGeneration(ctx context.Context) error {
	if p.noObjects {
		return nil
	}
	gen, err := p.e.publication.ResolveGeneration(ctx, p.record)
	if err != nil {
		return p.publicationFailure(err, ErrFederatedReadFailed, "fetch the published generation's manifest")
	}
	if gen.SchemaID != p.record.SchemaID || gen.Epoch != p.record.Epoch || gen.Generation != p.record.Generation || len(gen.Keys) != len(gen.Paths) {
		return fmt.Errorf("the publication reader answered schema %d epoch %d generation %d with %d keys for %d paths, for schema %d epoch %d generation %d: %w",
			gen.SchemaID, gen.Epoch, gen.Generation, len(gen.Keys), len(gen.Paths), p.record.SchemaID, p.record.Epoch, p.record.Generation, errPgxHotLegNotConfigured)
	}
	p.gen = &gen
	if len(gen.Paths) == 0 {
		p.noObjects = true
		return nil
	}
	p.src = pgxScanSources{scanSources: scanSources{paths: gen.Paths, fromSource: true, graceCutoffMs: noGraceCutoffMs}}
	return p.checkPublishedObjects(ctx)
}

// checkPublishedObjects runs the pre-read checks the V1 resolution runs
// (resolvePgxScanSources): the objects cached as corrupt (#722), then the
// schema and type checks (#189, #732), with the manifest's columns as stamps.
func (p *pgxPass) checkPublishedObjects(ctx context.Context) error {
	e := p.e
	if corrupt := e.corruptPaths.Cached(p.gen.Paths); len(corrupt) > 0 {
		return p.refusePublishedCorrupt(ctx, corrupt)
	}
	if isBenchmarkSchemaID(p.q.SchemaID) {
		return nil
	}
	cold, err := e.validateScanSet(ctx, p.q.SchemaID, p.gen.Paths, true, p.gen.Columns)
	var unverified *unverifiedParquetError
	if errors.As(err, &unverified) {
		return e.failDuckDBScan(ctx, p.q, p.scan(), err, "pre-read parquet type verification")
	}
	if err != nil {
		return err
	}
	p.src.cold = cold
	return nil
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
	if err := e.publishedObjectsMissing(ctx, sc, cause, "probe a generation listing objects cached as corrupt"); err != nil {
		p.resolved = true // the probe went back with the consistency error
		return err
	}
	return &CorruptParquetError{SchemaID: p.q.SchemaID, Paths: corrupt, cause: fmt.Errorf("%w: %w", cause, ErrFederatedReadFailed)}
}

// publishedObjectsMissing probes a failed published read's objects, and
// answers the consistency error when the store lost any of them. The breaker
// gets its probe back: a lost object says nothing about DuckDB. A probe that
// cannot run proves no loss, and answers nil.
func (e *DBFederatedQueryEngine) publishedObjectsMissing(ctx context.Context, sc scan, cause error, op string) error {
	gen := sc.published
	if gen == nil || e.publication == nil {
		return nil
	}
	missing, err := e.publication.MissingIn(ctx, *gen, sc.parquetPaths)
	if err != nil || len(missing) == 0 {
		return nil
	}
	e.breaker.ReleaseProbe(sc.probe)
	return &PublicationConsistencyError{
		SchemaID: gen.SchemaID, Epoch: gen.Epoch, Generation: gen.Generation, MissingKeys: missing,
		// The cause is text only: it may carry ErrFederatedReadFailed or an
		// expired deadline, and neither may reclassify a lost object.
		Err: fmt.Errorf("%w: %s: %s", errPublishedObjectMissing, op, cause.Error()),
	}
}
