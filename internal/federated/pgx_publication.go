package federated

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lychee-technology/forma"
)

// The parquet source of a pgx pass (docs/publication-protocol-v2.md,
// "Readers"). Every pass resolves what its parquet leg scans inside its
// snapshot, through the engine's PublicationReader: the publication record is
// the snapshot's first statement, so the generation the pass scans and the
// dirty set it discards come from one snapshot (Invariant R). No glob is
// listed, and the dirty set is the strict one: a generation read in the
// snapshot holds every row the snapshot shows flushed, so nothing widens the
// barrier. The reader's implementation is internal/federatedv2's Source,
// which the factory wires whenever DuckDB is enabled.

// DefaultPublishedPassDeadline is R_max, the pass deadline of a published read
// when WithPublicationReader is given none. Garbage collection keeps a
// superseded generation for a grace G >= R_max + C (the clock margin), so a
// pass must end by then whatever DuckDBConfig.QueryTimeout says, and that
// budget may be zero (no bound). It is twice the default query budget, so
// under the default configuration QueryTimeout expires first and R_max only
// bounds a deployment that raised or disabled it.
const DefaultPublishedPassDeadline = 60 * time.Second

// PublicationReader resolves a pass's parquet source. Its implementation is
// internal/federatedv2's Source. An engine without one fails every pass as
// misconfigured, before it reads anything.
type PublicationReader interface {
	// ReadRecord reads schemaID's publication record on tx, the pass's
	// REPEATABLE READ snapshot, as the snapshot's first statement. A record
	// that is absent, malformed, AWAITING_INIT, DROPPED or at an epoch other
	// than the one the process pinned fails with a
	// *PublicationConsistencyError; any other error is a failed PostgreSQL
	// read.
	ReadRecord(ctx context.Context, tx pgx.Tx, schemaID int16) (PublicationRecord, error)
	// ResolveGeneration answers the objects a published record's generation
	// lists, from its verified manifest. A missing or invalid manifest is a
	// *PublicationConsistencyError; any other error is a failed fetch.
	ResolveGeneration(ctx context.Context, rec PublicationRecord) (PublishedGeneration, error)
	// MissingIn answers the keys of gen's objects among scanned that the
	// store does not hold. It runs only after a failed read. A probe that
	// fails leaves the keys the other probes found missing in the answer,
	// beside the error: a confirmed loss is never taken back.
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
// is not positive. A pass of an engine built without it fails as
// misconfigured, non-degradably: without the record no pass can tell a
// schema whose rows are all hot from one whose parquet it would drop.
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
// cannot be answered from. It is forma.ErrPublicationInconsistent, defined in
// the root package so that embedders and internal/httpapi can match it: no
// record, a record that is AWAITING_INIT or DROPPED or at an epoch the process
// did not pin, a manifest that is missing or is not the one PostgreSQL
// committed, or a listed object the store does not hold. It is a read-path
// consistency error, never a 4xx: it is not degradable, since a
// PostgreSQL-only answer would drop every row the parquet tiers hold, it is
// never charged to the circuit breaker, which measures DuckDB's health, and it
// is never ErrFederatedReadFailed.
var ErrPublicationInconsistent = forma.ErrPublicationInconsistent

// PublicationConsistencyError names the schema, the epoch and the generation
// of a published read's consistency failure (forma.PublicationConsistencyError).
// MissingKeys are storage keys, operator detail that the HTTP boundary keeps
// out of response bodies.
type PublicationConsistencyError = forma.PublicationConsistencyError

// armDeadline bounds a pass by R_max from t0, before its snapshot begins.
// The cancel is the caller's to defer.
func (p *pgxPass) armDeadline(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeoutCause(ctx, p.e.publishedPassDeadline, ErrPassDeadline)
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
	if err := refuseGlobPaths(gen); err != nil {
		return err
	}
	if err := p.admitPublishedScan(ctx); err != nil {
		return err
	}
	p.src = scanSources{paths: gen.Paths}
	return p.checkPublishedObjects(ctx)
}

// ErrPublishedPathNotConcrete is the fault of a generation that lists an
// object whose address holds a glob metacharacter (forma.
// ParquetGlobMetacharacters). DuckDB would expand such an address, in the
// pre-read checks and in read_parquet alike, and read whatever matches it:
// objects the generation does not list. The pass refuses the generation
// before any DuckDB work, as a PublicationConsistencyError.
var ErrPublishedPathNotConcrete = errors.New("the generation lists an object whose address DuckDB would expand as a glob")

// ErrPublishedPathNotCanonical is the fault of a generation that lists an
// object whose address holds a `.` or `..` segment (forma.HasDotPathSegment).
// The writer stored the object under the key as spelled, while DuckDB
// normalizes the segment away and would address another object. The pass
// refuses the generation before any DuckDB work, as a
// PublicationConsistencyError: the committed inventory is not what a read can
// address, and a PostgreSQL-only answer would drop its rows.
var ErrPublishedPathNotCanonical = errors.New("the generation lists an object whose address DuckDB would normalize to another object")

// refuseGlobPaths refuses a generation that lists an address DuckDB would
// not read as written: every listed object must reach DuckDB as one concrete
// object, under the key the writer stored it at.
func refuseGlobPaths(gen PublishedGeneration) error {
	var globs, dotted []string
	for _, path := range gen.Paths {
		switch {
		case strings.ContainsAny(path, forma.ParquetGlobMetacharacters):
			globs = append(globs, path)
		case forma.HasDotPathSegment(path):
			dotted = append(dotted, path)
		}
	}
	var err error
	switch {
	case len(globs) > 0:
		err = fmt.Errorf("%w: %s", ErrPublishedPathNotConcrete, strings.Join(globs, ", "))
	case len(dotted) > 0:
		err = fmt.Errorf("%w: %s", ErrPublishedPathNotCanonical, strings.Join(dotted, ", "))
	default:
		return nil
	}
	return &PublicationConsistencyError{
		SchemaID: gen.SchemaID, Epoch: gen.Epoch, Generation: gen.Generation, Err: err,
	}
}

// admitPublishedScan is a pass's DuckDB admission: DuckDB must be there,
// and the breaker must admit the scan, reserving its half-open probe. A pass
// meets it only here, once its snapshot's generation lists objects to scan. A pass without a parquet leg, unpublished or over an
// empty generation, needs no DuckDB, so DuckDB's health never refuses it,
// and a record the pass must fail closed on is refused before admission can
// answer for it with a degradable rejection.
func (p *pgxPass) admitPublishedScan(ctx context.Context) error {
	if p.e.duck == nil {
		p.planCtx.recordClientUnavailable()
		return fmt.Errorf("duckdb client not available: %w", ErrDuckDBUnavailable)
	}
	admitted, probe := p.e.breakerAllow(ctx)
	if !admitted {
		p.planCtx.recordClientUnavailable()
		return fmt.Errorf("%w: %w", errBreakerRejected, ErrDuckDBUnavailable)
	}
	p.probe = probe
	return nil
}

// checkPublishedObjects runs the pre-read checks over the generation's
// objects: the objects cached as corrupt (#722), then the schema and type
// checks (#189, #732), with the manifest's columns as stamps.
func (p *pgxPass) checkPublishedObjects(ctx context.Context) error {
	e := p.e
	if corrupt := e.corruptPaths.Cached(p.gen.Paths); len(corrupt) > 0 {
		return p.refusePublishedCorrupt(ctx, corrupt)
	}
	if isBenchmarkSchemaID(p.q.SchemaID) {
		return nil
	}
	cold, err := e.validateScanSet(ctx, p.q.SchemaID, p.gen.Paths, p.gen.Columns)
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
