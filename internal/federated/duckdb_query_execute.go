package federated

import (
	"context"
	"errors"
	"fmt"

	"github.com/lychee-technology/forma/internal/model"
	"go.uber.org/zap"
)

// Execute-and-stream half of the DuckDB federated read path: running the
// compiled query, streaming its rows, and reporting the outcome to the circuit
// breaker. Split from duckdb_query.go to keep that file under the 500-line cap
// (#299 review; same seam family as duckdb_query_build.go per #220) — and
// because this is precisely the half that has touched the dependency, which is
// what makes it the owner of every breaker Record* call.

// scan carries the resolved storage context of one federated read, so the
// execute/stream half can classify failures against the exact path set the scan
// used without re-threading four parameters.
type scan struct {
	parquetPaths    []string
	pathsFromSource bool
	// probe is this call's half-open reservation (zero when the breaker was
	// closed at admission), so the corrupt-confirmed release below can only
	// free a slot this caller actually holds (#349 review R2-2).
	probe ProbeToken
	// globListed marks a pgx pass's frozen inventory: objects a glob listed,
	// which no manifest vouches for, so a missing one is not a manifest
	// inconsistency.
	globListed bool
}

// retryablePassFailure reports whether a failed pass earns the one retry,
// and why. A pass whose empty parquet scan a publication raced is discarded,
// and the retry's resolution finds the published objects (#720). Confirmed
// corruption earns no retry (#722): a remainder without the corrupt objects
// can answer a newer row with an older version.
func retryablePassFailure(err error) (string, bool) {
	var raced *parquetPublishedDuringReadError
	if errors.As(err, &raced) {
		return "a parquet publication raced the empty scan", true
	}
	return "", false
}

// duckDBScanOutcome is what one successful DuckDB pass measured. Both
// consumers below read from it so the metric stream and the execution plan
// can never disagree about the same pass.
type duckDBScanOutcome struct {
	// translateMs is the wall time of SQL rendering; executeMs is the wall
	// time of the statement's execution alone; streamMs is the wall time of
	// the rows.Next/Scan/handler loop. They are disjoint stages: an
	// operator uses the split to tell a slow scan from slow result
	// consumption, which the previous "elapsed since query start" measure
	// (streaming folded into execution, streaming itself ~0) could not.
	translateMs, executeMs, streamMs int64
	// rowCount is the number of rows the pass streamed (the page).
	rowCount int64
	// dirtyRows is the size of the anti-join dirty set the pass staged.
	dirtyRows int64
}

// recordScanOutcome completes the requested execution plan with the pass's
// timings and row counts. Unlike the fed_query_* metrics it IS gated on
// IncludeExecutionPlan: the plan is the caller's opt-in diagnostic payload.
// duckdb_fetch and the DuckDB source's DurationMs keep their meaning as the
// whole fetch (execute plus stream), so existing plan readers are unaffected
// by the stage split the metrics now report.
func (c *duckDBExecutionPlanContext) recordScanOutcome(o duckDBScanOutcome) {
	if c.opts == nil || !c.opts.IncludeExecutionPlan || c.opts.ExecutionPlan == nil {
		return
	}
	plan := c.opts.ExecutionPlan
	fetchMs := o.executeMs + o.streamMs

	// Update the last source with actual rows and duration
	if len(plan.Sources) > 0 {
		idx := len(plan.Sources) - 1
		dp := plan.Sources[idx]
		dp.ActualRows = o.rowCount
		dp.DurationMs = fetchMs
		plan.Sources[idx] = dp
	}

	plan.Timings["duckdb_fetch"] = fetchMs
	plan.Timings["total"] = c.millisSince(c.startTotal)
}

// failDuckDBScan classifies a failed scan and reports it to the breaker.
// Classification order is a contract: a manifest-listed object missing from
// storage is inconsistency (#187 scenario 2) — non-degradable, breaker-worthy,
// never retried — and must win over the corruption probe. Confirmed per-file
// corruption (#251) is the one outcome that is NOT engine sickness: the
// verification pass just read every other object through the same engine and
// session, so it hands back the probe slot instead of recording a failure —
// a permanently corrupt object must not hold the breaker open forever. The
// objects are cached, so the queries that follow fail at path resolution
// without a scan, and the read fails with CorruptParquetError rather than
// answering from the remainder (#722).
// Last, a failure neither branch claimed runs the #351 guard identification:
// the bare drains just read every object clean, so if a guarded single-file
// drain fails deterministically the failure is a schema-invariant violation
// and the error names the object(s). Identification decorates — it never
// caches, never retries, and never changes the classification chain.
func (e *DBFederatedQueryEngine) failDuckDBScan(ctx context.Context, q *model.FederatedAttributeQuery, sc scan, cause error, op string) error {
	classified := e.classifyDuckDBReadError(ctx, q, sc.parquetPaths, sc.pathsFromSource && !sc.globListed)
	var inconsistent *ParquetSetInconsistentError
	wrapped := fmt.Errorf("%s: %w: %w", op, classified, cause)
	if errors.As(classified, &inconsistent) {
		e.breakerRecordFailure(ctx, wrapped)
		return wrapped
	}
	if corrupt := e.confirmCorruptPaths(ctx, sc); len(corrupt) > 0 {
		e.corruptPaths.Add(corrupt)
		e.breaker.ReleaseProbe(sc.probe)
		// Once per object per retention window. A degraded fallback or the
		// HTTP layer logs each refused query, but only this line marks the
		// verdict, and it names the objects an operator has to repair.
		e.log().Error("parquet objects confirmed corrupt; federated reads of the schema fail or degrade until they are repaired or retired",
			zap.Int16("schema_id", q.SchemaID),
			zap.Strings("paths", corrupt))
		return &CorruptParquetError{SchemaID: q.SchemaID, Paths: corrupt, cause: wrapped}
	}
	e.breakerRecordFailure(ctx, wrapped)
	violating := e.identifyGuardViolationPaths(ctx, sc)
	if len(violating) == 0 {
		return wrapped
	}
	// Degraded mode absorbs this error and toExecutionPlan drops plan Notes,
	// so the log is the only outlet that survives the fallback.
	e.log().Error("parquet scan-guard failure attributed to objects",
		zap.Int16("schema_id", q.SchemaID),
		zap.Strings("paths", violating))
	return &ParquetGuardViolationError{SchemaID: q.SchemaID, Paths: violating, cause: wrapped}
}

// failScanSources resolves the probe for a pre-flight (resolvePgxScanSources)
// failure. Objects the #732 type guard could not read fail the read closed,
// classified exactly like a failed scan over the same path set, so
// corrupt-object confirmation (#251) and missing-object inconsistency (#187)
// keep their semantics, and an expired budget in its cause stays
// non-degradable (#465). A cached-corrupt refusal that found listed objects
// missing (#722) probed storage and reports the same inconsistency a failed
// scan would, so it records a failure the same way. Any other pre-flight
// failure learned nothing about the dependency and hands the probe back.
func (e *DBFederatedQueryEngine) failScanSources(ctx context.Context, q *model.FederatedAttributeQuery, probe ProbeToken, err error) error {
	var unverified *unverifiedParquetError
	if errors.As(err, &unverified) {
		return e.failDuckDBScan(ctx, q, scan{
			parquetPaths:    unverified.scanned,
			pathsFromSource: unverified.fromSource,
			globListed:      unverified.globListed,
			probe:           probe,
		}, err, "pre-read parquet type verification")
	}
	var inconsistent *ParquetSetInconsistentError
	if errors.As(err, &inconsistent) {
		e.breakerRecordFailure(ctx, err)
		return err
	}
	e.breaker.ReleaseProbe(probe)
	return err
}

// identifyGuardViolationPaths gates the #351 identification the way
// confirmCorruptPaths gates #251 verification: source-authored sets only —
// hint-authored paths name objects no manifest vouches for, and the caller
// pinned them deliberately. Unlike #251 it runs for single-path sets too:
// verification needs an object that reads clean to tell a corrupt file from
// a sick store, whereas
// naming the one object in the set is exactly what an operator needs.
func (e *DBFederatedQueryEngine) identifyGuardViolationPaths(ctx context.Context, sc scan) []string {
	if e == nil || e.duck == nil || !sc.pathsFromSource || len(sc.parquetPaths) == 0 {
		return nil
	}
	return identifyGuardViolations(ctx, e.duck, sc.parquetPaths)
}

// confirmCorruptPaths runs per-file verification when the failed scan ran
// over a source-authored multi-object set. It confirms corruption only when
// at least one object verified readable — if every object fails to read, the
// store or engine is sick, not the files, and a cached verdict would refuse
// reads that a recovered store could answer.
func (e *DBFederatedQueryEngine) confirmCorruptPaths(ctx context.Context, sc scan) []string {
	if !sc.pathsFromSource || len(sc.parquetPaths) < 2 || e == nil || e.duck == nil {
		return nil
	}
	corrupt := verifyParquetPaths(ctx, e.duck, sc.parquetPaths)
	if len(corrupt) == 0 || len(corrupt) >= len(sc.parquetPaths) {
		return nil
	}
	return corrupt
}
