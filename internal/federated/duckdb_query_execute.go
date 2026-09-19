package federated

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/redact"
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
	dirtyIDs        []uuid.UUID
	// translateMs is the wall time SQL rendering took for this pass. It
	// rides along rather than being emitted at the render site so the pass's
	// telemetry is reported as one set once the pass has succeeded (see
	// emitDuckDBScanMetrics).
	translateMs int64
	// probe is this call's half-open reservation (zero when the breaker was
	// closed at admission), so the corrupt-confirmed release below can only
	// free a slot this caller actually holds (#349 review R2-2).
	probe ProbeToken
}

// executeAndStreamDuckDB runs the compiled query, streams its rows through
// rowHandler, and reports the outcome to the circuit breaker. Split from
// StreamDuckDBFederatedQuery to keep both halves under the function-length cap
// (#299 review): this half is the part that has actually touched DuckDB, so it
// owns every RecordFailure/RecordSuccess.
func (e *DBFederatedQueryEngine) executeAndStreamDuckDB(
	ctx context.Context,
	q *model.FederatedAttributeQuery,
	sqlStr string,
	args []any,
	sc scan,
	rowHandler func(context.Context, *model.PersistentRecord) error,
	planCtx *duckDBExecutionPlanContext,
) (int64, error) {
	planCtx.recordQueryStart()
	rows, err := e.duck.Query(ctx, sqlStr, args...)
	if err != nil {
		// #306: a postgres_scan attach failure echoes the whole conn string,
		// password included, in DuckDB's own prose. Scrub before the text
		// enters any chain or the execution-plan failure note — this is the
		// source, so every consumer (embedder logs, future transports) is
		// covered without repeating #301's boundary redaction.
		err = redact.Error(err)
		planCtx.recordQueryFailure(err)
		return 0, e.failDuckDBScan(ctx, q, sc, err, "execute duckdb query")
	}
	defer rows.Close()
	executeMs := planCtx.millisSince(planCtx.startQuery)

	streamStart := planCtx.now()
	totalRecords, rowCount, err := e.streamDuckDBRows(ctx, rows, rowHandler)
	if err != nil {
		// #306: lazy object opens mean the attach failure can surface here
		// instead of at Query; same scrub, same reason as above.
		err = redact.Error(err)
		if !errors.Is(err, ErrFederatedReadFailed) {
			// Handler errors are not read failures: they report to the
			// breaker as before and pass through unclassified.
			if e.breaker != nil {
				e.breaker.RecordFailure()
			}
			return 0, fmt.Errorf("stream duckdb federated rows: %w", err)
		}
		// A mid-stream read failure classifies like an execute failure:
		// DuckDB opens listed objects lazily, so a missing object can
		// surface here instead of at Query.
		//
		// Free the single pooled connection (#285 SetMaxOpenConns(1)) BEFORE
		// verification issues its own DuckDB queries — the deferred Close
		// runs too late and would deadlock the pool. sql.Rows.Close is
		// idempotent, so the defer stays harmless.
		_ = rows.Close()
		return 0, e.failDuckDBScan(ctx, q, sc, err, "stream duckdb federated rows")
	}

	if e.breaker != nil {
		e.breaker.RecordSuccess()
	}
	outcome := duckDBScanOutcome{
		translateMs:  sc.translateMs,
		executeMs:    executeMs,
		streamMs:     planCtx.millisSince(streamStart),
		rowCount:     rowCount,
		totalRecords: totalRecords,
		dirtyRows:    int64(len(sc.dirtyIDs)),
	}
	// Metrics first and unconditionally: the execution plan is an optional
	// diagnostic payload (IncludeExecutionPlan defaults to false on the API),
	// and the fed_query_* series must describe every successful pass, not
	// only the ones a caller asked to see a plan for (PR #595 review).
	// This is also the ONLY place the fed_query_* series are emitted, and it
	// is reached only after the pass succeeded: a failed pass leaves no
	// partial sample behind, and the #251 corrupt-parquet retry counts a
	// logical query once (the failed first pass is silent), which is what
	// docs/telemetry.md means by "per successful DuckDB pass".
	e.emitDuckDBScanMetrics(ctx, q.SchemaID, outcome)
	planCtx.recordScanOutcome(outcome)

	return totalRecords, nil
}

// duckDBScanOutcome is what one successful DuckDB pass measured. Both
// consumers below read from it so the metric stream and the execution plan
// can never disagree about the same pass.
type duckDBScanOutcome struct {
	// translateMs is the wall time of SQL rendering (buildDuckDBQueryWithPlan);
	// executeMs is the wall time of duck.Query alone; streamMs is the wall
	// time of the rows.Next/Scan/handler loop. They are disjoint stages: an
	// operator uses the split to tell a slow scan from slow result
	// consumption, which the previous "elapsed since query start" measure
	// (streaming folded into execution, streaming itself ~0) could not.
	translateMs, executeMs, streamMs int64
	// rowCount is the number of rows the pass streamed (the page); totalRecords
	// is the query's total match count as reported by the template's window
	// count, 0 when no row carried one.
	rowCount, totalRecords int64
	// dirtyRows is the size of the anti-join dirty set fetched for the pass;
	// see pushdownEfficiency for what it stands in for.
	dirtyRows int64
}

// emitDuckDBScanMetrics reports one successful pass to the engine's telemetry
// sink as a complete set: the three latency stages, both row-count sources
// and the per-schema pushdown-efficiency proxy. Everything the pass measured
// before DuckDB ran (the dirty-set size, the render time) is carried here in
// the outcome instead of being emitted where it was measured, so a consumer
// never sees a translation or pg sample without the execution that followed
// it, and a failed-then-retried query is not counted twice (PR #595 review).
// Nil-sink safe (Sink methods no-op).
func (e *DBFederatedQueryEngine) emitDuckDBScanMetrics(ctx context.Context, schemaID int16, o duckDBScanOutcome) {
	e.metrics.EmitLatency(ctx, "translation", o.translateMs)
	e.metrics.EmitRowCount(ctx, "pg", o.dirtyRows)
	e.metrics.EmitLatency(ctx, "execution", o.executeMs)
	e.metrics.EmitLatency(ctx, "streaming", o.streamMs)
	e.metrics.EmitRowCount(ctx, "duckdb", o.rowCount)
	ratio, _ := pushdownEfficiency(o)
	e.metrics.EmitPushdownEfficiency(ctx, schemaID, ratio)
}

// pushdownEfficiency is the value behind fed_query_pushdown_efficiency: the
// dirty-set size over the final matching row count, with the row count of
// the streamed page as the denominator when the template reported no total
// and 1 when the pass matched nothing at all (so an empty result reads as
// "dirtyRows hot rows considered per zero results", never as a division by
// zero). It also returns the denominator it used, for the plan note.
//
// The numerator is a proxy, and the descriptor says so: Forma never sees how
// many rows the postgres_scan inside the pg_source CTE touched, so the
// anti-join dirty set — the upper bound of hot rows pg_source can return when
// nothing is pushed down — stands in for "Postgres rows scanned". Measuring
// the real scan count, or retiring the gauge, is #596.
func pushdownEfficiency(o duckDBScanOutcome) (ratio float64, finalRows int64) {
	finalRows = o.totalRecords
	if finalRows <= 0 {
		finalRows = o.rowCount
	}
	if finalRows <= 0 {
		finalRows = 1
	}
	return float64(o.dirtyRows) / float64(finalRows), finalRows
}

// recordScanOutcome completes the requested execution plan with the pass's
// timings and row counts. Unlike emitDuckDBScanMetrics it IS gated on
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

	ratio, finalRows := pushdownEfficiency(o)
	plan.Notes = append(plan.Notes,
		fmt.Sprintf("pushdown_efficiency=%.3f (dirty_rows=%d final_rows=%d)", ratio, o.dirtyRows, finalRows))
}

// failDuckDBScan classifies a failed scan and reports it to the breaker.
// Classification order is a contract: a manifest-listed object missing from
// storage is inconsistency (#187 scenario 2) — non-degradable, breaker-worthy,
// never retried — and must win over the corruption probe. Confirmed per-file
// corruption (#251) is the one outcome that is NOT engine sickness: the
// verification pass just read every other object through the same engine and
// session, so it hands back the probe slot instead of recording a failure —
// a permanently corrupt object must not hold the breaker open forever.
// Last, a failure neither branch claimed runs the #351 guard identification:
// the bare drains just read every object clean, so if a guarded single-file
// drain fails deterministically the failure is a schema-invariant violation
// and the error names the object(s). Identification decorates — it never
// excludes, never retries, and never changes the classification chain.
func (e *DBFederatedQueryEngine) failDuckDBScan(ctx context.Context, q *model.FederatedAttributeQuery, sc scan, cause error, op string) error {
	classified := e.classifyDuckDBReadError(ctx, q, sc.parquetPaths, sc.pathsFromSource)
	var inconsistent *ParquetSetInconsistentError
	if errors.As(classified, &inconsistent) {
		if e.breaker != nil {
			e.breaker.RecordFailure()
		}
		return fmt.Errorf("%s: %w: %w", op, classified, cause)
	}
	if corrupt := e.confirmCorruptPaths(ctx, sc); len(corrupt) > 0 {
		e.corruptPaths.Add(corrupt)
		if e.breaker != nil {
			e.breaker.ReleaseProbe(sc.probe)
		}
		return &corruptParquetRetryError{Corrupt: corrupt, cause: fmt.Errorf("%s: %w: %w", op, classified, cause)}
	}
	if e.breaker != nil {
		e.breaker.RecordFailure()
	}
	wrapped := fmt.Errorf("%s: %w: %w", op, classified, cause)
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

// identifyGuardViolationPaths gates the #351 identification the way
// confirmCorruptPaths gates #251 verification: source-authored sets only —
// hint-authored paths name objects no manifest vouches for, and the caller
// pinned them deliberately. Unlike #251 it runs for single-path sets too:
// verification needs a readable remainder to be worth confirming, whereas
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
// store or engine is sick, not the files, and exclusion would be both wrong
// and useless (an empty remainder cannot answer the query).
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

// streamDuckDBRows iterates through DuckDB rows and invokes the handler.
func (e *DBFederatedQueryEngine) streamDuckDBRows(
	ctx context.Context,
	rows duckDBRowsIterator,
	rowHandler func(context.Context, *model.PersistentRecord) error,
) (int64, int64, error) {
	buffers := newDuckDBScanBuffers()

	var totalRecords int64
	totalSet := false
	rowCount := int64(0)

	for rows.Next() {
		scanArgs, attrsJSON, totalRec, _, _ := buffers.buildScanArgs()

		if err := rows.Scan(scanArgs...); err != nil {
			return 0, 0, fmt.Errorf("scan duckdb row: %w: %w", ErrFederatedReadFailed, err)
		}

		// Build record from buffers
		record := buffers.buildRecordFromBuffers()

		// Parse attributes JSON
		if attrsJSON.Valid {
			if err := parseDuckDBAttributesJSON(attrsJSON.String, record); err != nil {
				return 0, 0, err
			}
		}

		// Clean up empty maps
		model.CleanupEmptyMaps(record)

		if !totalSet && totalRec.Valid {
			totalRecords = totalRec.Int64
			totalSet = true
		}

		// Invoke handler
		if rowHandler != nil {
			if err := rowHandler(ctx, record); err != nil {
				return 0, 0, err
			}
		}

		rowCount++
	}

	if err := rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("iterate duckdb rows: %w: %w", ErrFederatedReadFailed, err)
	}

	return totalRecords, rowCount, nil
}
