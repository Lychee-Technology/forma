package federated

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/redact"
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
	e.finalizeDuckDBExecutionPlan(ctx, planCtx, sc.dirtyIDs, totalRecords, rowCount)

	return totalRecords, nil
}

// failDuckDBScan classifies a failed scan and reports it to the breaker.
// Classification order is a contract: a manifest-listed object missing from
// storage is inconsistency (#187 scenario 2) — non-degradable, breaker-worthy,
// never retried — and must win over the corruption probe. Confirmed per-file
// corruption (#251) is the one outcome that is NOT engine sickness: the
// verification pass just read every other object through the same engine and
// session, so it hands back the probe slot instead of recording a failure —
// a permanently corrupt object must not hold the breaker open forever.
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
			e.breaker.ReleaseProbe()
		}
		return &corruptParquetRetryError{Corrupt: corrupt, cause: fmt.Errorf("%s: %w: %w", op, classified, cause)}
	}
	if e.breaker != nil {
		e.breaker.RecordFailure()
	}
	return fmt.Errorf("%s: %w: %w", op, classified, cause)
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
