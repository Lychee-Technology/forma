package federated

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/model"
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
		planCtx.recordQueryFailure(err)
		if e.breaker != nil {
			e.breaker.RecordFailure()
		}
		return 0, fmt.Errorf("execute duckdb query: %w: %w",
			e.classifyDuckDBReadError(ctx, q, sc.parquetPaths, sc.pathsFromSource), err)
	}
	defer rows.Close()

	totalRecords, rowCount, err := e.streamDuckDBRows(ctx, rows, rowHandler)
	if err != nil {
		if e.breaker != nil {
			e.breaker.RecordFailure()
		}
		// A mid-stream read failure classifies like an execute failure:
		// DuckDB opens listed objects lazily, so a missing object can
		// surface here instead of at Query. Handler errors are not read
		// failures and pass through unclassified.
		if errors.Is(err, ErrFederatedReadFailed) {
			if classified := e.classifyDuckDBReadError(ctx, q, sc.parquetPaths, sc.pathsFromSource); classified != ErrFederatedReadFailed {
				return 0, fmt.Errorf("%w: %w", classified, err)
			}
		}
		return 0, fmt.Errorf("stream duckdb federated rows: %w", err)
	}

	if e.breaker != nil {
		e.breaker.RecordSuccess()
	}
	e.finalizeDuckDBExecutionPlan(ctx, planCtx, sc.dirtyIDs, totalRecords, rowCount)

	return totalRecords, nil
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
