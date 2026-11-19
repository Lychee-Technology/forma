package cdc

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/lychee-technology/forma/internal/duckdbinit"
)

// stagePostgresRows reads every stage's PostgreSQL rows and appends them to
// the stage's DuckDB temp table on duckConn, which must be the connection
// the stage tables were created on and the COPY will run on.
//
// All reads share one REPEATABLE READ READ ONLY transaction, so one export
// pairs each change_log version with main and EAV values of the same
// snapshot. Marking still compares against the versions listed when the
// batch was selected (MarkFlushedVersions). The transaction ends before the
// COPY starts, so the S3 write does not extend it.
//
// Rows are appended as pgx yields them; nothing buffers a whole stage in Go.
// It answers the number of rows staged across all stages. cdc-flush's delta
// export stages this way; cdc-init's base export stages on the snapshot it is
// handed instead (stageRowsOnTx).
func stagePostgresRows(ctx context.Context, pg *sql.DB, duckConn *sql.Conn, stages []exportStage, rowIDs []uuid.UUID) (int64, error) {
	if len(stages) == 0 {
		return 0, nil
	}
	if pg == nil {
		return 0, errors.New("stage export rows: no PostgreSQL connection")
	}
	pgConn, err := pg.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("stage export rows: acquire postgres connection: %w", err)
	}
	defer pgConn.Close()

	var staged int64
	err = pgConn.Raw(func(driverConn any) error {
		stdConn, ok := driverConn.(*stdlib.Conn)
		if !ok {
			return fmt.Errorf("postgres connection %T is not a pgx connection", driverConn)
		}
		n, snapshotErr := stageInSnapshot(ctx, stdConn.Conn(), duckConn, stages, rowIDs)
		staged = n
		return snapshotErr
	})
	if err != nil {
		return staged, fmt.Errorf("stage export rows: %w", err)
	}
	return staged, nil
}

func stageInSnapshot(ctx context.Context, pgConn *pgx.Conn, duckConn *sql.Conn, stages []exportStage, rowIDs []uuid.UUID) (int64, error) {
	tx, err := pgConn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return 0, fmt.Errorf("begin read-only export transaction: %w", err)
	}
	// A read-only transaction has nothing to undo; Rollback after Commit is
	// a no-op, and after a failure it releases the snapshot.
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	staged, err := stageAll(ctx, pgxQuery(tx), duckConn, stages, rowIDs)
	if err != nil {
		return staged, err
	}
	if err := tx.Commit(ctx); err != nil {
		return staged, fmt.Errorf("end read-only export transaction: %w", err)
	}
	return staged, nil
}

// stageRowsOnTx is stagePostgresRows on snap, a transaction the caller
// began, holds and ends: cdc-init's export snapshot, whose first statement
// listed the rows the stages read (cdcv2.BaseExporter). The reads go through
// database/sql, so each scan target also takes the value pgx's stdlib
// answers for its column (stagedDouble.Scan).
func stageRowsOnTx(ctx context.Context, snap *sql.Tx, duckConn *sql.Conn, stages []exportStage, rowIDs []uuid.UUID) (int64, error) {
	if snap == nil {
		return 0, errors.New("stage export rows: no export snapshot")
	}
	staged, err := stageAll(ctx, sqlTxQuery(snap), duckConn, stages, rowIDs)
	if err != nil {
		return staged, fmt.Errorf("stage export rows in the export snapshot: %w", err)
	}
	return staged, nil
}

// stageRows is the row surface the staging reads: pgx.Rows, and *sql.Rows
// through sqlRows.
type stageRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close()
}

// stageQueryFunc runs one stage's query with the batch's row ids bound as $1.
type stageQueryFunc func(ctx context.Context, query string, rowIDs []uuid.UUID) (stageRows, error)

func pgxQuery(tx pgx.Tx) stageQueryFunc {
	return func(ctx context.Context, query string, rowIDs []uuid.UUID) (stageRows, error) {
		rows, err := tx.Query(ctx, query, rowIDs)
		if err != nil {
			return nil, fmt.Errorf("query postgres: %w", err)
		}
		return rows, nil
	}
}

func sqlTxQuery(tx *sql.Tx) stageQueryFunc {
	return func(ctx context.Context, query string, rowIDs []uuid.UUID) (stageRows, error) {
		rows, err := tx.QueryContext(ctx, query, rowIDs)
		if err != nil {
			return nil, fmt.Errorf("query postgres: %w", err)
		}
		return sqlRows{rows}, nil
	}
}

// sqlRows adapts *sql.Rows to stageRows.
type sqlRows struct{ *sql.Rows }

func (r sqlRows) Close() { _ = r.Rows.Close() }

// stageAll stages every stage in plan order through query.
func stageAll(ctx context.Context, query stageQueryFunc, duckConn *sql.Conn, stages []exportStage, rowIDs []uuid.UUID) (int64, error) {
	var staged int64
	for _, stage := range stages {
		n, err := stageOne(ctx, query, duckConn, stage, rowIDs)
		if err != nil {
			return staged, fmt.Errorf("stage %s rows: %w", stage.alias, err)
		}
		staged += n
	}
	return staged, nil
}

// stageOne streams one stage's PostgreSQL result into its temp table.
func stageOne(ctx context.Context, query stageQueryFunc, duckConn *sql.Conn, stage exportStage, rowIDs []uuid.UUID) (int64, error) {
	var appended int64
	err := duckConn.Raw(func(driverConn any) error {
		app, err := duckdbinit.NewAppender(driverConn, "temp", "main", stage.table)
		if err != nil {
			return err // names the temp table; stageOne wraps every closure error
		}
		var appendErr error
		appended, appendErr = appendStageRows(ctx, query, stage, rowIDs, app.AppendRow)
		closeErr := app.CloseWithCancel(ctx)
		if closeErr != nil {
			duckdbinit.ObserveRaw(driverConn, closeErr)
		}
		if appendErr != nil {
			return appendErr
		}
		if closeErr != nil {
			return fmt.Errorf("flush duckdb appender: %w", closeErr)
		}
		return nil
	})
	if err != nil {
		return appended, fmt.Errorf("copy postgres rows into duckdb: %w", err)
	}
	return appended, nil
}

func appendStageRows(ctx context.Context, query stageQueryFunc, stage exportStage, rowIDs []uuid.UUID, appendRow func(...driver.Value) error) (int64, error) {
	rows, err := query(ctx, stage.query, rowIDs)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	decoder := newStageRowDecoder(stage.columns)
	var appended int64
	for rows.Next() {
		// pgx names the offending column in a scan error.
		if err := rows.Scan(decoder.targets...); err != nil {
			return appended, fmt.Errorf("decode postgres row: %w", err)
		}
		if err := appendRow(decoder.values()...); err != nil {
			return appended, fmt.Errorf("append row to %s: %w", stage.table, err)
		}
		appended++
	}
	if err := rows.Err(); err != nil {
		return appended, fmt.Errorf("read postgres rows: %w", err)
	}
	return appended, nil
}
