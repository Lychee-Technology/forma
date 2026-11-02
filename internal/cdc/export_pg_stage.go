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
// It answers the number of rows staged across all stages.
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
			return fmt.Errorf("stage export rows: postgres connection %T is not a pgx connection", driverConn)
		}
		staged, err = stageInSnapshot(ctx, stdConn.Conn(), duckConn, stages, rowIDs)
		return err
	})
	return staged, err
}

func stageInSnapshot(ctx context.Context, pgConn *pgx.Conn, duckConn *sql.Conn, stages []exportStage, rowIDs []uuid.UUID) (int64, error) {
	tx, err := pgConn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return 0, fmt.Errorf("begin read-only export transaction: %w", err)
	}
	// A read-only transaction has nothing to undo; Rollback after Commit is
	// a no-op, and after a failure it releases the snapshot.
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	var staged int64
	for _, stage := range stages {
		n, err := stageOne(ctx, tx, duckConn, stage, rowIDs)
		if err != nil {
			return staged, err
		}
		staged += n
	}
	if err := tx.Commit(ctx); err != nil {
		return staged, fmt.Errorf("end read-only export transaction: %w", err)
	}
	return staged, nil
}

// stageOne streams one stage's PostgreSQL result into its temp table.
func stageOne(ctx context.Context, tx pgx.Tx, duckConn *sql.Conn, stage exportStage, rowIDs []uuid.UUID) (int64, error) {
	var appended int64
	err := duckConn.Raw(func(driverConn any) error {
		app, err := duckdbinit.NewAppender(driverConn, "temp", "main", stage.table)
		if err != nil {
			return fmt.Errorf("stage %s rows: %w", stage.alias, err)
		}
		var appendErr error
		appended, appendErr = appendStageRows(ctx, tx, stage, rowIDs, app.AppendRow)
		closeErr := app.CloseWithCancel(ctx)
		if closeErr != nil {
			duckdbinit.ObserveRaw(driverConn, closeErr)
		}
		if appendErr != nil {
			return fmt.Errorf("stage %s rows: %w", stage.alias, appendErr)
		}
		if closeErr != nil {
			return fmt.Errorf("stage %s rows: flush duckdb appender: %w", stage.alias, closeErr)
		}
		return nil
	})
	return appended, err
}

func appendStageRows(ctx context.Context, tx pgx.Tx, stage exportStage, rowIDs []uuid.UUID, appendRow func(...driver.Value) error) (int64, error) {
	rows, err := tx.Query(ctx, stage.query, rowIDs)
	if err != nil {
		return 0, fmt.Errorf("query postgres: %w", err)
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
