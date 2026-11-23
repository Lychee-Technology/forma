package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/schemaledger"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// defaultDropPurgeBatch is the rows one purge statement of drop-schema
// deletes from one table.
const defaultDropPurgeBatch = 5000

// dropTarget names the schema drop-schema retires: by name, through the
// registry, or by id, for an id whose registry row is gone already.
type dropTarget struct {
	name string
	id   int
}

// runDropSchema is the drop transition of a registered schema
// (docs/publication-protocol-v2.md, "Lifecycle of the row"): it retires the
// schema's publication record, then purges its rows, so that init-db can
// register the id again at a new epoch.
func runDropSchema(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("drop-schema", flag.ContinueOnError)
	flags.SetOutput(os.Stdout)
	flags.Usage = func() {
		fmt.Println("Usage: forma-tools drop-schema (--schema-name NAME | --schema-id ID) [options]")
		fmt.Println("")
		fmt.Println("Retires a schema: deletes its registry row and schema ledger, moves its publication record to")
		fmt.Println("DROPPED at the next epoch in the same transaction, then purges its entity_main, EAV and")
		fmt.Println("change_log rows and its unique-key entries and build state. Stop every server, cdc-flush")
		fmt.Println("and cdc-init that loaded the schema first.")
		fmt.Println("A rerun resumes an interrupted purge. Its parquet objects stay until garbage collection.")
		fmt.Println("")
		fmt.Println("Options:")
		flags.PrintDefaults()
	}
	opts := initDBOptions{}
	var pg postgresFlags
	portErr := registerDatabaseFlags(flags, &opts, &pg)
	var target dropTarget
	flags.StringVar(&target.name, "schema-name", "", "name of the registered schema to drop")
	flags.IntVar(&target.id, "schema-id", 0, "id of the schema to drop, also when its registry row is gone already")
	batch := flags.Int("purge-batch", defaultDropPurgeBatch, "rows one purge statement deletes from one table")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if portErr != nil {
		return fmt.Errorf("resolve --db-port default: %w", portErr)
	}
	if (target.name == "") == (target.id == 0) {
		return &forma.ConfigError{Field: "schema-name", Message: "give exactly one of --schema-name and --schema-id"}
	}
	if target.id < 0 || target.id > 32767 {
		return &forma.ConfigError{Field: "schema-id", Message: "must be a schema id between 1 and 32767"}
	}
	if *batch <= 0 {
		return &forma.ConfigError{Field: "purge-batch", Message: "must be positive"}
	}
	pool, err := buildToolPostgresPool(ctx, pg.databaseConfig("DB_PASSWORD", toolPostgresPoolSettings{maxConnections: 2, timeout: 30 * time.Second}))
	if err != nil {
		return fmt.Errorf("create connection pool: %w", err)
	}
	defer pool.Close()
	return dropSchema(ctx, pool, opts, target, *batch, os.Stdout)
}

// dropSchema retires target's record, then purges its rows in chunks.
func dropSchema(ctx context.Context, pool *pgxpool.Pool, opts initDBOptions, target dropTarget, batch int, out io.Writer) error {
	var dropped publication.Record
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		var err error
		dropped, err = retireRecord(ctx, tx, opts, target, out)
		return err
	})
	if err != nil {
		return fmt.Errorf("drop schema: %w", err)
	}
	for _, table := range schemaResidue(opts) {
		n, err := purgeDroppedRows(ctx, pool, opts, dropped, table, batch)
		if err != nil {
			return fmt.Errorf("purge schema %d's rows from %s (deleted %d): %w; run drop-schema --schema-id %d again to resume",
				dropped.SchemaID, table.Table, n, err, dropped.SchemaID)
		}
		_, _ = fmt.Fprintf(out, "Purged %d row(s) of schema %d from %s\n", n, dropped.SchemaID, table.Table)
	}
	_, _ = fmt.Fprintf(out, "Schema %d is dropped at epoch %d and purged; init-db registers its id again at the next epoch\n",
		dropped.SchemaID, dropped.Epoch)
	return nil
}

// retireRecord is the drop transaction: under the schema lock it deletes
// the registry row and the ledger, and resets the record to DROPPED. A record
// that is DROPPED already is a drop to resume: the registry row and ledger
// are deleted again, and the record stays.
func retireRecord(ctx context.Context, tx pgx.Tx, opts initDBOptions, target dropTarget, out io.Writer) (publication.Record, error) {
	id, err := resolveDropID(ctx, tx, opts, target)
	if err != nil {
		return publication.Record{}, err
	}
	var locked bool
	if err := tx.QueryRow(ctx, trySchemaLockSQL, int32(id), int32(id)).Scan(&locked); err != nil {
		return publication.Record{}, fmt.Errorf("take schema %d's lock: %w", id, err)
	}
	if !locked {
		return publication.Record{}, fmt.Errorf("schema %d's lock is held by a cdc-flush or cdc-init: stop it and run drop-schema again", id)
	}
	tables := publication.TablesFor(opts.entityMain)
	snap, err := publication.Read(ctx, publication.PgxTx(tx), tables, id)
	if errors.Is(err, publication.ErrNoRecord) {
		return publication.Record{}, fmt.Errorf("schema %d has no publication record: run init-db to give it one, then drop it: %w", id, err)
	}
	if err != nil {
		return publication.Record{}, fmt.Errorf("read schema %d's record: %w", id, err)
	}
	if err := deleteRegistration(ctx, tx, opts, id); err != nil {
		return publication.Record{}, err
	}
	if snap.State == publication.Dropped {
		_, _ = fmt.Fprintf(out, "Schema %d is dropped already at epoch %d; resuming its purge\n", id, snap.Epoch)
		return snap.Record, nil
	}
	dropped, err := publication.Reset(ctx, publication.PgxTx(tx), tables, snap.Record)
	if err != nil {
		return publication.Record{}, fmt.Errorf("reset schema %d's record: %w", id, err)
	}
	_, _ = fmt.Fprintf(out, "Schema %d: record %s at epoch %d moved to %s at epoch %d\n", id, snap.State, snap.Epoch, dropped.State, dropped.Epoch)
	return dropped, nil
}

// resolveDropID answers the id target names.
func resolveDropID(ctx context.Context, tx pgx.Tx, opts initDBOptions, target dropTarget) (int16, error) {
	if target.name == "" {
		return int16(target.id), nil
	}
	registered, err := loadRegisteredSchemaIDs(ctx, tx, opts.schemaTable)
	if err != nil {
		return 0, fmt.Errorf("resolve schema %q: %w", target.name, err)
	}
	id, ok := registered[target.name]
	if !ok {
		return 0, fmt.Errorf("schema %q is not registered in %s; to finish a drop whose registry row is gone, give --schema-id", target.name, opts.schemaTable)
	}
	return id, nil
}

// deleteRegistration deletes schema id's registry row and its schema ledger.
func deleteRegistration(ctx context.Context, tx pgx.Tx, opts initDBOptions, id int16) error {
	tables := []string{
		sqlutil.SuffixedIdentifier(opts.schemaTable, schemaledger.AttributeTableSuffix),
		sqlutil.SuffixedIdentifier(opts.schemaTable, schemaledger.GenerationTableSuffix),
		opts.schemaTable,
	}
	for _, table := range tables {
		if _, err := tx.Exec(ctx, "DELETE FROM "+quoteIdentifier(table)+" WHERE schema_id = $1", id); err != nil {
			return fmt.Errorf("delete schema %d's rows from %s: %w", id, table, err)
		}
	}
	return nil
}

// purgeDroppedRows deletes the dropped schema's rows from table, batch rows
// per transaction. Each transaction first holds the record DROPPED at its
// epoch, so a purge never deletes a row once the id was registered again:
// Activate waits for the chunk, and its residue check refuses while rows
// remain.
func purgeDroppedRows(ctx context.Context, pool *pgxpool.Pool, opts initDBOptions, dropped publication.Record, table publication.Residue, batch int) (int64, error) {
	column := table.SchemaIDColumn
	if column == "" {
		column = "schema_id"
	}
	stmt := fmt.Sprintf("DELETE FROM %[1]s WHERE ctid IN (SELECT ctid FROM %[1]s WHERE %[2]s = $1 LIMIT $2)",
		quoteIdentifier(table.Table), quoteIdentifier(column))
	tables := publication.TablesFor(opts.entityMain)
	var total int64
	for {
		var n int64
		err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			if err := publication.HoldDropped(ctx, publication.PgxTx(tx), tables, dropped.SchemaID, dropped.Epoch); err != nil {
				return err
			}
			tag, err := tx.Exec(ctx, stmt, dropped.SchemaID, batch)
			n = tag.RowsAffected()
			return err
		})
		if err != nil {
			return total, err
		}
		total += n
		if n < int64(batch) {
			return total, nil
		}
	}
}
