package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/rfc3339migrate"
	"github.com/lychee-technology/forma/internal/schemaledger"
)

// runMigrateRFC3339 rewrites the whole-second images of the retired iso8601
// encoding into the rfc3339 shape (#842). It is the first step of the
// rename: migrate with writers stopped, declare rfc3339 and run apply-schema,
// rebuild the unique keys that include such an attribute, start the new
// release (docs/rfc3339-storage-contract.md).
func runMigrateRFC3339(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("migrate-rfc3339", flag.ContinueOnError)
	flags.SetOutput(os.Stdout)
	flags.Usage = func() {
		fmt.Println("Usage: forma-tools migrate-rfc3339 [--schema <name>] [--dry-run] [options]")
		fmt.Println("")
		fmt.Println("Rewrites every whole-second image (YYYY-MM-DDThh:mm:ssZ) of each attribute an active generation")
		fmt.Println("records with the iso8601 encoding to the rfc3339 image of the same instant (YYYY-MM-DDThh:mm:ss.000Z).")
		fmt.Println("Any other image in those columns is listed and left unchanged. --dry-run counts and changes nothing.")
		fmt.Println("It exits non-zero while a whole-second image remains, so a scripted cutover stops before apply-schema.")
		fmt.Println("")
		fmt.Println("Stop every writer of the schema first, and start only the new release afterwards: the tool cannot")
		fmt.Println("detect a running writer, and an old-release write after the rewrite stores a whole-second image again.")
		fmt.Println("")
		fmt.Println("Options:")
		flags.PrintDefaults()
	}
	var opts ledgerToolOptions
	pg, portErr := registerLedgerDBFlags(flags, &opts)
	registerEntityMainFlag(flags, &opts, "Entity main table to rewrite")
	batch := flags.Int("batch-size", rfc3339migrate.BatchSize, "Rows each rewrite statement (one transaction) changes; at most 2999, under Aurora DSQL's row limit")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return fmt.Errorf("parse flags: %w", err)
	}
	if portErr != nil {
		return fmt.Errorf("resolve --db-port default: %w", portErr)
	}
	if opts.entityMain == "" {
		return errors.New("--entity-main-table is required")
	}
	pool, err := buildToolPostgresPool(ctx, pg.databaseConfig("DB_PASSWORD", toolPostgresPoolSettings{
		maxConnections: 2,
		timeout:        30 * time.Second,
	}))
	if err != nil {
		return fmt.Errorf("create connection pool: %w", err)
	}
	defer pool.Close()
	return migrateRFC3339(ctx, pool, opts, *batch, os.Stdout)
}

// migrateRFC3339 migrates every target of the selected schemas and reports
// per column. It answers an error while any whole-second image remains: on a
// dry run whenever there is one to rewrite.
func migrateRFC3339(ctx context.Context, pool *pgxpool.Pool, opts ledgerToolOptions, batch int, out io.Writer) error {
	targets, err := rfc3339MigrationTargets(ctx, pool, opts, out)
	if err != nil {
		return err
	}
	if len(targets) == 0 {
		fmt.Fprintln(out, "No active generation records the iso8601 encoding; nothing to migrate.")
		return nil
	}
	var remaining int64
	var columns int
	for _, t := range targets {
		left, err := migrateRFC3339Target(ctx, pool, opts, t, batch, out)
		if err != nil {
			return err
		}
		if left > 0 {
			remaining += left
			columns++
		}
	}
	if remaining > 0 {
		if opts.dryRun {
			return fmt.Errorf("dry run: %d whole-second image(s) in %d column(s) are left to rewrite; nothing was changed", remaining, columns)
		}
		return fmt.Errorf("%d whole-second image(s) remain in %d column(s); a writer may still be running: stop it and run migrate-rfc3339 again",
			remaining, columns)
	}
	fmt.Fprintln(out, "No whole-second image remains: declare encoding rfc3339 for these attributes and run apply-schema.")
	return nil
}

// migrateRFC3339Target lists the images the migration leaves, rewrites the
// whole-second ones unless this is a dry run, and answers how many remain.
func migrateRFC3339Target(ctx context.Context, pool *pgxpool.Pool, opts ledgerToolOptions, t rfc3339migrate.Target, batch int, out io.Writer) (int64, error) {
	others, err := rfc3339migrate.Others(ctx, pool, opts.entityMain, t)
	if err != nil {
		return 0, err
	}
	count, err := rfc3339migrate.CountLegacy(ctx, pool, opts.entityMain, t)
	if err != nil {
		return 0, err
	}
	if opts.dryRun {
		fmt.Fprintf(out, "%s: %d whole-second image(s) to rewrite, %d other image(s) left unchanged\n", t, count, len(others))
	} else {
		rewritten, err := rfc3339migrate.Rewrite(ctx, pool, opts.entityMain, t, batch)
		if err != nil {
			return 0, err
		}
		if count, err = rfc3339migrate.CountLegacy(ctx, pool, opts.entityMain, t); err != nil {
			return 0, err
		}
		fmt.Fprintf(out, "%s: rewrote %d whole-second image(s), %d remain, %d other image(s) left unchanged\n", t, rewritten, count, len(others))
	}
	for _, o := range others {
		fmt.Fprintf(out, "  left unchanged: row %s holds %q\n", o.RowID, o.Image)
	}
	return count, nil
}

// rfc3339MigrationTargets reads the targets from each selected schema's
// active generation. A schema with no active generation is reported and
// skipped: the data-plane gate refuses it before any read of its images.
func rfc3339MigrationTargets(ctx context.Context, pool *pgxpool.Pool, opts ledgerToolOptions, out io.Writer) ([]rfc3339migrate.Target, error) {
	schemas, err := selectRegisteredSchemas(ctx, pool, opts)
	if err != nil {
		return nil, err
	}
	var targets []rfc3339migrate.Target
	for _, s := range schemas {
		active, ok, err := schemaledger.ReadActive(ctx, pool, opts.schemaTable, s.id)
		if err != nil {
			return nil, fmt.Errorf("read the active generation of schema %s: %w", s.name, err)
		}
		if !ok {
			fmt.Fprintf(out, "Schema %s (id %d) has no active generation; skipped (run baseline-schema first)\n", s.name, s.id)
			continue
		}
		targets = append(targets, rfc3339migrate.TargetsOf(s.name, active)...)
	}
	return targets, nil
}

// unmigratedRFC3339Changes refuses each iso8601 → rfc3339 change of plan
// whose column still holds a whole-second image (#842). It counts through
// the migration's own target and query, so apply-schema refuses exactly
// while migrate-rfc3339 has work left. Without it an operator who renames
// the encoding without migrating would start a server whose range
// comparisons and sorts misorder the old rows, with no error.
func unmigratedRFC3339Changes(ctx context.Context, q rfc3339migrate.Querier, entityMain, schemaName string,
	active schemaledger.Generation, plan schemaledger.Plan) ([]string, error) {
	renamed := make(map[int16]bool)
	for _, c := range plan.Changes {
		if c.Verdict != schemaledger.Refused && c.Old.Encoding == forma.MainColumnEncodingLegacyISO8601 &&
			c.New.Encoding == forma.MainColumnEncodingRFC3339 {
			renamed[c.New.AttrID] = true
		}
	}
	if len(renamed) == 0 {
		return nil, nil
	}
	if entityMain == "" {
		return nil, fmt.Errorf("schema %s changes iso8601 to rfc3339, which apply-schema judges against the stored images: --entity-main-table is required", schemaName)
	}
	var refusals []string
	for _, t := range rfc3339migrate.TargetsOf(schemaName, active) {
		if !renamed[t.AttrID] {
			continue
		}
		n, err := rfc3339migrate.CountLegacy(ctx, q, entityMain, t)
		if err != nil {
			return nil, err
		}
		if n > 0 {
			refusals = append(refusals, fmt.Sprintf("%s: iso8601 to rfc3339 is refused while %d row(s) of %s still hold a whole-second iso8601 image; "+
				"stop every writer of the schema, run forma-tools migrate-rfc3339, then apply again", t, n, entityMain))
		}
	}
	return refusals, nil
}
