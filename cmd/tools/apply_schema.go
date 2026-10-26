package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma/internal/bootstrap"
	"github.com/lychee-technology/forma/internal/schemaledger"
	"github.com/lychee-technology/forma/internal/schemameta"
)

// ledgerToolOptions is what apply-schema and baseline-schema act on.
type ledgerToolOptions struct {
	schemaTable string
	schemaDir   string
	// schema limits the run to one registered schema; empty means all.
	schema string
	dryRun bool
}

// registerLedgerFlags registers the flags apply-schema and baseline-schema
// share: init-db's database flags and environment defaults, so one
// environment serves all three, and the schema directory.
func registerLedgerFlags(flags *flag.FlagSet, opts *ledgerToolOptions) (*postgresFlags, error) {
	portDefault, portErr := bootstrap.EnvInt("DB_PORT", 5432)
	var pg postgresFlags
	pg.register(flags, postgresFlagOptions{
		hostFlag: "db-host", portFlag: "db-port", userFlag: "db-user",
		passwordFlag: "db-password", databaseFlag: "db-name", sslModeFlag: "db-ssl-mode",
		hostDefault:     bootstrap.Env("DB_HOST", "localhost"),
		portDefault:     portDefault,
		userDefault:     bootstrap.Env("DB_USER", "postgres"),
		passwordDefault: bootstrap.Env("DB_PASSWORD", "postgres"),
		databaseDefault: bootstrap.Env("DB_NAME", "forma"),
		sslModeDefault:  bootstrap.Env("DB_SSL_MODE", "disable"),
		hostUsage:       "database host", portUsage: "database port", userUsage: "database user",
		passwordUsage: "database password", databaseUsage: "database name", sslModeUsage: "database sslmode",
	})
	flags.StringVar(&opts.schemaTable, "schema-table", bootstrap.Env("SCHEMA_TABLE", "schema_registry"), "schema registry table name")
	flags.StringVar(&opts.schemaDir, "schema-dir", bootstrap.Env("SCHEMA_DIR", ""), "Directory containing the JSON schema files (required)")
	flags.StringVar(&opts.schema, "schema", "", "Registered schema to act on (default: every registered schema)")
	flags.BoolVar(&opts.dryRun, "dry-run", false, "Report what would be recorded, and record nothing")
	return &pg, portErr
}

// runApplySchema records each registered schema's edited declaration as a
// new generation, when the D2 type rules allow every change in it (#733).
func runApplySchema(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("apply-schema", flag.ContinueOnError)
	flags.SetOutput(os.Stdout)
	flags.Usage = func() {
		fmt.Println("Usage: forma-tools apply-schema --schema-dir <dir> [--schema <name>] [--dry-run] [options]")
		fmt.Println("")
		fmt.Println("Judges each registered schema's declaration in --schema-dir against its active generation")
		fmt.Println("and records it as the next generation when every change is allowed. A refused change")
		fmt.Println("records nothing for any schema. Run it before rolling the data plane onto the new files.")
		fmt.Println("")
		fmt.Println("Options:")
		flags.PrintDefaults()
	}
	var opts ledgerToolOptions
	pg, portErr := registerLedgerFlags(flags, &opts)
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return fmt.Errorf("parse flags: %w", err)
	}
	if portErr != nil {
		return fmt.Errorf("resolve --db-port default: %w", portErr)
	}
	if opts.schemaDir == "" {
		return fmt.Errorf("--schema-dir is required")
	}
	pool, err := buildToolPostgresPool(ctx, pg.databaseConfig("DB_PASSWORD", toolPostgresPoolSettings{
		maxConnections: 2,
		timeout:        30 * time.Second,
	}))
	if err != nil {
		return fmt.Errorf("create connection pool: %w", err)
	}
	defer pool.Close()
	return applySchemas(ctx, pool, opts, os.Stdout)
}

// applySchemas judges every selected schema, then records all of them or
// none, in one transaction whose compare-and-set refuses the whole apply if
// another control-plane run moved a pointer meanwhile.
func applySchemas(ctx context.Context, pool *pgxpool.Pool, opts ledgerToolOptions, out io.Writer) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin the apply transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	plans, refusals, err := planSchemas(ctx, tx, opts)
	if err != nil {
		return err
	}
	printApplyPlans(out, plans)
	if len(refusals) > 0 {
		fmt.Fprintln(out, "Refused:")
		for _, r := range refusals {
			fmt.Fprintf(out, "  %s\n", r)
		}
		return fmt.Errorf("refused %d change(s); no generation was recorded", len(refusals))
	}
	if opts.dryRun {
		fmt.Fprintln(out, "Dry run: no generation was recorded.")
		return nil
	}
	recorded, err := recordPlans(ctx, tx, opts.schemaTable, plans)
	if err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit the apply: %w", err)
	}
	for _, line := range recorded {
		fmt.Fprintln(out, line)
	}
	return nil
}

func planSchemas(ctx context.Context, tx pgx.Tx, opts ledgerToolOptions) ([]schemaledger.Plan, []string, error) {
	targets, err := selectRegisteredSchemas(ctx, tx, opts)
	if err != nil {
		return nil, nil, err
	}
	var plans []schemaledger.Plan
	var refusals []string
	for _, target := range targets {
		decl, _, err := schemameta.ReadDeclaration(opts.schemaDir, target.name, target.id)
		if err != nil {
			return nil, nil, fmt.Errorf("read the declaration of schema %s: %w", target.name, err)
		}
		active, ok, err := schemaledger.ReadActive(ctx, tx, opts.schemaTable, target.id)
		if err != nil {
			return nil, nil, fmt.Errorf("read the active generation of schema %s: %w", target.name, err)
		}
		if !ok {
			refusals = append(refusals, fmt.Sprintf("schema %s (id %d) has no active generation; run baseline-schema first", target.name, target.id))
			continue
		}
		plan := schemaledger.PlanApply(active, decl)
		plans = append(plans, plan)
		refusals = append(refusals, plan.Refusals()...)
	}
	return plans, refusals, nil
}

type registeredSchema struct {
	name string
	id   int16
}

// selectRegisteredSchemas answers the registered schemas a run acts on, by
// name: every one, or opts.schema alone.
func selectRegisteredSchemas(ctx context.Context, tx pgx.Tx, opts ledgerToolOptions) ([]registeredSchema, error) {
	registered, err := loadRegisteredSchemaIDs(ctx, tx, opts.schemaTable)
	if err != nil {
		return nil, err
	}
	if opts.schema != "" {
		id, ok := registered[opts.schema]
		if !ok {
			return nil, fmt.Errorf("schema %s is not registered in %s; register it with init-db", opts.schema, opts.schemaTable)
		}
		return []registeredSchema{{name: opts.schema, id: id}}, nil
	}
	if len(registered) == 0 {
		return nil, fmt.Errorf("no schemas are registered in %s", opts.schemaTable)
	}
	targets := make([]registeredSchema, 0, len(registered))
	for name, id := range registered {
		targets = append(targets, registeredSchema{name: name, id: id})
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].name < targets[j].name })
	return targets, nil
}

func printApplyPlans(out io.Writer, plans []schemaledger.Plan) {
	for _, plan := range plans {
		if plan.Unchanged() {
			fmt.Fprintf(out, "Schema %s (id %d): generation %d is the declaration; nothing to apply\n", plan.SchemaName, plan.SchemaID, plan.From)
			continue
		}
		fmt.Fprintf(out, "Schema %s (id %d): generation %d to %d\n", plan.SchemaName, plan.SchemaID, plan.From, plan.From+1)
		if len(plan.Changes) == 0 {
			fmt.Fprintln(out, "  no attribute changes")
		}
		for _, line := range plan.Describe() {
			fmt.Fprintf(out, "  %s\n", line)
		}
		for _, w := range plan.Warnings() {
			fmt.Fprintf(out, "  WARNING: %s\n", w)
		}
	}
}

func recordPlans(ctx context.Context, tx pgx.Tx, schemaTable string, plans []schemaledger.Plan) ([]string, error) {
	var recorded []string
	for _, plan := range plans {
		if plan.Unchanged() {
			continue
		}
		rec := schemaledger.Record{SchemaID: plan.SchemaID, From: plan.From, Hash: plan.Hash, Entries: plan.Entries,
			RecordedBy: "apply-schema", Warnings: plan.Warnings()}
		if err := schemaledger.Write(ctx, tx, schemaTable, rec); err != nil {
			if errors.Is(err, schemaledger.ErrGenerationConflict) {
				return nil, fmt.Errorf("schema %s: another control-plane run recorded a generation meanwhile; "+
					"no generation was recorded, run apply-schema again: %w", plan.SchemaName, err)
			}
			return nil, fmt.Errorf("record generation %d of schema %s: %w", rec.Number(), plan.SchemaName, err)
		}
		recorded = append(recorded, fmt.Sprintf("Recorded generation %d of schema %s", rec.Number(), plan.SchemaName))
	}
	return recorded, nil
}
