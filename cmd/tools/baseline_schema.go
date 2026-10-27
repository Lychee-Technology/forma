package main

import (
	"context"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/federated"
	"github.com/lychee-technology/forma/internal/schemaledger"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/widthaudit"
)

// parquetTier is the lakehouse tier a baseline audits. objects resolves a
// schema's live objects, the set the federated read scans, and audit audits
// those paths against the declaration's active attributes. The baseline
// audits the set it resolved before the audit and resolves it again when it
// records, so the objects it certifies are the ones it audited.
type parquetTier struct {
	objects func(ctx context.Context, schemaID int16) ([]string, error)
	audit   func(ctx context.Context, schemaID int16, paths []string, cache forma.SchemaAttributeCache) ([]federated.DomainFinding, error)
}

// baselineOptions is what baseline-schema audits and seeds.
type baselineOptions struct {
	ledgerToolOptions
	tables widthaudit.Tables
	// parquet is nil when --no-parquet-tier says there is no lakehouse tier
	// to audit.
	parquet *parquetTier
}

// baselineSeed is a schema whose audit found nothing, with the stored state
// that audit read.
type baselineSeed struct {
	decl  schemaledger.Declaration
	state storedState
}

// baselineSchemas seeds generation 1 of every selected schema that has none,
// after auditing its stored data against its current declaration (#733):
// PostgreSQL through the integer-width, numeric-domain and storage-domain
// censuses, the live parquet objects through federated.AuditParquetDomain. A schema with any
// finding is refused, and a refusal records nothing for any schema, as
// apply-schema does.
//
// The audit reads through the pool, and only the writes share a transaction:
// an audit can outlast Aurora DSQL's transaction duration limit, and holding
// a transaction open across it guards nothing. No write is held off by it.
// A run that seeds a schema concurrently moves the pointer off NULL, so this
// run's compare-and-set fails and it records nothing. A write to a schema's
// stored data during its audit moves its storedState, and the transaction
// that records refuses it.
func baselineSchemas(ctx context.Context, pool *pgxpool.Pool, opts baselineOptions, out io.Writer) error {
	targets, err := selectRegisteredSchemas(ctx, pool, opts.ledgerToolOptions)
	if err != nil {
		return err
	}
	var seeds []baselineSeed
	var refusals []string
	for _, target := range targets {
		seed, problems, err := auditForBaseline(ctx, pool, opts, target, out)
		if err != nil {
			return err
		}
		refusals = append(refusals, problems...)
		if seed != nil && len(problems) == 0 {
			seeds = append(seeds, *seed)
		}
	}
	if len(refusals) > 0 {
		fmt.Fprintln(out, "Refused: the stored data contradicts the declaration, or could not be read:")
		for _, r := range refusals {
			fmt.Fprintf(out, "  %s\n", r)
		}
		return fmt.Errorf("refused %d finding(s); no generation was recorded. Correct the stored data or the declaration "+
			"(converting stored values is a migration, #730), then run baseline-schema again", len(refusals))
	}
	if opts.dryRun {
		fmt.Fprintln(out, "Dry run: no generation was recorded.")
		return nil
	}
	return recordBaselines(ctx, pool, opts, seeds, out)
}

// recordBaselines records generation 1 of every seed in one transaction,
// once the stored state of each reads as its audit read it.
func recordBaselines(ctx context.Context, pool *pgxpool.Pool, opts baselineOptions, seeds []baselineSeed, out io.Writer) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin the baseline transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	moved, err := movedDuringAudit(ctx, tx, opts, seeds)
	if err != nil {
		return err
	}
	if len(moved) > 0 {
		fmt.Fprintln(out, "Refused: stored data changed while it was audited, so the audit no longer describes it:")
		for _, m := range moved {
			fmt.Fprintf(out, "  %s\n", m)
		}
		return fmt.Errorf("refused %d change(s) made during the audit; no generation was recorded. A process of the old release "+
			"still writes or exports: stop every server, Lambda function, embedder, cdc-flush and cdc-init run and pause the "+
			"compactor (docs/schema-evolution.md, \"Upgrading an existing deployment\"), then run baseline-schema again", len(moved))
	}
	for _, seed := range seeds {
		decl := seed.decl
		rec := schemaledger.Record{SchemaID: decl.SchemaID, Hash: decl.Hash, Entries: decl.Entries, RecordedBy: "baseline-schema"}
		if err := schemaledger.Write(ctx, tx, opts.schemaTable, rec); err != nil {
			return fmt.Errorf("record generation 1 of schema %s: %w", decl.SchemaName, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit the baseline: %w", err)
	}
	for _, seed := range seeds {
		fmt.Fprintf(out, "Recorded generation 1 of schema %s\n", seed.decl.SchemaName)
	}
	return nil
}

// auditForBaseline audits one schema. It answers a nil seed for a schema
// that already has an active generation: a baseline seeds, it never
// replaces (apply-schema records later edits).
func auditForBaseline(ctx context.Context, q widthaudit.Querier, opts baselineOptions, target registeredSchema, out io.Writer) (*baselineSeed, []string, error) {
	active, ok, err := schemaledger.ReadPointer(ctx, q, opts.schemaTable, target.id)
	if err != nil {
		return nil, nil, fmt.Errorf("read the active generation of schema %s: %w", target.name, err)
	}
	if ok {
		fmt.Fprintf(out, "Schema %s (id %d): generation %d is active; nothing to baseline (use apply-schema for edits)\n", target.name, target.id, active)
		return nil, nil, nil
	}
	decl, full, err := schemameta.ReadDeclaration(opts.schemaDir, target.name, target.id)
	if err != nil {
		return nil, nil, fmt.Errorf("read the declaration of schema %s: %w", target.name, err)
	}
	cache := schemameta.NewMetadataCache()
	if err := cache.RegisterSchema(target.name, target.id, full); err != nil {
		return nil, nil, fmt.Errorf("load the declaration of schema %s: %w", target.name, err)
	}
	state, err := readStoredState(ctx, q, opts, target.id)
	if err != nil {
		return nil, nil, fmt.Errorf("read the stored state of schema %s: %w", target.name, err)
	}
	findings, err := censusPostgres(ctx, q, opts.tables, cache)
	if err != nil {
		return nil, nil, fmt.Errorf("census the stored values of schema %s: %w", target.name, err)
	}
	var problems []string
	for _, f := range findings {
		problems = append(problems, censusRefusal(f))
	}
	parquetProblems, err := auditParquetForBaseline(ctx, opts, target, state.objects, cache)
	if err != nil {
		return nil, nil, err
	}
	problems = append(problems, parquetProblems...)
	if len(problems) == 0 {
		fmt.Fprintf(out, "Schema %s (id %d): stored data is within the declaration\n", target.name, target.id)
	}
	return &baselineSeed{decl: decl, state: state}, problems, nil
}

// censusPostgres finds every PostgreSQL value outside its declared storage
// domain: an integer outside its width (widthaudit.Census), a numeric that
// is not a finite float64 (widthaudit.NumericCensus), and any other value
// the read refuses, a value in the wrong eav_data column included
// (widthaudit.DomainCensus). Each also holds the eav_data rows a bound
// attribute kept from before its promotion, which the read still decodes.
func censusPostgres(ctx context.Context, q widthaudit.Querier, tables widthaudit.Tables, cache *schemameta.MetadataCache) ([]widthaudit.Finding, error) {
	findings, err := widthaudit.Census(ctx, q, tables, widthaudit.TargetsWithRetainedEAV(cache))
	if err != nil {
		return nil, err
	}
	numeric, err := widthaudit.NumericCensus(ctx, q, tables, widthaudit.NumericTargets(cache))
	if err != nil {
		return nil, err
	}
	domain, err := widthaudit.DomainCensus(ctx, q, tables, widthaudit.DomainTargets(cache))
	if err != nil {
		return nil, err
	}
	return append(append(findings, numeric...), domain...), nil
}

func auditParquetForBaseline(ctx context.Context, opts baselineOptions, target registeredSchema, paths []string, cache *schemameta.MetadataCache) ([]string, error) {
	if opts.parquet == nil {
		return nil, nil
	}
	active, _ := cache.GetSchemaCacheByID(target.id)
	findings, err := opts.parquet.audit(ctx, target.id, paths, active)
	if err != nil {
		return nil, fmt.Errorf("audit the parquet objects of schema %s: %w", target.name, err)
	}
	problems := make([]string, 0, len(findings))
	for _, f := range findings {
		if f.Attribute == "" {
			problems = append(problems, fmt.Sprintf("schema %s (id %d): object %s: %s", target.name, target.id, f.Path, f.Evidence))
			continue
		}
		problems = append(problems, fmt.Sprintf("schema %s (id %d) attribute %s declared %s: object %s (%s): %s",
			target.name, target.id, f.Attribute, f.Declared, f.Path, f.FileType, f.Evidence))
	}
	return problems, nil
}

// censusRefusal names one stored value outside the declared type: a number,
// or quoted text when it was read from a text column, which is the image the
// operator needs to find the row.
func censusRefusal(f widthaudit.Finding) string {
	where := "eav_data"
	if f.Column != "" {
		where = "column " + f.Column
	}
	if f.ArrayIndices != "" {
		where += " element " + f.ArrayIndices
	}
	return fmt.Sprintf("schema %s (id %d) attribute %s (id %d) declared %s: row %s holds %s in %s, outside the declared type",
		f.SchemaName, f.SchemaID, f.AttrName, f.AttrID, f.Declared, f.RowID, f.StoredValue, where)
}
