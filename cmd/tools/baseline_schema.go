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

// parquetAuditFn audits one schema's live parquet objects against its
// declaration's active attributes. It is nil for a deployment with no
// lakehouse tier.
type parquetAuditFn func(ctx context.Context, schemaID int16, cache forma.SchemaAttributeCache) ([]federated.DomainFinding, error)

// baselineOptions is what baseline-schema audits and seeds.
type baselineOptions struct {
	ledgerToolOptions
	tables widthaudit.Tables
	// auditParquet is nil when --no-parquet-tier says there is no lakehouse
	// tier to audit.
	auditParquet parquetAuditFn
}

// baselineSchemas seeds generation 1 of every selected schema that has none,
// after auditing its stored data against its current declaration (#733):
// PostgreSQL through the integer-width and numeric-domain censuses, the live
// parquet objects through federated.AuditParquetDomain. A schema with any
// finding is refused, and a refusal records nothing for any schema, as
// apply-schema does.
//
// The audit reads through the pool, and only the writes share a transaction:
// an audit can outlast Aurora DSQL's transaction duration limit, and holding
// a transaction open across it guards nothing. No write is held off by it,
// and a run that seeds a schema concurrently moves the pointer off NULL, so
// this run's compare-and-set fails and it records nothing.
func baselineSchemas(ctx context.Context, pool *pgxpool.Pool, opts baselineOptions, out io.Writer) error {
	targets, err := selectRegisteredSchemas(ctx, pool, opts.ledgerToolOptions)
	if err != nil {
		return err
	}
	var seeds []schemaledger.Declaration
	var refusals []string
	for _, target := range targets {
		decl, problems, err := auditForBaseline(ctx, pool, opts, target, out)
		if err != nil {
			return err
		}
		refusals = append(refusals, problems...)
		if decl != nil && len(problems) == 0 {
			seeds = append(seeds, *decl)
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
	return recordBaselines(ctx, pool, opts.schemaTable, seeds, out)
}

// recordBaselines records generation 1 of every seed in one transaction.
func recordBaselines(ctx context.Context, pool *pgxpool.Pool, schemaTable string, seeds []schemaledger.Declaration, out io.Writer) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin the baseline transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, decl := range seeds {
		rec := schemaledger.Record{SchemaID: decl.SchemaID, Hash: decl.Hash, Entries: decl.Entries, RecordedBy: "baseline-schema"}
		if err := schemaledger.Write(ctx, tx, schemaTable, rec); err != nil {
			return fmt.Errorf("record generation 1 of schema %s: %w", decl.SchemaName, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit the baseline: %w", err)
	}
	for _, decl := range seeds {
		fmt.Fprintf(out, "Recorded generation 1 of schema %s\n", decl.SchemaName)
	}
	return nil
}

// auditForBaseline audits one schema. It answers a nil declaration for a
// schema that already has an active generation: a baseline seeds, it never
// replaces (apply-schema records later edits).
func auditForBaseline(ctx context.Context, q widthaudit.Querier, opts baselineOptions, target registeredSchema, out io.Writer) (*schemaledger.Declaration, []string, error) {
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
	findings, err := censusPostgres(ctx, q, opts.tables, cache)
	if err != nil {
		return nil, nil, fmt.Errorf("census the stored values of schema %s: %w", target.name, err)
	}
	var problems []string
	for _, f := range findings {
		problems = append(problems, censusRefusal(f))
	}
	parquetProblems, err := auditParquetForBaseline(ctx, opts, target, cache)
	if err != nil {
		return nil, nil, err
	}
	problems = append(problems, parquetProblems...)
	if len(problems) == 0 {
		fmt.Fprintf(out, "Schema %s (id %d): stored data is within the declaration\n", target.name, target.id)
	}
	return &decl, problems, nil
}

// censusPostgres finds every PostgreSQL value outside its declared storage
// domain: an integer outside its width (widthaudit.Census), and a numeric
// that is not a finite float64 (widthaudit.NumericCensus).
func censusPostgres(ctx context.Context, q widthaudit.Querier, tables widthaudit.Tables, cache *schemameta.MetadataCache) ([]widthaudit.Finding, error) {
	findings, err := widthaudit.Census(ctx, q, tables, widthaudit.Targets(cache))
	if err != nil {
		return nil, err
	}
	numeric, err := widthaudit.NumericCensus(ctx, q, tables, widthaudit.NumericTargets(cache))
	if err != nil {
		return nil, err
	}
	return append(findings, numeric...), nil
}

func auditParquetForBaseline(ctx context.Context, opts baselineOptions, target registeredSchema, cache *schemameta.MetadataCache) ([]string, error) {
	if opts.auditParquet == nil {
		return nil, nil
	}
	active, _ := cache.GetSchemaCacheByID(target.id)
	findings, err := opts.auditParquet(ctx, target.id, active)
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

// censusRefusal names one stored value outside the declared width. It is a
// number, the only image the operator needs to find the row.
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
