// Package ledgertest records schema generations for test fixtures and
// harnesses that create their tables by hand rather than through init-db
// (#733). It skips baseline-schema's audit of stored data, so production code
// must never import it; a guard test in schemameta enforces that.
package ledgertest

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma/internal/schemaledger"
	"github.com/lychee-technology/forma/internal/schemameta"
)

// Seed adds the ledger to registry, an existing schema registry table, and
// records generation 1 of every registered schema that has none, from its
// declaration in schemaDir. A schema without an attributes file there is
// left without one, as init-db leaves it.
func Seed(ctx context.Context, pool *pgxpool.Pool, registry, schemaDir string) error {
	for _, stmt := range schemaledger.Statements(registry) {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("create the generation ledger of %s: %w", registry, err)
		}
	}
	return record(ctx, pool, registry, schemaDir, true, false)
}

// Apply records each registered schema's declaration in schemaDir as its
// next generation, as apply-schema does: a change the D2 rules refuse fails
// and records nothing. A schema with no generation gets generation 1.
func Apply(ctx context.Context, pool *pgxpool.Pool, registry, schemaDir string) error {
	return record(ctx, pool, registry, schemaDir, false, false)
}

// Force is Apply without the D2 rules. It models a deployment whose
// declaration changed before the gate existed, which is how the Phase 1
// guard's tests (#732) reach a numeric to bigint redeclaration.
func Force(ctx context.Context, pool *pgxpool.Pool, registry, schemaDir string) error {
	return record(ctx, pool, registry, schemaDir, false, true)
}

func record(ctx context.Context, pool *pgxpool.Pool, registry, schemaDir string, seedOnly, force bool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, fmt.Sprintf("SELECT schema_name, schema_id FROM %s ORDER BY schema_id", schemaledger.TablesFor(registry).Registry))
	if err != nil {
		return fmt.Errorf("read the schemas registered in %s: %w", registry, err)
	}
	type schema struct {
		name string
		id   int16
	}
	schemas, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (schema, error) {
		var s schema
		return s, row.Scan(&s.name, &s.id)
	})
	if err != nil {
		return fmt.Errorf("scan the schemas registered in %s: %w", registry, err)
	}
	for _, s := range schemas {
		if err := recordOne(ctx, tx, registry, schemaDir, s.name, s.id, seedOnly, force); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func recordOne(ctx context.Context, tx pgx.Tx, registry, schemaDir, name string, id int16, seedOnly, force bool) error {
	decl, _, err := schemameta.ReadDeclaration(schemaDir, name, id)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read the declaration of schema %s: %w", name, err)
	}
	active, ok, err := schemaledger.ReadActive(ctx, tx, registry, id)
	if err != nil {
		return fmt.Errorf("read the active generation of schema %s: %w", name, err)
	}
	rec := schemaledger.Record{SchemaID: id, Hash: decl.Hash, Entries: decl.Entries, RecordedBy: "ledgertest"}
	if ok {
		if seedOnly {
			return nil
		}
		plan := schemaledger.PlanApply(active, decl)
		if refusals := plan.Refusals(); len(refusals) > 0 && !force {
			return fmt.Errorf("apply refused: %s", strings.Join(refusals, "; "))
		}
		if plan.Unchanged() {
			return nil
		}
		rec = schemaledger.Record{SchemaID: id, From: plan.From, Hash: plan.Hash, Entries: plan.Entries,
			RecordedBy: "ledgertest", Warnings: plan.Warnings()}
	}
	if err := schemaledger.Write(ctx, tx, registry, rec); err != nil {
		return fmt.Errorf("record schema %s: %w", name, err)
	}
	return nil
}
