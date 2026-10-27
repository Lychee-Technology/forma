package schemameta_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma/internal/schemaledger"
	"github.com/lychee-technology/forma/internal/schemaledger/ledgertest"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/testdb"
)

const smallQty = `{"qty":{"attributeID":1,"valueType":"smallint","column_binding":{"col_name":"smallint_01"}}}`

// gateFixture registers schema orders (id 9) in a fresh registry table with
// the ledger and no generation, and writes its declaration to a directory.
func gateFixture(t *testing.T) (*pgxpool.Pool, string, string) {
	t.Helper()
	ctx := context.Background()
	pool := testdb.Connect(t, ctx)
	registry := fmt.Sprintf("gate_registry_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		for _, table := range []string{registry, registry + schemaledger.GenerationTableSuffix, registry + schemaledger.AttributeTableSuffix} {
			_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table)
		}
	})
	stmts := append([]string{
		fmt.Sprintf("CREATE TABLE %s (schema_name TEXT PRIMARY KEY, schema_id SMALLINT UNIQUE NOT NULL)", registry),
		fmt.Sprintf("INSERT INTO %s (schema_name, schema_id) VALUES ('orders', 9)", registry),
	}, schemaledger.Statements(registry)...)
	for _, stmt := range stmts {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("fixture statement %q: %v", stmt, err)
		}
	}
	return pool, registry, writeDeclaration(t, smallQty)
}

func writeDeclaration(t *testing.T, attrs string) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "orders.json"), []byte(`{"type":"object"}`), 0o644); err != nil {
		t.Fatalf("write schema: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "orders_attributes.json"), []byte(attrs), 0o644); err != nil {
		t.Fatalf("write attributes: %v", err)
	}
	return dir
}

func TestGatedLoadsRefuseASchemaWithNoBaseline(t *testing.T) {
	pool, registry, dir := gateFixture(t)
	ctx := context.Background()
	_, err := schemameta.NewFileSchemaRegistryContext(ctx, pool, registry, dir)
	if !errors.Is(err, schemaledger.ErrNotActive) || !strings.Contains(err.Error(), "baseline-schema") {
		t.Fatalf("NewFileSchemaRegistryContext = %v, want ErrNotActive naming baseline-schema", err)
	}
	_, err = schemameta.NewMetadataLoader(pool, registry, dir).LoadMetadata(ctx)
	if !errors.Is(err, schemaledger.ErrNotActive) {
		t.Fatalf("LoadMetadata = %v, want ErrNotActive", err)
	}
}

// TestGatedRegistryTracksTheGenerationItStartedOn: the registry admits the
// active declaration and notices a generation recorded after it started; a
// restart on the old declaration is refused, one on the new is admitted.
func TestGatedRegistryTracksTheGenerationItStartedOn(t *testing.T) {
	pool, registry, oldDir := gateFixture(t)
	ctx := context.Background()
	if err := ledgertest.Seed(ctx, pool, registry, oldDir); err != nil {
		t.Fatalf("Seed: %v", err)
	}
	running, err := schemameta.NewFileSchemaRegistryContext(ctx, pool, registry, oldDir)
	if err != nil {
		t.Fatalf("NewFileSchemaRegistryContext on the active declaration: %v", err)
	}
	if n, ok := running.(schemaledger.Tracker).LoadedGeneration(9); !ok || n != 1 {
		t.Fatalf("LoadedGeneration = %d, %t; want 1", n, ok)
	}
	if err := schemaledger.RequireUnchanged(ctx, running, 9); err != nil {
		t.Fatalf("RequireUnchanged before any apply: %v", err)
	}

	newDir := writeDeclaration(t, strings.Replace(smallQty, `"smallint"`, `"integer"`, 1))
	if err := ledgertest.Apply(ctx, pool, registry, newDir); err != nil {
		t.Fatalf("Apply smallint to integer: %v", err)
	}
	if err := schemaledger.RequireUnchanged(ctx, running, 9); !errors.Is(err, schemaledger.ErrGenerationChanged) {
		t.Fatalf("RequireUnchanged after an apply = %v, want ErrGenerationChanged", err)
	}
	_, err = schemameta.NewFileSchemaRegistryContext(ctx, pool, registry, oldDir)
	if !errors.Is(err, schemaledger.ErrNotActive) || !strings.Contains(err.Error(), "smallint bound to smallint_01") {
		t.Fatalf("a restart on the old declaration = %v, want ErrNotActive naming the declared type", err)
	}
	if _, err := schemameta.NewMetadataLoader(pool, registry, newDir).LoadMetadata(ctx); err != nil {
		t.Fatalf("LoadMetadata on the new declaration: %v", err)
	}
}

// TestControlPlaneLoadsSkipTheGate: the control-plane constructors load a
// declaration the ledger does not hold, and the registry is not a Tracker,
// so RequireUnchanged checks nothing.
func TestControlPlaneLoadsSkipTheGate(t *testing.T) {
	pool, registry, dir := gateFixture(t)
	ctx := context.Background()
	registryCP, err := schemameta.NewControlPlaneSchemaRegistryContext(ctx, pool, registry, dir)
	if err != nil {
		t.Fatalf("NewControlPlaneSchemaRegistryContext: %v", err)
	}
	if _, ok := registryCP.(schemaledger.Tracker); ok {
		t.Fatal("a control-plane registry must not be a Tracker")
	}
	if _, err := schemameta.NewMetadataLoader(pool, registry, dir).ControlPlane().LoadMetadata(ctx); err != nil {
		t.Fatalf("ControlPlane LoadMetadata: %v", err)
	}
}
