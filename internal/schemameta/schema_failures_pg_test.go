package schemameta_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/testdb"
)

// The registry-table path reports every failing schema the table lists, in
// name order, not the first (#604). alpha and gamma each fail validation;
// beta is valid and must not keep the registry from refusing.
func TestRegistryTableLoadReportsEveryFailingSchema(t *testing.T) {
	ctx := context.Background()
	pool := testdb.Connect(t, ctx)
	registry := fmt.Sprintf("agg_registry_%d", time.Now().UnixNano())
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+registry) })
	for _, stmt := range []string{
		fmt.Sprintf("CREATE TABLE %s (schema_name TEXT PRIMARY KEY, schema_id SMALLINT UNIQUE NOT NULL)", registry),
		fmt.Sprintf("INSERT INTO %s (schema_name, schema_id) VALUES ('gamma', 3), ('beta', 2), ('alpha', 1)", registry),
	} {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("fixture statement %q: %v", stmt, err)
		}
	}
	dir := t.TempDir()
	for name, attrs := range map[string]string{
		"alpha": `{"a":{"attributeID":7,"valueType":"text"},"b":{"attributeID":7,"valueType":"text"}}`,
		"beta":  `{"status":{"attributeID":1,"valueType":"text"}}`,
		"gamma": `not json`,
	} {
		if err := os.WriteFile(filepath.Join(dir, name+"_attributes.json"), []byte(attrs), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}

	got, err := schemameta.NewControlPlaneSchemaRegistryContext(ctx, pool, registry, dir)
	if err == nil || got != nil {
		t.Fatalf("NewControlPlaneSchemaRegistryContext = %v, %v; want refusal", got, err)
	}
	msg := err.Error()
	alpha := strings.Index(msg, "schema alpha listed in registry table")
	gamma := strings.Index(msg, "failed to load schema gamma listed in registry table")
	if !strings.Contains(msg, "2 schemas failed to load") || alpha == -1 || gamma == -1 || alpha > gamma || strings.Contains(msg, "beta") {
		t.Fatalf("error = %q, want alpha's then gamma's diagnosis under a count, and nothing naming beta", msg)
	}
}
