//go:build e2e

package federated

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/schemameta"
)

// TestUseRegistryBuildsDeclaredIndexes pins that a federated harness database
// carries the indexes its registry's schemas declare (#647): UseRegistry
// builds them through the function init-db calls before attaching the
// registry.
func TestUseRegistryBuildsDeclaredIndexes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	h, err := NewFederatedTestHarness(ctx)
	if err != nil {
		t.Fatalf("create harness: %v", err)
	}
	defer h.CleanupOrLog(ctx, t)

	dir := t.TempDir()
	files := map[string]string{
		"queue.json": `{"type": "object",
			"properties": {"status": {"type": "string"}, "due_at": {"type": "string", "format": "date-time"}},
			"x-forma-indexes": [{"name": "due", "attributes": ["status", "due_at"]}]}`,
		"queue_attributes.json": `{
			"status": {"attributeID": 1, "valueType": "text", "column_binding": {"col_name": "text_01"}},
			"due_at": {"attributeID": 2, "valueType": "datetime", "column_binding": {"col_name": "bigint_01"}}}`,
	}
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	registry, err := schemameta.NewFileSchemaRegistryFromDirectory(dir)
	if err != nil {
		t.Fatalf("load registry: %v", err)
	}

	if err := h.UseRegistry(ctx, registry); err != nil {
		t.Fatalf("UseRegistry: %v", err)
	}
	if h.Registry != registry {
		t.Fatal("UseRegistry did not attach the registry")
	}
	var def string
	var valid bool
	err = h.PGDB.QueryRowContext(ctx, `
		SELECT p.indexdef, i.indisvalid
		FROM pg_indexes p
		JOIN pg_class c ON c.relname = p.indexname AND c.relnamespace = to_regnamespace(p.schemaname)
		JOIN pg_index i ON i.indexrelid = c.oid
		WHERE p.schemaname = 'public' AND p.tablename = 'entity_main' AND p.indexname LIKE 'entity\_main\_cx\_%'`).Scan(&def, &valid)
	if err != nil {
		t.Fatalf("read the declared index: %v", err)
	}
	if !valid || !strings.HasSuffix(def, "(ltbase_schema_id, text_01, bigint_01, ltbase_row_id)") {
		t.Fatalf("declared index is %q (valid %v), want a valid index keyed (ltbase_schema_id, text_01, bigint_01, ltbase_row_id)", def, valid)
	}
}
