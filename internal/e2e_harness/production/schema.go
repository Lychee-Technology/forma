package production

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// Fixture schema IDs live in 20-40: sqlgen.IsBenchmarkSchemaID reserves
// 100-102 for hardcoded benchmark parquet projections, and the production
// harness must never collide with that range.
const (
	SchemaIDSimple int16 = 20
	SchemaIDWide   int16 = 21
	SchemaIDSecond int16 = 22
)

// SchemaRef names a fixture schema and its fixed ID.
type SchemaRef struct {
	ID   int16
	Name string
}

// DefaultSchemaFixtures returns the bundled fixture schemas:
//
//   - e2e_simple: two attributes (one column-bound text, one EAV numeric)
//   - e2e_wide:   one attribute per scalar forma.ValueType, mixing
//     main-column bindings and EAV-only storage (for #174)
//   - e2e_second: a second schema for multi-schema isolation (for #186)
func DefaultSchemaFixtures() []SchemaRef {
	return []SchemaRef{
		{ID: SchemaIDSimple, Name: "e2e_simple"},
		{ID: SchemaIDWide, Name: "e2e_wide"},
		{ID: SchemaIDSecond, Name: "e2e_second"},
	}
}

// FixtureSchemasDir returns the bundled schemas/ fixture directory.
func FixtureSchemasDir() string {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return ""
	}
	return filepath.Join(filepath.Dir(file), "schemas")
}

// ValidateFixtureSchemaID rejects schema IDs that collide with the
// engine-reserved benchmark range (100-102), which would silently swap in
// benchmark parquet projections instead of production ones.
func ValidateFixtureSchemaID(id int16) error {
	if id <= 0 {
		return fmt.Errorf("fixture schema id must be positive, got %d", id)
	}
	if sqlgen.IsBenchmarkSchemaID(id) {
		return fmt.Errorf("fixture schema id %d collides with the benchmark-reserved range (100-102)", id)
	}
	return nil
}

// RegisterSchemas inserts fixture (id, name) rows into the per-test
// schema_registry table, guarding against reserved IDs.
func RegisterSchemas(ctx context.Context, pool *pgxpool.Pool, refs ...SchemaRef) error {
	for _, ref := range refs {
		if err := ValidateFixtureSchemaID(ref.ID); err != nil {
			return fmt.Errorf("register schema %s: %w", ref.Name, err)
		}
		if _, err := pool.Exec(ctx,
			`INSERT INTO schema_registry (schema_id, schema_name) VALUES ($1, $2)
			 ON CONFLICT (schema_id) DO UPDATE SET schema_name = EXCLUDED.schema_name`,
			ref.ID, ref.Name,
		); err != nil {
			return fmt.Errorf("register schema %s (%d): %w", ref.Name, ref.ID, err)
		}
	}
	return nil
}
