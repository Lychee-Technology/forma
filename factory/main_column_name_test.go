package factory

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/schemameta"
)

// #470: an attribute named after an entity_main column makes Query and
// CrossSchemaSearch filter on the column (hybridMainColumn,
// hasMainTableCondition) and the federated outer ORDER BY sort on the outer
// select's column alias. Every one of those runs on the EntityManager this
// factory builds, and the factory loads its metadata through the real loader
// before anything else; a stored schema with such a name therefore fails
// startup, so no manager, and no query or keyset page, ever sees it.
func TestNewEntityManager_RefusesStoredSchemaWithEntityMainColumnName(t *testing.T) {
	for attr, column := range map[string]string{"text_01": "text_01", "Integer.02": "integer_02"} {
		t.Run(attr, func(t *testing.T) {
			dir := t.TempDir()
			data, err := json.Marshal(map[string]any{
				attr: map[string]any{"attributeID": 1, "valueType": "text"},
			})
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(dir, "lead_attributes.json"), data, 0o600))

			pool, err := pgxmock.NewPool()
			require.NoError(t, err)
			defer pool.Close()
			pool.ExpectQuery(`SELECT schema_name, schema_id FROM`).
				WillReturnRows(pgxmock.NewRows([]string{"schema_name", "schema_id"}).AddRow("lead", int16(7)))

			deps := buildUnitEntityManagerDeps(schemameta.NewMetadataCache())
			deps.newMetadataLoader = func(_ *pgxpool.Pool, schemaTable, schemaDir string) metadataLoader {
				return schemameta.NewMetadataLoader(pool, schemaTable, schemaDir)
			}
			config := newUnitEntityManagerConfig(t)
			config.Entity.SchemaDirectory = dir

			em, err := newEntityManagerWithConfigContext(context.Background(), config, nil, deps)
			require.Error(t, err)
			assert.Nil(t, em)
			assert.Contains(t, err.Error(), "failed to load metadata")
			assert.Contains(t, err.Error(), `attribute "`+attr+`"`)
			assert.Contains(t, err.Error(), `entity_main column "`+column+`"`)
			require.NoError(t, pool.ExpectationsWereMet())
		})
	}
}
