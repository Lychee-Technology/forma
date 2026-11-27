package internal

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/transform"

	"github.com/stretchr/testify/require"
)

// TestQueryHandsTheEngineAnUnderivedSchemasReason pins #937 on the
// federated route: a schema whose array ancestry the registry could not
// derive reaches the engine with a ParquetUnrepresentable reason naming the
// unplaced attribute, which the engine answers from PostgreSQL alone without
// a DuckDB pass (federated.TestQueryParquetUnrepresentableIsServedPostgresOnly).
// So cdc-flush, cdc-init and the federated route decide the schema with one
// guard. The flat schema the $ref points at is derived and keeps its tiers.
func TestQueryHandsTheEngineAnUnderivedSchemasReason(t *testing.T) {
	registry, err := newFileSchemaRegistryFromDir("testdata/underived_ancestry")
	require.NoError(t, err)
	engine := &mockFederatedQueryEngine{}
	config := createTestConfig()
	config.DuckDB.Enabled = true
	em := mustNewEntityManager(t, transform.NewPersistentRecordTransformer(registry),
		newMockPersistentRecordRepository(), engine, registry, config, nil)

	for schema, want := range map[string]string{
		"ua_union":   `could not place attribute "choice.kind"`,
		"ua_nested":  `could not place attribute "matrix.cell"`,
		"ua_ref":     `could not place attribute "contact.phone"`,
		"ua_contact": "",
	} {
		_, err := em.Query(context.Background(), &forma.QueryRequest{SchemaName: schema, Page: 1, ItemsPerPage: 10})
		require.NoError(t, err, schema)
		require.NotNil(t, engine.lastQuery, schema)
		if want == "" {
			require.Empty(t, engine.lastQuery.ParquetUnrepresentable, schema)
			continue
		}
		require.Contains(t, engine.lastQuery.ParquetUnrepresentable, want, schema)
	}
}
