package federated

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/lychee-technology/forma/internal/model"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// duckDBLiteralTypeForKind mirrors the DuckDB column types produced by the
// federated outer SELECT for each entity_main column kind.
func duckDBLiteralTypeForKind(kind model.ColumnKind) string {
	switch kind {
	case model.ColumnKindText:
		return "VARCHAR"
	case model.ColumnKindSmallint:
		return "SMALLINT"
	case model.ColumnKindInteger:
		return "INTEGER"
	case model.ColumnKindBigint:
		return "BIGINT"
	case model.ColumnKindDouble:
		return "DOUBLE"
	case model.ColumnKindUUID:
		return "UUID"
	default:
		return "VARCHAR"
	}
}

// TestStreamDuckDBRows_RealDriver_UUIDColumnsScan pins issue #147: the
// duckdb-go driver returns UUID columns as 16 raw bytes, and the scan buffers
// must decode them instead of silently leaving uuid.Nil row ids.
func TestStreamDuckDBRows_RealDriver_UUIDColumnsScan(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	require.NoError(t, err)
	defer db.Close()

	rowID := uuid.MustParse("1a0f337e-0f82-5679-b842-1254f95c0642")
	customerID := uuid.MustParse("c79ae7be-710b-5540-9c05-19c2dec9950b")

	parts := make([]string, 0, len(model.EntityMainColumnDescriptors)+4)
	for _, desc := range model.EntityMainColumnDescriptors {
		switch desc.Name {
		case "ltbase_schema_id":
			parts = append(parts, "102::SMALLINT AS ltbase_schema_id")
		case "ltbase_row_id":
			parts = append(parts, fmt.Sprintf("CAST('%s' AS UUID) AS ltbase_row_id", rowID))
		case "ltbase_created_at":
			parts = append(parts, "11::BIGINT AS ltbase_created_at")
		case "ltbase_updated_at":
			parts = append(parts, "22::BIGINT AS ltbase_updated_at")
		case "uuid_01":
			parts = append(parts, fmt.Sprintf("CAST('%s' AS UUID) AS uuid_01", customerID))
		default:
			parts = append(parts, fmt.Sprintf("NULL::%s AS %s", duckDBLiteralTypeForKind(desc.Kind), desc.Name))
		}
	}
	parts = append(parts,
		"'[]'::VARCHAR AS attributes_json",
		"1::BIGINT AS total_records",
		"1::BIGINT AS total_pages",
		"1::BIGINT AS current_page",
	)

	rows, err := db.Query("SELECT " + strings.Join(parts, ", "))
	require.NoError(t, err)
	defer rows.Close()

	engine := &DBFederatedQueryEngine{}
	var records []*model.PersistentRecord
	total, rowCount, err := engine.streamDuckDBRows(context.Background(), rows, func(_ context.Context, record *model.PersistentRecord) error {
		records = append(records, record)
		return nil
	})

	require.NoError(t, err)
	require.Equal(t, int64(1), total)
	require.Equal(t, int64(1), rowCount)
	require.Len(t, records, 1)
	require.Equal(t, rowID, records[0].RowID, "ltbase_row_id must survive the DuckDB UUID scan")
	require.Equal(t, customerID, records[0].UUIDItems["uuid_01"], "uuid_01 must survive the DuckDB UUID scan")
	require.Equal(t, int16(102), records[0].SchemaID)
}
