package transform

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// duckDBDateCache is an EAV-only schema mixing date attributes, scalar and
// list, with numeric neighbours.
func duckDBDateCache() forma.SchemaAttributeCache {
	return forma.SchemaAttributeCache{
		"seenAt": {AttributeID: 9, ValueType: forma.ValueTypeDateTime},
		"amount": {AttributeID: 11, ValueType: forma.ValueTypeNumeric},
		"days":   {AttributeID: 12, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeDate},
		"scores": {AttributeID: 13, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeNumeric},
	}
}

// readThroughDuckDBProjection runs the federated attributes_json expression
// on a real DuckDB over one literal row of unified columns (BIGINT dates,
// LIST(BIGINT) date lists, DOUBLE numerics), then reads the result the way
// the federated path does: model.ParseAttributesJSON, then
// FromPersistentRecord.
func readThroughDuckDBProjection(t *testing.T, db *sql.DB, seenAt, days string) (map[string]any, error) {
	t.Helper()
	cache := duckDBDateCache()
	sp, err := sqlgen.BuildSchemaProjection(300, cache)
	require.NoError(t, err)
	idx := strings.Index(sp.OuterSelect, "to_json(list_filter(")
	require.GreaterOrEqual(t, idx, 0)
	end := strings.Index(sp.OuterSelect[idx:], " AS attributes_json")
	require.GreaterOrEqual(t, end, 0)

	query := "SELECT " + sp.OuterSelect[idx:idx+end] + ` FROM (SELECT '018f4a3c-0000-7000-8000-000000000001' AS row_id, ` +
		seenAt + `::BIGINT AS seenAt, 1.5::DOUBLE AS amount, ` + days + `::BIGINT[] AS days, [2.5]::DOUBLE[] AS scores)`
	var attrsJSON string
	require.NoError(t, db.QueryRow(query).Scan(&attrsJSON), "query: %s", query)

	record := &model.PersistentRecord{SchemaID: 300}
	require.NoError(t, model.ParseAttributesJSON([]byte(attrsJSON), record), attrsJSON)
	tr := NewPersistentRecordTransformer(&stubSchemaRegistry{schemaID: 300, schemaName: "eav_dates", cache: cache})
	return tr.FromPersistentRecord(context.Background(), record)
}

// #592 regression matrix, DuckDB projection rows: a Parquet BIGINT (scalar
// or LIST element) past 2^53 reaches the reader as its exact digits and is
// refused as a consistency error, never read as the 2^53 the former CAST to
// DOUBLE made of it. Values inside the contract read exactly next to the
// numeric neighbours.
func TestDuckDBProjection_DateImagesAreJudgedExactly(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	got, err := readThroughDuckDBProjection(t, db, "9007199254740992", "[-9007199254740992, 1704067200123, 0, -1, 9007199254740991, -9007199254740991]")
	require.NoError(t, err)
	require.Equal(t, time.UnixMilli(9007199254740992).UTC(), got["seenAt"])
	require.Equal(t, []any{
		time.UnixMilli(-9007199254740992).UTC(), time.UnixMilli(1704067200123).UTC(),
		time.UnixMilli(0).UTC(), time.UnixMilli(-1).UTC(),
		time.UnixMilli(9007199254740991).UTC(), time.UnixMilli(-9007199254740991).UTC(),
	}, got["days"])
	require.Equal(t, 1.5, got["amount"])
	require.Equal(t, []any{2.5}, got["scores"])

	for name, row := range map[string][2]string{
		"scalar 2^53+1":    {"9007199254740993", "[0]"},
		"scalar -(2^53+1)": {"-9007199254740993", "[0]"},
		"scalar 2^53+2":    {"9007199254740994", "[0]"},
		"scalar MaxInt64":  {"9223372036854775807", "[0]"},
		"scalar MinInt64":  {"(-9223372036854775808)", "[0]"},
		"list 2^53+1":      {"0", "[1704067200123, 9007199254740993]"},
		"list -(2^53+1)":   {"0", "[-9007199254740993]"},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := readThroughDuckDBProjection(t, db, row[0], row[1])
			require.Error(t, err)
			require.Nil(t, got)
			require.Contains(t, err.Error(), "is outside the epoch milliseconds a float64 image keeps exactly (up to 9007199254740992, 2^53)")
			require.NotErrorIs(t, err, forma.ErrInvalidInput)
		})
	}
}
