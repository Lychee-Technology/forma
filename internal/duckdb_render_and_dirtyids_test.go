package internal

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// Test RenderS3ParquetPath templating.
func TestRenderS3ParquetPath_Render(t *testing.T) {
	got, err := RenderS3ParquetPath("s3://bucket/schema_{{.SchemaID}}/data.parquet", 42)
	require.NoError(t, err)
	require.Equal(t, "s3://bucket/schema_42/data.parquet", got)

	_, err = RenderS3ParquetPath("", 1)
	require.Error(t, err)
}

// Test RenderDirtyIDsValuesCSV produces correct VALUES list.
func TestRenderDirtyIDsValuesCSV(t *testing.T) {
	u1 := uuid.New()
	u2 := uuid.New()
	csv := RenderDirtyIDsValuesCSV([]uuid.UUID{u1, u2})
	require.Contains(t, csv, "('"+u1.String()+"')")
	require.Contains(t, csv, "('"+u2.String()+"')")
}

// Test AppendDirtyExclusion builds clause and args.
func TestAppendDirtyExclusion(t *testing.T) {
	base := "age > 18"
	u1 := uuid.New()
	u2 := uuid.New()
	clause, args := AppendDirtyExclusion(base, []uuid.UUID{u1, u2})
	require.Contains(t, clause, "age > 18")
	require.Contains(t, clause, "row_id NOT IN (")
	require.Len(t, args, 2)
	require.Equal(t, u1.String(), args[0])
}
