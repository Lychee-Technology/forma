package factory

import (
	"context"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Unit tests for collectTablesFromPool (uses pgxmock)
// ---------------------------------------------------------------------------

func TestCollectTablesFromPool_QueryError(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	mock.ExpectQuery(`SELECT table_name FROM information_schema.tables`).
		WithArgs("tenant_schema").
		WillReturnError(assert.AnError)

	_, err = collectTablesFromPool(context.Background(), mock, "tenant_schema")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to verify database connection")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCollectTablesFromPool_Success(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows([]string{"table_name"}).
		AddRow("schema_registry").
		AddRow("eav_data")
	mock.ExpectQuery(`SELECT table_name FROM information_schema.tables`).
		WithArgs("custom_schema").
		WillReturnRows(rows)

	tables, err := collectTablesFromPool(context.Background(), mock, "custom_schema")
	require.NoError(t, err)
	assert.Contains(t, tables, "schema_registry")
	assert.Contains(t, tables, "eav_data")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCollectTablesFromPool_DefaultSchema(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	rows := pgxmock.NewRows([]string{"table_name"}).
		AddRow("schema_registry").
		AddRow("eav_data")
	mock.ExpectQuery(`SELECT table_name FROM information_schema.tables`).
		WithArgs("public").
		WillReturnRows(rows)

	_, err = collectTablesFromPool(context.Background(), mock, "")
	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}
