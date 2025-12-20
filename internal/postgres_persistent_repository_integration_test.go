package internal

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ptr[T any](t T) *T {
	return &t
}

func TestInsertPersistentRecordIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	pool := connectTestPostgres(t, ctx)
	tables := createTempPersistentTables(t, ctx, pool)

	repo := NewPostgresPersistentRecordRepository(pool, nil)
	fixed := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	repo.withClock(func() time.Time { return fixed })

	rowID := uuid.New()
	record := &PersistentRecord{
		SchemaID: 1,
		RowID:    rowID,
		TextItems: map[string]string{
			"text_01": "hello",
		},
		Int16Items: map[string]int16{
			"smallint_01": 7,
		},
		Int32Items: map[string]int32{
			"integer_01": 42,
		},
		Int64Items: map[string]int64{
			"bigint_01": 123456789,
		},
		Float64Items: map[string]float64{
			"double_01": 3.14,
		},
		OtherAttributes: []EAVRecord{
			{SchemaID: 1, RowID: rowID, AttrID: 10, ArrayIndices: "", ValueText: ptr("foo")},
			{SchemaID: 1, RowID: rowID, AttrID: 11, ArrayIndices: "0", ValueNumeric: ptr(99.0)},
		},
	}

	err := repo.InsertPersistentRecord(ctx, tables, record)
	require.NoError(t, err)

	stored, err := repo.GetPersistentRecord(ctx, tables, record.SchemaID, record.RowID)
	require.NoError(t, err)
	require.NotNil(t, stored)

	assert.Equal(t, fixed.UnixMilli(), record.CreatedAt)
	assert.Equal(t, fixed.UnixMilli(), record.UpdatedAt)

	assert.Equal(t, fixed.UnixMilli(), stored.CreatedAt)
	assert.Equal(t, fixed.UnixMilli(), stored.UpdatedAt)
	assert.Equal(t, record.SchemaID, stored.SchemaID)
	assert.Equal(t, record.RowID, stored.RowID)
	assert.Equal(t, record.TextItems, stored.TextItems)
	assert.Equal(t, record.Int16Items, stored.Int16Items)
	assert.Equal(t, record.Int32Items, stored.Int32Items)
	assert.Equal(t, record.Int64Items, stored.Int64Items)
	assert.Equal(t, record.Float64Items, stored.Float64Items)
	assert.ElementsMatch(t, record.OtherAttributes, stored.OtherAttributes)
}

func connectTestPostgres(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()

	dsn := "postgres://postgres:postgres@localhost:5432/forma?sslmode=disable"

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("invalid postgres dsn: %v", err)
	}
	cfg.ConnConfig.ConnectTimeout = 2 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Skipf("skipping integration test, cannot connect to postgres: %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("skipping integration test, postgres not reachable: %v", err)
	}

	t.Cleanup(func() {
		pool.Close()
	})

	return pool
}

func createTempPersistentTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) StorageTables {
	t.Helper()

	suffix := time.Now().UnixNano()
	entityTable := fmt.Sprintf("entity_main_it_%d", suffix)
	eavTable := fmt.Sprintf("eav_data_it_%d", suffix)

	entityColumns := []string{
		"ltbase_schema_id SMALLINT NOT NULL",
		"ltbase_row_id UUID NOT NULL",
		"ltbase_created_at BIGINT NOT NULL",
		"ltbase_updated_at BIGINT NOT NULL",
		"ltbase_deleted_at BIGINT",
	}

	for _, col := range textColumns {
		entityColumns = append(entityColumns, fmt.Sprintf("%s TEXT", col))
	}
	for _, col := range smallintColumns {
		entityColumns = append(entityColumns, fmt.Sprintf("%s SMALLINT", col))
	}
	for _, col := range integerColumns {
		entityColumns = append(entityColumns, fmt.Sprintf("%s INTEGER", col))
	}
	for _, col := range bigintColumns {
		entityColumns = append(entityColumns, fmt.Sprintf("%s BIGINT", col))
	}
	for _, col := range doubleColumns {
		entityColumns = append(entityColumns, fmt.Sprintf("%s DOUBLE PRECISION", col))
	}
	for _, col := range uuidColumns {
		entityColumns = append(entityColumns, fmt.Sprintf("%s UUID", col))
	}

	entityDDL := fmt.Sprintf(
		"CREATE TABLE %s (%s, PRIMARY KEY (ltbase_schema_id, ltbase_row_id))",
		sanitizeIdentifier(entityTable),
		strings.Join(entityColumns, ", "),
	)

	eavDDL := fmt.Sprintf(
		"CREATE TABLE %s (schema_id SMALLINT NOT NULL, row_id UUID NOT NULL, attr_id SMALLINT NOT NULL, array_indices TEXT NOT NULL DEFAULT '', value_text TEXT, value_numeric DOUBLE PRECISION, PRIMARY KEY (schema_id, row_id, attr_id, array_indices))",
		sanitizeIdentifier(eavTable),
	)

	_, err := pool.Exec(ctx, entityDDL)
	require.NoError(t, err)

	_, err = pool.Exec(ctx, eavDDL)
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pool.Exec(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", sanitizeIdentifier(eavTable)))
		_, _ = pool.Exec(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", sanitizeIdentifier(entityTable)))
	})

	return StorageTables{
		EntityMain: entityTable,
		EAVData:    eavTable,
	}
}
