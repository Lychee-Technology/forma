package internal

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
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

	repo := NewDBPersistentRecordRepository(pool, nil, nil)
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

	var (
		flushedAt       int64
		changeTimestamp int64
		deletedAt       pgtype.Int8
	)
	query := fmt.Sprintf(`SELECT flushed_at, changed_at, deleted_at FROM %s WHERE schema_id = $1 AND row_id = $2 AND flushed_at = 0`, sanitizeIdentifier(tables.ChangeLog))
	err = pool.QueryRow(ctx, query, record.SchemaID, record.RowID).Scan(&flushedAt, &changeTimestamp, &deletedAt)
	require.NoError(t, err)
	assert.Equal(t, int64(0), flushedAt)
	assert.Equal(t, fixed.UnixMilli(), changeTimestamp)
	assert.False(t, deletedAt.Valid)
}

func TestChangeLogWritesOnUpdateAndDeleteIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	pool := connectTestPostgres(t, ctx)
	tables := createTempPersistentTables(t, ctx, pool)

	repo := NewDBPersistentRecordRepository(pool, nil, nil)
	rowID := uuid.New()

	createdAt := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	repo.withClock(func() time.Time { return createdAt })
	record := &PersistentRecord{
		SchemaID: 1,
		RowID:    rowID,
		TextItems: map[string]string{
			"text_01": "hello",
		},
	}
	require.NoError(t, repo.InsertPersistentRecord(ctx, tables, record))

	updatedAt := time.Date(2024, 1, 2, 4, 5, 6, 0, time.UTC)
	repo.withClock(func() time.Time { return updatedAt })
	record.TextItems["text_01"] = "updated"
	require.NoError(t, repo.UpdatePersistentRecord(ctx, tables, record))

	var (
		changeTimestamp int64
		deletedStamp    pgtype.Int8
	)
	query := fmt.Sprintf(`SELECT changed_at, deleted_at FROM %s WHERE schema_id = $1 AND row_id = $2 AND flushed_at = 0`, sanitizeIdentifier(tables.ChangeLog))
	err := pool.QueryRow(ctx, query, record.SchemaID, record.RowID).Scan(&changeTimestamp, &deletedStamp)
	require.NoError(t, err)
	assert.Equal(t, updatedAt.UnixMilli(), changeTimestamp)
	assert.False(t, deletedStamp.Valid)

	deletedAt := time.Date(2024, 1, 2, 5, 6, 7, 0, time.UTC)
	repo.withClock(func() time.Time { return deletedAt })
	require.NoError(t, repo.DeletePersistentRecord(ctx, tables, record.SchemaID, record.RowID))

	err = pool.QueryRow(ctx, query, record.SchemaID, record.RowID).Scan(&changeTimestamp, &deletedStamp)
	require.NoError(t, err)
	assert.Equal(t, deletedAt.UnixMilli(), changeTimestamp)
	require.True(t, deletedStamp.Valid)
	assert.Equal(t, deletedAt.UnixMilli(), deletedStamp.Int64)
}

func TestRunOptimizedQueryIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
	defer cancel()

	pool := connectTestPostgres(t, ctx)
	tables := createTempPersistentTables(t, ctx, pool)

	repo := NewDBPersistentRecordRepository(pool, nil, nil)
	fixed := time.Date(2024, 2, 3, 4, 5, 6, 0, time.UTC)
	repo.withClock(func() time.Time { return fixed })

	rowID := uuid.New()
	record := &PersistentRecord{
		SchemaID: 1,
		RowID:    rowID,
		TextItems: map[string]string{
			"text_01": "hello",
		},
	}
	require.NoError(t, repo.InsertPersistentRecord(ctx, tables, record))

	records, total, err := repo.runOptimizedQuery(
		ctx,
		tables,
		1,
		"1=1",
		nil,
		10,
		0,
		nil,
		true,
	)
	require.NoError(t, err)
	require.Len(t, records, 1)
	assert.Equal(t, int64(1), total)
	assert.Equal(t, record.SchemaID, records[0].SchemaID)
	assert.Equal(t, record.RowID, records[0].RowID)
	assert.Equal(t, record.TextItems, records[0].TextItems)
	assert.Nil(t, records[0].OtherAttributes)
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
	changeLogTable := fmt.Sprintf("change_log_it_%d", suffix)

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

	changeLogDDL := fmt.Sprintf(
		`CREATE TABLE %s (schema_id SMALLINT NOT NULL, row_id UUID NOT NULL, flushed_at BIGINT NOT NULL DEFAULT 0, changed_at BIGINT NOT NULL, deleted_at BIGINT, PRIMARY KEY (schema_id, row_id, flushed_at))`,
		sanitizeIdentifier(changeLogTable),
	)

	_, err = pool.Exec(ctx, changeLogDDL)
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pool.Exec(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", sanitizeIdentifier(eavTable)))
		_, _ = pool.Exec(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", sanitizeIdentifier(entityTable)))
		_, _ = pool.Exec(cleanupCtx, fmt.Sprintf("DROP TABLE IF EXISTS %s", sanitizeIdentifier(changeLogTable)))
	})

	return StorageTables{
		EntityMain: entityTable,
		EAVData:    eavTable,
		ChangeLog:  changeLogTable,
	}
}
