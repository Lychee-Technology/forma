package cdc

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestRunOnce_RequiresSchemaRegistry(t *testing.T) {
	err := RunOnce(context.Background(), CDCConfig{}, nil, false, zap.NewNop(), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "schema registry is required")
}

func TestResolveS3Clients_UsesFallbackWhenCustomClientNotProvided(t *testing.T) {
	fallback := &fullS3ClientMock{}

	objectClient, fullClient, err := resolveS3Clients(nil, fallback, false)
	require.NoError(t, err)
	require.Same(t, fallback, objectClient)
	require.Same(t, fallback, fullClient)
}

func TestResolveS3Clients_AcceptsObjectOnlyClientWhenFullNotRequired(t *testing.T) {
	customObjectClient := &objectOnlyS3Client{}
	fallback := &fullS3ClientMock{}

	objectClient, fullClient, err := resolveS3Clients(customObjectClient, fallback, false)
	require.NoError(t, err)
	require.Same(t, customObjectClient, objectClient)
	require.Nil(t, fullClient)
}

func TestResolveS3Clients_RejectsObjectOnlyClientWhenFullRequired(t *testing.T) {
	customObjectClient := &objectOnlyS3Client{}
	fallback := &fullS3ClientMock{}

	_, _, err := resolveS3Clients(customObjectClient, fallback, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "manifest requires S3FullClient")
}

func TestResolveS3Clients_UsesCustomFullClientWhenProvided(t *testing.T) {
	customFullClient := &fullS3ClientMock{}
	fallback := &fullS3ClientMock{}

	objectClient, fullClient, err := resolveS3Clients(customFullClient, fallback, true)
	require.NoError(t, err)
	require.Same(t, customFullClient, objectClient)
	require.Same(t, customFullClient, fullClient)
}

func TestSetupPostgresConnection_UsesIAMToken(t *testing.T) {
	orig := generateIAMTokenFn
	defer func() { generateIAMTokenFn = orig }()

	const token = "iam-token"
	generateIAMTokenFn = func(ctx context.Context, endpoint, region string, creds any) (string, error) {
		return token, nil
	}

	cfg := CDCConfig{
		PGHost:     "localhost",
		PGPort:     5432,
		PGUser:     "user",
		PGPassword: "envpass",
		PGDB:       "db",
		PGUseIAM:   true,
	}

	db, pgPassword, err := setupPostgresConnection(context.Background(), cfg, "us-east-1", nil, zap.NewNop())
	require.NoError(t, err)
	require.NotNil(t, db)
	require.Equal(t, token, pgPassword)
	require.NoError(t, db.Close())
}

func TestSetupPostgresConnection_FallsBackToPGPassword(t *testing.T) {
	orig := generateIAMTokenFn
	defer func() { generateIAMTokenFn = orig }()

	generateIAMTokenFn = func(ctx context.Context, endpoint, region string, creds any) (string, error) {
		return "", errors.New("token unavailable")
	}

	cfg := CDCConfig{
		PGHost:     "localhost",
		PGPort:     5432,
		PGUser:     "user",
		PGPassword: "envpass",
		PGDB:       "db",
		PGUseIAM:   true,
	}

	db, pgPassword, err := setupPostgresConnection(context.Background(), cfg, "us-east-1", nil, zap.NewNop())
	require.NoError(t, err)
	require.NotNil(t, db)
	require.Equal(t, "envpass", pgPassword)
	require.NoError(t, db.Close())
}

func TestShouldFlush(t *testing.T) {
	now := time.Now().UnixMilli()
	tests := []struct {
		name   string
		cfg    CDCConfig
		cnt    int64
		oldest int64
		want   bool
	}{
		{
			name:   "flushes when min records threshold is reached",
			cfg:    CDCConfig{MinRecords: 10, MaxAgeMs: 60000},
			cnt:    10,
			oldest: now - 1000,
			want:   true,
		},
		{
			name:   "flushes when max age threshold is reached",
			cfg:    CDCConfig{MinRecords: 100, MaxAgeMs: 1000},
			cnt:    1,
			oldest: now - 5000,
			want:   true,
		},
		{
			name:   "does not flush when neither threshold is reached",
			cfg:    CDCConfig{MinRecords: 100, MaxAgeMs: 60000},
			cnt:    99,
			oldest: now - 1000,
			want:   false,
		},
		{
			name:   "does not flush without oldest timestamp and insufficient rows",
			cfg:    CDCConfig{MinRecords: 2, MaxAgeMs: 1000},
			cnt:    1,
			oldest: 0,
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, shouldFlush(tt.cfg, tt.cnt, tt.oldest))
		})
	}
}

func TestMinMaxRowID(t *testing.T) {
	idOldest := uuid.MustParse("018f05c0-0000-7000-8000-000000000001")
	idMiddle := uuid.MustParse("018f05c0-0001-7000-8000-000000000001")
	idNewest := uuid.MustParse("018f05c0-0002-7000-8000-000000000001")

	minID, maxID := minMaxRowID(nil)
	require.Equal(t, uuid.Nil, minID)
	require.Equal(t, uuid.Nil, maxID)

	minID, maxID = minMaxRowID([]uuid.UUID{idMiddle})
	require.Equal(t, idMiddle, minID)
	require.Equal(t, idMiddle, maxID)

	minID, maxID = minMaxRowID([]uuid.UUID{idMiddle, idNewest, idOldest})
	require.Equal(t, idOldest, minID)
	require.Equal(t, idNewest, maxID)
}

func TestGetUnflushedSchemaIDs(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE change_log (schema_id BIGINT, flushed_at BIGINT)")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO change_log VALUES (1, 0), (1, 0), (2, 0), (3, 100)")
	require.NoError(t, err)

	schemaIDs, err := getUnflushedSchemaIDs(ctx, db, "change_log")
	require.NoError(t, err)
	require.ElementsMatch(t, []int64{1, 2}, schemaIDs)
}

func TestGetUnflushedSchemaIDs_QueryError(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	schemaIDs, err := getUnflushedSchemaIDs(context.Background(), db, "missing_table")
	require.Error(t, err)
	require.Nil(t, schemaIDs)
	require.Contains(t, err.Error(), "query distinct schema ids")
}

func TestGetUnflushedSchemaIDs_CanceledContext(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	schemaIDs, err := getUnflushedSchemaIDs(ctx, db, "change_log")
	require.Error(t, err)
	require.Nil(t, schemaIDs)
	require.Contains(t, err.Error(), "query distinct schema ids")
}

func TestProcessSchemas_ContinuesAcrossFailuresAndJoinsErrors(t *testing.T) {
	processed := make([]int16, 0, 3)
	flushCtx := &schemaFlushContext{
		logger:         zap.NewNop(),
		schemaRegistry: stubSchemaRegistry{cache: testAttrCache()},
		processSchemaFn: func(_ context.Context, schemaID int16) error {
			processed = append(processed, schemaID)
			if schemaID == 2 {
				return errors.New("boom")
			}
			return nil
		},
	}

	err := flushCtx.processSchemas(context.Background(), []int64{1, 2, 3})
	require.Error(t, err)
	require.Equal(t, []int16{1, 2, 3}, processed)
	require.Contains(t, err.Error(), "schema 2")
	require.Contains(t, err.Error(), "boom")
}

func TestProcessSchemas_ReturnsNilForNilOrEmptySchemaIDsWithoutProcessing(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		calls := 0
		flushCtx := &schemaFlushContext{
			logger: zap.NewNop(),
			processSchemaFn: func(context.Context, int16) error {
				calls++
				return nil
			},
		}

		err := flushCtx.processSchemas(context.Background(), nil)
		require.NoError(t, err)
		require.Zero(t, calls)
	})

	t.Run("empty", func(t *testing.T) {
		calls := 0
		flushCtx := &schemaFlushContext{
			logger: zap.NewNop(),
			processSchemaFn: func(context.Context, int16) error {
				calls++
				return nil
			},
		}

		err := flushCtx.processSchemas(context.Background(), []int64{})
		require.NoError(t, err)
		require.Zero(t, calls)
	})
}

func TestExecuteFlush_NoSelectedRowIDsReturnsWithoutExportWork(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE change_log (schema_id BIGINT, row_id UUID, changed_at BIGINT, flushed_at BIGINT)")
	require.NoError(t, err)

	flushCtx := &schemaFlushContext{
		db:        db,
		cfg:       CDCConfig{BatchSize: 10},
		tableName: "change_log",
		logger:    zap.NewNop(),
	}

	err = flushCtx.executeFlush(ctx, 7)
	require.NoError(t, err)
}

func TestProcessSchema_SkipsWhenSchemaLockIsAlreadyHeld(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	lockAttempts := 0
	releaseCalls := 0
	flushCtx := &schemaFlushContext{
		db:     db,
		cfg:    CDCConfig{MinRecords: 1, MaxAgeMs: 1000},
		logger: zap.NewNop(),
		acquireLock: func(context.Context, *sql.DB, int16) (bool, error) {
			lockAttempts++
			return false, nil
		},
		releaseLock: func(context.Context, *sql.DB, int16) error {
			releaseCalls++
			return nil
		},
	}

	err = flushCtx.processSchema(context.Background(), 7)
	require.NoError(t, err)
	require.Equal(t, 1, lockAttempts)
	require.Zero(t, releaseCalls)
}

func TestProcessSchema_ReturnsErrorWhenChangeLogStatsCannotBeRead(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	releaseCalls := 0
	flushCtx := &schemaFlushContext{
		db:        db,
		cfg:       CDCConfig{MinRecords: 1, MaxAgeMs: 1000},
		tableName: "missing_change_log",
		logger:    zap.NewNop(),
		acquireLock: func(context.Context, *sql.DB, int16) (bool, error) {
			return true, nil
		},
		releaseLock: func(context.Context, *sql.DB, int16) error {
			releaseCalls++
			return nil
		},
	}

	err = flushCtx.processSchema(context.Background(), 7)
	require.Error(t, err)
	require.Contains(t, err.Error(), "get changelog stats")
	require.Equal(t, 1, releaseCalls)
}

func TestProcessSchema_SkipsWhenNoPendingRowsExist(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE change_log (schema_id SMALLINT, changed_at BIGINT, flushed_at BIGINT)")
	require.NoError(t, err)

	releaseCalls := 0
	flushCtx := &schemaFlushContext{
		db:        db,
		cfg:       CDCConfig{MinRecords: 1, MaxAgeMs: 1000},
		tableName: "change_log",
		logger:    zap.NewNop(),
		acquireLock: func(context.Context, *sql.DB, int16) (bool, error) {
			return true, nil
		},
		releaseLock: func(context.Context, *sql.DB, int16) error {
			releaseCalls++
			return nil
		},
	}

	err = flushCtx.processSchema(ctx, 7)
	require.NoError(t, err)
	require.Equal(t, 1, releaseCalls)
}

func TestProcessSchema_SkipsWhenThresholdsAreNotMet(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	ctx := context.Background()
	_, err = db.ExecContext(ctx, "CREATE TABLE change_log (schema_id SMALLINT, changed_at BIGINT, flushed_at BIGINT)")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO change_log VALUES (7, ?, 0)", time.Now().UnixMilli())
	require.NoError(t, err)

	releaseCalls := 0
	flushCtx := &schemaFlushContext{
		db:        db,
		cfg:       CDCConfig{MinRecords: 10, MaxAgeMs: 1_000_000},
		tableName: "change_log",
		logger:    zap.NewNop(),
		acquireLock: func(context.Context, *sql.DB, int16) (bool, error) {
			return true, nil
		},
		releaseLock: func(context.Context, *sql.DB, int16) error {
			releaseCalls++
			return nil
		},
	}

	err = flushCtx.processSchema(ctx, 7)
	require.NoError(t, err)
	require.Equal(t, 1, releaseCalls)
}

func TestProcessSchemas_AbortsWhenSchemaCacheUnavailable(t *testing.T) {
	processed := false
	flushCtx := &schemaFlushContext{
		logger:         zap.NewNop(),
		schemaRegistry: errorSchemaRegistry{err: errors.New("schema unavailable")},
		processSchemaFn: func(context.Context, int16) error {
			processed = true
			return nil
		},
	}

	err := flushCtx.processSchemas(context.Background(), []int64{7})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrSchemaAttrCacheUnavailable)
	require.Contains(t, err.Error(), "7")
	require.False(t, processed, "no schema must be flushed when pre-flight aborts")
}

func TestProcessSchemas_ResolvesCachesBeforeProcessing(t *testing.T) {
	var processedIDs []int16
	flushCtx := &schemaFlushContext{
		logger:         zap.NewNop(),
		schemaRegistry: stubSchemaRegistry{cache: testAttrCache()},
		processSchemaFn: func(_ context.Context, id int16) error {
			processedIDs = append(processedIDs, id)
			return nil
		},
	}

	err := flushCtx.processSchemas(context.Background(), []int64{7, 8})
	require.NoError(t, err)
	require.Equal(t, []int16{7, 8}, processedIDs)
	require.NotEmpty(t, flushCtx.attrCaches[7])
	require.NotEmpty(t, flushCtx.attrCaches[8])
}
