package federated

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/model"
)

const (
	benchmarkSchemaIDCustomer int16 = 100
	benchmarkSchemaIDSecurity int16 = 101
	benchmarkSchemaIDTrade    int16 = 102
)

// ExecuteFederatedQuery executes a federated query using DuckDB.
func (h *FederatedTestHarness) ExecuteFederatedQuery(ctx context.Context, opts *QueryOptions) (*QueryResult, error) {
	opts = normalizeQueryOptions(opts)
	start := time.Now()
	if opts.PreferHot {
		return h.executePreferHotQuery(ctx, opts, start)
	}
	benchmarkProjection := usesBenchmarkProjectionForSelect(opts)
	tradeTimeOnlyProjection := usesTradeTimeOnlyBenchmarkProjectionForSelect(opts)
	if needsBenchmarkDuckDBMacros(opts, benchmarkProjection, tradeTimeOnlyProjection) {
		if err := prepareBenchmarkDuckDBMacros(ctx, h); err != nil {
			return nil, err
		}
	}

	tiers, err := h.probeFederatedTiers(ctx)
	if err != nil {
		return nil, err
	}
	if tiers.isEmpty() {
		return h.ExecutePostgresQuery(ctx, opts)
	}

	// Build the S3 paths
	basePath := fmt.Sprintf("s3://%s/%s/%d/base/*.parquet", h.S3Bucket, h.S3Prefix, h.SchemaID)
	deltaPath := fmt.Sprintf("s3://%s/%s/%d/delta/*.parquet", h.S3Bucket, h.S3Prefix, h.SchemaID)

	// Build and execute the federated query
	query := h.buildFederatedQuerySQLDynamic(basePath, deltaPath, tiers.hasBaseFiles, tiers.hasDeltaFiles, tiers.dirtyIDs, opts)
	countQuery := h.buildFederatedQueryCountSQLDynamic(basePath, deltaPath, tiers.hasBaseFiles, tiers.hasDeltaFiles, tiers.dirtyIDs, opts)

	var totalRecords int64
	if err := h.Duck.DB.QueryRowContext(ctx, countQuery).Scan(&totalRecords); err != nil {
		if isFederatedTierFileError(err) {
			return h.ExecutePostgresQuery(ctx, opts)
		}
		return nil, fmt.Errorf("count query: %w", err)
	}
	if opts.CountOnly {
		return &QueryResult{TotalRecords: totalRecords, Duration: time.Since(start), Plan: tiers.executionPlan(time.Since(start))}, nil
	}
	if shouldSkipFederatedSelect(totalRecords, opts.Offset) {
		return tiers.emptyPageResult(totalRecords, time.Since(start)), nil
	}

	rows, err := h.Duck.DB.QueryContext(ctx, query)
	if err != nil {
		if isFederatedTierFileError(err) {
			return h.ExecutePostgresQuery(ctx, opts)
		}
		return nil, fmt.Errorf("execute query: %w", err)
	}
	defer rows.Close()

	records, err := h.scanQueryResults(rows, benchmarkProjection)
	if err != nil {
		return nil, err
	}

	duration := time.Since(start)
	plan := tiers.executionPlan(duration)

	return &QueryResult{
		Records:      records,
		TotalRecords: totalRecords,
		Duration:     duration,
		Plan:         plan,
	}, nil
}

// executePreferHotQuery serves the PreferHot override, which is a separate
// execution path rather than a step in the federated one: it answers straight
// from Postgres and only annotates the plan to say why.
func (h *FederatedTestHarness) executePreferHotQuery(ctx context.Context, opts *QueryOptions, start time.Time) (*QueryResult, error) {
	result, err := h.ExecutePostgresQuery(ctx, opts)
	if err != nil {
		return nil, err
	}
	if result.Plan == nil {
		result.Plan = &model.ExecutionPlan{Notes: []string{}, Timings: map[string]int64{}}
	}
	result.Plan.Notes = append(result.Plan.Notes, "prefer_hot_override", "postgres_only_execution")
	result.Plan.Timings["total"] = time.Since(start).Milliseconds()
	result.Duration = time.Since(start)
	return result, nil
}

// federatedTierState is what one probe of the three tiers found: which parquet
// tiers have files, and which row IDs are still unflushed in Postgres.
type federatedTierState struct {
	hasBaseFiles  bool
	hasDeltaFiles bool
	dirtyIDs      []uuid.UUID
}

// isEmpty reports that there is nothing for DuckDB to federate — no parquet in
// either tier and no hot rows — so the query belongs on Postgres alone.
func (s federatedTierState) isEmpty() bool {
	return !s.hasBaseFiles && !s.hasDeltaFiles && len(s.dirtyIDs) == 0
}

func (s federatedTierState) executionPlan(duration time.Duration) *model.ExecutionPlan {
	return buildExecutionPlan(len(s.dirtyIDs), s.hasBaseFiles, s.hasDeltaFiles, duration)
}

// emptyPageResult answers a page that starts past the last row: the count is
// still real, so it is reported, but no select is worth running.
func (s federatedTierState) emptyPageResult(totalRecords int64, duration time.Duration) *QueryResult {
	plan := s.executionPlan(duration)
	plan.Notes = append(plan.Notes, "empty_page_short_circuit")
	return &QueryResult{
		Records:      nil,
		TotalRecords: totalRecords,
		Duration:     duration,
		Plan:         plan,
	}
}

// probeFederatedTiers gathers everything the query shape depends on: parquet
// availability per tier plus the dirty set that decides which S3 rows are
// discarded in favour of their hot versions.
func (h *FederatedTestHarness) probeFederatedTiers(ctx context.Context) (federatedTierState, error) {
	hasBaseFiles, hasDeltaFiles, err := h.checkTierFiles(ctx)
	if err != nil {
		return federatedTierState{}, err
	}
	dirtyIDs, err := h.getDirtyIDs(ctx)
	if err != nil {
		return federatedTierState{}, fmt.Errorf("get dirty ids: %w", err)
	}
	return federatedTierState{hasBaseFiles: hasBaseFiles, hasDeltaFiles: hasDeltaFiles, dirtyIDs: dirtyIDs}, nil
}

// normalizeQueryOptions sets default values for query options.
func normalizeQueryOptions(opts *QueryOptions) *QueryOptions {
	if opts == nil {
		return &QueryOptions{Limit: 100}
	}
	if opts.Limit == 0 {
		opts.Limit = 100
	}
	return opts
}

// checkTierFiles checks which tiers have parquet files.
func (h *FederatedTestHarness) checkTierFiles(ctx context.Context) (hasBase, hasDelta bool, err error) {
	baseFiles, err := h.ListParquetFiles(ctx, "base")
	if err != nil {
		return false, false, fmt.Errorf("list base files: %w", err)
	}
	deltaFiles, err := h.ListParquetFiles(ctx, "delta")
	if err != nil {
		return false, false, fmt.Errorf("list delta files: %w", err)
	}
	return len(baseFiles) > 0, len(deltaFiles) > 0, nil
}

// scanQueryResults scans DuckDB query rows into PersistentRecords.
func (h *FederatedTestHarness) scanQueryResults(rows *sql.Rows, benchmarkProjection bool) ([]*model.PersistentRecord, error) {
	var records []*model.PersistentRecord
	for rows.Next() {
		var rowID string
		var schemaID int16
		var changedAt, deletedAt int64
		var name sql.NullString
		var version sql.NullInt64
		var symbol, exchange, region sql.NullString
		var tradeType sql.NullInt64
		var tradeTime any

		if benchmarkProjection {
			if err := rows.Scan(&rowID, &schemaID, &changedAt, &deletedAt, &name, &version, &symbol, &exchange, &region, &tradeType, &tradeTime); err != nil {
				return nil, fmt.Errorf("scan row: %w", err)
			}
		} else {
			if err := rows.Scan(&rowID, &schemaID, &changedAt, &deletedAt, &name, &version); err != nil {
				return nil, fmt.Errorf("scan row: %w", err)
			}
		}

		rec := &model.PersistentRecord{
			RowID:        uuid.MustParse(rowID),
			SchemaID:     schemaID,
			CreatedAt:    changedAt,
			UpdatedAt:    changedAt,
			TextItems:    make(map[string]string),
			Float64Items: make(map[string]float64),
		}
		if deletedAt > 0 {
			rec.DeletedAt = &deletedAt
		}
		if name.Valid {
			rec.TextItems["name"] = name.String
		}
		if benchmarkProjection {
			if symbol.Valid {
				rec.TextItems["symbol"] = symbol.String
			}
			if exchange.Valid {
				rec.TextItems["exchange"] = exchange.String
			}
			if region.Valid {
				rec.TextItems["region"] = region.String
			}
		}
		if version.Valid {
			rec.Float64Items["version"] = float64(version.Int64)
		}
		normalizedTradeTime, tradeTimeOK := normalizeBenchmarkTradeTimeValue(tradeTime)
		if benchmarkProjection && (tradeType.Valid || tradeTimeOK) {
			rec.Int64Items = make(map[string]int64)
			if tradeType.Valid {
				rec.Int64Items["tradeType"] = tradeType.Int64
			}
			if tradeTimeOK {
				rec.Int64Items["tradeTime"] = normalizedTradeTime
			}
		}

		records = append(records, rec)
	}
	return records, nil
}

func normalizeBenchmarkTradeTimeValue(value any) (int64, bool) {
	switch v := value.(type) {
	case int64:
		return v, true
	case int32:
		return int64(v), true
	case int:
		return int64(v), true
	case float64:
		return int64(v), true
	case time.Time:
		return v.UnixMilli(), true
	case sql.NullInt64:
		if v.Valid {
			return v.Int64, true
		}
	case sql.NullTime:
		if v.Valid {
			return v.Time.UnixMilli(), true
		}
	case string:
		if unixMillis, err := strconv.ParseInt(v, 10, 64); err == nil {
			return unixMillis, true
		}
		if parsed, err := time.Parse(time.RFC3339, v); err == nil {
			return parsed.UnixMilli(), true
		}
	case []byte:
		return normalizeBenchmarkTradeTimeValue(string(v))
	}
	return 0, false
}

// buildExecutionPlan creates an execution plan with tier and timing info.
func buildExecutionPlan(dirtyIDCount int, hasBase, hasDelta bool, duration time.Duration) *model.ExecutionPlan {
	planNotes := []string{fmt.Sprintf("dirty_ids_excluded:%d", dirtyIDCount)}
	if hasBase {
		planNotes = append(planNotes, "base_files_scanned")
	}
	if hasDelta {
		planNotes = append(planNotes, "delta_files_scanned")
	}
	planNotes = append(planNotes, "hot_buffer_scanned")

	return &model.ExecutionPlan{
		Notes: planNotes,
		Timings: map[string]int64{
			"total": duration.Milliseconds(),
		},
	}
}

func shouldSkipFederatedSelect(totalRecords int64, offset int) bool {
	if totalRecords <= 0 {
		return true
	}
	if offset < 0 {
		return false
	}
	return int64(offset) >= totalRecords
}

func isFederatedTierFileError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "No files found") || strings.Contains(err.Error(), "does not exist")
}

// buildPGConnString builds the Postgres connection string for DuckDB.
func (h *FederatedTestHarness) buildPGConnString() string {
	host := h.PGHost
	if host == "" {
		host = "localhost"
	}
	port := h.PGPort
	if port == "" {
		port = "5432"
	}
	user := h.PGUser
	if user == "" {
		user = "postgres"
	}
	password := h.PGPassword
	if password == "" {
		password = "password"
	}
	database := h.PGDatabase
	if database == "" {
		database = "postgres"
	}
	sslMode := h.PGSSLMode
	if sslMode == "" {
		sslMode = "disable"
	}
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		host, port, user, password, database, sslMode)
}

// getDirtyIDs fetches unflushed row IDs from change_log.
func (h *FederatedTestHarness) getDirtyIDs(ctx context.Context) ([]uuid.UUID, error) {
	rows, err := h.PGDB.QueryContext(ctx, `
		SELECT row_id FROM change_log 
		WHERE schema_id = $1 AND flushed_at = 0
	`, h.SchemaID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// ExecutePostgresQuery executes a direct Postgres query (no DuckDB).
func (h *FederatedTestHarness) ExecutePostgresQuery(ctx context.Context, opts *QueryOptions) (*QueryResult, error) {
	opts = normalizeQueryOptions(opts)
	start := time.Now()
	countQuery, countArgs := h.buildPostgresOnlyCountQuery(opts)
	var total int64
	if err := h.PGDB.QueryRowContext(ctx, countQuery, countArgs...).Scan(&total); err != nil {
		return nil, err
	}
	if opts.CountOnly {
		return &QueryResult{TotalRecords: total, Duration: time.Since(start), Plan: buildPostgresOnlyExecutionPlan(time.Since(start), opts.PreferHot)}, nil
	}

	query, args := h.buildPostgresOnlySelectQuery(opts)
	rows, err := h.PGDB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	benchmarkProjection := usesBenchmarkProjectionForSelect(opts)
	var records []*model.PersistentRecord
	for rows.Next() {
		var rowID string
		var schemaID int16
		var changedAt, deletedAt int64
		var name sql.NullString
		var version sql.NullInt64
		var symbol, exchange, region sql.NullString
		var tradeType, tradeTime sql.NullInt64
		if benchmarkProjection {
			if err := rows.Scan(&rowID, &schemaID, &changedAt, &deletedAt, &name, &version, &symbol, &exchange, &region, &tradeType, &tradeTime); err != nil {
				return nil, err
			}
		} else {
			if err := rows.Scan(&rowID, &schemaID, &changedAt, &deletedAt, &name, &version); err != nil {
				return nil, err
			}
		}
		rec := &model.PersistentRecord{RowID: uuid.MustParse(rowID), SchemaID: schemaID, CreatedAt: changedAt, UpdatedAt: changedAt, TextItems: map[string]string{}, Float64Items: map[string]float64{}}
		if deletedAt > 0 {
			rec.DeletedAt = &deletedAt
		}
		if name.Valid {
			rec.TextItems["name"] = name.String
		}
		if benchmarkProjection {
			if symbol.Valid {
				rec.TextItems["symbol"] = symbol.String
			}
			if exchange.Valid {
				rec.TextItems["exchange"] = exchange.String
			}
			if region.Valid {
				rec.TextItems["region"] = region.String
			}
		}
		if version.Valid {
			rec.Float64Items["version"] = float64(version.Int64)
		}
		if benchmarkProjection && (tradeType.Valid || tradeTime.Valid) {
			rec.Int64Items = make(map[string]int64)
			if tradeType.Valid {
				rec.Int64Items["tradeType"] = tradeType.Int64
			}
			if tradeTime.Valid {
				rec.Int64Items["tradeTime"] = tradeTime.Int64
			}
		}
		records = append(records, rec)
	}

	return &QueryResult{
		Records:      records,
		TotalRecords: total,
		Duration:     time.Since(start),
		Plan:         buildPostgresOnlyExecutionPlan(time.Since(start), opts.PreferHot),
	}, nil
}

// StreamFederatedQuery streams query results with a handler callback.
func (h *FederatedTestHarness) StreamFederatedQuery(ctx context.Context, opts *QueryOptions, handler func(*model.PersistentRecord) error) error {
	result, err := h.ExecuteFederatedQuery(ctx, opts)
	if err != nil {
		return err
	}

	for _, rec := range result.Records {
		if err := handler(rec); err != nil {
			return err
		}
	}
	return nil
}
