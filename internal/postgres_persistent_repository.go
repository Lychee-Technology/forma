package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

type persistentRecordPool interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

type DBPersistentRecordRepository struct {
	pool          persistentRecordPool
	metadataCache *MetadataCache
	duckDBClient  *DuckDBClient
	nowFunc       func() time.Time
}

func NewDBPersistentRecordRepository(pool persistentRecordPool, metadataCache *MetadataCache, duckDBClient *DuckDBClient) *DBPersistentRecordRepository {
	return &DBPersistentRecordRepository{
		pool:          pool,
		metadataCache: metadataCache,
		duckDBClient:  nil,
		nowFunc:       time.Now,
	}
}

func (r *DBPersistentRecordRepository) withClock(now func() time.Time) {
	if now == nil {
		return
	}
	r.nowFunc = now
}

func (r *DBPersistentRecordRepository) nowMillis() int64 {
	if r.nowFunc == nil {
		return time.Now().UnixMilli()
	}
	return r.nowFunc().UnixMilli()
}

func validateTables(tables StorageTables) error {
	if tables.EntityMain == "" {
		return fmt.Errorf("entity main table name cannot be empty")
	}
	if tables.EAVData == "" {
		return fmt.Errorf("eav table name cannot be empty")
	}
	return nil
}

func validateWriteTables(tables StorageTables) error {
	if err := validateTables(tables); err != nil {
		return err
	}
	if tables.ChangeLog == "" {
		zap.S().Info("change log table name is empty, cdc will be disabled")
	}
	return nil
}

func (r *DBPersistentRecordRepository) upsertChangeLog(ctx context.Context, tx pgx.Tx, table string, schemaID int16, rowID uuid.UUID, changedAt int64, deletedAt *int64) error {
	flushedAt := int64(0)
	query := fmt.Sprintf(
		`INSERT INTO %s (schema_id, row_id, flushed_at, changed_at, deleted_at)
			VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT (schema_id, row_id, flushed_at)
			DO UPDATE SET changed_at = EXCLUDED.changed_at, deleted_at = EXCLUDED.deleted_at`,
		sanitizeIdentifier(table),
	)
	var deleted any
	if deletedAt != nil {
		deleted = *deletedAt
	}
	if _, err := tx.Exec(ctx, query, schemaID, rowID, flushedAt, changedAt, deleted); err != nil {
		return fmt.Errorf("insert change log: %w", err)
	}
	return nil
}

func (r *DBPersistentRecordRepository) InsertPersistentRecord(ctx context.Context, tables StorageTables, record *PersistentRecord) error {
	if record == nil {
		return fmt.Errorf("record cannot be nil")
	}
	if err := validateWriteTables(tables); err != nil {
		return err
	}

	now := r.nowMillis()
	record.CreatedAt = now
	record.UpdatedAt = now

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) // no-op if committed

	if err := r.insertMainRow(ctx, tx, tables.EntityMain, record); err != nil {
		return err
	}

	if err := r.insertEAVAttributes(ctx, tx, tables.EAVData, record.OtherAttributes); err != nil {
		return err
	}

	if tables.ChangeLog != "" {
		if err := r.upsertChangeLog(ctx, tx, tables.ChangeLog, record.SchemaID, record.RowID, record.CreatedAt, record.DeletedAt); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (r *DBPersistentRecordRepository) UpdatePersistentRecord(ctx context.Context, tables StorageTables, record *PersistentRecord) error {
	if record == nil {
		return fmt.Errorf("record cannot be nil")
	}
	if err := validateWriteTables(tables); err != nil {
		return err
	}

	record.UpdatedAt = r.nowMillis()

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := r.updateMainRow(ctx, tx, tables.EntityMain, record); err != nil {
		return err
	}

	if err := r.replaceEAVAttributes(ctx, tx, tables.EAVData, record.SchemaID, record.RowID, record.OtherAttributes); err != nil {
		return err
	}

	if tables.ChangeLog != "" {
		if err := r.upsertChangeLog(ctx, tx, tables.ChangeLog, record.SchemaID, record.RowID, record.UpdatedAt, record.DeletedAt); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (r *DBPersistentRecordRepository) DeletePersistentRecord(ctx context.Context, tables StorageTables, schemaID int16, rowID uuid.UUID) error {
	if err := validateWriteTables(tables); err != nil {
		return err
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	deleteMain := fmt.Sprintf("DELETE FROM %s WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2", sanitizeIdentifier(tables.EntityMain))
	if _, err := tx.Exec(ctx, deleteMain, schemaID, rowID); err != nil {
		return fmt.Errorf("delete entity_main row: %w", err)
	}

	deleteEAV := fmt.Sprintf("DELETE FROM %s WHERE schema_id = $1 AND row_id = $2", sanitizeIdentifier(tables.EAVData))
	if _, err := tx.Exec(ctx, deleteEAV, schemaID, rowID); err != nil {
		return fmt.Errorf("delete eav attributes: %w", err)
	}

	if tables.ChangeLog != "" {
		now := r.nowMillis()
		deletedAt := now
		if err := r.upsertChangeLog(ctx, tx, tables.ChangeLog, schemaID, rowID, now, &deletedAt); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (r *DBPersistentRecordRepository) GetPersistentRecord(ctx context.Context, tables StorageTables, schemaID int16, rowID uuid.UUID) (*PersistentRecord, error) {
	if err := validateTables(tables); err != nil {
		return nil, err
	}

	record, err := r.loadMainRecord(ctx, tables.EntityMain, schemaID, rowID)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, nil
	}

	attributes, err := r.fetchAttributes(ctx, tables.EAVData, schemaID, rowID)
	if err != nil {
		return nil, err
	}
	record.OtherAttributes = attributes

	return record, nil
}

func (r *DBPersistentRecordRepository) QueryPersistentRecords(ctx context.Context, query *PersistentRecordQuery) (*PersistentRecordPage, error) {
	zap.S().Debugw("query persistent records", "query", query)
	if query == nil {
		return nil, fmt.Errorf("query cannot be nil")
	}
	if err := validateTables(query.Tables); err != nil {
		return nil, err
	}
	if query.SchemaID <= 0 {
		return nil, fmt.Errorf("schema id must be positive")
	}

	limit := query.Limit
	if limit <= 0 {
		limit = 50
	}
	offset := query.Offset
	if offset < 0 {
		offset = 0
	}

	attrQuery := AttributeQuery{
		SchemaID:  query.SchemaID,
		Condition: query.Condition,
	}

	// Get schema cache first for checking main table conditions
	var cache forma.SchemaAttributeCache
	if query.SchemaID > 0 && r.metadataCache != nil {
		if cacheLocal, ok := r.metadataCache.GetSchemaCacheByID(query.SchemaID); ok {
			cache = cacheLocal
		} else {
			return nil, fmt.Errorf("no cache for schema id %d", query.SchemaID)
		}
	}

	useMainTableAsAnchor := hasMainTableCondition(query.Condition, cache)

	conditions, args, err := r.buildHybridConditions(
		sanitizeIdentifier(query.Tables.EAVData),
		sanitizeIdentifier(query.Tables.EntityMain),
		attrQuery,
		1,
		useMainTableAsAnchor,
	)
	if err != nil {
		return nil, fmt.Errorf("build hybrid conditions: %w", err)
	}

	// Use the optimized single-query approach that eliminates N+1 queries
	records, totalRecords, err := r.runOptimizedQuery(
		ctx,
		query.Tables,
		query.SchemaID,
		conditions,
		args,
		limit,
		offset,
		query.AttributeOrders,
		useMainTableAsAnchor,
	)
	if err != nil {
		return nil, err
	}

	currentPage := 1
	if limit > 0 {
		currentPage = offset/limit + 1
	}

	return &PersistentRecordPage{
		Records:      records,
		TotalRecords: totalRecords,
		TotalPages:   computeTotalPages(totalRecords, limit),
		CurrentPage:  currentPage,
	}, nil
}

// FetchDirtyRowIDs returns all row_ids present in the change_log with flushed_at = 0
// for the given schema. This can be used by federated query coordinator to exclude
// dirty rows from columnar/duckdb reads (anti-join).
func (r *DBPersistentRecordRepository) FetchDirtyRowIDs(ctx context.Context, changeLogTable string, schemaID int16) ([]uuid.UUID, error) {
	if changeLogTable == "" {
		return nil, fmt.Errorf("change log table name cannot be empty")
	}
	query := fmt.Sprintf(`SELECT row_id FROM %s WHERE schema_id = $1 AND flushed_at = 0`, sanitizeIdentifier(changeLogTable))
	rows, err := r.pool.Query(ctx, query, schemaID)
	if err != nil {
		return nil, fmt.Errorf("query dirty row ids: %w", err)
	}
	defer rows.Close()

	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan dirty row id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dirty row ids: %w", err)
	}
	return ids, nil
}

// QueryPersistentRecordsFederated performs a federated query across configured data tiers.
// Backwards compatible: hot-only hints delegate to QueryPersistentRecords.
func (r *DBPersistentRecordRepository) QueryPersistentRecordsFederated(ctx context.Context, tables StorageTables, fq *FederatedAttributeQuery, opts *FederatedQueryOptions) (*PersistentRecordPage, error) {
	if fq == nil {
		return nil, fmt.Errorf("federated query cannot be nil")
	}
	// If no explicit tiers or a hot-only preference is indicated, delegate to existing OLTP path.
	if len(fq.PreferredTiers) == 0 || fq.PreferHot || (len(fq.PreferredTiers) == 1 && fq.PreferredTiers[0] == DataTierHot) {
		prq := &PersistentRecordQuery{
			Tables:          tables,
			SchemaID:        fq.SchemaID,
			Condition:       fq.Condition,
			AttributeOrders: fq.AttributeOrders,
			Limit:           fq.Limit,
			Offset:          fq.Offset,
		}
		return r.QueryPersistentRecords(ctx, prq)
	}

	// Evaluate routing policy before executing
	var routingCfg forma.DuckDBConfig
	// Attempt to read global default if available via metadata cache - fallback to zero value
	if r.metadataCache != nil {
		// no-op for now; use schema-level defaults if added later
		_ = routingCfg
	}

	// Initialize execution plan if requested
	if opts != nil && opts.IncludeExecutionPlan {
		if opts.ExecutionPlan == nil {
			opts.ExecutionPlan = &ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}}
		}
		opts.ExecutionPlan.Notes = append(opts.ExecutionPlan.Notes, "EvaluateRoutingPolicy")
	}

	decision := EvaluateRoutingPolicy(routingCfg, fq, opts)
	if opts != nil && opts.IncludeExecutionPlan && opts.ExecutionPlan != nil {
		opts.ExecutionPlan.Routing = decision
	}

	if !decision.UseDuckDB {
		// route to Postgres-only
		prq := &PersistentRecordQuery{
			Tables:          tables,
			SchemaID:        fq.SchemaID,
			Condition:       fq.Condition,
			AttributeOrders: fq.AttributeOrders,
			Limit:           fq.Limit,
			Offset:          fq.Offset,
		}
		return r.QueryPersistentRecords(ctx, prq)
	}

	// Attempt DuckDB federated execution.
	records, totalRecords, err := r.ExecuteDuckDBFederatedQuery(ctx, tables, fq, fq.Limit, fq.Offset, fq.AttributeOrders, opts)
	if err != nil {
		// Fallback to Postgres-only when partial degraded mode allowed.
		if opts != nil && opts.AllowPartialDegradedMode {
			prq := &PersistentRecordQuery{
				Tables:          tables,
				SchemaID:        fq.SchemaID,
				Condition:       fq.Condition,
				AttributeOrders: fq.AttributeOrders,
				Limit:           fq.Limit,
				Offset:          fq.Offset,
			}
			return r.QueryPersistentRecords(ctx, prq)
		}
		return nil, fmt.Errorf("duckdb federated query: %w", err)
	}

	currentPage := 1
	limit := fq.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 0 {
		currentPage = fq.Offset/limit + 1
	}

	return &PersistentRecordPage{
		Records:      records,
		TotalRecords: totalRecords,
		TotalPages:   computeTotalPages(totalRecords, limit),
		CurrentPage:  currentPage,
	}, nil
}
