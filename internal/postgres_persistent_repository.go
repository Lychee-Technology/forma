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

type PostgresPersistentRecordRepository struct {
	pool          persistentRecordPool
	metadataCache *MetadataCache
	nowFunc       func() time.Time
}

func NewPostgresPersistentRecordRepository(pool persistentRecordPool, metadataCache *MetadataCache) *PostgresPersistentRecordRepository {
	return &PostgresPersistentRecordRepository{
		pool:          pool,
		metadataCache: metadataCache,
		nowFunc:       time.Now,
	}
}

func (r *PostgresPersistentRecordRepository) withClock(now func() time.Time) {
	if now == nil {
		return
	}
	r.nowFunc = now
}

func (r *PostgresPersistentRecordRepository) nowMillis() int64 {
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
		return fmt.Errorf("change log table name cannot be empty")
	}
	return nil
}

func (r *PostgresPersistentRecordRepository) insertChangeLog(ctx context.Context, tx pgx.Tx, table string, schemaID int16, rowID uuid.UUID, changedAt int64, deletedAt *int64) error {
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

func (r *PostgresPersistentRecordRepository) InsertPersistentRecord(ctx context.Context, tables StorageTables, record *PersistentRecord) error {
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

	if err := r.insertChangeLog(ctx, tx, tables.ChangeLog, record.SchemaID, record.RowID, record.CreatedAt, record.DeletedAt); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (r *PostgresPersistentRecordRepository) UpdatePersistentRecord(ctx context.Context, tables StorageTables, record *PersistentRecord) error {
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

	if err := r.insertChangeLog(ctx, tx, tables.ChangeLog, record.SchemaID, record.RowID, record.UpdatedAt, record.DeletedAt); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (r *PostgresPersistentRecordRepository) DeletePersistentRecord(ctx context.Context, tables StorageTables, schemaID int16, rowID uuid.UUID) error {
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

	now := r.nowMillis()
	deletedAt := now
	if err := r.insertChangeLog(ctx, tx, tables.ChangeLog, schemaID, rowID, now, &deletedAt); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (r *PostgresPersistentRecordRepository) GetPersistentRecord(ctx context.Context, tables StorageTables, schemaID int16, rowID uuid.UUID) (*PersistentRecord, error) {
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

func (r *PostgresPersistentRecordRepository) QueryPersistentRecords(ctx context.Context, query *PersistentRecordQuery) (*PersistentRecordPage, error) {
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
