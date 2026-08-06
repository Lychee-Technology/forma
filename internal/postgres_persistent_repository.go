package internal

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/queryplan"
	"github.com/lychee-technology/forma/internal/schemameta"

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
	metadataCache *schemameta.MetadataCache
	nowFunc       func() time.Time
	// planCache caches the rendered optimized-query SQL per query shape
	// (#142). The default is repository-local; WithPlanCache injects a cache
	// shared across repository instances so hits survive transient
	// construction (the benchmark and production reuse lifecycle).
	planCache *queryplan.Cache
}

// RepoOption customizes optional repository collaborators.
type RepoOption func(*DBPersistentRecordRepository)

// WithPlanCache injects a shared plan cache (#142).
func WithPlanCache(c *queryplan.Cache) RepoOption {
	return func(r *DBPersistentRecordRepository) { r.planCache = c }
}

func NewDBPersistentRecordRepository(pool persistentRecordPool, metadataCache *schemameta.MetadataCache, opts ...RepoOption) *DBPersistentRecordRepository {
	r := &DBPersistentRecordRepository{
		pool:          pool,
		metadataCache: metadataCache,
		nowFunc:       time.Now,
		planCache:     queryplan.NewCache(1024),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
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

func validateTables(tables model.StorageTables) error {
	if tables.EntityMain == "" {
		return fmt.Errorf("entity main table name cannot be empty")
	}
	if tables.EAVData == "" {
		return fmt.Errorf("eav table name cannot be empty")
	}
	return nil
}

func validateWriteTables(tables model.StorageTables) error {
	if err := validateTables(tables); err != nil {
		return err
	}
	if tables.ChangeLog == "" {
		zap.S().Info("change log table name is empty, cdc will be disabled")
	}
	return nil
}

// rowVersionLockKey derives the advisory-lock key that serializes version
// allocation for one (schema_id, row_id). The single-bigint
// pg_advisory_xact_lock keyspace is disjoint from the (int4, int4) form the
// per-schema CDC lock uses (internal/cdc/schema_lock.go), so colliding with
// that lock is impossible by construction; a hash collision between two
// distinct rows only serializes two unrelated writes — a latency curiosity,
// never a correctness issue.
func rowVersionLockKey(schemaID int16, rowID uuid.UUID) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte{byte(uint16(schemaID) >> 8), byte(uint16(schemaID))})
	_, _ = h.Write(rowID[:])
	return int64(h.Sum64())
}

// lockRowVersion takes the transaction-scoped per-row advisory lock under
// which create and delete allocate versions (#274 round 4). The two writers
// share no table row to lock (delete locks the existing main row; create
// inserts a new one), so without this a recreate could read the row's
// version history while a concurrent delete commits a tombstone the
// recreate then ties — and an equal-ver_ts live/tombstone pair resolves
// tombstone-wins, hiding the recreate in cold reads for good. Updates need
// no advisory lock: their version is computed inside the row UPDATE under
// the row lock, which every competing delete also takes. Batch writers
// acquire these locks in input order, the same discipline as their existing
// row locks; an order inversion between two batches is detected and errored
// by PostgreSQL's deadlock checker like any row-lock inversion. The lock
// releases at transaction end.
func lockRowVersion(ctx context.Context, tx pgx.Tx, schemaID int16, rowID uuid.UUID) error {
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", rowVersionLockKey(schemaID, rowID)); err != nil {
		return fmt.Errorf("acquire row version lock for %s: %w", rowID, err)
	}
	return nil
}

// nextRowVersion returns the version a (re)created row must carry: no
// earlier than the clock read, and strictly above every version change_log
// retains for the row across ALL slots — flushed tombstones included. A
// recreate that stamped bare wall time could land BELOW its own clock-ahead
// tombstone (#274 versions can outrun the clock) and lose the LWW fold to
// it forever once that tombstone reaches parquet. change_log's flushed
// entries are never garbage-collected, so the MAX is the row's full
// retained history; a fresh row_id has none and gets the plain clock read.
func nextRowVersion(ctx context.Context, tx pgx.Tx, table string, schemaID int16, rowID uuid.UUID, nowMillis int64) (int64, error) {
	if table == "" {
		return nowMillis, nil
	}
	var prev int64
	query := fmt.Sprintf("SELECT COALESCE(MAX(changed_at), 0) FROM %s WHERE schema_id = $1 AND row_id = $2", sanitizeIdentifier(table))
	if err := tx.QueryRow(ctx, query, schemaID, rowID).Scan(&prev); err != nil {
		return 0, fmt.Errorf("resolve prior row version for %s: %w", rowID, err)
	}
	if prev+1 > nowMillis {
		return prev + 1, nil
	}
	return nowMillis, nil
}

// computeTombstoneStamp is the version a hard delete's tombstone carries: strictly
// after the deleted row's last update (#274 per-row monotonic versions can
// run ahead of the wall clock, and delete-wins only holds on an equal or
// greater ver_ts), and no earlier than the current clock read.
func computeTombstoneStamp(nowMillis, prevUpdatedAt int64) int64 {
	if prevUpdatedAt+1 > nowMillis {
		return prevUpdatedAt + 1
	}
	return nowMillis
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

func (r *DBPersistentRecordRepository) InsertPersistentRecord(ctx context.Context, tables model.StorageTables, record *model.PersistentRecord) error {
	if record == nil {
		return fmt.Errorf("record cannot be nil")
	}
	if err := validateWriteTables(tables); err != nil {
		return fmt.Errorf("validate tables for insert: %w", err)
	}

	now := r.nowMillis()

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op if committed

	// A recreate of a deleted row_id must outrank the retained tombstone
	// (#274): the version comes from nextRowVersion, while CreatedAt stays
	// the identity clock read. The per-row lock serializes the history read
	// against a concurrent delete's tombstone allocation.
	if err := lockRowVersion(ctx, tx, record.SchemaID, record.RowID); err != nil {
		return err
	}
	effective, err := nextRowVersion(ctx, tx, tables.ChangeLog, record.SchemaID, record.RowID, now)
	if err != nil {
		return fmt.Errorf("stamp create version for row %s: %w", record.RowID, err)
	}
	record.CreatedAt = now
	record.UpdatedAt = effective

	if err := r.insertMainRow(ctx, tx, tables.EntityMain, record); err != nil {
		return fmt.Errorf("insert main row for %s: %w", record.RowID, err)
	}

	if err := r.insertEAVAttributes(ctx, tx, tables.EAVData, record.OtherAttributes); err != nil {
		return fmt.Errorf("insert eav attributes for %s: %w", record.RowID, err)
	}

	if tables.ChangeLog != "" {
		if err := r.upsertChangeLog(ctx, tx, tables.ChangeLog, record.SchemaID, record.RowID, record.UpdatedAt, record.DeletedAt); err != nil {
			return fmt.Errorf("upsert change log for %s: %w", record.RowID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (r *DBPersistentRecordRepository) UpdatePersistentRecord(ctx context.Context, tables model.StorageTables, record *model.PersistentRecord) error {
	if record == nil {
		return fmt.Errorf("record cannot be nil")
	}
	if err := validateWriteTables(tables); err != nil {
		return fmt.Errorf("validate tables for update: %w", err)
	}

	record.UpdatedAt = r.nowMillis()

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op if committed

	if err := r.updateMainRow(ctx, tx, tables.EntityMain, record); err != nil {
		return fmt.Errorf("update main row for %s: %w", record.RowID, err)
	}

	if err := r.replaceEAVAttributes(ctx, tx, tables.EAVData, record.SchemaID, record.RowID, record.OtherAttributes); err != nil {
		return fmt.Errorf("replace eav attributes for %s: %w", record.RowID, err)
	}

	if tables.ChangeLog != "" {
		if err := r.upsertChangeLog(ctx, tx, tables.ChangeLog, record.SchemaID, record.RowID, record.UpdatedAt, record.DeletedAt); err != nil {
			return fmt.Errorf("upsert change log for %s: %w", record.RowID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (r *DBPersistentRecordRepository) DeletePersistentRecord(ctx context.Context, tables model.StorageTables, schemaID int16, rowID uuid.UUID) error {
	if err := validateWriteTables(tables); err != nil {
		return fmt.Errorf("validate tables for delete: %w", err)
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // no-op if committed

	// The per-row lock pairs with the create path's: a concurrent recreate
	// must observe this delete's tombstone before allocating its version.
	if err := lockRowVersion(ctx, tx, schemaID, rowID); err != nil {
		return err
	}

	// RETURNING captures the deleted row's version so the tombstone can be
	// stamped strictly after it (#274): update timestamps are per-row
	// monotonic and may run ahead of the wall clock, so a bare nowMillis()
	// tombstone could rank BELOW the live version it deletes — a lost delete.
	deleteMain := fmt.Sprintf("DELETE FROM %s WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2 RETURNING ltbase_updated_at", sanitizeIdentifier(tables.EntityMain))
	var prevUpdatedAt int64
	if err := tx.QueryRow(ctx, deleteMain, schemaID, rowID).Scan(&prevUpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return forma.WithOperatorDetail(forma.NotFoundf("entity not found (row=%s)", rowID),
				fmt.Errorf("schema=%d", schemaID))
		}
		return fmt.Errorf("delete entity_main row: %w", err)
	}

	deleteEAV := fmt.Sprintf("DELETE FROM %s WHERE schema_id = $1 AND row_id = $2", sanitizeIdentifier(tables.EAVData))
	if _, err := tx.Exec(ctx, deleteEAV, schemaID, rowID); err != nil {
		return fmt.Errorf("delete eav attributes: %w", err)
	}

	if tables.ChangeLog != "" {
		stamp := computeTombstoneStamp(r.nowMillis(), prevUpdatedAt)
		if err := r.upsertChangeLog(ctx, tx, tables.ChangeLog, schemaID, rowID, stamp, &stamp); err != nil {
			return fmt.Errorf("upsert tombstone change log for %s: %w", rowID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

func (r *DBPersistentRecordRepository) GetPersistentRecord(ctx context.Context, tables model.StorageTables, schemaID int16, rowID uuid.UUID) (*model.PersistentRecord, error) {
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

func (r *DBPersistentRecordRepository) QueryPersistentRecords(ctx context.Context, query *model.PersistentRecordQuery) (*model.PersistentRecordPage, error) {
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
		limit = model.DefaultPageSize
	}
	offset := max(query.Offset, 0)

	attrQuery := model.AttributeQuery{
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

	return &model.PersistentRecordPage{
		Records:      records,
		TotalRecords: totalRecords,
		TotalPages:   model.ComputeTotalPages(totalRecords, limit),
		CurrentPage:  currentPage,
	}, nil
}
