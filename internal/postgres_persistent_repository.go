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
	// uniqueKeys is the schemas' unique keys (#651), which every write core
	// enforces; nil enforces none. It is set once, by WithUniqueKeyCatalog
	// when the repository is built, and never replaced, so the write cores
	// read it without synchronization and a Tx's reader, a copy of the
	// repository, shares it.
	uniqueKeys *UniqueKeyCatalog
	// locks is the database engine's per-row write lock (#633): advisory
	// locks on PostgreSQL, the default, and guard rows on Aurora DSQL.
	locks rowLocking
}

// RepoOption customizes optional repository collaborators.
type RepoOption func(*DBPersistentRecordRepository)

// WithPlanCache injects a shared plan cache (#142).
func WithPlanCache(c *queryplan.Cache) RepoOption {
	return func(r *DBPersistentRecordRepository) { r.planCache = c }
}

// WithUniqueKeyCatalog makes the repository enforce catalog's unique keys
// (#651) for as long as it lives. NewEntityManager refuses a repository whose
// keys differ from those of the manager's registry, in declaration or in
// where an attribute is stored.
func WithUniqueKeyCatalog(catalog *UniqueKeyCatalog) RepoOption {
	return func(r *DBPersistentRecordRepository) { r.uniqueKeys = catalog }
}

// WithDatabaseEngine selects how the repository's writes exclude each other
// on engine (#633). It refuses an engine name it does not know, so a typo
// cannot fall back to the PostgreSQL engine's locks on another database.
func WithDatabaseEngine(engine forma.DatabaseEngine) (RepoOption, error) {
	locks, err := rowLockingFor(engine)
	if err != nil {
		return nil, fmt.Errorf("select the row locking of database engine %q: %w", engine, err)
	}
	return func(r *DBPersistentRecordRepository) { r.locks = locks }, nil
}

func NewDBPersistentRecordRepository(pool persistentRecordPool, metadataCache *schemameta.MetadataCache, opts ...RepoOption) *DBPersistentRecordRepository {
	r := &DBPersistentRecordRepository{
		pool:          pool,
		metadataCache: metadataCache,
		nowFunc:       time.Now,
		planCache:     queryplan.NewCache(1024),
		locks:         advisoryLocking{},
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
// tombstone-wins, hiding the recreate in cold reads for good. Single-row
// updates take it too (#457): their version is safe under the row lock
// alone, but their merge base is not — an update reads the whole document,
// merges, and rewrites every EAV row, so two updates that only took the row
// lock at write time would each merge onto a pre-write snapshot and the
// second would drop the first's fields. Holding this lock across the read
// makes the read and the write one critical section. The batch writers —
// insert, delete and the batch merge behind atomic BatchUpdate — take every
// row's lock up front in ascending (schemaID, rowID) order (#554, see
// sortedRowKeys): two overlapping batches of any kind then request the
// shared locks in the same sequence and cannot invert, and because this
// lock precedes the main-table row lock in every writer, the sort orders
// that acquisition too. The lock releases at transaction end.
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

	// READ COMMITTED on PostgreSQL, whatever the session's default: the
	// unique-key reservation waits for a concurrent writer of a value and
	// then judges its committed entry, which a snapshot level answers with
	// 40001 (#651). On Aurora DSQL that 40001 is the contract.
	now := r.nowMillis()
	return r.inWriteTx(ctx, r.locks.writeTxOptions(true), func(tx pgx.Tx) error {
		return r.insertRecordInTx(ctx, tx, tables, record, now)
	})
}

// insertRecordInTx is InsertPersistentRecord's body: every statement of the
// create, run on tx.
func (r *DBPersistentRecordRepository) insertRecordInTx(ctx context.Context, tx pgx.Tx, tables model.StorageTables, record *model.PersistentRecord, now int64) error {
	// The per-row lock serializes prepareCreate's history read against a
	// concurrent delete's tombstone allocation (#274).
	if err := r.locks.lockRow(ctx, tx, tables, record.SchemaID, record.RowID); err != nil {
		return fmt.Errorf("lock row version for insert: %w", err)
	}
	claim, err := r.prepareCreate(ctx, tx, tables, record, now)
	if err != nil {
		return fmt.Errorf("prepare create of %s: %w", record.RowID, err)
	}

	// Unique keys are reserved before the entity write, so a conflict leaves
	// nothing written; a row ID a live row has is the main insert's zero
	// rows, after which the reservation is removed (#651).
	reservation, err := r.reserveUniqueKeys(ctx, tx, tables, claim)
	if err != nil {
		return fmt.Errorf("reserve unique keys of %s: %w", record.RowID, err)
	}
	inserted, err := r.insertMainRow(ctx, tx, tables.EntityMain, record)
	if err != nil {
		return fmt.Errorf("insert main row for %s: %w", record.RowID, err)
	}
	if !inserted {
		return r.undoAfter(ctx, tx, tables, reservation, r.rowIDConflict(record.SchemaID, record.RowID))
	}
	if err := r.applyUniqueKeys(ctx, tx, tables, claim); err != nil {
		return fmt.Errorf("write unique keys of %s: %w", record.RowID, err)
	}

	if err := r.insertEAVAttributes(ctx, tx, tables.EAVData, record.OtherAttributes); err != nil {
		return fmt.Errorf("insert eav attributes for %s: %w", record.RowID, err)
	}

	if tables.ChangeLog != "" {
		if err := r.upsertChangeLog(ctx, tx, tables.ChangeLog, record.SchemaID, record.RowID, record.UpdatedAt, record.DeletedAt); err != nil {
			return fmt.Errorf("upsert change log for %s: %w", record.RowID, err)
		}
	}
	return nil
}

// prepareCreate stamps record as the create stores it and answers its
// unique-key claim; the caller holds the row's version lock. CreatedAt is
// the identity clock read now; UpdatedAt is the version nextRowVersion
// allocates, so a recreate of a deleted row_id outranks its retained
// tombstone (#274). The claim is planned from the stamped record because a
// key may name an attribute bound to ltbase_created_at, whose entry must
// hold the stamp the row is stored with (#651).
func (r *DBPersistentRecordRepository) prepareCreate(
	ctx context.Context, tx pgx.Tx, tables model.StorageTables, record *model.PersistentRecord, now int64,
) (*uniqueKeyClaim, error) {
	effective, err := nextRowVersion(ctx, tx, tables.ChangeLog, record.SchemaID, record.RowID, now)
	if err != nil {
		return nil, fmt.Errorf("stamp create version: %w", err)
	}
	record.CreatedAt = now
	record.UpdatedAt = effective
	claim, err := r.planUniqueKeys(ctx, tx, tables, record, false)
	if err != nil {
		return nil, fmt.Errorf("plan unique keys: %w", err)
	}
	return claim, nil
}

// DeletePersistentRecord pins READ COMMITTED for a guarded delete on
// PostgreSQL, for the reason MergePersistentRecord does: the guard judges the
// row after its lock is granted, and a snapshot level would freeze that
// judgement at the lock statement, before the previous holder committed. An
// unguarded delete keeps the session's level, where a snapshot conflict is a
// retryable 40001 (#645), as every write is on Aurora DSQL.
func (r *DBPersistentRecordRepository) DeletePersistentRecord(ctx context.Context, tables model.StorageTables, schemaID int16, rowID uuid.UUID, guard model.RowGuard) error {
	if err := validateWriteTables(tables); err != nil {
		return fmt.Errorf("validate tables for delete: %w", err)
	}
	rendered, err := r.renderRowGuard(tables, schemaID, guard)
	if err != nil {
		return fmt.Errorf("prepare delete of %s: %w", rowID, err)
	}
	return r.inWriteTx(ctx, r.locks.writeTxOptions(rendered != nil), func(tx pgx.Tx) error {
		return r.deleteRecordInTx(ctx, tx, tables, schemaID, rowID, rendered)
	})
}

// deleteRecordInTx is DeletePersistentRecord's body: every statement of the
// delete, run on tx. guard was rendered before the delete sent any
// statement; nil requires nothing.
func (r *DBPersistentRecordRepository) deleteRecordInTx(ctx context.Context, tx pgx.Tx, tables model.StorageTables, schemaID int16, rowID uuid.UUID, guard *renderedGuard) error {
	// The per-row lock pairs with the create path's: a concurrent recreate
	// must observe this delete's tombstone before allocating its version.
	if err := r.locks.lockRow(ctx, tx, tables, schemaID, rowID); err != nil {
		return fmt.Errorf("lock row version for delete: %w", err)
	}
	if guard != nil {
		exists, err := checkRowGuard(ctx, tx, tables, schemaID, rowID, guard)
		if err != nil {
			return fmt.Errorf("check precondition for %s: %w", rowID, err)
		}
		if !exists {
			return rowNotFound(schemaID, rowID)
		}
	}

	// RETURNING captures the deleted row's version so the tombstone can be
	// stamped strictly after it (#274): update timestamps are per-row
	// monotonic and may run ahead of the wall clock, so a bare nowMillis()
	// tombstone could rank BELOW the live version it deletes — a lost delete.
	deleteMain := fmt.Sprintf("DELETE FROM %s WHERE ltbase_schema_id = $1 AND ltbase_row_id = $2 RETURNING ltbase_updated_at", sanitizeIdentifier(tables.EntityMain))
	var prevUpdatedAt int64
	if err := tx.QueryRow(ctx, deleteMain, schemaID, rowID).Scan(&prevUpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return rowNotFound(schemaID, rowID)
		}
		return fmt.Errorf("delete entity_main row: %w", err)
	}

	deleteEAV := fmt.Sprintf("DELETE FROM %s WHERE schema_id = $1 AND row_id = $2", sanitizeIdentifier(tables.EAVData))
	if _, err := tx.Exec(ctx, deleteEAV, schemaID, rowID); err != nil {
		return fmt.Errorf("delete eav attributes: %w", err)
	}
	if err := r.releaseRowUniqueKeys(ctx, tx, tables, schemaID, rowID); err != nil {
		return fmt.Errorf("delete unique keys: %w", err)
	}

	if tables.ChangeLog != "" {
		stamp := computeTombstoneStamp(r.nowMillis(), prevUpdatedAt)
		if err := r.upsertChangeLog(ctx, tx, tables.ChangeLog, schemaID, rowID, stamp, &stamp); err != nil {
			return fmt.Errorf("upsert tombstone change log for %s: %w", rowID, err)
		}
	}
	return nil
}

// GetPersistentRecord reads one row's main columns and EAV attributes in a
// single statement, so the two can never come from different snapshots
// (#457). See loadRecordWithAttributes.
func (r *DBPersistentRecordRepository) GetPersistentRecord(ctx context.Context, tables model.StorageTables, schemaID int16, rowID uuid.UUID) (*model.PersistentRecord, error) {
	if err := validateTables(tables); err != nil {
		return nil, err
	}
	return r.loadRecordWithAttributes(ctx, r.pool, tables, schemaID, rowID)
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

	// A group head is a predicate on the entity_main row, so a claim with
	// one anchors there whatever its condition tests (#653).
	useMainTableAsAnchor := query.Head != nil || hasMainTableCondition(query.Condition, cache)

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
	if query.Head != nil {
		head, headArgs, _, err := r.headPredicate(query.Tables, query.SchemaID, query.Head, 1+len(args))
		if err != nil {
			return nil, fmt.Errorf("build group head: %w", err)
		}
		conditions = "(" + conditions + ") AND " + head
		args = append(args, headArgs...)
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
