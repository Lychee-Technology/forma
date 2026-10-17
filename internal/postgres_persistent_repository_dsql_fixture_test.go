package internal

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/rowguard"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/testdb"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// The aurora-dsql engine's test target (#633). By default it is the
// integration PostgreSQL with every session at REPEATABLE READ, DSQL's one
// isolation level, which CI runs. FORMA_DSQL_DSN names an Aurora DSQL
// cluster instead (docs/aurora-dsql.md says how to build one), and the same
// tests run there.
type dsqlTarget struct {
	pool *pgxpool.Pool
	// app names the emulation's sessions, so lockWaiters sees only this
	// test's.
	app string
	// cluster says pool reaches an Aurora DSQL cluster.
	cluster bool
}

// openDSQLTarget opens a pool on the test target of the aurora-dsql engine.
func openDSQLTarget(t *testing.T, ctx context.Context) dsqlTarget {
	t.Helper()
	if dsn := os.Getenv("FORMA_DSQL_DSN"); dsn != "" {
		pool, err := pgxpool.New(ctx, dsn)
		require.NoError(t, err, "connect to FORMA_DSQL_DSN")
		t.Cleanup(pool.Close)
		return dsqlTarget{pool: pool, cluster: true}
	}
	testdb.Connect(t, ctx) // skips, or fails in CI, when PostgreSQL is absent
	app := fmt.Sprintf("dsql_mode_%d", time.Now().UnixNano())
	pool := openTunedPool(t, ctx, func(cfg *pgxpool.Config) {
		cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = "repeatable read"
		cfg.ConnConfig.RuntimeParams["application_name"] = app
	})
	return dsqlTarget{pool: pool, app: app}
}

// createDSQLTables creates the tables the aurora-dsql engine writes, with
// DDL DSQL runs: one CREATE TABLE a transaction, keyed by its primary key
// alone. The unique-key tables are left out, since their row index needs
// CREATE INDEX ASYNC there and these tests declare no key.
func (d dsqlTarget) createDSQLTables(t *testing.T, ctx context.Context) model.StorageTables {
	t.Helper()
	suffix := time.Now().UnixNano()
	tables := model.StorageTables{
		EntityMain: fmt.Sprintf("entity_main_dsql_%d", suffix),
		EAVData:    fmt.Sprintf("eav_data_dsql_%d", suffix),
		ChangeLog:  fmt.Sprintf("change_log_dsql_%d", suffix),
	}
	guards := rowguard.Table(tables.EntityMain)
	main := dsqlEntityMainDDL(tables.EntityMain)
	require.Empty(t, model.EntityMainDDLDrift(main), "entity_main matches Forma's column contract")
	statements := []string{
		main,
		fmt.Sprintf(`CREATE TABLE %s (schema_id SMALLINT NOT NULL, row_id UUID NOT NULL, attr_id SMALLINT NOT NULL,
			array_indices TEXT NOT NULL DEFAULT '', value_text TEXT, value_numeric DOUBLE PRECISION,
			PRIMARY KEY (schema_id, row_id, attr_id, array_indices))`, sanitizeIdentifier(tables.EAVData)),
		fmt.Sprintf(`CREATE TABLE %s (schema_id SMALLINT NOT NULL, row_id UUID NOT NULL, flushed_at BIGINT NOT NULL DEFAULT 0,
			changed_at BIGINT NOT NULL, deleted_at BIGINT, PRIMARY KEY (schema_id, row_id, flushed_at))`, sanitizeIdentifier(tables.ChangeLog)),
		rowguard.Statement(sanitizeIdentifier(guards)),
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, table := range []string{tables.EntityMain, tables.EAVData, tables.ChangeLog, guards} {
			_, _ = d.pool.Exec(cleanupCtx, "DROP TABLE IF EXISTS "+sanitizeIdentifier(table))
		}
	})
	for _, stmt := range statements {
		_, err := d.pool.Exec(ctx, stmt)
		require.NoError(t, err, "create the aurora-dsql engine's tables")
	}
	return tables
}

// dsqlEntityMainDDL is entity_main with every column Forma reads, one per
// line, NOT NULL where init-db's is.
func dsqlEntityMainDDL(table string) string {
	notNull := map[string]bool{"ltbase_schema_id": true, "ltbase_row_id": true, "ltbase_created_at": true, "ltbase_updated_at": true}
	sqlType := map[model.ColumnKind]string{
		model.ColumnKindText: "TEXT", model.ColumnKindSmallint: "SMALLINT", model.ColumnKindInteger: "INTEGER",
		model.ColumnKindBigint: "BIGINT", model.ColumnKindDouble: "DOUBLE PRECISION", model.ColumnKindUUID: "UUID",
	}
	columns := make([]string, 0, len(model.EntityMainColumnDescriptors)+1)
	for _, c := range model.EntityMainColumnDescriptors {
		column := "\t" + c.Name + " " + sqlType[c.Kind]
		if notNull[c.Name] {
			column += " NOT NULL"
		}
		columns = append(columns, column)
	}
	columns = append(columns, "\tPRIMARY KEY (ltbase_schema_id, ltbase_row_id)")
	return fmt.Sprintf("CREATE TABLE %s (\n%s\n)", sanitizeIdentifier(table), strings.Join(columns, ",\n"))
}

// newDSQLModeRepo answers a repository on the aurora-dsql engine.
func newDSQLModeRepo(t *testing.T, pool persistentRecordPool, mc *schemameta.MetadataCache) *DBPersistentRecordRepository {
	t.Helper()
	engine, err := WithDatabaseEngine(forma.EngineAuroraDSQL)
	require.NoError(t, err)
	return NewDBPersistentRecordRepository(pool, mc, engine)
}

// waitForLockWaiter blocks until one of the emulation's sessions waits on a
// row lock: a writer that reached a guard row another transaction wrote.
func (d dsqlTarget) waitForLockWaiter(ctx context.Context, t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var waiting bool
		require.NoError(t, d.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity
			WHERE application_name = $1 AND wait_event_type = 'Lock')`, d.app).Scan(&waiting))
		if waiting {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no %s session started waiting on a lock", d.app)
}

// guardGeneration answers how many committed transactions wrote rowID's
// guard row.
func (d dsqlTarget) guardGeneration(ctx context.Context, t *testing.T, tables model.StorageTables, rowID uuid.UUID) int64 {
	t.Helper()
	var generation int64
	require.NoError(t, d.pool.QueryRow(ctx, fmt.Sprintf("SELECT generation FROM %s WHERE ltbase_schema_id = 1 AND ltbase_row_id = $1",
		sanitizeIdentifier(rowguard.Table(tables.EntityMain))), rowID).Scan(&generation))
	return generation
}

// liveVersion answers rowID's hot, live change_log stamp.
func (d dsqlTarget) liveVersion(ctx context.Context, t *testing.T, tables model.StorageTables, rowID uuid.UUID) int64 {
	t.Helper()
	var live int64
	require.NoError(t, d.pool.QueryRow(ctx, fmt.Sprintf(
		"SELECT changed_at FROM %s WHERE schema_id = 1 AND row_id = $1 AND flushed_at = 0 AND deleted_at IS NULL",
		sanitizeIdentifier(tables.ChangeLog)), rowID).Scan(&live))
	return live
}
