package coordinationtest

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/factory"
	"github.com/lychee-technology/forma/internal"
	"github.com/lychee-technology/forma/internal/declaredindex"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/testdb"
	"github.com/lychee-technology/forma/internal/uniquekey"
	"github.com/stretchr/testify/require"
)

// The suite's environment: one table set per test, created from the DDL
// sources init-db uses, the schemas of testdata/schemas registered through
// the public factory, and a manager built by the public factory whose
// federated engine counts its calls. Every check of committed state reads
// the tables with direct SQL.

const schemaDir = "testdata/schemas"

// schemaIDs are the registry IDs of the suite's schemas.
var schemaIDs = map[string]int16{
	InstanceSchema: 601, TaskSchema: 602, IdempotencySchema: 603, LinkSchema: 604,
	OutboxSchema: 605, HistorySchema: 606, DefinitionSchema: 607,
}

// backend is the engine the suite runs against.
type backend struct {
	name string
	dsn  string
}

// activeBackend answers the backend FORMA_COORDINATION_DSN and
// FORMA_COORDINATION_BACKEND name, or the integration PostgreSQL that
// testdb resolves when no DSN is set.
func activeBackend() backend {
	if dsn := os.Getenv("FORMA_COORDINATION_DSN"); dsn != "" {
		return backend{name: os.Getenv("FORMA_COORDINATION_BACKEND"), dsn: dsn}
	}
	return backend{name: "postgres", dsn: testdb.ResolveDSN()}
}

func (b backend) postgres() bool { return b.name == "postgres" }

// requirePostgresMechanisms skips a mechanism assertion on a backend other
// than PostgreSQL: blocking seen in pg_locks, the non-blocking skip, and
// ErrPreconditionFailed rather than ErrRetryableConflict for every loser.
func requirePostgresMechanisms(t *testing.T) {
	t.Helper()
	if b := activeBackend(); !b.postgres() {
		t.Skipf("mechanism assertion: PostgreSQL only, the backend is %q", b.name)
	}
}

// tableSet names one test's tables.
type tableSet struct {
	schemaRegistry, entityMain, eavData, changeLog string
}

// env is one test's environment.
type env struct {
	ctx     context.Context
	pool    *pgxpool.Pool
	app     string
	tables  tableSet
	em      forma.EntityManager
	store   *Store
	engine  *countingEngine
	columns map[string]map[string]attrColumn
	// registry is the schema registry the manager was built with.
	registry forma.SchemaRegistry
	// tracer sees every statement sent through pool, recording while on.
	tracer *statementTracer
}

var envSerial atomic.Int64

// newEnv builds a test's environment over a pool of the active backend.
func newEnv(t *testing.T) *env {
	t.Helper()
	return newEnvOn(t, nil)
}

// newEnvOn is newEnv whose manager reaches the database through managerPool
// when it is not nil, the pool a fault proxy sits in front of; the tables
// and every direct check use the env's own pool.
func newEnvOn(t *testing.T, managerPool func(*env) *pgxpool.Pool) *env {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	b := activeBackend()
	if b.name == "" {
		t.Fatal("FORMA_COORDINATION_DSN is set without FORMA_COORDINATION_BACKEND")
	}
	if b.postgres() {
		testdb.Connect(t, ctx) // skips, or fails in CI, when PostgreSQL is absent
	}
	serial := fmt.Sprintf("coord_%d_%d", time.Now().UnixNano(), envSerial.Add(1))
	e := &env{ctx: ctx, app: serial, engine: &countingEngine{}, tables: tableSet{
		schemaRegistry: serial + "_schema_registry", entityMain: serial + "_entity_main",
		eavData: serial + "_eav_data", changeLog: serial + "_change_log",
	}, tracer: &statementTracer{}}
	e.pool = e.openPool(t, b.dsn)
	e.createTables(t)
	e.columns = loadColumns(t)
	pool := e.pool
	if managerPool != nil {
		pool = managerPool(e)
	}
	e.em = e.buildManager(t, pool)
	tr, err := factory.NewTransactor(e.em)
	require.NoError(t, err)
	e.store = NewStore(tr)
	t.Cleanup(func() {
		if n := e.engine.calls.Load(); n != 0 {
			t.Errorf("the federated engine was called %d times: no coordination path may reach it", n)
		}
	})
	return e
}

// openPool opens a pool on dsn wide enough for every worker of the
// concurrency tests and a pg_locks poller. Its backends carry the env's
// application name, so a pg_locks poll sees only this test's sessions.
func (e *env) openPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	cfg.MaxConns = 40
	cfg.ConnConfig.RuntimeParams["application_name"] = e.app
	cfg.ConnConfig.Tracer = e.tracer
	pool, err := pgxpool.NewWithConfig(e.ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

// createTables creates the test's tables from init-db's sources: the
// entity_main columns Forma reads (checked against model's column contract),
// the unique-key tables (#651) and the schema registry rows. The declared
// indexes (#647) and the keys' build follow once the registry is loaded.
// The EAV and change_log tables and init-db's default indexes are copies of
// cmd/tools/init_db.go, a main package no test can import.
func (e *env) createTables(t *testing.T) {
	t.Helper()
	main := entityMainDDL(e.tables.entityMain)
	require.Empty(t, model.EntityMainDDLDrift(main), "the suite's entity_main matches Forma's column contract")
	statements := append([]string{
		fmt.Sprintf("CREATE TABLE %s (schema_name TEXT PRIMARY KEY, schema_id SMALLINT UNIQUE NOT NULL)", e.tables.schemaRegistry),
		main,
		fmt.Sprintf(`CREATE TABLE %s (schema_id SMALLINT NOT NULL, row_id UUID NOT NULL, attr_id SMALLINT NOT NULL,
			array_indices TEXT NOT NULL DEFAULT '', value_text TEXT, value_numeric NUMERIC,
			PRIMARY KEY (schema_id, row_id, attr_id, array_indices))`, e.tables.eavData),
		fmt.Sprintf(`CREATE TABLE %s (schema_id SMALLINT NOT NULL, row_id UUID NOT NULL, flushed_at BIGINT NOT NULL DEFAULT 0,
			changed_at BIGINT NOT NULL, deleted_at BIGINT, PRIMARY KEY (schema_id, row_id, flushed_at))`, e.tables.changeLog),
	}, uniquekey.StatementsFor(e.tables.entityMain)...)
	statements = append(statements, initDBIndexes(e.tables)...)
	t.Cleanup(e.dropTables)
	for _, stmt := range statements {
		_, err := e.pool.Exec(e.ctx, stmt)
		require.NoError(t, err, "create the suite's tables")
	}
	for name, id := range schemaIDs {
		_, err := e.pool.Exec(e.ctx, fmt.Sprintf("INSERT INTO %s (schema_name, schema_id) VALUES ($1, $2)", e.tables.schemaRegistry), name, id)
		require.NoError(t, err)
	}
}

func (e *env) dropTables() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, table := range []string{e.tables.schemaRegistry, e.tables.entityMain, e.tables.eavData, e.tables.changeLog,
		uniquekey.KeyTable(e.tables.entityMain), uniquekey.StateTable(e.tables.entityMain)} {
		_, _ = e.pool.Exec(ctx, "DROP TABLE IF EXISTS "+table)
	}
}

// analyze gathers statistics on the suite's tables, as autovacuum would
// have on a deployment's. Without them PostgreSQL plans a claim's
// candidate select with nested loops whose cost grows with the cube of the
// live outbox (#685).
func (e *env) analyze(t *testing.T) {
	t.Helper()
	require.NoError(t, e.analyzeTables())
}

func (e *env) analyzeTables() error {
	for _, table := range []string{e.tables.entityMain, e.tables.eavData, e.tables.changeLog} {
		if _, err := e.pool.Exec(e.ctx, "ANALYZE "+table); err != nil {
			return fmt.Errorf("analyze %s: %w", table, err)
		}
	}
	return nil
}

// initDBIndexes are the indexes init-db creates on every deployment's EAV
// table and entity_main (eavIndexDDL and mainColumnIndexDDL), so that a plan
// the suite asserts is chosen among the indexes production has.
func initDBIndexes(tables tableSet) []string {
	statements := []string{
		fmt.Sprintf("CREATE INDEX %s_numeric_idx ON %s (schema_id, attr_id, value_numeric, row_id) WHERE value_numeric IS NOT NULL",
			tables.eavData, tables.eavData),
		fmt.Sprintf("CREATE INDEX %s_text_idx ON %s (schema_id, attr_id, value_text, row_id) WHERE value_text IS NOT NULL",
			tables.eavData, tables.eavData),
	}
	for _, col := range []string{"text_01", "text_02", "text_03", "smallint_01", "integer_01", "bigint_01", "bigint_02", "double_01", "double_02", "uuid_01"} {
		statements = append(statements, fmt.Sprintf("CREATE INDEX %s_%s_idx ON %s (ltbase_schema_id, ltbase_row_id, %s)",
			tables.entityMain, col, tables.entityMain, col))
	}
	return statements
}

// entityMainDDL is entity_main with every column Forma reads, one per line,
// NOT NULL where init-db's is.
func entityMainDDL(table string) string {
	notNull := map[string]bool{"ltbase_schema_id": true, "ltbase_row_id": true, "ltbase_created_at": true, "ltbase_updated_at": true}
	sqlType := map[model.ColumnKind]string{
		model.ColumnKindText: "TEXT", model.ColumnKindSmallint: "SMALLINT", model.ColumnKindInteger: "INTEGER",
		model.ColumnKindBigint: "BIGINT", model.ColumnKindDouble: "DOUBLE PRECISION", model.ColumnKindUUID: "UUID",
	}
	lines := make([]string, 0, len(model.EntityMainColumnDescriptors)+1)
	for _, c := range model.EntityMainColumnDescriptors {
		line := "\t" + c.Name + " " + sqlType[c.Kind]
		if notNull[c.Name] {
			line += " NOT NULL"
		}
		lines = append(lines, line)
	}
	lines = append(lines, "\tPRIMARY KEY (ltbase_schema_id, ltbase_row_id)")
	return fmt.Sprintf("CREATE TABLE %s (\n%s\n)", table, strings.Join(lines, ",\n"))
}

// buildManager registers testdata/schemas through the public factory,
// builds the keys and declared indexes init-db would, and builds the
// manager with the public factory, its federated engine wrapped to count.
func (e *env) buildManager(t *testing.T, pool *pgxpool.Pool) forma.EntityManager {
	t.Helper()
	registry, err := factory.NewFileSchemaRegistryContext(e.ctx, e.pool, e.tables.schemaRegistry, schemaDir)
	require.NoError(t, err)
	e.registry = registry
	catalog, err := internal.NewUniqueKeyCatalog(registry)
	require.NoError(t, err)
	_, pending, err := catalog.BuildUniqueKeysOfEmptySchemas(e.ctx, e.pool,
		model.StorageTables{EntityMain: e.tables.entityMain, EAVData: e.tables.eavData, ChangeLog: e.tables.changeLog})
	require.NoError(t, err)
	require.Empty(t, pending, "every unique key of the empty schemas is built")
	_, err = declaredindex.EnsureFromRegistry(e.ctx, e.pool, e.tables.entityMain, registry)
	require.NoError(t, err)

	config := &forma.Config{
		Database: forma.DatabaseConfig{TableNames: forma.TableNames{SchemaRegistry: e.tables.schemaRegistry,
			EntityMain: e.tables.entityMain, EAVData: e.tables.eavData, ChangeLog: e.tables.changeLog}},
		Query:          forma.QueryConfig{DefaultPageSize: 20, MaxPageSize: 100},
		Entity:         forma.EntityConfig{SchemaDirectory: schemaDir},
		Transaction:    forma.TransactionConfig{DefaultTimeout: time.Minute},
		SchemaRegistry: registry,
	}
	ctx := internal.WithFederatedEngineWrapper(e.ctx, e.engine.wrap)
	em, err := factory.NewEntityManagerWithConfigContext(ctx, config, pool)
	require.NoError(t, err)
	t.Cleanup(func() { _ = em.Close() })
	return em
}

// countingEngine is the manager's federated engine, wrapped to count its
// calls before it delegates.
type countingEngine struct {
	inner model.FederatedQueryEngine
	calls atomic.Int64
}

func (c *countingEngine) wrap(inner model.FederatedQueryEngine) model.FederatedQueryEngine {
	c.inner = inner
	return c
}

func (c *countingEngine) Query(ctx context.Context, tables model.StorageTables, fq *model.FederatedAttributeQuery, opts *model.FederatedQueryOptions) (*model.PersistentRecordPage, error) {
	c.calls.Add(1)
	page, err := c.inner.Query(ctx, tables, fq, opts)
	if err != nil {
		return nil, fmt.Errorf("federated query: %w", err)
	}
	return page, nil
}

// inTx runs fn through the adapter's InTx on the env's context.
func (e *env) inTx(fn func(UnitOfWork) error) error {
	return e.store.InTx(e.ctx, fn)
}
