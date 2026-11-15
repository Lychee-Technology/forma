package testdb

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/textcollation"
)

// OpenDSQLPool opens a pool on the target ResolveDSQL names: a cluster, or
// MiniStack's strict proxy. Without a target it opens the integration
// PostgreSQL, and skips (or fails in CI) where that is absent. The readers
// these pools serve open their own REPEATABLE READ snapshots, so no session
// default is set.
func OpenDSQLPool(t testing.TB, ctx context.Context) (*pgxpool.Pool, DSQLKind) {
	t.Helper()
	dsn, kind := ResolveDSQL(t)
	if kind == DSQLEmulated {
		return Connect(t, ctx), kind
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to %s: %v", DSQLDSNEnv, err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("connect to %s: %v", DSQLDSNEnv, err)
	}
	return pool, kind
}

// CreateDSQLStorageTables creates a change_log, an entity_main and an
// eav_data of init-db's shape, named after prefix and the clock, and drops
// them when t ends. The DDL is what DSQL runs: one CREATE TABLE a
// statement, each keyed by its primary key alone, and text without the
// COLLATE clause DSQL refuses (#816). init-db's secondary indexes are left
// out: on DSQL they need CREATE INDEX ASYNC, and no reader depends on them
// for its answer.
func CreateDSQLStorageTables(t testing.TB, ctx context.Context, pool *pgxpool.Pool, prefix string) model.StorageTables {
	t.Helper()
	suffix := time.Now().UnixNano()
	tables := model.StorageTables{
		ChangeLog:  fmt.Sprintf("%s_change_log_%d", prefix, suffix),
		EntityMain: fmt.Sprintf("%s_entity_main_%d", prefix, suffix),
		EAVData:    fmt.Sprintf("%s_eav_data_%d", prefix, suffix),
	}
	statements := []string{
		fmt.Sprintf(`CREATE TABLE %s (schema_id SMALLINT NOT NULL, row_id UUID NOT NULL, flushed_at BIGINT NOT NULL DEFAULT 0,
			changed_at BIGINT NOT NULL, deleted_at BIGINT, PRIMARY KEY (schema_id, row_id, flushed_at))`, quote(tables.ChangeLog)),
		dsqlEntityMainDDL(tables.EntityMain),
		fmt.Sprintf(`CREATE TABLE %s (schema_id SMALLINT NOT NULL, row_id UUID NOT NULL, attr_id SMALLINT NOT NULL,
			array_indices TEXT NOT NULL DEFAULT '', value_text TEXT, value_numeric NUMERIC,
			PRIMARY KEY (schema_id, row_id, attr_id, array_indices))`, quote(tables.EAVData)),
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		for _, table := range []string{tables.ChangeLog, tables.EntityMain, tables.EAVData} {
			_, _ = pool.Exec(cleanup, "DROP TABLE IF EXISTS "+quote(table))
		}
	})
	for _, stmt := range statements {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("create a storage table with DSQL's DDL: %v\n%s", err, stmt)
		}
	}
	return tables
}

// dsqlEntityMainDDL is entity_main with every column Forma reads, NOT NULL
// where init-db's is.
func dsqlEntityMainDDL(table string) string {
	notNull := map[string]bool{"ltbase_schema_id": true, "ltbase_row_id": true, "ltbase_created_at": true, "ltbase_updated_at": true}
	columns := make([]string, 0, len(model.EntityMainColumnDescriptors)+1)
	for _, c := range model.EntityMainColumnDescriptors {
		column := "\t" + c.Name + " " + model.ColumnKindSQLType(c.Kind)
		if notNull[c.Name] {
			column += " NOT NULL"
		}
		columns = append(columns, column)
	}
	columns = append(columns, "\tPRIMARY KEY (ltbase_schema_id, ltbase_row_id)")
	ddl := fmt.Sprintf("CREATE TABLE %s (\n%s\n)", quote(table), strings.Join(columns, ",\n"))
	return textcollation.ForEngine(forma.EngineAuroraDSQL, ddl)
}

func quote(table string) string { return pgx.Identifier{table}.Sanitize() }
