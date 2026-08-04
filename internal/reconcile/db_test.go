package reconcile

import (
	"context"
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/duckdb/duckdb-go/v2"
)

func openRegistryDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec(`CREATE TABLE schema_registry (schema_id SMALLINT, definition VARCHAR)`); err != nil {
		t.Fatalf("create registry: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO schema_registry VALUES (2, 'b'), (1, 'a'), (9, 'c')`); err != nil {
		t.Fatalf("seed registry: %v", err)
	}
	return db
}

func TestRegistrySchemaEnumerator_AllOrdered(t *testing.T) {
	enum := &RegistrySchemaEnumerator{DB: openRegistryDB(t), Table: "schema_registry"}
	ids, err := enum.SchemaIDs(context.Background())
	if err != nil {
		t.Fatalf("SchemaIDs: %v", err)
	}
	if len(ids) != 3 || ids[0] != 1 || ids[1] != 2 || ids[2] != 9 {
		t.Fatalf("SchemaIDs = %v, want [1 2 9]", ids)
	}
}

func TestRegistrySchemaEnumerator_Filter(t *testing.T) {
	enum := &RegistrySchemaEnumerator{DB: openRegistryDB(t), Table: "schema_registry", SchemaIDFilter: 2}
	ids, err := enum.SchemaIDs(context.Background())
	if err != nil {
		t.Fatalf("SchemaIDs: %v", err)
	}
	if len(ids) != 1 || ids[0] != 2 {
		t.Fatalf("SchemaIDs = %v, want [2]", ids)
	}
}

func TestRegistrySchemaEnumerator_FilterMissingSchemaErrors(t *testing.T) {
	enum := &RegistrySchemaEnumerator{DB: openRegistryDB(t), Table: "schema_registry", SchemaIDFilter: 42}
	_, err := enum.SchemaIDs(context.Background())
	if err == nil {
		t.Fatal("an unregistered --schema-id must error, not report an empty (clean) run")
	}
}

func TestPGLiveRows_LiveRowCount(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM "entity_main" WHERE ltbase_schema_id = \$1 AND ltbase_deleted_at IS NULL`).
		WithArgs(int16(7)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(42)))

	p := &PGLiveRows{DB: db, Table: "entity_main"}
	n, err := p.LiveRowCount(context.Background(), 7)
	if err != nil {
		t.Fatalf("LiveRowCount: %v", err)
	}
	if n != 42 {
		t.Fatalf("count = %d, want 42", n)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("mock expectations not met: %v", err)
	}
}
