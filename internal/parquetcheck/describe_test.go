package parquetcheck_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/lychee-technology/forma/internal/parquetcheck"
)

func TestDescribeColumnsReadsFooter(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	path := filepath.Join(t.TempDir(), "f.parquet")
	copySQL := fmt.Sprintf(
		`COPY (SELECT CAST('018f0000-0000-7000-8000-000000000001' AS UUID) AS row_id,
		              CAST(1 AS BIGINT) AS changed_at,
		              CAST(NULL AS BIGINT) AS deleted_at,
		              CAST(50 AS BIGINT) AS ltbase_created_at,
		              'x' AS attr_name) TO '%s' (FORMAT PARQUET)`, path)
	if _, err := db.ExecContext(context.Background(), copySQL); err != nil {
		t.Fatalf("write fixture parquet: %v", err)
	}
	cols, err := parquetcheck.DescribeColumns(context.Background(), db, path)
	if err != nil {
		t.Fatalf("DescribeColumns: %v", err)
	}
	want := map[string]string{"row_id": "UUID", "changed_at": "BIGINT", "deleted_at": "BIGINT", "ltbase_created_at": "BIGINT", "attr_name": "VARCHAR"}
	for name, typ := range want {
		if cols[name] != typ {
			t.Fatalf("column %s = %q, want %q (all: %#v)", name, cols[name], typ, cols)
		}
	}
	if err := parquetcheck.Check(path, cols); err != nil {
		t.Fatalf("described columns must satisfy the invariant: %v", err)
	}
}

func TestDescribeColumnsMissingFile(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()
	if _, err := parquetcheck.DescribeColumns(context.Background(), db, filepath.Join(t.TempDir(), "absent.parquet")); err == nil {
		t.Fatal("expected error for a missing object")
	}
}
