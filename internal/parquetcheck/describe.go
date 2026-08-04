package parquetcheck

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lychee-technology/forma/internal/sqlutil"
)

// DescribeRows is the row surface of a DESCRIBE result — satisfied by
// *sql.Rows and by the federated engine's duckDBRowsIterator. It deliberately
// omits Close: lifetime stays with the caller that issued the query, so this
// package never closes a handle it did not open.
type DescribeRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

// ScanDescribeColumns folds one DESCRIBE result into column name → DuckDB
// type. It is the single row-scan surface behind both footer probes: this
// package's *sql.DB probe (manifest schema stamping #256, compaction's
// pre-merge validation) and the federated read path's probe over its executor
// abstraction. The two used to be verbatim twins; the DESCRIBE row shape (6
// columns: name, type, null, key, default, extra) is now pinned in one place.
//
// uri is carried purely for error context — every returned error names the
// object being described, per the read-path consistency contract.
func ScanDescribeColumns(rows DescribeRows, uri string) (map[string]string, error) {
	cols := map[string]string{}
	for rows.Next() {
		var name, typ string
		var null, key, def, extra sql.NullString
		if err := rows.Scan(&name, &typ, &null, &key, &def, &extra); err != nil {
			return nil, fmt.Errorf("scan describe row for %s: %w", uri, err)
		}
		cols[name] = typ
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate describe rows for %s: %w", uri, err)
	}
	return cols, nil
}

// DescribeColumns reads one parquet object's footer schema through the given
// DuckDB session and returns column name → DuckDB type. It is the shared
// byte-truth probe behind manifest schema stamping (#256) and compaction's
// pre-merge validation; the federated read path issues its own query over its
// executor abstraction and shares the row scan via ScanDescribeColumns. The
// URI is escaped as a SQL literal, so callers may pass untrusted paths.
func DescribeColumns(ctx context.Context, db *sql.DB, uri string) (map[string]string, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s')", sqlutil.EscapeLiteral(uri)))
	if err != nil {
		return nil, fmt.Errorf("describe parquet %s: %w", uri, err)
	}
	defer func() { _ = rows.Close() }()

	return ScanDescribeColumns(rows, uri)
}
