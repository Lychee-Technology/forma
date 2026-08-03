package parquetcheck

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lychee-technology/forma/internal/sqlutil"
)

// DescribeColumns reads one parquet object's footer schema through the given
// DuckDB session and returns column name → DuckDB type. It is the shared
// byte-truth probe behind manifest schema stamping (#256) and compaction's
// pre-merge validation; the federated read path keeps its own copy over its
// executor abstraction. The URI is escaped as a SQL literal, so callers may
// pass untrusted paths.
func DescribeColumns(ctx context.Context, db *sql.DB, uri string) (map[string]string, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s')", sqlutil.EscapeLiteral(uri)))
	if err != nil {
		return nil, fmt.Errorf("describe parquet %s: %w", uri, err)
	}
	defer func() { _ = rows.Close() }()

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
