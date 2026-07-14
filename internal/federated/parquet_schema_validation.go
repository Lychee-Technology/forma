package federated

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
)

// parquetSystemColumns is the schema invariant every generation of
// Forma-written parquet satisfies regardless of attribute evolution: the
// three system columns every federated projection touches, with the exact
// DuckDB types both exporters emit (delta flush and init/compaction base).
// Attribute columns may come and go across schema generations (#189) — these
// may not. Ordered so violation errors are deterministic, row_id first.
var parquetSystemColumns = []struct{ name, duckType string }{
	{"row_id", "UUID"},
	{"changed_at", "BIGINT"},
	{"deleted_at", "BIGINT"},
}

// parquetSchemaValidator enforces the system-column invariant on every
// concrete parquet path before the main scan runs. It exists because the
// read path's union_by_name (#189) NULL-fills absent columns: a
// wrong-schema object's rows would get NULL row_id, silently drop out of
// the dirty anti-join, and the query would succeed while ignoring the file
// — flipping #187's loud corruption contract into silent data loss. The
// validator restores the loud failure for schema violations while leaving
// byte corruption (unreadable footer) to the execution-path classifier.
//
// Paths that pass are cached forever: parquet objects are write-once
// (flush, init and compaction always mint new keys), so a validated path
// never changes and steady-state cost is one footer probe per new object.
type parquetSchemaValidator struct {
	mu    sync.Mutex
	valid map[string]struct{}
}

func newParquetSchemaValidator() *parquetSchemaValidator {
	return &parquetSchemaValidator{valid: map[string]struct{}{}}
}

// Validate probes each concrete path's parquet schema and fails on a system
// column that is missing or mistyped. Glob paths are skipped (unprovable
// per-file, mirroring ParquetSource.MissingIn), and an unreadable footer is
// inconclusive — byte corruption or storage failure, not schema drift — so
// the main read keeps producing today's classified execution failure (#187
// CorruptBytes/Truncated).
func (v *parquetSchemaValidator) Validate(ctx context.Context, duck DuckDBQueryExecutor, paths []string) error {
	if v == nil || duck == nil {
		return nil
	}
	for _, path := range paths {
		if strings.ContainsAny(path, "*?[") || strings.ContainsAny(path, `'";`) {
			// Globs cannot be probed per-file; quote-bearing paths cannot be
			// embedded in a DuckDB literal — the main read fails on them
			// with its own classification either way.
			continue
		}
		if v.isValidated(path) {
			continue
		}
		cols, err := describeParquetColumns(ctx, duck, path)
		if err != nil {
			continue // inconclusive: defer to the execution-path classifier
		}
		if err := checkParquetSystemColumns(path, cols); err != nil {
			return err
		}
		v.markValidated(path)
	}
	return nil
}

func (v *parquetSchemaValidator) isValidated(path string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	_, ok := v.valid[path]
	return ok
}

func (v *parquetSchemaValidator) markValidated(path string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.valid[path] = struct{}{}
}

// describeParquetColumns reads one parquet object's footer schema and
// returns column name → DuckDB type.
func describeParquetColumns(ctx context.Context, duck DuckDBQueryExecutor, path string) (map[string]string, error) {
	rows, err := duck.Query(ctx, fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s')", path))
	if err != nil {
		return nil, fmt.Errorf("describe parquet %s: %w", path, err)
	}
	defer func() { _ = rows.Close() }()

	cols := map[string]string{}
	for rows.Next() {
		var name, typ string
		var null, key, def, extra sql.NullString
		if err := rows.Scan(&name, &typ, &null, &key, &def, &extra); err != nil {
			return nil, fmt.Errorf("scan describe row for %s: %w", path, err)
		}
		cols[name] = typ
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate describe rows for %s: %w", path, err)
	}
	return cols, nil
}

// checkParquetSystemColumns verifies the invariant on one probed schema.
func checkParquetSystemColumns(path string, cols map[string]string) error {
	for _, sys := range parquetSystemColumns {
		got, ok := cols[sys.name]
		if !ok {
			return fmt.Errorf(
				"parquet object %s violates the export schema invariant: system column %q (%s) is missing — the object was not written by a Forma exporter: %w",
				path, sys.name, sys.duckType, ErrFederatedReadFailed)
		}
		if got != sys.duckType {
			return fmt.Errorf(
				"parquet object %s violates the export schema invariant: system column %q has type %s, want %s: %w",
				path, sys.name, got, sys.duckType, ErrFederatedReadFailed)
		}
	}
	return nil
}
