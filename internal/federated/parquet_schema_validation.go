package federated

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/lychee-technology/forma/internal/parquetcheck"
)

// parquetSchemaValidator enforces the parquetcheck system-column invariant on
// every scanned parquet path before the main scan runs. It exists because the
// read path's union_by_name (#189) NULL-fills absent columns: a wrong-schema
// object's rows would get NULL row_id, silently drop out of the dirty
// anti-join, and the query would succeed while ignoring the file — flipping
// #187's loud corruption contract into silent data loss. The validator
// restores the loud failure for schema violations while leaving byte
// corruption (unreadable footer) to the execution-path classifier.
//
// Paths that pass are cached forever: parquet objects are write-once (flush,
// init and compaction always mint new keys), so a validated path never
// changes and steady-state cost is one footer probe per new object. Glob
// paths are expanded per query (the match set changes as objects land) and
// their concrete matches hit the same cache.
type parquetSchemaValidator struct {
	mu    sync.Mutex
	valid map[string]struct{}
}

func newParquetSchemaValidator() *parquetSchemaValidator {
	return &parquetSchemaValidator{valid: map[string]struct{}{}}
}

// Validate probes each scanned path's parquet schema and fails on a system
// column that is missing or mistyped. Globs (explicit S3ParquetPathTemplate
// hints and the legacy manifest fallback) are expanded to their concrete
// matches first — an unexpanded glob would let a malformed matched file
// bypass the invariant and vanish silently under union_by_name. Unreadable
// footers and failed glob listings are inconclusive — byte corruption or
// storage failure, not schema drift — so the main read keeps producing
// today's classified execution failure (#187 CorruptBytes/Truncated).
func (v *parquetSchemaValidator) Validate(ctx context.Context, duck DuckDBQueryExecutor, paths []string) error {
	if v == nil || duck == nil {
		return nil
	}
	for _, path := range paths {
		if strings.ContainsAny(path, `'";`) {
			// Cannot embed in a DuckDB literal; the main read fails on it
			// with its own classification either way.
			continue
		}
		if strings.ContainsAny(path, "*?[") {
			expanded, err := globParquetPaths(ctx, duck, path)
			if err != nil {
				continue // inconclusive: defer to the execution-path classifier
			}
			if err := v.validateConcrete(ctx, duck, expanded); err != nil {
				return err
			}
			continue
		}
		if err := v.validateConcrete(ctx, duck, []string{path}); err != nil {
			return err
		}
	}
	return nil
}

// validateConcrete checks concrete (non-glob) paths against the invariant,
// consulting and feeding the write-once cache.
func (v *parquetSchemaValidator) validateConcrete(ctx context.Context, duck DuckDBQueryExecutor, paths []string) error {
	for _, path := range paths {
		if strings.ContainsAny(path, `'";`) || v.isValidated(path) {
			continue
		}
		cols, err := describeParquetColumns(ctx, duck, path)
		if err != nil {
			continue // inconclusive: defer to the execution-path classifier
		}
		if err := parquetcheck.Check(path, cols); err != nil {
			return fmt.Errorf("%w: %w", ErrFederatedReadFailed, err)
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

// globParquetPaths expands one glob pattern to its concrete matches via
// DuckDB's glob() table function (httpfs LIST under s3://).
func globParquetPaths(ctx context.Context, duck DuckDBQueryExecutor, pattern string) ([]string, error) {
	rows, err := duck.Query(ctx, fmt.Sprintf("SELECT file FROM glob('%s')", pattern))
	if err != nil {
		return nil, fmt.Errorf("expand parquet glob %s: %w", pattern, err)
	}
	defer func() { _ = rows.Close() }()

	var paths []string
	for rows.Next() {
		var file string
		if err := rows.Scan(&file); err != nil {
			return nil, fmt.Errorf("scan glob match for %s: %w", pattern, err)
		}
		paths = append(paths, file)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate glob matches for %s: %w", pattern, err)
	}
	return paths, nil
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
