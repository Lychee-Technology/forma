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
//
// The cache stores each validated path's columns so a repeat query can
// contribute them to the column union (#255) without a second probe. It has
// two feeders, trusted equally once validated: manifest column stamps written
// by the exporters (#256) and footer probes. Both are checked against the same
// parquetcheck invariant before they land, and a stamp records the same
// name → DuckDB type map a DESCRIBE would have produced, so a cache hit cannot
// tell — and need not tell — which feeder filled it.
type parquetSchemaValidator struct {
	mu    sync.Mutex
	valid map[string]map[string]string
}

func newParquetSchemaValidator() *parquetSchemaValidator {
	return &parquetSchemaValidator{valid: map[string]map[string]string{}}
}

// Validate probes each scanned path's parquet schema and fails on a system
// column that is missing or mistyped. Globs (explicit S3ParquetPathTemplate
// hints and the legacy manifest fallback) are expanded to their concrete
// matches first — an unexpanded glob would let a malformed matched file
// bypass the invariant and vanish silently under union_by_name. Unreadable
// footers and failed glob listings are inconclusive — byte corruption or
// storage failure, not schema drift — so the main read keeps producing
// today's classified execution failure (#187 CorruptBytes/Truncated).
//
// It also returns the union of the probed footers' columns (name → DuckDB
// type) and whether that union is complete. The union feeds #255 NULL
// augmentation for columns that no scanned parquet generation carries yet;
// complete is true only when every path in the set contributed its columns, so
// an incomplete union must not drive augmentation — augmenting a column that
// an unprobed file actually carries would collide with the real column.
//
// stamps carries manifest-recorded column maps keyed by the exact path strings
// in paths (#256); it may be nil. A stamp that satisfies the invariant spares
// that path its probe. A stamp that does not is ignored and the path is probed
// as usual: a stamp may only short-circuit success, never author a failure, so
// a stale or corrupt manifest can cost a probe but can never fail a healthy
// object.
func (v *parquetSchemaValidator) Validate(
	ctx context.Context, duck DuckDBQueryExecutor, paths []string,
	stamps map[string]map[string]string,
) (map[string]string, bool, error) {
	if v == nil || duck == nil {
		return nil, false, nil
	}
	union, complete := map[string]string{}, true
	for _, path := range paths {
		if strings.ContainsAny(path, `'";`) {
			// Cannot embed in a DuckDB literal; the main read fails on it
			// with its own classification either way.
			complete = false
			continue
		}
		if strings.ContainsAny(path, "*?[") {
			expanded, err := globParquetPaths(ctx, duck, path)
			if err != nil {
				complete = false // inconclusive: defer to the execution-path classifier
				continue
			}
			ok, err := v.validateConcrete(ctx, duck, expanded, union, stamps)
			if err != nil {
				return nil, false, err
			}
			complete = complete && ok
			continue
		}
		ok, err := v.validateConcrete(ctx, duck, []string{path}, union, stamps)
		if err != nil {
			return nil, false, err
		}
		complete = complete && ok
	}
	return union, complete, nil
}

// validateConcrete checks concrete (non-glob) paths against the invariant,
// consulting and feeding the write-once cache from either feeder — a passing
// manifest stamp (#256) or a footer probe. It merges every contributing column
// map into union and reports whether all of the given paths contributed.
func (v *parquetSchemaValidator) validateConcrete(
	ctx context.Context, duck DuckDBQueryExecutor, paths []string, union map[string]string,
	stamps map[string]map[string]string,
) (bool, error) {
	complete := true
	for _, path := range paths {
		if strings.ContainsAny(path, `'";`) {
			complete = false
			continue
		}
		if cols, ok := v.validatedCols(path); ok {
			mergeColumnUnion(union, cols)
			continue
		}
		if stamp, ok := stamps[path]; ok && len(stamp) > 0 {
			if parquetcheck.Check(path, stamp) == nil {
				v.markValidated(path, stamp)
				mergeColumnUnion(union, stamp)
				continue
			}
			// The stamp fails the invariant: fall through to the probe. A
			// stamp may only short-circuit success, never author a failure —
			// byte truth (#187) decides whether the file is malformed, and a
			// corrupt manifest must not fail a healthy object (#256).
		}
		cols, err := describeParquetColumns(ctx, duck, path)
		if err != nil {
			complete = false // inconclusive: defer to the execution-path classifier
			continue
		}
		if err := parquetcheck.Check(path, cols); err != nil {
			return false, fmt.Errorf("%w: %w", ErrFederatedReadFailed, err)
		}
		v.markValidated(path, cols)
		mergeColumnUnion(union, cols)
	}
	return complete, nil
}

// mergeColumnUnion folds one footer's columns into the running union.
// First-seen type wins on a cross-generation type conflict: #255 consumes
// only the names (a column present anywhere is never augmented), and the
// scan's union_by_name owns type widening (#189).
func mergeColumnUnion(union, cols map[string]string) {
	for name, typ := range cols {
		if _, ok := union[name]; !ok {
			union[name] = typ
		}
	}
}

func (v *parquetSchemaValidator) validatedCols(path string) (map[string]string, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	cols, ok := v.valid[path]
	return cols, ok
}

func (v *parquetSchemaValidator) markValidated(path string, cols map[string]string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.valid[path] = cols
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
