package federated

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strings"
	"sync"

	"github.com/lychee-technology/forma/internal/parquetcheck"
	"go.uber.org/zap"
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
// A validated path is cached so steady-state cost is one footer probe per new
// object, and glob paths — expanded per query, since the match set changes as
// objects land — hit the same cache through their concrete matches.
//
// The cache is keyed by path AND by the manifest stamp the entry was validated
// under (nil for a probe-validated path). Path alone would be wrong: parquet
// objects are write-once for flush and compaction, which always mint fresh
// UUIDv7 keys, but NOT for init — cdc.BuildBasePath is deterministic
// ({min}_{max}.parquet), so an init rerun overwrites the object in place and
// rewrites its manifest entry under the same key. A path-keyed cache would
// serve a warmed server the pre-rerun columns for the rest of its life. Adding
// the stamp to the key makes the manifest rewrite the invalidation signal:
// same stamp, same bytes, keep the entry; new stamp, re-validate.
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
	valid map[string]validatedParquet
	// inflight single-flights the footer probe per (path, stamp): concurrent
	// misses on the same key would otherwise each issue their own DESCRIBE,
	// and — because the probe runs outside mu — an older probe could publish
	// its result after a newer one. Keyed by probeKey; the channel closes when
	// the leader has published. See probeColumns.
	inflight map[string]chan struct{}
	// logger reports the stamp/footer cross-check (#256). Optional; see
	// WithLogger. Nil is safe — log() substitutes a nop.
	logger *zap.Logger
}

// validatedParquet is one cache entry: the columns the path was validated
// with, and the manifest stamp that validation happened under. A lookup only
// hits while the current stamp still equals stamp — nil==nil for a path that
// was probe-validated with no stamp in play.
type validatedParquet struct {
	cols  map[string]string
	stamp map[string]string
}

func newParquetSchemaValidator() *parquetSchemaValidator {
	return &parquetSchemaValidator{
		valid:    map[string]validatedParquet{},
		inflight: map[string]chan struct{}{},
	}
}

// log is the nil-safe accessor for the optional logger.
func (v *parquetSchemaValidator) log() *zap.Logger {
	if v == nil || v.logger == nil {
		return zap.NewNop()
	}
	return v.logger
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
// that path its probe; one that does not is ignored and the path is probed as
// usual. On the Check verdict a stamp may only short-circuit success, never
// author a failure — a rejected stamp costs at most one extra probe. The
// current stamp is also part of the cache key, so a rewritten manifest entry
// re-validates the path instead of being answered from a stale entry.
//
// The union is the one channel where a stamp can still fail a query. Check
// inspects only the system columns, so a stamp that passes it while
// UNDER-REPORTING the file's attribute columns contributes a short union with
// complete still true, and #255 then augments a NULL alias for a column the
// file really carries. Unlike the accepted glob skew, which self-heals on the
// next query, that binder failure persists until the manifest entry is
// corrected — but correcting it now suffices: the rewritten stamp is a new
// cache key, so the fix takes effect on the next query without a restart.
// Accepted (plan decision 4) — stamps are written from a write-time DESCRIBE
// of the actual bytes, so under-reporting takes a corrupted or tampered
// manifest, and the outcome is a loud classified failure, never silent data
// loss. The bytes themselves are guarded independently
// (sqlgen.BuildParquetScanSource): any scanned row with a NULL row_id or a
// NULL changed_at fails the query outright, and changed_at/deleted_at are
// CAST to BIGINT so a rogue VARCHAR cannot widen the union and reorder the LWW
// merge. So a stamp can never license a silently ignored object, nor a
// silently misordered one. deleted_at PRESENCE is the one residual — pre-#274
// legacy delta objects encode live rows as NULL, so it cannot be
// value-guarded until those objects are retired (#365).
//
// When a probe DOES run on a stamped path, the two are cross-checked and any
// divergence is logged — see warnStampDivergence.
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
// consulting and feeding the stamp-keyed cache from either feeder — a passing
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
		stamp := currentStamp(stamps, path)
		if cols, ok := v.lookupValidatedCols(path, stamp); ok {
			mergeColumnUnion(union, cols)
			continue
		}
		if len(stamp) > 0 && parquetcheck.Check(path, stamp) == nil {
			// Clone: the cache outlives the call and must not alias a
			// caller-owned map (a manifest entry in production), which
			// would make it poisonable and racy.
			v.markValidated(path, maps.Clone(stamp), maps.Clone(stamp))
			mergeColumnUnion(union, stamp)
			continue
		}
		// Either there is no stamp, or the stamp fails the invariant. A stamp
		// may only short-circuit success, never author a failure — byte truth
		// (#187) decides whether the file is malformed, and a corrupt manifest
		// must not fail a healthy object (#256). So: probe.
		cols, ok, err := v.probeColumns(ctx, duck, path, stamp)
		if err != nil {
			return false, err
		}
		if !ok {
			complete = false // inconclusive: defer to the execution-path classifier
			continue
		}
		mergeColumnUnion(union, cols)
	}
	return complete, nil
}

// probeKey identifies one probe's work: the path AND the stamp generation it
// would be validated under, so two queries racing on the same object under
// DIFFERENT manifest stamps are not collapsed into one another's result. The
// stamp is fingerprinted as a sorted name/type join. EVERY separator is NUL —
// between pairs AND between a name and its type — because a printable
// separator aliases: with "=", {"a": "b=c"} and {"a=b": "c"} both render
// "a=b=c". NUL does not occur in a DuckDB identifier, a type name, or an S3
// key, so realistic stamps cannot collide — but a manifest is JSON and JSON
// can encode an escaped NUL, so this is a strong practical guarantee, not a proof.
//
// It does not need to be a proof. The fingerprint only gates the single-flight;
// it never answers a query. Whatever a follower wakes to is re-checked by
// maps.Equal in lookupValidatedCols against the stamp actually in play, so a
// contrived collision costs one extra probe and can never serve one
// generation's columns for another's.
func probeKey(path string, stamp map[string]string) string {
	names := make([]string, 0, len(stamp))
	for name := range stamp {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, 2*len(names)+1)
	parts = append(parts, path)
	for _, name := range names {
		parts = append(parts, name, stamp[name])
	}
	return strings.Join(parts, "\x00")
}

// probeColumns runs (or waits on) exactly one footer probe per (path, stamp).
// It reports the validated columns and whether the probe was conclusive; an
// unreadable footer is (nil, false, nil) — inconclusive, deferred to the
// execution-path classifier — while an invariant violation is a loud error.
//
// The single-flight matters because the probe is the expensive part and it
// deliberately runs OUTSIDE mu (a DESCRIBE is a network round trip; holding
// the mutex across it would serialize every path in every concurrent query).
// Without it, N concurrent queries hitting the same cold object issue N
// DESCRIBEs, and a slower one can publish its cache entry after a faster,
// newer one.
//
// A follower waits for the leader and then re-runs the ordinary lookup, which
// re-checks stamp equality — that IS the generation recheck, so a leader whose
// entry has already been superseded cannot hand a follower a stale answer.
// A follower that finds no entry (the leader's probe was inconclusive, or it
// raised a violation, or a newer stamp displaced it) probes itself rather than
// inheriting a downgrade: the loud failure must reach every caller, not just
// whichever one happened to win the race. That bounds a follower at two
// probes and cannot livelock — it never re-enters the wait.
//
// Context cancellation while waiting returns the inconclusive path, exactly as
// a failed probe does, so a cancelled follower never blocks on the leader.
func (v *parquetSchemaValidator) probeColumns(
	ctx context.Context, duck DuckDBQueryExecutor, path string, stamp map[string]string,
) (map[string]string, bool, error) {
	key := probeKey(path, stamp)
	done, leader := v.beginProbe(key)
	if leader {
		defer v.finishProbe(key)
		return v.runProbe(ctx, duck, path, stamp)
	}
	select {
	case <-done:
		if cols, ok := v.lookupValidatedCols(path, stamp); ok {
			return cols, true, nil
		}
		return v.runProbe(ctx, duck, path, stamp)
	case <-ctx.Done():
		return nil, false, nil
	}
}

// beginProbe claims leadership for key, or returns the incumbent leader's done
// channel. The leader must call finishProbe.
func (v *parquetSchemaValidator) beginProbe(key string) (<-chan struct{}, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if done, ok := v.inflight[key]; ok {
		return done, false
	}
	done := make(chan struct{})
	v.inflight[key] = done
	return done, true
}

// finishProbe releases leadership and wakes the followers. It runs AFTER the
// leader has published to the cache, so a woken follower's lookup sees it.
func (v *parquetSchemaValidator) finishProbe(key string) {
	v.mu.Lock()
	done := v.inflight[key]
	delete(v.inflight, key)
	v.mu.Unlock()
	if done != nil {
		close(done)
	}
}

// runProbe is the un-deduplicated footer probe: DESCRIBE, enforce the
// invariant, cross-check the stamp, publish.
func (v *parquetSchemaValidator) runProbe(
	ctx context.Context, duck DuckDBQueryExecutor, path string, stamp map[string]string,
) (map[string]string, bool, error) {
	cols, err := describeParquetColumns(ctx, duck, path)
	if err != nil {
		return nil, false, nil // inconclusive: defer to the execution-path classifier
	}
	if err := parquetcheck.Check(path, cols); err != nil {
		return nil, false, fmt.Errorf("%w: %w", ErrFederatedReadFailed, err)
	}
	v.warnStampDivergence(path, stamp, cols)
	// The footer wins, and the CURRENT stamp is recorded next to it: a stamp
	// the invariant rejected would otherwise never match the entry it
	// produced, and the path would re-probe on every single query.
	v.markValidated(path, cols, maps.Clone(stamp))
	return cols, true, nil
}

// currentStamp returns the manifest stamp in play for path, normalizing an
// absent or empty stamp to nil so cache keying by maps.Equal is stable.
func currentStamp(stamps map[string]map[string]string, path string) map[string]string {
	if stamp := stamps[path]; len(stamp) > 0 {
		return stamp
	}
	return nil
}

// warnStampDivergence reports a manifest stamp that disagrees with the bytes.
// It can only fire on the probe fallthrough — a stamp the invariant accepts
// short-circuits before any probe — so it names exactly the case where the
// manifest claimed something the footer does not support. The footer wins:
// the probed columns are what get cached and unioned, because a column only
// the stamp claims would make #255 augment a NULL alias over a real one.
//
// It is a log rather than an error because the read SUCCEEDS: nothing in the
// caller's result would ever mention it, and a stale or hand-edited manifest
// entry is an operator's problem to fix, not a reason to fail a query the
// bytes can answer. Warn carries the path and the two set sizes; the full
// column maps are Debug — a manifest entry can list hundreds of columns.
func (v *parquetSchemaValidator) warnStampDivergence(path string, stamp, probed map[string]string) {
	if len(stamp) == 0 || maps.Equal(stamp, probed) {
		return
	}
	v.log().Warn("manifest column stamp disagrees with the parquet footer; using the footer",
		zap.String("path", path),
		zap.Int("stamp_columns", len(stamp)),
		zap.Int("footer_columns", len(probed)))
	v.log().Debug("manifest column stamp divergence detail",
		zap.String("path", path),
		zap.Any("stamp_cols", stamp),
		zap.Any("footer_cols", probed))
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

// lookupValidatedCols answers from the cache only while the entry's recorded
// stamp still equals the one currently in play. A changed stamp (including one
// appearing on, or disappearing from, a previously validated path) is a miss,
// so the caller re-validates and overwrites the entry.
func (v *parquetSchemaValidator) lookupValidatedCols(path string, stamp map[string]string) (map[string]string, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	entry, ok := v.valid[path]
	if !ok || !maps.Equal(entry.stamp, stamp) {
		return nil, false
	}
	return entry.cols, true
}

// markValidated records cols for path together with the stamp they were
// validated under. stamp must already be a clone the cache can own.
func (v *parquetSchemaValidator) markValidated(path string, cols, stamp map[string]string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.valid[path] = validatedParquet{cols: cols, stamp: stamp}
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
// returns column name → DuckDB type. The query is issued over the engine's
// executor seam; the row scan itself is shared with the *sql.DB probe in
// parquetcheck so the DESCRIBE row shape lives in exactly one place.
func describeParquetColumns(ctx context.Context, duck DuckDBQueryExecutor, path string) (map[string]string, error) {
	rows, err := duck.Query(ctx, fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s')", path))
	if err != nil {
		return nil, fmt.Errorf("describe parquet %s: %w", path, err)
	}
	defer func() { _ = rows.Close() }()

	return parquetcheck.ScanDescribeColumns(rows, path)
}
