package federated

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// Path resolution of a pgx pass (design.md §6.1.1, step 1): today's
// resolution, with the flush-grace cutoff the pgx hot form needs, and the
// frozen inventory. A source-authored glob (the level-3 fallback) is listed
// once, at resolution, and the pass scans the listed objects, never the
// glob: an object published after the listing is then outside the scan
// whatever its key, so the parquet leg reads exactly what the path set held
// at P, which the snapshot argument needs.

// pgxScanSources is the resolved storage context of one pgx pass.
type pgxScanSources struct {
	scanSources
	// globListed marks a set whose globs were listed into objects. No
	// manifest lists those objects, so one deleted before the scan fails
	// the read without claiming a manifest inconsistency.
	globListed bool
	// emptyCandidate marks a source-authored set whose every path was a
	// glob listing no object. Whether the schema has parquet to read is
	// decided in the snapshot, from HasParquetHistory (#720).
	emptyCandidate bool
}

// resolvePgxScanSources stamps the pass's cutoff, resolves and freezes the
// parquet path set, and runs the pre-read checks over the frozen objects.
func (e *DBFederatedQueryEngine) resolvePgxScanSources(ctx context.Context, tables model.StorageTables, q *model.FederatedAttributeQuery, hasHot bool) (pgxScanSources, error) {
	// Stamped before resolution, as on the attach path (#252).
	graceCutoffMs := e.passCutoffMs(e.clock()().UnixMilli(), hasHot)
	paths, fromSource, stamps, err := e.resolveParquetPaths(ctx, q)
	if err != nil {
		return pgxScanSources{}, fmt.Errorf("resolve parquet paths: %w", err)
	}
	if len(paths) == 0 {
		return pgxScanSources{}, &NoParquetPathsError{SchemaID: q.SchemaID, SourceConfigured: e.parquetSource != nil}
	}
	out := pgxScanSources{scanSources: scanSources{fromSource: fromSource, graceCutoffMs: graceCutoffMs}}
	if fromSource {
		// A caller hint stays a glob and expands at execution: hints are
		// outside the snapshot guarantee (design.md §6.1.1).
		frozen, listed, err := freezeParquetPaths(ctx, e.duck, paths)
		if err != nil {
			return pgxScanSources{}, err
		}
		paths, out.globListed = frozen, listed
		if len(paths) == 0 {
			cache, _ := e.schemaCacheByID(q.SchemaID)
			out.cold = emptyParquetScanSet(cache)
			out.emptyCandidate = true
			return out, nil
		}
		if corrupt := e.corruptPaths.Cached(paths); len(corrupt) > 0 {
			return pgxScanSources{}, e.refuseCachedCorrupt(ctx, tables, q, paths, corrupt, stamps, !listed)
		}
	}
	out.paths = paths
	if isBenchmarkSchemaID(q.SchemaID) {
		return out, nil
	}
	cold, err := e.validateScanSet(ctx, q.SchemaID, paths, fromSource, stamps)
	if err != nil {
		return pgxScanSources{}, err
	}
	out.cold = cold
	return out, nil
}

// freezeParquetPaths replaces each glob of paths with the objects it lists,
// in listing order, and keeps every other path. listed reports whether any
// path was a glob. A failed listing fails the pass as a degradable
// ErrFederatedReadFailed: scanning the glob instead would answer outside the
// snapshot without saying so.
func freezeParquetPaths(ctx context.Context, duck DuckDBQueryExecutor, paths []string) (frozen []string, listed bool, err error) {
	frozen = make([]string, 0, len(paths))
	for _, path := range paths {
		if !strings.ContainsAny(path, forma.ParquetGlobMetacharacters) {
			frozen = append(frozen, path)
			continue
		}
		if duck == nil {
			return nil, false, fmt.Errorf("list parquet glob %s: no duckdb executor: %w", path, ErrFederatedReadFailed)
		}
		matched, err := globParquetPaths(ctx, duck, path)
		if err != nil {
			return nil, false, fmt.Errorf("list parquet glob for the pass's frozen inventory: %w: %w", ErrFederatedReadFailed, err)
		}
		listed = true
		for _, m := range matched {
			if !slices.Contains(frozen, m) {
				frozen = append(frozen, m)
			}
		}
	}
	return frozen, listed, nil
}

// settleEmptyCandidate decides, in the pass's snapshot, a frozen set that
// lists no object (#720). Without parquet history the hot leg is the whole
// answer and DuckDB is not run. With it, a source that now resolves objects
// raced a publication and the pass is retried; one that still resolves
// nothing cannot be read, as a glob scan matching nothing fails today.
func (p *pgxPass) settleEmptyCandidate(ctx context.Context, hasParquetHistory bool) error {
	if !hasParquetHistory {
		p.noObjects = true
		return nil
	}
	if !p.e.sourceResolvesNoObjects(ctx, p.q) {
		return &parquetPublishedDuringReadError{SchemaID: p.q.SchemaID}
	}
	return fmt.Errorf("schema %d: the parquet path set lists no object, but PostgreSQL records that parquet was published for the schema: %w", p.q.SchemaID, ErrFederatedReadFailed)
}
