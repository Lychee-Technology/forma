package compaction

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/manifest"
	"github.com/lychee-technology/forma/internal/telemetry"
	"go.uber.org/zap"
)

// canRewrite reports whether the compactor has everything the physical
// rewrite needs. When it does not, a rewrite-eligible pass degrades to
// RewritePending exactly as the pre-#188 stub did.
func (c *Compactor) canRewrite() bool {
	return c.Merger != nil && c.S3 != nil && c.Bucket != "" && c.DataPrefix != ""
}

// runRewrite executes the dirty-ratio rewrite (#188): merge the schema's
// COMPLETE base+delta parquet set into one new base file, swap the manifest
// under the loaded ETag, and RETAIN the now-unlisted sources.
//
// Ordering is what makes this safe under manifest-driven reads
// (manifest ⊆ live objects, internal/manifest/query_source.go):
//  0. verify every stamped source still hashes to its manifest checksum and
//     refuse the whole pass otherwise (#347) — subject to the exceptions on
//     verifySourceChecksums' godoc: it covers stamped sources inside this
//     compactor's own bucket, and does nothing when the config opts out or no
//     ObjectReader is wired,
//  1. merge to a _tmp key (never listed),
//  2. copy to a UUID-named final base key (never reused, so no listed object
//     is ever overwritten),
//  3. commit the manifest swap — the only atomic step; a conditional-put
//     conflict surfaces as ErrConcurrentModification and the retry recomputes
//     everything from a fresh manifest,
//  4. after the commit, the now-unlisted sources are deliberately NOT
//     deleted (#461): an in-flight query that resolved the pre-swap object
//     list may still lazily open them, and a zero-grace delete would fail it
//     with the non-degradable ParquetSetInconsistentError. They are left as
//     unlisted orphans for manifest-reconcile's sighting-state GC grace
//     (base/tmp shapes via --gc, delta shapes via --repair --gc). The only
//     inline deletion is a confirmed-412 conflict dropping its own
//     never-listed staged base.
//
// A crash before step 3 leaves the manifest untouched (staged objects become
// #203 orphans); a crash after it changes nothing — the retained sources are
// the same unlisted orphans the happy path leaves.
func (c *Compactor) runRewrite(
	ctx context.Context,
	schemaID int16,
	m *manifest.Manifest,
	etag string,
	sources []manifest.FileEntry,
	analysis CompactionResult,
) (CompactionResult, error) {
	// Fail closed before anything is merged: past this point the sources are
	// folded into one object and then deleted, so a corrupt input would be
	// laundered into the new base and lose its name (#347).
	if err := c.verifySourceChecksums(ctx, schemaID, sources); err != nil {
		return CompactionResult{}, err
	}

	sourceURIs := make([]string, 0, len(sources))
	sourcePaths := make(map[string]bool, len(sources))
	for _, f := range sources {
		sourceURIs = append(sourceURIs, c.objectURI(f.Path))
		sourcePaths[f.Path] = true
	}

	tmpKey := cdc.BuildBaseTempPath(c.DataPrefix, schemaID, uuid.Must(uuid.NewV7()).String())
	finalKey := cdc.BuildMergedBasePath(c.DataPrefix, schemaID, uuid.Must(uuid.NewV7()).String())

	stats, err := c.Merger.MergeToTmp(ctx, sourceURIs, c.objectURI(tmpKey))
	if err != nil {
		return CompactionResult{}, fmt.Errorf("rewrite merge for schema %d: %w", schemaID, err)
	}
	if err := cdc.CopyTmpToFinal(ctx, c.S3, c.Bucket, tmpKey, finalKey, c.Logger); err != nil {
		return CompactionResult{}, fmt.Errorf("promote merged base for schema %d: %w", schemaID, err)
	}

	sizeBytes, err := cdc.HeadObjectSize(ctx, c.S3, c.Bucket, finalKey)
	if err != nil {
		// Best-effort: SizeBytes only feeds the promotion heuristic.
		c.Logger.Warn("failed to stat merged base; manifest SizeBytes stays 0",
			zap.Int16("schema_id", schemaID), zap.String("final_key", finalKey), zap.Error(err))
	}

	checksum := c.stampChecksum(ctx, schemaID, finalKey)

	spliceManifest(m, sourcePaths, manifest.FileEntry{
		Tier:       "base",
		Path:       finalKey,
		RowIDMin:   stats.RowIDMin,
		RowIDMax:   stats.RowIDMax,
		RowCount:   stats.RowsOut,
		CreatedMin: stats.CreatedMin,
		CreatedMax: stats.CreatedMax,
		SizeBytes:  sizeBytes,
		Columns:    stats.Columns,
		Checksum:   checksum,
	})

	if _, err := c.saveManifestChecked(ctx, schemaID, m, etag); err != nil {
		if errors.Is(err, ErrConcurrentModification) {
			// Confirmed 412: the conditional put was rejected, so the staged
			// base is provably unlisted; drop it so the conflict retry (which
			// re-merges from a fresh manifest) does not accumulate orphans.
			c.deleteObjects(ctx, schemaID, []string{finalKey})
			return CompactionResult{}, fmt.Errorf("rewrite manifest swap for schema %d: %w", schemaID, err)
		}
		// Ambiguous outcome: the put may have committed before the response
		// was lost, in which case the manifest already lists the staged base
		// — deleting it would break a committed manifest. Retain it and
		// surface the key as the operator's pointer (#197 pattern). Either
		// way the next pass self-heals: a committed swap reads as Noop with a
		// healthy manifest; an uncommitted one re-merges, leaving the staged
		// object as an unlisted orphan for #203.
		return CompactionResult{}, fmt.Errorf("rewrite manifest swap for schema %d had an ambiguous outcome; staged base retained at %s: %w", schemaID, finalKey, err)
	}

	// #461: the merged sources are NOT deleted here. An in-flight federated
	// read that resolved the pre-swap manifest may still lazily open them;
	// deleting now would fail that read with the non-degradable,
	// breaker-worthy ParquetSetInconsistentError. They stay behind as
	// unlisted orphans for manifest-reconcile, whose gcSchema sighting-state
	// grace exists exactly for this window (base/tmp shapes via --gc, delta
	// shapes via --repair --gc). files_merged below counts them.
	telemetry.EmitCompactionRewriteApplied(ctx, schemaID)
	c.Logger.Info("compaction rewrite completed",
		zap.Int16("schema_id", schemaID),
		zap.Int64("version", m.Version),
		zap.Int("files_merged", len(sources)),
		zap.Int64("rows_in", stats.RowsIn),
		zap.Int64("rows_out", stats.RowsOut),
		zap.String("new_base_key", finalKey))

	result := analysis
	result.Outcome = RewriteApplied
	result.Version = m.Version
	result.FilesMerged = len(sources)
	result.RowsIn = stats.RowsIn
	result.RowsOut = stats.RowsOut
	result.NewBaseKey = finalKey
	return result, nil
}

// stampChecksum hashes the object the rewrite just published so the manifest
// entry can carry a content checksum a later verification pass compares
// against (#347). It hashes the FINAL key — the tmp object is gone by now —
// and is best-effort under the same policy as the flush stamp: a failure is
// reported and leaves the entry unstamped (verification skips empty
// checksums), never fails the rewrite. Nil ObjectReader means no stamping.
func (c *Compactor) stampChecksum(ctx context.Context, schemaID int16, finalKey string) string {
	// This plain nil check needs no typed-nil companion (#302): both Compactor
	// construction sites assign the same client value to S3 and to ObjectReader,
	// and the CopyTmpToFinal call above already dispatched on c.S3 — a nil
	// pointer behind that interface faults there, before this line runs.
	if c.ObjectReader == nil {
		return ""
	}
	sum, err := cdc.ObjectSHA256(ctx, c.ObjectReader, c.Bucket, finalKey)
	if err != nil {
		c.Logger.Warn("failed to checksum merged base; manifest entry stays unstamped",
			zap.Int16("schema_id", schemaID), zap.String("final_key", finalKey), zap.Error(err))
		return ""
	}
	return sum
}

// spliceManifest removes exactly the merged entries by path and appends the
// new base entry. Surgical splice, not a wholesale tier replace: entries the
// rewrite did not merge (whatever their tier) must survive, keeping the
// "only remove what you merged" invariant explicit.
func spliceManifest(m *manifest.Manifest, mergedPaths map[string]bool, newBase manifest.FileEntry) {
	kept := make([]manifest.FileEntry, 0, len(m.Files)-len(mergedPaths)+1)
	for _, f := range m.Files {
		if !mergedPaths[f.Path] {
			kept = append(kept, f)
		}
	}
	m.Files = append(kept, newBase)
}

// objectURI renders a manifest path as an s3:// URI against the compactor's
// bucket; absolute URIs pass through (mirrors manifest.QuerySource.Paths).
func (c *Compactor) objectURI(path string) string {
	if strings.HasPrefix(path, "s3://") {
		return path
	}
	return fmt.Sprintf("s3://%s/%s", c.Bucket, strings.TrimPrefix(path, "/"))
}

// deleteObjects best-effort deletes bucket-relative keys. Failures (and
// absolute foreign-bucket paths) are logged and left to #203 reconciliation.
// Since #461 the rewrite only calls it for a confirmed-412 attempt's own
// never-listed staged base — merged sources are retained for the
// in-flight-reader window and reclaimed by manifest-reconcile's GC grace.
func (c *Compactor) deleteObjects(ctx context.Context, schemaID int16, paths []string) {
	for _, path := range paths {
		key := path
		if strings.HasPrefix(path, "s3://") {
			trimmed := strings.TrimPrefix(path, "s3://"+c.Bucket+"/")
			if trimmed == path {
				c.Logger.Warn("skipping deletion of object outside compactor bucket",
					zap.Int16("schema_id", schemaID), zap.String("path", path))
				continue
			}
			key = trimmed
		}
		if err := cdc.DeleteObjectKey(ctx, c.S3, c.Bucket, key); err != nil {
			c.Logger.Warn("failed to delete merged source object; leaving orphan for manifest-reconcile (#203; base/tmp shapes need --gc, delta shapes --repair --gc)",
				zap.Int16("schema_id", schemaID), zap.String("key", key), zap.Error(err))
		}
	}
}
