package reconcile

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/manifest"
)

// repairSchema appends manifest entries for delta-shaped orphans — the #197
// failure mode where the file was exported and its rows marked flushed but
// the manifest append failed. Entry metadata is recomputed from the parquet
// contents, never trusted from filenames. Runs under the schema advisory
// lock; the manifest save still goes through etag optimistic concurrency
// because manifest writers (compactor) do not take the lock.
//
// Returns the keys resolved: appended entries plus orphans a concurrent
// writer already listed by the time the manifest was (re)loaded. Only a
// confirmed 412 precondition failure is retried — after an ambiguous save
// error the write may have landed, and a blind retry could duplicate
// entries.
func (r *Reconciler) repairSchema(ctx context.Context, schemaID int16, m *manifest.Manifest, etag string, orphans []ObjectInfo) ([]string, error) {
	if r.Stats == nil {
		return nil, fmt.Errorf("repair requested for schema %d but no stats reader is configured", schemaID)
	}

	// Stats survive across etag retries: a conflict changes the manifest,
	// not the parquet contents.
	entryCache := make(map[string]manifest.FileEntry, len(orphans))

	for attempt := 0; ; attempt++ {
		known := make(map[string]struct{}, len(m.Files))
		for _, f := range m.Files {
			if key, ok := normalizeKey(r.Bucket, f.Path); ok {
				known[key] = struct{}{}
			}
		}

		var appended []manifest.FileEntry
		var resolved []string
		for _, obj := range orphans {
			if _, listed := known[obj.Key]; listed {
				resolved = append(resolved, obj.Key)
				continue
			}
			entry, ok := entryCache[obj.Key]
			if !ok {
				stats, err := r.Stats.FileStats(ctx, obj.Key)
				if err != nil {
					r.Logger.Warn("skipping orphan with unreadable parquet stats; leaving it orphaned",
						zap.Int16("schema_id", schemaID), zap.String("key", obj.Key), zap.Error(err))
					continue
				}
				entry = manifest.FileEntry{
					Tier:       "delta",
					Path:       obj.Key,
					RowIDMin:   stats.RowIDMin,
					RowIDMax:   stats.RowIDMax,
					CreatedMin: stats.CreatedMin,
					CreatedMax: stats.CreatedMax,
					RowCount:   stats.RowsOut,
					SizeBytes:  obj.Size,
				}
				entryCache[obj.Key] = entry
			}
			appended = append(appended, entry)
			resolved = append(resolved, obj.Key)
		}

		if len(appended) == 0 {
			return resolved, nil
		}

		m.Files = append(m.Files, appended...)
		if _, err := r.Manifests.Save(ctx, schemaID, m, etag); err == nil {
			for _, e := range appended {
				r.Logger.Info("appended orphaned delta to manifest",
					zap.Int16("schema_id", schemaID), zap.String("key", e.Path), zap.Int64("row_count", e.RowCount))
			}
			return resolved, nil
		} else if !compaction.IsConcurrentModification(err) {
			return nil, fmt.Errorf("save schema %d manifest: %w", schemaID, err)
		} else if attempt >= r.Opts.MaxETagRetries {
			return nil, fmt.Errorf("save schema %d manifest: still conflicting after %d optimistic-concurrency retries: %w",
				schemaID, r.Opts.MaxETagRetries, err)
		}

		var err error
		m, etag, err = r.Manifests.Load(ctx, schemaID)
		if err != nil {
			return nil, fmt.Errorf("reload schema %d manifest after save conflict: %w", schemaID, err)
		}
	}
}
