package reconcile

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/manifest"
)

// repairOutcome is repairSchema's per-schema result: keys resolved by
// appending (or found concurrently listed) and delta orphans classified as
// compaction leftovers, which the GC pass may delete.
type repairOutcome struct {
	repaired  []string
	leftovers []ObjectInfo
}

// repairSchema handles delta-shaped orphans. A file is appended back to the
// manifest ONLY when it still carries rows the manifest-listed inventory
// does not cover and every such row is live in Postgres — the #197 failure
// mode, where the data exists nowhere else. Everything else is refused:
//
//   - No uncovered rows, or every uncovered row deleted in Postgres: the
//     file is a compaction leftover (#188 failed post-commit delete) whose
//     tombstone winners the full merge already dropped — re-appending it
//     would resurrect deleted rows. Classified as a leftover for --gc.
//   - A mix of live and deleted uncovered rows: appending resurrects the
//     dead ones, deleting loses the live ones — left for manual surgery.
//
// Entry metadata is recomputed from the parquet contents, never trusted
// from filenames. Runs under the schema advisory lock; the manifest save
// still goes through etag optimistic concurrency (creates are If-None-Match
// guarded) because compaction and init do not take the lock. Only a
// confirmed 412 precondition failure is retried — after an ambiguous save
// error the write may have landed, and a blind retry could duplicate
// entries.
func (r *Reconciler) repairSchema(ctx context.Context, schemaID int16, m *manifest.Manifest, etag string, orphans []ObjectInfo) (repairOutcome, error) {
	if r.Stats == nil || r.LiveRows == nil {
		return repairOutcome{}, fmt.Errorf("repair requested for schema %d but stats reader or live-row checker is not configured", schemaID)
	}

	// Stats survive across etag retries: a conflict changes the manifest,
	// not the parquet contents.
	entryCache := make(map[string]manifest.FileEntry, len(orphans))

	for attempt := 0; ; attempt++ {
		known := make(map[string]struct{}, len(m.Files))
		listedKeys := make([]string, 0, len(m.Files))
		for _, f := range m.Files {
			if key, ok := normalizeKey(r.Bucket, f.Path); ok {
				known[key] = struct{}{}
				listedKeys = append(listedKeys, key)
			}
		}

		var outcome repairOutcome
		var appended []manifest.FileEntry
		for _, obj := range orphans {
			if _, listed := known[obj.Key]; listed {
				outcome.repaired = append(outcome.repaired, obj.Key)
				continue
			}
			verdict, err := r.classifyDeltaOrphan(ctx, schemaID, obj, listedKeys)
			if err != nil {
				r.Logger.Warn("skipping delta orphan the repair guard could not classify",
					zap.Int16("schema_id", schemaID), zap.String("key", obj.Key), zap.Error(err))
				continue
			}
			switch verdict {
			case deltaLeftover:
				outcome.leftovers = append(outcome.leftovers, obj)
				continue
			case deltaMixed:
				r.Logger.Warn("delta orphan mixes live and deleted uncovered rows; refusing auto-repair and auto-GC — manual reconciliation required",
					zap.Int16("schema_id", schemaID), zap.String("key", obj.Key))
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
			outcome.repaired = append(outcome.repaired, obj.Key)
		}

		if len(appended) == 0 {
			return outcome, nil
		}

		m.Files = append(m.Files, appended...)
		if _, err := r.Manifests.Save(ctx, schemaID, m, etag); err == nil {
			for _, e := range appended {
				r.Logger.Info("appended orphaned delta to manifest",
					zap.Int16("schema_id", schemaID), zap.String("key", e.Path), zap.Int64("row_count", e.RowCount))
			}
			return outcome, nil
		} else if !compaction.IsConcurrentModification(err) {
			return repairOutcome{}, fmt.Errorf("save schema %d manifest: %w", schemaID, err)
		} else if attempt >= r.Opts.MaxETagRetries {
			return repairOutcome{}, fmt.Errorf("save schema %d manifest: still conflicting after %d optimistic-concurrency retries: %w",
				schemaID, r.Opts.MaxETagRetries, err)
		}

		var err error
		m, etag, err = r.Manifests.Load(ctx, schemaID)
		if err != nil {
			return repairOutcome{}, fmt.Errorf("reload schema %d manifest after save conflict: %w", schemaID, err)
		}
	}
}

type deltaVerdict int

const (
	deltaAppend deltaVerdict = iota
	deltaLeftover
	deltaMixed
)

// classifyDeltaOrphan decides an orphan's fate from its row coverage and
// Postgres liveness (see repairSchema's contract).
func (r *Reconciler) classifyDeltaOrphan(ctx context.Context, schemaID int16, obj ObjectInfo, listedKeys []string) (deltaVerdict, error) {
	uncovered, err := r.Stats.UncoveredRowIDs(ctx, obj.Key, listedKeys)
	if err != nil {
		return 0, fmt.Errorf("probe uncovered rows of %s: %w", obj.Key, err)
	}
	if len(uncovered) == 0 {
		return deltaLeftover, nil
	}
	missing, err := r.LiveRows.MissingLiveRows(ctx, schemaID, uncovered)
	if err != nil {
		return 0, fmt.Errorf("check live rows of %s: %w", obj.Key, err)
	}
	switch {
	case len(missing) == 0:
		return deltaAppend, nil
	case len(missing) == len(uncovered):
		return deltaLeftover, nil
	default:
		return deltaMixed, nil
	}
}
