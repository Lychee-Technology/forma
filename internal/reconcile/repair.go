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
// manifest ONLY when the version-aware guard (classifyDeltaOrphan) proves
// it still carries versions the manifest-listed inventory lost — the #197
// failure mode. Provable compaction leftovers (#188 failed post-commit
// deletes) are classified for --gc instead: their tombstone winners were
// dropped by the merge, so re-appending them would resurrect deleted rows.
// Ambiguous files are refused and left for manual surgery.
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

	// Cached entries survive across etag retries: a conflict changes the
	// manifest, not the parquet contents the stats and checksum describe.
	entryCache := make(map[string]manifest.FileEntry, len(orphans))

	for attempt := 0; ; attempt++ {
		outcome, appended := r.planRepair(ctx, schemaID, m, orphans, entryCache)
		if len(appended) == 0 {
			return outcome, nil
		}

		m.Files = append(m.Files, appended...)
		_, err := r.Manifests.Save(ctx, schemaID, m, etag)
		if err == nil {
			for _, e := range appended {
				r.Logger.Info("appended orphaned delta to manifest",
					zap.Int16("schema_id", schemaID), zap.String("key", e.Path), zap.Int64("row_count", e.RowCount))
			}
			return outcome, nil
		}
		if !compaction.IsConcurrentModification(err) {
			return repairOutcome{}, fmt.Errorf("save schema %d manifest: %w", schemaID, err)
		}
		if attempt >= r.Opts.MaxETagRetries {
			return repairOutcome{}, fmt.Errorf("save schema %d manifest: still conflicting after %d optimistic-concurrency retries: %w",
				schemaID, r.Opts.MaxETagRetries, err)
		}
		if m, etag, err = r.Manifests.Load(ctx, schemaID); err != nil {
			return repairOutcome{}, fmt.Errorf("reload schema %d manifest after save conflict: %w", schemaID, err)
		}
	}
}

// planRepair classifies every orphan against the current manifest snapshot
// and returns the outcome plus the entries to append in this attempt.
func (r *Reconciler) planRepair(ctx context.Context, schemaID int16, m *manifest.Manifest, orphans []ObjectInfo, entryCache map[string]manifest.FileEntry) (repairOutcome, []manifest.FileEntry) {
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
		switch verdict, err := r.classifyDeltaOrphan(ctx, schemaID, obj, listedKeys); {
		case err != nil:
			r.Logger.Warn("skipping delta orphan the repair guard could not classify",
				zap.Int16("schema_id", schemaID), zap.String("key", obj.Key), zap.Error(err))
			continue
		case verdict == deltaLeftover:
			outcome.leftovers = append(outcome.leftovers, obj)
			continue
		case verdict == deltaMixed:
			r.Logger.Warn("delta orphan mixes restorable and resurrecting rows; refusing auto-repair and auto-GC — manual reconciliation required",
				zap.Int16("schema_id", schemaID), zap.String("key", obj.Key))
			continue
		}
		entry, ok := r.buildRepairEntry(ctx, schemaID, obj, entryCache)
		if !ok {
			continue
		}
		appended = append(appended, entry)
		outcome.repaired = append(outcome.repaired, obj.Key)
	}
	return outcome, appended
}

// buildRepairEntry recomputes (or reuses) the manifest entry for one orphan.
func (r *Reconciler) buildRepairEntry(ctx context.Context, schemaID int16, obj ObjectInfo, entryCache map[string]manifest.FileEntry) (manifest.FileEntry, bool) {
	if entry, ok := entryCache[obj.Key]; ok {
		return entry, true
	}
	stats, err := r.Stats.FileStats(ctx, obj.Key)
	if err != nil {
		r.Logger.Warn("skipping orphan with unreadable parquet stats; leaving it orphaned",
			zap.Int16("schema_id", schemaID), zap.String("key", obj.Key), zap.Error(err))
		return manifest.FileEntry{}, false
	}
	entry := manifest.FileEntry{
		Tier:       "delta",
		Path:       obj.Key,
		RowIDMin:   stats.RowIDMin,
		RowIDMax:   stats.RowIDMax,
		CreatedMin: stats.CreatedMin,
		CreatedMax: stats.CreatedMax,
		RowCount:   stats.RowsOut,
		SizeBytes:  obj.Size,
		Checksum: r.stampChecksum(ctx, schemaID, obj.Key,
			"failed to checksum adopted orphan; entry stays unstamped"),
	}
	entryCache[obj.Key] = entry
	return entry, true
}

type deltaVerdict int

const (
	deltaAppend deltaVerdict = iota
	deltaLeftover
	deltaMixed
)

// classifyDeltaOrphan decides an orphan's fate from its version-aware row
// coverage (compaction.UncoveredRows) and Postgres liveness. Per uncovered
// row (the newest version no listed file supersedes):
//
//   - live version + row live in Postgres  → the #197 lost data, append.
//   - tombstone   + row dead in Postgres   → a lost delete whose absence is
//     actively resurrecting older listed versions, append (restores it).
//   - live version + row dead in Postgres  → resurrection risk: the row's
//     tombstone won a compaction merge and was dropped; appending would
//     revive it.
//   - tombstone   + row live in Postgres   → contradicts the entity state;
//     never auto-handled.
//
// No uncovered rows, or only resurrection-risk rows → provable leftover
// (GC-eligible). Any append-worthy rows alongside risk/contradiction rows →
// mixed, refused entirely.
func (r *Reconciler) classifyDeltaOrphan(ctx context.Context, schemaID int16, obj ObjectInfo, listedKeys []string) (deltaVerdict, error) {
	uncovered, err := r.Stats.UncoveredRows(ctx, obj.Key, listedKeys)
	if err != nil {
		return 0, fmt.Errorf("probe uncovered rows of %s: %w", obj.Key, err)
	}
	if len(uncovered) == 0 {
		return deltaLeftover, nil
	}

	rowIDs := make([]string, 0, len(uncovered))
	for _, row := range uncovered {
		rowIDs = append(rowIDs, row.RowID)
	}
	missing, err := r.LiveRows.MissingLiveRows(ctx, schemaID, rowIDs)
	if err != nil {
		return 0, fmt.Errorf("check live rows of %s: %w", obj.Key, err)
	}
	dead := make(map[string]bool, len(missing))
	for _, id := range missing {
		dead[id] = true
	}

	appendNeeded, resurrectRisk, contradiction := false, false, false
	for _, row := range uncovered {
		switch {
		case row.Tombstone && dead[row.RowID]:
			appendNeeded = true // lost delete: restore the tombstone
		case row.Tombstone && !dead[row.RowID]:
			contradiction = true
		case !row.Tombstone && dead[row.RowID]:
			resurrectRisk = true
		default:
			appendNeeded = true // lost live data
		}
	}
	switch {
	case contradiction, appendNeeded && resurrectRisk:
		return deltaMixed, nil
	case appendNeeded:
		return deltaAppend, nil
	default:
		return deltaLeftover, nil
	}
}
