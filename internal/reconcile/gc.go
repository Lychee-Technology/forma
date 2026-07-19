package reconcile

import (
	"context"

	"go.uber.org/zap"
)

// gcSchema deletes base-shaped and _tmp orphans — compaction-rewrite
// leftovers (#188): staging objects from crashed rewrites and merged
// sources whose post-commit deletion failed. Their data already lives in
// the manifest-listed merged base, so deletion loses nothing. Delta-shaped
// orphans are never candidates: their data exists nowhere else.
//
// The grace period guards the in-flight-reader race: a query that resolved
// its object list from a pre-splice manifest can still reference an
// unlisted key for about one query duration after the splice. Only objects
// whose LastModified is older than the grace period are deleted; with the
// default well above any query timeout the residual window is negligible.
// Deletion is best-effort — a failed delete is logged and the key stays in
// the report as an orphan, exactly like the compactor's own source cleanup.
func (r *Reconciler) gcSchema(ctx context.Context, schemaID int16, candidates []ObjectInfo) []string {
	if r.Opts.GCGrace <= 0 {
		// A zero grace would delete objects uploaded milliseconds ago —
		// including a live compaction rewrite's staging files. Refuse
		// instead of silently degrading the documented protection.
		r.Logger.Error("gc requested with a non-positive grace period; refusing to delete anything",
			zap.Int16("schema_id", schemaID), zap.Duration("gc_grace", r.Opts.GCGrace))
		return nil
	}
	cutoff := r.Now().Add(-r.Opts.GCGrace)
	var deleted []string
	for _, obj := range candidates {
		if !obj.LastModified.Before(cutoff) {
			r.Logger.Info("orphan within GC grace period, keeping",
				zap.Int16("schema_id", schemaID), zap.String("key", obj.Key),
				zap.Time("last_modified", obj.LastModified))
			continue
		}
		if err := r.Deleter.DeleteObject(ctx, obj.Key); err != nil {
			r.Logger.Warn("failed to delete orphaned object; leaving it for the next run",
				zap.Int16("schema_id", schemaID), zap.String("key", obj.Key), zap.Error(err))
			continue
		}
		r.Logger.Info("deleted orphaned compaction leftover",
			zap.Int16("schema_id", schemaID), zap.String("key", obj.Key))
		deleted = append(deleted, obj.Key)
	}
	return deleted
}
