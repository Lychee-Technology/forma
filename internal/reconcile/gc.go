package reconcile

import (
	"context"
	"fmt"
	"maps"
	"time"

	"go.uber.org/zap"
)

// gcSchema deletes provable garbage — merged-base and _tmp orphans (#188
// rewrite leftovers) plus delta orphans the repair guard classified as
// leftovers. Init-shaped base orphans never reach here (see
// reconcileSchema), and delta orphans require the repair analysis first.
//
// Deletion implements the #188 follow-up's "unlisted longer than the grace
// period" contract with a persisted sighting state: the first run that
// observes a candidate unlisted only records the sighting; a later run
// deletes it once BOTH the observed-unlisted duration and the object age
// exceed the grace period. LastModified alone cannot express unlisted
// duration — an old source freshly spliced out by the compactor must
// survive the in-flight-reader window. Deletion is best-effort per key; an
// unreadable sighting state refuses deletion and surfaces as a tool
// failure.
func (r *Reconciler) gcSchema(ctx context.Context, schemaID int16, candidates []ObjectInfo) ([]string, error) {
	if r.Opts.GCGrace <= 0 {
		// A zero grace would delete objects the moment they are first
		// seen. Refuse instead of silently degrading the documented
		// protection.
		return nil, fmt.Errorf("gc requested for schema %d with non-positive grace %v", schemaID, r.Opts.GCGrace)
	}
	if r.GCStates == nil {
		return nil, fmt.Errorf("gc requested for schema %d but no sighting state store is configured", schemaID)
	}
	state, etag, err := r.GCStates.Load(ctx, schemaID)
	if err != nil {
		return nil, fmt.Errorf("load schema %d gc sighting state: %w", schemaID, err)
	}

	now := r.Now()
	cutoff := now.Add(-r.Opts.GCGrace)
	next := make(map[string]int64, len(candidates))
	var deleted []string
	for _, obj := range candidates {
		firstSeen, seen := state[obj.Key]
		if !seen {
			next[obj.Key] = now.UnixMilli()
			r.Logger.Info("recorded first unlisted sighting; deletable after the grace period",
				zap.Int16("schema_id", schemaID), zap.String("key", obj.Key))
			continue
		}
		unlistedLongEnough := time.UnixMilli(firstSeen).Before(cutoff)
		objectOldEnough := obj.LastModified.Before(cutoff)
		if !unlistedLongEnough || !objectOldEnough {
			next[obj.Key] = firstSeen
			continue
		}
		if err := r.Deleter.DeleteObject(ctx, obj.Key); err != nil {
			r.Logger.Warn("failed to delete orphaned object; leaving it for the next run",
				zap.Int16("schema_id", schemaID), zap.String("key", obj.Key), zap.Error(err))
			next[obj.Key] = firstSeen
			continue
		}
		r.Logger.Info("deleted orphaned compaction leftover",
			zap.Int16("schema_id", schemaID), zap.String("key", obj.Key))
		deleted = append(deleted, obj.Key)
	}

	// Keys no longer among the candidates (re-listed or gone) drop out, so
	// a later unlisting restarts their grace clock. Persisting is
	// best-effort: a lost save only means the next run re-records, which
	// delays deletion — never accelerates it.
	if !maps.Equal(next, state) {
		if _, err := r.GCStates.Save(ctx, schemaID, next, etag); err != nil {
			r.Logger.Warn("failed to persist gc sighting state; next run will re-record",
				zap.Int16("schema_id", schemaID), zap.Error(err))
		}
	}
	return deleted, nil
}
