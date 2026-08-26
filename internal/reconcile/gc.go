package reconcile

import (
	"context"
	"fmt"
	"maps"
	"time"

	"go.uber.org/zap"
)

// refuseEmptyManifestGC is the #463/#481 guard: a schema with at least one
// live base-tier object whose loaded manifest accounts for NONE of this
// schema's data — zero entries normalize into its data prefix — is far more
// likely a manifest-resolution failure than genuine mass orphaning;
// proceeding would classify the whole base tier as orphaned and, past the
// grace, delete it irreversibly. GC therefore fails the schema instead.
// Two signatures share that invariant:
//
//   - #463: the manifest resolved EMPTY (a mis-pointed --manifest-prefix/
//     --manifest-template turned into an empty manifest by the resolver's
//     LoadOrCreate semantics). An operator who has confirmed the schema's
//     manifest is genuinely empty can waive this per schema via
//     --allow-empty-manifest-schema.
//   - #481: the manifest is NON-EMPTY but foreign — a fixed-file template
//     (no {{.SchemaID}}), a cross-schema/cross-tier mispointing, or entries
//     all in a foreign bucket. Never waivable: entries exist but none
//     describe this schema's data, which indicates a wrong template or
//     bucket, not a legitimately empty manifest.
//
// A successful init promotion this run clears the guard (it just published
// this schema's manifest, proving resolution works).
func (r *Reconciler) refuseEmptyManifestGC(schemaID int16, d diffResult, promotedInit bool) error {
	baseObjects := len(d.baseInitOrphans) + len(d.baseMergedOrphans)
	if d.manifestEntriesInPrefix > 0 || baseObjects == 0 || promotedInit {
		return nil
	}
	if d.manifestEntries > 0 {
		return fmt.Errorf("gc refused for schema %d: %d base object(s) live in storage (init=%d merged=%d, %d objects seen) but none of the %d manifest entries resolved lie inside this schema's data prefix (0 in-prefix) — the loaded manifest is foreign to this schema (fixed-file or cross-schema --manifest-template, or entries pointing at another bucket), not a record of its data; verify --manifest-prefix/--manifest-template and --s3-bucket against the writers' configuration (--allow-empty-manifest-schema only waives genuinely EMPTY manifests, never a foreign one)",
			schemaID, baseObjects, len(d.baseInitOrphans), len(d.baseMergedOrphans), d.objectsSeen, d.manifestEntries)
	}
	for _, id := range r.Opts.AllowEmptyManifestSchemas {
		if id == schemaID {
			r.Logger.Warn("empty-manifest gc guard waived by explicit allowance",
				zap.Int16("schema_id", schemaID), zap.Int("base_objects", baseObjects))
			return nil
		}
	}
	return fmt.Errorf("gc refused for schema %d: %d base object(s) live in storage (init=%d merged=%d, %d objects seen) against 0 manifest entries resolved — treating this as a manifest-resolution failure, not mass orphaning; verify --manifest-prefix/--manifest-template against the writers' configuration, or re-run with --allow-empty-manifest-schema %d after confirming this schema's manifest is genuinely empty",
		schemaID, baseObjects, len(d.baseInitOrphans), len(d.baseMergedOrphans), d.objectsSeen, schemaID)
}

// gcSchema deletes provable garbage — merged-base and _tmp orphans (#188
// rewrite leftovers), init-shaped base orphans (#290: cdc-init holds the same
// per-schema advisory lock, so under it an init-shaped orphan is provably not
// from an in-flight init), plus delta orphans the repair guard classified as
// leftovers. Delta orphans require the repair analysis first (see
// reconcileSchema).
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
		r.Logger.Info("deleted orphaned object past the grace period",
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
