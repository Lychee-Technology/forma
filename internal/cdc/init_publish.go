package cdc

import (
	"context"
	"errors"
	"fmt"

	"github.com/lychee-technology/forma/internal/manifest"
	"go.uber.org/zap"
)

// initPublishConflictRetries bounds how many confirmed-412 conflicts the
// final manifest publish absorbs before the run fails. The only writer that
// can race it is the compactor (init holds the schema advisory lock against
// the flusher and reconcile, not the compactor), and its swaps are seconds
// apart at most, so a handful of reloads is plenty.
const initPublishConflictRetries = 3

// loadInitManifest loads the schema's manifest for a cdc-init read or
// publish through manifest.LoadOrCreateForSchema, the one schema-identity
// rule (#520, #522): a manifest whose stamp names another schema is a
// collided or mis-pointed --manifest-template, and with --replace-delta
// acting on it would purge that other schema's delta tier and publish this
// schema's base under its identity (#371 review) — refused with
// forma.ManifestSchemaMismatchError. A manifest listing entries under
// schema_id 0 cannot prove which schema owns them and is refused with
// forma.ManifestUnstampedError; an empty one is stamped for this schema in
// memory so the coming save persists the stamp and every later load is
// checked. Every load in the run — the pre-flight inventory and each publish
// attempt, including a 412 or ambiguous-save reload — fails closed here.
func loadInitManifest(ctx context.Context, runCtx *initRunContext, schemaID int16, manifestPath string) (*manifest.Manifest, string, error) {
	m, etag, err := manifest.LoadOrCreateForSchema(ctx, runCtx.manifestStore, manifestPath, schemaID)
	if err != nil {
		return nil, "", fmt.Errorf("load manifest %s for schema %d: %w", manifestPath, schemaID, err)
	}
	return m, etag, nil
}

// updateSchemaManifest records the exported base files in the schema
// manifest. Failures propagate: base files absent from the manifest are
// invisible to manifest consumers (e.g. compaction), and a silent miss here
// would let RunInit report success for an unusable export. Init is a full
// re-export, so the base tier is replaced wholesale with this run's files:
// reruns neither duplicate entries nor leave stale ranges behind (#176).
// The previous run's objects are not deleted here: since #416 every batch
// lands on a fresh write-once key, so after a re-run the superseded set is
// unlisted and reclaimed by `forma-tools manifest-reconcile --gc` (#203) —
// never overwritten under a listed entry's stamp.
//
// With ReplaceDelta (#371) the same save also empties the delta tier, so the
// manifest never publishes a state of new base plus stale delta; the delta
// entries it drops join state.deltaPurge for the post-swap purge. The
// publish is a CAS under the loaded etag. A CONFIRMED 412 means a compactor
// swap landed during the export; the run reloads the manifest and re-splices
// its tiers rather than discard hours of export (#416). Only a confirmed 412
// is retried, and only initPublishConflictRetries times. Any other save
// error is ambiguous — the put may have committed before the response was
// lost — and is never blindly retried; with ReplaceDelta it is resolved by
// confirmAmbiguousSwap instead, because a committed swap whose purge is
// skipped would leave the old delta objects as unlisted orphans that
// manifest-reconcile --repair re-adopts as lost deletes.
func updateSchemaManifest(ctx context.Context, runCtx *initRunContext, state *schemaInitState) error {
	if runCtx.manifestStore == nil || len(state.fileEntries) == 0 || runCtx.dryRun {
		return nil
	}

	manifestPath, err := runCtx.manifestResolver.Resolve(state.schemaID)
	if err != nil {
		return fmt.Errorf("resolve manifest path: %w", err)
	}
	for attempt := 0; ; attempt++ {
		err := publishInitTiers(ctx, runCtx, state, manifestPath)
		if err == nil {
			break
		}
		if !manifest.IsPreconditionFailed(err) {
			if runCtx.replaceDelta {
				return confirmAmbiguousSwap(ctx, runCtx, state, manifestPath, err)
			}
			return fmt.Errorf("update manifest: %w", err)
		}
		if attempt >= initPublishConflictRetries {
			return fmt.Errorf("update manifest: %w", err)
		}
		runCtx.logger.Warn("manifest publish lost a conditional-put race; reloading and re-splicing the base tier",
			zap.Int16("schema_id", state.schemaID),
			zap.String("manifest_path", manifestPath),
			zap.Int("attempt", attempt+1),
			zap.Error(err))
	}
	runCtx.logger.Info("manifest updated",
		zap.Int16("schema_id", state.schemaID),
		zap.String("manifest_path", manifestPath),
		zap.Int("files_added", len(state.fileEntries)),
		zap.Bool("delta_tier_replaced", runCtx.replaceDelta))
	return nil
}

// publishInitTiers is one CAS attempt. Without ReplaceDelta it is the
// base-only splice; with it, base and delta are spliced under one etag so
// no intermediate manifest carries both the new base and the old delta.
// Both modes load through loadInitManifest so a foreign-schema manifest is
// rejected on every attempt, not only at the pre-flight.
func publishInitTiers(ctx context.Context, runCtx *initRunContext, state *schemaInitState, manifestPath string) error {
	m, etag, err := loadInitManifest(ctx, runCtx, state.schemaID, manifestPath)
	if err != nil {
		return err
	}
	if runCtx.replaceDelta {
		// Delta entries that appeared since the pre-flight (a lost 412 race
		// reloads a manifest another writer changed) are purged too; one
		// the purge could not honour fails the publish rather than silently
		// surviving under the new base or deleting outside the namespace.
		keys, unnormalizable, foreign := classifyDeltaEntries(runCtx, state.schemaID, manifest.FilterByTier(m, "delta"))
		if err := refuseUnpurgeable(runCtx, state.schemaID, unnormalizable, foreign); err != nil {
			return err
		}
		state.deltaPurge = mergeKeys(state.deltaPurge, keys)
		manifest.SpliceTierFiles(m, "delta", nil)
	}
	manifest.SpliceTierFiles(m, "base", state.fileEntries)
	if _, err := manifest.Save(ctx, runCtx.manifestStore, manifestPath, m, etag); err != nil {
		return fmt.Errorf("save manifest: %w", err)
	}
	return nil
}

// confirmAmbiguousSwap resolves a non-412 save failure of the ReplaceDelta
// publish (#371 review). The put may have committed before the response was
// lost, so the manifest is reloaded and compared with what this run set out
// to publish: its base entries — write-once keys since #416, so a matching
// path set can only be this publish — and an empty delta tier. A match means
// the swap is committed and the purge must follow; the delete-after-swap
// ordering is intact because the swap is now confirmed. Anything else —
// the pre-swap manifest still in place, or a reload that fails too — keeps
// the outcome ambiguous: the error is returned, nothing is deleted, and the
// operator reruns `cdc-init --replace-delta`, whose pre-flight inventories
// the delta objects again whether or not the manifest still lists them.
func confirmAmbiguousSwap(ctx context.Context, runCtx *initRunContext, state *schemaInitState, manifestPath string, saveErr error) error {
	m, _, reloadErr := loadInitManifest(ctx, runCtx, state.schemaID, manifestPath)
	if reloadErr != nil {
		return fmt.Errorf("update manifest: save outcome ambiguous and the reload failed too; delta objects not deleted, rerun cdc-init --replace-delta: %w", errors.Join(saveErr, reloadErr))
	}
	if !swapCommitted(m, state.fileEntries) {
		return fmt.Errorf("update manifest: reload shows the swap did not commit; delta tier left listed and nothing deleted: %w", saveErr)
	}
	runCtx.logger.Warn("manifest save reported an error but the reload shows this run's swap committed; proceeding to the delta purge",
		zap.Int16("schema_id", state.schemaID),
		zap.String("manifest_path", manifestPath),
		zap.Error(saveErr))
	return nil
}

// swapCommitted reports whether m carries exactly the run's base entries
// and no delta entry — the state the ReplaceDelta publish writes.
func swapCommitted(m *manifest.Manifest, entries []manifest.FileEntry) bool {
	if len(manifest.FilterByTier(m, "delta")) != 0 {
		return false
	}
	base := manifest.ListPaths(m, "base")
	if len(base) != len(entries) {
		return false
	}
	want := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		want[e.Path] = struct{}{}
	}
	for _, p := range base {
		if _, ok := want[p]; !ok {
			return false
		}
	}
	return true
}
