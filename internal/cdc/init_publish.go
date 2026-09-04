package cdc

import (
	"context"
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
// The publish is a CAS under the loaded etag. A CONFIRMED 412 means a
// compactor swap landed during the export; the run reloads the manifest and
// re-splices its base tier rather than discard hours of export (#416). Only a
// confirmed 412 is retried, and only initPublishConflictRetries times — an
// ambiguous save error may have committed, and a blind retry could publish
// twice.
func updateSchemaManifest(ctx context.Context, runCtx *initRunContext, state *schemaInitState) error {
	if runCtx.manifestStore == nil || len(state.fileEntries) == 0 || runCtx.dryRun {
		return nil
	}

	manifestPath, err := runCtx.manifestResolver.Resolve(state.schemaID)
	if err != nil {
		return fmt.Errorf("resolve manifest path: %w", err)
	}
	for attempt := 0; ; attempt++ {
		err := manifest.ReplaceTierFiles(ctx, runCtx.manifestStore, manifestPath, state.schemaID, "base", state.fileEntries)
		if err == nil {
			break
		}
		if !manifest.IsPreconditionFailed(err) || attempt >= initPublishConflictRetries {
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
		zap.Int("files_added", len(state.fileEntries)))
	return nil
}
