package cdc

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/manifest"
	"go.uber.org/zap"
)

// ErrDeltaTierPresent marks a cdc-init run that found delta objects for a
// schema it was asked to re-export without permission to replace them
// (#371). Init replaces the base tier wholesale, but a delta file written
// before the re-init still carries the pre-init generation of every row it
// holds — possibly under a column type the schema no longer uses (#315) —
// and every federated read would keep folding it under the new base. The
// operator opts into the purge explicitly with --replace-delta.
var ErrDeltaTierPresent = errors.New("delta tier present; cdc-init replaces only the base tier unless --replace-delta is set")

// deltaInventory is one schema's delta tier as seen before the export:
// manifest-listed entries (as bucket-relative keys), delta-shaped objects
// under the delta prefix the manifest does not mention, and manifest
// entries that cannot be resolved to a key in this bucket (globs, foreign
// buckets), which a purge could not honour.
type deltaInventory struct {
	listed         []string
	unlisted       []string
	unnormalizable []string
}

func (inv deltaInventory) empty() bool {
	return len(inv.listed) == 0 && len(inv.unlisted) == 0 && len(inv.unnormalizable) == 0
}

// purgeKeys is the sorted, de-duplicated union of every purgeable key.
func (inv deltaInventory) purgeKeys() []string {
	return mergeKeys(nil, append(append([]string{}, inv.listed...), inv.unlisted...))
}

// mergeKeys returns the sorted union of two key sets without duplicates.
func mergeKeys(existing, extra []string) []string {
	seen := make(map[string]struct{}, len(existing)+len(extra))
	out := make([]string, 0, len(existing)+len(extra))
	for _, key := range append(append([]string{}, existing...), extra...) {
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// NormalizeObjectKey resolves a manifest path to a bucket-relative key. A
// path with glob metacharacters, or an s3:// URI for a different bucket,
// cannot be addressed through this bucket's client and reports ok=false.
// Relative keys are returned verbatim. manifest-reconcile shares this rule.
func NormalizeObjectKey(bucket, path string) (string, bool) {
	if strings.ContainsAny(path, "*?[") {
		return "", false
	}
	if strings.HasPrefix(path, "s3://") {
		key, found := strings.CutPrefix(path, "s3://"+bucket+"/")
		if !found {
			return "", false
		}
		return key, true
	}
	return path, true
}

// isDeltaShapedKey reports whether key is a flusher-written delta object
// directly under prefix: `{prefix}{uuid}.parquet` (BuildDeltaPath). Nested
// keys (`_tmp/`), init- and merged-base shapes, and anything else are not
// delta and never enter the purge set.
func isDeltaShapedKey(prefix, key string) bool {
	rel, found := strings.CutPrefix(key, prefix)
	if !found || !strings.HasSuffix(rel, ".parquet") || strings.Contains(rel, "/") {
		return false
	}
	return uuid.Validate(strings.TrimSuffix(rel, ".parquet")) == nil
}

// preflightDeltaTier inventories the schema's delta tier before any row is
// counted or exported and applies the #371 rule: refuse while delta exists
// unless the run may replace it. With permission, an entry the purge could
// not honour fails the schema here, before hours of export, rather than
// after the swap. Dry-run inventories and refuses exactly like a real run.
func preflightDeltaTier(ctx context.Context, runCtx *initRunContext, schemaID int16) (deltaInventory, error) {
	inv, err := inventoryDeltaTier(ctx, runCtx, schemaID)
	if err != nil {
		return deltaInventory{}, fmt.Errorf("inventory delta tier: %w", err)
	}
	if inv.empty() {
		return inv, nil
	}
	if !runCtx.replaceDelta {
		return deltaInventory{}, fmt.Errorf("schema %d has %d manifest-listed and %d unlisted delta objects (%d unresolvable entries); rerun with --replace-delta to purge them after the base swap: %w",
			schemaID, len(inv.listed), len(inv.unlisted), len(inv.unnormalizable), ErrDeltaTierPresent)
	}
	if runCtx.manifestStore == nil {
		return deltaInventory{}, fmt.Errorf("schema %d: --replace-delta needs a configured manifest template: the purge is ordered after the manifest swap, and without a manifest there is no swap to order it after", schemaID)
	}
	if len(inv.unnormalizable) > 0 {
		return deltaInventory{}, fmt.Errorf("schema %d: manifest lists %d delta entries this run cannot delete through bucket %q (glob or foreign-bucket paths: %s); resolve them before --replace-delta",
			schemaID, len(inv.unnormalizable), runCtx.cfg.S3Bucket, strings.Join(inv.unnormalizable, ", "))
	}
	runCtx.logger.Info("delta tier will be purged after the base swap",
		zap.Int16("schema_id", schemaID),
		zap.Int("listed", len(inv.listed)),
		zap.Int("unlisted", len(inv.unlisted)))
	return inv, nil
}

// inventoryDeltaTier gathers the delta tier from both sources that can hold
// it: the manifest's delta entries and a listing of the delta prefix.
// Unlisted delta-shaped objects count as well: manifest-reconcile --repair
// re-lists an unlisted delta whose tombstones the new base does not cover
// (a "lost delete"), which after a re-init is every tombstone it holds, so a
// stale unlisted delta would return to the manifest on the next repair.
func inventoryDeltaTier(ctx context.Context, runCtx *initRunContext, schemaID int16) (deltaInventory, error) {
	var inv deltaInventory
	if runCtx.manifestStore != nil {
		entries, err := loadManifestDeltaEntries(ctx, runCtx, schemaID)
		if err != nil {
			return deltaInventory{}, err
		}
		inv.listed, inv.unnormalizable = splitNormalizableKeys(runCtx.cfg.S3Bucket, entries)
	}

	if runCtx.deltaPrefix == "" || runCtx.listObjectKeys == nil {
		runCtx.logger.Info("delta prefix listing skipped; only manifest-listed delta entries are inventoried",
			zap.Int16("schema_id", schemaID),
			zap.Bool("delta_prefix_set", runCtx.deltaPrefix != ""),
			zap.Bool("s3_client_set", runCtx.listObjectKeys != nil))
		return inv, nil
	}
	prefix := fmt.Sprintf("%s/%d/", strings.TrimSuffix(runCtx.deltaPrefix, "/"), schemaID)
	keys, err := runCtx.listObjectKeys(ctx, prefix)
	if err != nil {
		return deltaInventory{}, fmt.Errorf("list delta prefix %s: %w", prefix, err)
	}
	listed := make(map[string]struct{}, len(inv.listed))
	for _, key := range inv.listed {
		listed[key] = struct{}{}
	}
	for _, key := range keys {
		if _, known := listed[key]; known || !isDeltaShapedKey(prefix, key) {
			continue
		}
		inv.unlisted = append(inv.unlisted, key)
	}
	return inv, nil
}

func loadManifestDeltaEntries(ctx context.Context, runCtx *initRunContext, schemaID int16) ([]manifest.FileEntry, error) {
	manifestPath, err := runCtx.manifestResolver.Resolve(schemaID)
	if err != nil {
		return nil, fmt.Errorf("resolve manifest path: %w", err)
	}
	m, _, err := manifest.LoadOrCreate(ctx, runCtx.manifestStore, manifestPath, schemaID)
	if err != nil {
		return nil, fmt.Errorf("load manifest %s: %w", manifestPath, err)
	}
	return manifest.FilterByTier(m, "delta"), nil
}

// splitNormalizableKeys resolves delta entries to bucket-relative keys and
// separates the ones this bucket's client cannot address.
func splitNormalizableKeys(bucket string, entries []manifest.FileEntry) (keys, unnormalizable []string) {
	for _, e := range entries {
		key, ok := NormalizeObjectKey(bucket, e.Path)
		if !ok {
			unnormalizable = append(unnormalizable, e.Path)
			continue
		}
		keys = append(keys, key)
	}
	return keys, unnormalizable
}

// purgeDeltaTier deletes the schema's purge set. It runs only after
// updateSchemaManifest committed the swap: the manifest already lists the
// new base and no delta, so a reader that resolves it now never touches
// these objects, and one that resolved the pre-swap manifest hits at most
// one missing-object failure before it re-resolves (there is no sighting
// grace here, unlike compaction's #461 GC). A failed delete is logged and
// surfaces as an error naming the key; the manifest is not rolled back —
// the object is unlisted garbage, not a consistency hazard.
func purgeDeltaTier(ctx context.Context, runCtx *initRunContext, state *schemaInitState) error {
	if len(state.deltaPurge) == 0 {
		return nil
	}
	if runCtx.dryRun {
		runCtx.logger.Info("dry-run: would delete delta objects after the base swap",
			zap.Int16("schema_id", state.schemaID),
			zap.Int("count", len(state.deltaPurge)),
			zap.Strings("keys", state.deltaPurge))
		return nil
	}
	if runCtx.deleteObject == nil {
		return fmt.Errorf("purge delta tier of schema %d: no S3 client to delete %d objects with", state.schemaID, len(state.deltaPurge))
	}

	var failed []string
	var errs []error
	for _, key := range state.deltaPurge {
		if err := runCtx.deleteObject(ctx, key); err != nil {
			runCtx.logger.Error("failed to delete superseded delta object; manifest swap is already published",
				zap.Int16("schema_id", state.schemaID), zap.String("key", key), zap.Error(err))
			failed = append(failed, key)
			errs = append(errs, err)
			continue
		}
		runCtx.logger.Info("deleted superseded delta object", zap.Int16("schema_id", state.schemaID), zap.String("key", key))
	}
	if len(failed) > 0 {
		return fmt.Errorf("purge delta tier of schema %d: %d of %d objects not deleted, manifest swap stays published (undeleted: %s): %w",
			state.schemaID, len(failed), len(state.deltaPurge), strings.Join(failed, ", "), errors.Join(errs...))
	}
	return nil
}
