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
// under the delta prefix the manifest does not mention, and two kinds of
// manifest entry a purge could not honour — ones that cannot be resolved to
// a key in this bucket (globs, foreign buckets) and ones that resolve to a
// key outside this schema's delta namespace (a base-shaped or other-schema
// path labelled delta), which the purge refuses to delete.
type deltaInventory struct {
	listed         []string
	unlisted       []string
	unnormalizable []string
	foreign        []string
}

func (inv deltaInventory) empty() bool {
	return len(inv.listed) == 0 && len(inv.unlisted) == 0 && len(inv.unnormalizable) == 0 && len(inv.foreign) == 0
}

// unpurgeable counts the manifest entries the purge could not honour.
func (inv deltaInventory) unpurgeable() int {
	return len(inv.unnormalizable) + len(inv.foreign)
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

// deltaNamespace is the key prefix the flusher writes a schema's delta
// objects under: `{deltaPrefix}/{schemaID}/` (BuildDeltaPath).
func deltaNamespace(deltaPrefix string, schemaID int16) string {
	return fmt.Sprintf("%s/%d/", strings.TrimSuffix(deltaPrefix, "/"), schemaID)
}

// ownedDeltaKey reports whether a manifest delta entry's key is one the
// purge may delete: a delta-shaped object inside this schema's delta
// namespace. The purge is irreversible, so a `Tier: "delta"` label alone is
// not trusted — a base-shaped key or another schema's delta under that label
// (a misclassified or hand-edited entry) must fail the run closed rather
// than be deleted. With no configured delta prefix the prefix part of the
// namespace is unconstrained, but the `/{schemaID}/{uuid}.parquet` tail is
// still required.
func ownedDeltaKey(deltaPrefix string, schemaID int16, key string) bool {
	if deltaPrefix != "" {
		return isDeltaShapedKey(deltaNamespace(deltaPrefix, schemaID), key)
	}
	dir := key[:strings.LastIndex(key, "/")+1] // "" for a bare file name
	schemaDir := fmt.Sprintf("%d/", schemaID)
	if dir != schemaDir && !strings.HasSuffix(dir, "/"+schemaDir) {
		return false
	}
	return isDeltaShapedKey(dir, key)
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
		return deltaInventory{}, fmt.Errorf("schema %d has %d manifest-listed and %d unlisted delta objects (%d entries a purge could not honour); rerun with --replace-delta to purge them after the base swap: %w",
			schemaID, len(inv.listed), len(inv.unlisted), inv.unpurgeable(), ErrDeltaTierPresent)
	}
	if runCtx.manifestStore == nil {
		return deltaInventory{}, fmt.Errorf("schema %d: --replace-delta needs a configured manifest template: the purge is ordered after the manifest swap, and without a manifest there is no swap to order it after", schemaID)
	}
	if err := refuseUnpurgeable(runCtx, schemaID, inv.unnormalizable, inv.foreign); err != nil {
		return deltaInventory{}, fmt.Errorf("%w; resolve them before --replace-delta", err)
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
		inv.listed, inv.unnormalizable, inv.foreign = classifyDeltaEntries(runCtx, schemaID, entries)
	}

	if runCtx.deltaPrefix == "" || runCtx.listObjectKeys == nil {
		runCtx.logger.Info("delta prefix listing skipped; only manifest-listed delta entries are inventoried",
			zap.Int16("schema_id", schemaID),
			zap.Bool("delta_prefix_set", runCtx.deltaPrefix != ""),
			zap.Bool("s3_client_set", runCtx.listObjectKeys != nil))
		return inv, nil
	}
	prefix := deltaNamespace(runCtx.deltaPrefix, schemaID)
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
	m, _, err := loadInitManifest(ctx, runCtx, schemaID, manifestPath)
	if err != nil {
		return nil, fmt.Errorf("load manifest delta entries: %w", err)
	}
	return manifest.FilterByTier(m, "delta"), nil
}

// classifyDeltaEntries resolves manifest delta entries to bucket-relative
// keys and separates the two kinds the purge must not touch: entries this
// bucket's client cannot address at all, and entries that address a key
// outside the schema's delta namespace (ownedDeltaKey).
func classifyDeltaEntries(runCtx *initRunContext, schemaID int16, entries []manifest.FileEntry) (keys, unnormalizable, foreign []string) {
	for _, e := range entries {
		key, ok := NormalizeObjectKey(runCtx.cfg.S3Bucket, e.Path)
		if !ok {
			unnormalizable = append(unnormalizable, e.Path)
			continue
		}
		if !ownedDeltaKey(runCtx.deltaPrefix, schemaID, key) {
			foreign = append(foreign, e.Path)
			continue
		}
		keys = append(keys, key)
	}
	return keys, unnormalizable, foreign
}

// refuseUnpurgeable is the fail-closed answer to manifest delta entries the
// purge could not honour, shared by the pre-flight and the publish reload.
func refuseUnpurgeable(runCtx *initRunContext, schemaID int16, unnormalizable, foreign []string) error {
	if len(unnormalizable) == 0 && len(foreign) == 0 {
		return nil
	}
	var reasons []string
	if len(unnormalizable) > 0 {
		reasons = append(reasons, fmt.Sprintf("%d cannot be addressed through bucket %q (glob or foreign-bucket paths: %s)",
			len(unnormalizable), runCtx.cfg.S3Bucket, strings.Join(unnormalizable, ", ")))
	}
	if len(foreign) > 0 {
		reasons = append(reasons, fmt.Sprintf("%d are not delta-shaped objects in this schema's delta namespace %s (%s)",
			len(foreign), deltaNamespace(runCtx.deltaPrefix, schemaID), strings.Join(foreign, ", ")))
	}
	return fmt.Errorf("schema %d: manifest lists delta entries this run refuses to delete: %s", schemaID, strings.Join(reasons, "; "))
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

// finishEmptySchema is the zero-live-rows tail of initSchema (#519). A
// schema whose every row is dead has no rows to export, yet its manifest
// may still list an old-typed base and delta tier. Without ReplaceDelta
// nothing changes (the pre-flight already refused a non-empty inventory).
// With it the run exports ONE zero-row base object and publishes it as the
// schema's whole base tier under the same CAS path as a populated schema —
// old base unlisted, delta tier emptied — and only then purges the
// inventoried delta objects, so the flag retires the stale tiers and the
// next init once rows exist no longer refuses.
//
// The zero-row object is not optional: a manifest that lists nothing sends
// every read to the legacy per-schema glob (manifest.QuerySource.Paths),
// which scans the unlisted old base and resurrects the rows its tombstones
// had deleted. Compaction keeps the same invariant when a merge yields no
// rows (a RowCount 0 base entry so the manifest never empties), and the
// object takes compaction's UUID-only key shape (BuildMergedBasePath): an
// init-shaped `{min}_{max}_{uuid}` name needs a row-id range, and a range-
// less stem is unclassifiable garbage for manifest-reconcile --gc. The
// unlisted old base objects are left for --gc, as after any re-init.
//
// A schema that lists nothing and has no delta objects has nothing to
// retire; no base is exported and no manifest is minted for it. The same
// holds for a run without a manifest template: there is no store to load a
// manifest from and no swap to publish (updateSchemaManifest no-ops on the
// populated path for the same reason), and the pre-flight only reaches this
// branch storeless with an empty inventory. The returned state carries the
// run's counts (nil when nothing was done).
func finishEmptySchema(ctx context.Context, runCtx *initRunContext, schemaID int16, inventory deltaInventory) (*schemaInitState, error) {
	if !runCtx.replaceDelta {
		return nil, nil
	}
	if runCtx.manifestStore == nil {
		runCtx.logger.Info("schema has no live rows and the run has no manifest template; no manifest to swap, nothing to retire",
			zap.Int16("schema_id", schemaID))
		return nil, nil
	}
	listed, err := countListedManifestEntries(ctx, runCtx, schemaID)
	if err != nil {
		return nil, fmt.Errorf("retire tiers of schema %d with no live rows: %w", schemaID, err)
	}
	state := &schemaInitState{schemaID: schemaID, deltaPurge: inventory.purgeKeys()}
	if listed == 0 && len(state.deltaPurge) == 0 {
		runCtx.logger.Info("schema has no live rows, lists nothing and has no delta objects; nothing to retire",
			zap.Int16("schema_id", schemaID))
		return nil, nil
	}
	// Cache was resolved and validated by the processInitSchemas pre-flight (#193).
	state.attrCache = runCtx.attrCaches[schemaID]
	runCtx.logger.Info("schema has no live rows; exporting a zero-row base to retire its listed tiers before the delta purge",
		zap.Int16("schema_id", schemaID),
		zap.Int("listed_entries", listed),
		zap.Int("delta_objects", len(state.deltaPurge)),
		zap.Bool("dry_run", runCtx.dryRun))
	if err := exportBatch(ctx, runCtx, state, buildEmptyBaseExport(runCtx, schemaID)); err != nil {
		return nil, fmt.Errorf("export zero-row base of schema %d with no live rows: %w", schemaID, err)
	}
	if err := publishAndPurge(ctx, runCtx, state); err != nil {
		return nil, fmt.Errorf("retire tiers of schema %d with no live rows: %w", schemaID, err)
	}
	return state, nil
}

// countListedManifestEntries returns how many entries the schema's manifest
// lists, zero for an absent manifest.
func countListedManifestEntries(ctx context.Context, runCtx *initRunContext, schemaID int16) (int, error) {
	manifestPath, err := runCtx.manifestResolver.Resolve(schemaID)
	if err != nil {
		return 0, fmt.Errorf("resolve manifest path: %w", err)
	}
	m, _, err := loadInitManifest(ctx, runCtx, schemaID, manifestPath)
	if err != nil {
		return 0, fmt.Errorf("count listed manifest entries: %w", err)
	}
	return len(m.Files), nil
}

// buildEmptyBaseExport is the batch for the zero-row base object of an
// emptied schema (#519): no row ids, no row-id range, a fresh write-once key
// in compaction's merged-base shape (see finishEmptySchema), with tmp and
// final sharing the UUID as every init batch does.
func buildEmptyBaseExport(runCtx *initRunContext, schemaID int16) schemaBatchExport {
	fileUUID := uuid.Must(uuid.NewV7()).String()
	tmpKey := BuildBaseTempPath(runCtx.cfg.S3Prefix, schemaID, fileUUID)
	return schemaBatchExport{
		tmpKey:    tmpKey,
		finalKey:  BuildMergedBasePath(runCtx.cfg.S3Prefix, schemaID, fileUUID),
		s3TmpPath: fmt.Sprintf("s3://%s/%s", runCtx.cfg.S3Bucket, tmpKey),
	}
}
