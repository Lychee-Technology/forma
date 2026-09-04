package reconcile

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"go.uber.org/zap"

	"github.com/lychee-technology/forma/internal/cdc"
	"github.com/lychee-technology/forma/internal/compaction"
	"github.com/lychee-technology/forma/internal/manifest"
)

// promoteLiveCheckBatch bounds one MissingLiveRows query's id-list size so a
// large init batch does not turn into a multi-megabyte SQL parameter.
const promoteLiveCheckBatch = 10000

// initCoverage is verifyInitCoverage's proof output: the base-tier entries
// ready to splice, plus, per file, the row ids the coverage count found DEAD
// in Postgres. Dead ids do not count toward coverage, but they are exactly
// the resurrection candidates verifyNoResurrection must clear before the
// set may replace the base tier. deadByFile is empty in the common
// quick-recovery case (init published nothing, nothing deleted since), and
// then the resurrection guard costs zero extra queries.
type initCoverage struct {
	entries    []manifest.FileEntry
	deadByFile map[string][]string
}

// verifyInitCoverage proves an init-shaped orphan set is a complete init
// export: per-file stats readable and consistent with the {min}_{max}[_{uuid}] name,
// pairwise-disjoint row ranges (one run exports disjoint ordered batches, so
// an overlap means the set mixes init generations), no tombstones (cdc-init
// exports live rows only), and — the #292 completeness proof — the set's
// distinct row ids cover every live entity_main row. Ranges disjoint means
// per-file distinct row sets are disjoint, so per-file live-match counts sum
// to the set's distinct live coverage; equality with LiveRowCount proves
// full coverage. Any failure to prove returns a refusal reason and the set
// stays on the GC path — always safe, because re-running cdc-init rebuilds
// the base tier from entity_main.
//
// The two sides of the identity are read without a shared PG snapshot; any
// concurrent write skews them toward inequality (refusal), and post-snapshot
// inserts are hot-masked via change_log — the same contract cdc-init's own
// batched export relies on.
//
// Coverage alone is NOT sufficient to promote: it says nothing about rows
// deleted since the export. verifyNoResurrection completes the proof.
func (r *Reconciler) verifyInitCoverage(ctx context.Context, schemaID int16, orphans []ObjectInfo) (initCoverage, string) {
	liveCount, err := r.LiveRows.LiveRowCount(ctx, schemaID)
	if err != nil {
		return initCoverage{}, fmt.Sprintf("live-row count unavailable: %v", err)
	}
	if liveCount == 0 {
		return initCoverage{}, "schema has no live rows; an init-shaped orphan set is necessarily superseded"
	}

	entries := make([]manifest.FileEntry, 0, len(orphans))
	for _, obj := range orphans {
		stats, err := r.Stats.FileStats(ctx, obj.Key)
		if err != nil {
			return initCoverage{}, fmt.Sprintf("unreadable parquet stats for %s: %v", obj.Key, err)
		}
		if refusal := checkInitFilenameStats(obj.Key, stats); refusal != "" {
			return initCoverage{}, refusal
		}
		entries = append(entries, manifest.FileEntry{
			Tier:       "base",
			Path:       obj.Key,
			RowIDMin:   strings.ToLower(stats.RowIDMin),
			RowIDMax:   strings.ToLower(stats.RowIDMax),
			CreatedMin: stats.CreatedMin,
			CreatedMax: stats.CreatedMax,
			RowCount:   stats.RowsOut,
			SizeBytes:  obj.Size,
		})
	}
	// Canonical lowercase keeps range ordering and cross-checks uniform
	// regardless of reader casing.
	if refusal := checkDisjointRanges(entries); refusal != "" {
		return initCoverage{}, refusal
	}

	cov := initCoverage{entries: entries, deadByFile: map[string][]string{}}
	var matched int64
	for _, e := range entries {
		n, dead, refusal := r.countLiveRowsInFile(ctx, schemaID, e.Path)
		if refusal != "" {
			return initCoverage{}, refusal
		}
		if len(dead) > 0 {
			cov.deadByFile[e.Path] = dead
		}
		matched += n
	}
	if matched != liveCount {
		return initCoverage{}, fmt.Sprintf("orphan set covers %d of %d live rows; a partial init export must not replace the base tier", matched, liveCount)
	}
	return cov, ""
}

// checkInitFilenameStats cross-checks the {min}_{max} row range in the
// filename (write-once {min}_{max}_{uuid} or legacy {min}_{max}) against the
// recomputed parquet stats. Entry metadata is never trusted from filenames
// (mirrors buildRepairEntry); the check exists so a foreign or truncated
// file that merely looks init-shaped cannot enter the base tier.
func checkInitFilenameStats(key string, stats compaction.MergeStats) string {
	base := key[strings.LastIndex(key, "/")+1:]
	stem := strings.TrimSuffix(base, ".parquet")
	minID, maxID, found := cdc.ParseInitBaseStem(stem)
	if !found {
		// Unreachable after classifyObjectKey, kept as a defensive refusal.
		return fmt.Sprintf("%s is not an init-shaped key", key)
	}
	if !strings.EqualFold(minID, stats.RowIDMin) || !strings.EqualFold(maxID, stats.RowIDMax) {
		return fmt.Sprintf("%s row range %s_%s does not match parquet contents %s_%s",
			key, minID, maxID, stats.RowIDMin, stats.RowIDMax)
	}
	return ""
}

// checkDisjointRanges refuses overlapping [RowIDMin, RowIDMax] ranges.
// Canonical lowercase UUID text ordering equals uuid byte ordering — the
// same ordering Postgres used to batch the export — so plain string
// comparison is sound.
func checkDisjointRanges(entries []manifest.FileEntry) string {
	sorted := append([]manifest.FileEntry(nil), entries...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].RowIDMin < sorted[j].RowIDMin })
	for i := 1; i < len(sorted); i++ {
		if sorted[i].RowIDMin <= sorted[i-1].RowIDMax {
			return fmt.Sprintf("row ranges of %s and %s overlap; the set mixes init generations and would put duplicate row versions in the base tier",
				sorted[i-1].Path, sorted[i].Path)
		}
	}
	return ""
}

// countLiveRowsInFile returns how many of the file's distinct row ids are
// live in Postgres, plus the ids that are dead. UncoveredRows with no listed
// files enumerates every distinct row id with its tombstone flag (#289
// machinery, reused verbatim).
func (r *Reconciler) countLiveRowsInFile(ctx context.Context, schemaID int16, key string) (int64, []string, string) {
	rows, err := r.Stats.UncoveredRows(ctx, key, nil)
	if err != nil {
		return 0, nil, fmt.Sprintf("enumerate rows of %s: %v", key, err)
	}
	rowIDs := make([]string, 0, len(rows))
	for _, row := range rows {
		if row.Tombstone {
			return 0, nil, fmt.Sprintf("%s carries tombstone rows; cdc-init exports live rows only, so this is not an init output", key)
		}
		rowIDs = append(rowIDs, row.RowID)
	}
	var dead []string
	for start := 0; start < len(rowIDs); start += promoteLiveCheckBatch {
		end := start + promoteLiveCheckBatch
		if end > len(rowIDs) {
			end = len(rowIDs)
		}
		missing, err := r.LiveRows.MissingLiveRows(ctx, schemaID, rowIDs[start:end])
		if err != nil {
			return 0, nil, fmt.Sprintf("check live rows of %s: %v", key, err)
		}
		dead = append(dead, missing...)
	}
	return int64(len(rowIDs) - len(dead)), dead, ""
}

// promotionColumns best-effort probes the parquet footer for the #256
// column stamp, mirroring cdc-init's initStampColumns: failure leaves the
// entry unstamped (readers fall back to probing) and never blocks promotion.
func (r *Reconciler) promotionColumns(ctx context.Context, schemaID int16, key string) map[string]string {
	cols, err := r.Stats.FileColumns(ctx, key)
	if err != nil {
		r.Logger.Warn("failed to describe init orphan; promoted entry stays unstamped",
			zap.Int16("schema_id", schemaID), zap.String("key", key), zap.Error(err))
		return nil
	}
	return cols
}

// stampPromotedEntries puts the #256 column set and the #347 content checksum
// on every entry about to be spliced into the base tier. Both stamps are
// best-effort and independent: either may fail without costing the entry the
// other, or the set its promotion.
//
// WHERE this runs is load-bearing, not incidental. The checksum stamp is a
// FULL GET of every object in the init export set — the most expensive thing
// this pass does — so it runs only on an attempt that has already cleared
// verifyReplacementSafety: a promotion the survivor fence, the eviction proof,
// or the resurrection guard refuses downloads nothing at all. The caller's
// once-guard covers the other side: an etag conflict changes the manifest, not
// the parquet objects these stamps describe, so a retry re-proves the
// inventory guards but never re-reads the bytes (the same reasoning as
// repair's entryCache).
func (r *Reconciler) stampPromotedEntries(ctx context.Context, schemaID int16, entries []manifest.FileEntry) {
	for i := range entries {
		entries[i].Columns = r.promotionColumns(ctx, schemaID, entries[i].Path)
		entries[i].Checksum = r.stampChecksum(ctx, schemaID, entries[i].Path,
			"failed to checksum promoted orphan; entry stays unstamped")
	}
}

// promotionResult is promoteInitOrphans' per-schema outcome: either every
// orphan promoted, or none plus the refusal reason.
type promotionResult struct {
	promoted []string
	refusal  string
}

// promoteInitOrphans verifies and, on proof, promotes the ENTIRE init-shaped
// orphan set into the manifest base tier with ReplaceTierFiles semantics
// (manifest.SpliceTierFiles): cdc-init is a full re-export, so a proven
// complete set IS the base tier's canonical inventory — exactly the manifest
// the failed publish would have written (#292). On refusal the set stays on
// the #290 GC path and the reason is reported. Runs under the schema
// advisory lock; the save still goes through etag optimistic concurrency
// because compaction does not take the lock. Eviction safety is re-proven
// on every retry: a 412 means the inventory changed under us. The entry
// stamps (stampPromotedEntries) are taken lazily instead — once, on the first
// attempt that clears those proofs — so a refused promotion pays no object
// reads and a retry does not re-read them. Returns the (possibly reloaded)
// manifest and etag so the delta-repair pass that follows works against the
// post-promotion inventory.
func (r *Reconciler) promoteInitOrphans(ctx context.Context, schemaID int16, m *manifest.Manifest, etag string, orphans []ObjectInfo, listed map[string]ObjectInfo) (promotionResult, *manifest.Manifest, string, error) {
	if r.Stats == nil || r.LiveRows == nil {
		return promotionResult{}, nil, "", fmt.Errorf(
			"init promotion requested for schema %d but stats reader or live-row checker is not configured", schemaID)
	}
	// The whole orphan set is written by one cdc-init run, so its newest
	// object time is the instant that run stopped writing — the fence the
	// eviction proof dates listed base objects against.
	rc := replacementContext{listed: listed}
	for _, o := range orphans {
		if o.LastModified.After(rc.maxInitWritten) {
			rc.maxInitWritten = o.LastModified
		}
	}
	cov, refusal := r.verifyInitCoverage(ctx, schemaID, orphans)
	if refusal != "" {
		r.Logger.Warn("refusing init orphan promotion; set stays GC-eligible",
			zap.Int16("schema_id", schemaID), zap.String("reason", refusal))
		return promotionResult{refusal: refusal}, m, etag, nil
	}

	stamped := false
	for attempt := 0; ; attempt++ {
		if refusal := r.verifyReplacementSafety(ctx, m, cov, rc); refusal != "" {
			r.Logger.Warn("refusing init orphan promotion; set stays GC-eligible",
				zap.Int16("schema_id", schemaID), zap.String("reason", refusal))
			return promotionResult{refusal: refusal}, m, etag, nil
		}
		if !stamped {
			r.stampPromotedEntries(ctx, schemaID, cov.entries)
			stamped = true
		}
		manifest.SpliceTierFiles(m, "base", cov.entries)
		newETag, err := r.Manifests.Save(ctx, schemaID, m, etag)
		if err == nil {
			promoted := make([]string, 0, len(cov.entries))
			for _, e := range cov.entries {
				promoted = append(promoted, e.Path)
				r.Logger.Info("promoted init-shaped orphan into base tier",
					zap.Int16("schema_id", schemaID), zap.String("key", e.Path), zap.Int64("row_count", e.RowCount))
			}
			return promotionResult{promoted: promoted}, m, newETag, nil
		}
		if !compaction.IsConcurrentModification(err) {
			return promotionResult{}, nil, "", fmt.Errorf("save schema %d manifest after init promotion: %w", schemaID, err)
		}
		if attempt >= r.Opts.MaxETagRetries {
			return promotionResult{}, nil, "", fmt.Errorf(
				"save schema %d manifest after init promotion: still conflicting after %d optimistic-concurrency retries: %w",
				schemaID, r.Opts.MaxETagRetries, err)
		}
		if m, etag, err = r.Manifests.Load(ctx, schemaID); err != nil {
			return promotionResult{}, nil, "", fmt.Errorf("reload schema %d manifest after save conflict: %w", schemaID, err)
		}
	}
}

// verifyReplacementSafety runs the three proofs that depend on the CURRENT
// manifest inventory rather than on the parquet contents alone, so all must
// be re-run on every save attempt: a 412 means the inventory changed under
// us, and a reloaded manifest may have gained or lost base AND delta entries.
//
// The survivor fence runs first: it is pure arithmetic over the manifest and
// this run's listing, so refusing there costs no probe at all.
func (r *Reconciler) verifyReplacementSafety(ctx context.Context, m *manifest.Manifest, cov initCoverage, rc replacementContext) string {
	if refusal := r.checkSurvivorDates(m, cov, rc); refusal != "" {
		return refusal
	}
	if refusal := r.verifyEvictionSafety(ctx, m, cov.entries, rc); refusal != "" {
		return refusal
	}
	return r.verifyNoResurrection(ctx, m, cov)
}

// verifyNoResurrection refuses a set whose promotion would revive a deleted
// row — the same resurrection rule classifyDeltaOrphan applies to delta
// orphans (repair.go), which coverage counting alone misses because dead
// rows simply do not count toward the identity.
//
// The reachable failure: a row is deleted AFTER the failed init export, its
// tombstone delta is flushed and then compacted away (a merged base drops
// tombstoned rows — merge_sql.go's WHERE deleted_at IS NULL OR 0), and its
// change_log row is already marked flushed so the hot tier does not mask it.
// The init file still holds that row's stale LIVE version. Coverage passes
// (the dead row is not counted) and eviction safety passes (the row is
// absent from the evicted base), yet promoting re-lists the live version and
// the delete is undone.
//
// The probe therefore masks each init file against the SURVIVING DELTA
// entries only. The init set itself and its siblings are excluded — they are
// the files under suspicion, and including them would make the check
// vacuously pass. Evicted base entries are excluded too: they are leaving,
// so they can mask nothing after the splice.
func (r *Reconciler) verifyNoResurrection(ctx context.Context, m *manifest.Manifest, cov initCoverage) string {
	if len(cov.deadByFile) == 0 {
		return "" // nothing deleted since the export: no probe needed
	}
	deltaKeys := survivingDeltaKeys(r.Bucket, m)
	for _, e := range cov.entries {
		dead := cov.deadByFile[e.Path]
		if len(dead) == 0 {
			continue
		}
		deadSet := make(map[string]bool, len(dead))
		for _, id := range dead {
			deadSet[id] = true
		}
		uncovered, err := r.Stats.UncoveredRows(ctx, e.Path, deltaKeys)
		if err != nil {
			return fmt.Sprintf("cannot prove %s resurrects no deleted rows: %v", e.Path, err)
		}
		for _, row := range uncovered {
			if deadSet[row.RowID] {
				return fmt.Sprintf("%s carries a live version of deleted row %s that no surviving delta supersedes; promoting would resurrect it", e.Path, row.RowID)
			}
		}
	}
	return ""
}

// survivingDeltaKeys returns the bucket-relative keys of the manifest's
// non-base entries — the only listed files that outlive a base-tier
// replacement. Paths this bucket's reader cannot resolve (globs,
// foreign-bucket URIs) are skipped: an unreadable file must never be counted
// as masking coverage.
func survivingDeltaKeys(bucket string, m *manifest.Manifest) []string {
	keys := make([]string, 0, len(m.Files))
	for _, f := range m.Files {
		if strings.EqualFold(f.Tier, "base") {
			continue
		}
		if key, ok := normalizeKey(bucket, f.Path); ok {
			keys = append(keys, key)
		}
	}
	return keys
}

// verifyEvictionSafety proves that replacing the whole base tier loses no
// row versions: every version in every evicted (currently listed) base
// entry must be superseded by the promoted set or a surviving delta entry.
// This closes the compaction race the issue's coverage proof alone misses:
// a base-{uuid} entry merged after the failed init carries re-flushed
// versions NEWER than the orphan set's; wholesale replacement would orphan
// those versions and readers would regress to stale values, because their
// change_log rows are already marked flushed (no hot-tier masking).
//
// The proof has two parts, because the version probe's coverage predicate is
// `l.changed_at >= o.changed_at` — equal-timestamp is accepted as superseding,
// and the probe cannot see values. Strict `>` is not an option: unchanged rows
// tie at equal changed_at across generations, so it would refuse every
// promotion facing a non-empty base tier. The tie is therefore fenced by
// OBJECT DATE instead:
//
//  1. Evicted base entries whose object is not STRICTLY OLDER than the
//     promoted set are refused outright. cdc-init holds the per-schema
//     advisory lock for its whole run and the flusher needs that same lock, so
//     a delta carrying any write made after init's snapshot read can only be
//     flushed AFTER the failed init released the lock — after every init
//     object was written. Any base entry folding such a delta therefore has a
//     later LastModified. Refusing those is what stops a same-millisecond
//     post-snapshot write (equal changed_at, different value) from being
//     "covered" by the stale init version and silently regressing readers.
//
//  2. For entries strictly older than the promoted set, `>=` coverage IS
//     sound. Such an object can only contain pre-lock writes, and for those an
//     equal-changed_at tie means the SAME Postgres write: changed_at is the
//     row's ltbase_updated_at stamp, so identical stamps on one row id are one
//     stamping event, hence value-identical. Nothing is lost by evicting it.
//
// The date fence runs BEFORE the version probe: it is cheap, and a base entry
// this run cannot date at all (absent from the listing) is refused rather than
// probed. Conservative refusals (dangling base entry, unreadable file,
// undatable entry, equal-second entry) are fine — the documented fallback is
// re-running cdc-init.
func (r *Reconciler) verifyEvictionSafety(ctx context.Context, m *manifest.Manifest, entries []manifest.FileEntry, rc replacementContext) string {
	survivors := make([]string, 0, len(entries)+len(m.Files))
	for _, e := range entries {
		survivors = append(survivors, e.Path)
	}
	var evicted []string
	for _, f := range m.Files {
		isBase := strings.EqualFold(f.Tier, "base")
		key, ok := normalizeKey(r.Bucket, f.Path)
		if !ok {
			if isBase {
				return fmt.Sprintf("listed base entry %s cannot be verified from this bucket; refusing wholesale base replacement", f.Path)
			}
			continue
		}
		if isBase {
			evicted = append(evicted, key)
			continue
		}
		survivors = append(survivors, key)
	}
	if refusal := checkEvictionDates(evicted, rc); refusal != "" {
		return refusal
	}
	for _, key := range evicted {
		uncovered, err := r.Stats.UncoveredRows(ctx, key, survivors)
		if err != nil {
			return fmt.Sprintf("cannot prove eviction safety of listed base entry %s: %v", key, err)
		}
		if len(uncovered) > 0 {
			return fmt.Sprintf("evicting listed base entry %s would lose %d row versions the promoted set does not supersede", key, len(uncovered))
		}
	}
	return ""
}
