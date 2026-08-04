package reconcile

import (
	"fmt"
	"strings"
	"time"

	"github.com/lychee-technology/forma/internal/manifest"
)

// The two object-date fences guarding an init-orphan promotion (#292). Both
// exist because the version anti-join accepts an equal changed_at as
// superseding and cannot see values, while the read path's equal-changed_at
// tie-break is deterministically base-wins (#183). checkEvictionDates covers
// the entries a promotion REMOVES; checkSurvivorDates covers the ones it
// LEAVES BEHIND. Strict `>` on the version predicate is not an alternative:
// unchanged rows tie across generations, so it would refuse every promotion
// facing a non-empty inventory.

// replacementContext carries the run-level object facts the two date fences
// need, alongside the manifest inventory: this run's S3 listing
// (bucket-relative key -> object) and the newest LastModified across the
// promoted init set. See verifyEvictionSafety (entries leaving) and
// checkSurvivorDates (entries staying) for what the dates prove.
type replacementContext struct {
	listed         map[string]ObjectInfo
	maxInitWritten time.Time
}

// checkSurvivorDates fences the OTHER half of the equal-changed_at tie: the
// non-base entries that SURVIVE the splice. checkEvictionDates only covers
// entries the promotion evicts, which leaves this reachable regression: the
// manifest has no base tier at all, a listed delta carries `R=new@T` (a
// post-snapshot same-millisecond write, flushed after the failed init), and
// the init set holds `R=old@T`. Promotion publishes `old` as base, and the
// read path's deterministic tie-break at equal changed_at is BASE-WINS (#183,
// `deleted_at` 0-vs-NULL) — so reads regress to `old` with no probe able to
// see it. The mirror image is resurrection: a surviving tombstone `R@T` loses
// the tie to a promoted LIVE `R@T` and the delete comes back.
//
// Every listed non-base entry is fenced, not merely those the resurrection
// probe masks against: an entry this bucket cannot resolve is skipped by the
// probe but stays LISTED after the splice, so it still ties at read time.
// An entry passes on either of two independent grounds:
//
//  1. Its object is strictly OLDER than every promoted init object. cdc-init
//     holds the per-schema advisory lock for its whole run and the flusher
//     needs that same lock, so no delta object can be written while init runs.
//     A pre-export object therefore holds only pre-snapshot writes, and for
//     those an equal changed_at on one row id is the same ltbase_updated_at
//     stamping event ⇒ value-identical, so the tie is harmless.
//
//  2. Its version range is strictly ABOVE the promoted set's: CreatedMin >
//     max(CreatedMax) over the init entries. A strictly-newer range cannot tie
//     with any init row at all, so the survivor wins LWW legitimately. This is
//     the lost-delete convergence shape — the restored tombstone delta is
//     necessarily written after the failed init, and a flat "refuse any
//     post-init survivor" rule would deadlock it forever.
//
// Ground 2 trusts the survivor entry's own CreatedMin metadata. That is a
// TIMING-safety argument about system writers (flusher, compactor), not a
// tamper-proof one; detecting metadata that disagrees with the bytes is
// --verify-stamps' job, not this fence's.
func (r *Reconciler) checkSurvivorDates(m *manifest.Manifest, cov initCoverage, rc replacementContext) string {
	// Byte truth on the init side: CreatedMax was recomputed from the parquet
	// stats, never read from a manifest entry.
	var maxInitChangedAt int64
	for _, e := range cov.entries {
		if e.CreatedMax > maxInitChangedAt {
			maxInitChangedAt = e.CreatedMax
		}
	}
	for _, f := range m.Files {
		if strings.EqualFold(f.Tier, "base") {
			continue // evicted, not surviving: checkEvictionDates owns these
		}
		key, ok := normalizeKey(r.Bucket, f.Path)
		if !ok {
			return undatableSurvivor(f.Path)
		}
		obj, inListing := rc.listed[key]
		if !inListing {
			return undatableSurvivor(f.Path)
		}
		if obj.LastModified.Before(rc.maxInitWritten) {
			continue // ground 1: pre-export content, ties are value-identical
		}
		if f.CreatedMin > 0 && f.CreatedMin > maxInitChangedAt {
			continue // ground 2: strictly newer range, cannot tie at all
		}
		return fmt.Sprintf("surviving non-base entry %s was written no earlier than the init export (%s vs %s) and its version range does not start strictly above the promoted set (created_min %d vs max changed_at %d); at equal changed_at the promoted base would silently win the read-path tie-break",
			key, obj.LastModified.UTC().Format(time.RFC3339), rc.maxInitWritten.UTC().Format(time.RFC3339),
			f.CreatedMin, maxInitChangedAt)
	}
	return ""
}

// undatableSurvivor is the refusal for a surviving non-base entry this run
// cannot date at all — a glob or foreign-bucket path, or a key missing from
// this run's listing.
func undatableSurvivor(path string) string {
	return fmt.Sprintf("listed non-base entry %s cannot be dated against the init export; a surviving entry that ties with the promoted set at equal changed_at would silently win the read-path tie-break", path)
}

// checkEvictionDates is verifyEvictionSafety's part-1 fence: every base entry
// about to be evicted must be datable from this run's listing AND written
// STRICTLY EARLIER than the promoted init set. See verifyEvictionSafety for
// the proof this implements.
//
// Strictly earlier — not merely "no later" — closes the second-granularity
// residual: S3 LastModified is second-granular, so an equal-second base entry
// could still hide a lock release, delta flush and compaction that all landed
// inside the init export's final second. Requiring `Before` makes the fence
// unconditional at the cost of conservative false refusals exactly on the
// second boundary; the fallback for those is re-running cdc-init.
func checkEvictionDates(evicted []string, rc replacementContext) string {
	for _, key := range evicted {
		obj, ok := rc.listed[key]
		if !ok {
			return fmt.Sprintf("listed base entry %s is absent from this run's object listing; cannot date it against the init export", key)
		}
		if !obj.LastModified.Before(rc.maxInitWritten) {
			return fmt.Sprintf("listed base entry %s postdates the init export (%s >= %s); it may fold post-export same-millisecond writes, so equal-timestamp coverage cannot prove value identity",
				key, obj.LastModified.UTC().Format(time.RFC3339), rc.maxInitWritten.UTC().Format(time.RFC3339))
		}
	}
	return ""
}
