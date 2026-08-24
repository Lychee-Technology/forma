package reconcile

import (
	"fmt"
	"io"
)

// SchemaReport is one schema's reconcile outcome. Key slices hold
// bucket-relative keys except Unverifiable, which preserves the manifest
// path verbatim (the raw path is what the operator must inspect).
type SchemaReport struct {
	SchemaID     int16
	Skipped      bool // advisory lock not acquired; nothing inspected
	DeltaOrphans []string
	BaseOrphans  []string
	TmpOrphans   []string
	Unknown      []string // unrecognized shapes; reported, never repaired or deleted
	Dangling     []string // manifest entries with no live object; removal stays manual
	Unverifiable []string // manifest paths this listing cannot prove absent
	// ObjectsSeen counts this schema's classified parquet objects in
	// storage; ManifestEntries counts its manifest's file entries as
	// loaded (#463). Together they distinguish "the manifest genuinely
	// lists nothing" from "the manifest failed to resolve" — N objects
	// against 0 entries is the mis-pointed-template signature --gc refuses.
	ObjectsSeen     int
	ManifestEntries int
	Repaired     []string // delta orphans appended to the manifest (--repair)
	// DeltaLeftovers are delta orphans the repair guard classified as
	// compaction leftovers (no uncovered rows, or every uncovered row
	// deleted in Postgres): never appended, GC-eligible under --gc.
	DeltaLeftovers []string
	Deleted        []string // leftover/merged-base/tmp orphans removed (--gc)
	// PromotedBase are init-shaped base orphans promoted into the manifest
	// base tier after the coverage + eviction-safety proof (--repair, #292).
	PromotedBase []string
	// InitPromotionRefusal explains why an init-shaped orphan set was NOT
	// promoted. Promotion is all-or-nothing over the set, so one reason
	// covers every file; empty when promotion succeeded or never ran. The
	// refused files stay ordinary GC candidates.
	InitPromotionRefusal string
	// StampDivergences are listed entries whose stamp no longer matches the
	// object footer (--verify-stamps); each is a byte-truth breach requiring
	// operator action — restamp via rewrite, or investigate the overwrite.
	StampDivergences []string
	// ChecksumDivergences are listed entries whose object bytes no longer hash
	// to the entry's #347 content stamp (--verify-checksums): silent
	// corruption, actionable exactly like a stamp divergence.
	ChecksumDivergences []string
	// SkippedUnstamped counts entries --verify-checksums could not cover
	// because they carry no checksum — either legacy (never backfilled) or a
	// best-effort write-side hash that failed. It is coverage observability,
	// not a discrepancy: it never affects the exit code, but a "clean"
	// verdict over mostly-unstamped entries means far less than a clean
	// verdict over stamped ones.
	SkippedUnstamped int
	Err              error // per-schema failure; other schemas still reconcile
}

// Report is a full reconcile run across schemas.
type Report struct {
	Schemas []SchemaReport
}

// HasResidualDiscrepancies reports whether anything actionable is left after
// repair and GC: orphans not repaired, promoted, or deleted, dangling entries, skipped
// schemas, unknown shapes, stamp divergences (--verify-stamps), checksum
// divergences (--verify-checksums), or per-schema failures. Unverifiable paths
// are informational — they cannot be proven inconsistent from this run, and so
// is the unstamped-entry count, which reports missing coverage rather than a
// discrepancy.
func (r Report) HasResidualDiscrepancies() bool {
	for _, s := range r.Schemas {
		if s.Residual() {
			return true
		}
	}
	return false
}

// Residual reports whether this schema still has actionable discrepancies
// after repair and GC.
func (s SchemaReport) Residual() bool {
	if s.Skipped || s.Err != nil {
		return true
	}
	// Stamp and checksum divergences are actionable but are not orphan keys,
	// so they can never be "resolved" by the repair/GC bookkeeping below.
	if len(s.Dangling) > 0 || len(s.Unknown) > 0 ||
		len(s.StampDivergences) > 0 || len(s.ChecksumDivergences) > 0 {
		return true
	}
	resolved := make(map[string]struct{}, len(s.Repaired)+len(s.Deleted)+len(s.PromotedBase))
	for _, k := range s.Repaired {
		resolved[k] = struct{}{}
	}
	for _, k := range s.Deleted {
		resolved[k] = struct{}{}
	}
	for _, k := range s.PromotedBase {
		resolved[k] = struct{}{}
	}
	for _, orphans := range [][]string{s.DeltaOrphans, s.BaseOrphans, s.TmpOrphans} {
		for _, k := range orphans {
			if _, ok := resolved[k]; !ok {
				return true
			}
		}
	}
	return false
}

// Render writes the human-readable report: exact keys per class per schema.
func (r Report) Render(w io.Writer) {
	for _, s := range r.Schemas {
		if s.Skipped {
			fmt.Fprintf(w, "schema %d: skipped (advisory lock not acquired)\n", s.SchemaID)
			continue
		}
		if s.clean() {
			// A clean verdict still reports the inventory and any coverage
			// gap: "clean" over entries the scrub could not check is a
			// weaker statement.
			parenthetical := fmt.Sprintf("%d objects, %d manifest entries", s.ObjectsSeen, s.ManifestEntries)
			if note := s.unstampedNote(); note != "" {
				parenthetical += "; " + note
			}
			fmt.Fprintf(w, "schema %d: clean (%s)\n", s.SchemaID, parenthetical)
			continue
		}
		fmt.Fprintf(w, "schema %d:\n", s.SchemaID)
		fmt.Fprintf(w, "  inventory: %d objects in storage, %d manifest entries resolved; orphan candidates: delta=%d base=%d tmp=%d unknown=%d\n",
			s.ObjectsSeen, s.ManifestEntries, len(s.DeltaOrphans), len(s.BaseOrphans), len(s.TmpOrphans), len(s.Unknown))
		renderKeys(w, "delta orphan", s.DeltaOrphans)
		renderKeys(w, "delta leftover (gc-eligible)", s.DeltaLeftovers)
		renderKeys(w, "base orphan", s.BaseOrphans)
		renderKeys(w, "tmp orphan", s.TmpOrphans)
		renderKeys(w, "unknown shape", s.Unknown)
		renderKeys(w, "dangling entry", s.Dangling)
		renderKeys(w, "unverifiable entry", s.Unverifiable)
		renderKeys(w, "stamp divergence", s.StampDivergences)
		renderKeys(w, "checksum divergence", s.ChecksumDivergences)
		renderKeys(w, "repaired", s.Repaired)
		renderKeys(w, "promoted base-init", s.PromotedBase)
		if s.InitPromotionRefusal != "" {
			fmt.Fprintf(w, "  init promotion refused: %s\n", s.InitPromotionRefusal)
		}
		renderKeys(w, "deleted", s.Deleted)
		if note := s.unstampedNote(); note != "" {
			fmt.Fprintf(w, "  %s\n", note)
		}
		if s.Err != nil {
			fmt.Fprintf(w, "  error: %v\n", s.Err)
		}
	}
}

func (s SchemaReport) clean() bool {
	return len(s.DeltaOrphans)+len(s.DeltaLeftovers)+len(s.BaseOrphans)+
		len(s.TmpOrphans)+len(s.Unknown)+len(s.Dangling)+len(s.Unverifiable)+
		len(s.Repaired)+len(s.PromotedBase)+len(s.Deleted)+
		len(s.StampDivergences)+len(s.ChecksumDivergences) == 0 &&
		s.Err == nil && s.InitPromotionRefusal == ""
}

// unstampedNote renders the --verify-checksums coverage gap, empty when the
// scrub had nothing it could not cover. It is deliberately not part of
// clean(): missing coverage is a caveat on the verdict, never a discrepancy
// that changes it.
func (s SchemaReport) unstampedNote() string {
	switch s.SkippedUnstamped {
	case 0:
		return ""
	case 1:
		return "1 unstamped entry skipped"
	default:
		return fmt.Sprintf("%d unstamped entries skipped", s.SkippedUnstamped)
	}
}

func renderKeys(w io.Writer, label string, keys []string) {
	for _, k := range keys {
		fmt.Fprintf(w, "  %s: %s\n", label, k)
	}
}
