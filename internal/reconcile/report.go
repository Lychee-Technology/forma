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
	Err                  error // per-schema failure; other schemas still reconcile
}

// Report is a full reconcile run across schemas.
type Report struct {
	Schemas []SchemaReport
}

// HasResidualDiscrepancies reports whether anything actionable is left after
// repair and GC: orphans not repaired, promoted, or deleted, dangling entries, skipped
// schemas, unknown shapes, or per-schema failures. Unverifiable paths are
// informational — they cannot be proven inconsistent from this run.
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
	if len(s.Dangling) > 0 || len(s.Unknown) > 0 {
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
			fmt.Fprintf(w, "schema %d: clean\n", s.SchemaID)
			continue
		}
		fmt.Fprintf(w, "schema %d:\n", s.SchemaID)
		renderKeys(w, "delta orphan", s.DeltaOrphans)
		renderKeys(w, "delta leftover (gc-eligible)", s.DeltaLeftovers)
		renderKeys(w, "base orphan", s.BaseOrphans)
		renderKeys(w, "tmp orphan", s.TmpOrphans)
		renderKeys(w, "unknown shape", s.Unknown)
		renderKeys(w, "dangling entry", s.Dangling)
		renderKeys(w, "unverifiable entry", s.Unverifiable)
		renderKeys(w, "repaired", s.Repaired)
		renderKeys(w, "promoted base-init", s.PromotedBase)
		if s.InitPromotionRefusal != "" {
			fmt.Fprintf(w, "  init promotion refused: %s\n", s.InitPromotionRefusal)
		}
		renderKeys(w, "deleted", s.Deleted)
		if s.Err != nil {
			fmt.Fprintf(w, "  error: %v\n", s.Err)
		}
	}
}

func (s SchemaReport) clean() bool {
	return len(s.DeltaOrphans)+len(s.DeltaLeftovers)+len(s.BaseOrphans)+
		len(s.TmpOrphans)+len(s.Unknown)+len(s.Dangling)+len(s.Unverifiable)+
		len(s.Repaired)+len(s.PromotedBase)+len(s.Deleted) == 0 && s.Err == nil && s.InitPromotionRefusal == ""
}

func renderKeys(w io.Writer, label string, keys []string) {
	for _, k := range keys {
		fmt.Fprintf(w, "  %s: %s\n", label, k)
	}
}
