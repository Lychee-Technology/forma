package reconcile

import (
	"fmt"
	"io"

	"github.com/lychee-technology/forma/internal/publication"
)

// FindingKind names what a pass found wrong with a schema's publication.
type FindingKind string

const (
	NoRecord                FindingKind = "no_record"
	RecordInvalid           FindingKind = "record_invalid"
	AwaitingInit            FindingKind = "awaiting_init"
	MissingGeneration       FindingKind = "missing_generation"
	HistoryMismatch         FindingKind = "history_mismatch"
	HistoryGap              FindingKind = "missing_history"
	MissingRetainedManifest FindingKind = "missing_retained_manifest"
	InvalidManifest         FindingKind = "invalid_manifest"
	ForeignManifest         FindingKind = "foreign_manifest"
	LostObject              FindingKind = "lost_object"
	CorruptObject           FindingKind = "corrupt_object"
	MissingLifecycleEpoch   FindingKind = "missing_lifecycle_epoch"
	StaleLifecycleEpoch     FindingKind = "lifecycle_epoch_mismatch"
	UnboundStorage          FindingKind = "storage_not_bound"
	UnrecognizedObject      FindingKind = "unrecognized_object"
)

func (k FindingKind) blocking() bool {
	switch k {
	case UnboundStorage, NoRecord, RecordInvalid, MissingLifecycleEpoch, StaleLifecycleEpoch,
		MissingGeneration, HistoryMismatch, HistoryGap, MissingRetainedManifest, InvalidManifest, ForeignManifest:
		return true
	}
	return false
}

// Finding describes one discrepancy with its identity and safe operator action.
type Finding struct {
	Kind       FindingKind
	Epoch      int64
	Generation int64
	Current    bool
	Key        string
	Detail     string
	Action     string
	absent     bool
}

// Collected records only the mutations a --gc pass actually completed.
type Collected struct {
	Retired         []int64
	RecoveryRetired bool
	Objects         []string
	Manifests       []string
	Forgotten       []int64
}

// SchemaReport is one schema's diagnosis or collection outcome.
type SchemaReport struct {
	SchemaID            int16
	Skipped             bool
	State               publication.State
	Epoch               int64
	Generation          int64
	Horizon             int64
	Retained            int
	Expired             int
	Listed              int
	PendingOrphans      int
	ConcurrentlyExpired int
	Plan                publication.Plan
	Refused             string
	Collected           *Collected
	Findings            []Finding
	Err                 error
}

// Residual reports whether the schema needs operator attention or a rerun.
// Routine garbage awaiting collection is not residual.
func (s SchemaReport) Residual() bool {
	return s.Skipped || s.Err != nil || len(s.Findings) > 0 || s.Inconclusive()
}

// Inconclusive reports whether lock-free diagnosis dropped absent-object
// findings because their generation expired while diagnosis ran.
func (s SchemaReport) Inconclusive() bool {
	return s.ConcurrentlyExpired > 0
}

// Report aggregates per-schema outcomes for a run.
type Report struct {
	Schemas []SchemaReport
}

// Render writes a human-readable operator report.
func (r Report) Render(w io.Writer) {
	for _, s := range r.Schemas {
		s.render(w)
	}
}

func (s SchemaReport) render(w io.Writer) {
	if s.Skipped {
		fmt.Fprintf(w, "schema %d: skipped (schema lock held by another holder)\n", s.SchemaID)
		return
	}
	verdict := "clean"
	switch {
	case s.Err != nil || len(s.Findings) > 0:
		verdict = "needs attention"
	case s.Inconclusive():
		verdict = "inconclusive (run the diagnosis again)"
	}
	state := string(s.State)
	if state == "" {
		state = "no record read"
	}
	fmt.Fprintf(w, "schema %d: %s (%s, epoch %d, generation %d)\n", s.SchemaID, verdict, state, s.Epoch, s.Generation)
	fmt.Fprintf(w, "  generations: %d retained, %d expired; objects: %d listed, %d unreferenced inside horizon %d\n",
		s.Retained, s.Expired, s.Listed, s.PendingOrphans, s.Horizon)
	for _, f := range s.Findings {
		fmt.Fprintf(w, "  %s: %s\n", f.Kind, f.describe())
		fmt.Fprintf(w, "    action: %s\n", f.Action)
	}
	if s.ConcurrentlyExpired > 0 {
		fmt.Fprintf(w, "  %d finding(s) left out: generation expired while diagnosis ran "+
			"(a collection may have deleted what it listed, or none ran and it was missing before; run diagnosis again)\n", s.ConcurrentlyExpired)
	}
	if s.Refused != "" {
		fmt.Fprintf(w, "  garbage collection refused: %s\n", s.Refused)
	}
	if s.Collected != nil && len(s.Collected.Retired) > 0 {
		fmt.Fprintf(w, "  retired generations: %v\n", s.Collected.Retired)
	}
	if s.Collected != nil && s.Collected.RecoveryRetired {
		fmt.Fprintf(w, "  retired recovery fence (grace period started)\n")
	}
	switch {
	case s.Refused != "":
	case s.Collected == nil:
		s.renderPlan(w)
	default:
		s.renderCollected(w)
	}
	if s.Err != nil {
		fmt.Fprintf(w, "  error: %v\n", s.Err)
	}
}

func (s SchemaReport) renderPlan(w io.Writer) {
	fmt.Fprintf(w, "  garbage collection would delete %d objects and %d manifests, and forget generations %v\n",
		len(s.Plan.Objects), len(s.Plan.Manifests), s.Plan.Forget)
	for _, k := range s.Plan.Objects {
		fmt.Fprintf(w, "  would delete object: %s\n", k)
	}
	for _, k := range s.Plan.Manifests {
		fmt.Fprintf(w, "  would delete manifest: %s\n", k)
	}
}

func (s SchemaReport) renderCollected(w io.Writer) {
	c := s.Collected
	fmt.Fprintf(w, "  garbage collection deleted %d of %d planned objects and %d of %d planned manifests, and forgot generations %v of %v planned\n",
		len(c.Objects), len(s.Plan.Objects), len(c.Manifests), len(s.Plan.Manifests), c.Forgotten, s.Plan.Forget)
	for _, k := range c.Objects {
		fmt.Fprintf(w, "  deleted object: %s\n", k)
	}
	for _, k := range c.Manifests {
		fmt.Fprintf(w, "  deleted manifest: %s\n", k)
	}
	left := len(s.Plan.Objects) - len(c.Objects) + len(s.Plan.Manifests) - len(c.Manifests) + len(s.Plan.Forget) - len(c.Forgotten)
	if left > 0 {
		fmt.Fprintf(w, "  left for the next pass: %d planned deletes and forgets did not run\n", left)
	}
}

func (f Finding) describe() string {
	out := ""
	if f.Generation != 0 {
		which := "superseded"
		if f.Current {
			which = "current"
		}
		out = fmt.Sprintf("epoch %d generation %d (%s)", f.Epoch, f.Generation, which)
	}
	if f.Key != "" {
		if out != "" {
			out += ", "
		}
		out += "key " + f.Key
	}
	if f.Detail != "" {
		if out != "" {
			out += ": "
		}
		out += f.Detail
	}
	return out
}
