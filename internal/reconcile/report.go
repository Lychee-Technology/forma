package reconcile

import (
	"fmt"
	"io"

	"github.com/lychee-technology/forma/internal/publication"
)

// FindingKind names what a pass found wrong with a schema's publication.
type FindingKind string

const (
	// NoRecord: the schema has no publication record. Readers and writers
	// fail closed, and nothing tells the collector what readers may scan.
	NoRecord FindingKind = "no_record"
	// RecordInvalid: the record row is one no transition writes.
	RecordInvalid FindingKind = "record_invalid"
	// AwaitingInit: the V2 cutover left the schema for a cdc-init.
	AwaitingInit FindingKind = "awaiting_init"
	// MissingGeneration: the record names a committed generation whose
	// manifest object is not in S3, or that has no history row.
	MissingGeneration FindingKind = "missing_generation"
	// HistoryMismatch: the record's generation has a history row, but the
	// row disagrees with the record: one of the two was restored or written
	// outside the protocol.
	HistoryMismatch FindingKind = "history_mismatch"
	// HistoryGap: the generation history has no row for a generation
	// number a reader could still hold: one above the highest expired row
	// and at or below the record's counter.
	HistoryGap FindingKind = "missing_history"
	// MissingRetainedManifest: a superseded generation readers may still
	// scan has no manifest object.
	MissingRetainedManifest FindingKind = "missing_retained_manifest"
	// InvalidManifest: a manifest object's bytes are not the manifest
	// PostgreSQL committed.
	InvalidManifest FindingKind = "invalid_manifest"
	// ForeignManifest: a generation's manifest key is not one the pass's
	// layout builds for the schema: the pass runs under another data
	// prefix than the writers publish under.
	ForeignManifest FindingKind = "foreign_manifest"
	// LostObject: a retained generation lists an object S3 confirmed
	// missing.
	LostObject FindingKind = "lost_object"
	// CorruptObject: a listed object's bytes do not hash to the digest, or
	// do not have the size, its manifest lists.
	CorruptObject FindingKind = "corrupt_object"
	// MissingLifecycleEpoch: the schema has a record but no lifecycle
	// epoch row, so nothing tells when its last lost-record recovery was,
	// and a reader may still hold a generation no history row names.
	MissingLifecycleEpoch FindingKind = "missing_lifecycle_epoch"
	// StaleLifecycleEpoch: the lifecycle epoch row names another epoch
	// than the record. Every transition writes the two together, so one of
	// them was restored or written outside the protocol, and the row's
	// stamp does not fence the record's last recovery.
	StaleLifecycleEpoch FindingKind = "lifecycle_epoch_mismatch"
	// UnboundStorage: the endpoint the run's client sends its requests to,
	// its bucket and its prefix are not the storage target the deployment
	// is bound to, or it is bound to none.
	// Nothing a bucket holds proves it is the writers' (a copy verifies as
	// well), so garbage collection deletes nothing there.
	UnboundStorage FindingKind = "storage_not_bound"
	// UnrecognizedObject: an object under the schema's prefix whose key no
	// V2 writer stages. Garbage collection never deletes it.
	UnrecognizedObject FindingKind = "unrecognized_object"
)

// blocking answers whether a finding of kind k stops garbage collection: a
// pass that cannot prove which objects readers may still scan deletes
// nothing.
func (k FindingKind) blocking() bool {
	switch k {
	case UnboundStorage, NoRecord, RecordInvalid, MissingLifecycleEpoch, StaleLifecycleEpoch, MissingGeneration, HistoryMismatch,
		HistoryGap, MissingRetainedManifest, InvalidManifest, ForeignManifest:
		return true
	}
	return false
}

// Finding is one thing a pass found wrong, with the identity it concerns
// and what the operator does about it.
type Finding struct {
	Kind FindingKind
	// Epoch and Generation name the generation the finding concerns, 0
	// when it concerns none (a record fault, an unrecognized object).
	Epoch      int64
	Generation int64
	// Current says the generation is the one the record names.
	Current bool
	// Key is the object the finding concerns, if any.
	Key    string
	Detail string
	// Action is the safe operator action.
	Action string
	// absent says the finding is a manifest or an object S3 confirmed
	// missing: what a collection deletes once the generation expires, so
	// a lock-free diagnosis checks the generation again before it reports
	// one (dropConcurrentlyExpired).
	absent bool
}

// Collected is what a garbage-collection pass did.
type Collected struct {
	// Retired are the superseded generations the pass stamped retired_at
	// on; they expire the retention grace later.
	Retired []int64
	// Objects and Manifests are the keys deleted, Forgotten the history
	// rows deleted, in that order.
	Objects   []string
	Manifests []string
	Forgotten []int64
}

// SchemaReport is one schema's outcome.
type SchemaReport struct {
	SchemaID int16
	// Skipped: another holder had the schema lock, and nothing was read.
	Skipped bool
	// State, Epoch and Generation are the record's, State empty when no
	// record was read.
	State      publication.State
	Epoch      int64
	Generation int64
	// Horizon is the collector's fence the plan was made against: the one
	// the pass raised under --gc, the record's otherwise.
	Horizon int64
	// Retained and Expired count the generations readers may still scan
	// and those past their grace.
	Retained int
	Expired  int
	// Listed counts the objects under the schema's prefix.
	Listed int
	// PendingOrphans counts V2 objects no history manifest names (only a
	// displaced inventory, or nothing) not yet below the horizon: staging
	// of a publication that may still commit, or garbage a later pass
	// collects once the horizon passes it.
	PendingOrphans int
	// ConcurrentlyExpired counts the findings a diagnosis left out because
	// the generation they concerned, current or superseded in its
	// snapshot, expired while it ran: a collection beside it may have
	// deleted its manifest or objects, or none ran and they were missing
	// before. The diagnosis observed no delete, so it tells neither apart.
	ConcurrentlyExpired int
	// Plan is what garbage collection deletes (under --gc) or would delete
	// against Horizon (without it). Empty when the pass refused.
	Plan publication.Plan
	// Refused says why garbage collection deletes nothing for the schema,
	// empty when nothing stops it.
	Refused string
	// Collected is nil unless the pass ran garbage collection.
	Collected *Collected
	Findings  []Finding
	// Err is a failure of the pass itself (a store or database call); the
	// other schemas still run.
	Err error
}

// Residual answers whether the schema needs an operator: a finding, a
// failure, a lock another holder kept, or a diagnosis left inconclusive.
// Garbage awaiting collection is not residual: collecting it is the
// collector's routine work.
func (s SchemaReport) Residual() bool {
	return s.Skipped || s.Err != nil || len(s.Findings) > 0 || s.Inconclusive()
}

// Inconclusive answers whether the diagnosis left findings out because their
// generation expired while it ran. It then established neither that what it
// found missing was collected nor that it was lost, so the schema is not
// clean: like a skipped schema, it needs another run.
func (s SchemaReport) Inconclusive() bool {
	return s.ConcurrentlyExpired > 0
}

// Report is one run over the schemas.
type Report struct {
	Schemas []SchemaReport
}

// Render writes the report for an operator: one block per schema, each
// finding with its identity and action.
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
	fmt.Fprintf(w, "  generations: %d retained, %d expired; objects: %d listed, %d unreferenced inside the horizon %d\n",
		s.Retained, s.Expired, s.Listed, s.PendingOrphans, s.Horizon)
	for _, f := range s.Findings {
		fmt.Fprintf(w, "  %s: %s\n", f.Kind, f.describe())
		fmt.Fprintf(w, "    action: %s\n", f.Action)
	}
	if s.ConcurrentlyExpired > 0 {
		fmt.Fprintf(w, "  %d finding(s) left out: the generation they concerned expired while the diagnosis ran "+
			"(replaced, retired and past the retention grace): a collection may have deleted what it listed, or none ran and it "+
			"was missing before; the diagnosis cannot tell which, so run it again\n", s.ConcurrentlyExpired)
	}
	if s.Refused != "" {
		fmt.Fprintf(w, "  garbage collection refused: %s\n", s.Refused)
	}
	if s.Collected != nil && len(s.Collected.Retired) > 0 {
		fmt.Fprintf(w, "  retired generations: %v\n", s.Collected.Retired)
	}
	switch {
	case s.Refused != "":
	case s.Collected == nil:
		s.renderPlan(w)
	default:
		s.renderCollected(w)
	}
	if s.Refused == "" && len(s.Plan.Deferred) > 0 {
		fmt.Fprintf(w, "  kept until below the orphan cutoff: %d key(s) only a displaced inventory names, which a publication "+
			"may still commit; the displacement is kept, and a later pass deletes them\n", len(s.Plan.Deferred))
		for _, k := range s.Plan.Deferred {
			fmt.Fprintf(w, "  kept: %s\n", k)
		}
	}
	if s.Err != nil {
		fmt.Fprintf(w, "  error: %v\n", s.Err)
	}
}

// renderPlan writes the exact deletes a collection would run, so that an
// operator can inspect them before enabling --gc.
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

// renderCollected writes what the collection did, from Collected: a pass
// that failed part way reports what it deleted and forgot, and what of its
// plan it left for the next pass.
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
