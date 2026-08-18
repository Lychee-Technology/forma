package internal

import (
	"strings"
	"sync"
)

// Violation kinds for the #317 aggregate. "required" means the document lacks
// a property, so the repair is to backfill data; "constraint" means a present
// value is illegal, so the repair is to fix the value. The rollout actions
// differ, which is why the label exists.
const (
	violationKindRequired   = "required"
	violationKindConstraint = "constraint"
)

// classifyViolation sorts a schema violation into required-versus-constraint
// for the #317 kind label.
//
// jsonschema-go returns unstructured errors, so this keys on library prose:
// the "required" and "dependentRequired" keywords render as "... missing
// properties ..." and no other keyword does. Keying behavior on library prose
// was rejected for explainStrippedRelationRoots' gate; it is accepted here
// because the blast radius is one metric label, never a write outcome. The
// enum prose embeds the caller's own value, so a value that itself contains
// "missing properties" mislabels its increment — accepted at the same cost.
// TestClassifyViolationPinsLibraryProse pins both halves against the real
// validator, so a library upgrade re-opens this deliberately.
func classifyViolation(err error) string {
	if err != nil && strings.Contains(err.Error(), "missing properties") {
		return violationKindRequired
	}
	return violationKindConstraint
}

// reportOnlyMilestoneMessage is the aggregate answer to "is it safe to flip
// VALIDATE_UPDATES_STRICT yet" (#317): as long as these lines keep appearing
// for a schema, rows still violate it. The line is payload-free on purpose —
// counts only, no violation text, no row id — so it adds no disclosure however
// often it fires.
const reportOnlyMilestoneMessage = "report-only schema validation violations reached a milestone; " +
	"rows still violate their schema and VALIDATE_UPDATES_STRICT is not yet safe to flip"

// reportOnlyMilestoneEvery is the milestone stride after the first violation.
// 100 matches zap.NewProduction's sampling stride, so on the busiest corpus
// roughly one milestone line survives per sampled bucket of per-write lines.
const reportOnlyMilestoneEvery = 100

// reportOnlyStats aggregates accepted violations per schema for the lifetime
// of one EntityManager (#317). Counts reset with the process; the milestone
// log line carries them, so the trend survives in the log stream rather than
// in memory.
type reportOnlyStats struct {
	mu      sync.Mutex
	schemas map[int16]*reportOnlySchemaCounts
}

type reportOnlySchemaCounts struct {
	total      uint64
	required   uint64
	constraint uint64
}

func newReportOnlyStats() *reportOnlyStats {
	return &reportOnlyStats{schemas: map[int16]*reportOnlySchemaCounts{}}
}

// record adds one accepted violation and reports whether the new total is a
// milestone — the 1st, then every reportOnlyMilestoneEvery-th — together with
// the counts to log when it is. A nil receiver records nothing and never
// reaches a milestone: stats are optional wiring, validation is not.
func (s *reportOnlyStats) record(schemaID int16, kind string) (milestone bool, total, required, constraint uint64) {
	if s == nil {
		return false, 0, 0, 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	counts := s.schemas[schemaID]
	if counts == nil {
		counts = &reportOnlySchemaCounts{}
		s.schemas[schemaID] = counts
	}
	counts.total++
	if kind == violationKindRequired {
		counts.required++
	} else {
		counts.constraint++
	}
	milestone = counts.total == 1 || counts.total%reportOnlyMilestoneEvery == 0
	return milestone, counts.total, counts.required, counts.constraint
}
