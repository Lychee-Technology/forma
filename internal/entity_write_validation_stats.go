package internal

import (
	"strings"
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
