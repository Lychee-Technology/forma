package internal

import (
	"errors"
	"fmt"

	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/transform"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

// writeValidation is one write's worth of input to validateWritePayload.
//
// It is a struct rather than a parameter list because the four write sites must
// fill in every field deliberately: schemaName and rowID are needed only by the
// report-only log line, and passing them positionally alongside schemaID is how
// they would silently be got wrong.
//
// data is typed map[string]any rather than any so that "the normalized document
// does not reach the writer" is checked by the compiler at the boundary: there is
// no untyped value here to be silently passed over. All four write paths hold a
// map already — EntityOperation.Data and .Updates are map[string]any, and both
// StripComputedFields and mergeMaps return one.
type writeValidation struct {
	validator *schemavalidate.Validator
	schemaID  int16
	// schemaName and rowID identify the offending row in the report-only log.
	// They are not used for validation itself.
	schemaName string
	rowID      uuid.UUID
	cache      forma.SchemaAttributeCache
	data       map[string]any
	enforce    bool
	// relationRoots names the schema's relation roots, sorted
	// (RelationIndex.RelationRootNames). It is diagnostic input and nothing else:
	// it decorates a validation error that has already been decided, and the one
	// branch that reads it decides only whether that decoration applies — nothing
	// below chooses what to validate, what to normalize, or what to write from
	// it. Empty for a schema declaring no relations, and for a manager built
	// without a relation index.
	//
	// It is emphatically NOT the normalization carve-out this struct used to
	// carry. That field existed so NormalizeDottedKeys could leave relation
	// subtrees alone, and it became unreachable once StripComputedFields started
	// removing the whole subtree — root and dotted descendants — before
	// normalization runs (#318). Nothing under a relation root reaches the
	// validator any more, so there is nothing left to carve out; do not
	// reintroduce that use through this field.
	relationRoots []string
}

// validateWritePayload validates a write payload against the JSON Schema
// registered for the schema. Before #314 enum, pattern and min/max were declared
// in every shipped schema and enforced nowhere.
//
// It returns an error and nothing else, on purpose. Dotted keys are normalized
// for the validator's benefit only, and the normalized document must never reach
// ToPersistentRecord: transform's dedupe decides which record wins from the key
// *spelling* that produced it (#312), and merging the spellings beforehand
// destroys that provenance. Every call site keeps handing the writer exactly the
// map it built.
//
// enforce is true on create and follows Entity.ValidateUpdatesStrict on update.
// It governs *caller input only*. With enforcement off a violation is logged
// and the write proceeds: rows written before #314 may already violate their
// schema, and rejecting on update would leave them un-updatable over an
// unrelated field.
//
// Anything that is not caller input is returned regardless of enforce. Validate
// distinguishes the two by wrapping forma.ErrInvalidInput for caller input —
// a genuine violation, or a payload json.Marshal refuses (NaN/Inf, #322) — and
// returning a plain error otherwise: a missing resolved schema, or a numeric
// literal that fits neither int64 nor float64. Those must not be absorbed by
// report-only mode: the document would be written with *zero* validation while
// a log line claimed it had been checked and merely failed
// (docs/error-handling.md). The absorbed marshal case cannot write a
// non-finite row: transform's finiteForEAV independently rejects NaN/Inf with
// the attribute name before anything is staged for storage.
//
// A nil validator means validation is unconfigured and both steps are skipped.
// Validate on a nil validator returns an error rather than doing nothing, so
// calling through would fail every write for the embedders and tests that
// construct a manager without one.
//
// It is a package-level function rather than a method so the CRUD and batch
// services cannot drift apart on any of the above.
func validateWritePayload(v writeValidation) error {
	if v.validator == nil {
		return nil
	}

	normalized := transform.NormalizeDottedKeys(v.data, v.cache, v.validator.ArrayPaths(v.schemaID))
	err := v.validator.Validate(v.schemaID, normalized)
	if err == nil {
		return nil
	}
	// The gate lives here rather than inside the decorator: a schema declaring no
	// relation root strips nothing, so there is nothing to explain, and asking
	// explainStrippedRelationRoots to decide that would make it a function that
	// sometimes hands its own input straight back.
	if len(v.relationRoots) > 0 {
		err = explainStrippedRelationRoots(err, v.schemaName, v.relationRoots)
	}
	if v.enforce || !errors.Is(err, forma.ErrInvalidInput) {
		return fmt.Errorf("failed to validate payload against schema %d: %w", v.schemaID, err)
	}

	// Report-only mode exists so an operator can find and repair violating rows
	// before flipping VALIDATE_UPDATES_STRICT, which is impossible without the
	// schema name and the row id. The payload itself is deliberately not logged:
	// entity data is caller content and may be sensitive.
	zap.S().Warnw("write payload violates the entity JSON schema; accepted because strict update validation is off",
		"schemaName", v.schemaName, "schemaID", v.schemaID, "rowID", v.rowID, "error", err.Error())
	return nil
}

// explainStrippedRelationRoots attaches operator-only detail to a failed
// validation of a schema that declares relation roots, naming them and saying
// that they are removed before this validation runs.
//
// It exists because the startup guard is deliberately incomplete. That guard
// only judges an unconditional "required" (the root's own array, or one in an
// allOf chain — see collectUnconditionalRootRequired); a schema that demands a
// relation root through "not", "oneOf", "then", "dependentRequired" or a "$ref"
// boots cleanly and then fails every affected write. Without this, the operator
// sees a 4xx complaining about a property, retries with the property, and gets
// the same 4xx forever, because the strip removes it again each time. The
// explanation is the thing that makes that loop diagnosable.
//
// # What it changes
//
// The error's class, status and published body are untouched. forma.Error() and
// the log line grow the explanation; PublicMessage() does not, because that is
// what WithOperatorDetail is for (docs/error-handling.md).
//
// # Why the gate is "the schema declares relation roots", and not the message
//
// The narrower gate would be to fire only when the validator's message names a
// relation root as a missing required property. Measured over every shape the
// startup guard no longer judges, that gate covers *some* of them:
//
//   - it would fire for the dependent* family. jsonschema-go names the property
//     in all four spellings, e.g. `dependentRequired["parentId"]: missing
//     properties ["contactSnapshot"]`;
//   - it would stay silent for "not", "oneOf" and the "if"/"else" form. The
//     first two render the offending branch anonymously (`not: validated against
//     <anonymous schema>`, `oneOf: did not validate against any of [...]`), and
//     the third reports whichever *other* branch the document took, so its text
//     names "fallback" and never the relation root.
//
// So this is a choice rather than a forced move. The gate is deliberately
// widened to "the schema declares relation roots", accepting that the
// explanation is attached to unrelated validation failures on those schemas too,
// because the wide gate never misses. The hybrid — message shape, falling back
// to the wide gate for the anonymous forms — was rejected: recognising those
// forms means keying on library prose, which is fragile, and it would still miss
// any applicator not on the list. Guaranteed coverage is worth more here than
// quiet logs, because the failure being explained is one the caller cannot fix
// and the operator cannot otherwise diagnose.
//
// Both halves of that measurement are pinned by
// TestValidatorNamesTheMissingRootUnderOnlySomeApplicators, so a library change
// in either direction re-opens the trade.
//
// The costs of the wide gate, stated plainly:
//
//   - an unrelated violation on a relation-declaring schema also carries the
//     note. The wording is therefore conditional — it states the mechanism and
//     lets the reader decide whether it applies — rather than asserting a cause
//     it has not established;
//   - internal/httpapi keys a log level on forma.HasOperatorDetail
//     (error_response.go), so *every* disclosed 4xx on a relation-declaring
//     schema now logs at Warnw rather than Debugw, not only the ones the strip
//     caused. That is the intended effect where the note is the real
//     explanation — it has to clear the production Info threshold to be of any
//     use — and accepted noise everywhere else.
//
// # The caller owns the gate
//
// relationRoots is non-empty by precondition: validateWritePayload calls this
// only when the schema declares at least one root, so every call has an
// explanation to attach and the attachment is unconditional here. The gate used
// to live in this function as an early `return err`, and a helper that sometimes
// answers its own input is the shape the wrapping rule forbids — there is no
// callee to wrap, so there is no context to add. Moving the test to the one call
// site removes the shape rather than excusing it. What the property protected is
// unchanged: a validation error on a schema with no relations must not grow a
// paragraph about a mechanism it has no part in, and
// TestWriteValidationAttachesNoDiagnosisWithoutRelationRoots pins that where it
// now holds, at validateWritePayload.
//
// One narrowing does come for free, and is not a special case here:
// forma.WithOperatorDetail returns its input unchanged when that input publishes
// nothing (client_error.go), and schemavalidate.Validate answers a plain error
// rather than an ErrInvalidInput carrier for everything that is not caller
// input — a missing resolved schema — and it passes through untouched,
// which is the surviving require.Same in
// TestExplainStrippedRelationRootsNamesEveryRootOnce.
func explainStrippedRelationRoots(err error, schemaName string, relationRoots []string) error {
	return forma.WithOperatorDetail(err, fmt.Errorf(
		"schema %s declares relation root(s) %q, and every one of them is removed from the payload — the root and its "+
			"dotted descendants alike — before this validation runs (#318); if the violation above is that one of them "+
			"is missing, the caller cannot fix it by sending the field, and the schema must stop requiring it or drop "+
			"its x-relation marker",
		schemaName, relationRoots))
}
