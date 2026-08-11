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
	// it decorates a validation error that has already been decided, and no
	// branch below reads it to choose what to validate, what to normalize, or
	// what to write. Empty for a schema declaring no relations, and for a manager
	// built without a relation index.
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
// It governs *violations only*. With enforcement off a violation is logged and
// the write proceeds: rows written before #314 may already violate their schema,
// and rejecting on update would leave them un-updatable over an unrelated field.
//
// Anything that is not a violation is returned regardless of enforce. Validate
// distinguishes the two by wrapping forma.ErrInvalidInput for a genuine
// violation (→4xx) and returning a plain error otherwise — a missing resolved
// schema, or a payload that will not marshal (NaN/Inf). Those must not be
// absorbed by report-only mode: the document would be written with *zero*
// validation while a log line claimed it had been checked and merely failed, and
// the message would blame a caller violation for what is an operator fault
// (docs/error-handling.md).
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
	err = explainStrippedRelationRoots(err, v.schemaName, v.relationRoots)
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
// what WithOperatorDetail is for (docs/error-handling.md). One visible
// consequence: internal/httpapi logs a disclosed 4xx at Warnw rather than Debugw
// once operator detail is present, which is the intended effect — the
// explanation must clear the production Info threshold to be of any use.
//
// # Why the gate is "the schema declares relation roots", and not the message
//
// The narrower gate would be to fire only when the validator's message names a
// relation root as a missing required property. That gate was measured and
// rejected: it goes silent on exactly the cases this function exists for. For
// {"not":{"not":{"required":["contactSnapshot"]}}} jsonschema-go reports
// `not: validated against <anonymous schema>`, and for a oneOf
// `oneOf: did not validate against any of [<anonymous schema> …]` — neither text
// contains the property name, and both are shapes the startup guard now declines
// to judge, so the diagnosis is the only backstop there is. Pinned by
// TestValidatorDoesNotNameTheMissingRootUnderEveryApplicator, which fails if the
// library ever starts naming the property and a narrower gate becomes viable.
//
// The cost is that an unrelated violation on a relation-declaring schema also
// carries the note. The wording is therefore conditional — it states the
// mechanism and lets the reader decide whether it applies — rather than
// asserting a cause it has not established.
//
// One narrowing does come for free, and is not a special case here:
// forma.WithOperatorDetail returns its input unchanged when that input publishes
// nothing (client_error.go), and schemavalidate.Validate answers a plain error
// rather than an ErrInvalidInput carrier for everything that is not a caller
// violation — a missing resolved schema, a payload that will not marshal. Those
// are operator faults bound for a 500 and they pass through untouched.
func explainStrippedRelationRoots(err error, schemaName string, relationRoots []string) error {
	if len(relationRoots) == 0 {
		return err
	}
	return forma.WithOperatorDetail(err, fmt.Errorf(
		"schema %s declares relation root(s) %q, and every one of them is removed from the payload — the root and its "+
			"dotted descendants alike — before this validation runs (#318); if the violation above is that one of them "+
			"is missing, the caller cannot fix it by sending the field, and the schema must stop requiring it or drop "+
			"its x-relation marker",
		schemaName, relationRoots))
}
