package internal

import (
	"errors"
	"fmt"

	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/transform"

	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

// validateWritePayload validates a write payload against the JSON Schema
// registered for schemaID. Before #314 enum, pattern and min/max were declared
// in every shipped schema and enforced nowhere.
//
// It returns an error and nothing else, on purpose. Dotted keys are normalized
// for the validator's benefit only, and the normalized document must never reach
// ToPersistentRecord: transform's dedupe decides which record wins from the key
// *spelling* that produced it (#312), and merging the spellings beforehand
// destroys that provenance. Every call site keeps handing the writer exactly the
// map it built.
//
// data is typed map[string]any rather than any so that "the normalized document
// does not reach the writer" is checked by the compiler at the boundary: there is
// no untyped value here to be silently passed over. All four write paths hold a
// map already — EntityOperation.Data and .Updates are map[string]any, and both
// StripComputedFields and mergeMaps return one.
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
func validateWritePayload(
	validator *schemavalidate.Validator,
	schemaID int16,
	cache forma.SchemaAttributeCache,
	data map[string]any,
	enforce bool,
) error {
	if validator == nil {
		return nil
	}

	normalized := transform.NormalizeDottedKeys(data, cache, validator.ArrayPaths(schemaID))
	err := validator.Validate(schemaID, normalized)
	if err == nil {
		return nil
	}
	if enforce || !errors.Is(err, forma.ErrInvalidInput) {
		return fmt.Errorf("failed to validate payload against schema %d: %w", schemaID, err)
	}

	zap.S().Warnw("write payload violates the entity JSON schema; accepted because strict update validation is off",
		"schemaID", schemaID, "error", err.Error())
	return nil
}
