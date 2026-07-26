package internal

import (
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
// enforce is true on create and follows Entity.ValidateUpdatesStrict on update.
// With enforcement off a violation is logged and the write proceeds: rows
// written before #314 may already violate their schema, and rejecting on update
// would leave them un-updatable over an unrelated field.
//
// A violation arrives already wrapping forma.ErrInvalidInput and so surfaces as
// 4xx; a missing resolved schema arrives as a plain error and stays an
// operator-facing failure (docs/error-handling.md). Wrapping preserves both.
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
	data any,
	enforce bool,
) error {
	if validator == nil {
		return nil
	}
	doc, ok := data.(map[string]any)
	if !ok {
		// Only an object payload has dotted keys to normalize, and every write
		// path builds one. Anything else is left to the writer to reject.
		return nil
	}

	normalized := transform.NormalizeDottedKeys(doc, cache, validator.ArrayPaths(schemaID))
	err := validator.Validate(schemaID, normalized)
	if err == nil {
		return nil
	}
	if enforce {
		return fmt.Errorf("failed to validate payload against schema %d: %w", schemaID, err)
	}

	zap.S().Warnw("write payload violates the entity JSON schema; accepted because strict update validation is off",
		"schemaID", schemaID, "error", err.Error())
	return nil
}
