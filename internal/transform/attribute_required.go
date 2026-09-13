package transform

import (
	"fmt"
	"slices"
	"strings"

	"github.com/lychee-technology/forma"
	"go.uber.org/zap"
)

// relationRootsFor resolves the relation roots of schemaID, translating the ID
// to the schema name the lookup is keyed by.
func (c *AttributeConverter) relationRootsFor(schemaID int16) (RelationRoots, error) {
	if c.relationRoots == nil {
		return nil, nil
	}
	schemaName, _, err := c.registry.GetSchemaByID(schemaID)
	if err != nil {
		return nil, fmt.Errorf("resolve schema name for id %d: %w", schemaID, err)
	}
	return c.relationRoots(schemaName), nil
}

// checkRequiredAttributes enforces each attribute's required policy against the
// attribute names the records actually carried, per array-index context.
func (c *AttributeConverter) checkRequiredAttributes(
	schemaID int16,
	cache forma.SchemaAttributeCache,
	presentAttrIndices map[string]map[string]struct{},
) error {
	relationRoots, err := c.relationRootsFor(schemaID)
	if err != nil {
		return fmt.Errorf("resolve relation roots for required-policy check: %w", err)
	}

	missingRequired := make(map[int16]string)
	for attrName, metadata := range cache {
		// #314/#315: relation-root data is derived on read and never
		// schema-validated on write — since #318 the whole subtree is stripped
		// from the payload before validation — so required policies beneath a
		// root must follow the same rule, otherwise expanding a root's
		// attributes (#315 resolved contactSnapshot's $ref) turns payloads #314
		// ruled acceptable into 400s.
		//
		// The carve-out belongs to this check, not to the read path: ToAttributes
		// reaches FromEAVRecords on every create and update (transformer.go). The
		// write path's own required check (validateRequiredAttributesFromInput,
		// transformer.go) has none, so a required_always beneath a root still
		// rejects the stripped payload there. Documented in docs/error-handling.md.
		if relationRoots.Covers(attrName) {
			continue
		}
		switch metadata.EffectiveRequiredPolicy() {
		case forma.RequiredPolicyAlways:
			if isRequiredAttributeMissing(attrName, presentAttrIndices, true) {
				missingRequired[metadata.AttributeID] = attrName
			}
		case forma.RequiredPolicyIfParentPresent:
			if isRequiredAttributeMissing(attrName, presentAttrIndices, false) {
				missingRequired[metadata.AttributeID] = attrName
			}
		}
	}
	if len(missingRequired) == 0 {
		return nil
	}

	zap.S().Infow("missing EAV records for attrIDs.", "idToName", missingRequired)
	names := make([]string, 0, len(missingRequired))
	idsByName := make(map[string]int16, len(missingRequired))
	for id, name := range missingRequired {
		names = append(names, name)
		idsByName[name] = id
	}
	// Name the alphabetically first missing attribute, not whichever the map
	// yields first, so the same drift produces the same error on every run.
	missingAttrName := slices.Min(names)

	// Plain error, deliberately. FromEAVRecords is not write-only: the read
	// path rebuilds already-stored records through it
	// (persistent_record.go's FromPersistentRecord), so a persisted row
	// missing a required EAV row reaches here too. Wrapping
	// forma.ErrInvalidInput here — as an earlier #301 sweep did — made the
	// HTTP boundary answer that persisted-drift case with a verbatim 400,
	// inverting the split AGENTS.md and this repo's error-handling doc
	// draw: write validation carries the sentinel, read-path consistency
	// failures stay plain and operator-visible.
	//
	// The write path's 400 does not depend on this wrap. It has its own
	// write-only validator, validateRequiredAttributesFromInput
	// (transformer.go), which ToAttributes runs against the caller's input
	// before flattening and which does carry the sentinel.
	return fmt.Errorf("missing required attribute '%s' (attrID=%d) in EAV records",
		missingAttrName, idsByName[missingAttrName])
}

// shouldEnforceRequiredAttribute applies RequiredPolicyIfParentPresent semantics
// to an attribute using the observed EAV array-index context.
func shouldEnforceRequiredAttribute(attrName string, presentAttrIndices map[string]map[string]struct{}) bool {
	return isRequiredAttributeMissing(attrName, presentAttrIndices, false)
}

// isRequiredAttributeMissing reports whether a required attribute is missing.
//
// For nested attributes, the required check is contextual:
//   - RequiredPolicyAlways enforces the attribute even when its parent path is absent.
//   - RequiredPolicyIfParentPresent enforces the attribute only when its parent path
//     is present in the observed EAV records.
//   - Array-backed attributes must exist for every parent array index that is present.
func isRequiredAttributeMissing(attrName string, presentAttrIndices map[string]map[string]struct{}, enforceWhenParentMissing bool) bool {
	if indices, ok := presentAttrIndices[attrName]; ok && len(indices) > 0 {
		return parentIndexMissing(attrName, presentAttrIndices, indices, enforceWhenParentMissing)
	}

	parentPath, hasParent := attributeParentPath(attrName)
	if !hasParent {
		return true
	}

	parentIndices := collectParentIndices(parentPath, presentAttrIndices)
	if len(parentIndices) == 0 {
		return enforceWhenParentMissing
	}
	// The attribute is absent entirely while its parent context exists, so the
	// required attribute is missing for every observed parent context.
	return true
}

// parentIndexMissing verifies that a child attribute is present for every parent
// context that appears in the EAV records.
func parentIndexMissing(attrName string, presentAttrIndices map[string]map[string]struct{}, childIndices map[string]struct{}, enforceWhenParentMissing bool) bool {
	parentPath, hasParent := attributeParentPath(attrName)
	if !hasParent {
		return len(childIndices) == 0
	}

	parentIndices := collectParentIndices(parentPath, presentAttrIndices)
	if len(parentIndices) == 0 {
		// No parent context exists, so only RequiredPolicyAlways should fail here.
		return enforceWhenParentMissing && len(childIndices) == 0
	}
	if _, hasNonArrayChild := childIndices[""]; hasNonArrayChild {
		_, hasNonArrayParent := parentIndices[""]
		return !hasNonArrayParent
	}
	for idx := range parentIndices {
		if _, ok := childIndices[idx]; !ok {
			return true
		}
	}
	return false
}

// collectParentIndices gathers the array-index contexts that imply a parent path
// exists in the current EAV row. Descendant attributes contribute their observed
// indices so required children can be checked against the same contexts.
func collectParentIndices(parentPath string, presentAttrIndices map[string]map[string]struct{}) map[string]struct{} {
	parentIndices := make(map[string]struct{})
	prefix := parentPath + "."
	for presentAttrName, indexSet := range presentAttrIndices {
		if presentAttrName != parentPath && !strings.HasPrefix(presentAttrName, prefix) {
			continue
		}
		for idx := range indexSet {
			parentIndices[idx] = struct{}{}
		}
	}
	return parentIndices
}

func attributeParentPath(attrPath string) (string, bool) {
	lastDot := strings.LastIndex(attrPath, ".")
	if lastDot < 0 {
		return "", false
	}
	return attrPath[:lastDot], true
}
