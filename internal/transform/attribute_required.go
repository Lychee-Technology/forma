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
// attribute names the records actually carried, per array-index context. It
// runs on every stored row a read rebuilds, and on the records a write
// produced, after requireWrittenAttributes (document_walk.go) has already
// refused the write that would store such a row.
//
// relationRoots is taken resolved rather than looked up here so that the two
// required checks on one write read the same snapshot:
// forma.SchemaRegistry promises nothing about repeated reads
// (SnapshotSchemaDocuments, package internal), so a second lookup could
// answer differently, or fail, after the first passed.
func (c *AttributeConverter) checkRequiredAttributes(
	cache forma.SchemaAttributeCache,
	presentAttrIndices map[string]map[string]struct{},
	relationRoots RelationRoots,
) error {
	missingRequired := missingRequiredAttributes(cache, presentAttrIndices, nil, relationRoots)
	if len(missingRequired) == 0 {
		return nil
	}

	zap.S().Infow("missing EAV records for attrIDs.", "idToName", missingRequired)
	missingAttrName, missingAttrID := firstMissingAttribute(missingRequired)

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
	// The write path's 400 does not depend on this wrap. ToAttributes runs
	// requireWrittenAttributes on what the write will store before it builds
	// a record, and that check does carry the sentinel.
	return fmt.Errorf("missing required attribute '%s' (attrID=%d) in EAV records",
		missingAttrName, missingAttrID)
}

// missingRequiredAttributes judges every required policy against the
// attributes a row holds (leaves) and returns the ones it misses, by attrID.
// Parent contexts are those the leaves imply plus extraParents, which a write
// adds for the parents its caller sent explicitly; a read passes nil.
//
// #314/#315: relation-root data is derived on read and never schema-validated
// on write — since #318 the whole subtree is stripped from the payload before
// validation — so required policies beneath a root must follow the same rule,
// otherwise expanding a root's attributes (#315 resolved contactSnapshot's
// $ref) turns payloads #314 ruled acceptable into 400s. A policy there could
// only ever fail, and unfixably: sending the value gives the strip more to
// remove (#389). The root's own policy stays enforced; a nil or empty
// relationRoots leaves enforcement exactly as it was.
func missingRequiredAttributes(
	cache forma.SchemaAttributeCache,
	leaves map[string]map[string]struct{},
	extraParents map[string]map[string]struct{},
	relationRoots RelationRoots,
) map[int16]string {
	missingRequired := make(map[int16]string)
	for attrName, metadata := range cache {
		if relationRoots.Covers(attrName) {
			continue
		}
		var enforceWhenParentMissing bool
		switch metadata.EffectiveRequiredPolicy() {
		case forma.RequiredPolicyAlways:
			enforceWhenParentMissing = true
		case forma.RequiredPolicyIfParentPresent:
			enforceWhenParentMissing = false
		default:
			continue
		}
		if isRequiredAttributeMissing(attrName, leaves, extraParents, enforceWhenParentMissing) {
			missingRequired[metadata.AttributeID] = attrName
		}
	}
	return missingRequired
}

// firstMissingAttribute names the alphabetically first missing attribute, not
// whichever the map yields first, so the same row produces the same error on
// every run.
func firstMissingAttribute(missingRequired map[int16]string) (string, int16) {
	names := make([]string, 0, len(missingRequired))
	idsByName := make(map[string]int16, len(missingRequired))
	for id, name := range missingRequired {
		names = append(names, name)
		idsByName[name] = id
	}
	name := slices.Min(names)
	return name, idsByName[name]
}

// addPresence records that a row holds attrName at the array-index context
// arrayIndices.
func addPresence(present map[string]map[string]struct{}, attrName, arrayIndices string) {
	indexSet := present[attrName]
	if indexSet == nil {
		indexSet = make(map[string]struct{})
		present[attrName] = indexSet
	}
	indexSet[arrayIndices] = struct{}{}
}

// shouldEnforceRequiredAttribute applies RequiredPolicyIfParentPresent semantics
// to an attribute using the observed EAV array-index context.
func shouldEnforceRequiredAttribute(attrName string, presentAttrIndices map[string]map[string]struct{}) bool {
	return isRequiredAttributeMissing(attrName, presentAttrIndices, nil, false)
}

// isRequiredAttributeMissing reports whether a required attribute is missing.
//
// For nested attributes, the required check is contextual:
//   - RequiredPolicyAlways enforces the attribute even when its parent path is absent.
//   - RequiredPolicyIfParentPresent enforces the attribute only when its parent path
//     is present: implied by the attributes the row holds (leaves), or in
//     extraParents.
//   - Array-backed attributes must exist for every parent array index that is present.
//
// Only leaves make the attribute itself present; extraParents only establish
// parent contexts.
func isRequiredAttributeMissing(
	attrName string,
	leaves map[string]map[string]struct{},
	extraParents map[string]map[string]struct{},
	enforceWhenParentMissing bool,
) bool {
	if indices, ok := leaves[attrName]; ok && len(indices) > 0 {
		return parentIndexMissing(attrName, leaves, extraParents, indices, enforceWhenParentMissing)
	}

	parentPath, hasParent := attributeParentPath(attrName)
	if !hasParent {
		return true
	}

	parentIndices := collectParentIndices(parentPath, leaves, extraParents)
	if len(parentIndices) == 0 {
		return enforceWhenParentMissing
	}
	// The attribute is absent entirely while its parent context exists, so the
	// required attribute is missing for every observed parent context.
	return true
}

// parentIndexMissing verifies that a child attribute is present for every parent
// context that appears in the row.
func parentIndexMissing(
	attrName string,
	leaves map[string]map[string]struct{},
	extraParents map[string]map[string]struct{},
	childIndices map[string]struct{},
	enforceWhenParentMissing bool,
) bool {
	parentPath, hasParent := attributeParentPath(attrName)
	if !hasParent {
		return len(childIndices) == 0
	}

	parentIndices := collectParentIndices(parentPath, leaves, extraParents)
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
// exists in the current row. Descendant attributes contribute their observed
// indices so required children can be checked against the same contexts.
func collectParentIndices(parentPath string, presences ...map[string]map[string]struct{}) map[string]struct{} {
	parentIndices := make(map[string]struct{})
	prefix := parentPath + "."
	for _, present := range presences {
		for presentAttrName, indexSet := range present {
			if presentAttrName != parentPath && !strings.HasPrefix(presentAttrName, prefix) {
				continue
			}
			for idx := range indexSet {
				parentIndices[idx] = struct{}{}
			}
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
