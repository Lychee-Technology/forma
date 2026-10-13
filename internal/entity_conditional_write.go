package internal

import (
	"fmt"
	"strings"
	"time"

	"github.com/lychee-technology/forma"
)

// Intake rules for EntityOperation.Preconditions and UpdateOps (#649). Every
// check runs before the operation sends any SQL, so a refusal is an
// ErrInvalidInput that leaves a Tx usable. What can only be judged against
// the locked row (a missing value to increment, an overflow) is
// applyUpdateOps's.

// validateConditionalCreate refuses both fields on a create: there is no
// current row to test or to compute from.
func validateConditionalCreate(req *forma.EntityOperation) error {
	if req.Preconditions != nil {
		return forma.InvalidInputf("create does not accept preconditions (the precondition tests '%s'): a create has no current row to test",
			firstConditionAttr(req.Preconditions))
	}
	if len(req.UpdateOps) > 0 {
		return forma.InvalidInputf("create does not accept update operators (%s on '%s'): a create has no current row to compute from; write the value in data",
			req.UpdateOps[0].Kind, req.UpdateOps[0].Attribute)
	}
	return nil
}

// validateConditionalUpdate checks an update's precondition and operators
// against the schema.
func validateConditionalUpdate(req *forma.EntityOperation, cache forma.SchemaAttributeCache, relations *RelationIndex) error {
	if err := validatePrecondition(req.Preconditions, req.SchemaName, cache); err != nil {
		return fmt.Errorf("check precondition: %w", err)
	}
	if err := validateUpdateOps(req, cache, relations); err != nil {
		return fmt.Errorf("check update operators: %w", err)
	}
	return nil
}

// validateConditionalDelete refuses operators on a delete, which writes no
// value, and checks its precondition against the schema.
func validateConditionalDelete(req *forma.EntityOperation, cache forma.SchemaAttributeCache) error {
	if len(req.UpdateOps) > 0 {
		return forma.InvalidInputf("delete does not accept update operators (%s on '%s'): a delete writes no value",
			req.UpdateOps[0].Kind, req.UpdateOps[0].Attribute)
	}
	if err := validatePrecondition(req.Preconditions, req.SchemaName, cache); err != nil {
		return fmt.Errorf("check precondition: %w", err)
	}
	return nil
}

// validatePrecondition checks what the condition DSL accepts but a
// precondition must not hold: a nil node, a composite whose logic is neither
// and nor or (the renderer would join it with AND), and a leaf on anything
// but a schema attribute. Query also accepts raw entity_main columns; a
// precondition does not, so the system ltbase_updated_at stamp is no
// precondition. Operators and operands are left to the renderer, which
// refuses them before the write sends any statement. nil passes: no
// precondition.
func validatePrecondition(cond forma.Condition, schemaName string, cache forma.SchemaAttributeCache) error {
	if cond == nil {
		return nil
	}
	switch c := cond.(type) {
	case *forma.KvCondition:
		if c == nil {
			return forma.InvalidInputf("precondition holds a nil condition")
		}
		if _, ok := cache[c.Attr]; !ok {
			return forma.InvalidInputf("precondition tests attribute '%s', which schema '%s' does not define; a precondition tests schema attributes only",
				c.Attr, schemaName)
		}
		return nil
	case *forma.CompositeCondition:
		if c == nil {
			return forma.InvalidInputf("precondition holds a nil condition")
		}
		if c.Logic != forma.LogicAnd && c.Logic != forma.LogicOr {
			return forma.InvalidInputf("precondition composite has logic '%s'; expected '%s' or '%s'", c.Logic, forma.LogicAnd, forma.LogicOr)
		}
		for i, child := range c.Conditions {
			if child == nil {
				return forma.InvalidInputf("precondition holds a nil condition")
			}
			if err := validatePrecondition(child, schemaName, cache); err != nil {
				return fmt.Errorf("%s condition %d: %w", c.Logic, i, err)
			}
		}
		return nil
	default:
		return forma.InvalidInputf("precondition holds an unsupported condition type %T", cond)
	}
}

// validateUpdateOps checks each operator against its attribute and against
// the rest of the update: one operator per attribute, and none on an
// attribute Updates also writes.
func validateUpdateOps(req *forma.EntityOperation, cache forma.SchemaAttributeCache, relations *RelationIndex) error {
	if len(req.UpdateOps) == 0 {
		return nil
	}
	written := updatePaths("", req.Updates, nil)
	seen := make(map[string]struct{}, len(req.UpdateOps))
	for i, op := range req.UpdateOps {
		if err := validateUpdateOp(op, req.SchemaName, cache, relations); err != nil {
			return fmt.Errorf("update operator %d: %w", i, err)
		}
		if _, dup := seen[op.Attribute]; dup {
			return forma.InvalidInputf("attribute '%s' has more than one update operator; send one", op.Attribute)
		}
		seen[op.Attribute] = struct{}{}
		if path, ok := overlappingPath(written, op.Attribute); ok {
			return forma.InvalidInputf("attribute '%s' appears in both updates (as '%s') and update operators; an attribute takes one or the other",
				op.Attribute, path)
		}
	}
	return nil
}

func validateUpdateOp(op forma.UpdateOp, schemaName string, cache forma.SchemaAttributeCache, relations *RelationIndex) error {
	if op.Attribute == "" {
		return forma.InvalidInputf("update operator %s names no attribute", op.Kind)
	}
	meta, ok := cache[op.Attribute]
	if !ok {
		return forma.InvalidInputf("update operator %s names attribute '%s', which schema '%s' does not define", op.Kind, op.Attribute, schemaName)
	}
	if root, ok := relationRootCovering(relations, schemaName, op.Attribute); ok {
		return forma.InvalidInputf("attribute '%s' belongs to relation '%s', which is derived on read and never written", op.Attribute, root)
	}
	switch op.Kind {
	case forma.UpdateOpIncrement:
		if op.Offset != 0 {
			return forma.InvalidInputf("increment on attribute '%s' sets Offset, which only server_time takes", op.Attribute)
		}
		switch meta.ValueType {
		case forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeBigInt:
			return nil
		}
		return forma.InvalidInputf("increment applies to smallint, integer and bigint attributes; '%s' is %s", op.Attribute, meta.ValueType)
	case forma.UpdateOpServerTime:
		if op.By != 0 {
			return forma.InvalidInputf("server_time on attribute '%s' sets By, which only increment takes", op.Attribute)
		}
		if meta.ValueType != forma.ValueTypeDate && meta.ValueType != forma.ValueTypeDateTime {
			return forma.InvalidInputf("server_time applies to date and datetime attributes; '%s' is %s", op.Attribute, meta.ValueType)
		}
		if op.Offset%time.Millisecond != 0 {
			return forma.InvalidInputf("server_time offset %s on %s attribute '%s' must be a whole number of milliseconds, the precision %s values are stored at",
				op.Offset, meta.ValueType, op.Attribute, meta.ValueType)
		}
		return nil
	}
	return forma.InvalidInputf("update operator on attribute '%s' has kind '%s'; expected %s or %s",
		op.Attribute, op.Kind, forma.UpdateOpIncrement, forma.UpdateOpServerTime)
}

// updatePaths appends the dotted path of every value updates writes, as the
// deep merge applies them (mergeMaps): a nested map writes its own entries
// beneath its path, anything else (a scalar, a list, nil) the whole path. A
// literal dotted key and the nested spelling name the same path (#312).
func updatePaths(prefix string, updates map[string]any, paths []string) []string {
	for key, value := range updates {
		path := key
		if prefix != "" {
			path = prefix + "." + key
		}
		if nested, ok := value.(map[string]any); ok {
			paths = updatePaths(path, nested, paths)
			continue
		}
		paths = append(paths, path)
	}
	return paths
}

// overlappingPath answers a written path that writes attr: attr itself, a
// container holding it, or a path beneath it.
func overlappingPath(written []string, attr string) (string, bool) {
	for _, path := range written {
		if path == attr || strings.HasPrefix(attr, path+".") || strings.HasPrefix(path, attr+".") {
			return path, true
		}
	}
	return "", false
}

// relationRootCovering answers the relation root attr lies beneath, if any.
func relationRootCovering(relations *RelationIndex, schemaName, attr string) (string, bool) {
	for _, root := range relations.RelationRootNames(schemaName) {
		if attr == root || strings.HasPrefix(attr, root+".") {
			return root, true
		}
	}
	return "", false
}

// firstConditionAttr names the first attribute a condition tests, for
// messages; "" when it tests none.
func firstConditionAttr(cond forma.Condition) string {
	switch c := cond.(type) {
	case *forma.KvCondition:
		if c != nil {
			return c.Attr
		}
	case *forma.CompositeCondition:
		if c == nil {
			return ""
		}
		for _, child := range c.Conditions {
			if attr := firstConditionAttr(child); attr != "" {
				return attr
			}
		}
	}
	return ""
}
