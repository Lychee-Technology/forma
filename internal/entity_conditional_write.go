package internal

import (
	"fmt"
	"strings"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/schemavalidate"
	"github.com/lychee-technology/forma/internal/transform"
)

// Intake rules for EntityOperation.Preconditions and UpdateOps (#649), and
// for the text values an update's Updates write (#679). Every
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
// against the schema, and the text values its Updates write
// (checkUpdatesText). arrays are the paths the schema declares as arrays,
// nil when the manager has no validator to know them.
func validateConditionalUpdate(req *forma.EntityOperation, cache forma.SchemaAttributeCache, relations *RelationIndex, arrays schemavalidate.ArrayPaths) error {
	if err := validatePrecondition(req.Preconditions, req.SchemaName, cache); err != nil {
		return fmt.Errorf("check precondition: %w", err)
	}
	if err := validateUpdateOps(req, cache, relations, arrays); err != nil {
		return fmt.Errorf("check update operators: %w", err)
	}
	return checkUpdatesText(req.SchemaName, req.Updates, cache, relations)
}

// checkUpdatesText refuses, before the update sends any statement, a text
// value in updates that PostgreSQL text cannot hold (#679). The merged write
// refuses it too, but only after the row lock and read, which poisons a Tx;
// before #679 it reached the INSERT and failed it (22021). The relation
// subtrees are stripped first, as the merge strips them
// (mergeUpdateDocument).
func checkUpdatesText(schemaName string, updates map[string]any, cache forma.SchemaAttributeCache, relations *RelationIndex) error {
	if err := transform.CheckTextValues(relations.StripComputedFields(schemaName, updates), cache); err != nil {
		return fmt.Errorf("check updates: %w", err)
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
// precondition. A schema attribute whose name is itself an entity_main
// column is refused too: the renderer resolves a raw column name before the
// cache, so the guard would test that column, not the attribute.
// Registration still admits such names (#470). Operators and operands are
// left to the renderer, which refuses them before the write sends any
// statement. nil passes: no precondition.
func validatePrecondition(cond forma.Condition, schemaName string, cache forma.SchemaAttributeCache) error {
	return validateGuardCondition("precondition", cond, schemaName, cache)
}

// validateGuardCondition is validatePrecondition for any condition rendered
// as a row guard; noun names it in the published message ("precondition",
// or "condition" for a predicate mutation's, #652).
func validateGuardCondition(noun string, cond forma.Condition, schemaName string, cache forma.SchemaAttributeCache) error {
	if cond == nil {
		return nil
	}
	switch c := cond.(type) {
	case *forma.KvCondition:
		if c == nil {
			return forma.InvalidInputf("%s holds a nil condition", noun)
		}
		if _, ok := cache[c.Attr]; !ok {
			return forma.InvalidInputf("%s tests attribute '%s', which schema '%s' does not define; a %s tests schema attributes only",
				noun, c.Attr, schemaName, noun)
		}
		if model.IsMainTableColumn(c.Attr) {
			return forma.InvalidInputf("%s tests attribute '%s' of schema '%s', whose name is also an entity_main column: the condition DSL reads that name as the column, not the attribute, so a %s cannot test it",
				noun, c.Attr, schemaName, noun)
		}
		return nil
	case *forma.CompositeCondition:
		if c == nil {
			return forma.InvalidInputf("%s holds a nil condition", noun)
		}
		if c.Logic != forma.LogicAnd && c.Logic != forma.LogicOr {
			return forma.InvalidInputf("%s composite has logic '%s'; expected '%s' or '%s'", noun, c.Logic, forma.LogicAnd, forma.LogicOr)
		}
		for i, child := range c.Conditions {
			if child == nil {
				return forma.InvalidInputf("%s holds a nil condition", noun)
			}
			if err := validateGuardCondition(noun, child, schemaName, cache); err != nil {
				return fmt.Errorf("%s condition %d: %w", c.Logic, i, err)
			}
		}
		return nil
	default:
		return forma.InvalidInputf("%s holds an unsupported condition type %T", noun, cond)
	}
}

// validateUpdateOps checks each operator against its attribute and against
// the rest of the update: one operator per attribute, and none on an
// attribute Updates also writes. The row is not read yet, so the overlap
// check finds the overlaps every row has; the merge repeats it on the locked
// row (mergeUpdateDocument).
func validateUpdateOps(req *forma.EntityOperation, cache forma.SchemaAttributeCache, relations *RelationIndex, arrays schemavalidate.ArrayPaths) error {
	seen := make(map[string]struct{}, len(req.UpdateOps))
	for i, op := range req.UpdateOps {
		if err := validateUpdateOp(op, req.SchemaName, cache, relations, arrays); err != nil {
			return fmt.Errorf("update operator %d: %w", i, err)
		}
		if _, dup := seen[op.Attribute]; dup {
			return forma.InvalidInputf("attribute '%s' has more than one update operator; send one", op.Attribute)
		}
		seen[op.Attribute] = struct{}{}
		if err := updatesOverlap(req.Updates, nil, op.Attribute); err != nil {
			return fmt.Errorf("update operator %d: %w", i, err)
		}
	}
	return nil
}

// validateUpdateOp checks one operator against its attribute. Like a
// relation attribute, one bound to a system column (ltbase_created_at, say)
// is never written: the transformer drops an update's value for it
// (storeInMainColumn), so an operator there would report success and change
// nothing. Literal Updates keep that read-only behavior; an operator, which
// promises a stored result, is refused.
//
// An attribute inside an array the schema declares is refused here,
// whatever the row holds: where the row holds the array the operator would
// write one value for many, and where it holds none it would create an
// object the schema says is an array. An array the schema does not show (one
// behind a $ref, or any when there is no validator) is judged on the locked
// row (documentParent).
func validateUpdateOp(op forma.UpdateOp, schemaName string, cache forma.SchemaAttributeCache, relations *RelationIndex, arrays schemavalidate.ArrayPaths) error {
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
	if binding := meta.ColumnBinding; binding != nil && model.IsSystemColumn(string(binding.ColumnName)) {
		return forma.InvalidInputf("update operator %s names %s attribute '%s', which is bound to system column '%s': the store sets that column itself and never writes an update's value there",
			op.Kind, meta.ValueType, op.Attribute, binding.ColumnName)
	}
	if array, ok := arrays.CrossedArray("", op.Attribute); ok {
		return operatorInsideArray(op.Attribute, array)
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

// updatesOverlap refuses attr, an update operator's attribute, when updates
// writes it too. row is the locked row's document, nil before it is read.
// The row decides how updates applies (mergeMaps): an object in updates
// merges into an object the row holds, and is placed where the row holds
// nothing, but replaces any other value, and everything beneath that value
// with it. A nil row finds only the overlaps every row has.
func updatesOverlap(updates, row map[string]any, attr string) error {
	path, replaces, ok := updatesWritingAttr("", updates, row, attr)
	switch {
	case !ok:
		return nil
	case replaces:
		return forma.InvalidInputf("attribute '%s' appears in both updates and update operators: the row holds a value at '%s' that is not an object, so the object updates sends there replaces it rather than merging into it; an attribute takes one or the other",
			attr, path)
	}
	return forma.InvalidInputf("attribute '%s' appears in both updates (as '%s') and update operators; an attribute takes one or the other",
		attr, path)
}

// updatesWritingAttr answers the dotted path of a value in updates, which
// sits at prefix over row, that writes attr: attr itself, a path beneath it,
// or a container holding it that is replaced rather than merged. A literal
// dotted key and the nested spelling name the same path (#312). The second
// answer reports a container that replaces only because of what row holds
// there (rowObject).
//
// It descends only into a map whose path is a proper prefix of attr, so it
// goes no deeper than attr has segments, whatever updates or row hold. A
// cyclic Go value is therefore left to the merge's own depth cap (#406),
// which refuses it as ErrInvalidInput.
func updatesWritingAttr(prefix string, updates, row map[string]any, attr string) (string, bool, bool) {
	for key, value := range updates {
		path := key
		if prefix != "" {
			path = prefix + "." + key
		}
		if path == attr || strings.HasPrefix(path, attr+".") {
			return path, false, true
		}
		if !strings.HasPrefix(attr, path+".") {
			continue
		}
		nested, isObject := value.(map[string]any)
		if !isObject {
			return path, false, true
		}
		stored, merges := rowObject(row, key)
		if !merges {
			return path, true, true
		}
		if written, replaces, ok := updatesWritingAttr(path, nested, stored, attr); ok {
			return written, replaces, true
		}
	}
	return "", false, false
}

// rowObject answers what mergeMaps merges an object at key into: the object
// row holds there, or nil where row holds nothing. It reports false where
// row holds anything else, a list or a scalar, which the object replaces.
// mergeMaps looks key up literally, as here, so a dotted key never merges
// into the row's nested objects.
func rowObject(row map[string]any, key string) (map[string]any, bool) {
	stored, present := row[key]
	if !present || stored == nil {
		return nil, true
	}
	object, isObject := stored.(map[string]any)
	return object, isObject
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
