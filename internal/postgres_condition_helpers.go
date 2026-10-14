package internal

import (
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/conditionexpr"
	"github.com/lychee-technology/forma/internal/model"
)

// hasMainTableCondition checks if a condition contains predicates on main table columns.
//
// It answers true whenever the condition could hold for an entity with no
// eav_data row, which an eav_data anchor never visits. Every eav_data leaf
// but is_null is an EXISTS, false for such an entity, and the tree has no
// negation, so only three nodes can make it true there: a main-table leaf,
// is_null, and an empty and group, which renders 1=1.
func hasMainTableCondition(cond forma.Condition, cache forma.SchemaAttributeCache) bool {
	if cond == nil {
		// An empty condition implies no filtering; treat as having main table condition
		return true
	}
	switch c := cond.(type) {
	case *forma.CompositeCondition:
		if c == nil {
			return false
		}
		// An empty and holds for every entity (#649 review); an empty or
		// renders 1=0 and holds for none.
		if len(c.Conditions) == 0 && c.Logic == forma.LogicAnd {
			return true
		}
		for _, child := range c.Conditions {
			if hasMainTableCondition(child, cache) {
				return true
			}
		}
		return false
	case *forma.KvCondition:
		if c == nil {
			return false
		}
		// Check if it's a raw main table column name
		if model.IsMainTableColumn(c.Attr) {
			return true
		}
		// is_null must reach every entity, including one with no eav_data
		// row at all, which an EAV anchor never visits (#646).
		lenient := conditionexpr.ParseOperatorValueLenient(c.Value)
		if conditionexpr.CanonicalOperator(lenient.Operator) == conditionexpr.OpIsNull {
			return true
		}
		// Check if it's an attribute with column_binding to main table
		if cache != nil {
			if meta, ok := cache[c.Attr]; ok {
				if meta.Location() == forma.AttributeStorageLocationMain {
					return true
				}
			}
		}
		return false
	default:
		return false
	}
}
