package sqlgen

import "github.com/lychee-technology/forma"

// checkOperatorForType is the operator whitelist of a scalar comparison
// leaf: LIKE (starts_with, contains) only on text attributes, and only = and
// != on bool ones. Every Postgres route applies it before converting the
// operand: the eav_data form, the hybrid entity_main form that Query and a
// write's precondition render (#649), and the federated entity_main
// pushdown. The entity_main forms used to leave the LIKE rule to operand
// conversion, which refuses a pattern for every value type but text and
// uuid. A uuid column took the pattern, and PostgreSQL then failed the
// statement (42883 `uuid ~~ unknown`); in a conditional write that came
// after the row lock and aborted the Tx (#674 review).
//
// Both rules refuse caller-supplied input, so they publish (#307 round-4
// Finding 4).
func checkOperatorForType(attr, op, sqlOp string, vt forma.ValueType) error {
	if vt != forma.ValueTypeText && sqlOp == "LIKE" {
		return forma.InvalidInputf("operator '%s' only supported for text attributes, not '%s' (attribute '%s')", op, vt, attr)
	}
	if vt == forma.ValueTypeBool && sqlOp != "=" && sqlOp != "!=" {
		return forma.InvalidInputf("operator '%s' not supported for boolean attributes (attribute '%s')", op, attr)
	}
	return nil
}
