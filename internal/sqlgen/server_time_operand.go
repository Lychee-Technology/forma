package sqlgen

import (
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/conditionexpr"
)

// ServerTimeMillisSQL is the PostgreSQL transaction timestamp truncated to
// epoch milliseconds: the instant $now names (#646) and the value a
// server_time update operator writes (#649). Both read it through this one
// spelling, so the two cannot name different instants in one transaction.
const ServerTimeMillisSQL = "floor(extract(epoch from now()) * 1000)::bigint"

// ServerTimeOperand is the right-hand side of a $now leaf (#646): the
// PostgreSQL transaction timestamp truncated to milliseconds, shifted by
// OffsetMs. The offset binds as one bigint placeholder, so the clause text
// is the same for every offset and the plan caches never key on it.
type ServerTimeOperand struct {
	OffsetMs int64
	// Date truncates the instant to its UTC calendar day, the image a date
	// attribute stores.
	Date bool
	// ISO8601 compares against the whole-second RFC3339 text image an
	// iso8601-bound entity_main column stores instead of epoch millis.
	ISO8601 bool
}

// Args returns the operand's single bind.
func (s ServerTimeOperand) Args() []any {
	return []any{s.OffsetMs}
}

// RenderComparison spells `lhs <sqlOp> $now` over the offset placeholder ph.
// It is PostgreSQL-only: no DuckDB tier can read the transaction clock, so
// the federated payloads refuse the operand (serverTimeFederatedError).
func (s ServerTimeOperand) RenderComparison(lhs, sqlOp, ph string) string {
	ms := "(" + ServerTimeMillisSQL + " + " + ph + "::bigint)"
	if s.Date {
		ms = "(floor(" + ms + " / 86400000.0) * 86400000)::bigint"
	}
	if !s.ISO8601 {
		return lhs + " " + sqlOp + " " + ms
	}
	return iso8601ServerTimeComparison(lhs, sqlOp, ms)
}

// iso8601ServerTimeComparison compares a whole-second text image t against
// the millisecond instant ms. The image is ordered like its instant, so
// each operator maps onto the image of a whole second:
//
//	t*1000 <= ms  ⇔  t <= floor(ms/1000)     t*1000 > ms   ⇔  t > floor(ms/1000)
//	t*1000 < ms   ⇔  t < ceil(ms/1000)       t*1000 >= ms  ⇔  t >= ceil(ms/1000)
//
// Equality holds only when ms is itself a whole second; not_equals keeps
// the IS NOT NULL its plain spelling implies, since the OR would otherwise
// admit an absent value.
func iso8601ServerTimeComparison(lhs, sqlOp, ms string) string {
	floorImage := iso8601SecondsImage("floor(" + ms + " / 1000.0)")
	ceilImage := iso8601SecondsImage("ceil(" + ms + " / 1000.0)")
	wholeSecond := "mod(" + ms + ", 1000) = 0"
	switch sqlOp {
	case "<=", ">":
		return lhs + " " + sqlOp + " " + floorImage
	case "<", ">=":
		return lhs + " " + sqlOp + " " + ceilImage
	case "=":
		return lhs + " = CASE WHEN " + wholeSecond + " THEN " + floorImage + " END"
	default:
		return "(" + lhs + " IS NOT NULL AND (" + lhs + " <> " + floorImage + " OR mod(" + ms + ", 1000) <> 0))"
	}
}

// iso8601SecondsImage renders epoch seconds as the image iso8601.Image
// writes: RFC3339 in UTC at second precision.
func iso8601SecondsImage(seconds string) string {
	return `to_char(to_timestamp(` + seconds + `) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')`
}

// serverTimeFederatedError is the refusal of a $now leaf on a federated
// query: the warm and cold tiers are evaluated by DuckDB, which cannot read
// the PostgreSQL transaction clock the operand is defined by.
func serverTimeFederatedError(attr string, vt forma.ValueType) error {
	return forma.InvalidInputf(
		"the $now operand on %s attribute '%s' is not supported on a federated query: it is the PostgreSQL transaction time, which the DuckDB-evaluated tiers cannot read; send an absolute timestamp instead",
		vt, attr)
}

// FederatedServerTimeError returns the refusal for the first leaf of cond
// that compares a date or datetime attribute against $now, or nil. The
// entity query service calls it before routing a federated request, since
// the engine may answer one through its Postgres-only route, where the
// operand would otherwise render.
func FederatedServerTimeError(cond forma.Condition, cache forma.SchemaAttributeCache) error {
	switch c := cond.(type) {
	case *forma.CompositeCondition:
		if c == nil {
			return nil
		}
		for _, child := range c.Conditions {
			if err := FederatedServerTimeError(child, cache); err != nil {
				return err
			}
		}
	case *forma.KvCondition:
		if c == nil {
			return nil
		}
		meta, ok := cache[c.Attr]
		if isServerTimeLeaf(meta, ok, conditionexpr.ParseOperatorValueLenient(c.Value)) {
			return serverTimeFederatedError(c.Attr, meta.ValueType)
		}
	}
	return nil
}

// isServerTimeLeaf reports whether a leaf compares against the $now operand:
// a known scalar operator, a date or datetime attribute, and a value spelled
// $now. On any other value type the value is an ordinary literal; the set
// and null-test operators have operands of their own.
func isServerTimeLeaf(meta forma.AttributeMetadata, hasMeta bool, lenient conditionexpr.OperatorValue) bool {
	if !hasMeta || (meta.ValueType != forma.ValueTypeDate && meta.ValueType != forma.ValueTypeDateTime) {
		return false
	}
	if !conditionexpr.IsServerTime(lenient.Value) {
		return false
	}
	switch conditionexpr.CanonicalOperator(lenient.Operator) {
	case conditionexpr.OpIn, conditionexpr.OpNotIn, conditionexpr.OpIsNull, conditionexpr.OpIsNotNull:
		return false
	}
	_, err := conditionexpr.ToSQLOperator(lenient.Operator, lenient.Value)
	return err == nil
}
