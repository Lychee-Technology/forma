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
	// RFC3339 compares against the millisecond RFC 3339 text image an
	// rfc3339-bound entity_main column stores instead of epoch millis.
	RFC3339 bool
}

// Args returns the operand's single bind.
func (s ServerTimeOperand) Args() []any {
	return []any{s.OffsetMs}
}

// RenderComparison spells `lhs <sqlOp> $now` over the offset placeholder ph.
// It is PostgreSQL-only: no DuckDB tier can read the transaction clock, so
// the federated payloads refuse the operand (serverTimeFederatedError).
//
// On an rfc3339 column the operand is the canonical image of the same
// millisecond instant. Canonical images order as their instants (#842), so
// every operator compares text against text, which a declared index on the
// column seeks; no operator needs the whole-second mapping the iso8601
// encoding did.
func (s ServerTimeOperand) RenderComparison(lhs, sqlOp, ph string) string {
	ms := "(" + ServerTimeMillisSQL + " + " + ph + "::bigint)"
	if s.Date {
		ms = "(floor(" + ms + " / 86400000.0) * 86400000)::bigint"
	}
	if !s.RFC3339 {
		return lhs + " " + sqlOp + " " + ms
	}
	return lhs + " " + sqlOp + " " + rfc3339MillisImage(ms)
}

// rfc3339MillisImage renders the epoch millis ms (a bigint expression) as
// the image rfc3339.Image writes: UTC, three fractional digits. The whole
// seconds and the milliseconds are split in exact arithmetic (bigint over a
// numeric divisor, then a bigint remainder) and the milliseconds are added
// as an interval, since to_timestamp of a fractional double can land a
// microsecond short of the millisecond and render the one below.
func rfc3339MillisImage(ms string) string {
	seconds := "floor(" + ms + " / 1000.0)"
	millis := "((" + ms + " % 1000 + 1000) % 1000)"
	return "to_char((to_timestamp(" + seconds + ") + " + millis + " * interval '1 millisecond') AT TIME ZONE 'UTC', " +
		`'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')`
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
