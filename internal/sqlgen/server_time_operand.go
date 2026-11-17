package sqlgen

import (
	"fmt"
	"strconv"

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
// a federated query pins the operand to one instant first
// (ResolveServerTime), and the DuckDB payloads refuse an unresolved one
// (serverTimeUnresolvedError).
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

// serverTimeUnresolvedError refuses a $now leaf that reached a DuckDB
// payload unresolved. DuckDB cannot read the PostgreSQL clock the operand
// is defined by, so a federated Query pins every $now leaf to one instant
// read from PostgreSQL before routing (ResolveServerTime, #715); only a
// direct engine caller that skipped that step lands here.
func serverTimeUnresolvedError(attr string, vt forma.ValueType) error {
	return fmt.Errorf("the $now operand on %s attribute '%s' reached a duckdb pass unresolved: "+
		"a federated query must pin it to one postgresql instant first (sqlgen.ResolveServerTime)", vt, attr)
}

// HasServerTime reports whether cond holds a well-formed $now leaf that
// ResolveServerTime would pin, so a caller reads the clock only for a
// condition that needs it.
func HasServerTime(cond forma.Condition, cache forma.SchemaAttributeCache) bool {
	found := false
	_ = rewriteServerTime(cond, cache, func(forma.AttributeMetadata, extendedLeaf) string {
		found = true
		return ""
	})
	return found
}

// ResolveServerTime returns a copy of cond in which every well-formed $now
// leaf on a date or datetime attribute compares against the fixed instant
// nowMs, shifted by the leaf's offset, instead of the statement's now()
// (#715). A federated Query reads nowMs once from PostgreSQL before routing,
// so the PostgreSQL and the DuckDB legs, and any fallback, compare against
// the same instant.
//
// Each such leaf becomes an ordinary epoch-millisecond literal under the
// same operator, which every leg already compares exactly as $now does: a
// datetime attribute against the shifted instant, a date attribute against
// the start of its UTC calendar day (the truncation RenderComparison
// applies), and an rfc3339-bound column against the instant's canonical
// image, which orders as the instant does (#842). A literal is a bind, so
// the plan caches never key on the instant. A malformed $now leaf is left
// as it is, so normalization refuses it with its own message. cond itself
// is not modified.
func ResolveServerTime(cond forma.Condition, cache forma.SchemaAttributeCache, nowMs int64) forma.Condition {
	return rewriteServerTime(cond, cache, func(meta forma.AttributeMetadata, ext extendedLeaf) string {
		instant := nowMs + ext.now.OffsetMs
		if meta.ValueType == forma.ValueTypeDate {
			instant = utcDayStartMillis(instant)
		}
		return strconv.FormatInt(instant, 10)
	})
}

// utcDayStartMillis floors ms to the start of its UTC calendar day,
// matching RenderComparison's floor(ms / 86400000.0) * 86400000 for
// instants before the epoch too.
func utcDayStartMillis(ms int64) int64 {
	const day = int64(86400000)
	q := ms / day
	if ms%day < 0 {
		q--
	}
	return q * day
}

// rewriteServerTime copies cond, replacing the operand of each well-formed
// $now leaf with what literal returns for it. A leaf whose replacement is
// empty, and every other node, keeps its value.
func rewriteServerTime(cond forma.Condition, cache forma.SchemaAttributeCache, literal func(forma.AttributeMetadata, extendedLeaf) string) forma.Condition {
	switch c := cond.(type) {
	case *forma.CompositeCondition:
		if c == nil {
			return cond
		}
		out := &forma.CompositeCondition{Logic: c.Logic, Conditions: make([]forma.Condition, len(c.Conditions))}
		for i, child := range c.Conditions {
			out.Conditions[i] = rewriteServerTime(child, cache, literal)
		}
		return out
	case *forma.KvCondition:
		if c == nil {
			return cond
		}
		meta, ok := cache[c.Attr]
		lenient := conditionexpr.ParseOperatorValueLenient(c.Value)
		if !isServerTimeLeaf(meta, ok, lenient) {
			return cond
		}
		ext := classifyServerTimeLeaf(c.Attr, conditionexpr.CanonicalOperator(lenient.Operator), meta, lenient.Value)
		if ext.err != nil {
			return cond
		}
		value := literal(meta, ext)
		if value == "" {
			return cond
		}
		return &forma.KvCondition{Attr: c.Attr, Value: lenient.Operator + ":" + value}
	}
	return cond
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
