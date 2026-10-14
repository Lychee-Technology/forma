package internal

import (
	"context"
	"fmt"
	"maps"
	"math"
	"strings"
	"time"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// An update's operators (#649) apply in two places, by what they read.
// Intake has checked every operator against the schema
// (validateConditionalUpdate) and refused any attribute Updates also writes,
// and the merge has refused one Updates writes only on the locked row
// (mergeUpdateDocument), so each operator is the only writer of its
// attribute.
//
// server_time reads only the transaction clock and replaces its attribute
// outright, so it is an attributeAssignment that the merge writes as it
// writes Updates (mergeUpdateDocument). The stored value it replaces is then
// discarded undecoded, as a literal replacement's is (#590), and a legacy
// value the read refuses cannot block it.
//
// increment reads the locked row's value, so applyIncrements runs on the
// merged document once the values it keeps are decoded.

// attributeAssignment is a value an update writes at an attribute's dotted
// path, over whatever the merged document holds there.
type attributeAssignment struct {
	attr  string
	value any
}

// serverTimeAssignments answers the value of each server_time operator in
// ops, in order. The clock is read once, and only when one needs it.
func serverTimeAssignments(ctx context.Context, ops []forma.UpdateOp, cache forma.SchemaAttributeCache, clock model.TxClock) ([]attributeAssignment, error) {
	var assigned []attributeAssignment
	var now int64
	for _, op := range ops {
		if op.Kind != forma.UpdateOpServerTime {
			continue
		}
		if len(assigned) == 0 {
			ms, err := readTxClock(ctx, clock)
			if err != nil {
				return nil, fmt.Errorf("server_time on attribute '%s': %w", op.Attribute, err)
			}
			now = ms
		}
		assigned = append(assigned, attributeAssignment{attr: op.Attribute, value: serverTimeValue(now, op.Offset, cache[op.Attribute])})
	}
	return assigned, nil
}

// applyIncrements applies the increment operators in ops to doc, the merged
// document with the locked row's kept values decoded.
func applyIncrements(doc map[string]any, ops []forma.UpdateOp, cache forma.SchemaAttributeCache) error {
	for _, op := range ops {
		switch op.Kind {
		case forma.UpdateOpIncrement:
			if err := applyIncrement(doc, op, cache[op.Attribute].ValueType); err != nil {
				return fmt.Errorf("increment on attribute '%s': %w", op.Attribute, err)
			}
		case forma.UpdateOpServerTime:
			// assigned during the merge (serverTimeAssignments)
		default:
			return fmt.Errorf("update operator on attribute '%s' has unvalidated kind '%s'", op.Attribute, op.Kind)
		}
	}
	return nil
}

func readTxClock(ctx context.Context, clock model.TxClock) (int64, error) {
	if clock == nil {
		return 0, fmt.Errorf("the repository supplied no transaction clock")
	}
	ms, err := clock(ctx)
	if err != nil {
		return 0, fmt.Errorf("read the transaction clock: %w", err)
	}
	return ms, nil
}

// applyIncrement adds op.By to the attribute's value in doc. A missing value
// is refused rather than read as zero: the caller writes the initial value.
func applyIncrement(doc map[string]any, op forma.UpdateOp, vt forma.ValueType) error {
	current, err := documentValue(doc, op.Attribute)
	if err != nil {
		return fmt.Errorf("read the current value: %w", err)
	}
	if current == nil {
		return forma.InvalidInputf("increment on %s attribute '%s' needs a current value, and the row has none; write an initial value first",
			vt, op.Attribute)
	}
	value, ok := integerValue(current)
	if !ok {
		return fmt.Errorf("increment on %s attribute '%s': the row holds %v (%T), not an integer", vt, op.Attribute, current, current)
	}
	next, err := incrementWithin(value, op.By, op.Attribute, vt)
	if err != nil {
		return fmt.Errorf("add %d: %w", op.By, err)
	}
	if err := setDocumentValue(doc, op.Attribute, next); err != nil {
		return fmt.Errorf("write the incremented value: %w", err)
	}
	return nil
}

// integerValue reads a decoded smallint, integer or bigint value.
func integerValue(v any) (int64, bool) {
	switch n := v.(type) {
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case int:
		return int64(n), true
	}
	return 0, false
}

// incrementWithin answers current+by, or ErrInvalidInput when the sum leaves
// vt's range. The comparisons are arranged so that none of them overflows.
func incrementWithin(current, by int64, attr string, vt forma.ValueType) (int64, error) {
	lo, hi := integerRange(vt)
	if by > 0 && current > hi-by {
		return 0, forma.InvalidInputf("increment by %d on %s attribute '%s' would take its value %d past the %s maximum %d",
			by, vt, attr, current, vt, hi)
	}
	if by < 0 && current < lo-by {
		return 0, forma.InvalidInputf("increment by %d on %s attribute '%s' would take its value %d past the %s minimum %d",
			by, vt, attr, current, vt, lo)
	}
	return current + by, nil
}

// integerRange is the value range of an integer value type.
func integerRange(vt forma.ValueType) (int64, int64) {
	switch vt {
	case forma.ValueTypeSmallInt:
		return math.MinInt16, math.MaxInt16
	case forma.ValueTypeInteger:
		return math.MinInt32, math.MaxInt32
	}
	return math.MinInt64, math.MaxInt64
}

const millisPerDay = 24 * 60 * 60 * 1000

// serverTimeValue is server_time's value: the transaction instant nowMs
// shifted by offset, at the precision the attribute stores. A date stores
// the instant's UTC calendar day, the day $now names on a date (#646); a
// datetime bound to an iso8601 text column stores whole seconds and refuses
// anything finer, so the instant is floored to its second.
func serverTimeValue(nowMs int64, offset time.Duration, meta forma.AttributeMetadata) time.Time {
	ms := nowMs + offset.Milliseconds()
	switch {
	case meta.ValueType == forma.ValueTypeDate:
		ms = floorMillis(ms, millisPerDay)
	case meta.ColumnBinding != nil && meta.ColumnBinding.Encoding == forma.MainColumnEncodingISO8601:
		ms = floorMillis(ms, 1000)
	}
	return time.UnixMilli(ms).UTC()
}

func floorMillis(ms, unit int64) int64 {
	rem := ms % unit
	if rem < 0 {
		rem += unit
	}
	return ms - rem
}

// documentValue answers the value at attr's dotted path in doc, nil when
// there is none.
func documentValue(doc map[string]any, attr string) (any, error) {
	segments := strings.Split(attr, ".")
	parent, err := documentParent(doc, attr, segments, false)
	if err != nil {
		return nil, fmt.Errorf("find the object holding '%s': %w", attr, err)
	}
	if parent == nil {
		return nil, nil
	}
	return parent[segments[len(segments)-1]], nil
}

// setDocumentValue writes value at attr's dotted path in doc, creating the
// objects on the way that the row does not hold.
func setDocumentValue(doc map[string]any, attr string, value any) error {
	segments := strings.Split(attr, ".")
	parent, err := documentParent(doc, attr, segments, true)
	if err != nil {
		return fmt.Errorf("find the object holding '%s': %w", attr, err)
	}
	parent[segments[len(segments)-1]] = value
	return nil
}

// documentParent walks to the object holding attr's last segment. A missing
// object answers nil, or is created when write is set. A list on the way is
// refused: an operator writes one value, and an attribute inside an array has
// one per element.
//
// When write is set, every object on the way is replaced by a shallow copy
// before the walk enters it. The merged document holds the caller's own
// Updates maps where the row has no object to merge them into (mergeMaps),
// and a write into one would change the caller's request: sent again, it
// would then be refused for writing the operator's attribute in Updates.
func documentParent(doc map[string]any, attr string, segments []string, write bool) (map[string]any, error) {
	current := doc
	for i, segment := range segments[:len(segments)-1] {
		next, present := current[segment]
		if !present || next == nil {
			if !write {
				return nil, nil
			}
			child := map[string]any{}
			current[segment] = child
			current = child
			continue
		}
		switch child := next.(type) {
		case map[string]any:
			if write {
				clone := make(map[string]any, len(child)+1)
				maps.Copy(clone, child)
				current[segment], child = clone, clone
			}
			current = child
		case []any:
			return nil, operatorInsideArray(attr, strings.Join(segments[:i+1], "."))
		default:
			return nil, fmt.Errorf("attribute '%s': the row holds a %T at '%s', not the object the attribute nests in",
				attr, next, strings.Join(segments[:i+1], "."))
		}
	}
	return current, nil
}

// operatorInsideArray refuses an operator on attr, which lies inside the
// array at array: an operator writes one value, and the attribute has one
// per element. Intake refuses with it where the schema declares the array,
// documentParent where the locked row holds one.
func operatorInsideArray(attr, array string) error {
	return forma.InvalidInputf("attribute '%s' is inside the array '%s'; an update operator writes one value, not one per element", attr, array)
}
