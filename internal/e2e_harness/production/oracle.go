package production

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	forma "github.com/lychee-technology/forma"
)

// ExpectedRow is the oracle's folded state for one row.
type ExpectedRow struct {
	RowID     uuid.UUID      `json:"row_id"`
	Attrs     map[string]any `json:"attrs"`
	Deleted   bool           `json:"deleted"`
	ChangedAt int64          `json:"changed_at"`
	Seq       int            `json:"seq"`
}

// ExpectedState is the independent expected-state model required by #173:
// pure functions over the event log, importing nothing from the federated
// engine. Pipeline: group by row_id -> latest by (ChangedAt, Seq) -> drop
// deleted -> typed filters -> sort (row_id ASC tiebreak) -> paginate.
type ExpectedState struct {
	Schema SchemaRef
	Cache  forma.SchemaAttributeCache
	Rows   map[uuid.UUID]*ExpectedRow
}

// ExpectedResult is the oracle's answer for one Query.
type ExpectedResult struct {
	Total int64
	Rows  []*ExpectedRow
}

// ExpectedStateFromEvents folds the event log into per-row expected state.
// Events are applied in (ChangedAt, Seq) order — the last write wins, with
// the harness-assigned sequence breaking equal-millisecond timestamps.
func ExpectedStateFromEvents(events []*Event, schema SchemaRef, cache forma.SchemaAttributeCache) (*ExpectedState, error) {
	ordered := make([]*Event, 0, len(events))
	for _, ev := range events {
		if ev.Schema.ID == schema.ID {
			ordered = append(ordered, ev)
		}
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].ChangedAt != ordered[j].ChangedAt {
			return ordered[i].ChangedAt < ordered[j].ChangedAt
		}
		return ordered[i].Seq < ordered[j].Seq
	})

	state := &ExpectedState{Schema: schema, Cache: cache, Rows: make(map[uuid.UUID]*ExpectedRow)}
	for _, ev := range ordered {
		if err := state.applyEvent(ev); err != nil {
			return nil, err
		}
	}
	return state, nil
}

func (s *ExpectedState) applyEvent(ev *Event) error {
	row := s.Rows[ev.RowID]
	if row == nil {
		row = &ExpectedRow{RowID: ev.RowID, Attrs: make(map[string]any)}
		s.Rows[ev.RowID] = row
	}
	row.ChangedAt = ev.ChangedAt
	row.Seq = ev.Seq

	switch ev.Kind {
	case EventCreate, EventUpdate:
		row.Deleted = false
		for name, value := range ev.Attrs {
			meta, ok := s.Cache[name]
			if !ok {
				return fmt.Errorf("event seq=%d references unknown attribute %q", ev.Seq, name)
			}
			normalized, err := normalizeValue(value, meta.ValueType)
			if err != nil {
				return fmt.Errorf("normalize %s (seq=%d): %w", name, ev.Seq, err)
			}
			row.Attrs[name] = normalized
		}
	case EventDelete:
		row.Deleted = true
	default:
		return fmt.Errorf("unknown event kind %q", ev.Kind)
	}
	return nil
}

// Run evaluates a Query against the expected state.
func (s *ExpectedState) Run(q Query) (*ExpectedResult, error) {
	var visible []*ExpectedRow
	for _, row := range s.Rows {
		if row.Deleted {
			continue
		}
		match, err := s.matches(row, q.Filters)
		if err != nil {
			return nil, err
		}
		if match {
			visible = append(visible, row)
		}
	}

	if err := s.sortRows(visible, q.Sorts); err != nil {
		return nil, err
	}

	result := &ExpectedResult{Total: int64(len(visible))}
	start := q.Offset
	if start < 0 {
		start = 0
	}
	if start >= len(visible) {
		return result, nil
	}
	end := len(visible)
	if q.Limit > 0 && start+q.Limit < end {
		end = start + q.Limit
	}
	result.Rows = visible[start:end]
	return result, nil
}

// matches applies the typed filters with SQL NULL semantics: any comparison
// against an absent attribute excludes the row.
func (s *ExpectedState) matches(row *ExpectedRow, filters []Filter) (bool, error) {
	for _, f := range filters {
		meta, ok := s.Cache[f.Attr]
		if !ok {
			return false, fmt.Errorf("filter references unknown attribute %q", f.Attr)
		}
		actual, present := row.Attrs[f.Attr]
		if !present || actual == nil {
			return false, nil
		}
		expected, err := normalizeFilterValue(f.Value, meta.ValueType)
		if err != nil {
			return false, fmt.Errorf("filter value for %s: %w", f.Attr, err)
		}
		match, err := evalFilterOp(f.Op, actual, expected)
		if err != nil {
			return false, fmt.Errorf("filter %s: %w", f.Attr, err)
		}
		if !match {
			return false, nil
		}
	}
	return true, nil
}

func evalFilterOp(op string, actual, expected any) (bool, error) {
	if op == "" {
		op = string(forma.FilterEquals)
	}
	switch forma.FilterType(op) {
	case forma.FilterEquals:
		return compareValues(actual, expected) == 0, nil
	case forma.FilterNotEquals:
		return compareValues(actual, expected) != 0, nil
	case forma.FilterGreaterThan:
		return compareValues(actual, expected) > 0, nil
	case forma.FilterGreaterEq:
		return compareValues(actual, expected) >= 0, nil
	case forma.FilterLessThan:
		return compareValues(actual, expected) < 0, nil
	case forma.FilterLessEq:
		return compareValues(actual, expected) <= 0, nil
	case forma.FilterStartsWith:
		a, okA := actual.(string)
		b, okB := expected.(string)
		if !okA || !okB {
			return false, fmt.Errorf("starts_with requires text operands, got %T/%T", actual, expected)
		}
		return strings.HasPrefix(a, b), nil
	case forma.FilterContains:
		a, okA := actual.(string)
		b, okB := expected.(string)
		if !okA || !okB {
			return false, fmt.Errorf("contains requires text operands, got %T/%T", actual, expected)
		}
		return strings.Contains(a, b), nil
	default:
		return false, fmt.Errorf("unsupported filter op %q", op)
	}
}

// sortRows orders rows by the sort keys with row_id ASC as the final
// tiebreak. Absent values sort last regardless of direction (DuckDB's
// default NULLS LAST).
func (s *ExpectedState) sortRows(rows []*ExpectedRow, sorts []Sort) error {
	for _, key := range sorts {
		if _, ok := s.Cache[key.Attr]; !ok {
			return fmt.Errorf("sort references unknown attribute %q", key.Attr)
		}
	}
	sort.SliceStable(rows, func(i, j int) bool {
		for _, key := range sorts {
			a, aOK := rows[i].Attrs[key.Attr]
			b, bOK := rows[j].Attrs[key.Attr]
			if !aOK || a == nil || !bOK || b == nil {
				if (aOK && a != nil) == (bOK && b != nil) {
					continue
				}
				return aOK && a != nil // non-null first (NULLS LAST)
			}
			cmp := compareValues(a, b)
			if cmp == 0 {
				continue
			}
			if key.Desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return compareUUIDs(rows[i].RowID, rows[j].RowID) < 0
	})
	return nil
}

// normalizeValue converts a raw attribute value into the oracle's canonical
// comparison form. The numeric normalization matches the verified EAV
// storage convention on BOTH the mainline write path
// (transform.populateTypedValue / ToEAVRecord) and read path
// (extractValueFromEAVRecord): the numeric family — smallint, integer,
// bigint, numeric, bool (as 1/0), date and datetime (as epoch millis) —
// lives in value_numeric as float64; text and uuid live in value_text.
func normalizeValue(value any, vt forma.ValueType) (any, error) {
	if value == nil {
		return nil, nil
	}
	switch vt {
	case forma.ValueTypeText:
		s, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("text value has type %T", value)
		}
		return s, nil
	case forma.ValueTypeUUID:
		return normalizeUUID(value)
	case forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeNumeric:
		return toFloat64(value)
	case forma.ValueTypeBool:
		return normalizeBool(value)
	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		return normalizeTimeMillis(value)
	default:
		return nil, fmt.Errorf("unsupported value type %q", vt)
	}
}

// normalizeFilterValue parses the string form used by the public condition
// API into the canonical comparison form for the attribute's type.
func normalizeFilterValue(raw string, vt forma.ValueType) (any, error) {
	switch vt {
	case forma.ValueTypeText, forma.ValueTypeUUID:
		return normalizeValue(raw, vt)
	case forma.ValueTypeBool:
		// The engine's condition parser accepts integers ("1"/"0").
		n, err := strconv.Atoi(raw)
		if err != nil {
			return nil, fmt.Errorf("bool filter value %q must be 1/0: %w", raw, err)
		}
		if n > 0 {
			return float64(1), nil
		}
		return float64(0), nil
	case forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeNumeric:
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return nil, fmt.Errorf("numeric filter value %q: %w", raw, err)
		}
		return f, nil
	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		return normalizeTimeMillis(raw)
	default:
		return nil, fmt.Errorf("unsupported filter value type %q", vt)
	}
}

func normalizeUUID(value any) (any, error) {
	switch v := value.(type) {
	case uuid.UUID:
		return v.String(), nil
	case string:
		parsed, err := uuid.Parse(v)
		if err != nil {
			return nil, fmt.Errorf("parse uuid %q: %w", v, err)
		}
		return parsed.String(), nil
	default:
		return nil, fmt.Errorf("uuid value has type %T", value)
	}
}

func normalizeBool(value any) (any, error) {
	switch v := value.(type) {
	case bool:
		if v {
			return float64(1), nil
		}
		return float64(0), nil
	case float64:
		if v != 0 {
			return float64(1), nil
		}
		return float64(0), nil
	default:
		return nil, fmt.Errorf("bool value has type %T", value)
	}
}

// normalizeTimeMillis converts date/datetime representations (epoch millis,
// RFC3339 strings, "2006-01-02" dates, time.Time) to epoch-millis float64.
func normalizeTimeMillis(value any) (any, error) {
	switch v := value.(type) {
	case time.Time:
		return float64(v.UnixMilli()), nil
	case float64:
		return v, nil
	case int64:
		return float64(v), nil
	case int:
		return float64(v), nil
	case string:
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil {
			return float64(ms), nil
		}
		for _, format := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02"} {
			if parsed, err := time.Parse(format, v); err == nil {
				return float64(parsed.UnixMilli()), nil
			}
		}
		return nil, fmt.Errorf("unsupported time format %q", v)
	default:
		return nil, fmt.Errorf("time value has type %T", value)
	}
}

func toFloat64(value any) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int16:
		return float64(v), nil
	case int32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return 0, fmt.Errorf("numeric value %q: %w", v, err)
		}
		return f, nil
	default:
		return 0, fmt.Errorf("numeric value has type %T", value)
	}
}

// compareValues is the single typed comparator used for filters and sorts:
// float64 for the numeric family (per the EAV value_numeric convention),
// lexicographic for strings. Mixed types compare by their string forms as a
// last resort.
func compareValues(a, b any) int {
	af, aIsNum := a.(float64)
	bf, bIsNum := b.(float64)
	if aIsNum && bIsNum {
		switch {
		case af < bf:
			return -1
		case af > bf:
			return 1
		default:
			return 0
		}
	}
	as, aIsStr := a.(string)
	bs, bIsStr := b.(string)
	if aIsStr && bIsStr {
		return strings.Compare(as, bs)
	}
	return strings.Compare(fmt.Sprintf("%v", a), fmt.Sprintf("%v", b))
}

func compareUUIDs(a, b uuid.UUID) int {
	for i := range a {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}
