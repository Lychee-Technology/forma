package production

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// maxAttrMismatches caps the attribute mismatches recorded in a diff.
const maxAttrMismatches = 20

// Diff is the expected-vs-actual comparison written to diff.json.
type Diff struct {
	Query         Query          `json:"query"`
	ExpectedTotal int64          `json:"expected_total"`
	ActualTotal   int64          `json:"actual_total"`
	Missing       []string       `json:"missing_row_ids,omitempty"`
	Extra         []string       `json:"extra_row_ids,omitempty"`
	Misordered    bool           `json:"misordered,omitempty"`
	ExpectedOrder []string       `json:"expected_order,omitempty"`
	ActualOrder   []string       `json:"actual_order,omitempty"`
	Attrs         []AttrMismatch `json:"attribute_mismatches,omitempty"`
}

// AttrMismatch describes one attribute value divergence.
type AttrMismatch struct {
	RowID    string `json:"row_id"`
	Attr     string `json:"attr"`
	Expected any    `json:"expected"`
	Actual   any    `json:"actual"`
}

// IsEmpty reports whether the diff carries no divergence.
func (d *Diff) IsEmpty() bool {
	return d.ExpectedTotal == d.ActualTotal && len(d.Missing) == 0 &&
		len(d.Extra) == 0 && !d.Misordered && len(d.Attrs) == 0
}

// ExpectedState folds the Env's event log into the oracle model for one
// schema.
func (e *Env) ExpectedState(schema SchemaRef) (*ExpectedState, error) {
	cache, ok := e.Metadata.GetSchemaCacheByID(schema.ID)
	if !ok {
		return nil, fmt.Errorf("no metadata cache for schema %d", schema.ID)
	}
	return ExpectedStateFromEvents(e.events, schema, cache)
}

// AssertQueryMatches runs the query through the real engine AND the oracle
// and compares totals, row IDs (ordered when the query sorts), and
// attribute values. On mismatch it dumps all diagnostic artifacts and fails
// the test with the artifact path.
func (e *Env) AssertQueryMatches(ctx context.Context, q Query) *QueryResult {
	e.T.Helper()

	actual, err := e.Query(ctx, q)
	if err != nil {
		e.failWithArtifacts(ctx, fmt.Sprintf("engine query failed: %v", err))
		return nil
	}
	state, err := e.ExpectedState(q.Schema)
	if err != nil {
		e.T.Fatalf("build oracle state: %v", err)
		return nil
	}
	expected, err := state.Run(q)
	if err != nil {
		e.T.Fatalf("oracle run: %v", err)
		return nil
	}

	diff := computeDiff(q, expected, actual, state.Cache)
	if !diff.IsEmpty() {
		e.lastDiff = diff
		e.failWithArtifacts(ctx, fmt.Sprintf(
			"query result diverges from oracle (expected_total=%d actual_total=%d missing=%d extra=%d misordered=%t attr_mismatches=%d)",
			diff.ExpectedTotal, diff.ActualTotal, len(diff.Missing), len(diff.Extra), diff.Misordered, len(diff.Attrs)))
		return nil
	}
	return actual
}

func (e *Env) failWithArtifacts(ctx context.Context, msg string) {
	e.T.Helper()
	dir, err := e.DumpArtifacts(ctx)
	if err != nil {
		e.T.Fatalf("%s\n(artifact dump incomplete: %v; partial output in %s)", msg, err, dir)
		return
	}
	e.T.Fatalf("%s\ndiagnostic artifacts: %s", msg, dir)
}

// computeDiff compares the oracle result with the engine result.
func computeDiff(q Query, expected *ExpectedResult, actual *QueryResult, cache forma.SchemaAttributeCache) *Diff {
	diff := &Diff{Query: q, ExpectedTotal: expected.Total, ActualTotal: actual.Total}

	expectedByID := make(map[uuid.UUID]*ExpectedRow, len(expected.Rows))
	expectedOrder := make([]string, 0, len(expected.Rows))
	for _, row := range expected.Rows {
		expectedByID[row.RowID] = row
		expectedOrder = append(expectedOrder, row.RowID.String())
	}
	actualByID := make(map[uuid.UUID]*model.PersistentRecord, len(actual.Records))
	actualOrder := make([]string, 0, len(actual.Records))
	for _, rec := range actual.Records {
		actualByID[rec.RowID] = rec
		actualOrder = append(actualOrder, rec.RowID.String())
	}

	for id := range expectedByID {
		if _, ok := actualByID[id]; !ok {
			diff.Missing = append(diff.Missing, id.String())
		}
	}
	for id := range actualByID {
		if _, ok := expectedByID[id]; !ok {
			diff.Extra = append(diff.Extra, id.String())
		}
	}

	// Order is asserted only for sorted queries; unsorted result order is an
	// engine implementation detail.
	if len(q.Sorts) > 0 && len(diff.Missing) == 0 && len(diff.Extra) == 0 {
		for i := range expectedOrder {
			if expectedOrder[i] != actualOrder[i] {
				diff.Misordered = true
				diff.ExpectedOrder = expectedOrder
				diff.ActualOrder = actualOrder
				break
			}
		}
	}

	diff.Attrs = compareAttributes(expectedByID, actualByID, cache)
	return diff
}

func compareAttributes(
	expectedByID map[uuid.UUID]*ExpectedRow,
	actualByID map[uuid.UUID]*model.PersistentRecord,
	cache forma.SchemaAttributeCache,
) []AttrMismatch {
	var mismatches []AttrMismatch
	for id, expectedRow := range expectedByID {
		record, ok := actualByID[id]
		if !ok {
			continue
		}
		for name, meta := range cache {
			if len(mismatches) >= maxAttrMismatches {
				return mismatches
			}
			expectedVal := expectedRow.Attrs[name]
			actualVal, err := actualAttrValue(record, meta)
			if err != nil {
				mismatches = append(mismatches, AttrMismatch{
					RowID: id.String(), Attr: name, Expected: expectedVal,
					Actual: fmt.Sprintf("extract error: %v", err),
				})
				continue
			}
			if !valuesEqual(expectedVal, actualVal) {
				mismatches = append(mismatches, AttrMismatch{
					RowID: id.String(), Attr: name, Expected: expectedVal, Actual: actualVal,
				})
			}
		}
	}
	return mismatches
}

func valuesEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return compareValues(a, b) == 0
}

// actualAttrValue extracts one attribute from an engine record and
// normalizes it into the oracle's canonical form: column-bound attributes
// come from the typed entity_main maps, EAV-only attributes from
// OtherAttributes by attr_id.
func actualAttrValue(record *model.PersistentRecord, meta forma.AttributeMetadata) (any, error) {
	if meta.ColumnBinding != nil {
		raw, ok := mainColumnValue(record, meta.ColumnBinding)
		if !ok {
			return nil, nil
		}
		return normalizeValue(raw, meta.ValueType)
	}
	for _, attr := range record.OtherAttributes {
		if attr.AttrID != meta.AttributeID {
			continue
		}
		switch {
		case attr.ValueNumeric != nil:
			return normalizeEAVNumeric(*attr.ValueNumeric, meta.ValueType), nil
		case attr.ValueText != nil:
			return normalizeValue(*attr.ValueText, meta.ValueType)
		default:
			return nil, nil
		}
	}
	return nil, nil
}

// normalizeEAVNumeric maps a value_numeric payload to the canonical form;
// bool is stored as 1/0 and dates as epoch millis, both already float64.
func normalizeEAVNumeric(v float64, vt forma.ValueType) any {
	if vt == forma.ValueTypeBool {
		if v != 0 {
			return float64(1)
		}
		return float64(0)
	}
	return v
}

func mainColumnValue(record *model.PersistentRecord, binding *forma.MainColumnBinding) (any, bool) {
	col := string(binding.ColumnName)
	switch binding.ColumnType() {
	case forma.MainColumnTypeText:
		v, ok := record.TextItems[col]
		return v, ok
	case forma.MainColumnTypeSmallint:
		v, ok := record.Int16Items[col]
		return v, ok
	case forma.MainColumnTypeInteger:
		v, ok := record.Int32Items[col]
		return v, ok
	case forma.MainColumnTypeBigint:
		v, ok := record.Int64Items[col]
		return v, ok
	case forma.MainColumnTypeDouble:
		v, ok := record.Float64Items[col]
		return v, ok
	case forma.MainColumnTypeUUID:
		v, ok := record.UUIDItems[col]
		return v, ok
	default:
		return nil, false
	}
}
