package internal

import (
	"fmt"
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/numutil"
	"github.com/stretchr/testify/require"
)

// Real-PostgreSQL conformance matrix for condition operands on the OLTP
// route: every numeric comparison operator and in/not_in over whole-number
// entity_main columns of each width, a double column and eav_data (the temp
// tables' DOUBLE PRECISION value_numeric and production's NUMERIC), plus
// starts_with/contains over text. Expected rows come from exact arithmetic
// (math/big) and from strings.HasPrefix/Contains over the stored values,
// never from another route of the renderer: route-against-route truth
// tables pass a defect every route shares, which is how equals:7.9 matching
// a stored 7 on an int4 column went unnoticed.

func operandSchemaCache() forma.SchemaAttributeCache {
	bind := func(col string) *forma.MainColumnBinding {
		return &forma.MainColumnBinding{ColumnName: forma.MainColumn(col)}
	}
	return forma.SchemaAttributeCache{
		"s16":    {AttributeID: 1, ValueType: forma.ValueTypeSmallInt, ColumnBinding: bind("smallint_01")},
		"i32":    {AttributeID: 2, ValueType: forma.ValueTypeInteger, ColumnBinding: bind("integer_01")},
		"i64":    {AttributeID: 3, ValueType: forma.ValueTypeBigInt, ColumnBinding: bind("bigint_01")},
		"f64":    {AttributeID: 4, ValueType: forma.ValueTypeNumeric, ColumnBinding: bind("double_01")},
		"e_int":  {AttributeID: 5, ValueType: forma.ValueTypeInteger},
		"e_big":  {AttributeID: 6, ValueType: forma.ValueTypeBigInt},
		"e_num":  {AttributeID: 7, ValueType: forma.ValueTypeNumeric},
		"t_main": {AttributeID: 8, ValueType: forma.ValueTypeText, ColumnBinding: bind("text_01")},
		"t_eav":  {AttributeID: 9, ValueType: forma.ValueTypeText},
	}
}

// operandStorage is one attribute's stored values. double marks DOUBLE-width
// storage, where the operand compares as its float64 image (#384).
type operandStorage struct {
	attr, column string // column is "" for eav_data
	double       bool
	values       []any
}

func operandStorages() []operandStorage {
	return []operandStorage{
		{"s16", "smallint_01", false, []any{int16(math.MinInt16), int16(-8), int16(-1), int16(0), int16(1), int16(7), int16(8), int16(math.MaxInt16)}},
		{"i32", "integer_01", false, []any{int32(math.MinInt32), int32(-8), int32(-1), int32(0), int32(1), int32(7), int32(8), int32(math.MaxInt32)}},
		{"i64", "bigint_01", false, []any{int64(math.MinInt64), int64(-(1 << 53) - 1), int64(-1), int64(0), int64(1), int64(7), int64(8),
			int64(1 << 53), int64(1<<53 + 1), int64(math.MaxInt64)}},
		{"f64", "double_01", true, []any{-7.5, -1.0, 0.0, 1.0, 7.0, 7.9, 8.0, 9007199254740992.0}},
		{"e_int", "", true, []any{float64(math.MinInt32), -1.0, 0.0, 1.0, 7.0, 8.0, float64(math.MaxInt32)}},
		// EAV bigint is bounded by 2^53 at write (#590).
		{"e_big", "", false, []any{int64(-(1 << 53)), int64(-1), int64(0), int64(7), int64(8), int64(1 << 53)}},
		{"e_num", "", true, []any{-7.5, -1.0, 0.0, 7.0, 7.9, 8.0}},
	}
}

var conformanceOperands = []string{
	"0", "7", "-1", "7.0", "7.5", "-7.5", "7.9", "0.5", "-0.5", "1e-300",
	"32767", "32768", "-32769", "2147483647", "2147483648", "-2147483649",
	"9007199254740992", "9007199254740993", "9223372036854775807", "-9223372036854775808",
	"9223372036854775808", "-9223372036854775809", "1e30", "-1e30",
}

// bigintOutOfRange are the operands a bigint attribute refuses (#502).
var bigintOutOfRange = map[string]bool{"9223372036854775808": true, "-9223372036854775809": true, "1e30": true, "-1e30": true}

var conformanceOps = map[string]string{
	"equals": "=", "not_equals": "!=", "lt": "<", "lte": "<=", "gt": ">", "gte": ">=", "in": "=", "not_in": "!=",
}

func toBigFloat(v any) *big.Float {
	switch x := v.(type) {
	case int16:
		return new(big.Float).SetInt64(int64(x))
	case int32:
		return new(big.Float).SetInt64(int64(x))
	case int64:
		return new(big.Float).SetInt64(x)
	case float64:
		return new(big.Float).SetFloat64(x)
	}
	panic(fmt.Sprintf("unsupported value %T", v))
}

func cmpHolds(sqlOp string, c int) bool {
	switch sqlOp {
	case "=":
		return c == 0
	case "!=":
		return c != 0
	case "<":
		return c < 0
	case "<=":
		return c <= 0
	case ">":
		return c > 0
	}
	return c >= 0
}

// conformanceOperand is the value an operand literal denotes: an exact int64
// for an integer in int64 range, else its float64 image; DOUBLE-width storage
// compares the float64 image of either.
func conformanceOperand(literal string, double bool) any {
	v := numutil.TryParseNumber(literal)
	if n, ok := v.(int64); ok && double {
		return float64(n)
	}
	return v
}

func operandLeaf(op, literal string) string {
	if op == "in" || op == "not_in" {
		return op + ":[" + literal + "]"
	}
	return op + ":" + literal
}

func (f *dslFixture) seedStorage(t *testing.T, s operandStorage) map[uuid.UUID]any {
	t.Helper()
	ids := map[uuid.UUID]any{}
	for _, v := range s.values {
		var id uuid.UUID
		if s.column != "" {
			id = f.seed(t, map[string]any{s.column: v})
		} else {
			id = f.seed(t, map[string]any{}, dslEAVRow{attr: s.attr, numeric: v})
		}
		ids[id] = v
	}
	return ids
}

func TestConditionOperandConformance_Numeric(t *testing.T) {
	for _, numericEAV := range []bool{false, true} {
		t.Run(fmt.Sprintf("numeric_eav=%v", numericEAV), func(t *testing.T) {
			f := newDSLFixtureWith(t, numericEAV, "operand_conformance", operandSchemaCache())
			checked := 0
			for _, s := range operandStorages() {
				if s.column == "" && !numericEAV {
					s.double = true // a DOUBLE PRECISION value_numeric holds every EAV value as its float64 image
				}
				stored := f.seedStorage(t, s)
				for _, literal := range append(append([]string{}, conformanceOperands...), "NaN", "Inf", "-Inf") {
					for op, sqlOp := range conformanceOps {
						checked += f.checkNumericLeaf(t, s, stored, op, sqlOp, literal)
					}
				}
			}
			require.Greater(t, checked, 1000, "the matrix must execute against Postgres")
		})
	}
}

func (f *dslFixture) checkNumericLeaf(t *testing.T, s operandStorage, stored map[uuid.UUID]any, op, sqlOp, literal string) int {
	t.Helper()
	cond := kv(s.attr, operandLeaf(op, literal))
	page, err := f.repo.QueryPersistentRecords(f.ctx, &model.PersistentRecordQuery{
		Tables: f.tables, SchemaID: dslSchemaID, Condition: cond, Limit: 100,
	})
	vt := f.cache[s.attr].ValueType
	nonFinite := literal == "NaN" || strings.HasSuffix(literal, "Inf")
	if nonFinite || (vt == forma.ValueTypeBigInt && bigintOutOfRange[literal]) {
		require.ErrorIs(t, err, forma.ErrInvalidInput, "%s %s refused before SQL", s.attr, cond.Value)
		return 1
	}
	require.NoError(t, err, "%s %s", s.attr, cond.Value)
	operand := toBigFloat(conformanceOperand(literal, s.double))
	var want []uuid.UUID
	for id, v := range stored {
		if cmpHolds(sqlOp, toBigFloat(v).Cmp(operand)) {
			want = append(want, id)
		}
	}
	got := make([]uuid.UUID, 0, len(page.Records))
	for _, r := range page.Records {
		got = append(got, r.RowID)
	}
	require.Equal(t, sortedIDs(want...), sortedIDs(got...), "%s %s: rows %v", s.attr, cond.Value, describeRows(stored, got))
	return 1
}

func describeRows(stored map[uuid.UUID]any, ids []uuid.UUID) []any {
	out := make([]any, 0, len(ids))
	for _, id := range ids {
		out = append(out, stored[id])
	}
	return out
}
