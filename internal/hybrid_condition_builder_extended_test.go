package internal

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// Golden output of the #646 leaves on the OLTP route (hybrid builder). The
// helper's cache: name/tag text EAV, age integer_01, created bigint_01
// unix_ms, created_iso text_02 iso8601, active_int smallint_01 bool,
// active_text text_03 bool, score double_01.

const hybridNowMs = "(floor(extract(epoch from now()) * 1000)::bigint + $2::bigint)"

func TestHybrid_SetOperandsOnMainColumns(t *testing.T) {
	h := newHybridTestHelper(true)
	cases := []struct {
		name, attr, value, want string
		args                    []any
	}{
		{"integer column binds bigint[] and drops fractions", "age", "in:[18, 18.5, 21]",
			`m."integer_01" = ANY($2::bigint[])`, []any{[]int64{18, 21}}},
		{"double column binds float8[]", "score", "not_in:[1.5, 2]",
			`(m."double_01" IS NOT NULL AND m."double_01" <> ALL($2::float8[]))`, []any{[]float64{1.5, 2}}},
		{"unix_ms datetime binds epoch millis", "created", `in:["2024-01-02T03:04:05Z"]`,
			`m."bigint_01" = ANY($2::bigint[])`, []any{[]int64{1704164645000}}},
		{"iso8601 datetime binds the stored image", "created_iso", "in:[1704164645000]",
			`m."text_02" = ANY($2::text[])`, []any{[]string{"2024-01-02T03:04:05Z"}}},
		{"bool_smallint compares the truthiness image", "active_int", "in:[true]",
			`(m."smallint_01" > 0.5) = ANY($2::boolean[])`, []any{[]bool{true}}},
		{"bool_text compares the read contract", "active_text", "not_in:[false]",
			`((m."text_03" = '1') IS NOT NULL AND (m."text_03" = '1') <> ALL($2::boolean[]))`, []any{[]bool{false}}},
		{"raw bigint column", "ltbase_created_at", "in:[1, 2]",
			`m."ltbase_created_at" = ANY($2::bigint[])`, []any{[]int64{1, 2}}},
		{"raw uuid column binds uuid[]", "ltbase_row_id", `in:["6F1C3C3E-1111-4111-8111-111111111111"]`,
			`m."ltbase_row_id" = ANY($2::uuid[])`, []any{[]string{"6f1c3c3e-1111-4111-8111-111111111111"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clause, args, err := h.build(&forma.KvCondition{Attr: tc.attr, Value: tc.value})
			require.NoError(t, err)
			require.Equal(t, tc.want, clause)
			require.Equal(t, tc.args, args)
		})
	}
}

func TestHybrid_SetOperandOnEAV(t *testing.T) {
	h := newHybridTestHelper(false)
	clause, args, err := h.build(&forma.KvCondition{Attr: "name", Value: `not_in:["a","b"]`})
	require.NoError(t, err)
	require.Equal(t,
		"EXISTS (SELECT 1 FROM eav_data x WHERE x.schema_id = t.schema_id AND x.row_id = t.row_id AND x.attr_id = $2 "+
			"AND (x.value_text IS NOT NULL AND x.value_text <> ALL($3)))",
		clause)
	require.Equal(t, []any{int16(7), []string{"a", "b"}}, args)
}

func TestHybrid_NullTests(t *testing.T) {
	h := newHybridTestHelper(true)
	clause, args, err := h.build(&forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
		&forma.KvCondition{Attr: "age", Value: "is_null:true"},
		&forma.KvCondition{Attr: "name", Value: "is_null:true"},
		&forma.KvCondition{Attr: "tag", Value: "is_not_null:true"},
		&forma.KvCondition{Attr: "age", Value: "lt:5"},
	}})
	require.NoError(t, err)
	require.Equal(t,
		`(m."integer_01" IS NULL) AND `+
			"(NOT EXISTS (SELECT 1 FROM eav_data x WHERE x.schema_id = m.ltbase_schema_id AND x.row_id = m.ltbase_row_id AND x.attr_id = $2 AND x.value_text IS NOT NULL)) AND "+
			"(EXISTS (SELECT 1 FROM eav_data x WHERE x.schema_id = m.ltbase_schema_id AND x.row_id = m.ltbase_row_id AND x.attr_id = $3 AND x.value_text IS NOT NULL)) AND "+
			`(m."integer_01" < $4)`,
		clause)
	require.Equal(t, []any{int16(7), int16(9), int64(5)}, args)
}

func TestHybrid_ServerTime(t *testing.T) {
	h := newHybridTestHelper(true)
	image := func(seconds string) string {
		return `to_char(to_timestamp(` + seconds + `) AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')`
	}
	cases := []struct {
		name, attr, value, want string
		args                    []any
	}{
		{"unix_ms datetime", "created", "lte:$now",
			`m."bigint_01" <= ` + hybridNowMs, []any{int64(0)}},
		{"offset is a bind", "created", "gt:$now-90s",
			`m."bigint_01" > ` + hybridNowMs, []any{int64(-90000)}},
		{"iso8601 datetime compares the floor image", "created_iso", "gt:$now+1h",
			`m."text_02" > ` + image("floor("+hybridNowMs+" / 1000.0)"), []any{int64(3600000)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clause, args, err := h.build(&forma.KvCondition{Attr: tc.attr, Value: tc.value})
			require.NoError(t, err)
			require.Equal(t, tc.want, clause)
			require.Equal(t, tc.args, args)
		})
	}

	// $now names a server time only on date and datetime attributes.
	_, _, err := h.build(&forma.KvCondition{Attr: "ltbase_created_at", Value: "lte:$now"})
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	require.Contains(t, err.Error(), "invalid numeric value for 'ltbase_created_at': $now")
	clause, args, err := h.build(&forma.KvCondition{Attr: "name", Value: "equals:$now"})
	require.NoError(t, err)
	require.Contains(t, clause, "x.value_text = $3")
	require.Equal(t, []any{int16(7), "$now"}, args)
}

// is_null must see an entity with no eav_data row, so it forces the
// entity_main anchor; every other operator keeps the existing choice.
func TestHasMainTableCondition_IsNullForcesMainAnchor(t *testing.T) {
	cache := forma.SchemaAttributeCache{"name": {AttributeID: 7, ValueType: forma.ValueTypeText}}
	or := &forma.CompositeCondition{Logic: forma.LogicOr, Conditions: []forma.Condition{
		&forma.KvCondition{Attr: "name", Value: "equals:x"},
		&forma.KvCondition{Attr: "name", Value: "is_null:true"},
	}}
	require.True(t, hasMainTableCondition(or, cache))
	require.True(t, hasMainTableCondition(&forma.KvCondition{Attr: "name", Value: "IS_NULL:true"}, cache))
	require.False(t, hasMainTableCondition(&forma.KvCondition{Attr: "name", Value: "is_not_null:true"}, cache))
	require.False(t, hasMainTableCondition(&forma.KvCondition{Attr: "name", Value: `not_in:["x"]`}, cache))
}

// An empty and renders 1=1, so like is_null it holds for an entity with no
// eav_data row and forces the entity_main anchor, alone or inside an or. An
// empty or renders 1=0 and keeps the existing choice.
func TestHasMainTableCondition_EmptyAndForcesMainAnchor(t *testing.T) {
	cache := forma.SchemaAttributeCache{"name": {AttributeID: 7, ValueType: forma.ValueTypeText}}
	eav := &forma.KvCondition{Attr: "name", Value: "equals:x"}
	emptyAnd := &forma.CompositeCondition{Logic: forma.LogicAnd}
	emptyOr := &forma.CompositeCondition{Logic: forma.LogicOr}
	require.True(t, hasMainTableCondition(emptyAnd, cache))
	require.True(t, hasMainTableCondition(&forma.CompositeCondition{Logic: forma.LogicOr, Conditions: []forma.Condition{emptyAnd, eav}}, cache))
	require.False(t, hasMainTableCondition(emptyOr, cache))
	require.False(t, hasMainTableCondition(&forma.CompositeCondition{Logic: forma.LogicOr, Conditions: []forma.Condition{emptyOr, eav}}, cache))
}

// The OLTP render cache keys on the clause text and the arg count, so a list
// of any length and a $now of any offset must reuse one rendered statement.
func TestOptimizedQueryShapeKey_IgnoresListLengthAndNowOffset(t *testing.T) {
	h := newHybridTestHelper(true)
	tables := model.StorageTables{EAVData: "eav_data", EntityMain: "entity_main", ChangeLog: "change_log"}
	key := func(cond forma.Condition) uint64 {
		clause, args, err := h.build(cond)
		require.NoError(t, err)
		return optimizedQueryShapeKey(tables, true, clause, len(args), nil)
	}
	and := func(value string) forma.Condition {
		return &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: []forma.Condition{
			&forma.KvCondition{Attr: "name", Value: value},
			&forma.KvCondition{Attr: "age", Value: "gt:1"},
		}}
	}

	require.Equal(t, key(and(`in:["a","b"]`)), key(and(`in:["a","b","c","d","e","f","g"]`)))
	require.Equal(t, key(&forma.KvCondition{Attr: "age", Value: "in:[1,2]"}),
		key(&forma.KvCondition{Attr: "age", Value: "in:[1,2,3,4,5,6,7]"}))
	require.Equal(t, key(&forma.KvCondition{Attr: "created", Value: "lte:$now"}),
		key(&forma.KvCondition{Attr: "created", Value: "lte:$now-30m"}))
	require.NotEqual(t, key(&forma.KvCondition{Attr: "created", Value: "lte:$now"}),
		key(&forma.KvCondition{Attr: "created", Value: "lte:1704164645000"}),
		"$now and an absolute timestamp are different statements")
}
