package queryplan

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

func mustHash(t *testing.T, q *model.FederatedAttributeQuery) string {
	t.Helper()
	h, err := HashFederatedQueryShape(q)
	require.NoError(t, err)
	return h
}

func baseQuery(value string) *model.FederatedAttributeQuery {
	return &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			SchemaID:  7,
			Condition: &forma.KvCondition{Attr: "age", Value: value},
			Limit:     10,
			Offset:    0,
		},
	}
}

func TestShapeHashIgnoresFilterValues(t *testing.T) {
	require.Equal(t, mustHash(t, baseQuery("gt:10")), mustHash(t, baseQuery("gt:99999")))
}

func TestShapeHashCanonicalizesOperatorAliases(t *testing.T) {
	require.Equal(t, mustHash(t, baseQuery("eq:10")), mustHash(t, baseQuery("equals:10")))
}

func TestShapeHashDistinguishesOperators(t *testing.T) {
	require.NotEqual(t, mustHash(t, baseQuery("gt:10")), mustHash(t, baseQuery("lt:10")))
}

func TestShapeHashDistinguishesLogic(t *testing.T) {
	and := &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{Condition: &forma.CompositeCondition{
		Logic: forma.LogicAnd,
		Conditions: []forma.Condition{
			&forma.KvCondition{Attr: "a", Value: "1"},
			&forma.KvCondition{Attr: "b", Value: "2"},
		},
	}}}
	or := &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{Condition: &forma.CompositeCondition{
		Logic: forma.LogicOr,
		Conditions: []forma.Condition{
			&forma.KvCondition{Attr: "a", Value: "1"},
			&forma.KvCondition{Attr: "b", Value: "2"},
		},
	}}}
	require.NotEqual(t, mustHash(t, and), mustHash(t, or))
}

func TestShapeHashDistinguishesSortKeys(t *testing.T) {
	asc := baseQuery("gt:10")
	asc.AttributeOrders = []model.AttributeOrder{{AttrID: 3, AttrName: "age", SortOrder: forma.SortOrderAsc}}
	desc := baseQuery("gt:10")
	desc.AttributeOrders = []model.AttributeOrder{{AttrID: 3, AttrName: "age", SortOrder: forma.SortOrderDesc}}
	require.NotEqual(t, mustHash(t, asc), mustHash(t, desc))
}

func TestShapeHashKeysetValuesDoNotParticipate(t *testing.T) {
	a := baseQuery("gt:10")
	a.KeysetCursor = &model.KeysetCursor{
		Mode:    model.KeysetCursorModeAfter,
		Columns: []model.KeysetColumn{{Attribute: "created_at", Direction: forma.SortOrderDesc}},
		Values:  []any{int64(100)},
	}
	b := baseQuery("gt:10")
	b.KeysetCursor = &model.KeysetCursor{
		Mode:    model.KeysetCursorModeAfter,
		Columns: []model.KeysetColumn{{Attribute: "created_at", Direction: forma.SortOrderDesc}},
		Values:  []any{int64(999999)},
	}
	require.Equal(t, mustHash(t, a), mustHash(t, b))
}

func TestShapeHashKeysetColumnsParticipate(t *testing.T) {
	a := baseQuery("gt:10")
	a.KeysetCursor = &model.KeysetCursor{Mode: model.KeysetCursorModeAfter,
		Columns: []model.KeysetColumn{{Attribute: "created_at", Direction: forma.SortOrderDesc}}}
	b := baseQuery("gt:10")
	b.KeysetCursor = &model.KeysetCursor{Mode: model.KeysetCursorModeAfter,
		Columns: []model.KeysetColumn{{Attribute: "row_id", Direction: forma.SortOrderDesc}}}
	require.NotEqual(t, mustHash(t, a), mustHash(t, b))

	c := baseQuery("gt:10")
	require.NotEqual(t, mustHash(t, a), mustHash(t, c), "keyset vs offset pagination must differ")
}

func TestShapeHashAnchorParticipates(t *testing.T) {
	a := baseQuery("gt:10")
	b := baseQuery("gt:10")
	b.UseMainAsAnchor = true
	require.NotEqual(t, mustHash(t, a), mustHash(t, b))
}

func TestShapeHashLimitOffsetDoNotParticipate(t *testing.T) {
	a := baseQuery("gt:10")
	b := baseQuery("gt:10")
	b.Limit = 500
	b.Offset = 10000
	require.Equal(t, mustHash(t, a), mustHash(t, b))
}

func TestShapeHashChildOrderPreserved(t *testing.T) {
	ab := &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{Condition: &forma.CompositeCondition{
		Logic: forma.LogicAnd,
		Conditions: []forma.Condition{
			&forma.KvCondition{Attr: "a", Value: "1"},
			&forma.KvCondition{Attr: "b", Value: "2"},
		},
	}}}
	ba := &model.FederatedAttributeQuery{AttributeQuery: model.AttributeQuery{Condition: &forma.CompositeCondition{
		Logic: forma.LogicAnd,
		Conditions: []forma.Condition{
			&forma.KvCondition{Attr: "b", Value: "2"},
			&forma.KvCondition{Attr: "a", Value: "1"},
		},
	}}}
	require.NotEqual(t, mustHash(t, ab), mustHash(t, ba),
		"child order determines arg ordering and must stay in the hash")
}
