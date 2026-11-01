package federated

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"

	"github.com/stretchr/testify/require"
)

// TestEverySeamRefusesACursorThatReplacesTheRequestOrder pins the
// continuation rule at all four seams (#381 review): the renderer builds the
// keyset ORDER BY from the cursor alone, so a request sorted on `count ASC`
// continued with a valid `created_at DESC, row_id ASC` cursor used to answer a
// page ordered and filtered on created_at. Each seam is handed the order it
// will render — fq.AttributeOrders at Query, the attributeOrders argument
// elsewhere — so the check and the render see one order.
func TestEverySeamRefusesACursorThatReplacesTheRequestOrder(t *testing.T) {
	orders := []model.AttributeOrder{{AttrID: 3, AttrName: "count", SortOrder: forma.SortOrderAsc,
		ValueType: forma.ValueTypeNumeric, StorageLocation: forma.AttributeStorageLocationEAV}}
	createdAtCursor := &model.KeysetCursor{
		Columns: []model.KeysetColumn{
			{Attribute: "created_at", Direction: forma.SortOrderDesc},
			{Attribute: "row_id", Direction: forma.SortOrderAsc},
		},
		Values: []interface{}{int64(5), "r1"},
		Mode:   model.KeysetCursorModeAfter,
	}
	tables := model.StorageTables{EntityMain: "main", EAVData: "eav"}
	newQuery := func() *model.FederatedAttributeQuery {
		return &model.FederatedAttributeQuery{
			AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10, AttributeOrders: orders},
			KeysetCursor:   createdAtCursor,
		}
	}
	const want = `keyset cursor column 1 is "created_at" but the request sorts on "count"`

	engine := NewDBFederatedQueryEngine(
		&fakePostgresFederatedSource{page: &model.PersistentRecordPage{}},
		nil, nil, nil, forma.DuckDBConfig{Enabled: true}, nil, "")
	ctx := context.Background()

	_, err := engine.Query(ctx, tables, newQuery(), &model.FederatedQueryOptions{})
	require.ErrorContains(t, err, want, "Query seam")

	_, _, err = engine.ExecuteFederatedKeysetQuery(ctx, tables, newQuery(), 10, orders, &model.FederatedQueryOptions{})
	require.ErrorContains(t, err, want, "keyset coordinator seam")

	_, _, err = engine.ExecuteDuckDBFederatedQuery(ctx, tables, newQuery(), 10, 0, orders, nil)
	require.ErrorContains(t, err, want, "ExecuteDuckDBFederatedQuery seam")

	_, err = engine.StreamDuckDBFederatedQuery(ctx, tables, newQuery(), 10, 0, orders, nil,
		func(context.Context, *model.PersistentRecord) error { return nil })
	require.ErrorContains(t, err, want, "StreamDuckDBFederatedQuery seam")
}

// TestValidateKeysetCursorContinuesTheDispatchedOrder pins which order the
// validator judges: the one passed for this dispatch, not any other. A cursor
// continuing `count ASC` is admitted against those orders and against none,
// and refused against a different sort.
func TestValidateKeysetCursorContinuesTheDispatchedOrder(t *testing.T) {
	cursor := &model.KeysetCursor{
		Columns: []model.KeysetColumn{
			{Attribute: "count", Direction: forma.SortOrderAsc},
			{Attribute: "row_id", Direction: forma.SortOrderAsc},
		},
		Values: []interface{}{int64(5), "r1"},
		Mode:   model.KeysetCursorModeAfter,
	}
	countAsc := []model.AttributeOrder{{AttrID: 3, AttrName: "count", SortOrder: forma.SortOrderAsc}}
	amountDesc := []model.AttributeOrder{{AttrID: 4, AttrName: "amount", SortOrder: forma.SortOrderDesc}}

	require.NoError(t, validateKeysetCursor(cursor, countAsc), "the cursor continues count ASC")
	require.NoError(t, validateKeysetCursor(cursor, nil), "with no request order the cursor is the order")
	require.ErrorContains(t, validateKeysetCursor(cursor, amountDesc),
		`keyset cursor column 1 is "count" but the request sorts on "amount"`)
}

// TestEverySeamRefusesABoundaryNoRowCanHold pins the text rule (#771) at all
// four seams: a string boundary with a NUL or invalid UTF-8 is refused before
// any source is read. DuckDB fails to bind invalid UTF-8 ("could not bind
// parameter") and compares a NUL against values no row holds.
func TestEverySeamRefusesABoundaryNoRowCanHold(t *testing.T) {
	tables := model.StorageTables{EntityMain: "main", EAVData: "eav"}
	for _, tc := range []struct{ name, value, want string }{
		{"NUL", "r\x001", "text cannot contain a NUL character (U+0000)"},
		{"invalid UTF-8", "r\xff1", "text must be valid UTF-8"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			newQuery := func() *model.FederatedAttributeQuery {
				return &model.FederatedAttributeQuery{
					AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
					KeysetCursor: &model.KeysetCursor{
						Columns: []model.KeysetColumn{{Attribute: "row_id", Direction: forma.SortOrderAsc}},
						Values:  []interface{}{tc.value},
						Mode:    model.KeysetCursorModeAfter,
					},
				}
			}
			want := `keyset cursor value 1 (for column "row_id") is not a value a row can hold: ` + tc.want
			source := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{}}
			engine := NewDBFederatedQueryEngine(source, nil, nil, nil, forma.DuckDBConfig{Enabled: true}, nil, "")
			ctx := context.Background()

			_, err := engine.Query(ctx, tables, newQuery(), &model.FederatedQueryOptions{})
			require.ErrorContains(t, err, want, "Query seam")

			_, _, err = engine.ExecuteFederatedKeysetQuery(ctx, tables, newQuery(), 10, nil, &model.FederatedQueryOptions{})
			require.ErrorContains(t, err, want, "keyset coordinator seam")

			_, _, err = engine.ExecuteDuckDBFederatedQuery(ctx, tables, newQuery(), 10, 0, nil, nil)
			require.ErrorContains(t, err, want, "ExecuteDuckDBFederatedQuery seam")

			_, err = engine.StreamDuckDBFederatedQuery(ctx, tables, newQuery(), 10, 0, nil, nil,
				func(context.Context, *model.PersistentRecord) error { return nil })
			require.ErrorContains(t, err, want, "StreamDuckDBFederatedQuery seam")

			require.Zero(t, source.queryCalls, "no seam may read the Postgres source for a refused cursor")
		})
	}
}
