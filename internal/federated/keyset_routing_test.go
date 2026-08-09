package federated

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// keysetCursorAfterCreatedAt builds a minimal well-formed cursor: one ordering
// column plus the trailing row_id tiebreak validateKeysetTiebreak requires
// (#183). The values are inert — these tests assert routing, never row
// selection, so no seeded row needs to match.
func keysetCursorAfterCreatedAt() *model.KeysetCursor {
	return &model.KeysetCursor{
		Columns: []model.KeysetColumn{
			{Attribute: "created_at", Direction: forma.SortOrderAsc},
			{Attribute: "row_id", Direction: forma.SortOrderAsc},
		},
		Values: []any{int64(500), "00000000-0000-0000-0000-000000000000"},
		Mode:   model.KeysetCursorModeAfter,
	}
}

// TestQueryPreferHotRejectsKeysetCursor pins door 1 of #354: the hot-only gate
// short-circuits to a Postgres-only path with no keyset support, so pre-fix a
// PreferHot request carrying a cursor received an unfiltered first page and
// pagination never advanced.
func TestQueryPreferHotRejectsKeysetCursor(t *testing.T) {
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 5}}
	engine := NewDBFederatedQueryEngine(pg, nil, nil, nil, forma.DuckDBConfig{Enabled: true}, nil, "")

	page, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav"},
		&model.FederatedAttributeQuery{
			AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
			PreferHot:      true,
			KeysetCursor:   keysetCursorAfterCreatedAt(),
		},
		&model.FederatedQueryOptions{IncludeExecutionPlan: true})

	require.Nil(t, page)
	require.ErrorIs(t, err, ErrKeysetUnsupportedOnPostgres)
	require.NotErrorIs(t, err, forma.ErrInvalidInput,
		"settled error class: a plain read-path error, never a write-path validation carrier — rebuilding this as forma.InvalidInputf would silently turn an operator-visible routing failure into a caller-facing 4xx")
	require.Zero(t, pg.queryCalls,
		"fail closed: postgres must not be queried at all, or the engine pays for a page the caller must never see")
}

// TestQueryHotOnlyTiersRejectsKeysetCursor pins door 1's other half: the gate
// fires on PreferredTiers == [hot] as well as on PreferHot (engine.go:150).
func TestQueryHotOnlyTiersRejectsKeysetCursor(t *testing.T) {
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 5}}
	engine := NewDBFederatedQueryEngine(pg, nil, nil, nil, forma.DuckDBConfig{Enabled: true}, nil, "")

	page, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav"},
		&model.FederatedAttributeQuery{
			AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
			PreferredTiers: []model.DataTier{model.DataTierHot},
			KeysetCursor:   keysetCursorAfterCreatedAt(),
		},
		&model.FederatedQueryOptions{IncludeExecutionPlan: true})

	require.Nil(t, page)
	require.ErrorIs(t, err, ErrKeysetUnsupportedOnPostgres)
	require.Zero(t, pg.queryCalls)
}

// TestQueryRejectsKeysetCursorWhenDuckDBDisabled pins door 2b: with DuckDB
// globally disabled every request routes Postgres-only and there is nothing to
// override onto, so a cursor must fail rather than be dropped.
func TestQueryRejectsKeysetCursorWhenDuckDBDisabled(t *testing.T) {
	pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 5}}
	engine := NewDBFederatedQueryEngine(pg, nil, nil, nil, forma.DuckDBConfig{Enabled: false}, nil, "")

	page, err := engine.Query(context.Background(),
		model.StorageTables{EntityMain: "main", EAVData: "eav"},
		&model.FederatedAttributeQuery{
			AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
			PreferredTiers: []model.DataTier{model.DataTierHot, model.DataTierCold},
			KeysetCursor:   keysetCursorAfterCreatedAt(),
		},
		&model.FederatedQueryOptions{IncludeExecutionPlan: true})

	require.Nil(t, page)
	require.ErrorIs(t, err, ErrKeysetUnsupportedOnPostgres)
	require.Zero(t, pg.queryCalls)
}

// TestQueryWithoutCursorStillServesPostgresOnly is the negative control: the
// guard must key on an ACTIVE cursor only, so an absent cursor and an empty
// column list (the open first page — the same no-op contract
// validateKeysetTiebreak applies) both keep the Postgres-only path working.
func TestQueryWithoutCursorStillServesPostgresOnly(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cursor *model.KeysetCursor
	}{
		{"nil_cursor", nil},
		{"empty_columns", &model.KeysetCursor{Mode: model.KeysetCursorModeAfter}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pg := &fakePostgresFederatedSource{page: &model.PersistentRecordPage{TotalRecords: 3}}
			engine := NewDBFederatedQueryEngine(pg, nil, nil, nil, forma.DuckDBConfig{Enabled: true}, nil, "")

			page, err := engine.Query(context.Background(),
				model.StorageTables{EntityMain: "main", EAVData: "eav"},
				&model.FederatedAttributeQuery{
					AttributeQuery: model.AttributeQuery{SchemaID: 7, Limit: 10},
					PreferHot:      true,
					KeysetCursor:   tc.cursor,
				},
				&model.FederatedQueryOptions{IncludeExecutionPlan: true})

			require.NoError(t, err)
			require.Equal(t, int64(3), page.TotalRecords)
			require.Equal(t, 1, pg.queryCalls)
		})
	}
}
