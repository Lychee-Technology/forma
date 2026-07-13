package sqlgen

import (
	"testing"

	"github.com/lychee-technology/forma/internal/model"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Tests for buildNonKeysetOrderBy
// ============================================================================

func TestBuildNonKeysetOrderBy_NilQuery_ReturnsDefault(t *testing.T) {
	got := buildNonKeysetOrderBy(nil)
	require.Equal(t, "created_at DESC, row_id ASC", got)
}

func TestBuildNonKeysetOrderBy_NoOrders_ReturnsDefault(t *testing.T) {
	q := &model.FederatedAttributeQuery{}
	got := buildNonKeysetOrderBy(q)
	require.Equal(t, "created_at DESC, row_id ASC", got)
}

func TestBuildNonKeysetOrderBy_MainColumn_ASC(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			AttributeOrders: []model.AttributeOrder{
				{
					StorageLocation: forma.AttributeStorageLocationMain,
					ColumnName:      "text_01",
					AttrName:        "title",
					SortOrder:       forma.SortOrderAsc,
				},
			},
		},
	}
	got := buildNonKeysetOrderBy(q)
	require.Equal(t, "text_01 ASC, row_id ASC", got)
}

func TestBuildNonKeysetOrderBy_MainColumn_DESC(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			AttributeOrders: []model.AttributeOrder{
				{
					StorageLocation: forma.AttributeStorageLocationMain,
					ColumnName:      "num_01",
					AttrName:        "price",
					SortOrder:       forma.SortOrderDesc,
				},
			},
		},
	}
	got := buildNonKeysetOrderBy(q)
	require.Equal(t, "num_01 DESC, row_id ASC", got)
}

// TestBuildNonKeysetOrderBy_EAVColumn uses the attribute's logical name (AttrName)
// as the ORDER BY column, since the unified CTE projects EAV attributes by their name.
func TestBuildNonKeysetOrderBy_EAVColumn_UsesAttrName(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			AttributeOrders: []model.AttributeOrder{
				{
					StorageLocation: forma.AttributeStorageLocationEAV,
					AttrName:        "tag",
					SortOrder:       forma.SortOrderAsc,
				},
			},
		},
	}
	got := buildNonKeysetOrderBy(q)
	require.Equal(t, "tag ASC, row_id ASC", got)
}

// TestBuildNonKeysetOrderBy_EAVColumn_NoAttrName_FallsBackToDefault ensures that
// an EAV model.AttributeOrder with no AttrName set is skipped and the default is returned.
func TestBuildNonKeysetOrderBy_EAVColumn_NoAttrName_FallsBackToDefault(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			AttributeOrders: []model.AttributeOrder{
				{
					StorageLocation: forma.AttributeStorageLocationEAV,
					// AttrName intentionally empty — simulates legacy/incomplete data
				},
			},
		},
	}
	got := buildNonKeysetOrderBy(q)
	require.Equal(t, "created_at DESC, row_id ASC", got)
}

// TestBuildNonKeysetOrderBy_Mixed_MainAndEAV confirms that both main-column and
// EAV sort keys are resolved and combined in order.
func TestBuildNonKeysetOrderBy_Mixed_MainAndEAV(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			AttributeOrders: []model.AttributeOrder{
				{
					StorageLocation: forma.AttributeStorageLocationMain,
					ColumnName:      "text_01",
					AttrName:        "title",
					SortOrder:       forma.SortOrderAsc,
				},
				{
					StorageLocation: forma.AttributeStorageLocationEAV,
					AttrName:        "priority",
					SortOrder:       forma.SortOrderDesc,
				},
			},
		},
	}
	got := buildNonKeysetOrderBy(q)
	require.Equal(t, "text_01 ASC, priority DESC, row_id ASC", got)
}

// TestBuildNonKeysetOrderBy_AlwaysAppendsRowIDTiebreak pins the stable-sort
// contract (#183): equal user-sort keys must not leave page windows to DuckDB
// scan order. Mirrors the PG optimized template (trailing m.ltbase_row_id) and
// the production-harness oracle (row_id ASC).
func TestBuildNonKeysetOrderBy_AlwaysAppendsRowIDTiebreak(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			AttributeOrders: []model.AttributeOrder{
				{
					StorageLocation: forma.AttributeStorageLocationEAV,
					AttrName:        "qty",
					SortOrder:       forma.SortOrderAsc,
				},
			},
		},
	}
	require.Equal(t, "qty ASC, row_id ASC", buildNonKeysetOrderBy(q))
}
