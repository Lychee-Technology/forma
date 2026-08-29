package sqlgen

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// dottedCache is a schema fixture with dotted attribute names (#260): two
// column-bound dotted attrs, one EAV-only dotted attr, one flat control.
func dottedCache() forma.SchemaAttributeCache {
	return forma.SchemaAttributeCache{
		"contact.name": {
			AttributeID: 1,
			ValueType:   forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{
				ColumnName: "text_01",
			},
		},
		"contact.annualIncome": {
			AttributeID: 2,
			ValueType:   forma.ValueTypeBigInt,
			ColumnBinding: &forma.MainColumnBinding{
				ColumnName: "bigint_01",
			},
		},
		"contact.note": {
			AttributeID: 3,
			ValueType:   forma.ValueTypeText,
		},
		"flag": {
			AttributeID: 4,
			ValueType:   forma.ValueTypeText,
		},
	}
}

// TestBuildSchemaProjection_DottedAttrsEmitAliasedColumns pins #260: every
// SQL fragment must reference dotted attributes by their parquet column
// alias (dots folded to underscores) — the physical name the CDC exporter
// writes — and never emit a bare dotted identifier, which DuckDB parses as
// table.column.
func TestBuildSchemaProjection_DottedAttrsEmitAliasedColumns(t *testing.T) {
	sp, err := BuildSchemaProjection(30, dottedCache())
	require.NoError(t, err)

	fragments := map[string]string{
		"S3SourceSelect": sp.S3SourceSelect,
		"PGSourceSelect": sp.PGSourceSelect,
		"EAVPivotSelect": sp.EAVPivotSelect,
		"OuterSelect":    sp.OuterSelect,
		"PGSelectNoEAV":  sp.BuildPGSelectNoEAV(),
	}
	for name, frag := range fragments {
		require.NotContains(t, frag, "contact.", "%s must not contain a bare dotted identifier:\n%s", name, frag)
	}

	// S3 projection: physical parquet columns, sorted by logical attr name.
	require.Equal(t,
		"row_id, ltbase_created_at AS created_at, changed_at AS ver_ts, deleted_at AS deleted_ts, "+
			"contact_annualIncome, contact_name, contact_note, flag",
		sp.S3SourceSelect)

	// PG source: hot_vals pivot references and output aliases both folded.
	require.Contains(t, sp.PGSourceSelect,
		"COALESCE(ANY_VALUE(hot_vals.contact_annualIncome), m.bigint_01) AS contact_annualIncome")
	require.Contains(t, sp.PGSourceSelect,
		"COALESCE(ANY_VALUE(hot_vals.contact_name), m.text_01) AS contact_name")
	require.Contains(t, sp.PGSourceSelect, "ANY_VALUE(hot_vals.contact_note) AS contact_note")

	// EAV pivot aliases folded.
	require.Contains(t, sp.EAVPivotSelect, "MAX(CASE WHEN attr_id = 3 THEN value_text END) AS contact_note")

	// Outer select: unified columns referenced by alias, mapped back to
	// physical entity_main descriptors.
	require.Contains(t, sp.OuterSelect, "CAST(contact_annualIncome AS BIGINT) AS bigint_01")
	require.Contains(t, sp.OuterSelect, "CAST(contact_name AS VARCHAR) AS text_01")
	require.Contains(t, sp.OuterSelect, "CASE WHEN contact_note IS NOT NULL THEN")

	// No-EAV PG select folds output aliases too.
	require.Contains(t, sp.BuildPGSelectNoEAV(), "m.bigint_01 AS contact_annualIncome")

	// Metadata maps stay keyed by the logical attribute name.
	require.Contains(t, sp.AttrToMainColumn, "contact.annualIncome")
	require.Equal(t, forma.ValueTypeBigInt, sp.UnifiedColumnTypes["contact.annualIncome"])
}

// TestBuildDuckClause_DottedAttrsEmitAliasedColumns pins #260 for the WHERE
// side: the DuckClause is applied both against raw read_parquet (physical
// parquet columns) and against the visible CTE (unified columns), so it must
// reference the folded alias in both — a bare dotted name binder-errors, a
// quoted dotted name misses the physical column.
func TestBuildDuckClause_DottedAttrsEmitAliasedColumns(t *testing.T) {
	cache := dottedCache()

	clause, args, err := BuildDuckClause(&forma.KvCondition{
		Attr:  "contact.annualIncome",
		Value: "gt:5000",
	}, cache)
	require.NoError(t, err)
	require.Contains(t, clause, "contact_annualIncome >")
	require.NotContains(t, clause, "contact.annualIncome")
	require.Len(t, args, 1)

	clause, args, err = BuildDuckClause(&forma.KvCondition{
		Attr:  "contact.note",
		Value: "hello",
	}, cache)
	require.NoError(t, err)
	require.Equal(t, "contact_note = ?", clause)
	require.Len(t, args, 1)
}

// TestBuildNonKeysetOrderBy_DottedEAVAttrUsesAlias pins #260 for the sort
// side: an EAV attribute order references the unified CTE column, which
// carries the folded alias. (Column-bound orders use the physical
// entity_main output alias and never contained dots.)
func TestBuildNonKeysetOrderBy_DottedEAVAttrUsesAlias(t *testing.T) {
	q := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			AttributeOrders: []model.AttributeOrder{{
				AttrName:  "contact.note",
				SortOrder: forma.SortOrderDesc,
			}},
		},
	}
	require.Equal(t, "contact_note DESC, row_id ASC", buildNonKeysetOrderBy(q))
}

// TestBuildSchemaProjection_AliasCollisionFails pins the lossy-fold guard:
// "contact.name" and "contact_name" both fold to parquet column
// "contact_name"; projecting both would silently serve one attribute's
// values as the other's, so the reader must refuse.
func TestBuildSchemaProjection_AliasCollisionFails(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"contact.name": {AttributeID: 1, ValueType: forma.ValueTypeText},
		"contact_name": {AttributeID: 2, ValueType: forma.ValueTypeText},
	}
	_, err := BuildSchemaProjection(30, cache)
	require.Error(t, err)
	require.Contains(t, err.Error(), "contact.name")
	require.Contains(t, err.Error(), "contact_name")
}

// TestBuildSchemaProjection_ReservedColumnFails pins the PR #273 review P1
// on the reader: "row.id" folds to the system column "row_id", which the
// s3_source/pg_source projections emit themselves.
func TestBuildSchemaProjection_ReservedColumnFails(t *testing.T) {
	_, err := BuildSchemaProjection(30, forma.SchemaAttributeCache{
		"row.id": {AttributeID: 1, ValueType: forma.ValueTypeText},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "row.id")
	require.Contains(t, err.Error(), "reserved")
}
