package widthaudit

import (
	"context"
	"math"
	"regexp"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/require"
)

func bound(column forma.MainColumn) *forma.MainColumnBinding {
	return &forma.MainColumnBinding{ColumnName: column}
}

// ledgerCache holds every numeric-family binding next to a double column, and
// one EAV-only bigint.
func ledgerCache(t *testing.T) *schemameta.MetadataCache {
	t.Helper()
	mc := schemameta.NewMetadataCache()
	require.NoError(t, mc.RegisterSchema("ledger", 300, forma.SchemaAttributeCache{
		"total":  {AttributeName: "total", AttributeID: 1, ValueType: forma.ValueTypeBigInt},
		"ratio":  {AttributeName: "ratio", AttributeID: 2, ValueType: forma.ValueTypeBigInt, ColumnBinding: bound(forma.MainColumnDouble02)},
		"amount": {AttributeName: "amount", AttributeID: 3, ValueType: forma.ValueTypeBigInt, ColumnBinding: bound(forma.MainColumnBigint01)},
		"share":  {AttributeName: "share", AttributeID: 4, ValueType: forma.ValueTypeInteger, ColumnBinding: bound(forma.MainColumnDouble01)},
		"price":  {AttributeName: "price", AttributeID: 5, ValueType: forma.ValueTypeNumeric, ColumnBinding: bound(forma.MainColumnDouble03)},
	}))
	return mc
}

// TestTargetsSelectsBigintBoundToDoubleColumn pins the column-bound targets
// (#618): a bigint in a double_* column is stored as its float64 image and
// is held to ±2^53, so it is audited and carries its column. A bigint in a
// bigint column keeps the exact int64. integer and numeric in a double
// column are not held to the bigint contract.
func TestTargetsSelectsBigintBoundToDoubleColumn(t *testing.T) {
	require.Equal(t, []Target{
		{SchemaID: 300, SchemaName: "ledger", AttrID: 1, AttrName: "total", Declared: forma.ValueTypeBigInt},
		{SchemaID: 300, SchemaName: "ledger", AttrID: 2, AttrName: "ratio", Declared: forma.ValueTypeBigInt, Column: "double_02"},
	}, Targets(ledgerCache(t)))
}

// TestTargetStorageMatchesDoubleColumnsExactly: a binding to a name
// entity_main does not have is not a stored value. The validator loads
// metadata with the binding check deferred, so such a binding can reach the
// census. It is reported by that check, and the census must not scan for it.
func TestTargetStorageMatchesDoubleColumnsExactly(t *testing.T) {
	for _, column := range []forma.MainColumn{"double_04", "DOUBLE_01", "double"} {
		_, _, ok := targetStorage(forma.AttributeMetadata{ValueType: forma.ValueTypeBigInt, ColumnBinding: bound(column)})
		require.False(t, ok, "bigint bound to %s", column)
	}
	for _, column := range []forma.MainColumn{forma.MainColumnDouble01, forma.MainColumnDouble02, forma.MainColumnDouble03} {
		declared, got, ok := targetStorage(forma.AttributeMetadata{ValueType: forma.ValueTypeBigInt, ColumnBinding: bound(column)})
		require.True(t, ok, "bigint bound to %s", column)
		require.Equal(t, forma.ValueTypeBigInt, declared)
		require.Equal(t, string(column), got)
	}
}

// TestBuildColumnCensusQuery: the column is a bound parameter matched by a
// CASE over the double columns, and the bound is the float8 2^53 the write
// funnel admits up to, inclusive.
func TestBuildColumnCensusQuery(t *testing.T) {
	targets := []Target{
		{SchemaID: 1, AttrID: 2, Declared: forma.ValueTypeBigInt, Column: "double_01"},
		{SchemaID: 7, AttrID: 9, Declared: forma.ValueTypeBigInt, Column: "double_03"},
	}
	query, args := BuildColumnCensusQuery(Tables{EntityMain: "entity_main", ChangeLog: "change_log"}, targets)
	require.Equal(t, []any{int16(1), int16(2), "double_01", int16(7), int16(9), "double_03"}, args)
	require.Contains(t, query, "($4::smallint, $5::smallint, $6::text)")
	require.Contains(t, query, `FROM "entity_main" AS m`)
	require.Contains(t, query,
		`CASE w.col WHEN 'double_01' THEN m."double_01" WHEN 'double_02' THEN m."double_02" WHEN 'double_03' THEN m."double_03" END AS image`)
	require.Contains(t, query, "ABS(v.image) > 9007199254740992::float8 OR v.image <> TRUNC(v.image)")
	require.Contains(t, query, `FROM "change_log" AS c`)
	require.Contains(t, query, "WHERE c.schema_id = m.ltbase_schema_id AND c.row_id = m.ltbase_row_id")
}

// TestBuildColumnCensusQueryWithoutChangeLog: with no CDC the census still
// runs and reports every row as never flushed.
func TestBuildColumnCensusQueryWithoutChangeLog(t *testing.T) {
	query, _ := BuildColumnCensusQuery(Tables{EntityMain: "entity_main"},
		[]Target{{SchemaID: 1, AttrID: 2, Declared: forma.ValueTypeBigInt, Column: "double_01"}})
	require.NotContains(t, query, "flushed_at = 0")
	require.Contains(t, query, "0::bigint AS last_flushed_at")
}

func columnCensusRows() *pgxmock.Rows {
	return pgxmock.NewRows(strings.Fields("ltbase_schema_id attr_id ltbase_row_id image pending last_flushed_at"))
}

// TestCensusScansBothStores: one census reads eav_data for the EAV-only
// targets and entity_main for the column-bound ones, and each finding
// carries its own target. The column value is printed in plain digits.
func TestCensusScansBothStores(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	targets := Targets(ledgerCache(t))
	tables := Tables{EAV: "eav_data", EntityMain: "entity_main", ChangeLog: "change_log"}
	eavRow := uuid.MustParse("55555555-5555-5555-5555-555555555555")
	mainRow := uuid.MustParse("66666666-6666-6666-6666-666666666666")

	eavQuery, eavArgs := BuildCensusQuery(tables, targets[:1])
	mock.ExpectQuery(regexp.QuoteMeta(eavQuery)).WithArgs(eavArgs...).
		WillReturnRows(censusRows().AddRow(int16(300), int16(1), eavRow, "", "9007199254740994", false, int64(0)))
	columnQuery, columnArgs := BuildColumnCensusQuery(tables, targets[1:])
	mock.ExpectQuery(regexp.QuoteMeta(columnQuery)).WithArgs(columnArgs...).
		WillReturnRows(columnCensusRows().
			AddRow(int16(300), int16(2), mainRow, float64(9007199254740994), false, int64(1700)).
			AddRow(int16(300), int16(2), eavRow, 1000.5, true, int64(0)))

	got, err := Census(context.Background(), mock, tables, targets)
	require.NoError(t, err)
	require.Equal(t, []Finding{
		{Target: targets[0], RowID: eavRow, StoredValue: "9007199254740994"},
		{Target: targets[1], RowID: mainRow, StoredValue: "9007199254740994", LastFlushedAt: 1700},
		{Target: targets[1], RowID: eavRow, StoredValue: "1000.5", Pending: true},
	}, got)
	require.Equal(t, "double_02", got[1].Column)
	for _, f := range got {
		require.Equal(t, ClassBigIntOutOfContract, f.Classify(0))
		require.False(t, f.Classify(0).Requeueable())
	}
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestCensusColumnTargetsOnly: with no EAV-only target the census issues
// only the entity_main query.
func TestCensusColumnTargetsOnly(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	targets := []Target{{SchemaID: 1, AttrID: 2, Declared: forma.ValueTypeBigInt, Column: "double_01"}}
	tables := Tables{EAV: "eav_data", EntityMain: "entity_main"}
	query, args := BuildColumnCensusQuery(tables, targets)
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(args...).WillReturnRows(columnCensusRows())

	got, err := Census(context.Background(), mock, tables, targets)
	require.NoError(t, err)
	require.Empty(t, got)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestCensusColumnTargetsNeedEntityMain: a census that cannot read the
// column must fail, not pass as clean over rows it never looked at.
func TestCensusColumnTargetsNeedEntityMain(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	targets := []Target{{SchemaID: 1, SchemaName: "ledger", AttrID: 2, AttrName: "ratio", Declared: forma.ValueTypeBigInt, Column: "double_01"}}
	_, err = Census(context.Background(), mock, Tables{EAV: "eav_data"}, targets)
	require.ErrorContains(t, err, "schema=ledger attribute=ratio column=double_01")
	require.ErrorContains(t, err, "column-bound attributes: no entity main table is configured")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCensusColumnRejectsRowOutsideTargets(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	targets := []Target{{SchemaID: 1, AttrID: 2, Declared: forma.ValueTypeBigInt, Column: "double_01"}}
	mock.ExpectQuery(`SELECT m\.ltbase_schema_id`).WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnRows(columnCensusRows().AddRow(int16(1), int16(9), uuid.New(), 1.5, false, int64(0)))

	_, err = Census(context.Background(), mock, Tables{EAV: "eav_data", EntityMain: "entity_main"}, targets)
	require.ErrorContains(t, err, "schema_id=1 attr_id=9, which is not a census target")
}

// TestFormatDoubleImage: a whole image prints as its exact digits, the
// spelling the write funnel's refusal uses, so 2^63 is not shortened to
// 9223372036854776000.
func TestFormatDoubleImage(t *testing.T) {
	tests := []struct {
		image float64
		want  string
	}{
		{9007199254740994, "9007199254740994"},
		{-9007199254740994, "-9007199254740994"},
		{math.Ldexp(1, 63), "9223372036854775808"},
		{1000.5, "1000.5"},
		{-0.25, "-0.25"},
		{1e300, "1e+300"},
		{math.NaN(), "NaN"},
		{math.Inf(1), "Infinity"},
		{math.Inf(-1), "-Infinity"},
	}
	for _, tt := range tests {
		require.Equal(t, tt.want, formatDoubleImage(tt.image))
	}
}
