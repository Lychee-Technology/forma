package widthaudit

import (
	"context"
	"regexp"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/require"
)

func auditCache(t *testing.T) *schemameta.MetadataCache {
	t.Helper()
	mc := schemameta.NewMetadataCache()
	require.NoError(t, mc.RegisterSchema("wide", 200, forma.SchemaAttributeCache{
		"qty":    {AttributeName: "qty", AttributeID: 14, ValueType: forma.ValueTypeInteger},
		"level":  {AttributeName: "level", AttributeID: 13, ValueType: forma.ValueTypeSmallInt},
		"big":    {AttributeName: "big", AttributeID: 15, ValueType: forma.ValueTypeBigInt},
		"scores": {AttributeName: "scores", AttributeID: 16, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeInteger},
		"tags":   {AttributeName: "tags", AttributeID: 17, ValueType: forma.ValueTypeList},
		"price":  {AttributeName: "price", AttributeID: 18, ValueType: forma.ValueTypeNumeric},
		"flag":   {AttributeName: "flag", AttributeID: 19, ValueType: forma.ValueTypeBool},
		"when":   {AttributeName: "when", AttributeID: 20, ValueType: forma.ValueTypeDate},
		"bound": {AttributeName: "bound", AttributeID: 21, ValueType: forma.ValueTypeInteger,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: "integer_01"}},
	}))
	require.NoError(t, mc.RegisterSchema("simple", 100, forma.SchemaAttributeCache{
		"count": {AttributeName: "count", AttributeID: 3, ValueType: forma.ValueTypeSmallInt},
	}))
	return mc
}

// TestTargetsSelectsEAVOnlyIntegerWidths pins who is audited: EAV-only
// attributes whose declared width is an integer width, and list attributes
// by their items type. Bound attributes are excluded because their storage
// is the physical int column. numeric, bool, date and text carry no integer
// width to violate.
func TestTargetsSelectsEAVOnlyIntegerWidths(t *testing.T) {
	got := Targets(auditCache(t))
	want := []Target{
		{SchemaID: 100, SchemaName: "simple", AttrID: 3, AttrName: "count", Declared: forma.ValueTypeSmallInt},
		{SchemaID: 200, SchemaName: "wide", AttrID: 13, AttrName: "level", Declared: forma.ValueTypeSmallInt},
		{SchemaID: 200, SchemaName: "wide", AttrID: 14, AttrName: "qty", Declared: forma.ValueTypeInteger},
		{SchemaID: 200, SchemaName: "wide", AttrID: 15, AttrName: "big", Declared: forma.ValueTypeBigInt},
		{SchemaID: 200, SchemaName: "wide", AttrID: 16, AttrName: "scores", Declared: forma.ValueTypeInteger},
	}
	require.Equal(t, want, got)
}

// TestBuildCensusQueryBindsExactBounds: the bounds are exact NUMERIC text
// equal to the write funnel's ranges. A float bound would round int64's
// maximum up to 2^63 and let 2^63 itself pass.
func TestBuildCensusQueryBindsExactBounds(t *testing.T) {
	targets := []Target{
		{SchemaID: 1, AttrID: 2, Declared: forma.ValueTypeSmallInt},
		{SchemaID: 1, AttrID: 3, Declared: forma.ValueTypeInteger},
		{SchemaID: 1, AttrID: 4, Declared: forma.ValueTypeBigInt},
	}
	query, args := BuildCensusQuery(Tables{EAV: "eav_data", ChangeLog: "change_log"}, targets)
	require.Equal(t, []any{
		int16(1), int16(2), "-32768", "32767",
		int16(1), int16(3), "-2147483648", "2147483647",
		int16(1), int16(4), "-9223372036854775808", "9223372036854775807",
	}, args)
	require.Contains(t, query, "($9::smallint, $10::smallint, $11::numeric, $12::numeric)")
	require.Contains(t, query, `FROM "eav_data" AS e`)
	require.Contains(t, query, `FROM "change_log" AS c`)
	require.Contains(t, query, "e.value_numeric <> TRUNC(e.value_numeric)")
}

// TestBuildCensusQueryWithoutChangeLog: a deployment without CDC has nothing
// exported, so the census still runs and reports every row as never flushed.
func TestBuildCensusQueryWithoutChangeLog(t *testing.T) {
	query, _ := BuildCensusQuery(Tables{EAV: "eav_data"}, []Target{{SchemaID: 1, AttrID: 2, Declared: forma.ValueTypeInteger}})
	require.NotContains(t, query, "LATERAL")
	require.Contains(t, query, "0::bigint AS last_flushed_at")
}

func TestCensusScansFindingsAgainstTargets(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	targets := Targets(auditCache(t))
	tables := Tables{EAV: "eav_data", ChangeLog: "change_log"}
	query, args := BuildCensusQuery(tables, targets)
	rowID := uuid.MustParse("55555555-5555-5555-5555-555555555555")
	mock.ExpectQuery(regexp.QuoteMeta(query)).WithArgs(args...).
		WillReturnRows(censusRows().
			AddRow(int16(200), int16(14), rowID, "", "4294967296", false, int64(1700)).
			AddRow(int16(200), int16(16), rowID, "1", "1.5", true, int64(0)))

	got, err := Census(context.Background(), mock, tables, targets)
	require.NoError(t, err)
	require.Equal(t, []Finding{
		{Target: targets[2], RowID: rowID, StoredValue: "4294967296", LastFlushedAt: 1700},
		{Target: targets[4], RowID: rowID, ArrayIndices: "1", StoredValue: "1.5", Pending: true},
	}, got)
	require.NoError(t, mock.ExpectationsWereMet())
}

// TestCensusRejectsRowOutsideTargets: the VALUES join confines the census to
// its targets, so any other row means the query and the index disagree. A
// row labelled with the wrong attribute must not be reported.
func TestCensusRejectsRowOutsideTargets(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	targets := []Target{{SchemaID: 1, AttrID: 2, Declared: forma.ValueTypeInteger}}
	mock.ExpectQuery(`SELECT e\.schema_id`).WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnRows(censusRows().AddRow(int16(1), int16(9), uuid.New(), "", "7", false, int64(0)))

	_, err = Census(context.Background(), mock, Tables{EAV: "eav_data"}, targets)
	require.ErrorContains(t, err, "schema_id=1 attr_id=9, which is not a census target")
}

func TestCensusWithoutTargetsIssuesNoQuery(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	defer mock.Close()

	got, err := Census(context.Background(), mock, Tables{EAV: "eav_data"}, nil)
	require.NoError(t, err)
	require.Empty(t, got)
	require.NoError(t, mock.ExpectationsWereMet())
}

func censusRows() *pgxmock.Rows {
	return pgxmock.NewRows(strings.Fields("schema_id attr_id row_id array_indices value_numeric pending last_flushed_at"))
}

func TestClassify(t *testing.T) {
	narrow := Target{Declared: forma.ValueTypeInteger}
	big := Target{Declared: forma.ValueTypeBigInt}
	const cutover = int64(5000)
	tests := []struct {
		name    string
		finding Finding
		cutover int64
		want    Class
	}{
		{"never exported", Finding{Target: narrow}, cutover, ClassConsistent},
		{"pending is served hot", Finding{Target: narrow, Pending: true, LastFlushedAt: 10}, cutover, ClassConsistent},
		{"exported before cutover", Finding{Target: narrow, LastFlushedAt: cutover - 1}, cutover, ClassStaleExport},
		{"exported at cutover", Finding{Target: narrow, LastFlushedAt: cutover}, cutover, ClassConsistent},
		{"exported, cutover unknown", Finding{Target: narrow, LastFlushedAt: 10}, 0, ClassStaleCandidate},
		{"bigint never exported", Finding{Target: big}, cutover, ClassBigIntOutOfContract},
		{"bigint pending", Finding{Target: big, Pending: true}, 0, ClassBigIntOutOfContract},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.finding.Classify(tt.cutover))
		})
	}
}

// TestRequeueableClasses: only the classes a re-export repairs are
// requeueable. bigint re-exports through the same BIGINT cast, and a
// consistent row needs nothing.
func TestRequeueableClasses(t *testing.T) {
	require.True(t, ClassStaleExport.Requeueable())
	require.True(t, ClassStaleCandidate.Requeueable())
	require.False(t, ClassConsistent.Requeueable())
	require.False(t, ClassBigIntOutOfContract.Requeueable())
}
