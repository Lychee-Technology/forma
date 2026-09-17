package sqlgen

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/stretchr/testify/require"
)

// TestBoolTruthiness pins the one read-side spelling every SQL reader derives
// a bool from (#404): the stored numeric image is the nearest of 0/1, so
// float noise around either end does not flip the answer. This is the SQL
// image of transform.float64ToBool's threshold; the two must stay in
// lockstep.
func TestBoolTruthiness(t *testing.T) {
	require.Equal(t, "(value_numeric > 0.5)", BoolTruthiness("value_numeric"))
	require.Equal(t, "(x.value_numeric > 0.5)", BoolTruthiness("x.value_numeric"))
}

// TestPgEavLeafPayload_ComparisonLHSUsesBoolTruthiness: the PG-EAV leg
// compares the same truthiness the DuckDB tiers derive, in the same spelling.
func TestPgEavLeafPayload_ComparisonLHSUsesBoolTruthiness(t *testing.T) {
	p := PgEavLeafPayload{ValueColumn: "value_numeric", Truthy: true}
	require.Equal(t, BoolTruthiness("x.value_numeric"), p.ComparisonLHS("x"))
}

// TestConvertPgMainValue_BoolAcceptsEveryOperandSpelling: the pg-main
// main-table route parses a bool operand under the same rule as the EAV and
// DuckDB routes (parseBoolOperand, #384 P2b). Before #404 it ran strconv.Atoi
// alone, so `equals:true` was a 400 on this route and a match on the others.
func TestConvertPgMainValue_BoolAcceptsEveryOperandSpelling(t *testing.T) {
	boolInt := forma.AttributeMetadata{
		ValueType:     forma.ValueTypeBool,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnSmallint01, Encoding: forma.MainColumnEncodingBoolInt},
	}
	boolText := forma.AttributeMetadata{
		ValueType:     forma.ValueTypeBool,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText02, Encoding: forma.MainColumnEncodingBoolText},
	}
	for _, tc := range []struct {
		operand  string
		wantInt  any
		wantText any
	}{
		{operand: "true", wantInt: int64(1), wantText: "1"},
		{operand: "TRUE", wantInt: int64(1), wantText: "1"},
		{operand: "t", wantInt: int64(1), wantText: "1"},
		{operand: "false", wantInt: int64(0), wantText: "0"},
		{operand: "1", wantInt: int64(1), wantText: "1"},
		{operand: "0", wantInt: int64(0), wantText: "0"},
		{operand: "2", wantInt: int64(1), wantText: "1"},
	} {
		t.Run(tc.operand, func(t *testing.T) {
			got, err := ConvertPgMainValue(tc.operand, "flag", boolInt)
			require.NoError(t, err)
			require.Equal(t, tc.wantInt, got)
			got, err = ConvertPgMainValue(tc.operand, "flag", boolText)
			require.NoError(t, err)
			require.Equal(t, tc.wantText, got)
		})
	}
	_, err := ConvertPgMainValue("banana", "flag", boolInt)
	require.ErrorIs(t, err, forma.ErrInvalidInput)
}

// TestToDualClauses_BoundBoolOperandSpellingParity is the #503 acceptance
// at the emitter level: one operand spelling on a column-bound bool must
// reach the same verdict on every route the dual-path generator serves —
// the pg-main pushdown bind (the BETWEEN range for bool_smallint, the bool
// bind against `= '1'` for bool_text; #565), the PG-EAV truthy bind and the
// DuckDB CAST(? AS BOOLEAN) bind — or be rejected as
// invalid input on all of them. Before #564 the pg-main bind ran
// strconv.Atoi alone, so `equals:true` was a 400 on the PreferHot route and
// a match through DuckDB; the converter-level pin above cannot see that
// split, this one can.
func TestToDualClauses_BoundBoolOperandSpellingParity(t *testing.T) {
	cache := characterizationCache()
	type encoding struct {
		attr   string
		clause string
		bind   func(truthy bool) []any
		eav    string // the PG-EAV clause after the pg-main ticks
	}
	encodings := []encoding{
		{"active", "m.bool_01 BETWEEN ? AND ?", func(v bool) []any {
			if v {
				return []any{int64(1), int64(32767)}
			}
			return []any{int64(-32768), int64(0)}
		}, charEXISTS + "$3 AND (x.value_numeric > 0.5) = $4)"},
		{"verified", "(m.text_02 = '1') = ?", func(v bool) []any {
			return []any{v}
		}, charEXISTS + "$2 AND (x.value_numeric > 0.5) = $3)"},
	}
	spellings := []struct {
		operand string
		truthy  bool
	}{
		{"true", true},
		{"2", true},
		{"0", false},
	}
	for _, enc := range encodings {
		attrID := int16(cache[enc.attr].AttributeID)
		for _, sp := range spellings {
			t.Run(enc.attr+"/equals:"+sp.operand, func(t *testing.T) {
				paramIndex := 0
				dc, err := ToDualClauses(charKv(enc.attr, "equals:"+sp.operand), "eav_table", 7, cache, &paramIndex)
				require.NoError(t, err)
				require.Equal(t, DualClauses{
					PgMainClause: enc.clause, PgMainArgs: enc.bind(sp.truthy),
					PgClause: enc.eav, PgArgs: []any{attrID, sp.truthy},
					DuckClause: enc.attr + " = CAST(? AS BOOLEAN)", DuckArgs: []any{sp.truthy},
				}, dc)
			})
		}
		t.Run(enc.attr+"/equals:banana rejected on every route", func(t *testing.T) {
			paramIndex := 0
			_, err := ToDualClauses(charKv(enc.attr, "equals:banana"), "eav_table", 7, cache, &paramIndex)
			require.ErrorIs(t, err, forma.ErrInvalidInput)
			require.Contains(t, err.Error(), "invalid boolean value for '"+enc.attr+"': banana")
		})
	}
}

// TestBuildPGSelectNoEAV_BoolColumnsUseMainColBoolExpr: the no-EAV hot
// projection (live for any production schema whose attributes are all
// column-bound) must derive a bound bool through the same mainColBoolExpr the
// EAV-joined projection uses. It used to project the raw column and leave the
// outer CAST(attr AS BOOLEAN) to read it — SMALLINT -1 → true and VARCHAR
// 'true' → true on that one route, while `> 0.5` / `= '1'` read them as
// false everywhere else, including the CDC export the same row lands in at
// flush (#404, PR #564 review, finding 2).
func TestBuildPGSelectNoEAV_BoolColumnsUseMainColBoolExpr(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"name": {AttributeID: 1, ValueType: forma.ValueTypeText,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText01}},
		"flagInt": {AttributeID: 2, ValueType: forma.ValueTypeBool,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnSmallint01, Encoding: forma.MainColumnEncodingBoolInt}},
		"flagText": {AttributeID: 3, ValueType: forma.ValueTypeBool,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText02, Encoding: forma.MainColumnEncodingBoolText}},
	}
	sp, err := BuildSchemaProjection(7, cache)
	require.NoError(t, err)
	require.False(t, sp.HasEAVAttrs, "fixture must qualify for the no-EAV shortcut")

	noEAV := sp.BuildPGSelectNoEAV()
	require.Contains(t, noEAV, "(m.smallint_01 > 0.5) AS flagInt")
	require.Contains(t, noEAV, "m.text_02 = '1' AS flagText")
	require.Contains(t, noEAV, "m.text_01 AS name", "non-bool columns still project raw")
	require.NotContains(t, noEAV, "m.smallint_01 AS", "the raw bool column must not reach the outer CAST")
	require.NotContains(t, noEAV, "m.text_02 AS")

	// Same spelling as the EAV-joined projection's COALESCE fallback.
	require.Contains(t, sp.PGSourceSelect, "(m.smallint_01 > 0.5)) AS flagInt")
	require.Contains(t, sp.PGSourceSelect, "m.text_02 = '1') AS flagText")
}

// TestBuildOuterSelect_BoundBoolCastsToPhysicalColumn: the outer select
// aliases a bound bool to its entity_main column, and the federated reader
// scans that column by kind (SMALLINT → sql.NullInt64, text →
// sql.NullString). CAST(attr AS BOOLEAN) fed a Go bool into the SMALLINT
// slot — a scan error that made every bound bool_smallint attribute
// unreadable through DuckDB — and "true"/"false" into the text slot the
// bool_text funnel reads by its "1"/"0" contract. The verdict is cast back
// to the stored image instead. Executed against DuckDB with the reader's
// own scan types so the assertion is on the scan, not the SQL text.
func TestBuildOuterSelect_BoundBoolCastsToPhysicalColumn(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"flagInt": {AttributeID: 2, ValueType: forma.ValueTypeBool,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnSmallint01, Encoding: forma.MainColumnEncodingBoolInt}},
		"flagText": {AttributeID: 3, ValueType: forma.ValueTypeBool,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText02, Encoding: forma.MainColumnEncodingBoolText}},
	}
	sp, err := BuildSchemaProjection(7, cache)
	require.NoError(t, err)
	require.Contains(t, sp.OuterSelect, "CAST(flagInt AS SMALLINT) AS smallint_01")
	require.Contains(t, sp.OuterSelect, "CAST(CAST(flagText AS TINYINT) AS VARCHAR) AS text_02")
	require.NotContains(t, sp.OuterSelect, "AS BOOLEAN")

	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	for _, tc := range []struct {
		verdict  string // the BOOLEAN every leg hands the outer select
		wantInt  sql.NullInt64
		wantText sql.NullString
	}{
		{"TRUE", sql.NullInt64{Int64: 1, Valid: true}, sql.NullString{String: "1", Valid: true}},
		{"FALSE", sql.NullInt64{Int64: 0, Valid: true}, sql.NullString{String: "0", Valid: true}},
		{"NULL::BOOLEAN", sql.NullInt64{}, sql.NullString{}},
	} {
		t.Run(tc.verdict, func(t *testing.T) {
			var gotInt sql.NullInt64
			var gotText sql.NullString
			query := "SELECT " + duckDBMainColCast("flagInt", forma.ValueTypeBool, model.ColumnKindSmallint) +
				", " + duckDBMainColCast("flagText", forma.ValueTypeBool, model.ColumnKindText) +
				" FROM (SELECT " + tc.verdict + " AS flagInt, " + tc.verdict + " AS flagText) t"
			require.NoError(t, db.QueryRow(query).Scan(&gotInt, &gotText),
				"outer select must scan with the reader's types")
			require.Equal(t, tc.wantInt, gotInt)
			require.Equal(t, tc.wantText, gotText)
		})
	}
}

// TestBoolMainPredicate_MatchesReadContract executes the pushdown predicate
// and the read contract (mainColBoolExpr: `> 0.5` for bool_smallint, `= '1'`
// for bool_text) side by side on DuckDB over the images the write funnel
// does not produce (#565): the pushdown must reach the contract's verdict
// on every image and stay NULL on NULL.
func TestBoolMainPredicate_MatchesReadContract(t *testing.T) {
	db, err := sql.Open("duckdb", ":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	for _, enc := range []struct {
		encoding forma.MainColumnEncoding
		cast     string
		images   []string
	}{
		{forma.MainColumnEncodingBoolInt, "SMALLINT", []string{"-32768", "-1", "0", "1", "2", "32767", "NULL"}},
		{forma.MainColumnEncodingBoolText, "VARCHAR", []string{"'1'", "'0'", "'true'", "'2'", "''", "NULL"}},
	} {
		for _, truthy := range []bool{true, false} {
			p := BoolMainPredicate{Encoding: enc.encoding, Truthy: truthy}
			positional := func() string { return "?" }
			for _, image := range enc.images {
				t.Run(fmt.Sprintf("%s/truthy=%t/image=%s", enc.encoding, truthy, image), func(t *testing.T) {
					query := "SELECT " + p.Render("m.v", positional) + ", (" + mainColBoolExpr("v", enc.encoding) + ") = ? " +
						"FROM (SELECT CAST(" + image + " AS " + enc.cast + ") AS v) m"
					args := append(p.Args(), truthy)
					var gotPushdown, gotContract sql.NullBool
					require.NoError(t, db.QueryRow(query, args...).Scan(&gotPushdown, &gotContract))
					require.Equal(t, gotContract, gotPushdown, "pushdown verdict must equal the read contract's verdict")
				})
			}
		}
	}
}

// TestPgMainAndHybridMain_BoolPredicateSpelling pins the entity_main
// pushdown predicate for both bool encodings and both equality operators
// (#565), the pg-main twin of TestPgEavLeafPayload_ComparisonLHSUsesBoolTruthiness.
// The clause text is operand-independent — the plan cache reuses it across
// operands, so `equals:true` and `equals:false` must differ only in their
// binds: bool_smallint renders the BETWEEN range whose bounds equal the
// `> 0.5` verdict on a SMALLINT, bool_text renders the `= '1'` contract
// every other leg reads by against a bool bind. Every case consumes one
// counter tick per placeholder.
func TestPgMainAndHybridMain_BoolPredicateSpelling(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"flagInt": {AttributeID: 2, ValueType: forma.ValueTypeBool,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnSmallint01, Encoding: forma.MainColumnEncodingBoolInt}},
		"flagText": {AttributeID: 3, ValueType: forma.ValueTypeBool,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText02, Encoding: forma.MainColumnEncodingBoolText}},
	}
	truthy := []any{int64(1), int64(32767)}
	falsy := []any{int64(-32768), int64(0)}
	for _, tc := range []struct {
		attr, value string
		wantMain    string
		wantArgs    []any
	}{
		{"flagInt", "equals:true", "m.smallint_01 BETWEEN ? AND ?", truthy},
		{"flagInt", "equals:false", "m.smallint_01 BETWEEN ? AND ?", falsy},
		{"flagInt", "not_equals:true", "m.smallint_01 BETWEEN ? AND ?", falsy},
		{"flagInt", "not_equals:false", "m.smallint_01 BETWEEN ? AND ?", truthy},
		{"flagInt", "equals:2", "m.smallint_01 BETWEEN ? AND ?", truthy},
		{"flagText", "equals:true", "(m.text_02 = '1') = ?", []any{true}},
		{"flagText", "equals:false", "(m.text_02 = '1') = ?", []any{false}},
		{"flagText", "not_equals:true", "(m.text_02 = '1') = ?", []any{false}},
		{"flagText", "not_equals:false", "(m.text_02 = '1') = ?", []any{true}},
		{"flagText", "equals:2", "(m.text_02 = '1') = ?", []any{true}},
	} {
		t.Run(tc.attr+"/"+tc.value, func(t *testing.T) {
			paramIndex := 0
			sqlText, args, err := buildPgMainClause(&forma.KvCondition{Attr: tc.attr, Value: tc.value}, cache, &paramIndex)
			require.NoError(t, err)
			require.Equal(t, tc.wantMain, sqlText)
			require.Equal(t, tc.wantArgs, args)
			require.Equal(t, len(tc.wantArgs), paramIndex, "one counter tick per placeholder")

			leaf := normalizeLeaf(&forma.KvCondition{Attr: tc.attr, Value: tc.value}, cache, targetHybrid)
			require.NoError(t, leaf.Hybrid.Err)
			require.True(t, leaf.Hybrid.IsMain)
			require.NotNil(t, leaf.Hybrid.MainBool)
			require.Equal(t, tc.wantArgs, leaf.Hybrid.MainBool.Args())
		})
	}
	_, _, err := buildPgMainClause(&forma.KvCondition{Attr: "flagInt", Value: "equals:banana"}, cache, new(int))
	require.ErrorIs(t, err, forma.ErrInvalidInput)
}
