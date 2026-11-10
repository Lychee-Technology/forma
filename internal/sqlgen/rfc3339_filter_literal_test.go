package sqlgen

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

func rfc3339FilterMeta(vt forma.ValueType, col forma.MainColumn) forma.AttributeMetadata {
	return forma.AttributeMetadata{ValueType: vt,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: col, Encoding: forma.MainColumnEncodingRFC3339}}
}

// #588: a filter literal on an rfc3339-bound date/datetime binds the one
// canonical image the column stores (UTC, three fractional digits since
// #842), whatever the literal's spelling and whatever the process zone
// (TestMain pins Europe/Berlin). Before #588, ConvertPgMainValue kept the
// literal's offset, rendered a unix-ms literal in the server's local zone,
// and silently dropped a sub-second fraction, while the DuckDB route
// compared the exact instant: the two engines answered the same filter
// differently. Since #842 a literal off a whole second is no longer refused:
// it binds its own millisecond image.
func TestRFC3339FilterLiteral_BindsCanonicalImageOnEveryBinder(t *testing.T) {
	meta := rfc3339FilterMeta(forma.ValueTypeDateTime, forma.MainColumn("text_03"))

	cases := []struct {
		name    string
		literal string
		wantPg  string // PG main-column and EAV bind
		wantMs  int64  // DuckDB bind
	}{
		{"Z literal", "2024-01-02T03:04:05Z", "2024-01-02T03:04:05.000Z", 1704164645000},
		{"sub-second Z literal", "2024-01-02T03:04:05.123Z", "2024-01-02T03:04:05.123Z", 1704164645123},
		{"short fraction padded to three digits", "2024-01-02T03:04:05.5Z", "2024-01-02T03:04:05.500Z", 1704164645500},
		{"sub-second literal with an offset", "2024-01-02T05:04:05.5+02:00", "2024-01-02T03:04:05.500Z", 1704164645500},
		{"sub-millisecond fraction floored", "2024-01-02T03:04:05.9999999Z", "2024-01-02T03:04:05.999Z", 1704164645999},
		{"positive offset canonicalised to UTC", "2024-01-02T05:04:05+02:00", "2024-01-02T03:04:05.000Z", 1704164645000},
		{"negative offset canonicalised to UTC", "2024-01-01T22:04:05-05:00", "2024-01-02T03:04:05.000Z", 1704164645000},
		{"offset crossing the day and year boundary", "2024-01-01T00:30:00+01:00", "2023-12-31T23:30:00.000Z", 1704065400000},
		{"unix-ms literal rendered in UTC, not the process zone", "1704164645000", "2024-01-02T03:04:05.000Z", 1704164645000},
		{"unix-ms literal off a whole second", "1704164645001", "2024-01-02T03:04:05.001Z", 1704164645001},
		{"year 0000 lower bound", "0000-01-01T00:00:00Z", "0000-01-01T00:00:00.000Z", -62167219200000},
		{"year 9999 upper bound", "9999-12-31T23:59:59.999Z", "9999-12-31T23:59:59.999Z", 253402300799999},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pgMain, err := ConvertPgMainValue(tc.literal, "joined", meta)
			require.NoError(t, err)
			require.Equal(t, tc.wantPg, pgMain, "ConvertPgMainValue")

			_, pgEav, err := parsePgEavValue("joined", meta, tc.literal)
			require.NoError(t, err)
			require.Equal(t, tc.wantPg, pgEav, "parsePgEavValue")

			duck, err := parseDuckDBRawParam(tc.literal, "joined", forma.ValueTypeDateTime)
			require.NoError(t, err)
			require.Equal(t, tc.wantMs, duck, "parseDuckDBRawParam")
		})
	}
}

// A literal the stored image cannot express is refused as invalid input on
// the Postgres binders and on the DuckDB leaf alike, with one published
// message naming the attribute, the literal, the column and the rule (the
// #582 write-side shape). Since #842 the one such literal is an instant
// outside the four-digit year: PG would render "10000-01-01T00:00:00.000Z",
// which sorts before every stored image and matches nothing, while DuckDB
// compared the exact instant.
func TestRFC3339FilterLiteral_RefusedUniformly(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"joined": {AttributeID: 9, ValueType: forma.ValueTypeDateTime,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_03"), Encoding: forma.MainColumnEncodingRFC3339}},
		"born": {AttributeID: 8, ValueType: forma.ValueTypeDate,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_02"), Encoding: forma.MainColumnEncodingRFC3339}},
	}
	cases := []struct {
		name, attr, literal, want string
	}{
		{"unix-ms literal past year 9999", "joined", "253402300800000",
			"datetime filter value 253402300800000 for 'joined' cannot be compared against main column text_03 with encoding rfc3339, which keeps years 0000 to 9999 (the RFC 3339 four-digit year)"},
		{"unix-ms literal before year 0000", "joined", "-62167219200001",
			"datetime filter value -62167219200001 for 'joined' cannot be compared against main column text_03 with encoding rfc3339, which keeps years 0000 to 9999 (the RFC 3339 four-digit year)"},
		{"date unix-ms literal past year 9999", "born", "253402300800000",
			"date filter value 253402300800000 for 'born' cannot be compared against main column text_02 with encoding rfc3339, which keeps years 0000 to 9999 (the RFC 3339 four-digit year)"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			meta := cache[tc.attr]
			cond := charKv(tc.attr, "gte:"+tc.literal)

			_, pgMainErr := ConvertPgMainValue(tc.literal, tc.attr, meta)
			_, _, pgEavErr := parsePgEavValue(tc.attr, meta, tc.literal)
			idx := 0
			_, _, pgMainClauseErr := BuildPgMainClause(cond, cache, &idx)
			_, _, duckErr := BuildDuckClause(cond, cache)
			_, dualErr := ToDualClauses(cond, "eav_table", 7, cache, &idx)

			for name, err := range map[string]error{
				"ConvertPgMainValue": pgMainErr, "parsePgEavValue": pgEavErr,
				"BuildPgMainClause": pgMainClauseErr, "BuildDuckClause": duckErr, "ToDualClauses": dualErr,
			} {
				require.ErrorIs(t, err, forma.ErrInvalidInput, name)
				msg, ok := forma.ResolvePublicMessage(err)
				require.True(t, ok, "%s must publish its message", name)
				require.Equal(t, tc.want, msg, name)
			}
		})
	}
}

// The unix_ms and unbound bindings are exact int64 epoch millis: a
// sub-second or out-of-year literal is a legitimate operand there and must
// not be refused.
func TestRFC3339FilterLiteral_RuleDoesNotLeakToExactBindings(t *testing.T) {
	unixMs := forma.AttributeMetadata{ValueType: forma.ValueTypeDate,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("bigint_01"), Encoding: forma.MainColumnEncodingUnixMs}}
	unbound := forma.AttributeMetadata{ValueType: forma.ValueTypeDateTime}
	for name, meta := range map[string]forma.AttributeMetadata{"unix_ms": unixMs, "unbound": unbound} {
		t.Run(name, func(t *testing.T) {
			got, err := ConvertPgMainValue("2024-01-02T03:04:05.123Z", "d", meta)
			require.NoError(t, err)
			require.Equal(t, int64(1704164645123), got)
			_, got, err = parsePgEavValue("d", meta, "253402300800000")
			require.NoError(t, err)
			require.Equal(t, int64(253402300800000), got)
		})
	}
}

// A literal that is not a date at all keeps the shared parser's refusal on
// an rfc3339 binding: the canonical rule runs after the parse, not instead.
func TestRFC3339FilterLiteral_NonDateStillRefusedByParser(t *testing.T) {
	meta := rfc3339FilterMeta(forma.ValueTypeDateTime, forma.MainColumn("text_03"))
	_, err := ConvertPgMainValue("not-a-date", "joined", meta)
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	require.Contains(t, err.Error(), "expected ISO 8601 format or unix milliseconds, got 'not-a-date'")
}
