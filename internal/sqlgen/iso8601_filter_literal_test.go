package sqlgen

import (
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

func iso8601FilterMeta(vt forma.ValueType, col forma.MainColumn) forma.AttributeMetadata {
	return forma.AttributeMetadata{ValueType: vt,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: col, Encoding: forma.MainColumnEncodingISO8601}}
}

// #588: a filter literal on an iso8601-bound date/datetime binds the one
// canonical image the column stores (UTC, whole seconds), whatever the
// literal's spelling and whatever the process zone (TestMain pins
// Europe/Berlin). Before this, ConvertPgMainValue kept the literal's offset,
// rendered a unix-ms literal in the server's local zone, and silently
// dropped a sub-second fraction, while the DuckDB route compared the exact
// instant: the two engines answered the same filter differently. This
// replaces TestParseDateValue_ISO8601EncodingFromUnixMs, which pinned the
// local-zone rendering and so only passed on a UTC machine.
func TestISO8601FilterLiteral_BindsCanonicalImageOnEveryBinder(t *testing.T) {
	meta := iso8601FilterMeta(forma.ValueTypeDateTime, forma.MainColumn("text_03"))

	cases := []struct {
		name    string
		literal string
		wantPg  string // PG main-column and EAV bind
		wantMs  int64  // DuckDB bind
	}{
		{"Z literal", "2024-01-02T03:04:05Z", "2024-01-02T03:04:05Z", 1704164645000},
		{"positive offset canonicalised to UTC", "2024-01-02T05:04:05+02:00", "2024-01-02T03:04:05Z", 1704164645000},
		{"negative offset canonicalised to UTC", "2024-01-01T22:04:05-05:00", "2024-01-02T03:04:05Z", 1704164645000},
		{"offset crossing the day and year boundary", "2024-01-01T00:30:00+01:00", "2023-12-31T23:30:00Z", 1704065400000},
		{"unix-ms literal rendered in UTC, not the process zone", "1704164645000", "2024-01-02T03:04:05Z", 1704164645000},
		{"year 0000 lower bound", "0000-01-01T00:00:00Z", "0000-01-01T00:00:00Z", -62167219200000},
		{"year 9999 upper bound", "9999-12-31T23:59:59Z", "9999-12-31T23:59:59Z", 253402300799000},
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
// #582 write-side shape). Without the DuckDB half a sub-second literal would
// be "400 on Postgres, exact compare on DuckDB". The year rule is refused
// too: PG rendered "10000-01-01T00:00:00Z", which sorts before every stored
// image and matched nothing, while DuckDB compared the exact instant.
func TestISO8601FilterLiteral_RefusedUniformly(t *testing.T) {
	cache := forma.SchemaAttributeCache{
		"joined": {AttributeID: 9, ValueType: forma.ValueTypeDateTime,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_03"), Encoding: forma.MainColumnEncodingISO8601}},
		"born": {AttributeID: 8, ValueType: forma.ValueTypeDate,
			ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumn("text_02"), Encoding: forma.MainColumnEncodingISO8601}},
	}
	cases := []struct {
		name, attr, literal, want string
	}{
		{"sub-second Z literal", "joined", "2024-01-02T03:04:05.123Z",
			"datetime filter value 2024-01-02T03:04:05.123Z for 'joined' cannot be compared against main column text_03 with encoding iso8601, which keeps whole seconds"},
		{"sub-second literal with an offset", "joined", "2024-01-02T05:04:05.5+02:00",
			"datetime filter value 2024-01-02T05:04:05.5+02:00 for 'joined' cannot be compared against main column text_03 with encoding iso8601, which keeps whole seconds"},
		{"unix-ms literal off a whole second", "born", "1704164645001",
			"date filter value 1704164645001 for 'born' cannot be compared against main column text_02 with encoding iso8601, which keeps whole seconds"},
		{"unix-ms literal past year 9999", "joined", "253402300800000",
			"datetime filter value 253402300800000 for 'joined' cannot be compared against main column text_03 with encoding iso8601, which keeps years 0000 to 9999 (the RFC3339 four-digit year)"},
		{"unix-ms literal before year 0000", "joined", "-62167219201000",
			"datetime filter value -62167219201000 for 'joined' cannot be compared against main column text_03 with encoding iso8601, which keeps years 0000 to 9999 (the RFC3339 four-digit year)"},
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
func TestISO8601FilterLiteral_RuleDoesNotLeakToExactBindings(t *testing.T) {
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
// an iso8601 binding: the canonical rule runs after the parse, not instead.
func TestISO8601FilterLiteral_NonDateStillRefusedByParser(t *testing.T) {
	meta := iso8601FilterMeta(forma.ValueTypeDateTime, forma.MainColumn("text_03"))
	_, err := ConvertPgMainValue("not-a-date", "joined", meta)
	require.ErrorIs(t, err, forma.ErrInvalidInput)
	require.Contains(t, err.Error(), "expected ISO 8601 format or unix milliseconds, got 'not-a-date'")
}
