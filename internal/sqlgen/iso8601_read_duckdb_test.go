package sqlgen

import (
	"database/sql"
	"testing"
	"time"

	"github.com/lychee-technology/forma/internal/iso8601"
	"github.com/stretchr/testify/require"
)

// iso8601ReadZones are session TimeZones the read must not depend on. The
// federated reader and the CDC exporter set none, so DuckDB's bundled ICU
// takes the host's zone; Pacific/Chatham has a quarter-hour offset.
var iso8601ReadZones = []string{"UTC", "Asia/Tokyo", "America/New_York", "Pacific/Chatham"}

// openDuckDBInZone opens one in-memory DuckDB connection whose session
// TimeZone is zone.
func openDuckDBInZone(t *testing.T, zone string) *sql.DB {
	t.Helper()
	db, err := sql.Open("duckdb", strictLambdaDSN)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	db.SetMaxOpenConns(1)
	_, err = db.Exec("SET TimeZone = '" + zone + "'")
	require.NoError(t, err)
	return db
}

func readISO8601(t *testing.T, db *sql.DB, image sql.NullString) sql.NullInt64 {
	t.Helper()
	var got sql.NullInt64
	require.NoError(t, db.QueryRow("SELECT "+ISO8601EpochMs("v")+" FROM (SELECT CAST(? AS VARCHAR) AS v)", image).Scan(&got))
	return got
}

// TestISO8601EpochMsReadsTheOLTPInstantInEveryZone is the #756 acceptance on
// DuckDB: every image the OLTP read parses (iso8601.Parse) reads as the
// instant that read names, offset applied, whatever the session TimeZone.
// The TIMESTAMP cast read 2024-01-02T11:04:06+08:00 as 11:04:06Z.
func TestISO8601EpochMsReadsTheOLTPInstantInEveryZone(t *testing.T) {
	images := []string{
		"2024-01-02T03:04:05Z",
		"2024-01-02T11:04:06+08:00",
		"2024-01-02T03:04:05-05:30",
		"2024-01-02T03:04:05.250+08:00",
		"2024-01-02T03:04:05.5Z",
		"2024-01-02T03:04:05+14:00",
		"2024-01-02T03:04:05-12:00",
		"2024-01-02T03:04:05+00:00",
		"1970-01-01T00:00:00+01:00",
		"9999-12-31T23:59:59Z",
	}
	for _, zone := range iso8601ReadZones {
		db := openDuckDBInZone(t, zone)
		for _, image := range images {
			parsed, err := iso8601.Parse(image)
			require.NoError(t, err, "fixture %q must be an image the OLTP read parses", image)
			got := readISO8601(t, db, sql.NullString{String: image, Valid: true})
			require.Equal(t, sql.NullInt64{Int64: parsed.UnixMilli(), Valid: true}, got, "zone %s, image %q", zone, image)
		}
	}
}

// TestISO8601EpochMsKeepsOffsetLessStringsInUTC: a string with no zone,
// which the OLTP read refuses, still reads as UTC wall clock on every
// session TimeZone, as the TIMESTAMP cast read it, and a string that is not
// a timestamp reads NULL (#555). The control proves each zone is live: the
// TIMESTAMPTZ cast alone reads the same string in that zone.
func TestISO8601EpochMsKeepsOffsetLessStringsInUTC(t *testing.T) {
	for _, zone := range iso8601ReadZones {
		db := openDuckDBInZone(t, zone)
		for _, tc := range []struct {
			image sql.NullString
			want  sql.NullInt64
		}{
			{sql.NullString{String: "2024-01-02 03:04:06", Valid: true}, sql.NullInt64{Int64: 1704164646000, Valid: true}},
			{sql.NullString{String: "2024-01-02T03:04:06", Valid: true}, sql.NullInt64{Int64: 1704164646000, Valid: true}},
			{sql.NullString{String: "2024-01-02", Valid: true}, sql.NullInt64{Int64: 1704153600000, Valid: true}},
			{sql.NullString{String: "not a date", Valid: true}, sql.NullInt64{}},
			{sql.NullString{String: "", Valid: true}, sql.NullInt64{}},
			{sql.NullString{String: "2024-13-45T00:00:00+08:00", Valid: true}, sql.NullInt64{}},
			{sql.NullString{}, sql.NullInt64{}},
		} {
			require.Equal(t, tc.want, readISO8601(t, db, tc.image), "zone %s, image %q", zone, tc.image.String)
		}
		if zone == "UTC" {
			continue
		}
		var zoned int64
		require.NoError(t, db.QueryRow("SELECT epoch_ms(CAST('2024-01-02 03:04:06' AS TIMESTAMPTZ))").Scan(&zoned))
		require.NotEqual(t, int64(1704164646000), zoned, "zone %s is not applied to the session", zone)
	}
}

// TestISO8601ReadRoundTripsThroughTheOuterSelect: every image the OLTP read
// parses, the read and the outer select's image (iso8601ImageFromEpochMs)
// hand the reader a string iso8601.Parse decodes to the same instant, at
// whole seconds. An offset can carry a four-digit-year image past year 9999
// or before year 0000 in UTC; strftime spelled that with a five- or
// one-digit year, which iso8601.Parse refuses, failing the whole page while
// a Get of the row read it. ±24:60 is the widest offset iso8601.Parse
// applies, so the last two images name its extreme instants.
func TestISO8601ReadRoundTripsThroughTheOuterSelect(t *testing.T) {
	images := []string{
		"2024-01-02T11:04:06+08:00",
		"2024-01-02T03:04:05.250+08:00",
		"9999-12-31T23:59:59Z",
		"0000-01-01T00:00:00Z",
		"9999-12-31T23:30:00-01:00",
		"0000-01-01T00:30:00+01:00",
		"9999-12-31T23:59:59-23:59",
		"0000-01-01T00:00:00+23:59",
		"9999-12-31T23:59:59.999-24:60",
		"0000-01-01T00:00:00+24:60",
	}
	for _, zone := range iso8601ReadZones {
		db := openDuckDBInZone(t, zone)
		for _, image := range images {
			want, err := iso8601.Parse(image)
			require.NoError(t, err, "fixture %q must be an image the OLTP read parses", image)
			var out string
			require.NoError(t, db.QueryRow("SELECT "+iso8601ImageFromEpochMs(ISO8601EpochMs("v"))+
				" FROM (SELECT CAST(? AS VARCHAR) AS v)", image).Scan(&out))
			got, err := iso8601.Parse(out)
			require.NoError(t, err, "zone %s: image %q reaches the reader as %q", zone, image, out)
			require.Equal(t, want.Truncate(time.Second).UnixMilli(), got.UnixMilli(),
				"zone %s: image %q reaches the reader as %q", zone, image, out)
		}
	}
}
