package iso8601

import (
	"context"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/lychee-technology/forma/internal/testdb"
)

const migrationDocPath = "../../docs/schema-consistency-migration.md"

// offsetRepairListingSQL returns the listing query of the migration doc's
// "iso8601 images with a UTC offset" section: the first ```sql fence after
// that heading, anchored on the heading rather than on line numbers.
func offsetRepairListingSQL(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(migrationDocPath)
	require.NoError(t, err)
	doc := string(raw)
	start := strings.Index(doc, "### iso8601 images with a UTC offset")
	require.GreaterOrEqual(t, start, 0, "the migration doc must keep the iso8601 offset section")
	m := regexp.MustCompile("(?s)```sql\\s*\n(.*?)```").FindStringSubmatch(doc[start:])
	require.NotNil(t, m, "the iso8601 offset section must hold a fenced SQL block")
	return strings.ReplaceAll(m[1], "<schema id>", "7")
}

// offsetRepairSweep spells offset images over the days next to either end of
// the four-digit years, at the offsets the read applies (up to ±24:60), with
// and without a fraction in both spellings the read accepts.
func offsetRepairSweep() []string {
	days := []string{"0000-01-01", "0000-01-02", "0000-01-03", "2024-01-02", "9999-12-29", "9999-12-30", "9999-12-31"}
	clocks := []string{"00:00:00", "00:30:00", "23:30:00", "23:59:59"}
	offsets := []string{"+00:00", "-00:00", "+01:00", "-01:00", "+23:59", "-23:59", "+24:60", "-24:60"}
	fractions := []string{"", ".250", ",250", ".000"}
	var images []string
	for _, day := range days {
		for _, clock := range clocks {
			for _, offset := range offsets {
				for _, fraction := range fractions {
					images = append(images, day+"T"+clock+fraction+offset)
				}
			}
		}
	}
	return images
}

// The documented API repair applies to every row the listing leaves
// check_by_hand false, so a false verdict must certify that the image reads
// (Parse) as an instant the write funnel stores unchanged (Image). The
// listing runs from the doc itself on Postgres, so the doc cannot drift from
// what is tested. The named images are the spellings the #761 reviews found
// (a fraction in either spelling, an offset crossing a year boundary from the
// last or the next-to-last day), plus in-range images the repair must still
// cover, so a classifier that flags everything fails too.
func TestOffsetRepairListingFlagsEveryImageTheAPICannotStore(t *testing.T) {
	ctx := context.Background()
	pool := testdb.Connect(t, ctx)
	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()

	// A temp table shadows any entity_main in this session only.
	_, err = conn.Exec(ctx, `CREATE TEMP TABLE entity_main (ltbase_schema_id smallint, ltbase_row_id bigint, text_02 text)`)
	require.NoError(t, err)

	named := map[string]bool{
		"2024-01-02T11:04:06+08:00":     false,
		"2024-01-02T03:04:05-05:00":     false,
		"0000-01-03T00:30:00+24:60":     false,
		"9999-12-29T23:30:00-24:60":     false,
		"2024-01-02T03:04:05.250+08:00": true,
		"2024-01-02T03:04:05,250+08:00": true,
		"9999-12-31T23:30:00-01:00":     true,
		"0000-01-01T00:30:00+01:00":     true,
		"9999-12-30T23:30:00-24:60":     true,
		"0000-01-02T00:30:00+24:60":     true,
		"2024-01-02T3:04:05+08:00":      true,
	}
	images := map[string]bool{}
	for _, image := range offsetRepairSweep() {
		images[image] = true
	}
	for image := range named {
		images[image] = true
	}
	const utcImage = "2024-01-02T03:04:05Z"
	images[utcImage] = true
	rowID := 0
	for image := range images {
		rowID++
		_, err = conn.Exec(ctx, `INSERT INTO entity_main VALUES (7, $1, $2), (8, $1, $2)`, rowID, image)
		require.NoError(t, err)
	}

	rows, err := conn.Query(ctx, offsetRepairListingSQL(t))
	require.NoError(t, err)
	listed := map[string]bool{}
	listings := 0
	for rows.Next() {
		var listedRowID int64
		var image string
		var checkByHand bool
		require.NoError(t, rows.Scan(&listedRowID, &image, &checkByHand))
		listed[image] = checkByHand
		listings++
	}
	require.NoError(t, rows.Err())

	require.NotContains(t, listed, utcImage, "a UTC image never had its offset dropped")
	require.Equal(t, len(images)-1, listings, "every offset image of the schema is listed once")
	require.Len(t, listed, len(images)-1)
	for image, want := range named {
		require.Equal(t, want, listed[image], "check_by_hand of %s", image)
	}
	for image, checkByHand := range listed {
		if checkByHand {
			continue
		}
		parsed, err := Parse(image)
		require.NoError(t, err, "check_by_hand is false for %s, which the read refuses", image)
		ms := parsed.UnixMilli()
		require.True(t, parsed.Equal(time.UnixMilli(ms)), "check_by_hand is false for %s, whose instant has sub-millisecond digits", image)
		_, rule := Image(ms)
		require.Empty(t, rule, "check_by_hand is false for %s, whose instant %s the write funnel refuses", image, parsed.UTC())
	}
}
