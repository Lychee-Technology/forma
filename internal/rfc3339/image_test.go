package rfc3339

import (
	"math/rand"
	"regexp"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The image is the one RFC 3339 rendering the rfc3339 encoding stores and
// the filter literal compares against: UTC, exactly three fractional digits,
// four-digit year (#842). It must not depend on the process zone
// (time.UnixMilli is local), so the test pins a non-UTC zone for its
// duration.
func TestImage(t *testing.T) {
	berlin, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)
	saved := time.Local
	time.Local = berlin
	t.Cleanup(func() { time.Local = saved })

	cases := []struct {
		name string
		ms   int64
		want string
		rule Rule
	}{
		{"epoch", 0, "1970-01-01T00:00:00.000Z", ""},
		{"whole second renders UTC under a non-UTC zone", 1704164645000, "2024-01-02T03:04:05.000Z", ""},
		{"millis keep all three digits", 1704164645250, "2024-01-02T03:04:05.250Z", ""},
		{"one millisecond", 1704164645001, "2024-01-02T03:04:05.001Z", ""},
		{"negative millis before the epoch", -1, "1969-12-31T23:59:59.999Z", ""},
		{"first instant of year 0000", time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), "0000-01-01T00:00:00.000Z", ""},
		{"last millisecond of year 9999", time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli() - 1, "9999-12-31T23:59:59.999Z", ""},
		{"first millisecond past year 9999", time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), "", KeepsFourDigitYear},
		{"last millisecond before year 0000", time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli() - 1, "", KeepsFourDigitYear},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, rule := Image(tc.ms)
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.rule, rule)
			if rule == "" {
				require.Len(t, got, ImageLength)
				parsed, err := Parse(got)
				require.NoError(t, err)
				require.Equal(t, tc.ms, parsed.UnixMilli(), "the read names the instant the image was rendered from")
			}
		})
	}
}

// TestImageTextOrderIsInstantOrder is the premise the PostgreSQL route's text
// comparison, its declared-index seek and the $now operand rest on (#842):
// sorting images as bytes sorts their instants, including images that differ
// only in milliseconds and instants on either side of the epoch.
func TestImageTextOrderIsInstantOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(842))
	millis := []int64{minMillis, maxMillis, -1, 0, 1, 999, 1000, 1001, 1704164645000, 1704164645001, 1704164645250, 1704164645999}
	for i := 0; i < 2000; i++ {
		millis = append(millis, minMillis+rng.Int63n(maxMillis-minMillis+1))
	}
	images := make([]string, len(millis))
	for i, ms := range millis {
		image, rule := Image(ms)
		require.Empty(t, rule)
		images[i] = image
	}
	sort.Slice(millis, func(i, j int) bool { return millis[i] < millis[j] })
	sort.Strings(images)
	for i, ms := range millis {
		want, _ := Image(ms)
		require.Equal(t, want, images[i], "position %d", i)
	}
}

// TestLegacyImagePattern matches exactly the whole-second UTC image the
// iso8601 encoding wrote, the migration's only rewrite target.
func TestLegacyImagePattern(t *testing.T) {
	re := regexp.MustCompile(LegacyImagePattern)
	for image, want := range map[string]bool{
		"2024-01-02T03:04:05Z":      true,
		"0000-01-01T00:00:00Z":      true,
		"2024-01-02T03:04:05.000Z":  false,
		"2024-01-02T03:04:05.5Z":    false,
		"2024-01-02T11:04:05+08:00": false,
		"2024-01-02 03:04:05Z":      false,
		"x2024-01-02T03:04:05Z":     false,
	} {
		require.Equal(t, want, re.MatchString(image), image)
	}
}
