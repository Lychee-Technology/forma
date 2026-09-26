package iso8601

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The image is the one RFC3339 rendering the iso8601 encoding stores and
// the filter literal compares against: UTC, whole seconds, four-digit year.
// It must not depend on the process zone (time.UnixMilli is local), so the
// test pins a non-UTC zone for its duration.
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
		{"epoch", 0, "1970-01-01T00:00:00Z", ""},
		{"whole second renders UTC under a non-UTC zone", 1704164645000, "2024-01-02T03:04:05Z", ""},
		{"first instant of year 0000", time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), "0000-01-01T00:00:00Z", ""},
		{"last second of year 9999", time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC).UnixMilli(), "9999-12-31T23:59:59Z", ""},
		{"millis off a whole second", 1704164645123, "", KeepsWholeSeconds},
		{"negative millis off a whole second", -1, "", KeepsWholeSeconds},
		{"one second past year 9999", time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), "", KeepsFourDigitYear},
		{"one second before year 0000", time.Date(-1, 12, 31, 23, 59, 59, 0, time.UTC).UnixMilli(), "", KeepsFourDigitYear},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, rule := Image(tc.ms)
			require.Equal(t, tc.want, got)
			require.Equal(t, tc.rule, rule)
		})
	}
}
