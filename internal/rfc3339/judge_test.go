package rfc3339

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestJudge classifies every kind of stored image the census and the
// migration listing meet (#842): the canonical image passes; the
// whole-second iso8601 image, an offset and a short or long fraction of a
// whole millisecond are respellings; a sub-millisecond fraction floors; an
// offset past the year range has no canonical image; and an image the read
// refuses, including one of the canonical shape naming no day, is
// unreadable.
func TestJudge(t *testing.T) {
	cases := []struct {
		image, canonical string
		defect           Defect
	}{
		{"2024-01-02T03:04:05.000Z", "2024-01-02T03:04:05.000Z", ""},
		{"2024-01-02T03:04:05.250Z", "2024-01-02T03:04:05.250Z", ""},
		{"0000-01-01T00:00:00.000Z", "0000-01-01T00:00:00.000Z", ""},
		{"9999-12-31T23:59:59.999Z", "9999-12-31T23:59:59.999Z", ""},
		{"1969-12-31T23:59:59.999Z", "1969-12-31T23:59:59.999Z", ""},
		{"2024-01-02T03:04:05Z", "2024-01-02T03:04:05.000Z", Respelled},
		{"2024-01-02T11:04:05.250+08:00", "2024-01-02T03:04:05.250Z", Respelled},
		{"2024-01-02T03:04:05.000+00:00", "2024-01-02T03:04:05.000Z", Respelled},
		{"2024-01-02T03:04:05.5Z", "2024-01-02T03:04:05.500Z", Respelled},
		{"2024-01-02T03:04:05.25Z", "2024-01-02T03:04:05.250Z", Respelled},
		{"2024-01-02T03:04:05.250000Z", "2024-01-02T03:04:05.250Z", Respelled},
		{"2024-01-02T03:04:05.2509Z", "2024-01-02T03:04:05.250Z", SubMillisecond},
		{"2024-01-02T03:04:06.0000009Z", "2024-01-02T03:04:06.000Z", SubMillisecond},
		{"1969-12-31T23:59:59.9999Z", "1969-12-31T23:59:59.999Z", SubMillisecond},
		{"9999-12-31T23:30:00-01:00", "", NoCanonicalImage},
		{"0000-01-01T00:30:00.000+01:00", "", NoCanonicalImage},
		{"2024-02-30T03:04:05.250Z", "", Unreadable},
		{"2024-01-02T25:04:05.000Z", "", Unreadable},
		{"2023-02-29T00:00:00Z", "", Unreadable},
		{"2024-01-02 03:04:05Z", "", Unreadable},
		{"not a time", "", Unreadable},
		{"", "", Unreadable},
	}
	for _, tc := range cases {
		t.Run(tc.image, func(t *testing.T) {
			canonical, defect := Judge(tc.image)
			require.Equal(t, tc.defect, defect)
			require.Equal(t, tc.canonical, canonical)
			require.NotEmpty(t, defect.Explain(canonical))
		})
	}
}

// TestJudgeAgreesWithImage: every image Image renders is judged canonical,
// so the census never reports a row the write funnel stored.
func TestJudgeAgreesWithImage(t *testing.T) {
	for _, ms := range []int64{minMillis, maxMillis, -1001, -1, 0, 1, 999, 1704164645000, 1704164645250} {
		image, rule := Image(ms)
		require.Empty(t, rule)
		canonical, defect := Judge(image)
		require.Empty(t, defect, image)
		require.Equal(t, image, canonical)
	}
}
