package production

import (
	"slices"
	"strings"
	"testing"
)

func TestParseBaselineSizes(t *testing.T) {
	ok := map[string][]int64{
		"1500":                       {1500},
		"2000, 2500":                 {2000, 2500},
		"10000,100000,1000000":       {10000, 100000, 1000000},
		"1500,2000,2500,3000,999999": {1500, 2000, 2500, 3000, 999999},
	}
	for raw, want := range ok {
		got, err := parseBaselineSizes(raw)
		if err != nil || !slices.Equal(got, want) {
			t.Errorf("parseBaselineSizes(%q) = %v, %v; want %v", raw, got, err, want)
		}
	}
	bad := map[string]string{
		"1499":       "at least 1500",
		"x":          "at least 1500",
		"2000,2000":  "at least 500", // duplicate: the hot set alone overshoots
		"2000,2100":  "at least 500", // under the hot set's creates
		"2000,2499":  "at least 500",
		"4000,2000":  "at least 500",  // descending
		"2000,,3000": "at least 1500", // empty field
	}
	for raw, want := range bad {
		if got, err := parseBaselineSizes(raw); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("parseBaselineSizes(%q) = %v, %v; want an error naming %q", raw, got, err, want)
		}
	}
}
