package federated

import "testing"

// TestTombstoneStamp pins the harness's copy of the production tombstone
// stamp rule (#508): strictly after the deleted version, never before the
// clock.
func TestTombstoneStamp(t *testing.T) {
	cases := []struct {
		name            string
		now, prev, want int64
	}{
		{name: "clock ahead of the live version", now: 1_000, prev: 500, want: 1_000},
		{name: "live version ahead of the clock", now: 1_000, prev: 5_000, want: 5_001},
		{name: "live version equal to the clock", now: 1_000, prev: 1_000, want: 1_001},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tombstoneStamp(tc.now, tc.prev); got != tc.want {
				t.Fatalf("tombstoneStamp(%d, %d) = %d, want %d", tc.now, tc.prev, got, tc.want)
			}
		})
	}
}
