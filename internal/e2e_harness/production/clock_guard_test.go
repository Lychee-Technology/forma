//go:build e2e

package production

import (
	"testing"
	"time"
)

// TestWaitClockPastMillis pins the mechanism every clock-ordering guard in this
// package leans on (#276): the helper must not return until the process clock
// is STRICTLY past its anchor, and must not spin when the anchor is already
// behind. It touches no cluster — SharedCluster starts lazily — so it runs in
// milliseconds without docker.
func TestWaitClockPastMillis(t *testing.T) {
	t.Run("waits past a future anchor", func(t *testing.T) {
		anchor := time.Now().UnixMilli() + 5
		waitClockPastMillis(anchor)
		if got := time.Now().UnixMilli(); got <= anchor {
			t.Fatalf("returned at %d, want strictly past anchor %d", got, anchor)
		}
	})

	t.Run("returns immediately for a past anchor", func(t *testing.T) {
		start := time.Now()
		waitClockPastMillis(time.Now().UnixMilli() - 10)
		if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
			t.Fatalf("returned after %s, want an immediate return for a past anchor", elapsed)
		}
	})
}
