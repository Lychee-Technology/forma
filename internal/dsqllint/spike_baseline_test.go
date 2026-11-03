//go:build dsqlspike

// EXPERIMENTAL (DSQL typed call-site spike): baseline timing of the PR #801
// SQL-text scanner. Not part of the gate; compiled only with -tags dsqlspike.
package dsqllint

import (
	"encoding/json"
	"os"
	"testing"
	"time"
)

func TestSpikeBaselineScan(t *testing.T) {
	var best time.Duration
	var snap Snapshot
	for i := 0; i < 5; i++ {
		start := time.Now()
		s, err := Scan("../..")
		if err != nil {
			t.Fatal(err)
		}
		if d := time.Since(start); best == 0 || d < best {
			best = d
		}
		snap = s
	}
	f, _ := os.Open("../../" + InventoryPath)
	inv, err := ParseInventory(f)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	problems := CheckInventory(inv.Entries, snap)
	check := time.Since(start)
	counts := map[Class]int{}
	for _, e := range inv.Entries {
		counts[e.Class]++
	}
	why := map[string]int{}
	for _, s := range snap.Sites {
		why[s.Why]++
	}
	t.Logf("BASELINE scan_best_of_5=%s check=%s sites=%d symbols=%d sqlfiles=%d problems=%d", best, check, len(snap.Sites), len(snap.Symbols), len(snap.SQLFiles), len(problems))
	t.Logf("BASELINE entries=%d classes=%v gaps=%d", len(inv.Entries), counts, len(inv.Gaps))
	t.Logf("BASELINE sites_by_reason=%v", why)
}

// TestSpikeDumpPRSites writes the PR #801 scanner's sites as JSON for the
// spike's cross-check against the typed inventory.
func TestSpikeDumpPRSites(t *testing.T) {
	snap, err := Scan("../..")
	if err != nil {
		t.Fatal(err)
	}
	f, _ := os.Open("../../" + InventoryPath)
	inv, _ := ParseInventory(f)
	class := map[string]string{}
	for _, e := range inv.Entries {
		class[e.Location] = string(e.Class)
	}
	type row struct {
		Loc, Why, Class string
		Line            int
	}
	var rows []row
	for loc, s := range snap.Sites {
		c := class[loc]
		if c == "" {
			c = class[fileOf(loc)+":*"]
		}
		rows = append(rows, row{loc, s.Why, c, s.Line})
	}
	b, _ := json.Marshal(rows)
	if err := os.WriteFile(os.Getenv("SPIKE_OUT"), b, 0o644); err != nil {
		t.Fatal(err)
	}
}
