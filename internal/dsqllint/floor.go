package dsqllint

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/lychee-technology/forma/internal/dsqllint/corpus"
)

// Floor is one corpus-floor line: the corpus must keep at least Min records of
// Label.
type Floor struct {
	Label string
	Min   int
	Line  int
}

// ParseFloor reads corpus-floor lines of the form
//
//	<label> <min>
func ParseFloor(r io.Reader) ([]Floor, error) {
	var floors []Floor
	seen := make(map[string]int)
	scanner := bufio.NewScanner(r)
	for n := 1; scanner.Scan(); n++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Fields(line)
		if len(f) != 2 || !corpus.LabelPattern.MatchString(f[0]) {
			return nil, fmt.Errorf("corpus floor line %d: want <label> <min>, got %q", n, line)
		}
		minimum, err := strconv.Atoi(f[1])
		if err != nil || minimum < 1 {
			return nil, fmt.Errorf("corpus floor line %d: min %q must be a positive number", n, f[1])
		}
		if prev, ok := seen[f[0]]; ok {
			return nil, fmt.Errorf("corpus floor line %d: %s is already listed on line %d", n, f[0], prev)
		}
		seen[f[0]] = n
		floors = append(floors, Floor{Label: f[0], Min: minimum, Line: n})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read corpus floor: %w", err)
	}
	if len(floors) == 0 {
		return nil, fmt.Errorf("the corpus floor lists nothing")
	}
	return floors, nil
}

// checkFloor answers a problem per label holding fewer records than its floor:
// a corpus test that was deleted, skipped, relabelled or lost a statement. It
// says nothing about SQL the corpus never held; that is the inventory's job
// (#804, #805).
func checkFloor(floors []Floor, units []*unit) []error {
	counts := make(map[string]int)
	for _, u := range units {
		for _, r := range u.records {
			counts[r.Label]++
		}
	}
	var problems []error
	for _, f := range floors {
		if got := counts[f.Label]; got < f.Min {
			problems = append(problems, fmt.Errorf("%s line %d: the corpus holds %d record(s) of %s, the floor is %d", FloorPath, f.Line, got, f.Label, f.Min))
		}
	}
	return problems
}
