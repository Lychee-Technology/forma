package production

import (
	"fmt"
	"strconv"
	"strings"
)

// The hot-leg latency baseline (#851) itself is e2e-tagged; the parts here
// need no cluster, so the unit suite checks them.

// baselineTemplates is the number of template rows written through the
// EntityManager.
const baselineTemplates = 500

// baselineHotSize is the fixed hot set: unflushed writes at every size, half
// updates of flushed rows and half creates.
const baselineHotSize = 1_000

// parseBaselineSizes parses FORMA_HOT_LEG_BASELINE_SIZES. The loader only
// grows the tables: it clones rows up to size - baselineHotSize/2, and the
// hot set's creates add the rest. Those creates land at every size, cloned
// or not, so each size must exceed the one before by at least that many
// rows, or the table would overshoot it. The first size leaves room for the
// templates and the hot set.
func parseBaselineSizes(raw string) ([]int64, error) {
	minSize := int64(baselineTemplates + baselineHotSize)
	step := int64(baselineHotSize / 2)
	var sizes []int64
	for _, field := range strings.Split(raw, ",") {
		n, err := strconv.ParseInt(strings.TrimSpace(field), 10, 64)
		if err != nil || n < minSize {
			return nil, fmt.Errorf("%q is not a size of at least %d", field, minSize)
		}
		if k := len(sizes); k > 0 && n-sizes[k-1] < step {
			return nil, fmt.Errorf("%d follows %d: each size must exceed the one before by at least %d, the hot set's creates", n, sizes[k-1], step)
		}
		sizes = append(sizes, n)
	}
	return sizes, nil
}
