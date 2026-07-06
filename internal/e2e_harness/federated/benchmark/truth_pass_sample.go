package benchmark

import (
	"hash/fnv"
	"math/rand"
)

// truthPassSampleIndices picks the candidate indices to spot-check when the
// truth-pass sample cap applies. It returns nil when sampling is disabled
// (cap <= 0) or unnecessary (total <= cap). The selection is deterministic
// for a given (seed, workloadName, total, cap) so identical benchmark
// configs verify identical candidates — a repeatability requirement for
// heavy-live baselines (#100).
func truthPassSampleIndices(seed int64, workloadName string, total, cap int) map[int]struct{} {
	if cap <= 0 || total <= cap {
		return nil
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(workloadName))
	rng := rand.New(rand.NewSource(seed ^ int64(h.Sum64()))) //nolint:gosec // deterministic sampling, not crypto
	picked := make(map[int]struct{}, cap)
	for _, idx := range rng.Perm(total)[:cap] {
		picked[idx] = struct{}{}
	}
	return picked
}
