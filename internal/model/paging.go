package model

// DefaultPageSize is the fallback page size used when the caller supplies a
// non-positive limit.
const DefaultPageSize = 50

// FederatedMaxRows is the default per-source fetch cap used by federated
// queries when no explicit MaxRows option is provided.
const FederatedMaxRows = 10000

// ComputeTotalPages returns the page count for total records at the given limit.
func ComputeTotalPages(total int64, limit int) int {
	if total == 0 || limit <= 0 {
		return 0
	}
	return int((total + int64(limit) - 1) / int64(limit))
}
