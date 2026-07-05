package federated

const defaultPageSize = 50
const federatedMaxRows = 10000

func computeTotalPages(total int64, limit int) int {
	if total == 0 || limit <= 0 {
		return 0
	}
	return int((total + int64(limit) - 1) / int64(limit))
}
