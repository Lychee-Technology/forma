package forma

// MaxClaimLimit is the largest ClaimRequest.Limit (#653).
const MaxClaimLimit = 1000

// ClaimRequest is Tx.Claim: take up to Limit rows of one schema that match
// Condition, apply the claim mutation to each, and answer them.
//
// The candidates are the matching rows in Sort order, with the row ID
// ascending as the final tiebreak, as the Tx sees them: the latest committed
// hot-tier state plus the Tx's own writes. On PostgreSQL a candidate that
// another Forma transaction holds for writing (a write, a row assertion or
// a claim in progress) is skipped, never waited on. Each row taken is
// re-checked against Condition while held, and only the rows that still
// match are claimed. With no concurrent writer, Claim answers exactly the
// first Limit matching rows, or all of them when fewer match; under
// concurrency it can answer fewer, none included. An empty result is not an
// error. See docs/claims.md.
type ClaimRequest struct {
	SchemaName string
	// Condition says which rows are claimable, for example due or whose
	// lease expired. It is required, and an empty and or or anywhere in it is
	// ErrInvalidInput. The claim mutation must make every claimed row stop
	// matching it (see Tx.Claim).
	Condition Condition
	// Sort orders the candidates; the row ID ascending is always the last
	// key.
	Sort []OrderBy
	// Limit caps the rows one call claims: 1 to MaxClaimLimit.
	Limit int
	// Updates and UpdateOps are the claim mutation, applied to each claimed
	// row as Update applies them (#649), typically a status, a lease from
	// ServerTime and a generation Increment. At least one is required, and
	// neither may change an attribute of a unique key.
	Updates   map[string]any
	UpdateOps []UpdateOp
}

// ClaimResult answers a claim.
type ClaimResult struct {
	// Records holds each claimed row as stored after the claim mutation, in
	// selection order. It is empty when nothing was claimed.
	Records []*DataRecord
}
