package forma

// MaxClaimLimit is the largest ClaimRequest.Limit (#653).
const MaxClaimLimit = 1000

// MaxClaimGroupKey is the most attributes a ClaimGroup.Key may list.
const MaxClaimGroupKey = 4

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
	// Group, when set, restricts the claim to group heads: per-group FIFO.
	Group *ClaimGroup
}

// ClaimGroup restricts a claim to the head of each group (#653). Rows with
// equal Key values form a group, a null component equal to a null. A row is
// eligible only if it matches Live and no other row of its group that
// matches Live precedes it by Order, then row ID ascending. So at most one
// row per group, its head, is claimable at a time. A head another
// transaction holds is skipped, and no later row of its group is claimed in
// its place. Every attribute in Key, Order and Live must be column-bound.
//
// Order is evaluated on stored values: a row that enters a group with an
// Order value earlier than a row already claimed does not revoke that claim.
// A caller needing strict FIFO assigns Order values monotonically.
//
// A claimed row must stay its group's head while it is in flight. The claim
// mutation may not write a Key or Order attribute, and neither may be bound
// to a column Forma stamps on every update, such as ltbase_updated_at; each
// is ErrInvalidInput. Live is the caller's to keep matching a claimed row.
type ClaimGroup struct {
	// Key lists 1 to MaxClaimGroupKey attributes, with no repeats.
	Key []string
	// Order is the head order within a group, at least one attribute; nulls
	// sort last in either direction, as in a query's Sort.
	Order []OrderBy
	// Live says which rows still block later rows of their group, for
	// example every row not yet published. It is required, and it must still
	// match a claimed row after the claim mutation, or the next row becomes
	// the head as soon as the claim commits.
	Live Condition
}

// ClaimResult answers a claim.
type ClaimResult struct {
	// Records holds each claimed row as stored after the claim mutation, in
	// selection order. It is empty when nothing was claimed.
	Records []*DataRecord
}
