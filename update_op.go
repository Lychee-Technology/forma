package forma

import "time"

// Atomic update operators (#649). An UpdateOp computes an attribute's new
// value from the row as it stands under the write's lock, or from the
// database's transaction clock, so a caller never reads a value outside the
// lock to compute the next one. They ride on EntityOperation.UpdateOps and
// apply to Update only. See docs/conditional-writes.md.

// UpdateOpKind names what an UpdateOp computes.
type UpdateOpKind string

const (
	// UpdateOpIncrement adds By to the attribute's current value. It applies
	// to smallint, integer and bigint attributes.
	UpdateOpIncrement UpdateOpKind = "increment"
	// UpdateOpServerTime sets the attribute to the transaction's $now plus
	// Offset. It applies to date and datetime attributes.
	UpdateOpServerTime UpdateOpKind = "server_time"
)

// UpdateOp is one atomic update of one attribute. Build it with Increment or
// ServerTime.
type UpdateOp struct {
	// Attribute is the schema attribute the operator writes, in the dotted
	// spelling the attributes file uses ("stats.attempts"). It must be one
	// the store writes: an attribute under an x-relation root, or one bound
	// to a system column such as ltbase_created_at, is refused with
	// ErrInvalidInput.
	Attribute string
	Kind      UpdateOpKind
	// By is the increment's addend; it may be negative or zero. It must be
	// zero for any other kind.
	By int64
	// Offset shifts server_time's instant; it must be a whole number of
	// milliseconds. It must be zero for any other kind.
	Offset time.Duration
}

// Increment adds by to attr's current value under the row's lock. The row
// must hold a value for attr, and the result must fit attr's value type; each
// failure is ErrInvalidInput naming the attribute.
func Increment(attr string, by int64) UpdateOp {
	return UpdateOp{Attribute: attr, Kind: UpdateOpIncrement, By: by}
}

// ServerTime sets attr to the database's transaction timestamp plus offset:
// the instant the condition DSL's $now names in the same transaction, never
// the process clock. A date attribute stores that instant's UTC calendar
// day.
func ServerTime(attr string, offset time.Duration) UpdateOp {
	return UpdateOp{Attribute: attr, Kind: UpdateOpServerTime, Offset: offset}
}
