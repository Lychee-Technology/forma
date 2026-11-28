package forma

import "time"

// Atomic update operators (#649). An UpdateOp computes an attribute's new
// value from the row as it stands under the write's lock, or from the
// database's transaction clock, so a caller never reads a value outside the
// lock to compute the next one; Unset (#938) makes the attribute absent. They
// ride on EntityOperation.UpdateOps and apply to Update only. See
// docs/conditional-writes.md.

// UpdateOpKind names what an UpdateOp computes.
type UpdateOpKind string

const (
	// UpdateOpIncrement adds By to the attribute's current value. It applies
	// to smallint, integer and bigint attributes.
	UpdateOpIncrement UpdateOpKind = "increment"
	// UpdateOpServerTime sets the attribute to the transaction's $now plus
	// Offset. It applies to date and datetime attributes.
	UpdateOpServerTime UpdateOpKind = "server_time"
	// UpdateOpUnset removes the attribute's value, so the row no longer
	// holds it. It applies to scalar and list attributes; By and Offset must
	// be zero.
	UpdateOpUnset UpdateOpKind = "unset"
)

// UpdateOp is one atomic update of one attribute. Build it with Increment,
// ServerTime or Unset.
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

// Unset makes attr absent: the update removes the value the row holds for
// it, whether it is stored in a main column or in EAV, and an object the
// removal leaves empty goes with it. A row that holds no value for attr is
// updated as usual and stays without one. The stored value is discarded
// without being decoded, so a value the read refuses can still be cleared.
//
// The result is validated like any other update's, and nothing about the
// validation is new. A required policy in the attribute metadata
// (AttributeMetadata.RequiredPolicy: required_always, or
// required_if_parent_present while the result still holds the parent)
// refuses the unset in every mode. A requirement only the JSON Schema states
// follows EntityConfig.ValidateUpdatesStrict: strict validation refuses the
// unset, report-only validation accepts it and logs and counts the violation,
// and a manager without a validator does not check it.
//
// attr must be a scalar or list attribute, registered under the name the
// attributes file gives it ("profile.nickname"). An unknown attribute, an
// object or an array of objects (whether the attributes file registers
// anything beneath it or not), an attribute inside an array of objects, an
// indexed, wildcard or JSONPath spelling ("tags[0]", "items.*", "$.note") of
// a name the schema does not register, a relation attribute and one bound to
// a system column are each refused with ErrInvalidInput naming attr, as is a
// nested attribute whose registry did not derive its array ancestry
// (AttributeMetadata.ArrayAncestors). Like every operator, Unset must be the
// attribute's only writer in the update.
//
// Unset is the only way to clear a value: an explicit null in Updates stays
// refused, and an empty list ([]any{}) is a value, the empty list, not a
// clear.
func Unset(attr string) UpdateOp {
	return UpdateOp{Attribute: attr, Kind: UpdateOpUnset}
}
