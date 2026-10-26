package schemaledger

import (
	"fmt"

	"github.com/lychee-technology/forma"
)

// Verdict is how the D2 type rules judge one attribute's change.
type Verdict int

const (
	// Unchanged: same name, type, items_type and binding.
	Unchanged Verdict = iota
	// Allowed: the change reads every stored value unchanged.
	Allowed
	// AllowedWithWarning: allowed, but a stored value can read rounded. The
	// warning is printed and stored on the generation.
	AllowedWithWarning
	// Refused: the change needs a migration (#730), or is not ruled.
	Refused
)

// Change is one attribute's judged change from the active generation.
type Change struct {
	Old, New Entry
	Verdict  Verdict
	// Reason explains a refusal or a warning, with the remediation.
	Reason string
}

// Judge applies D2's type rules (docs/schema-evolution.md, D2 Decision) to
// one attribute that both the active generation and the new declaration
// hold. Whether the change is allowed depends only on the two entries, never
// on stored values:
//
//   - an attribute id is never rebound to another name: a rename is a
//     removal plus an addition under a new id;
//   - a binding change under the same type is allowed (D2 item 4);
//   - a type change together with a binding change is refused: each is
//     judged in its own apply;
//   - an items_type change is refused, since it is not ruled;
//   - a valueType change is judged by typeChange, which allows only the
//     changes D2 lists; every other one is refused.
func Judge(old, next Entry) Change {
	c := Change{Old: old, New: next}
	switch {
	case old.Name != next.Name:
		return c.refuse("attribute id %d is recorded as %s but now declared as %s; an attribute id is never rebound. "+
			"A rename is a removal plus an addition under a new id", old.AttrID, old.Name, next.Name)
	case old.sameType(next) && old.sameBinding(next):
		return c
	case old.sameType(next):
		c.Verdict = Allowed
		return c
	case !old.sameBinding(next):
		return c.refuse("the change moves the attribute from %s to %s and changes its type from %s to %s in one generation; "+
			"apply the binding change and the type change separately", old.storageLabel(), next.storageLabel(), old.typeLabel(), next.typeLabel())
	case old.ValueType == forma.ValueTypeList && next.ValueType == forma.ValueTypeList:
		return c.refuse("an items_type change (%s to %s) is not ruled under D2, so it is refused; "+
			"converting stored elements is an explicit migration (#730)", old.ItemsType, next.ItemsType)
	}
	return typeChange(c)
}

// typeChange judges a valueType change under an unchanged binding.
func typeChange(c Change) Change {
	from, to := c.Old.ValueType, c.New.ValueType
	if c.Old.Column == "" && (from == forma.ValueTypeSmallInt || from == forma.ValueTypeInteger) {
		return c.refuse("an EAV-only %s attribute cannot change type until #729 is fixed: a process still on the old "+
			"declaration reads a value the new type allows converted, and an unrelated update writes it back", from)
	}
	switch {
	case widensInteger(from, to):
		c.Verdict = Allowed
	case to == forma.ValueTypeNumeric && (from == forma.ValueTypeSmallInt || from == forma.ValueTypeInteger):
		c.Verdict = Allowed
	case from == forma.ValueTypeBigInt && to == forma.ValueTypeNumeric:
		if c.Old.columnType() == forma.MainColumnTypeBigint {
			c.Verdict = AllowedWithWarning
			c.Reason = fmt.Sprintf("%s holds the full int64 range, so a stored value beyond ±2^53 reads rounded to its "+
				"float64 on every route once numeric is active; this change is one-way, and its reverse is a migration (#730)", c.Old.Column)
			return c
		}
		c.Verdict = Allowed
	case narrows(from, to):
		return c.refuse("%s to %s narrows the type, which is refused even when every stored value fits; "+
			"converting stored values is an explicit migration (#730)", from, to)
	default:
		return c.refuse("%s to %s is not a type change D2 allows (only smallint to integer to bigint, and smallint, "+
			"integer or bigint to numeric); converting stored values is an explicit migration (#730)", c.Old.typeLabel(), c.New.typeLabel())
	}
	return c
}

func (c Change) refuse(format string, args ...any) Change {
	c.Verdict = Refused
	c.Reason = fmt.Sprintf(format, args...)
	return c
}

// integerRank orders the integer family by width.
var integerRank = map[forma.ValueType]int{
	forma.ValueTypeSmallInt: 1,
	forma.ValueTypeInteger:  2,
	forma.ValueTypeBigInt:   3,
}

func widensInteger(from, to forma.ValueType) bool {
	f, okFrom := integerRank[from]
	t, okTo := integerRank[to]
	return okFrom && okTo && f < t
}

// narrows reports a change to a narrower member of the numeric family.
func narrows(from, to forma.ValueType) bool {
	t, okTo := integerRank[to]
	if !okTo {
		return false
	}
	if from == forma.ValueTypeNumeric {
		return true
	}
	f, okFrom := integerRank[from]
	return okFrom && f > t
}
