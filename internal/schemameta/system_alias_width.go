package schemameta

import (
	"errors"
	"fmt"
	"sort"

	"github.com/lychee-technology/forma"
)

// ErrNarrowSystemAlias marks a smallint or integer attribute bound to a
// system timestamp column (#745). validate-schema-consistency matches it to
// file the finding under its own category.
var ErrNarrowSystemAlias = errors.New("a declared type too narrow for the column's epoch-millisecond stamps")

// systemTimestampColumns are the system columns that hold the store's own
// epoch-millisecond stamps (about 1.7e12 today). ltbase_schema_id is not one:
// it holds a smallint, which both integer widths can hold.
var systemTimestampColumns = map[forma.MainColumn]struct{}{
	forma.MainColumnCreatedAt: {},
	forma.MainColumnUpdatedAt: {},
	forma.MainColumnDeletedAt: {},
}

// validateSystemColumnAlias refuses a smallint or integer bound to a system
// timestamp column (#745). The column matrix alone admits it, since
// ColumnType() classifies the stamps as bigint and the numeric family may
// narrow into a bigint column: for a user column the write funnel judges
// every value against the declared width (checkStorageFit). A system column
// is never written through the funnel (isSystemManagedColumn) and an update
// cannot name its alias (#674), so the alias only ever reads the store's
// stamp, which neither type can hold, and the read used to answer a wrapped
// value. Under DEC-PREPROD-COMPAT (no-legacy-by-default) an already-admitted
// alias is not grandfathered: registration and every load refuse it alike.
//
// The remedy names what the attribute may declare, and what it may bind to
// if it is meant to hold values of its own. smallint and integer widen to
// bigint and numeric under the same binding (schemaledger D2), so a
// registered schema can be redeclared without a migration.
func validateSystemColumnAlias(attrName string, meta forma.AttributeMetadata) error {
	if meta.ColumnBinding == nil {
		return nil
	}
	if meta.ValueType != forma.ValueTypeSmallInt && meta.ValueType != forma.ValueTypeInteger {
		return nil
	}
	column := meta.ColumnBinding.ColumnName
	if _, ok := systemTimestampColumns[column]; !ok {
		return nil
	}
	return fmt.Errorf("attribute %s (valueType %s) binds system column %s with %w (#745): "+
		"declare it bigint, numeric, date or datetime to read the stamp, or bind it to a smallint, integer, bigint "+
		"or double column to hold values of its own",
		attrName, meta.ValueType, column, ErrNarrowSystemAlias)
}

// validateSystemColumnAliases applies validateSystemColumnAlias to every
// active attribute of a cache no admission saw: a caller's own
// forma.SchemaRegistry (GuardReservedAttrNames). Attributes are judged in
// name order so the refusal is stable across reads.
func validateSystemColumnAliases(cache forma.SchemaAttributeCache) error {
	names := make([]string, 0, len(cache))
	for name, meta := range cache {
		if !meta.Retired && meta.ColumnBinding != nil {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		if err := validateSystemColumnAlias(name, cache[name]); err != nil {
			return err
		}
	}
	return nil
}
