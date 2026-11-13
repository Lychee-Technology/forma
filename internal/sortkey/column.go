package sortkey

import (
	"fmt"

	"github.com/lychee-technology/forma"
)

// The hot-keys query (pgx-hot-leg-plan.md step 4) reads, for each column of
// a page's order, the stored value its key derives from. The helpers below
// say where that value lives: SystemColumnName for the visible CTE's system
// columns, ColumnAttribute for an attribute, and DeriveEAVAttribute for an
// EAV-only attribute, whose row may hold several eav_data rows.

// SystemColumnName answers the system column of the visible CTE (row_id,
// created_at, ver_ts, deleted_ts) that name reaches, matched as DuckDB
// resolves an unquoted identifier, ASCII case aside.
func SystemColumnName(name string) (string, bool) {
	_, key, ok := systemColumn(name)
	return key, ok
}

// ColumnAttribute answers the attribute a sort or cursor column that is not a
// system column reads, resolved as the renderer's name reaches it in DuckDB:
// an exact attribute name, else the attribute whose ParquetAttrColumn fold
// reaches the same column (cacheAttribute). Every column of an Order built by
// OffsetOrder or KeysetOrder resolves this way, so the key a hot row derives
// for it has the type the Order compares.
func ColumnAttribute(name string, cache forma.SchemaAttributeCache) (forma.AttributeMetadata, error) {
	if _, _, ok := systemColumn(name); ok {
		return forma.AttributeMetadata{}, fmt.Errorf("sort column %q is a system column, not an attribute", name)
	}
	return cacheAttribute(name, cache)
}

// DeriveEAVAttribute derives an EAV-only attribute's hot sort key from every
// value its eav_data rows hold (value_numeric for the numeric family, bool
// and dates, value_text for text and uuid), as the export collapses them:
// MAX(CASE WHEN attr_id = … THEN <cast> END), the greatest derived value in
// the export type's order, NULLs skipped. A scalar attribute holds one row,
// and its key is that row's derivation; a row with none, or with only values
// the export's TRY_CAST refuses, derives NULL. A list attribute has no sort
// key (AttributeType), and a column-bound one derives from its column
// (DeriveAttribute).
func DeriveEAVAttribute(meta forma.AttributeMetadata, stored []any) (any, error) {
	if meta.ColumnBinding != nil {
		return nil, fmt.Errorf("derive sort key: attribute %q is bound to column %q, so its key derives from that column, not from eav_data", meta.AttributeName, meta.ColumnBinding.ColumnName)
	}
	typ, err := AttributeType(meta)
	if err != nil {
		return nil, fmt.Errorf("derive sort key: %w", err)
	}
	var best any
	for _, s := range stored {
		key, err := DeriveAttribute(meta, s)
		if err != nil {
			return nil, err
		}
		if key == nil {
			continue
		}
		if best == nil || compareValues(typ, key, best) > 0 {
			best = key
		}
	}
	return best, nil
}
