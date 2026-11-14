// Package sortkey holds the Go copy of the parquet leg's order: the export
// type of every sort and cursor column, the hot sort keys derived from
// stored values by the CDC export's rules, the comparator that sorts them,
// the three-valued keyset predicate, and the cursor normalization both legs
// share (design.md §6.1, Ordering parity; pgx-hot-leg-plan.md step 2).
//
// A key value has the Go type of its column's export type: int64 for
// BIGINT, INTEGER and SMALLINT, float64 for DOUBLE, string for VARCHAR, bool
// for BOOLEAN and uuid.UUID for UUID. nil is SQL NULL. Derivation and
// normalization produce only those types, and the comparator and the
// predicate read only them.
package sortkey

import (
	"fmt"
	"sort"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// ExportType is the DuckDB type a sort column has in parquet, as
// sqlgen.DuckDBNullScanType names it.
type ExportType string

const (
	BigInt   ExportType = "BIGINT"
	Integer  ExportType = "INTEGER"
	SmallInt ExportType = "SMALLINT"
	Double   ExportType = "DOUBLE"
	Varchar  ExportType = "VARCHAR"
	Boolean  ExportType = "BOOLEAN"
	UUID     ExportType = "UUID"
)

// System columns of the federated visible CTE that a page can order by.
const (
	ColumnRowID     = "row_id"
	ColumnCreatedAt = "created_at"
	ColumnVerTS     = "ver_ts"
	ColumnDeletedTS = "deleted_ts"
)

// ParseExportType reads a type name the renderer or a cursor carries.
func ParseExportType(name string) (ExportType, error) {
	switch t := ExportType(name); t {
	case BigInt, Integer, SmallInt, Double, Varchar, Boolean, UUID:
		return t, nil
	default:
		return "", fmt.Errorf("sort column type %q is not an export type a sort key can have: expected BIGINT, INTEGER, SMALLINT, DOUBLE, VARCHAR, BOOLEAN or UUID", name)
	}
}

// isInteger reports whether t compares as an integer.
func (t ExportType) isInteger() bool {
	return t == BigInt || t == Integer || t == SmallInt
}

// columnSpec is what a sort column needs beyond its name: its export type,
// and whether it is a uuid attribute, whose VARCHAR values are uuid text.
type columnSpec struct {
	typ      ExportType
	uuidText bool
}

// systemColumn resolves a system column of the visible CTE, matched as
// DuckDB resolves an unquoted identifier: ASCII case aside.
func systemColumn(name string) (columnSpec, string, bool) {
	switch key := sqlgen.DuckDBFoldIdentifier(name); key {
	case ColumnRowID:
		return columnSpec{typ: UUID}, key, true
	case ColumnCreatedAt, ColumnVerTS, ColumnDeletedTS:
		return columnSpec{typ: BigInt}, key, true
	default:
		return columnSpec{}, "", false
	}
}

// AttributeType returns the export type of an attribute's sort key. A list
// attribute has none: DuckDB orders a LIST element by element, the
// comparator does not, and the federated route refuses such a sort (#853).
func AttributeType(meta forma.AttributeMetadata) (ExportType, error) {
	spec, err := attributeSpec(meta)
	if err != nil {
		return "", err
	}
	return spec.typ, nil
}

func attributeSpec(meta forma.AttributeMetadata) (columnSpec, error) {
	if meta.ValueType == forma.ValueTypeList {
		return columnSpec{}, fmt.Errorf("attribute %q is a list: a list sort key orders element by element in DuckDB, which the hot leg's comparator does not reproduce, so the federated route cannot sort or page on it", meta.AttributeName)
	}
	typ, err := ParseExportType(sqlgen.DuckDBNullScanType(meta))
	if err != nil {
		return columnSpec{}, fmt.Errorf("attribute %q of value type %q: %w", meta.AttributeName, meta.ValueType, err)
	}
	return columnSpec{typ: typ, uuidText: meta.ValueType == forma.ValueTypeUUID}, nil
}

// orderSpec resolves a resolved sort key the way the offset ORDER BY renders
// it: a main-column order reads the bound column at the attribute's value
// type, an EAV order the pivoted attribute.
func orderSpec(ao model.AttributeOrder) (columnSpec, error) {
	meta := forma.AttributeMetadata{AttributeName: ao.AttrName, AttributeID: ao.AttrID, ValueType: ao.ValueType}
	if ao.IsMainColumn() {
		meta.ColumnBinding = &forma.MainColumnBinding{ColumnName: forma.MainColumn(ao.ColumnName)}
	}
	return attributeSpec(meta)
}

// cursorColumnSpec resolves a keyset cursor column: a system column by its
// own name, else the request's sort key of that name, else the schema's
// attribute DuckDB binds it to (cacheAttribute). A column none of them knows has no export type, so its boundary
// cannot be normalized and the cursor is refused.
func cursorColumnSpec(name string, orders []model.AttributeOrder, cache forma.SchemaAttributeCache) (columnSpec, error) {
	if spec, _, ok := systemColumn(name); ok {
		return spec, nil
	}
	for _, ao := range orders {
		if ao.AttrName == name {
			return orderSpec(ao)
		}
	}
	meta, err := cacheAttribute(name, cache)
	if err != nil {
		return columnSpec{}, err
	}
	return attributeSpec(meta)
}

// cacheAttribute finds the attribute a cursor column names as DuckDB binds
// it. The renderer emits the column's ParquetAttrColumn fold, which DuckDB
// resolves as an unquoted identifier, ASCII case aside, so "COUNT" reaches
// the column of the attribute "count". An exact name wins; otherwise the
// attribute whose fold reaches the same column is the one. Registration
// refuses two attributes folding onto one column
// (sqlgen.ValidateParquetAttrColumns), so at most one does; should a cache
// that skipped that guard hold two, the cursor is refused rather than typed
// by a guess. The name rules (system columns, placeholder, dedup machinery,
// safe identifier) are checkKeysetCursor's and run before this.
func cacheAttribute(name string, cache forma.SchemaAttributeCache) (forma.AttributeMetadata, error) {
	if meta, ok := cache[name]; ok {
		if meta.AttributeName == "" {
			meta.AttributeName = name
		}
		return meta, nil
	}
	key := sqlgen.DuckDBFoldIdentifier(sqlgen.ParquetAttrColumn(name))
	var found []string
	for attr := range cache {
		if sqlgen.DuckDBFoldIdentifier(sqlgen.ParquetAttrColumn(attr)) == key {
			found = append(found, attr)
		}
	}
	switch len(found) {
	case 0:
		return forma.AttributeMetadata{}, fmt.Errorf("keyset cursor column %q is neither a system column (row_id, created_at, ver_ts, deleted_ts) nor an attribute of the schema's metadata, so its boundary has no export type to be normalized to", name)
	case 1:
		meta := cache[found[0]]
		if meta.AttributeName == "" {
			meta.AttributeName = found[0]
		}
		return meta, nil
	default:
		sort.Strings(found)
		return forma.AttributeMetadata{}, fmt.Errorf("keyset cursor column %q reaches the parquet column %q, onto which the schema's attributes %q all fold, so its boundary has no single export type to be normalized to", name, key, found)
	}
}
