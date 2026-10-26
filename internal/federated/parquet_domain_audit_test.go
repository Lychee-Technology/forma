package federated

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

func auditQty(t *testing.T, duck *sqlDBExecutor, meta forma.AttributeMetadata, paths ...string) []DomainFinding {
	t.Helper()
	findings, err := AuditParquetDomain(context.Background(), duck.db, paths, nil, forma.SchemaAttributeCache{"qty": meta})
	require.NoError(t, err)
	return findings
}

// TestAuditParquetDomainScansWhatTheFooterCannotSettle is the #733 baseline
// case Phase 1 cannot see: an EAV-only integer exports DOUBLE, like numeric,
// so a DOUBLE object reads under it without conversion, yet can hold 1001.5.
func TestAuditParquetDomainScansWhatTheFooterCannotSettle(t *testing.T) {
	duck := newGuardDuck(t)
	dir := t.TempDir()
	eavInteger := forma.AttributeMetadata{AttributeName: "qty", AttributeID: 1, ValueType: forma.ValueTypeInteger}
	boundSmallint := forma.AttributeMetadata{AttributeName: "qty", AttributeID: 1, ValueType: forma.ValueTypeSmallInt,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: "smallint_01"}}
	numeric := forma.AttributeMetadata{AttributeName: "qty", AttributeID: 1, ValueType: forma.ValueTypeNumeric}
	eavBigint := forma.AttributeMetadata{AttributeName: "qty", AttributeID: 1, ValueType: forma.ValueTypeBigInt}
	doubleBigint := forma.AttributeMetadata{AttributeName: "qty", AttributeID: 1, ValueType: forma.ValueTypeBigInt,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: "double_01"}}
	cases := []struct {
		name     string
		typ      string
		values   []string
		meta     forma.AttributeMetadata
		evidence string
	}{
		{"fraction under EAV-only integer", "DOUBLE", []string{"7", "1001.5"}, eavInteger, "it holds 1001.5, which is not a whole number in [-2147483648, 2147483647]"},
		{"out of range under EAV-only integer", "DOUBLE", []string{"3e10"}, eavInteger, "which is not a whole number"},
		{"wider integer under smallint", "INTEGER", []string{"40000"}, boundSmallint, "it holds 40000"},
		{"text under numeric", "VARCHAR", []string{"'12'"}, numeric, "it holds a text value that is not a finite number"},
		{"boolean under numeric", "BOOLEAN", []string{"true"}, numeric, "a different type family"},
		// The storage domain, not what the read path converts: an EAV-only
		// bigint keeps its float64 image, admitted only within ±2^53, and
		// numeric storage refuses non-finite values.
		{"past 2^53 under EAV-only bigint", "DOUBLE", []string{"1152921504606846976"}, eavBigint, "which is not a whole number in [-9007199254740992, 9007199254740992]"},
		{"BIGINT past 2^53 under EAV-only bigint", "BIGINT", []string{"1152921504606846976"}, eavBigint, "it holds 1152921504606846976"},
		{"past 2^53 under double-bound bigint", "BIGINT", []string{"-1152921504606846976"}, doubleBigint, "it holds -1152921504606846976"},
		{"NaN under numeric", "DOUBLE", []string{"1", "'NaN'"}, numeric, "it holds nan, which is not a finite number"},
		{"infinity under numeric", "DOUBLE", []string{"'-Infinity'"}, numeric, "which is not a finite number"},
		{"list element past 2^53 under list<bigint>", "DOUBLE[]", []string{"[1, 1152921504606846976]"}, listOf(forma.ValueTypeBigInt),
			"a list element that is not a whole number in [-9007199254740992, 9007199254740992]"},
		{"infinite list element under list<numeric>", "DOUBLE[]", []string{"['1', 'inf']"}, listOf(forma.ValueTypeNumeric),
			"a list element that is not a finite number"},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeQtyObject(t, duck, dir, fmt.Sprintf("bad_%d.parquet", i), tc.typ, tc.values...)
			findings := auditQty(t, duck, tc.meta, path)
			require.Len(t, findings, 1)
			require.Equal(t, path, findings[0].Path)
			require.Equal(t, "qty", findings[0].Attribute)
			require.Contains(t, findings[0].Evidence, tc.evidence)
		})
	}
}

func TestAuditParquetDomainPassesAdmittedObjects(t *testing.T) {
	duck := newGuardDuck(t)
	dir := t.TempDir()
	eavInteger := forma.AttributeMetadata{AttributeName: "qty", AttributeID: 1, ValueType: forma.ValueTypeInteger}
	whole := writeQtyObject(t, duck, dir, "whole.parquet", "DOUBLE", "7", "-2147483648", "NULL")
	narrow := writeQtyObject(t, duck, dir, "narrow.parquet", "SMALLINT", "7")
	require.Empty(t, auditQty(t, duck, eavInteger, whole, narrow))
	require.Empty(t, auditQty(t, duck, eavInteger, filepath.Join(dir, "*.parquet")), "a glob audits each object it matches")

	// The edges of the storage domains: ±2^53 for a bigint's float64 image,
	// the full int64 range in a bigint_* column, any finite number under
	// numeric, and a BIGINT under numeric, which D2 lets round.
	eavBigint := forma.AttributeMetadata{AttributeName: "qty", AttributeID: 1, ValueType: forma.ValueTypeBigInt}
	boundBigint := forma.AttributeMetadata{AttributeName: "qty", AttributeID: 1, ValueType: forma.ValueTypeBigInt,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: "bigint_01"}}
	numeric := forma.AttributeMetadata{AttributeName: "qty", AttributeID: 1, ValueType: forma.ValueTypeNumeric}
	sub := t.TempDir()
	require.Empty(t, auditQty(t, duck, eavBigint,
		writeQtyObject(t, duck, sub, "image_edge.parquet", "DOUBLE", "9007199254740992", "-9007199254740992")))
	require.Empty(t, auditQty(t, duck, listOf(forma.ValueTypeBigInt),
		writeQtyObject(t, duck, sub, "list_edge.parquet", "DOUBLE[]", "[9007199254740992, NULL]")))
	require.Empty(t, auditQty(t, duck, boundBigint,
		writeQtyObject(t, duck, sub, "column_edge.parquet", "BIGINT", "9223372036854775807")))
	require.Empty(t, auditQty(t, duck, numeric,
		writeQtyObject(t, duck, sub, "finite.parquet", "DOUBLE", "1001.5", "1e308", "NULL"),
		writeQtyObject(t, duck, sub, "rounding.parquet", "BIGINT", "9223372036854775807")))
}

func listOf(items forma.ValueType) forma.AttributeMetadata {
	return forma.AttributeMetadata{AttributeName: "qty", AttributeID: 1, ValueType: forma.ValueTypeList, ItemsType: items}
}

func TestAuditParquetDomainScansListElements(t *testing.T) {
	duck := newGuardDuck(t)
	dir := t.TempDir()
	meta := forma.AttributeMetadata{AttributeName: "qty", AttributeID: 1, ValueType: forma.ValueTypeList, ItemsType: forma.ValueTypeSmallInt}
	clean := writeQtyObject(t, duck, dir, "clean.parquet", "DOUBLE[]", "[1, 2, NULL]", "NULL")
	require.Empty(t, auditQty(t, duck, meta, clean))

	dirty := writeQtyObject(t, duck, dir, "dirty.parquet", "DOUBLE[]", "[1, 2.5]")
	findings := auditQty(t, duck, meta, dirty)
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].Evidence, "a list element that is not a whole number in [-32768, 32767]")

	scalar := writeQtyObject(t, duck, dir, "scalar.parquet", "DOUBLE", "1")
	findings = auditQty(t, duck, meta, scalar)
	require.Len(t, findings, 1)
	require.Contains(t, findings[0].Evidence, "different type family")
}

// TestAuditParquetDomainReportsUnreadableObjects: a baseline never blesses
// an object it could not read.
func TestAuditParquetDomainReportsUnreadableObjects(t *testing.T) {
	duck := newGuardDuck(t)
	missing := filepath.Join(t.TempDir(), "missing.parquet")
	meta := forma.AttributeMetadata{AttributeName: "qty", AttributeID: 1, ValueType: forma.ValueTypeText}
	findings := auditQty(t, duck, meta, missing)
	require.Len(t, findings, 1)
	require.Equal(t, missing, findings[0].Path)
	require.Contains(t, findings[0].Evidence, "could not be read")
}
