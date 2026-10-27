package federated

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/stretchr/testify/require"
)

func auditQty(t *testing.T, duck *sqlDBExecutor, meta forma.AttributeMetadata, paths ...string) []DomainFinding {
	t.Helper()
	findings, err := AuditParquetDomain(context.Background(), duck.db, paths, forma.SchemaAttributeCache{"qty": meta})
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

// TestAuditParquetDomainJudgesDatesAndUUIDsByStorage: an EAV date keeps a
// float64 image, so its BIGINT export is held to ±2^53 like an EAV bigint's,
// while a bigint_* date is any int64; and an EAV uuid exported as text must
// be text uuid.Parse reads, which DuckDB's UUID cast is not.
func TestAuditParquetDomainJudgesDatesAndUUIDsByStorage(t *testing.T) {
	duck := newGuardDuck(t)
	dir := t.TempDir()
	eavDate := forma.AttributeMetadata{AttributeName: "qty", AttributeID: 1, ValueType: forma.ValueTypeDateTime}
	boundDate := forma.AttributeMetadata{AttributeName: "qty", AttributeID: 1, ValueType: forma.ValueTypeDateTime,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: "bigint_01", Encoding: forma.MainColumnEncodingUnixMs}}
	eavUUID := forma.AttributeMetadata{AttributeName: "qty", AttributeID: 1, ValueType: forma.ValueTypeUUID}
	const canonical = "'6ba7b810-9dad-11d1-80b4-00c04fd430c8'"

	refused := []struct {
		name, typ, value string
		meta             forma.AttributeMetadata
		evidence         string
	}{
		{"EAV date past 2^53", "BIGINT", "9007199254740993", eavDate, "it holds 9007199254740993"},
		{"list<date> element past 2^53", "BIGINT[]", "[0, -9007199254740993]", listOf(forma.ValueTypeDate),
			"a list element that is not a whole number in [-9007199254740992, 9007199254740992]"},
		{"EAV uuid that does not parse", "VARCHAR", "'not-a-uuid'", eavUUID, "a uuid"},
		{"EAV uuid DuckDB casts and uuid.Parse refuses", "VARCHAR", "'6ba7b810-9dad11d1-80b4-00c04fd430c8'", eavUUID, "a uuid"},
		{"list<uuid> element that does not parse", "VARCHAR[]", "[" + canonical + ", 'x']", listOf(forma.ValueTypeUUID),
			"a list element that is not a uuid"},
	}
	for i, tc := range refused {
		t.Run(tc.name, func(t *testing.T) {
			path := writeQtyObject(t, duck, dir, fmt.Sprintf("refused_%d.parquet", i), tc.typ, tc.value)
			findings := auditQty(t, duck, tc.meta, path)
			require.Len(t, findings, 1)
			require.Contains(t, findings[0].Evidence, tc.evidence)
		})
	}

	require.Empty(t, auditQty(t, duck, eavDate,
		writeQtyObject(t, duck, dir, "date_edge.parquet", "BIGINT", "9007199254740992", "-9007199254740992", "NULL")))
	require.Empty(t, auditQty(t, duck, boundDate,
		writeQtyObject(t, duck, dir, "bound_date.parquet", "BIGINT", "9223372036854775807")))
	require.Empty(t, auditQty(t, duck, eavUUID,
		writeQtyObject(t, duck, dir, "uuid_forms.parquet", "VARCHAR", canonical,
			"'urn:uuid:6ba7b810-9dad-11d1-80b4-00c04fd430c8'", "'{6ba7b810-9dad-11d1-80b4-00c04fd430c8}'", "NULL")))
	require.Empty(t, auditQty(t, duck, listOf(forma.ValueTypeUUID),
		writeQtyObject(t, duck, dir, "uuid_list.parquet", "VARCHAR[]", "["+canonical+", NULL]")))
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
// Each object here is listed with nothing to scan: a numeric qty whose
// footer would show DOUBLE, the case a manifest stamp used to settle without
// the object being opened.
func TestAuditParquetDomainReportsUnreadableObjects(t *testing.T) {
	duck := newGuardDuck(t)
	dir := t.TempDir()
	missing := filepath.Join(dir, "missing.parquet")
	corrupt := filepath.Join(dir, "corrupt.parquet")
	require.NoError(t, os.WriteFile(corrupt, []byte("not a parquet object"), 0o600))
	meta := forma.AttributeMetadata{AttributeName: "qty", AttributeID: 1, ValueType: forma.ValueTypeNumeric}
	for _, path := range []string{missing, corrupt} {
		findings := auditQty(t, duck, meta, path)
		require.Len(t, findings, 1, path)
		require.Equal(t, path, findings[0].Path)
		require.Contains(t, findings[0].Evidence, "could not be read")
	}
}

// TestAuditParquetDomainDecodesEveryObject: a footer that settles a column
// says nothing about its data pages. Here the footer is intact and shows a
// SMALLINT under a smallint bound to smallint_01, which needs no scan, and
// the column's only data is corrupt, so a read of it fails.
func TestAuditParquetDomainDecodesEveryObject(t *testing.T) {
	duck := newGuardDuck(t)
	path := filepath.Join(t.TempDir(), "corrupt_page.parquet")
	_, err := duck.db.Exec(fmt.Sprintf("COPY (SELECT gen_random_uuid() AS row_id, CAST(100 AS BIGINT) AS changed_at, "+
		"CAST(0 AS BIGINT) AS deleted_at, CAST(50 AS BIGINT) AS ltbase_created_at, CAST(i %% 30000 AS SMALLINT) AS qty "+
		"FROM range(5000) AS r(i)) TO '%s' (FORMAT PARQUET)", path))
	require.NoError(t, err)
	meta := forma.AttributeMetadata{AttributeName: "qty", AttributeID: 1, ValueType: forma.ValueTypeSmallInt,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: "smallint_01"}}
	require.Empty(t, auditQty(t, duck, meta, path), "the intact object")

	corruptColumnChunk(t, duck, path, "qty")
	var rows int64
	require.NoError(t, duck.db.QueryRow(fmt.Sprintf("SELECT count(*) FROM read_parquet('%s')", path)).Scan(&rows))
	require.EqualValues(t, 5000, rows, "the footer still reads")
	_, err = duck.db.Exec(fmt.Sprintf("SELECT sum(qty) FROM read_parquet('%s')", path))
	require.Error(t, err, "the column's data no longer reads")

	findings := auditQty(t, duck, meta, path)
	require.Len(t, findings, 1)
	require.Equal(t, path, findings[0].Path)
	require.Contains(t, findings[0].Evidence, "its data could not be read")
}

// corruptColumnChunk overwrites every byte of col's column chunk, leaving
// the footer as written.
func corruptColumnChunk(t *testing.T, duck *sqlDBExecutor, path, col string) {
	t.Helper()
	var start, size int64
	require.NoError(t, duck.db.QueryRow(fmt.Sprintf(
		"SELECT COALESCE(dictionary_page_offset, data_page_offset), total_compressed_size FROM parquet_metadata('%s') WHERE path_in_schema = '%s'",
		path, col)).Scan(&start, &size))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	for i := start; i < start+size; i++ {
		data[i] = 0xFF
	}
	require.NoError(t, os.WriteFile(path, data, 0o600))
}
