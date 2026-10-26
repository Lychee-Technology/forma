package federated

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/redact"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// DomainFinding is one parquet object baseline-schema refuses to bless: a
// column holding a value its attribute's declaration does not admit, or an
// object whose columns or values could not be read.
type DomainFinding struct {
	Path      string
	Attribute string
	Declared  string
	FileType  string
	Evidence  string
}

// AuditParquetDomain checks every live object in paths (concrete URIs or
// globs) against the declaration cache: whether each attribute column holds
// only values the declaration admits (#733).
//
// It reads every object's footer and takes no manifest stamps. A stamp lets
// the read path skip opening an object, which here would bless one that is
// missing or unreadable; and once the footer is read, its types are the
// bytes' own, so a stamp would add nothing.
//
// It differs from CheckTypes, which asks whether a read converts a value:
// there, the type the declaration exports is lossless by definition. Here it
// is not, since a declaration can share its export type with a wider one: an
// EAV-only integer exports DOUBLE, like numeric, so a DOUBLE object under it
// can still hold 1001.5. A footer type settles a column only when every value
// of that type is admitted (judgeDomain); otherwise the object's values are
// scanned with the Phase 1 proof predicates, list elements included, bounded
// by the declaration's storage domain rather than by what the read converts:
// ±2^53 for a bigint kept as a float64 image, and finite values for numeric.
// Every object is also decoded in full (decodeObject), so a corrupt data page
// is found even under a column its footer settles: the baseline reads every
// live object once. An object whose footer or data cannot be read is a
// finding, and a glob whose listing fails is an error: a baseline never
// blesses what it could not see.
//
// A scan proves the domain, not the meaning: a reinterpretation that keeps
// the domain (bigint to date, both BIGINT) passes.
func AuditParquetDomain(
	ctx context.Context, db *sql.DB, paths []string, cache forma.SchemaAttributeCache,
) ([]DomainFinding, error) {
	duck := duckSQLPool{db: db}
	v := newParquetSchemaValidator()
	union, complete, err := v.Validate(ctx, duck, paths, nil)
	if err != nil {
		return nil, fmt.Errorf("read the parquet footers: %w", err)
	}
	var findings []DomainFinding
	for _, path := range union.unverified {
		findings = append(findings, DomainFinding{Path: path, Evidence: "its columns could not be read"})
	}
	if !complete && len(union.unverified) == 0 {
		return nil, errors.New("a parquet glob could not be listed, so the object set is unknown")
	}
	undecoded := make(map[string]bool)
	for _, f := range union.files {
		if err := decodeObject(ctx, duck, f.path); err != nil {
			if ctx.Err() != nil {
				return nil, fmt.Errorf("decode %s: %w", f.path, ctx.Err())
			}
			undecoded[f.path] = true
			findings = append(findings, DomainFinding{Path: f.path, Evidence: "its data could not be read: " + err.Error()})
		}
	}
	for _, name := range slices.Sorted(maps.Keys(cache)) {
		meta := cache[name]
		col := sqlgen.ParquetAttrColumn(name)
		for _, f := range union.files {
			fileType, ok := f.cols[col]
			if !ok || undecoded[f.path] {
				continue
			}
			finding, bad, err := auditColumn(ctx, v, duck, f.path, col, fileType, meta)
			if err != nil {
				return nil, err
			}
			if bad {
				finding.Attribute = name
				findings = append(findings, finding)
			}
		}
	}
	return findings, nil
}

// decodeObject reads every column of every row of path, so a corrupt data
// page fails it whichever column it is under. The aggregate is the result,
// so DuckDB can neither prune a column nor answer from the footer, as it
// answers count(*). *COLUMNS(*) names no alias an object column could shadow.
func decodeObject(ctx context.Context, duck DuckDBQueryExecutor, path string) error {
	rows, err := duck.Query(ctx, fmt.Sprintf("SELECT bit_xor(hash(*COLUMNS(*))) FROM read_parquet('%s')", sqlutil.EscapeLiteral(path)))
	if err != nil {
		return redact.Error(err)
	}
	defer func() { _ = rows.Close() }()
	if rows.Next() {
		var digest any
		if err := rows.Scan(&digest); err != nil {
			return fmt.Errorf("scan the digest of %s: %w", path, err)
		}
	}
	if err := rows.Err(); err != nil {
		return redact.Error(err)
	}
	return nil
}

func auditColumn(
	ctx context.Context, v *parquetSchemaValidator, duck DuckDBQueryExecutor, path, col, fileType string, meta forma.AttributeMetadata,
) (DomainFinding, bool, error) {
	finding := DomainFinding{Path: path, Declared: declaredLabel(meta), FileType: fileType}
	j := judgeDomain(quoteDuckIdent(col), fileType, meta)
	switch j.kind {
	case judgeLossless:
		return finding, false, nil
	case judgeIncompatible:
		finding.Evidence = typeEvidence(judgeIncompatible, fileType, sqlgen.DuckDBNullScanType(meta))
		return finding, true, nil
	}
	value, found, err := v.runProof(ctx, duck, path, nil, col, j)
	if err != nil {
		if ctx.Err() != nil {
			return finding, false, fmt.Errorf("scan column %s of %s: %w", col, path, ctx.Err())
		}
		finding.Evidence = "its values could not be read: " + err.Error()
		return finding, true, nil
	}
	if !found {
		return finding, false, nil
	}
	if meta.ValueType == forma.ValueTypeList {
		finding.Evidence = "it holds a list element that is not " + j.demand
		return finding, true, nil
	}
	finding.Evidence = valueEvidence(fileType, value, j.demand)
	return finding, true, nil
}

// judgeDomain judges whether every value an object column of fileType can
// hold is admitted by meta, or which values to scan for. Whole-number and
// numeric declarations judge by domain (judgeWhole, judgeDouble), never by
// the export type: a DOUBLE column under an EAV-only integer needs its
// values proven whole and in range. Other types admit exactly their own
// export type. A list judges its elements, scanned with list_filter.
func judgeDomain(col, fileType string, meta forma.AttributeMetadata) typeJudgement {
	fileType = strings.ToUpper(fileType)
	isList := strings.HasSuffix(fileType, "[]")
	if (meta.ValueType == forma.ValueTypeList) != isList {
		return typeJudgement{kind: judgeIncompatible}
	}
	if !isList {
		return judgeDomainScalar(col, fileType, meta)
	}
	// Lists are EAV-only, so an element carries EAV storage width.
	elem := forma.AttributeMetadata{ValueType: meta.EffectiveItemsType()}
	j := judgeDomainScalar("x", strings.TrimSuffix(fileType, "[]"), elem)
	if j.kind == judgeNeedsProof {
		j.violation = fmt.Sprintf("len(list_filter(%s, x -> x IS NOT NULL AND (%s))) > 0", col, j.violation)
	}
	return j
}

// judgeDomainScalar judges against the declaration's storage domain, which
// is narrower than what the read path converts (judgeScalar) in two places:
// storedRange for a bigint, and a finite proof for DOUBLE under numeric. A
// BIGINT under numeric stays lossless: D2 admits its rounding (judgeDouble).
func judgeDomainScalar(col, fileType string, meta forma.AttributeMetadata) typeJudgement {
	switch meta.ValueType {
	case forma.ValueTypeSmallInt, forma.ValueTypeInteger, forma.ValueTypeBigInt:
		return judgeWhole(col, fileType, storedRange(meta))
	case forma.ValueTypeNumeric:
		if fileType == "DOUBLE" {
			// The write path refuses NaN and the infinities
			// (transform.finiteForEAV), and the read transformer reports
			// one as a consistency error.
			return typeJudgement{kind: judgeNeedsProof, violation: fmt.Sprintf("NOT isfinite(%s)", col), demand: "a finite number"}
		}
		return judgeDouble(col, fileType)
	}
	if sqlgen.ScanTypesCompatible(fileType, sqlgen.DuckDBNullScanType(meta)) {
		return typeJudgement{kind: judgeLossless}
	}
	if meta.ValueType == forma.ValueTypeBool && fileType == "VARCHAR" {
		return roundTrip(col, "BOOLEAN", "a boolean")
	}
	return typeJudgement{kind: judgeIncompatible}
}

// storedRange is the whole-number domain meta's storage admits. Only a
// bigint differs from declaredRange: unless it is bound to a bigint_* column,
// its storage keeps the float64 image alone (eav_data, a double_* column; a
// list element is EAV), where the write funnel admits [-2^53, 2^53]
// (transform.checkBigintImageFit) and the PostgreSQL census bounds the same
// storage by the same range. declaredRange gives an EAV-only bigint the full
// int64 range because the read path converts any whole image in it.
func storedRange(meta forma.AttributeMetadata) intRange {
	if meta.ValueType != forma.ValueTypeBigInt {
		return declaredRange(meta)
	}
	if meta.ColumnBinding != nil && meta.ColumnBinding.ColumnType() == forma.MainColumnTypeBigint {
		return bigintRange
	}
	return exactDoubleRange
}

// duckSQLPool runs the audit's DuckDB statements on a database/sql pool,
// such as the CDC exporter's, whose connections carry the S3 session.
type duckSQLPool struct{ db *sql.DB }

func (e duckSQLPool) Query(ctx context.Context, query string, args ...any) (duckDBRowsIterator, error) {
	rows, err := e.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("duckdb query: %w", err)
	}
	return rows, nil
}
