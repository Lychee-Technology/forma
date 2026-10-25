package federated

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/redact"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// CheckTypes is the #732 live-file type guard, run on the validator's union
// before the main scan. A declaration changed after its parquet was written
// (numeric → bigint over a DOUBLE column, #710) otherwise reaches the read as
// a cast of the unified column: a value the new type cannot hold either fails
// the statement with DuckDB's own conversion error or reads rounded (1000.5
// as 1000). The guard refuses such a read first, with a
// forma.ParquetTypeIncompatibleError naming the attribute, the object and
// the evidence, and never prunes the object or substitutes a value.
//
// Only columns the union reports as mixed or as a type the declaration does
// not export are judged — the same test that earns the #371 pin — so a
// healthy scan set costs nothing. Each object holding such a column is judged
// by typeJudgement. A judgement that needs proof scans the object's values
// once; a passing proof is cached under the object's manifest stamp like the
// footer cache, so a write-once object is scanned once per process while the
// entry lives.
//
// It returns the objects whose proof scan failed to read, each with its read
// error: the guard cannot judge them, and the caller fails the read closed on
// them. The error is kept because it can carry the request's expired budget,
// which must keep the failure non-degradable (#465). A refusal wins over
// unverified objects, because it is a verdict and they are not.
//
// What it cannot see: a redeclaration that keeps the exported type, such as
// numeric → integer on an EAV-only attribute (both write DOUBLE) or bigint →
// date (both BIGINT). Those need the declaration history Phase 2 (#733) adds.
func (v *parquetSchemaValidator) CheckTypes(
	ctx context.Context, duck DuckDBQueryExecutor, schemaID int16,
	cache forma.SchemaAttributeCache, union columnUnion, stamps map[string]map[string]string,
) ([]unverifiedProof, error) {
	if v == nil || duck == nil || union.types == nil {
		return nil, nil
	}
	var unverified []unverifiedProof
	for _, name := range slices.Sorted(maps.Keys(cache)) {
		meta := cache[name]
		col := sqlgen.ParquetAttrColumn(name)
		if !columnAtRisk(union, col, meta) {
			continue
		}
		viaDouble := unionHoldsType(union, col, doubleShape(meta))
		for _, f := range union.files {
			fileType, ok := f.cols[col]
			if !ok {
				continue
			}
			j := judgeColumn(quoteDuckIdent(col), fileType, meta, viaDouble)
			if j.kind == judgeLossless {
				continue
			}
			refusal := &forma.ParquetTypeIncompatibleError{
				SchemaID: schemaID, Attribute: name, Declared: declaredLabel(meta),
				Path: f.path, FileType: fileType,
			}
			if meta.ColumnBinding != nil {
				refusal.Column = meta.ColumnBinding.ColumnName
			}
			if j.kind != judgeNeedsProof {
				refusal.Evidence = typeEvidence(j.kind, fileType, sqlgen.DuckDBNullScanType(meta))
				return nil, refusal
			}
			value, found, err := v.prove(ctx, duck, f.path, currentStamp(stamps, f.path), col, j)
			if err != nil {
				unverified = append(unverified, unverifiedProof{path: f.path, err: err})
				if ctx.Err() != nil {
					// The request ended, not the object: every later
					// proof would fail the same way.
					return unverified, nil
				}
				continue
			}
			if found {
				refusal.Evidence = valueEvidence(fileType, value, j.demand)
				return nil, refusal
			}
		}
	}
	return unverified, nil
}

// unverifiedProof is an object whose value proof could not be read, and why.
type unverifiedProof struct {
	path string
	err  error
}

// columnAtRisk is the #371 pin's test: the column is present and either seen
// at more than one type or at a type the declaration does not export.
func columnAtRisk(union columnUnion, col string, meta forma.AttributeMetadata) bool {
	observed, present := union.types[col]
	return present && (union.isMixed(col) || !sqlgen.ScanTypesCompatible(observed, sqlgen.DuckDBNullScanType(meta)))
}

// doubleShape is the DOUBLE spelling for meta's shape: DOUBLE[] for a list.
func doubleShape(meta forma.AttributeMetadata) string {
	if meta.ValueType == forma.ValueTypeList {
		return "DOUBLE[]"
	}
	return "DOUBLE"
}

func unionHoldsType(union columnUnion, col, typ string) bool {
	for _, f := range union.files {
		if strings.EqualFold(f.cols[col], typ) {
			return true
		}
	}
	return false
}

func declaredLabel(meta forma.AttributeMetadata) string {
	if meta.ValueType == forma.ValueTypeList {
		return fmt.Sprintf("list<%s>", meta.EffectiveItemsType())
	}
	return string(meta.ValueType)
}

func typeEvidence(kind judgementKind, fileType, want string) string {
	if kind == judgeUnprovable {
		return fmt.Sprintf("reading %s as %s needs a value proof per list element, which this guard does not make, so the conversion is unverified", fileType, want)
	}
	return fmt.Sprintf("%s is a different type family from the %s the declaration reads, so reading it would change or lose values", fileType, want)
}

// valueEvidence names the offending value. A text value is not quoted: it is
// stored user data, and a number is the only image the operator needs to see
// to recognize the redeclaration (D1 names numeric images the same way).
func valueEvidence(fileType, value, demand string) string {
	if strings.EqualFold(fileType, "VARCHAR") {
		return "it holds a text value that is not " + demand
	}
	return fmt.Sprintf("it holds %s, which is not %s", value, demand)
}

// prove scans one object for the first value j's violation predicate
// accepts. A clean scan is cached; a violation is not, so a repaired object
// is re-judged on the next query.
//
// A clean proof reads the whole column — LIMIT 1 bounds the rows returned,
// not the bytes scanned — so concurrent misses on one (object, column,
// predicate, stamp) are single-flighted through the footer probe's inflight
// map, under a "proof" prefix no object key can start with. Followers behave
// as in probeColumns: on the leader's finish they re-read the cache and,
// finding no clean proof there (a violation, or a leader whose read failed),
// prove for themselves, so a refusal reaches every caller and a leader's
// expired budget does not fail a follower that has budget left. A follower
// whose own context ends while waiting reports that as its read error.
func (v *parquetSchemaValidator) prove(
	ctx context.Context, duck DuckDBQueryExecutor, path string, stamp map[string]string, col string, j typeJudgement,
) (string, bool, error) {
	key := proofKey(path, col, j)
	if v.lookupProof(key, stamp) {
		return "", false, nil
	}
	flight := probeKey("proof\x00"+key, stamp)
	done, leader := v.beginProbe(flight)
	if leader {
		defer v.finishProbe(flight)
		// A leader that finished between our lookup and our claim has
		// already published.
		if v.lookupProof(key, stamp) {
			return "", false, nil
		}
		return v.runProof(ctx, duck, path, stamp, col, j)
	}
	select {
	case <-done:
		if v.lookupProof(key, stamp) {
			return "", false, nil
		}
		return v.runProof(ctx, duck, path, stamp, col, j)
	case <-ctx.Done():
		return "", false, fmt.Errorf("wait for the proof of column %s in %s: %w", col, path, ctx.Err())
	}
}

// proofKey is a proof's cache key: the object, the column and the predicate.
func proofKey(path, col string, j typeJudgement) string {
	return strings.Join([]string{path, col, j.violation}, "\x00")
}

// runProof is the un-deduplicated proof scan.
func (v *parquetSchemaValidator) runProof(
	ctx context.Context, duck DuckDBQueryExecutor, path string, stamp map[string]string, col string, j typeJudgement,
) (string, bool, error) {
	quoted := quoteDuckIdent(col)
	rows, err := duck.Query(ctx, fmt.Sprintf(
		"SELECT CAST(%[1]s AS VARCHAR) FROM read_parquet('%[2]s') WHERE %[1]s IS NOT NULL AND (%[3]s) LIMIT 1",
		quoted, sqlutil.EscapeLiteral(path), j.violation))
	if err != nil {
		// Scrubbed at the source like a main-scan error (#306): the read
		// error now reaches the unverified-object error's text.
		return "", false, fmt.Errorf("prove column %s of %s: %w", col, path, redact.Error(err))
	}
	defer func() { _ = rows.Close() }()
	if rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return "", false, fmt.Errorf("scan offending value of column %s in %s: %w", col, path, err)
		}
		return value, true, nil
	}
	if err := rows.Err(); err != nil {
		return "", false, fmt.Errorf("prove column %s of %s: %w", col, path, redact.Error(err))
	}
	v.recordProof(proofKey(path, col, j), maps.Clone(stamp))
	return "", false, nil
}

func (v *parquetSchemaValidator) lookupProof(key string, stamp map[string]string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()
	_, ok := v.proved.get(key, stamp)
	return ok
}

func (v *parquetSchemaValidator) recordProof(key string, stamp map[string]string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.proved.put(key, map[string]string{}, stamp)
}

// quoteDuckIdent double-quotes a folded parquet column name for the proof
// scans, which interpolate it beside an arbitrary predicate.
func quoteDuckIdent(col string) string {
	return `"` + strings.ReplaceAll(col, `"`, `""`) + `"`
}
