package sqlgen

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lychee-technology/forma/internal/numutil"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma"
)

// ConvertPgMainValue converts a string value to the appropriate Go type based
// on attribute metadata. It is the canonical value converter for Postgres
// main-table predicates, shared by the dual-path generator and the hybrid
// condition builder. Numeric-family literals keep their own type via
// TryParseNumber: a literal denoting an integer in int64 range binds as exact
// int64 in every accepted spelling ("42", "42.0", "9.007199254740993e15" —
// #357), lossless for bigint beyond 2^53; genuinely fractional literals bind
// as float64.
func ConvertPgMainValue(valStr string, attr string, meta forma.AttributeMetadata) (any, error) {
	switch meta.ValueType {
	case forma.ValueTypeText, forma.ValueTypeUUID:
		return valStr, nil

	case forma.ValueTypeNumeric, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeSmallInt:
		switch v := numutil.TryParseNumber(valStr).(type) {
		case int64:
			return v, nil
		case float64:
			return v, nil
		default:
			return nil, forma.InvalidInputf("invalid numeric value for '%s': %s", attr, valStr)
		}

	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		parsedValue, err := parseDateValue(valStr, meta)
		if err != nil {
			return nil, fmt.Errorf("invalid date value for '%s': %w", attr, err)
		}
		return parsedValue, nil

	case forma.ValueTypeBool:
		return convertPgBoolValue(valStr, attr, meta)

	default:
		return nil, fmt.Errorf("unsupported value_type '%s' for attribute '%s'", meta.ValueType, attr)
	}
}

// convertPgBoolValue converts a boolean string value respecting column encoding.
func convertPgBoolValue(valStr string, attr string, meta forma.AttributeMetadata) (any, error) {
	parsedInt, err := strconv.Atoi(valStr)
	if err != nil {
		return nil, forma.InvalidInputf("invalid boolean value for '%s': %s", attr, valStr)
	}

	if meta.ColumnBinding == nil {
		// default to text "1"/"0"
		if parsedInt > 0 {
			return "1", nil
		}
		return "0", nil
	}

	switch meta.ColumnBinding.Encoding {
	case forma.MainColumnEncodingBoolInt:
		if parsedInt > 0 {
			return int64(1), nil
		}
		return int64(0), nil
	case forma.MainColumnEncodingBoolText:
		if parsedInt > 0 {
			return "1", nil
		}
		return "0", nil
	default:
		// default to text "1"/"0"
		if parsedInt > 0 {
			return "1", nil
		}
		return "0", nil
	}
}

// detectValueType infers the forma.ValueType from a string literal when no metadata is available.
func detectValueType(valStr string) forma.ValueType {
	if _, err := uuid.Parse(valStr); err == nil {
		return forma.ValueTypeUUID
	}
	if ls := strings.ToLower(valStr); ls == "true" || ls == "false" || ls == "1" || ls == "0" {
		return forma.ValueTypeBool
	}
	if _, err := strconv.ParseFloat(valStr, 64); err == nil {
		return forma.ValueTypeNumeric
	}
	if _, err := time.Parse(time.RFC3339Nano, valStr); err == nil {
		return forma.ValueTypeDateTime
	}
	if _, err := strconv.ParseInt(valStr, 10, 64); err == nil {
		return forma.ValueTypeNumeric
	}
	return forma.ValueTypeText
}

// parseDuckDBRawParam parses a string value into a typed Go value for DuckDB parameters.
func parseDuckDBRawParam(valStr string, attr string, valueType forma.ValueType) (any, error) {
	switch valueType {
	case forma.ValueTypeUUID:
		return valStr, nil

	case forma.ValueTypeBool:
		if b, e := strconv.ParseBool(strings.ToLower(valStr)); e == nil {
			return b, nil
		} else if valStr == "1" {
			return true, nil
		} else if valStr == "0" {
			return false, nil
		}
		return valStr, nil

	case forma.ValueTypeNumeric, forma.ValueTypeBigInt,
		forma.ValueTypeSmallInt, forma.ValueTypeInteger:
		// Integral literals in ANY accepted spelling — bare, decimal or
		// exponent — bind as exact int64 (#281, #357) via the same
		// TryParseNumber the two Postgres predicate paths and the batch
		// attr-value anchor use (#355), so every emitter agrees on the value.
		// A bigint predicate above 2^53 — legal column state since #205 —
		// compares exactly instead of riding float64 + %.15g. Fractional
		// literals keep float64, the numeric family's storage contract.
		// EAV-only bigints round at write (2^53 ceiling), so exact binds above
		// it miss on every tier alike — tier parity preserved.
		//
		// integer/smallint joined this arm in #355 purely to end the binder
		// divergence: it changes no query result. Since #384 their operand
		// cast is CAST(? AS BIGINT) (storage width, matching the BIGINT
		// EAV columns), so an operand beyond the declared 2^31/2^15 range
		// compares normally and matches nothing on both engines instead of
		// raising a DuckDB Conversion Error while Postgres answered.
		switch v := numutil.TryParseNumber(valStr).(type) {
		case int64:
			return v, nil
		case float64:
			return v, nil
		default:
			return nil, forma.InvalidInputf("invalid numeric literal for %s: %s", attr, valStr)
		}

	case forma.ValueTypeDate, forma.ValueTypeDateTime:
		// Epoch-ms int64: date columns in the federated CTEs are BIGINT (#200).
		if t, e := time.Parse(time.RFC3339Nano, valStr); e == nil {
			return t.UTC().UnixMilli(), nil
		} else if i, e := strconv.ParseInt(valStr, 10, 64); e == nil {
			return i, nil
		}
		return nil, forma.InvalidInputf("invalid date literal for %s: %s", attr, valStr)

	default:
		return valStr, nil
	}
}

// resolveMainTableColumn returns the column name prefixed with "m." for main table queries.
func resolveMainTableColumn(attr string, meta forma.AttributeMetadata) string {
	if meta.ColumnBinding != nil {
		return "m." + string(meta.ColumnBinding.ColumnName)
	}
	return "m." + attr
}

// resolveDuckDBColumn returns the column name the DuckClause must reference.
//
// The DuckClause (LOGICAL_WHERE_CLAUSE) is only ever applied against the DuckDB
// federated CTEs (s3_source / unified / visible), and those project every
// attribute by its ATTRIBUTE name — `COALESCE(hot_vals.age, m.integer_01) AS age`
// for column-bound attrs, `hot_vals.tag AS tag` for pure-EAV attrs. So the clause
// must use the attribute name, never the physical entity_main column
// (`integer_01`), which is not a column of those CTEs. Emitting the physical name
// referenced a nonexistent column on non-benchmark schemas — a binder error /
// empty result in production, previously masked in the benchmark by
// translateDuckClauseToBenchmark (#167). Contrast resolveMainTableColumn, which
// correctly uses the physical `m.<col>` because PgMainClause references the real
// entity_main table `m` joined in the pg_source CTE.
//
// Dotted attribute names fold through ParquetAttrColumn (#260): the CTEs
// project every attribute under its parquet column alias, because the same
// clause also runs against the parquet scan source, which exposes the physical
// (folded) columns. Since #255 that scan source may additionally project typed
// NULLs (BuildParquetScanSource) for attributes absent from EVERY file in the
// set — those aliases are folded names too, so the clause binds either way.
func resolveDuckDBColumn(attr string) string {
	return ParquetAttrColumn(attr)
}
