package sqlgen

import (
	"fmt"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/conditionexpr"
	"github.com/lychee-technology/forma/internal/iso8601"
)

// parseDateValue parses a date filter literal (RFC3339 or unix millis) and
// converts it to the Go value bound against the attribute's storage. attr
// names the attribute in the refusal an iso8601 binding can raise.
func parseDateValue(attr, valStr string, meta forma.AttributeMetadata) (any, error) {
	ms, err := parseDateMillis(attr, valStr, meta)
	if err != nil {
		return nil, err
	}
	if meta.ColumnBinding != nil && meta.ColumnBinding.Encoding == forma.MainColumnEncodingISO8601 {
		// The text column holds the canonical image; the literal must be
		// that image or it compares against nothing the store ever wrote.
		return iso8601FilterImage(attr, valStr, ms, meta)
	}
	// unix_ms, the bigint default and the EAV value_numeric slot are all
	// exact epoch millis.
	return ms, nil
}

// parseDateMillis parses a date filter literal (RFC3339 or unix millis) to
// its epoch millis. On an iso8601 binding it refuses what the canonical
// image cannot hold (iso8601FilterImage), so a leaf that compares the
// column's epoch-ms read refuses the literals a text comparison refuses.
func parseDateMillis(attr, valStr string, meta forma.AttributeMetadata) (int64, error) {
	parsedTime, err := conditionexpr.ParseRFC3339OrUnixMs(valStr)
	if err != nil {
		return 0, err
	}
	ms := parsedTime.UnixMilli()
	if meta.ColumnBinding != nil && meta.ColumnBinding.Encoding == forma.MainColumnEncodingISO8601 {
		if _, err := iso8601FilterImage(attr, valStr, ms, meta); err != nil {
			return 0, err
		}
	}
	return ms, nil
}

// iso8601FilterImage renders a filter literal on an iso8601-bound column as
// the canonical stored image, through iso8601.Image, the same function the
// write funnel renders through, so an offset literal, a unix-ms literal and
// a Z literal naming one instant all bind one string, independent of the
// process zone (#588). Before this the literal was the RFC3339 rendering of
// the parsed time: the offset was kept, a unix-ms literal rendered in the
// server's zone, and a fraction was dropped, so the Postgres route compared
// a string the store never wrote while DuckDB compared the exact instant. A
// literal the image cannot hold (off a whole second, or outside the
// four-digit year) is refused as invalid input on both routes:
// normalizeDuckPayload applies the same function to its epoch-ms operand.
func iso8601FilterImage(attr, literal string, ms int64, meta forma.AttributeMetadata) (string, error) {
	image, rule := iso8601.Image(ms)
	if rule != "" {
		return "", forma.InvalidInputf("%s filter value %s for '%s' cannot be compared against main column %s with encoding %s, which %s",
			meta.ValueType, literal, attr, meta.ColumnBinding.ColumnName, forma.MainColumnEncodingISO8601, rule)
	}
	return image, nil
}

// SQLGenerator converts parsed conditions into SQL fragments and argument lists.
type SQLGenerator struct{}

// NewSQLGenerator constructs a SQLGenerator.
func NewSQLGenerator() *SQLGenerator {
	return &SQLGenerator{}
}

// pgEavTypedEmitter renders typed predicate leaves as EAV EXISTS subqueries
// with $N placeholders. All parsing and value conversion happened in the
// normalizer; the emitter only assigns placeholders and formats the shell.
type pgEavTypedEmitter struct {
	eavTable   string
	paramIndex *int
}

func (e *pgEavTypedEmitter) EmitTypedLeaf(leaf *PredicateLeaf) (string, []any, error) {
	p := leaf.PgEav
	if p.Err != nil {
		return "", nil, p.Err
	}

	next := func() string {
		*e.paramIndex++
		return fmt.Sprintf("$%d", *e.paramIndex)
	}
	return p.Render(e.eavTable, "e.schema_id", "e.row_id", next), p.Args(), nil
}

// pgEavClausesFromTree walks an already-normalized predicate tree for the
// EAV target, letting ToDualClauses share one normalization pass.
func pgEavClausesFromTree(tree PredicateNode, eavTable string, paramIndex *int) (string, []any, error) {
	emitter := &pgEavTypedEmitter{eavTable: eavTable, paramIndex: paramIndex}
	return walkPredicate(tree, pgEavStyle, nil, emitter)
}

// ToSQLClauses builds the SQL clause and arguments for a condition tree.
func (g *SQLGenerator) ToSQLClauses(
	condition forma.Condition,
	eavTable string,
	schemaID int16,
	cache forma.SchemaAttributeCache,
	paramIndex *int,
) (string, []any, error) {
	if condition == nil {
		return "", nil, nil
	}
	return pgEavClausesFromTree(normalizePredicates(condition, cache, targetPgEav), eavTable, paramIndex)
}

// ToSqlClauses is kept for backward compatibility.
func (g *SQLGenerator) ToSqlClauses(
	condition forma.Condition,
	eavTable string,
	schemaID int16,
	cache forma.SchemaAttributeCache,
	paramIndex *int,
) (string, []any, error) {
	return g.ToSQLClauses(condition, eavTable, schemaID, cache, paramIndex)
}
