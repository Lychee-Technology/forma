package schemaledger

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lychee-technology/forma"
)

// Querier reads the ledger: a pool, a connection or a transaction.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Execer writes the ledger. Record is given a transaction, so the pointer
// move and the generation's rows commit together.
type Execer interface {
	Querier
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// ErrGenerationConflict reports a compare-and-set that lost: another
// control-plane run moved the active generation first. Nothing is recorded;
// judge the declaration again against the generation now active.
var ErrGenerationConflict = errors.New("the active generation moved concurrently")

const (
	sqlstateUndefinedColumn = "42703"
	sqlstateUndefinedTable  = "42P01"
	sqlstateUniqueViolation = "23505"
)

// upgradeHint is the remediation for a registry the ledger DDL has not
// reached: a deployment upgrading to the enforcing release.
const upgradeHint = "add the ledger (init-db on PostgreSQL; on DSQL the manual step in docs/schema-evolution.md), " +
	"then run baseline-schema before the first start"

// wrapReadError names the upgrade step when the ledger's column or tables
// do not exist yet.
func wrapReadError(err error, registry string) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && (pgErr.Code == sqlstateUndefinedColumn || pgErr.Code == sqlstateUndefinedTable) {
		return fmt.Errorf("schema registry table %s has no generation ledger: %s: %w", registry, upgradeHint, err)
	}
	return fmt.Errorf("failed to read the generation ledger of schema registry table %s: %w", registry, err)
}

// ReadPointer answers schemaID's active generation, and false when it has
// none (no baseline). An unregistered schema is an error.
func ReadPointer(ctx context.Context, q Querier, registry string, schemaID int16) (int64, bool, error) {
	t := TablesFor(registry)
	rows, err := q.Query(ctx, fmt.Sprintf("SELECT %s FROM %s WHERE schema_id = $1", PointerColumn, t.Registry), schemaID)
	if err != nil {
		return 0, false, wrapReadError(err, registry)
	}
	pointer, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[*int64])
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, fmt.Errorf("schema id %d is not registered in schema registry table %s", schemaID, registry)
	}
	if err != nil {
		return 0, false, wrapReadError(err, registry)
	}
	if pointer == nil {
		return 0, false, nil
	}
	return *pointer, true, nil
}

// ReadActive answers schemaID's active generation with its entries, and
// false when it has none.
func ReadActive(ctx context.Context, q Querier, registry string, schemaID int16) (Generation, bool, error) {
	number, ok, err := ReadPointer(ctx, q, registry, schemaID)
	if err != nil || !ok {
		return Generation{}, ok, err
	}
	t := TablesFor(registry)
	rows, err := q.Query(ctx, fmt.Sprintf("SELECT content_hash FROM %s WHERE schema_id = $1 AND generation = $2", t.Generation), schemaID, number)
	if err != nil {
		return Generation{}, false, wrapReadError(err, registry)
	}
	hash, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[string])
	if errors.Is(err, pgx.ErrNoRows) {
		return Generation{}, false, fmt.Errorf("schema id %d names active generation %d, which ledger table %s does not hold", schemaID, number, t.Generation)
	}
	if err != nil {
		return Generation{}, false, wrapReadError(err, registry)
	}
	entries, err := readEntries(ctx, q, t, schemaID, number)
	if err != nil {
		return Generation{}, false, wrapReadError(err, registry)
	}
	return Generation{SchemaID: schemaID, Number: number, Hash: hash, Entries: entries}, true, nil
}

const entryColumns = "attr_id, attr_name, value_type, items_type, column_name, encoding, retired"

func readEntries(ctx context.Context, q Querier, t Tables, schemaID int16, number int64) ([]Entry, error) {
	query := fmt.Sprintf("SELECT %s FROM %s WHERE schema_id = $1 AND generation = $2 ORDER BY attr_id", entryColumns, t.Attribute)
	rows, err := q.Query(ctx, query, schemaID, number)
	if err != nil {
		return nil, fmt.Errorf("failed to query generation %d of schema id %d: %w", number, schemaID, err)
	}
	entries, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Entry, error) {
		var e Entry
		var valueType, itemsType, column, encoding string
		if err := row.Scan(&e.AttrID, &e.Name, &valueType, &itemsType, &column, &encoding, &e.Retired); err != nil {
			return Entry{}, fmt.Errorf("failed to scan ledger entry: %w", err)
		}
		e.ValueType, e.ItemsType = forma.ValueType(valueType), forma.ValueType(itemsType)
		e.Column, e.Encoding = forma.MainColumn(column), forma.MainColumnEncoding(encoding)
		return e, nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read generation %d of schema id %d: %w", number, schemaID, err)
	}
	return entries, nil
}

// Record is one generation to record.
type Record struct {
	SchemaID int16
	// From is the active generation the record was judged against, and 0
	// when the schema has none: the compare-and-set expects it.
	From       int64
	Hash       string
	Entries    []Entry
	RecordedBy string
	Warnings   []string
}

// Number is the generation the record becomes.
func (r Record) Number() int64 { return r.From + 1 }

// Write records rec and moves the schema's active pointer to it, provided
// the pointer still names rec.From: it answers ErrGenerationConflict when it
// does not. The pointer moves first, so a concurrent run on PostgreSQL waits
// on its row lock and then finds the pointer moved; on DSQL the loser fails
// at commit instead. The caller commits tx.
func Write(ctx context.Context, tx Execer, registry string, rec Record) error {
	t := TablesFor(registry)
	if err := movePointer(ctx, tx, t, rec); err != nil {
		return err
	}
	insertGeneration := fmt.Sprintf(`INSERT INTO %s (schema_id, generation, content_hash, recorded_by, recorded_at, warnings)
		VALUES ($1, $2, $3, $4, (EXTRACT(EPOCH FROM NOW()) * 1000)::BIGINT, $5)`, t.Generation)
	_, err := tx.Exec(ctx, insertGeneration, rec.SchemaID, rec.Number(), rec.Hash, rec.RecordedBy, strings.Join(rec.Warnings, "\n"))
	if err != nil {
		return writeError(err, rec, "record generation")
	}
	if len(rec.Entries) == 0 {
		return nil
	}
	query, args := insertEntries(t, rec)
	if _, err := tx.Exec(ctx, query, args...); err != nil {
		return writeError(err, rec, "record the attributes of generation")
	}
	return nil
}

func movePointer(ctx context.Context, tx Execer, t Tables, rec Record) error {
	update := fmt.Sprintf("UPDATE %s SET %s = $2 WHERE schema_id = $1 AND %s = $3", t.Registry, PointerColumn, PointerColumn)
	args := []any{rec.SchemaID, rec.Number(), rec.From}
	if rec.From == 0 {
		update = fmt.Sprintf("UPDATE %s SET %s = $2 WHERE schema_id = $1 AND %s IS NULL", t.Registry, PointerColumn, PointerColumn)
		args = args[:2]
	}
	tag, err := tx.Exec(ctx, update, args...)
	if err != nil {
		return writeError(err, rec, "move the active pointer to generation")
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("schema id %d: generation %d expected active generation %s: %w",
			rec.SchemaID, rec.Number(), fromLabel(rec.From), ErrGenerationConflict)
	}
	return nil
}

func insertEntries(t Tables, rec Record) (string, []any) {
	const perRow = 9 // schema_id, generation and entryColumns
	values := make([]string, 0, len(rec.Entries))
	args := make([]any, 0, len(rec.Entries)*perRow)
	for i, e := range rec.Entries {
		base := i * perRow
		placeholders := make([]string, perRow)
		for j := range placeholders {
			placeholders[j] = fmt.Sprintf("$%d", base+j+1)
		}
		values = append(values, "("+strings.Join(placeholders, ", ")+")")
		args = append(args, rec.SchemaID, rec.Number(), e.AttrID, e.Name, string(e.ValueType), string(e.ItemsType),
			string(e.Column), string(e.Encoding), e.Retired)
	}
	query := fmt.Sprintf("INSERT INTO %s (schema_id, generation, %s) VALUES %s", t.Attribute, entryColumns, strings.Join(values, ", "))
	return query, args
}

// writeError maps a unique violation, a concurrent run recording the same
// generation number, to ErrGenerationConflict.
func writeError(err error, rec Record, what string) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == sqlstateUniqueViolation {
		return fmt.Errorf("schema id %d: generation %d is already recorded: %w (%w)", rec.SchemaID, rec.Number(), ErrGenerationConflict, err)
	}
	return fmt.Errorf("failed to %s %d of schema id %d: %w", what, rec.Number(), rec.SchemaID, err)
}

func fromLabel(from int64) string {
	if from == 0 {
		return "none"
	}
	return fmt.Sprintf("%d", from)
}
