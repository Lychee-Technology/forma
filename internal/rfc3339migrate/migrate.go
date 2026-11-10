// Package rfc3339migrate moves the images of the retired iso8601 encoding
// into the rfc3339 shape (#842). The iso8601 encoding stored a date/datetime
// as YYYY-MM-DDThh:mm:ssZ; rfc3339 stores YYYY-MM-DDThh:mm:ss.sssZ. The two
// shapes cannot share a column, since the PostgreSQL route compares the text
// and '.' sorts before 'Z', so every whole-second image is rewritten to
// <first 19 characters>.000Z while the schema's writers are stopped.
//
// The targets are read from the ledger, not from the attributes files: an
// active generation that records an attribute with the iso8601 encoding
// names a column holding such images, and registration refuses iso8601 in
// a declaration, so the files may already say rfc3339. The migrate-rfc3339
// tool rewrites through this package, and apply-schema counts what is left
// through it before it records iso8601 → rfc3339, so the two cannot disagree
// on what remains.
//
// The rewrite leaves change_log unstamped. The instant does not change, and
// a parquet copy holds the instant as epoch milliseconds, not the image
// (cdc.castDateMainValue), so no copy goes stale.
package rfc3339migrate

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/rfc3339"
	"github.com/lychee-technology/forma/internal/schemaledger"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// BatchSize bounds the rows one rewrite statement changes. Each statement is
// its own transaction, so this keeps it under Aurora DSQL's 3,000-row
// transaction limit (#692).
const BatchSize = 1000

// canonicalShapePattern matches the rfc3339 shape, YYYY-MM-DDThh:mm:ss.sssZ:
// an image already migrated, or written by the new release.
const canonicalShapePattern = `^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{3}Z$`

// Querier reads entity_main.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Execer rewrites entity_main.
type Execer interface {
	Querier
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Target is one column the migration rewrites: an attribute a schema's
// active generation records with the iso8601 encoding.
type Target struct {
	SchemaName string
	SchemaID   int16
	AttrID     int16
	AttrName   string
	Column     forma.MainColumn
}

// String names the target as every report and refusal does.
func (t Target) String() string {
	return fmt.Sprintf("schema %s (id %d) attribute %s (id %d) column %s", t.SchemaName, t.SchemaID, t.AttrName, t.AttrID, t.Column)
}

// TargetsOf answers the targets of a schema's active generation: every
// attribute it records with the iso8601 encoding, retired ones included. A
// retired attribute keeps its column (no other attribute may bind it), and
// an apply that declares it again judges its recorded binding, so its
// images are migrated too. Entries are in attribute-id order.
func TargetsOf(schemaName string, gen schemaledger.Generation) []Target {
	var targets []Target
	for _, e := range gen.Entries {
		if e.Encoding != forma.MainColumnEncodingLegacyISO8601 {
			continue
		}
		targets = append(targets, Target{SchemaName: schemaName, SchemaID: gen.SchemaID, AttrID: e.AttrID, AttrName: e.Name, Column: e.Column})
	}
	return targets
}

// column answers the target's column as an identifier, refusing one that is
// not a text column of entity_main: the name comes from the ledger and is
// spliced into SQL.
func (t Target) column() (string, error) {
	binding := forma.MainColumnBinding{ColumnName: t.Column}
	if !model.IsMainTableColumn(string(t.Column)) || binding.ColumnType() != forma.MainColumnTypeText {
		return "", fmt.Errorf("%s: the ledger records encoding iso8601 on %q, which is not a text column of entity_main", t, t.Column)
	}
	return sqlutil.SanitizeIdentifier(string(t.Column)), nil
}

// BuildCountQuery renders the count of a target's whole-second images.
func BuildCountQuery(entityMain string, t Target) (string, []any, error) {
	col, err := t.column()
	if err != nil {
		return "", nil, err
	}
	query := fmt.Sprintf(`SELECT count(*) FROM %s WHERE ltbase_schema_id = $1 AND %s ~ $2`,
		sqlutil.SanitizeIdentifier(entityMain), col)
	return query, []any{t.SchemaID, rfc3339.LegacyImagePattern}, nil
}

// BuildRewriteStatement renders one rewrite batch: at most limit of the
// target's whole-second images, each to its first 19 characters plus
// ".000Z". The outer predicate repeats the pattern, so a row a concurrent
// writer changed after the inner select is left alone.
func BuildRewriteStatement(entityMain string, t Target, limit int) (string, []any, error) {
	col, err := t.column()
	if err != nil {
		return "", nil, err
	}
	main := sqlutil.SanitizeIdentifier(entityMain)
	query := fmt.Sprintf(`UPDATE %[1]s SET %[2]s = left(%[2]s, 19) || '.000Z'
WHERE ltbase_schema_id = $1 AND %[2]s ~ $2 AND ltbase_row_id IN (
  SELECT ltbase_row_id FROM %[1]s WHERE ltbase_schema_id = $1 AND %[2]s ~ $2 ORDER BY ltbase_row_id LIMIT $3)`, main, col)
	return query, []any{t.SchemaID, rfc3339.LegacyImagePattern, limit}, nil
}

// BuildOthersQuery renders the listing of every other image in a target
// column: one of neither the whole-second shape nor the rfc3339 shape, such
// as an offset, a fraction of another length or an unparsable string. The
// migration leaves them unchanged; they are the #763 repair's.
func BuildOthersQuery(entityMain string, t Target) (string, []any, error) {
	col, err := t.column()
	if err != nil {
		return "", nil, err
	}
	query := fmt.Sprintf(`SELECT ltbase_row_id, %[2]s FROM %[1]s
WHERE ltbase_schema_id = $1 AND %[2]s IS NOT NULL AND %[2]s !~ $2 AND %[2]s !~ $3
ORDER BY ltbase_row_id`, sqlutil.SanitizeIdentifier(entityMain), col)
	return query, []any{t.SchemaID, rfc3339.LegacyImagePattern, canonicalShapePattern}, nil
}

// CountLegacy answers how many of a target's rows still hold a whole-second
// image.
func CountLegacy(ctx context.Context, q Querier, entityMain string, t Target) (int64, error) {
	query, args, err := BuildCountQuery(entityMain, t)
	if err != nil {
		return 0, err
	}
	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("count whole-second images of %s in %s: %w", t, entityMain, err)
	}
	n, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[int64])
	if err != nil {
		return 0, fmt.Errorf("count whole-second images of %s in %s: %w", t, entityMain, err)
	}
	return n, nil
}

// Rewrite rewrites every whole-second image of a target in statements of at
// most batch rows, each its own transaction, and answers how many it
// rewrote. A re-run finds nothing and rewrites nothing.
func Rewrite(ctx context.Context, e Execer, entityMain string, t Target, batch int) (int64, error) {
	if batch <= 0 || batch >= 3000 {
		return 0, fmt.Errorf("rewrite batch size %d is outside 1 to 2999 (Aurora DSQL's transaction row limit)", batch)
	}
	query, args, err := BuildRewriteStatement(entityMain, t, batch)
	if err != nil {
		return 0, err
	}
	var total int64
	for {
		tag, err := e.Exec(ctx, query, args...)
		if err != nil {
			return total, fmt.Errorf("rewrite whole-second images of %s in %s after %d row(s): %w", t, entityMain, total, err)
		}
		total += tag.RowsAffected()
		if tag.RowsAffected() == 0 {
			return total, nil
		}
	}
}

// Other is an image the migration leaves unchanged.
type Other struct {
	RowID uuid.UUID
	Image string
}

// Others lists a target's images of neither shape (BuildOthersQuery).
func Others(ctx context.Context, q Querier, entityMain string, t Target) ([]Other, error) {
	query, args, err := BuildOthersQuery(entityMain, t)
	if err != nil {
		return nil, err
	}
	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list the other images of %s in %s: %w", t, entityMain, err)
	}
	others, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Other, error) {
		var o Other
		err := row.Scan(&o.RowID, &o.Image)
		return o, err
	})
	if err != nil {
		return nil, fmt.Errorf("list the other images of %s in %s: %w", t, entityMain, err)
	}
	return others, nil
}
