package publication

import (
	"context"
	"fmt"
	"time"

	"github.com/lychee-technology/forma/internal/sqlgen"
)

// Generation is one committed generation as the history records it.
type Generation struct {
	Generation   int64
	Epoch        int64
	Kind         Kind
	ManifestKey  string
	SupersededAt int64
}

// Garbage collection keys on superseded_at, the database time of the
// transaction that replaced a generation (docs/publication-protocol-v2.md,
// "Garbage collection"). grace is the reader scan deadline plus the
// publication transaction budget plus a clock margin: a reader that chose a
// generation in its snapshot has finished, or given up, before that much time
// has passed since the generation was superseded.
const (
	retainedSQL = `SELECT generation, epoch, kind, manifest_key, superseded_at FROM %s
	WHERE schema_id = $1 AND (superseded_at = 0 OR superseded_at > ` + sqlgen.ServerTimeMillisSQL + ` - $2)
	ORDER BY generation`
	expiredSQL = `SELECT generation, epoch, kind, manifest_key, superseded_at FROM %s
	WHERE schema_id = $1 AND superseded_at <> 0 AND superseded_at <= ` + sqlgen.ServerTimeMillisSQL + ` - $2
	ORDER BY generation`
	forgetSQL = `DELETE FROM %s
	WHERE schema_id = $1 AND generation = $2 AND superseded_at <> 0 AND superseded_at <= ` + sqlgen.ServerTimeMillisSQL + ` - $3`
)

// Retained answers the schema's generations a reader may still scan: the
// current one and every one superseded less than grace ago. Garbage
// collection deletes no object any of their manifests lists. It reads them
// after it lists the objects it considers deleting, so a generation that
// commits while it lists is in this answer.
func Retained(ctx context.Context, q Querier, t Tables, schemaID int16, grace time.Duration) ([]Generation, error) {
	return generations(ctx, q, fmt.Sprintf(retainedSQL, t.Generation), schemaID, grace)
}

// Expired answers the schema's generations superseded at least grace ago,
// whose manifests garbage collection may delete once no retained generation
// lists the objects they list.
func Expired(ctx context.Context, q Querier, t Tables, schemaID int16, grace time.Duration) ([]Generation, error) {
	return generations(ctx, q, fmt.Sprintf(expiredSQL, t.Generation), schemaID, grace)
}

// Forget deletes an expired generation's history row, the last step of its
// collection: the objects only it listed and its manifest are deleted first,
// so a collector that crashes repeats the deletes instead of leaking them. It
// answers false for a row that is gone or not expired.
func Forget(ctx context.Context, q Querier, t Tables, schemaID int16, generation int64, grace time.Duration) (bool, error) {
	res, err := q.ExecContext(ctx, fmt.Sprintf(forgetSQL, t.Generation), schemaID, generation, grace.Milliseconds())
	if err != nil {
		return false, fmt.Errorf("forget schema %d generation %d: %w", schemaID, generation, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("count forgotten schema %d generations: %w", schemaID, err)
	}
	return n == 1, nil
}

func generations(ctx context.Context, q Querier, query string, schemaID int16, grace time.Duration) ([]Generation, error) {
	rows, err := q.QueryContext(ctx, query, schemaID, grace.Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("read schema %d generations: %w", schemaID, err)
	}
	defer rows.Close()
	var out []Generation
	for rows.Next() {
		var g Generation
		var kind string
		if err := rows.Scan(&g.Generation, &g.Epoch, &kind, &g.ManifestKey, &g.SupersededAt); err != nil {
			return nil, fmt.Errorf("scan schema %d generation: %w", schemaID, err)
		}
		g.Kind = Kind(kind)
		out = append(out, g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read schema %d generations: %w", schemaID, err)
	}
	return out, nil
}
