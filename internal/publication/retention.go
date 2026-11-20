package publication

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// Generation is one committed generation as the history records it.
type Generation struct {
	Generation       int64
	Epoch            int64
	Kind             Kind
	ParentGeneration int64
	ManifestKey      string
	// ManifestSHA256 is the lowercase hex SHA-256 of the manifest object's
	// stored bytes, Lineage the generation's content line, and Token the
	// generation's cas_token, which the manifest names as its token.
	ManifestSHA256 string
	Lineage        uuid.UUID
	Token          uuid.UUID
	// SupersededAt is the start time of the transaction that replaced the
	// generation, 0 while it is current.
	SupersededAt int64
	// RetiredAt is the time a collector stamped after it saw the replacement
	// committed (Retire), 0 until then. Retention counts from it.
	RetiredAt int64
}

// Garbage collection keys on retired_at, a time no earlier than the commit of
// the transaction that replaced the generation (docs/publication-protocol-v2.md,
// "Garbage collection"). superseded_at cannot serve: it is the replacing
// transaction's start, and nothing bounds how long after its start that
// transaction commits, so a reader may choose the generation in a snapshot
// taken long after superseded_at. grace is the reader's pass deadline plus a
// clock margin, and every statement compares against it in full (GraceMillis).
const (
	generationColumns = `generation, epoch, kind, parent_generation, manifest_key, manifest_sha256, lineage, cas_token, superseded_at, retired_at`
	retainedSQL       = `SELECT ` + generationColumns + ` FROM %s
	WHERE schema_id = $1 AND (retired_at = 0 OR retired_at > ` + sqlgen.ServerTimeMillisSQL + ` - $2)
	ORDER BY generation`
	expiredSQL = `SELECT ` + generationColumns + ` FROM %s
	WHERE schema_id = $1 AND retired_at <> 0 AND retired_at <= ` + sqlgen.ServerTimeMillisSQL + ` - $2
	ORDER BY generation`
	forgetSQL = `DELETE FROM %s
	WHERE schema_id = $1 AND generation = $2 AND retired_at <> 0 AND retired_at <= ` + sqlgen.ServerTimeMillisSQL + ` - $3`
	advanceForgottenSQL = `UPDATE %s SET forgotten_generation = CASE
			WHEN COALESCE(forgotten_generation, 0) > $2 THEN forgotten_generation
			ELSE $2 END
	WHERE schema_id = $1`
	retireSQL = `UPDATE %s SET retired_at = ` + sqlgen.ServerTimeMillisSQL + `
	WHERE schema_id = $1 AND generation = $2 AND superseded_at <> 0 AND retired_at = 0`
	// raiseHorizonSQL writes the record even when the horizon does not move,
	// so that it always conflicts with a publication in flight.
	raiseHorizonSQL = `UPDATE %s SET gc_horizon = CASE
			WHEN gc_horizon > ` + sqlgen.ServerTimeMillisSQL + ` - $2 THEN gc_horizon
			ELSE ` + sqlgen.ServerTimeMillisSQL + ` - $2 END
	WHERE schema_id = $1
	RETURNING gc_horizon`
	// schemaIDsSQL reads every schema id present in the publication tables,
	// including dropped schemas and schemas whose record row was lost.
	schemaIDsSQL = `SELECT schema_id FROM %s
	UNION
	SELECT schema_id FROM %s
	UNION
	SELECT schema_id FROM %s
	ORDER BY schema_id`
)

// StampPrecision is how much later than the event it records a stamp of
// sqlgen.ServerTimeMillisSQL can be: floor(extract(epoch from now()) * 1000)
// truncates to the millisecond, so an event at 12.0007 s is stamped 12000 ms.
const StampPrecision = time.Millisecond

// ceilMillis converts a non-negative duration to milliseconds rounded up so
// sub-millisecond parts are never truncated away.
func ceilMillis(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	ms := int64(d / time.Millisecond)
	if d%time.Millisecond > 0 {
		ms++
	}
	return ms
}

// GraceMillis answers grace as the milliseconds a statement subtracts from the
// database time to find a stamp at least grace old. For any positive grace it
// returns ceil(grace) + StampPrecision (overflow-clamped): a truncated stamp s
// and truncated clock n satisfy s <= n - (ceil(grace) + 1) only once real
// elapsed time is strictly greater than ceil(grace) >= grace. Zero grace
// answers 0 so tests checking immediate expiry do not need sleeps.
func GraceMillis(grace time.Duration) int64 {
	if grace <= 0 {
		return 0
	}
	ms := ceilMillis(grace)
	pad := StampPrecision.Milliseconds()
	if ms > math.MaxInt64-pad {
		return math.MaxInt64
	}
	return ms + pad
}

// RaiseHorizon is a collector pass's first step: it raises the record's
// horizon to the database time minus orphanAge, never lowering it, and
// answers the horizon. Run it on a pool, so that it has committed when it
// returns. It writes the record row, so a publication in flight and the
// raise cannot both commit unordered: on PostgreSQL one waits for the other's
// row lock, and on Aurora DSQL or at REPEATABLE READ one fails with 40001.
// Every publication that commits after it has staging that began after the
// horizon, and every one that committed before it is in the history the
// collector reads next. It does not change the record's token, so a writer
// that loses a 40001 to it sends the same publication again.
func RaiseHorizon(ctx context.Context, q Querier, t Tables, schemaID int16, orphanAge time.Duration) (int64, error) {
	var horizon int64
	err := q.runner().queryRow(ctx, fmt.Sprintf(raiseHorizonSQL, t.Record), schemaID, ceilMillis(orphanAge)).Scan(&horizon)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("raise schema %d collector horizon: %w", schemaID, ErrNoRecord)
	}
	if err != nil {
		return 0, fmt.Errorf("raise schema %d collector horizon: %w", schemaID, err)
	}
	return horizon, nil
}

// Retire stamps a superseded generation's retired_at with the database time,
// once. Run it on a pool after the snapshot in which the collector saw the
// generation superseded has ended: that snapshot saw the replacement
// committed, so a transaction that begins after it stamps a time no earlier
// than that commit. It answers false for a generation that is current, gone
// or already retired.
func Retire(ctx context.Context, q Querier, t Tables, schemaID int16, generation int64) (bool, error) {
	n, err := q.runner().exec(ctx, fmt.Sprintf(retireSQL, t.Generation), schemaID, generation)
	if err != nil {
		return false, fmt.Errorf("retire schema %d generation %d: %w", schemaID, generation, err)
	}
	return n == 1, nil
}

// Retained answers the schema's generations a reader may still scan: the
// current one, every superseded one no collector has retired yet, and every
// one retired less than grace ago. Garbage collection deletes no object any
// of their manifests lists, nor their manifests.
func Retained(ctx context.Context, q Querier, t Tables, schemaID int16, grace time.Duration) ([]Generation, error) {
	return generations(ctx, q, fmt.Sprintf(retainedSQL, t.Generation), schemaID, grace)
}

// Expired answers the schema's generations retired at least grace ago, whose
// manifests garbage collection may delete once no retained generation lists
// the objects they list (PlanCollection).
func Expired(ctx context.Context, q Querier, t Tables, schemaID int16, grace time.Duration) ([]Generation, error) {
	return generations(ctx, q, fmt.Sprintf(expiredSQL, t.Generation), schemaID, grace)
}

// Forget deletes an expired generation's history row and advances the schema's
// durable forgotten_generation floor in the same transaction tx. The caller
// deletes the generation's exclusive objects and manifest first, then runs
// Forget on tx and commits it. Because forgotten_generation lives on the epoch
// table, Forget never contends with Publish on the record row and survives a
// lost record. It answers false for a row that is gone or not expired.
func Forget(ctx context.Context, tx Tx, t Tables, schemaID int16, generation int64, grace time.Duration) (bool, error) {
	r := tx.runner()
	n, err := r.exec(ctx, fmt.Sprintf(forgetSQL, t.Generation), schemaID, generation, GraceMillis(grace))
	if err != nil {
		return false, fmt.Errorf("forget schema %d generation %d: %w", schemaID, generation, err)
	}
	if n != 1 {
		return false, nil
	}
	updated, err := r.exec(ctx, fmt.Sprintf(advanceForgottenSQL, t.Epoch), schemaID, generation)
	if err != nil {
		return false, fmt.Errorf("advance forgotten floor for schema %d generation %d: %w", schemaID, generation, err)
	}
	if updated != 1 {
		return false, fmt.Errorf("advance forgotten floor for schema %d generation %d: lifecycle row missing in %s: %w",
			schemaID, generation, t.Epoch, ErrLifecycleLost)
	}
	return true, nil
}

// SchemaIDs answers every schema id present in the record, generation history,
// or lifecycle epoch tables, in ascending order. Including all three ensures a
// collector visits dropped schemas and schemas whose record row was lost.
func SchemaIDs(ctx context.Context, q Querier, t Tables) ([]int16, error) {
	rows, err := q.runner().query(ctx, fmt.Sprintf(schemaIDsSQL, t.Record, t.Generation, t.Epoch))
	if err != nil {
		return nil, fmt.Errorf("read schema ids from publication tables: %w", err)
	}
	defer rows.Close()
	var ids []int16
	for rows.Next() {
		var id int16
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan publication schema id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read schema ids from publication tables: %w", err)
	}
	return ids, nil
}

func generations(ctx context.Context, q Querier, query string, schemaID int16, grace time.Duration) ([]Generation, error) {
	rows, err := q.runner().query(ctx, query, schemaID, GraceMillis(grace))
	if err != nil {
		return nil, fmt.Errorf("read schema %d generations: %w", schemaID, err)
	}
	defer rows.Close()
	var out []Generation
	for rows.Next() {
		var g Generation
		var kind string
		if err := rows.Scan(&g.Generation, &g.Epoch, &kind, &g.ParentGeneration, &g.ManifestKey, &g.ManifestSHA256, &g.Lineage, &g.Token, &g.SupersededAt, &g.RetiredAt); err != nil {
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
