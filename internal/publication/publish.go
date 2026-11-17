package publication

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lychee-technology/forma/internal/sqlgen"
)

// ErrRecordMoved reports a compare-and-set that found the record moved: another
// transition committed since the writer read it. The writer reads the record
// again and rebases (Rebase) or gives up; it never retries the same
// expectation blindly. Conflict also counts SQLSTATE 40001, which is how
// Aurora DSQL, and PostgreSQL at REPEATABLE READ, report the same race.
var ErrRecordMoved = errors.New("publication record moved")

// ErrStagingExpired reports a publication whose staging began longer ago than
// its budget allows. Garbage collection may already treat its unlisted objects
// as orphans, so it must not commit them (docs/publication-protocol-v2.md,
// "Garbage collection").
var ErrStagingExpired = errors.New("publication staging budget exceeded")

// Conflict reports whether err is a lost compare-and-set race, in either of
// the forms the engines answer it with.
func Conflict(err error) bool {
	var pgErr *pgconn.PgError
	return errors.Is(err, ErrRecordMoved) || (errors.As(err, &pgErr) && pgErr.Code == "40001")
}

// Next is the generation a writer stages and asks Publish to commit.
type Next struct {
	Kind Kind
	// Token is the generation's identity, minted once per staged generation
	// and reused by an idempotent retry of the same publication (Resolve).
	Token          uuid.UUID
	ManifestKey    string
	ManifestSHA256 string
	// Lineage is the current record's for a flush or a compaction of a
	// published schema, and a fresh one for cdc-init and for the first flush
	// of an unpublished schema.
	Lineage uuid.UUID
	// StagedAt is the database time the writer read before its first S3 write
	// for this generation; StagingBudget is how long after it the commit may
	// still land.
	StagedAt      int64
	StagingBudget time.Duration
}

// validate refuses a transition the protocol does not have, before any
// statement runs.
func (n Next) validate(cur Record) error {
	switch {
	case n.Token == uuid.Nil || n.Token == cur.Token:
		return fmt.Errorf("generation token %s must be fresh and differ from the record's %s", n.Token, cur.Token)
	case n.ManifestKey == "" || n.ManifestSHA256 == "":
		return errors.New("a generation names its manifest key and the manifest's sha256")
	case n.StagedAt <= 0 || n.StagingBudget <= 0:
		return fmt.Errorf("staging time %d and budget %s must both be positive", n.StagedAt, n.StagingBudget)
	case n.Lineage == uuid.Nil:
		return errors.New("a generation names a lineage")
	}
	switch n.Kind {
	case KindInit:
		if n.Lineage == cur.Lineage {
			return fmt.Errorf("cdc-init starts a new lineage, and %s is the record's", n.Lineage)
		}
	case KindCompact:
		if cur.State != Published {
			return fmt.Errorf("schema %d is %s: there is nothing to compact", cur.SchemaID, cur.State)
		}
		fallthrough
	case KindFlush:
		if cur.State == Published && n.Lineage != cur.Lineage {
			return fmt.Errorf("a %s keeps the record's lineage %s, not %s", n.Kind, cur.Lineage, n.Lineage)
		}
	default:
		return fmt.Errorf("unknown generation kind %q", n.Kind)
	}
	return nil
}

// casSQL is the compare-and-set: it moves the record from exactly the epoch,
// generation and token the writer read to the next generation, and only while
// the writer's staging is within its budget on the database clock.
const casSQL = `UPDATE %s SET state = '` + string(Published) + `',
		generation = generation + 1, parent_generation = generation,
		manifest_key = $5, manifest_sha256 = $6, lineage = $7, cas_token = $8,
		updated_at = ` + sqlgen.ServerTimeMillisSQL + `
	WHERE schema_id = $1 AND epoch = $2 AND generation = $3 AND cas_token = $4
	  AND $9 >= ` + sqlgen.ServerTimeMillisSQL + ` - $10
	RETURNING generation, updated_at`

// supersedeSQL stamps the generation a transition replaces. It matches no row
// when the record had no current generation (never published, or reset).
const supersedeSQL = `UPDATE %s SET superseded_at = ` + sqlgen.ServerTimeMillisSQL + `
	WHERE schema_id = $1 AND generation = $2 AND superseded_at = 0`

const insertGenerationSQL = `INSERT INTO %s (schema_id, generation, epoch, kind, parent_generation,
		manifest_key, manifest_sha256, lineage, cas_token, committed_at)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`

// Publish commits next on tx, a transaction the caller opened, as the
// generation after cur. The compare-and-set is its first statement, so a
// writer that lost the race learns it before it marks anything: the caller
// marks the generation's change_log entries on the same tx only after Publish
// returns, and commits once. Rolled back, the transaction leaves neither the
// pointer nor a mark. It modifies three rows besides the marks: the record,
// the replaced generation's row and the new one's.
func Publish(ctx context.Context, tx Querier, t Tables, cur Record, next Next) (Record, error) {
	if err := next.validate(cur); err != nil {
		return Record{}, fmt.Errorf("publish schema %d generation after %d: %w", cur.SchemaID, cur.Generation, err)
	}
	out := cur
	err := tx.QueryRowContext(ctx, fmt.Sprintf(casSQL, t.Record),
		cur.SchemaID, cur.Epoch, cur.Generation, cur.Token,
		next.ManifestKey, next.ManifestSHA256, next.Lineage, next.Token,
		next.StagedAt, next.StagingBudget.Milliseconds()).Scan(&out.Generation, &out.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, classifyMiss(ctx, tx, t, cur)
	}
	if err != nil {
		return Record{}, fmt.Errorf("compare-and-set schema %d from generation %d: %w", cur.SchemaID, cur.Generation, err)
	}
	if out.Generation != cur.Generation+1 {
		return Record{}, fmt.Errorf("compare-and-set schema %d from generation %d answered generation %d", cur.SchemaID, cur.Generation, out.Generation)
	}
	out.State, out.ParentGeneration = Published, cur.Generation
	out.ManifestKey, out.ManifestSHA256 = next.ManifestKey, next.ManifestSHA256
	out.Lineage, out.Token = next.Lineage, next.Token
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(supersedeSQL, t.Generation), cur.SchemaID, cur.Generation); err != nil {
		return Record{}, fmt.Errorf("supersede schema %d generation %d: %w", cur.SchemaID, cur.Generation, err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(insertGenerationSQL, t.Generation),
		out.SchemaID, out.Generation, out.Epoch, string(next.Kind), out.ParentGeneration,
		out.ManifestKey, out.ManifestSHA256, out.Lineage, out.Token, out.UpdatedAt); err != nil {
		return Record{}, fmt.Errorf("record schema %d generation %d: %w", out.SchemaID, out.Generation, err)
	}
	return out, nil
}

// classifyMiss tells a moved record from an expired staging budget after the
// compare-and-set matched no row, by reading the record on the same tx.
func classifyMiss(ctx context.Context, tx Querier, t Tables, cur Record) error {
	now, err := Read(ctx, tx, t, cur.SchemaID)
	if err != nil {
		return fmt.Errorf("classify a missed compare-and-set of schema %d: %w", cur.SchemaID, err)
	}
	if now.Epoch == cur.Epoch && now.Generation == cur.Generation && now.Token == cur.Token {
		return fmt.Errorf("publish schema %d generation %d: %w", cur.SchemaID, cur.Generation+1, ErrStagingExpired)
	}
	return fmt.Errorf("publish schema %d from epoch %d generation %d token %s, found epoch %d generation %d token %s: %w",
		cur.SchemaID, cur.Epoch, cur.Generation, cur.Token, now.Epoch, now.Generation, now.Token, ErrRecordMoved)
}
