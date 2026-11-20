package production

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/lychee-technology/forma/internal/publication"
)

// The harness provisions its databases as init-db does, but it cannot run
// init-db's registration: writing a publication record is a transition only
// init-db may make in production, and internal/publication's owners guard
// holds every non-test file of the module to that, this package's included.
// A test file may make it, so this one installs the harness's initializer.
func init() {
	initializePublicationRecords = initializeRecordsAsInitDB
}

// initializeRecordsAsInitDB writes the UNPUBLISHED record of each of refs in
// one transaction: what init-db's registration does for a schema with no
// parquet history (cmd/tools/init_db_records.go). A schema that already has a
// record fails, since a fresh test database has none.
func initializeRecordsAsInitDB(ctx context.Context, pool *pgxpool.Pool, entityMain string, refs []SchemaRef) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin the registration transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	tables := publication.TablesFor(entityMain)
	for _, ref := range refs {
		wrote, err := publication.Initialize(ctx, publication.PgxTx(tx), tables, ref.ID)
		if err != nil {
			return fmt.Errorf("initialize the record of schema %d (%s): %w", ref.ID, ref.Name, err)
		}
		if !wrote {
			return fmt.Errorf("schema %d (%s) already has a publication record", ref.ID, ref.Name)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit the publication records: %w", err)
	}
	return nil
}
