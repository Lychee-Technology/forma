package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma/internal/publication"
	"github.com/lychee-technology/forma/internal/schemaledger"
	"github.com/lychee-technology/forma/internal/sqlgen"
	"github.com/lychee-technology/forma/internal/sqlutil"
)

// record reads schema id's publication record; ok is false when it has none.
func (tg *initDBTarget) record(t *testing.T, id int16) (publication.Record, bool) {
	t.Helper()
	ctx := context.Background()
	var rec publication.Record
	found := true
	err := pgx.BeginFunc(ctx, tg.pool, func(tx pgx.Tx) error {
		s, err := publication.Read(ctx, publication.PgxTx(tx), publication.TablesFor(tg.opts.entityMain), id)
		if errors.Is(err, publication.ErrNoRecord) {
			found = false
			return nil
		}
		rec = s.Record
		return err
	})
	if err != nil {
		t.Fatalf("read schema %d's record: %v", id, err)
	}
	return rec, found
}

// publishInit commits an init generation of schema id over its record, as a
// cdc-init does (no rows to mark), and answers the record it committed.
func (tg *initDBTarget) publishInit(t *testing.T, id int16) publication.Record {
	t.Helper()
	ctx := context.Background()
	cur, ok := tg.record(t, id)
	if !ok {
		t.Fatalf("schema %d has no record to publish over", id)
	}
	var stagedAt int64
	if err := tg.pool.QueryRow(ctx, "SELECT "+sqlgen.ServerTimeMillisSQL).Scan(&stagedAt); err != nil {
		t.Fatalf("read the server time: %v", err)
	}
	token := uuid.New()
	next := publication.Next{
		Kind: publication.KindInit, Token: token, Lineage: uuid.New(),
		ManifestKey:    fmt.Sprintf("data/%d/_manifests/e%d/g%020d-%s.json", id, cur.Epoch, cur.Generation+1, token),
		ManifestSHA256: fmt.Sprintf("%064x", cur.Generation+1),
		StagedAt:       stagedAt, StagingBudget: time.Minute,
	}
	var head publication.Record
	err := pgx.BeginFunc(ctx, tg.pool, func(tx pgx.Tx) error {
		var err error
		head, err = publication.Publish(ctx, publication.PgxTx(tx), publication.TablesFor(tg.opts.entityMain), cur, next)
		return err
	})
	if err != nil {
		t.Fatalf("publish an init generation of schema %d: %v", id, err)
	}
	return head
}

// removeRegistration deletes schema id's registry row and ledger by hand,
// without the drop transition: what an operator who deleted "only the
// registry row" leaves behind.
func (tg *initDBTarget) removeRegistration(t *testing.T, id int16) {
	t.Helper()
	for _, table := range []string{
		sqlutil.SuffixedIdentifier(tg.opts.schemaTable, schemaledger.AttributeTableSuffix),
		sqlutil.SuffixedIdentifier(tg.opts.schemaTable, schemaledger.GenerationTableSuffix),
		tg.opts.schemaTable,
	} {
		if _, err := tg.pool.Exec(context.Background(), "DELETE FROM "+quoteIdentifier(table)+" WHERE schema_id = $1", id); err != nil {
			t.Fatalf("delete schema %d from %s: %v", id, table, err)
		}
	}
}

// removeRecord deletes schema id's record and nothing else: its generation
// history survives.
func (tg *initDBTarget) removeRecord(t *testing.T, id int16) {
	t.Helper()
	tables := publication.TablesFor(tg.opts.entityMain)
	if _, err := tg.pool.Exec(context.Background(), "DELETE FROM "+tables.Record+" WHERE schema_id = $1", id); err != nil {
		t.Fatalf("delete schema %d's record: %v", id, err)
	}
}

// rowsOf counts schema id's rows in entity_main and change_log.
func (tg *initDBTarget) rowsOf(t *testing.T, id int16) int {
	t.Helper()
	var n int
	err := tg.pool.QueryRow(context.Background(), fmt.Sprintf(
		"SELECT (SELECT COUNT(*) FROM %s WHERE ltbase_schema_id = $1) + (SELECT COUNT(*) FROM %s WHERE schema_id = $1)",
		quoteIdentifier(tg.opts.entityMain), quoteIdentifier(tg.opts.changeLog)), id).Scan(&n)
	if err != nil {
		t.Fatalf("count schema %d's rows: %v", id, err)
	}
	return n
}

// drop runs drop-schema on the target, purging two rows per statement.
func (tg *initDBTarget) drop(t *testing.T, target dropTarget) (string, error) {
	t.Helper()
	var out bytes.Buffer
	err := dropSchema(context.Background(), tg.pool, tg.opts, target, 2, &out)
	return out.String(), err
}

// A registration removed without the drop transition leaves its record
// behind. When init-db registers the id again it refuses to inherit that
// record, its epoch and its published generation: the run fails and the new
// registry row rolls back. drop-schema then retires the record and purges
// the old rows, and the next init-db activates the id at a new epoch, with
// the generation counter kept so no number is used again.
func TestInitDBRefusesARegistrationThatWouldInheritARecord(t *testing.T) {
	tg := newInitDBTarget(t)
	tg.writeOutboxAndTimer(t)
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db: %v\n%s", err, out)
	}
	published := tg.publishInit(t, 101)
	tg.insertRow(t, 101, 0)
	tg.insertRow(t, 101, 0)
	tg.insertRow(t, 101, -1)
	tg.removeRegistration(t, 101)

	out, err := tg.run(t)
	if !errors.Is(err, errRegistrationInheritsRecord) {
		t.Fatalf("init-db re-registered schema 101 over its old record: %v\n%s", err, out)
	}
	if !strings.Contains(err.Error(), "drop-schema --schema-id 101") {
		t.Errorf("the refusal does not name the remedy: %v", err)
	}
	if _, ok := tg.records(t)["timer"]; ok {
		t.Fatalf("the refused registration of timer committed")
	}
	if rec, _ := tg.record(t, 101); fmt.Sprint(rec) != fmt.Sprint(published) {
		t.Fatalf("the refused run changed the record:\nbefore %+v\n after %+v", published, rec)
	}

	out, err = tg.drop(t, dropTarget{id: 101})
	if err != nil {
		t.Fatalf("drop-schema --schema-id 101: %v\n%s", err, out)
	}
	if rec, _ := tg.record(t, 101); rec.State != publication.Dropped || rec.Epoch != 2 || rec.Generation != published.Generation {
		t.Fatalf("drop-schema left record %+v, want DROPPED at epoch 2 keeping generation %d", rec, published.Generation)
	}
	if n := tg.rowsOf(t, 101); n != 0 {
		t.Fatalf("drop-schema left %d rows of schema 101", n)
	}

	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db after drop-schema: %v\n%s", err, out)
	}
	timer := tg.records(t)["timer"]
	if timer.SchemaID != 101 || timer.State != publication.Unpublished || timer.Epoch != 3 || timer.Generation != published.Generation {
		t.Fatalf("re-registered timer has record %+v, want schema 101 UNPUBLISHED at epoch 3, counter %d", timer, published.Generation)
	}
	if next := tg.publishInit(t, 101); next.Generation != published.Generation+1 {
		t.Fatalf("the re-registered schema published generation %d, want %d", next.Generation, published.Generation+1)
	}
}

// drop-schema by name retires the registry row, the ledger and the record in
// one transaction and purges the rows in chunks. A rerun after an interrupted
// purge resumes it, and a purge never deletes anything once the id was
// registered again.
func TestDropSchemaRetiresPurgesAndResumes(t *testing.T) {
	tg := newInitDBTarget(t)
	tg.writeOutboxAndTimer(t)
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db: %v\n%s", err, out)
	}
	for range 5 {
		tg.insertRow(t, 101, 0)
	}
	tg.insertRow(t, 100, 0)

	out, err := tg.drop(t, dropTarget{name: "timer"})
	if err != nil {
		t.Fatalf("drop-schema --schema-name timer: %v\n%s", err, out)
	}
	if _, ok := tg.records(t)["timer"]; ok {
		t.Fatalf("timer is still registered")
	}
	dropped, _ := tg.record(t, 101)
	if dropped.State != publication.Dropped || dropped.Epoch != 2 {
		t.Fatalf("timer's record is %+v, want DROPPED at epoch 2", dropped)
	}
	if n := tg.rowsOf(t, 101); n != 0 {
		t.Fatalf("drop-schema left %d rows of schema 101", n)
	}
	if n := tg.rowsOf(t, 100); n != 2 {
		t.Fatalf("drop-schema purged another schema's rows: schema 100 has %d, want 2", n)
	}

	// An interrupted purge: rows written back while the record is DROPPED.
	tg.insertRow(t, 101, 0)
	out, err = tg.drop(t, dropTarget{id: 101})
	if err != nil || !strings.Contains(out, "dropped already at epoch 2") {
		t.Fatalf("a rerun does not resume the purge: %v\n%s", err, out)
	}
	if rec, _ := tg.record(t, 101); rec.Epoch != 2 || tg.rowsOf(t, 101) != 0 {
		t.Fatalf("the resumed drop moved the record to %+v or left rows", rec)
	}
	if _, err := tg.drop(t, dropTarget{name: "timer"}); err == nil || !strings.Contains(err.Error(), "--schema-id") {
		t.Fatalf("a drop by a name no longer registered must point to --schema-id: %v", err)
	}

	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db re-registers the dropped id: %v\n%s", err, out)
	}
	// timer sorts last, so init-db registers it again under its old id 101.
	if rec, _ := tg.record(t, 101); rec.State != publication.Unpublished || rec.Epoch != 3 {
		t.Fatalf("the re-registered id has record %+v, want UNPUBLISHED at epoch 3", rec)
	}
	tg.insertRow(t, 101, 0)
	n, err := purgeDroppedRows(context.Background(), tg.pool, tg.opts, dropped,
		publication.Residue{Table: tg.opts.changeLog}, 2)
	if !errors.Is(err, publication.ErrRecordMoved) || n != 0 {
		t.Fatalf("a purge of the dropped epoch after re-registration deleted %d rows: %v", n, err)
	}
	if got := tg.rowsOf(t, 101); got != 2 {
		t.Fatalf("the re-registered schema has %d rows, want its 2", got)
	}
}

// A V2 record lost while its generation history survives is not an
// untouched V1 schema: a record at epoch 1 and generation 0 would publish
// generation 1 again. init-db recovers it, under the schema lock, at the
// epoch after the history's with the history's highest generation as its
// counter: AWAITING_INIT for a schema still registered, and DROPPED then
// activated for an id the run registers again.
func TestInitDBRecoversALostRecordWithoutReusingItsHistory(t *testing.T) {
	tg := newInitDBTarget(t)
	tg.writeOutboxAndTimer(t)
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db: %v\n%s", err, out)
	}
	outbox := tg.publishInit(t, 100)
	timer := tg.publishInit(t, 101)
	timer = tg.publishInit(t, 101)

	tg.removeRecord(t, 100)
	out, err := tg.run(t)
	if err != nil {
		t.Fatalf("init-db after the record of schema 100 was lost: %v\n%s", err, out)
	}
	rec, _ := tg.record(t, 100)
	if rec.State != publication.AwaitingInit || rec.Epoch != outbox.Epoch+1 || rec.Generation != outbox.Generation {
		t.Fatalf("recovered record %+v, want AWAITING_INIT at epoch %d with counter %d", rec, outbox.Epoch+1, outbox.Generation)
	}
	if !strings.Contains(out, "Publication record of schema 100: recovered AWAITING_INIT at epoch 2") {
		t.Errorf("init-db does not report the recovery:\n%s", out)
	}
	if next := tg.publishInit(t, 100); next.Generation != outbox.Generation+1 || next.Epoch != 2 {
		t.Fatalf("the recovered schema published generation %d at epoch %d, want %d at 2", next.Generation, next.Epoch, outbox.Generation+1)
	}

	// The id's registration and record are both gone; its history is not.
	tg.removeRegistration(t, 101)
	tg.removeRecord(t, 101)
	tg.insertRow(t, 101, 0)
	if out, err := tg.run(t); !errors.Is(err, publication.ErrResidue) {
		t.Fatalf("init-db re-registered schema 101 over a row of its lost registration: %v\n%s", err, out)
	}
	if _, ok := tg.record(t, 101); ok {
		t.Fatalf("the refused registration committed a record")
	}
	tg.exec(t, `DELETE FROM %[1]s WHERE ltbase_schema_id = 101`)
	tg.exec(t, `DELETE FROM %[2]s WHERE schema_id = 101`)
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db re-registers schema 101: %v\n%s", err, out)
	}
	rec, _ = tg.record(t, 101)
	if rec.State != publication.Unpublished || rec.Epoch != timer.Epoch+2 || rec.Generation != timer.Generation {
		t.Fatalf("re-registered record %+v, want UNPUBLISHED at epoch %d with counter %d", rec, timer.Epoch+2, timer.Generation)
	}
}
