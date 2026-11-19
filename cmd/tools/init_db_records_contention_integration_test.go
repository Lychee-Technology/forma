package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lychee-technology/forma/internal/publication"
)

// lockHolder is a session holding schema advisory locks, as cdc-flush and
// cdc-init hold theirs, or as any other session can hold the same key.
type lockHolder struct{ conn *pgxpool.Conn }

func holdSchemaLocks(t *testing.T, tg *initDBTarget, ids ...int16) *lockHolder {
	t.Helper()
	conn, err := tg.pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	t.Cleanup(conn.Release)
	h := &lockHolder{conn: conn}
	for _, id := range ids {
		if _, err := conn.Exec(context.Background(), "SELECT pg_advisory_lock($1, $1)", int32(id)); err != nil {
			t.Fatalf("take schema %d's lock: %v", id, err)
		}
	}
	return h
}

func (h *lockHolder) release(t *testing.T, id int16) {
	t.Helper()
	if _, err := h.conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1, $1)", int32(id)); err != nil {
		t.Fatalf("release schema %d's lock: %v", id, err)
	}
}

// Under contention a registration commits with its record or not at all,
// while a schema registered before keeps the cutover's skip-and-retry.
// outbox_message (100) and timer (101) were registered before the V2
// cutover, so they have no record; orders is a new schema file, which this
// run registers as 102.
//
//   - With 102's lock held, init-db fails and commits nothing: no registry
//     row for orders, and no record for anyone, since the run is one
//     transaction.
//   - With only 100's lock held, init-db registers orders with its
//     UNPUBLISHED record, gives timer its record, and skips outbox_message,
//     which a later run initializes.
func TestInitDBRegistrationCommitsWithItsRecordOrNotAtAll(t *testing.T) {
	tg := newInitDBTarget(t)
	tg.writeOutboxAndTimer(t)
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db: %v\n%s", err, out)
	}
	if _, err := tg.pool.Exec(context.Background(), "DELETE FROM "+publication.TablesFor(tg.opts.entityMain).Record); err != nil {
		t.Fatalf("empty the record table: %v", err)
	}
	tg.writeSchema(t, "orders", map[string]string{"title": "text_01"}, nil)
	const ordersID int16 = 102
	holder := holdSchemaLocks(t, tg, 100, ordersID)

	out, err := tg.run(t)
	if !errors.Is(err, errRegistrationLockBusy) {
		t.Fatalf("init-db with the new schema's lock held: want errRegistrationLockBusy, got %v\n%s", err, out)
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("schema %d is registered by this init-db", ordersID)) {
		t.Errorf("the refusal does not name the new schema: %v", err)
	}
	ids, err := loadRegisteredSchemaIDs(context.Background(), tg.pool, tg.opts.schemaTable)
	if err != nil {
		t.Fatalf("read the registry: %v", err)
	}
	if _, ok := ids["orders"]; ok {
		t.Fatalf("orders' registry row committed without its record: %v", ids)
	}
	requireStates(t, "refused registration", tg.records(t), map[string]publication.State{"outbox_message": "", "timer": ""})

	holder.release(t, ordersID)
	out, err = tg.run(t)
	if err != nil {
		t.Fatalf("init-db with only a pre-existing schema's lock held: %v\n%s", err, out)
	}
	requireStates(t, "registration beside a skipped cutover", tg.records(t), map[string]publication.State{
		"orders": publication.Unpublished, "timer": publication.Unpublished, "outbox_message": "",
	})
	if ids, _ := loadRegisteredSchemaIDs(context.Background(), tg.pool, tg.opts.schemaTable); ids["orders"] != ordersID {
		t.Fatalf("orders registered as %d, want %d", ids["orders"], ordersID)
	}
	if !strings.Contains(out, "Publication record of schema 100: skipped: schema lock held") {
		t.Errorf("init-db output does not report the skipped pre-existing schema:\n%s", out)
	}

	holder.release(t, 100)
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db after the lock was released: %v\n%s", err, out)
	}
	requireStates(t, "retried cutover", tg.records(t), map[string]publication.State{
		"orders": publication.Unpublished, "timer": publication.Unpublished, "outbox_message": publication.Unpublished,
	})
}
