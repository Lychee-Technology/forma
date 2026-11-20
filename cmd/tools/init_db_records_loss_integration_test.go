package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/lychee-technology/forma/internal/federatedv2"
	"github.com/lychee-technology/forma/internal/manifestv2"
	"github.com/lychee-technology/forma/internal/publication"
)

// staleReader is a federated reader that pinned schema id's epoch when it
// loaded the schema's metadata, as a server does at startup.
func (tg *initDBTarget) staleReader(t *testing.T, id int16) (*federatedv2.Source, int64) {
	t.Helper()
	ctx := context.Background()
	tx, err := tg.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatalf("begin the pin snapshot: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	pins, err := federatedv2.LoadEpochPins(ctx, tx, tg.opts.entityMain, []int16{id})
	if err != nil {
		t.Fatalf("pin schema %d: %v", id, err)
	}
	cache, err := manifestv2.NewCache(&manifestv2.FSStore{Dir: t.TempDir()}, 4)
	if err != nil {
		t.Fatalf("manifest cache: %v", err)
	}
	src, err := federatedv2.New(federatedv2.Config{
		EntityMain: tg.opts.entityMain, Manifests: cache, Epochs: pins,
		ObjectURI: func(key string) string { return key },
		Exists:    func(context.Context, string) (bool, error) { return true, nil },
	})
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	return src, pins[id]
}

// readsClosed fails unless src's read of schema id's record fails closed on
// the epoch it pinned.
func (tg *initDBTarget) readsClosed(t *testing.T, src *federatedv2.Source, id int16, when string) {
	t.Helper()
	ctx := context.Background()
	err := pgx.BeginTxFunc(ctx, tg.pool, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, func(tx pgx.Tx) error {
		_, err := src.ReadRecord(ctx, tx, id)
		return err
	})
	if !errors.Is(err, federatedv2.ErrEpochChanged) {
		t.Fatalf("%s: a reader pinned before the loss read schema %d's record: %v", when, id, err)
	}
}

// writerPinHolds answers whether a cdc-flush or cdc-init that read pin
// before it loaded schema id's metadata would still find the record at it,
// the check cdcv2 makes under the schema lock before it stages anything.
func (tg *initDBTarget) writerPinHolds(t *testing.T, id int16, pin int64) bool {
	t.Helper()
	var epochs map[int16]int64
	err := pgx.BeginFunc(context.Background(), tg.pool, func(tx pgx.Tx) error {
		var err error
		epochs, err = publication.Epochs(context.Background(), publication.PgxTx(tx), publication.TablesFor(tg.opts.entityMain))
		return err
	})
	if err != nil {
		t.Fatalf("read the epochs: %v", err)
	}
	return epochs[id] == pin
}

// A record lost after its schema was dropped and registered again moved
// epochs without a generation row: drop-schema took timer from epoch 1 to 2
// and init-db to 3, while its generation history, when it has one, names
// epoch 1 at most. A server and a cdc writer that loaded timer at epoch 3
// still pin 3. init-db recovers the record after the id's lifecycle epoch
// (4, then 5 once activated for the run's registration, or 4 for a schema
// still registered), never at the history's epoch + 1 or at epoch 1, and no
// later drop and re-registration brings the record back to 3: the stale
// reader fails closed on every one of them, and the stale writer's pin never
// matches.
func TestInitDBRecoversALostRecordAboveTheEpochStaleProcessesPin(t *testing.T) {
	for _, published := range []bool{true, false} {
		name := "without generation history"
		if published {
			name = "with generation history"
		}
		t.Run(name, func(t *testing.T) {
			tg := newInitDBTarget(t)
			tg.writeOutboxAndTimer(t)
			if out, err := tg.run(t); err != nil {
				t.Fatalf("init-db: %v\n%s", err, out)
			}
			if published {
				tg.publishInit(t, 101)
			}
			if out, err := tg.drop(t, dropTarget{name: "timer"}); err != nil {
				t.Fatalf("drop-schema: %v\n%s", err, out)
			}
			if out, err := tg.run(t); err != nil {
				t.Fatalf("init-db re-registers timer: %v\n%s", err, out)
			}
			reader, pin := tg.staleReader(t, 101)
			if pin != 3 {
				t.Fatalf("the re-registered timer is pinned at epoch %d, want 3", pin)
			}

			tg.removeRecord(t, 101)
			out, err := tg.run(t)
			if err != nil {
				t.Fatalf("init-db after timer's record was lost: %v\n%s", err, out)
			}
			rec, _ := tg.record(t, 101)
			wantState := publication.Unpublished
			if published {
				wantState = publication.AwaitingInit
			}
			if rec.Epoch != pin+1 || rec.State != wantState {
				t.Fatalf("recovered record %+v, want %s at epoch %d, above the pinned %d", rec, wantState, pin+1, pin)
			}
			if !strings.Contains(out, fmt.Sprintf("Publication record of schema 101: recovered %s at epoch %d", wantState, pin+1)) {
				t.Errorf("init-db does not report the recovery:\n%s", out)
			}
			for round := range 2 {
				when := fmt.Sprintf("after %d re-registration(s) since the recovery", round)
				if rec.State != publication.AwaitingInit {
					tg.readsClosed(t, reader, 101, when)
				}
				if tg.writerPinHolds(t, 101, pin) {
					t.Fatalf("%s: a writer pinned at epoch %d before the loss finds the record at its pin", when, pin)
				}
				if out, err := tg.drop(t, dropTarget{name: "timer"}); err != nil {
					t.Fatalf("drop-schema: %v\n%s", err, out)
				}
				if out, err := tg.run(t); err != nil {
					t.Fatalf("init-db re-registers timer: %v\n%s", err, out)
				}
				rec, _ = tg.record(t, 101)
			}
			if rec.Epoch != pin+5 {
				t.Fatalf("the record is at epoch %d after two drops and re-registrations, want %d", rec.Epoch, pin+5)
			}
		})
	}
}

// A schema whose record and lifecycle epoch are both lost while its
// generation history survives fails init-db: no epoch above the history is
// known to be free of a stale pin. Once the operator has stopped every
// process and written the id's lifecycle epoch, init-db recovers the record
// above it.
func TestInitDBRefusesALostRecordWhoseLifecycleEpochIsLostToo(t *testing.T) {
	tg := newInitDBTarget(t)
	tg.writeOutboxAndTimer(t)
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db: %v\n%s", err, out)
	}
	published := tg.publishInit(t, 100)
	tables := publication.TablesFor(tg.opts.entityMain)
	tg.removeRecord(t, 100)
	if _, err := tg.pool.Exec(context.Background(), "DELETE FROM "+tables.Epoch+" WHERE schema_id = $1", 100); err != nil {
		t.Fatalf("delete schema 100's lifecycle epoch: %v", err)
	}

	out, err := tg.run(t)
	if !errors.Is(err, publication.ErrLifecycleLost) {
		t.Fatalf("init-db recovered a record whose lifecycle epoch is lost: %v\n%s", err, out)
	}
	if !strings.Contains(err.Error(), "A lost record") {
		t.Errorf("the refusal does not point to the recovery procedure: %v", err)
	}
	if _, ok := tg.record(t, 100); ok {
		t.Fatalf("the refused run wrote a record")
	}

	if _, err := tg.pool.Exec(context.Background(), "INSERT INTO "+tables.Epoch+" (schema_id, epoch, updated_at) VALUES ($1, $2, 0)", 100, 9); err != nil {
		t.Fatalf("establish schema 100's lifecycle epoch: %v", err)
	}
	if out, err := tg.run(t); err != nil {
		t.Fatalf("init-db after the operator established the lifecycle epoch: %v\n%s", err, out)
	}
	rec, _ := tg.record(t, 100)
	if rec.Epoch != 10 || rec.State != publication.AwaitingInit || rec.Generation != published.Generation {
		t.Fatalf("recovered record %+v, want AWAITING_INIT at epoch 10 with counter %d", rec, published.Generation)
	}
}
