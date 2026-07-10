//go:build e2e

package production

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

// TestApplyEventsWritesProductionState verifies events applied through the
// real EntityManager land in entity_main, eav_data, and change_log with
// read-back timestamps.
func TestApplyEventsWritesProductionState(t *testing.T) {
	cluster := SharedCluster(t)
	env := NewEnv(t, cluster)
	ctx := context.Background()
	wide := DefaultSchemaFixtures()[1] // e2e_wide

	create := CreateEvent(wide, map[string]any{
		"title":  "hello",
		"rank":   float64(3),
		"count":  float64(42),
		"amount": float64(1000000),
		"score":  1.5,
		"ref":    "0198c5f2-0000-7000-8000-000000000001",
		"note":   "eav note",
		"active": true,
		"born":   "1990-05-01",
		"seen":   "2026-07-10T01:02:03Z",
	})
	if err := env.ApplyEvents(ctx, create); err != nil {
		t.Fatalf("apply create: %v", err)
	}
	if create.RowID == (uuid.UUID{}) {
		t.Fatal("create event did not receive a row id")
	}
	if create.ChangedAt <= 0 {
		t.Fatalf("create event changed_at = %d, want > 0", create.ChangedAt)
	}

	update := UpdateEvent(wide, create.RowID, map[string]any{"title": "hello2", "note": "eav note 2"})
	victim := CreateEvent(wide, map[string]any{"title": "victim", "note": "to be deleted"})
	if err := env.ApplyEvents(ctx, update, victim); err != nil {
		t.Fatalf("apply update+create: %v", err)
	}
	del := DeleteEvent(wide, victim.RowID)
	if err := env.ApplyEvents(ctx, del); err != nil {
		t.Fatalf("apply delete: %v", err)
	}
	if del.DeletedAt <= 0 {
		t.Fatalf("delete event deleted_at = %d, want > 0", del.DeletedAt)
	}

	var mainTitle string
	if err := env.Pool.QueryRow(ctx,
		"SELECT text_01 FROM entity_main WHERE ltbase_schema_id=$1 AND ltbase_row_id=$2",
		wide.ID, create.RowID).Scan(&mainTitle); err != nil {
		t.Fatalf("query entity_main: %v", err)
	}
	if mainTitle != "hello2" {
		t.Errorf("entity_main text_01 = %q, want %q", mainTitle, "hello2")
	}

	var eavRows int
	if err := env.Pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM eav_data WHERE schema_id=$1 AND row_id=$2",
		wide.ID, create.RowID).Scan(&eavRows); err != nil {
		t.Fatalf("query eav_data: %v", err)
	}
	if eavRows == 0 {
		t.Error("eav_data has no rows for the created record")
	}

	// Production delete removes the entity_main/eav_data rows and records
	// the tombstone in change_log only.
	var victimRows int
	if err := env.Pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM entity_main WHERE ltbase_schema_id=$1 AND ltbase_row_id=$2",
		wide.ID, victim.RowID).Scan(&victimRows); err != nil {
		t.Fatalf("query deleted entity_main row: %v", err)
	}
	if victimRows != 0 {
		t.Errorf("deleted row still present in entity_main (%d rows)", victimRows)
	}
	var tombstoneDeletedAt int64
	if err := env.Pool.QueryRow(ctx,
		"SELECT COALESCE(deleted_at, 0) FROM change_log WHERE schema_id=$1 AND row_id=$2 AND flushed_at=0",
		wide.ID, victim.RowID).Scan(&tombstoneDeletedAt); err != nil {
		t.Fatalf("query tombstone: %v", err)
	}
	if tombstoneDeletedAt != del.DeletedAt {
		t.Errorf("change_log deleted_at = %d, want read-back value %d", tombstoneDeletedAt, del.DeletedAt)
	}

	var changedAt int64
	if err := env.Pool.QueryRow(ctx,
		"SELECT changed_at FROM change_log WHERE schema_id=$1 AND row_id=$2 AND flushed_at=0",
		wide.ID, create.RowID).Scan(&changedAt); err != nil {
		t.Fatalf("query change_log: %v", err)
	}
	if changedAt != update.ChangedAt {
		t.Errorf("change_log changed_at = %d, want read-back value %d", changedAt, update.ChangedAt)
	}

	if got := len(env.Events()); got != 4 {
		t.Errorf("event log length = %d, want 4", got)
	}
}
