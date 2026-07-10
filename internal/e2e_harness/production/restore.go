package production

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	forma "github.com/lychee-technology/forma"
)

// InjectRestore re-materializes a hard-deleted row under its original row_id
// by writing storage directly — the production API has no restore operation
// (Create mints a fresh UUIDv7, Update on a deleted row returns ErrNotFound),
// so this escape hatch models the storage effect a future restore API would
// have: rebuild the entity_main and eav_data rows from the create event's
// attributes and clear the slot-0 change_log tombstone. It appends a
// synthetic update event to the Env's log so the oracle folds the revival,
// and returns that event.
//
// The restore timestamp is forced strictly past the tombstone's changed_at:
// the federated LWW tiebreak (ver_ts DESC, tier, deleted_ts DESC) lets a
// tombstone win an equal timestamp, which would keep the row hidden.
//
// Scope guard: only text and numeric attributes are reconstructed (the
// e2e_simple fixture shapes) — value transformation for richer types belongs
// to the production write path, which the create/update scenarios cover.
func (e *Env) InjectRestore(ctx context.Context, create, del *Event) *Event {
	e.T.Helper()
	if create.RowID == (uuid.UUID{}) || create.RowID != del.RowID {
		e.T.Fatalf("InjectRestore needs applied create/delete events for one row, got create=%s delete=%s", create.RowID, del.RowID)
	}

	ts := time.Now().UnixMilli()
	if ts <= del.ChangedAt {
		ts = del.ChangedAt + 1
	}

	_, cache, err := e.Registry.GetSchemaAttributeCacheByID(create.Schema.ID)
	if err != nil {
		e.T.Fatalf("attribute cache for schema %d: %v", create.Schema.ID, err)
	}

	e.insertRestoredRows(ctx, create, cache, ts)

	// Clear the slot-0 tombstone the way the production upsert would
	// (postgres_persistent_repository.go upsertChangeLog).
	e.ExecSQL(ctx,
		`INSERT INTO change_log (schema_id, row_id, changed_at, deleted_at, flushed_at)
		 VALUES ($1, $2, $3, NULL, 0)
		 ON CONFLICT (schema_id, row_id, flushed_at)
		 DO UPDATE SET changed_at = EXCLUDED.changed_at, deleted_at = NULL`,
		create.Schema.ID, create.RowID, ts)

	// Oracle sync: an update event revives the row with the create's
	// attributes; readBackTimestamps loads the slot-0 entry just written.
	restored := UpdateEvent(create.Schema, create.RowID, create.Attrs)
	e.eventSeq++
	restored.Seq = e.eventSeq
	if err := e.readBackTimestamps(ctx, restored); err != nil {
		e.T.Fatalf("read back restore timestamps: %v", err)
	}
	e.events = append(e.events, restored)
	return restored
}

// insertRestoredRows rebuilds the entity_main row (bound columns inline,
// ltbase_deleted_at NULL) and one eav_data row per unbound attribute.
func (e *Env) insertRestoredRows(ctx context.Context, create *Event, cache forma.SchemaAttributeCache, ts int64) {
	e.T.Helper()
	mainCols := []string{"ltbase_schema_id", "ltbase_row_id", "ltbase_created_at", "ltbase_updated_at", "ltbase_deleted_at"}
	mainVals := []any{create.Schema.ID, create.RowID, ts, ts, nil}

	type eavRow struct {
		attrID  int16
		text    any
		numeric any
	}
	var eavRows []eavRow

	for name, raw := range create.Attrs {
		meta, ok := cache[name]
		if !ok {
			e.T.Fatalf("InjectRestore: attribute %q not in schema %d cache", name, create.Schema.ID)
		}
		var textVal, numericVal any
		switch meta.ValueType {
		case forma.ValueTypeText:
			s, isStr := raw.(string)
			if !isStr {
				e.T.Fatalf("InjectRestore: text attribute %q holds %T", name, raw)
			}
			textVal = s
		case forma.ValueTypeNumeric:
			f, isF64 := raw.(float64)
			if !isF64 {
				e.T.Fatalf("InjectRestore: numeric attribute %q holds %T", name, raw)
			}
			numericVal = f
		default:
			e.T.Fatalf("InjectRestore only reconstructs text/numeric attributes, %q is %s", name, meta.ValueType)
		}
		if meta.ColumnBinding != nil {
			if meta.ColumnBinding.Encoding != "" && meta.ColumnBinding.Encoding != forma.MainColumnEncodingDefault {
				e.T.Fatalf("InjectRestore does not support encoding %q on %q", meta.ColumnBinding.Encoding, name)
			}
			mainCols = append(mainCols, string(meta.ColumnBinding.ColumnName))
			if textVal != nil {
				mainVals = append(mainVals, textVal)
			} else {
				mainVals = append(mainVals, numericVal)
			}
			continue
		}
		eavRows = append(eavRows, eavRow{attrID: meta.AttributeID, text: textVal, numeric: numericVal})
	}

	placeholders := make([]string, len(mainCols))
	for i := range mainCols {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	e.ExecSQL(ctx, fmt.Sprintf("INSERT INTO entity_main (%s) VALUES (%s)",
		strings.Join(mainCols, ", "), strings.Join(placeholders, ", ")), mainVals...)

	for _, row := range eavRows {
		e.ExecSQL(ctx,
			`INSERT INTO eav_data (schema_id, row_id, attr_id, array_indices, value_text, value_numeric)
			 VALUES ($1, $2, $3, '', $4, $5)`,
			create.Schema.ID, create.RowID, row.attrID, row.text, row.numeric)
	}
}
