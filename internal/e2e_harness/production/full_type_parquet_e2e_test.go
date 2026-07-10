//go:build e2e

package production

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// wideParquetTypes pins the physical parquet schema of e2e_wide exports.
// Attribute types come from cdc.duckTypeForValue / castMainValue /
// castEAVValue (internal/cdc/duckdb_exporter.go); note the deliberate
// asymmetry: bound uuid (ref) is physical UUID, EAV uuid (token) is VARCHAR.
var wideParquetTypes = map[string]string{
	"schema_id": "SMALLINT", "row_id": "UUID",
	"changed_at": "BIGINT", "deleted_at": "BIGINT",
	"ltbase_created_at": "BIGINT", "ltbase_updated_at": "BIGINT", "ltbase_deleted_at": "BIGINT",
	"title": "VARCHAR", "rank": "SMALLINT", "count": "INTEGER", "amount": "BIGINT",
	"score": "DOUBLE", "ref": "UUID", "joined": "BIGINT", "touched": "BIGINT",
	"note": "VARCHAR", "active": "BOOLEAN", "born": "BIGINT", "seen": "BIGINT",
	"level": "SMALLINT", "qty": "INTEGER", "total": "BIGINT", "ratio": "DOUBLE", "token": "VARCHAR",
}

// wideVals is the parquet-physical expected row: pointers are nil for NULL.
// The attribute pointers come from the event; the system-column fields
// (changedAt/ltbaseCreatedAt/ltbaseUpdatedAt) are non-pointer and carry the
// storage-truth timestamps that only the export producer can supply.
type wideVals struct {
	title, note, ref, token *string
	rank, level             *int16
	count, qty              *int32
	amount, total           *int64
	joined, touched         *int64
	born, seen              *int64
	score, ratio            *float64
	active                  *bool

	// changedAt is the change_log timestamp the write read back into the event
	// (Event.ChangedAt); the delta exporter emits it as changed_at
	// (internal/cdc/duckdb_exporter.go:153). ltbaseCreatedAt / ltbaseUpdatedAt
	// are the entity_main source-row stamps, loaded from Postgres.
	changedAt       int64
	ltbaseCreatedAt int64
	ltbaseUpdatedAt int64
}

// buildWideTruth derives per-row expected parquet values: attribute values and
// changed_at come straight from the events (create-only scripts), and the
// entity_main system timestamps are loaded from Postgres — the parquet copy
// must equal that source row exactly.
func buildWideTruth(ctx context.Context, t *testing.T, env *Env, schemaID int16, events []*Event) map[uuid.UUID]*wideVals {
	t.Helper()
	truth := make(map[uuid.UUID]*wideVals, len(events))
	for _, ev := range events {
		if ev.Kind != EventCreate {
			t.Fatalf("buildWideTruth only supports create-only scripts, got %s", ev.Kind)
		}
		a := ev.Attrs
		v := &wideVals{
			title: extractStrAttr(t, a, "title"), note: extractStrAttr(t, a, "note"),
			ref: extractUUIDAttr(t, a, "ref"), token: extractUUIDAttr(t, a, "token"),
			rank: extractInt16Attr(t, a, "rank"), level: extractInt16Attr(t, a, "level"),
			count: extractInt32Attr(t, a, "count"), qty: extractInt32Attr(t, a, "qty"),
			amount: extractInt64Attr(t, a, "amount"), total: extractInt64Attr(t, a, "total"),
			score: extractF64Attr(t, a, "score"), ratio: extractF64Attr(t, a, "ratio"),
			active: extractBoolAttr(t, a, "active"),
			born:   extractDateMSAttr(t, a, "born"), joined: extractDateMSAttr(t, a, "joined"),
			seen: extractDatetimeMSAttr(t, a, "seen"), touched: extractDatetimeMSAttr(t, a, "touched"),
			changedAt: ev.ChangedAt,
		}
		truth[ev.RowID] = v
	}
	loadSystemTimestamps(ctx, t, env, schemaID, truth)
	return truth
}

// loadSystemTimestamps fills each truth row's ltbase_created_at /
// ltbase_updated_at from the entity_main source row — the exporter's input, so
// the parquet copy must equal it exactly. entity_main rows survive the
// change_log DELETE and the flush unchanged, so reading them at truth-build
// time yields the same values the export read.
func loadSystemTimestamps(ctx context.Context, t *testing.T, env *Env, schemaID int16, truth map[uuid.UUID]*wideVals) {
	t.Helper()
	rows, err := env.Pool.Query(ctx,
		`SELECT ltbase_row_id, ltbase_created_at, ltbase_updated_at
		 FROM entity_main WHERE ltbase_schema_id = $1`, schemaID)
	if err != nil {
		t.Fatalf("load entity_main system timestamps: %v", err)
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var rowIDBytes [16]byte
		var created, updated int64
		if err := rows.Scan(&rowIDBytes, &created, &updated); err != nil {
			t.Fatalf("scan entity_main timestamps: %v", err)
		}
		if v, ok := truth[uuid.UUID(rowIDBytes)]; ok {
			v.ltbaseCreatedAt = created
			v.ltbaseUpdatedAt = updated
			seen++
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("entity_main timestamp rows: %v", err)
	}
	if seen != len(truth) {
		t.Fatalf("loaded %d/%d entity_main system timestamps for schema %d", seen, len(truth), schemaID)
	}
}

func extractStrAttr(t *testing.T, a map[string]any, k string) *string {
	if raw, ok := a[k]; ok {
		s, isStr := raw.(string)
		if !isStr {
			t.Fatalf("attr %s is %T, want string", k, raw)
		}
		return &s
	}
	return nil
}

func extractUUIDAttr(t *testing.T, a map[string]any, k string) *string {
	s := extractStrAttr(t, a, k)
	if s == nil {
		return nil
	}
	parsed, err := uuid.Parse(*s)
	if err != nil {
		t.Fatalf("attr %s = %q is not a uuid: %v", k, *s, err)
	}
	canon := parsed.String()
	return &canon
}

func extractF64Attr(t *testing.T, a map[string]any, k string) *float64 {
	raw, ok := a[k]
	if !ok {
		return nil
	}
	switch v := raw.(type) {
	case float64:
		return &v
	case int64:
		f := float64(v)
		return &f
	default:
		t.Fatalf("attr %s is %T, want numeric", k, raw)
		return nil
	}
}

func extractInt16Attr(t *testing.T, a map[string]any, k string) *int16 {
	f := extractF64Attr(t, a, k)
	if f == nil {
		return nil
	}
	v := int16(*f)
	return &v
}

func extractInt32Attr(t *testing.T, a map[string]any, k string) *int32 {
	f := extractF64Attr(t, a, k)
	if f == nil {
		return nil
	}
	v := int32(*f)
	return &v
}

// extractInt64Attr keeps exact int64s exact: int64 passes through, float64 converts.
func extractInt64Attr(t *testing.T, a map[string]any, k string) *int64 {
	raw, ok := a[k]
	if !ok {
		return nil
	}
	switch v := raw.(type) {
	case int64:
		return &v
	case float64:
		i := int64(v)
		return &i
	default:
		t.Fatalf("attr %s is %T, want numeric", k, raw)
		return nil
	}
}

func extractBoolAttr(t *testing.T, a map[string]any, k string) *bool {
	raw, ok := a[k]
	if !ok {
		return nil
	}
	b, isBool := raw.(bool)
	if !isBool {
		t.Fatalf("attr %s is %T, want bool", k, raw)
	}
	return &b
}

func extractDateMSAttr(t *testing.T, a map[string]any, k string) *int64 {
	s := extractStrAttr(t, a, k)
	if s == nil {
		return nil
	}
	parsed, err := time.ParseInLocation("2006-01-02", *s, time.UTC)
	if err != nil {
		t.Fatalf("attr %s = %q: %v", k, *s, err)
	}
	ms := parsed.UnixMilli()
	return &ms
}

func extractDatetimeMSAttr(t *testing.T, a map[string]any, k string) *int64 {
	s := extractStrAttr(t, a, k)
	if s == nil {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339, *s)
	if err != nil {
		t.Fatalf("attr %s = %q: %v", k, *s, err)
	}
	ms := parsed.UnixMilli()
	return &ms
}

// assertWideParquetSchema pins the physical column set + types (#174
// hypothesis 1: the reader's changed_at expectation is met by the exporter).
func assertWideParquetSchema(ctx context.Context, t *testing.T, env *Env, key, tier string) {
	t.Helper()
	path := fmt.Sprintf("s3://%s/%s", env.Cluster.Bucket, strings.TrimPrefix(key, "/"))
	rows, err := env.Duck.DB.QueryContext(ctx,
		fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s')", path))
	if err != nil {
		t.Fatalf("%s parquet describe: %v", tier, err)
	}
	defer rows.Close()
	got := map[string]string{}
	for rows.Next() {
		var name, typ string
		var null, key2, def, extra sql.NullString
		if err := rows.Scan(&name, &typ, &null, &key2, &def, &extra); err != nil {
			t.Fatalf("%s describe scan: %v", tier, err)
		}
		got[name] = typ
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s describe rows: %v", tier, err)
	}
	for col, want := range wideParquetTypes {
		if got[col] != want {
			t.Errorf("%s parquet column %q = %q, want %q", tier, col, got[col], want)
		}
	}
	for col := range got {
		if _, ok := wideParquetTypes[col]; !ok {
			t.Errorf("%s parquet has unexpected column %q (type %s)", tier, col, got[col])
		}
	}
}

// assertWideParquetValues reads every attribute AND system column of one
// parquet file and compares row-by-row against the storage truth. Returns the
// row count so callers can assert per-tier coverage. tier drives the
// producer-specific expectations for changed_at and deleted_at (base vs delta
// exporters emit them differently — see checkSystemColumns).
func assertWideParquetValues(ctx context.Context, t *testing.T, env *Env, key, tier string, schemaID int16, truth map[uuid.UUID]*wideVals) int {
	t.Helper()
	path := fmt.Sprintf("s3://%s/%s", env.Cluster.Bucket, strings.TrimPrefix(key, "/"))
	rows, err := env.Duck.DB.QueryContext(ctx, fmt.Sprintf(
		`SELECT CAST(row_id AS VARCHAR), "title", "rank", "count", "amount", "score",
		        CAST("ref" AS VARCHAR), "joined", "touched", "note", "active",
		        "born", "seen", "level", "qty", "total", "ratio", "token",
		        "schema_id", "changed_at", "deleted_at",
		        "ltbase_created_at", "ltbase_updated_at", "ltbase_deleted_at"
		 FROM read_parquet('%s')`, path))
	if err != nil {
		t.Fatalf("%s parquet scan: %v", tier, err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var rowIDStr string
		var title, ref, note, token sql.NullString
		var rank, level sql.NullInt16
		var count, qty sql.NullInt32
		var amount, joined, touched, born, seen, total sql.NullInt64
		var score, ratio sql.NullFloat64
		var active sql.NullBool
		var schemaCol sql.NullInt16
		var changedAt, deletedAt, createdAt, updatedAt, ltbaseDeletedAt sql.NullInt64
		if err := rows.Scan(&rowIDStr, &title, &rank, &count, &amount, &score,
			&ref, &joined, &touched, &note, &active,
			&born, &seen, &level, &qty, &total, &ratio, &token,
			&schemaCol, &changedAt, &deletedAt, &createdAt, &updatedAt, &ltbaseDeletedAt); err != nil {
			t.Fatalf("%s parquet row scan: %v", tier, err)
		}
		rowID, err := uuid.Parse(rowIDStr)
		if err != nil {
			t.Fatalf("%s parquet row_id %q: %v", tier, rowIDStr, err)
		}
		want, ok := truth[rowID]
		if !ok {
			t.Fatalf("%s parquet holds unknown row %s", tier, rowID)
		}
		checkStr(t, tier, rowID, "title", title, want.title)
		checkStr(t, tier, rowID, "note", note, want.note)
		checkStr(t, tier, rowID, "ref", ref, want.ref)
		checkStr(t, tier, rowID, "token", token, want.token)
		checkI16(t, tier, rowID, "rank", rank, want.rank)
		checkI16(t, tier, rowID, "level", level, want.level)
		checkI32(t, tier, rowID, "count", count, want.count)
		checkI32(t, tier, rowID, "qty", qty, want.qty)
		checkI64(t, tier, rowID, "amount", amount, want.amount)
		checkI64(t, tier, rowID, "joined", joined, want.joined)
		checkI64(t, tier, rowID, "touched", touched, want.touched)
		checkI64(t, tier, rowID, "born", born, want.born)
		checkI64(t, tier, rowID, "seen", seen, want.seen)
		checkI64(t, tier, rowID, "total", total, want.total)
		checkF64(t, tier, rowID, "score", score, want.score)
		checkF64(t, tier, rowID, "ratio", ratio, want.ratio)
		checkBool(t, tier, rowID, "active", active, want.active)
		checkSystemColumns(t, tier, rowID, schemaID, want, systemCols{
			schema: schemaCol, changedAt: changedAt, deletedAt: deletedAt,
			createdAt: createdAt, updatedAt: updatedAt, ltbaseDeletedAt: ltbaseDeletedAt,
		})
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s parquet rows: %v", tier, err)
	}
	return n
}

// systemCols is one parquet row's scanned system columns.
type systemCols struct {
	schema                                                     sql.NullInt16
	changedAt, deletedAt, createdAt, updatedAt, ltbaseDeletedAt sql.NullInt64
}

// checkSystemColumns asserts the six CDC system columns against storage truth,
// picking the producer-specific expectation per tier. Observed emissions
// (2026-07): schema_id == fixture ID (both tiers); ltbase_created_at /
// ltbase_updated_at == the entity_main source stamps (both tiers). changed_at:
// BASE exporter emits m.ltbase_created_at AS changed_at
// (internal/cdc/init_exporter.go:35); DELTA emits cl.changed_at
// (internal/cdc/duckdb_exporter.go:153) == Event.ChangedAt. deleted_at: BASE
// COALESCEs to 0 (init_exporter.go:36); DELTA emits raw cl.deleted_at, NULL for
// a live create (duckdb_exporter.go:154). ltbase_deleted_at is the raw
// entity_main main column, NULL for every non-deleted row on both tiers.
func checkSystemColumns(t *testing.T, tier string, rowID uuid.UUID, schemaID int16, want *wideVals, got systemCols) {
	t.Helper()
	if !got.schema.Valid || got.schema.Int16 != schemaID {
		t.Errorf("%s %s.schema_id = %v (valid=%t), want %d", tier, rowID, got.schema.Int16, got.schema.Valid, schemaID)
	}
	created := want.ltbaseCreatedAt
	checkI64(t, tier, rowID, "ltbase_created_at", got.createdAt, &created)
	updated := want.ltbaseUpdatedAt
	checkI64(t, tier, rowID, "ltbase_updated_at", got.updatedAt, &updated)
	// ltbase_deleted_at: raw main column, NULL for every non-deleted row.
	checkI64(t, tier, rowID, "ltbase_deleted_at", got.ltbaseDeletedAt, nil)

	wantChangedAt := want.changedAt      // delta: cl.changed_at
	var wantDeletedAt *int64             // delta: raw cl.deleted_at, NULL for a create
	if tier == "base" {
		wantChangedAt = want.ltbaseCreatedAt // base: m.ltbase_created_at AS changed_at
		zero := int64(0)                     // base: COALESCE(ltbase_deleted_at, 0)
		wantDeletedAt = &zero
	}
	checkI64(t, tier, rowID, "changed_at", got.changedAt, &wantChangedAt)
	checkI64(t, tier, rowID, "deleted_at", got.deletedAt, wantDeletedAt)
}

func checkStr(t *testing.T, tier string, rowID uuid.UUID, col string, got sql.NullString, want *string) {
	t.Helper()
	switch {
	case want == nil && got.Valid:
		t.Errorf("%s %s.%s = %q, want NULL", tier, rowID, col, got.String)
	case want != nil && !got.Valid:
		t.Errorf("%s %s.%s = NULL, want %q", tier, rowID, col, *want)
	case want != nil && got.String != *want:
		t.Errorf("%s %s.%s = %q, want %q", tier, rowID, col, got.String, *want)
	}
}

func checkI16(t *testing.T, tier string, rowID uuid.UUID, col string, got sql.NullInt16, want *int16) {
	t.Helper()
	switch {
	case want == nil && got.Valid:
		t.Errorf("%s %s.%s = %d, want NULL", tier, rowID, col, got.Int16)
	case want != nil && !got.Valid:
		t.Errorf("%s %s.%s = NULL, want %d", tier, rowID, col, *want)
	case want != nil && got.Int16 != *want:
		t.Errorf("%s %s.%s = %d, want %d", tier, rowID, col, got.Int16, *want)
	}
}

func checkI32(t *testing.T, tier string, rowID uuid.UUID, col string, got sql.NullInt32, want *int32) {
	t.Helper()
	switch {
	case want == nil && got.Valid:
		t.Errorf("%s %s.%s = %d, want NULL", tier, rowID, col, got.Int32)
	case want != nil && !got.Valid:
		t.Errorf("%s %s.%s = NULL, want %d", tier, rowID, col, *want)
	case want != nil && got.Int32 != *want:
		t.Errorf("%s %s.%s = %d, want %d", tier, rowID, col, got.Int32, *want)
	}
}

func checkI64(t *testing.T, tier string, rowID uuid.UUID, col string, got sql.NullInt64, want *int64) {
	t.Helper()
	switch {
	case want == nil && got.Valid:
		t.Errorf("%s %s.%s = %d, want NULL", tier, rowID, col, got.Int64)
	case want != nil && !got.Valid:
		t.Errorf("%s %s.%s = NULL, want %d", tier, rowID, col, *want)
	case want != nil && got.Int64 != *want:
		t.Errorf("%s %s.%s = %d, want %d", tier, rowID, col, got.Int64, *want)
	}
}

func checkF64(t *testing.T, tier string, rowID uuid.UUID, col string, got sql.NullFloat64, want *float64) {
	t.Helper()
	switch {
	case want == nil && got.Valid:
		t.Errorf("%s %s.%s = %v, want NULL", tier, rowID, col, got.Float64)
	case want != nil && !got.Valid:
		t.Errorf("%s %s.%s = NULL, want %v", tier, rowID, col, *want)
	case want != nil && got.Float64 != *want:
		t.Errorf("%s %s.%s = %v, want %v", tier, rowID, col, got.Float64, *want)
	}
}

func checkBool(t *testing.T, tier string, rowID uuid.UUID, col string, got sql.NullBool, want *bool) {
	t.Helper()
	switch {
	case want == nil && got.Valid:
		t.Errorf("%s %s.%s = %v, want NULL", tier, rowID, col, got.Bool)
	case want != nil && !got.Valid:
		t.Errorf("%s %s.%s = NULL, want %v", tier, rowID, col, *want)
	case want != nil && got.Bool != *want:
		t.Errorf("%s %s.%s = %v, want %v", tier, rowID, col, got.Bool, *want)
	}
}
