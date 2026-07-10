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
type wideVals struct {
	title, note, ref, token *string
	rank, level             *int16
	count, qty              *int32
	amount, total           *int64
	joined, touched         *int64
	born, seen              *int64
	score, ratio            *float64
	active                  *bool
}

// wideTruth derives per-row expected parquet values straight from event
// attributes — independent of every storage path (create-only scripts).
func wideTruth(t *testing.T, events []*Event) map[uuid.UUID]*wideVals {
	t.Helper()
	truth := make(map[uuid.UUID]*wideVals, len(events))
	for _, ev := range events {
		if ev.Kind != EventCreate {
			t.Fatalf("wideTruth only supports create-only scripts, got %s", ev.Kind)
		}
		a := ev.Attrs
		v := &wideVals{
			title: strAttr(t, a, "title"), note: strAttr(t, a, "note"),
			ref: uuidAttr(t, a, "ref"), token: uuidAttr(t, a, "token"),
			rank: int16Attr(t, a, "rank"), level: int16Attr(t, a, "level"),
			count: int32Attr(t, a, "count"), qty: int32Attr(t, a, "qty"),
			amount: int64Attr(t, a, "amount"), total: int64Attr(t, a, "total"),
			score: f64Attr(t, a, "score"), ratio: f64Attr(t, a, "ratio"),
			active: boolAttr(t, a, "active"),
			born:   dateMSAttr(t, a, "born"), joined: dateMSAttr(t, a, "joined"),
			seen: datetimeMSAttr(t, a, "seen"), touched: datetimeMSAttr(t, a, "touched"),
		}
		truth[ev.RowID] = v
	}
	return truth
}

func strAttr(t *testing.T, a map[string]any, k string) *string {
	if raw, ok := a[k]; ok {
		s, isStr := raw.(string)
		if !isStr {
			t.Fatalf("attr %s is %T, want string", k, raw)
		}
		return &s
	}
	return nil
}

func uuidAttr(t *testing.T, a map[string]any, k string) *string {
	s := strAttr(t, a, k)
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

func f64Attr(t *testing.T, a map[string]any, k string) *float64 {
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

func int16Attr(t *testing.T, a map[string]any, k string) *int16 {
	f := f64Attr(t, a, k)
	if f == nil {
		return nil
	}
	v := int16(*f)
	return &v
}

func int32Attr(t *testing.T, a map[string]any, k string) *int32 {
	f := f64Attr(t, a, k)
	if f == nil {
		return nil
	}
	v := int32(*f)
	return &v
}

// int64Attr keeps exact int64s exact: int64 passes through, float64 converts.
func int64Attr(t *testing.T, a map[string]any, k string) *int64 {
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

func boolAttr(t *testing.T, a map[string]any, k string) *bool {
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

func dateMSAttr(t *testing.T, a map[string]any, k string) *int64 {
	s := strAttr(t, a, k)
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

func datetimeMSAttr(t *testing.T, a map[string]any, k string) *int64 {
	s := strAttr(t, a, k)
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

// assertWideParquetValues reads every attribute column of one parquet file
// and compares row-by-row against the event-derived truth. Returns the row
// count so callers can assert per-tier coverage.
func assertWideParquetValues(ctx context.Context, t *testing.T, env *Env, key, tier string, truth map[uuid.UUID]*wideVals) int {
	t.Helper()
	path := fmt.Sprintf("s3://%s/%s", env.Cluster.Bucket, strings.TrimPrefix(key, "/"))
	rows, err := env.Duck.DB.QueryContext(ctx, fmt.Sprintf(
		`SELECT CAST(row_id AS VARCHAR), "title", "rank", "count", "amount", "score",
		        CAST("ref" AS VARCHAR), "joined", "touched", "note", "active",
		        "born", "seen", "level", "qty", "total", "ratio", "token"
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
		if err := rows.Scan(&rowIDStr, &title, &rank, &count, &amount, &score,
			&ref, &joined, &touched, &note, &active,
			&born, &seen, &level, &qty, &total, &ratio, &token); err != nil {
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
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s parquet rows: %v", tier, err)
	}
	return n
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
