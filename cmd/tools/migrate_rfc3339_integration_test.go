package main

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/rfc3339migrate"
	"github.com/lychee-technology/forma/internal/schemaledger"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/widthaudit"
)

// The schema the migration tests run on: two rfc3339 attributes and a plain
// text one, plus a second schema whose text_02 is plain text.
const (
	migrateEvents = "events"
	migrateNotes  = "notes"
)

func migrateEventsAttrs() string {
	return "{" + encodedAttr("due", 1, "datetime", "text_02", "rfc3339") + "," +
		encodedAttr("day", 2, "date", "text_03", "rfc3339") + "," + boundAttr("title", 3, "text", "text_01") + "}"
}

// newLegacyISO8601Target registers events and notes, then records a
// generation of events that names the iso8601 encoding, as a deployment
// that recorded its ledger before #842 holds it: registration refuses
// iso8601 now, so the test writes the generation the old release wrote.
// Its content hash is not the files', so apply-schema judges the files
// against it.
func newLegacyISO8601Target(t *testing.T) *ledgerTarget {
	t.Helper()
	lt := newLedgerTarget(t, map[string]string{
		migrateEvents: migrateEventsAttrs(),
		migrateNotes:  "{" + boundAttr("body", 1, "text", "text_02") + "}",
	})
	ctx := context.Background()
	gen := lt.active(t, 100)
	entries := append([]schemaledger.Entry(nil), gen.Entries...)
	for i := range entries {
		if entries[i].Encoding == forma.MainColumnEncodingRFC3339 {
			entries[i].Encoding = forma.MainColumnEncodingLegacyISO8601
		}
	}
	tx, err := lt.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rec := schemaledger.Record{SchemaID: 100, From: gen.Number, Hash: "recorded-before-842", Entries: entries, RecordedBy: "apply-schema"}
	if err := schemaledger.Write(ctx, tx, lt.opts.schemaTable, rec); err != nil {
		t.Fatalf("record the iso8601 generation: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}
	// A generation recorded with iso8601 still loads from the ledger.
	loaded := lt.active(t, 100)
	if loaded.Number != 2 || loaded.Entries[0].Encoding != forma.MainColumnEncodingLegacyISO8601 {
		t.Fatalf("active generation = %+v, want generation 2 recording iso8601", loaded)
	}
	return lt
}

// seedMain inserts one entity_main row of schemaID, with the given text
// columns, and answers its row id.
func (lt *ledgerTarget) seedMain(t *testing.T, schemaID int16, cols map[string]any) uuid.UUID {
	t.Helper()
	id := uuid.New()
	names, holders, args := []string{"ltbase_schema_id", "ltbase_row_id", "ltbase_created_at", "ltbase_updated_at"}, []string{"$1", "$2", "1", "1"}, []any{schemaID, id}
	for col, v := range cols {
		args = append(args, v)
		names, holders = append(names, col), append(holders, fmt.Sprintf("$%d", len(args)))
	}
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", quoteIdentifier(lt.opts.entityMain), strings.Join(names, ", "), strings.Join(holders, ", "))
	if _, err := lt.pool.Exec(context.Background(), query, args...); err != nil {
		t.Fatalf("seed %s: %v", lt.opts.entityMain, err)
	}
	return id
}

// mainText answers every row's text_01 to text_03, keyed by row id; NULL
// is nil.
func (lt *ledgerTarget) mainText(t *testing.T) map[uuid.UUID][3]*string {
	t.Helper()
	rows, err := lt.pool.Query(context.Background(), "SELECT ltbase_row_id, text_01, text_02, text_03 FROM "+quoteIdentifier(lt.opts.entityMain))
	if err != nil {
		t.Fatalf("read %s: %v", lt.opts.entityMain, err)
	}
	defer rows.Close()
	out := map[uuid.UUID][3]*string{}
	for rows.Next() {
		var id uuid.UUID
		var v [3]*string
		if err := rows.Scan(&id, &v[0], &v[1], &v[2]); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out[id] = v
	}
	return out
}

// rendered spells a row's columns by value, for comparing snapshots.
func rendered(v [3]*string) string {
	parts := make([]string, len(v))
	for i, p := range v {
		parts[i] = "NULL"
		if p != nil {
			parts[i] = fmt.Sprintf("%q", *p)
		}
	}
	return strings.Join(parts, ",")
}

func renderedAll(m map[uuid.UUID][3]*string) map[uuid.UUID]string {
	out := make(map[uuid.UUID]string, len(m))
	for id, v := range m {
		out[id] = rendered(v)
	}
	return out
}

func (lt *ledgerTarget) migrate(t *testing.T, dryRun bool, batch int) (string, error) {
	t.Helper()
	var out bytes.Buffer
	opts := ledgerToolOptions{schemaTable: lt.opts.schemaTable, dryRun: dryRun, entityMain: lt.opts.entityMain}
	err := migrateRFC3339(context.Background(), lt.pool, opts, batch, &out)
	return out.String(), err
}

// TestMigrateRFC3339RewritesOnlyWholeSecondImages pins the tool's contract
// (#842): it rewrites exactly the whole-second images of the columns the
// active generation records as iso8601, each to its first 19 characters
// plus .000Z, in batches; it lists every other image and leaves it alone;
// it leaves change_log and every other schema untouched; its dry run
// changes nothing and exits non-zero; a re-run finds nothing to do.
func TestMigrateRFC3339RewritesOnlyWholeSecondImages(t *testing.T) {
	lt := newLegacyISO8601Target(t)
	legacy := map[uuid.UUID]string{}
	for i := 0; i < 5; i++ {
		image := fmt.Sprintf("2024-01-02T03:04:0%dZ", i)
		legacy[lt.seedMain(t, 100, map[string]any{"text_02": image, "text_01": "2024-01-02T03:04:05Z"})] = image
	}
	dayRow := lt.seedMain(t, 100, map[string]any{"text_03": "2024-01-02T00:00:00Z"})
	offset := lt.seedMain(t, 100, map[string]any{"text_02": "2024-01-02T11:04:05+08:00"})
	fraction := lt.seedMain(t, 100, map[string]any{"text_02": "2024-01-02T03:04:05.5Z"})
	garbage := lt.seedMain(t, 100, map[string]any{"text_02": "not a time"})
	migrated := lt.seedMain(t, 100, map[string]any{"text_02": "2024-01-02T03:04:05.250Z"})
	lt.seedMain(t, 100, map[string]any{})
	note := lt.seedMain(t, 101, map[string]any{"text_02": "2024-01-02T03:04:05Z"})
	before := lt.mainText(t)

	out, err := lt.migrate(t, true, 2)
	if err == nil || !strings.Contains(err.Error(), "dry run: 6 whole-second image(s) in 2 column(s)") {
		t.Fatalf("dry run = %v, want a non-zero exit naming what is left\n%s", err, out)
	}
	for _, want := range []string{
		"schema events (id 100) attribute due (id 1) column text_02: 5 whole-second image(s) to rewrite, 3 other image(s) left unchanged",
		"schema events (id 100) attribute day (id 2) column text_03: 1 whole-second image(s) to rewrite, 0 other image(s) left unchanged",
		fmt.Sprintf("left unchanged: row %s holds %q", offset, "2024-01-02T11:04:05+08:00"),
		fmt.Sprintf("left unchanged: row %s holds %q", fraction, "2024-01-02T03:04:05.5Z"),
		fmt.Sprintf("left unchanged: row %s holds %q", garbage, "not a time"),
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("dry-run report lacks %q:\n%s", want, out)
		}
	}
	if after := renderedAll(lt.mainText(t)); fmt.Sprint(after) != fmt.Sprint(renderedAll(before)) {
		t.Fatalf("a dry run changed entity_main")
	}

	out, err = lt.migrate(t, false, 2)
	if err != nil || !strings.Contains(out, "rewrote 5 whole-second image(s), 0 remain, 3 other image(s) left unchanged") ||
		!strings.Contains(out, "rewrote 1 whole-second image(s), 0 remain") {
		t.Fatalf("migrate = %v\n%s", err, out)
	}
	after := lt.mainText(t)
	for id, image := range legacy {
		if got := after[id][1]; got == nil || *got != image[:19]+".000Z" {
			t.Fatalf("row %s holds %v, want %s.000Z", id, got, image[:19])
		}
		if got := after[id][0]; got == nil || *got != "2024-01-02T03:04:05Z" {
			t.Fatalf("row %s: the plain text column changed to %v", id, got)
		}
	}
	if got := after[dayRow][2]; got == nil || *got != "2024-01-02T00:00:00.000Z" {
		t.Fatalf("day row holds %v", got)
	}
	for _, id := range []uuid.UUID{offset, fraction, garbage, migrated, note} {
		if rendered(after[id]) != rendered(before[id]) {
			t.Fatalf("row %s changed although it holds no whole-second image of a target", id)
		}
	}
	var stamped int
	if err := lt.pool.QueryRow(context.Background(), "SELECT count(*) FROM "+quoteIdentifier(lt.opts.changeLog)).Scan(&stamped); err != nil || stamped != 0 {
		t.Fatalf("change_log rows = %d (%v), want none: the rewrite keeps the instant", stamped, err)
	}

	out, err = lt.migrate(t, false, 2)
	if err != nil || !strings.Contains(out, "rewrote 0 whole-second image(s), 0 remain") {
		t.Fatalf("a re-run = %v, want nothing to rewrite\n%s", err, out)
	}
	if _, err = lt.migrate(t, true, 2); err != nil {
		t.Fatalf("a dry run after the migration = %v, want a zero exit", err)
	}

	// After the migration the storage-domain census finds only the image
	// no read parses, which the migration lists and leaves.
	cache := schemameta.NewMetadataCache()
	meta := forma.AttributeMetadata{AttributeName: "due", AttributeID: 1, ValueType: forma.ValueTypeDateTime,
		ColumnBinding: &forma.MainColumnBinding{ColumnName: forma.MainColumnText02, Encoding: forma.MainColumnEncodingRFC3339}}
	if err := cache.RegisterSchema(migrateEvents, 100, forma.SchemaAttributeCache{"due": meta}); err != nil {
		t.Fatalf("register: %v", err)
	}
	var columnTargets []widthaudit.Target
	for _, target := range widthaudit.DomainTargets(cache) {
		if target.Column != "" {
			columnTargets = append(columnTargets, target)
		}
	}
	findings, err := widthaudit.DomainCensus(context.Background(), lt.pool,
		widthaudit.Tables{EntityMain: lt.opts.entityMain, ChangeLog: lt.opts.changeLog}, columnTargets)
	if err != nil || len(findings) != 1 || findings[0].RowID != garbage {
		t.Fatalf("census = %+v, %v; want only row %s", findings, err, garbage)
	}
}

// TestMigrateRFC3339RefusesABatchOverTheDSQLLimit: a batch is one
// transaction, so it stays under Aurora DSQL's 3,000-row limit.
func TestMigrateRFC3339RefusesABatchOverTheDSQLLimit(t *testing.T) {
	lt := newLegacyISO8601Target(t)
	lt.seedMain(t, 100, map[string]any{"text_02": "2024-01-02T03:04:05Z"})
	for _, batch := range []int{0, 3000} {
		if _, err := lt.migrate(t, false, batch); err == nil || !strings.Contains(err.Error(), "outside 1 to 2999") {
			t.Fatalf("batch %d = %v, want a refusal", batch, err)
		}
	}
	targets := rfc3339migrate.TargetsOf(migrateEvents, lt.active(t, 100))
	if n, err := rfc3339migrate.CountLegacy(context.Background(), lt.pool, lt.opts.entityMain, targets[0]); err != nil || n != 1 {
		t.Fatalf("a refused batch rewrote rows: %d left, %v", n, err)
	}
}

// TestApplySchemaRefusesTheRenameUntilMigrated (#842): apply-schema judges
// iso8601 → rfc3339 a binding-only change, and refuses it while a target
// column holds a whole-second image, naming the schema, attribute, column
// and count and pointing at the migration tool. Once the tool has run it
// records the change.
func TestApplySchemaRefusesTheRenameUntilMigrated(t *testing.T) {
	lt := newLegacyISO8601Target(t)
	lt.seedMain(t, 100, map[string]any{"text_02": "2024-01-02T03:04:05Z"})
	lt.seedMain(t, 100, map[string]any{"text_02": "2024-01-02T03:04:06Z"})
	lt.seedMain(t, 100, map[string]any{"text_02": "2024-01-02T03:04:07.000Z"})

	out, err := lt.apply(t, false)
	if err == nil {
		t.Fatalf("apply over unmigrated images must be refused\n%s", out)
	}
	for _, want := range []string{
		"schema events (id 100) attribute due (id 1) column text_02: iso8601 to rfc3339 is refused while 2 row(s) of " + lt.opts.entityMain,
		"forma-tools migrate-rfc3339",
		"attribute due (id 1): datetime bound to text_02 (iso8601) to datetime bound to text_02 (rfc3339) [allowed]",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("apply output lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "attribute day (id 2) column text_03: iso8601 to rfc3339 is refused") {
		t.Fatalf("an attribute with no whole-second image was refused:\n%s", out)
	}
	if gen := lt.active(t, 100); gen.Number != 2 {
		t.Fatalf("a refused apply recorded generation %d", gen.Number)
	}

	if out, err := lt.migrate(t, false, rfc3339migrate.BatchSize); err != nil {
		t.Fatalf("migrate = %v\n%s", err, out)
	}
	out, err = lt.apply(t, false)
	if err != nil || !strings.Contains(out, "Recorded generation 3 of schema events") {
		t.Fatalf("apply after the migration = %v\n%s", err, out)
	}
	for _, e := range lt.active(t, 100).Entries {
		if e.Column == forma.MainColumnText02 && e.Encoding != forma.MainColumnEncodingRFC3339 {
			t.Fatalf("generation 3 records %+v, want rfc3339", e)
		}
	}
	out, err = lt.migrate(t, false, rfc3339migrate.BatchSize)
	if err != nil || !strings.Contains(out, "No active generation records the iso8601 encoding") {
		t.Fatalf("migrate after the apply = %v\n%s", err, out)
	}
}
