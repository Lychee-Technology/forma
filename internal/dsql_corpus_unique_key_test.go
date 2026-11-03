package internal

import (
	"context"
	"testing"

	"github.com/lychee-technology/forma/internal/dsqllint/corpus"
	"github.com/pashagolub/pgxmock/v4"
)

const (
	originInsertKeys   = "internal/postgres_persistent_repository_unique_key_reserve.go:insertKeyEntries"
	originKeyOwners    = "internal/postgres_persistent_repository_unique_key_reserve.go:keyEntryOwners"
	originUndoReserve  = "internal/postgres_persistent_repository_unique_key_reserve.go:DBPersistentRecordRepository.undoReservation"
	originHeldKeys     = "internal/postgres_persistent_repository_unique_keys.go:heldUniqueKeys"
	originKeyOwner     = "internal/postgres_persistent_repository_unique_keys.go:uniqueKeyOwner"
	originApplyRelease = "internal/postgres_persistent_repository_unique_keys.go:DBPersistentRecordRepository.applyUniqueKeys"
)

// TestDSQLCorpusUniqueKeyStatements renders the unique-key DML of the write
// path (#651) for the DSQL lint gate (#740), in the order a create that
// meets a held value sends it: reserve two entries (one is taken), read the
// owner of the taken one, undo the reservation; then the reads and the
// release an update sends.
func TestDSQLCorpusUniqueKeyStatements(t *testing.T) {
	ctx := context.Background()
	var rec corpus.Recorder
	mock, err := pgxmock.NewConn(pgxmock.QueryMatcherOption(pgxmock.QueryMatcherFunc(rec.Match)))
	if err != nil {
		t.Fatalf("create mock conn: %v", err)
	}
	t.Cleanup(func() { _ = mock.Close(ctx) })
	mock.ExpectBegin()
	mock.ExpectQuery("").WithArgs(anyArgs(4)...).WillReturnRows(
		pgxmock.NewRows([]string{"ltbase_schema_id", "key_id", "key_value"}).AddRow(corpusSchemaID, int16(1), "a@example.com"))
	mock.ExpectQuery("").WithArgs(anyArgs(3)...).WillReturnRows(
		pgxmock.NewRows([]string{"n", "ltbase_row_id"}).AddRow(int64(1), corpusRowB))
	mock.ExpectExec("").WithArgs(anyArgs(4)...).WillReturnResult(pgxmock.NewResult("DELETE", 1))
	mock.ExpectQuery("").WithArgs(anyArgs(2)...).WillReturnRows(pgxmock.NewRows([]string{"key_id", "key_value"}).AddRow(int16(1), "a@example.com"))
	mock.ExpectQuery("").WithArgs(anyArgs(3)...).WillReturnRows(pgxmock.NewRows([]string{"ltbase_row_id"}).AddRow(corpusRowB))
	mock.ExpectExec("").WithArgs(anyArgs(4)...).WillReturnResult(pgxmock.NewResult("DELETE", 1))
	tx, err := mock.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	email := uniqueKeyEntry{keyID: 1, keyName: "email", value: "a@example.com"}
	phone := uniqueKeyEntry{keyID: 2, keyName: "phone", value: "+1-555-0100"}
	entries := []claimedEntry{{schemaID: corpusSchemaID, rowID: corpusRowA, entry: email}, {schemaID: corpusSchemaID, rowID: corpusRowA, entry: phone}}
	inserted, err := insertKeyEntries(ctx, tx, corpusTables, entries)
	if err != nil {
		t.Fatalf("insertKeyEntries: %v", err)
	}
	if _, err := keyEntryOwners(ctx, tx, corpusTables, entriesNotIn(entries, inserted)); err != nil {
		t.Fatalf("keyEntryOwners: %v", err)
	}
	repo := &DBPersistentRecordRepository{}
	if err := repo.undoReservation(ctx, tx, corpusTables, &uniqueKeyReservation{inserted: inserted}); err != nil {
		t.Fatalf("undoReservation: %v", err)
	}
	specs := []uniqueKeySpec{{id: 1, name: "email"}}
	if _, err := heldUniqueKeys(ctx, tx, corpusTables, specs, corpusSchemaID, corpusRowA); err != nil {
		t.Fatalf("heldUniqueKeys: %v", err)
	}
	if _, _, err := uniqueKeyOwner(ctx, tx, corpusTables, corpusSchemaID, 1, email.value); err != nil {
		t.Fatalf("uniqueKeyOwner: %v", err)
	}
	claim := &uniqueKeyClaim{schemaID: corpusSchemaID, rowID: corpusRowA, release: []uniqueKeyEntry{email}}
	if err := repo.applyUniqueKeys(ctx, tx, corpusTables, claim); err != nil {
		t.Fatalf("applyUniqueKeys: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("the unique-key path sent other statements than recorded: %v", err)
	}
	corpus.Export(t, corpus.Unit{Name: "internal-dsql-unique-keys", Statements: rec.Take(t,
		originInsertKeys, originKeyOwners, originUndoReserve, originHeldKeys, originKeyOwner, originApplyRelease)})
}
