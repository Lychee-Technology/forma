package internal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/model"
	"github.com/lychee-technology/forma/internal/uniquekey"
)

// UniqueKeyBuildBatchSize bounds the rows each statement of a build reads or
// writes. Every write statement is its own transaction, so this keeps each
// one under Aurora DSQL's documented 3,000-row transaction limit (#651).
const UniqueKeyBuildBatchSize = 1000

// uniqueKeyBuildBatchSize is the batch size a build uses; tests lower it to
// cross page boundaries with a handful of rows.
var uniqueKeyBuildBatchSize = UniqueKeyBuildBatchSize

// UniqueKeyBuildResult is what a build did to one key.
type UniqueKeyBuildResult struct {
	Schema string
	Key    string
	// Entries is the number of key-table entries the build wrote.
	Entries int
	// AlreadyBuilt says the key was built for its current attributes, so the
	// build wrote nothing for it.
	AlreadyBuilt bool
	// Rebuilt says the key was built before the run, which wrote its entries
	// again because the request asked for a rebuild.
	Rebuilt bool
}

// UniqueKeyBuildReport is what one build-unique-keys run did.
type UniqueKeyBuildReport struct {
	Keys []UniqueKeyBuildResult
	// DroppedKeyIDs are the keys the schema no longer declares whose
	// entries and built state the build removed.
	DroppedKeyIDs []int16
}

// UniqueKeyDuplicatesError refuses a build because live rows already share a
// value of the key. Each group lists the rows that share one value, so the
// operator can resolve them; the values are left out, as in every unique-key
// error, because they may be sensitive.
type UniqueKeyDuplicatesError struct {
	Schema string
	Key    string
	Groups [][]uuid.UUID
}

func (e *UniqueKeyDuplicatesError) Error() string {
	groups := make([]string, len(e.Groups))
	for i, group := range e.Groups {
		ids := make([]string, len(group))
		for j, id := range group {
			ids[j] = id.String()
		}
		groups[i] = "[" + strings.Join(ids, " ") + "]"
	}
	return fmt.Sprintf("schema %s: %d values of unique key %s are shared by more than one live row; "+
		"resolve these rows (each group shares one value) and build again: %s",
		e.Schema, len(e.Groups), e.Key, strings.Join(groups, " "))
}

// UniqueKeyBuildRequest names what one build builds.
type UniqueKeyBuildRequest struct {
	Schema string
	// Key is the one key to build. "" builds every key the schema declares,
	// and drops the entries of every key it no longer declares.
	Key string
	// Rebuild builds a key again although it is built for its current
	// attributes: for entries that a writer which did not maintain the key,
	// such as an older binary, left short of the rows.
	Rebuild bool
}

// BuildUniqueKeys builds the keys req names from the schema's live rows. It
// is offline: every writer must be stopped while it runs, since a write it
// misses would leave the key table short of the rows.
//
// A key already built for its current attributes is left alone unless
// req.Rebuild, and one built for other attributes is refused, since a key's
// attributes never change under its id. Duplicates in any key fail the
// whole run before it writes anything, with a UniqueKeyDuplicatesError per
// key. Otherwise each key is unbuilt first, its entries rewritten in batches
// of UniqueKeyBuildBatchSize, and its state row written last, so an
// interrupted build leaves the key unbuilt: the server refuses to start until
// the build is run again.
func (c *UniqueKeyCatalog) BuildUniqueKeys(
	ctx context.Context, db UniqueKeyStore, tables model.StorageTables, req UniqueKeyBuildRequest,
) (UniqueKeyBuildReport, error) {
	var report UniqueKeyBuildReport
	schemaID, specs, err := c.keysToBuild(req)
	if err != nil {
		return report, fmt.Errorf("resolve the keys to build: %w", err)
	}
	built, err := readUniqueKeyState(ctx, db, tables.EntityMain)
	if err != nil {
		return report, fmt.Errorf("check which keys of schema %s are built: %w", req.Schema, err)
	}
	todo, err := c.keysPendingBuild(req, schemaID, specs, built, &report)
	if err != nil {
		return report, fmt.Errorf("check the keys of schema %s: %w", req.Schema, err)
	}
	if err := c.buildKeys(ctx, db, tables, schemaID, todo, built, &report); err != nil {
		return report, fmt.Errorf("build %d unique keys: %w", len(todo), err)
	}
	if req.Key != "" {
		return report, nil
	}
	for _, keyID := range builtKeyIDs(built, schemaID) {
		if slices.ContainsFunc(specs, func(s uniqueKeySpec) bool { return s.id == keyID }) {
			continue
		}
		if err := unbuildKey(ctx, db, tables.EntityMain, schemaID, keyID); err != nil {
			return report, fmt.Errorf("drop key id %d that schema %s no longer declares: %w", keyID, req.Schema, err)
		}
		report.DroppedKeyIDs = append(report.DroppedKeyIDs, keyID)
	}
	return report, nil
}

// keysToBuild resolves the schema and the keys req names.
func (c *UniqueKeyCatalog) keysToBuild(req UniqueKeyBuildRequest) (int16, []uniqueKeySpec, error) {
	schemaID, ok := c.schemaIDNamed(req.Schema)
	if !ok {
		return 0, nil, fmt.Errorf("schema %s is not registered", req.Schema)
	}
	if req.Key == "" {
		return schemaID, c.keysOf(schemaID), nil
	}
	spec, ok := c.keyNamed(schemaID, req.Key)
	if !ok {
		return 0, nil, fmt.Errorf("schema %s declares no unique key %s", req.Schema, req.Key)
	}
	return schemaID, []uniqueKeySpec{spec}, nil
}

// keysPendingBuild answers the keys of specs the build writes, reporting
// each one it skips as already built, and refuses a key built for other
// attributes.
func (c *UniqueKeyCatalog) keysPendingBuild(
	req UniqueKeyBuildRequest, schemaID int16, specs []uniqueKeySpec, built map[[2]int16]string, report *UniqueKeyBuildReport,
) ([]uniqueKeySpec, error) {
	var todo []uniqueKeySpec
	for _, spec := range specs {
		hash, ok := built[[2]int16{schemaID, spec.id}]
		switch {
		case ok && hash != spec.hash:
			return nil, fmt.Errorf("unique key %s of schema %s (id %d) was built for another attribute list: %s",
				spec.name, req.Schema, spec.id, changedKeyAdvice)
		case ok && !req.Rebuild:
			report.Keys = append(report.Keys, UniqueKeyBuildResult{Schema: req.Schema, Key: spec.name, AlreadyBuilt: true})
		default:
			todo = append(todo, spec)
		}
	}
	return todo, nil
}

// buildKeys scans schemaID's live rows once for every key of specs, refuses
// duplicates, then writes each key. built is the keys built before the run.
func (c *UniqueKeyCatalog) buildKeys(
	ctx context.Context, db UniqueKeyStore, tables model.StorageTables, schemaID int16, specs []uniqueKeySpec,
	built map[[2]int16]string, report *UniqueKeyBuildReport,
) error {
	if len(specs) == 0 {
		return nil
	}
	schemaName := c.schemaName(schemaID)
	owners, err := scanUniqueKeyOwners(ctx, db, tables, schemaID, specs)
	if err != nil {
		return fmt.Errorf("scan the live rows of schema %s: %w", schemaName, err)
	}
	var duplicates []error
	for _, spec := range specs {
		if err := duplicatesOf(schemaName, spec.name, owners[spec.id]); err != nil {
			duplicates = append(duplicates, err)
		}
	}
	if len(duplicates) > 0 {
		return errors.Join(duplicates...)
	}
	for _, spec := range specs {
		n, err := writeBuiltKey(ctx, db, tables.EntityMain, schemaID, spec, owners[spec.id])
		if err != nil {
			return fmt.Errorf("build unique key %s of schema %s: %w", spec.name, schemaName, err)
		}
		_, rebuilt := built[[2]int16{schemaID, spec.id}]
		report.Keys = append(report.Keys, UniqueKeyBuildResult{Schema: schemaName, Key: spec.name, Entries: n, Rebuilt: rebuilt})
	}
	return nil
}

// scanUniqueKeyOwners reads schemaID's live rows in row-ID keyset pages and
// answers, per key of specs, the rows that hold each value. Each row is
// decoded exactly as a read decodes it, so the build computes the values the
// write path computes.
func scanUniqueKeyOwners(
	ctx context.Context, db UniqueKeyStore, tables model.StorageTables, schemaID int16, specs []uniqueKeySpec,
) (map[int16]map[string][]uuid.UUID, error) {
	query := rowSelectWithAttributes(tables.EntityMain, tables.EAVData) + `
WHERE m.ltbase_schema_id = $1 AND m.ltbase_deleted_at IS NULL AND m.ltbase_row_id > $2
ORDER BY m.ltbase_row_id
LIMIT $3`
	owners := make(map[int16]map[string][]uuid.UUID, len(specs))
	for _, spec := range specs {
		owners[spec.id] = map[string][]uuid.UUID{}
	}
	after := uuid.Nil
	for {
		records, err := scanRowPage(ctx, db, query, schemaID, after)
		if err != nil {
			return nil, fmt.Errorf("scan the rows of schema id %d: %w", schemaID, err)
		}
		for _, record := range records {
			entries, err := uniqueKeyEntriesOf(specs, record)
			if err != nil {
				return nil, fmt.Errorf("compute the unique keys of row %s: %w", record.RowID, err)
			}
			for _, e := range entries {
				owners[e.keyID][e.value] = append(owners[e.keyID][e.value], record.RowID)
			}
		}
		if len(records) < uniqueKeyBuildBatchSize {
			return owners, nil
		}
		after = records[len(records)-1].RowID
	}
}

// scanRowPage reads the page of whole rows after row ID after.
func scanRowPage(ctx context.Context, db UniqueKeyStore, query string, schemaID int16, after uuid.UUID) ([]*model.PersistentRecord, error) {
	rows, err := db.Query(ctx, query, schemaID, after, uniqueKeyBuildBatchSize)
	if err != nil {
		return nil, fmt.Errorf("read rows after %s: %w", after, err)
	}
	defer rows.Close()
	var records []*model.PersistentRecord
	for rows.Next() {
		buffers := newColumnScanBuffers()
		var attrsJSON []byte
		if err := rows.Scan(append(buildScanArgs(buffers), &attrsJSON)...); err != nil {
			return nil, fmt.Errorf("scan a row after %s: %w", after, err)
		}
		record := buildRecordFromScanBuffers(buffers)
		if err := model.ParseAttributesJSON(attrsJSON, record); err != nil {
			return nil, fmt.Errorf("parse eav attributes for %s: %w", record.RowID, err)
		}
		model.CleanupEmptyMaps(record)
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read rows after %s: %w", after, err)
	}
	return records, nil
}

// duplicatesOf answers the UniqueKeyDuplicatesError of the values more than
// one row holds, nil when there are none. Groups and their rows are sorted,
// so the listing is stable across runs.
func duplicatesOf(schemaName, keyName string, owners map[string][]uuid.UUID) error {
	var groups [][]uuid.UUID
	for _, rows := range owners {
		if len(rows) > 1 {
			group := slices.Clone(rows)
			slices.SortFunc(group, compareUUIDs)
			groups = append(groups, group)
		}
	}
	if len(groups) == 0 {
		return nil
	}
	slices.SortFunc(groups, func(a, b []uuid.UUID) int { return slices.CompareFunc(a, b, compareUUIDs) })
	return &UniqueKeyDuplicatesError{Schema: schemaName, Key: keyName, Groups: groups}
}

func compareUUIDs(a, b uuid.UUID) int { return bytes.Compare(a[:], b[:]) }

// writeBuiltKey replaces spec's entries with owners, each value held by one
// row, and records the key as built for its current attributes.
func writeBuiltKey(ctx context.Context, db UniqueKeyStore, entityMain string, schemaID int16, spec uniqueKeySpec, owners map[string][]uuid.UUID) (int, error) {
	if err := unbuildKey(ctx, db, entityMain, schemaID, spec.id); err != nil {
		return 0, fmt.Errorf("clear the key before writing it: %w", err)
	}
	values := make([]string, 0, len(owners))
	for value := range owners {
		values = append(values, value)
	}
	slices.Sort(values)
	insert := fmt.Sprintf(`INSERT INTO %s (ltbase_schema_id, key_id, key_value, ltbase_row_id)
SELECT $1, $2, u.key_value, u.ltbase_row_id FROM unnest($3::text[], $4::uuid[]) AS u(key_value, ltbase_row_id)`,
		sanitizeIdentifier(uniquekey.KeyTable(entityMain)))
	for start := 0; start < len(values); start += uniqueKeyBuildBatchSize {
		batch := values[start:min(start+uniqueKeyBuildBatchSize, len(values))]
		rowIDs := make([]uuid.UUID, len(batch))
		for i, value := range batch {
			rowIDs[i] = owners[value][0]
		}
		if _, err := db.Exec(ctx, insert, schemaID, spec.id, batch, rowIDs); err != nil {
			return 0, fmt.Errorf("write entries %d to %d: %w", start, start+len(batch), err)
		}
	}
	state := fmt.Sprintf(`INSERT INTO %s (ltbase_schema_id, key_id, attributes_hash, built_at) VALUES ($1, $2, $3, $4)
ON CONFLICT (ltbase_schema_id, key_id) DO UPDATE SET attributes_hash = EXCLUDED.attributes_hash, built_at = EXCLUDED.built_at`,
		sanitizeIdentifier(uniquekey.StateTable(entityMain)))
	if _, err := db.Exec(ctx, state, schemaID, spec.id, spec.hash, time.Now().UnixMilli()); err != nil {
		return 0, fmt.Errorf("record the key as built: %w", err)
	}
	return len(values), nil
}

// unbuildKey deletes a key's state row, then its entries in batches. The
// state row goes first, so a failure part way leaves the key unbuilt rather
// than built over a partial entry set.
func unbuildKey(ctx context.Context, db UniqueKeyStore, entityMain string, schemaID, keyID int16) error {
	state := fmt.Sprintf("DELETE FROM %s WHERE ltbase_schema_id = $1 AND key_id = $2", sanitizeIdentifier(uniquekey.StateTable(entityMain)))
	if _, err := db.Exec(ctx, state, schemaID, keyID); err != nil {
		return fmt.Errorf("unbuild key id %d: %w", keyID, err)
	}
	keyTable := sanitizeIdentifier(uniquekey.KeyTable(entityMain))
	entries := fmt.Sprintf(`DELETE FROM %[1]s WHERE ltbase_schema_id = $1 AND key_id = $2 AND key_value IN (
	SELECT key_value FROM %[1]s WHERE ltbase_schema_id = $1 AND key_id = $2 LIMIT $3)`, keyTable)
	for {
		tag, err := db.Exec(ctx, entries, schemaID, keyID, uniqueKeyBuildBatchSize)
		if err != nil {
			return fmt.Errorf("delete the entries of key id %d: %w", keyID, err)
		}
		if tag.RowsAffected() == 0 {
			return nil
		}
	}
}
