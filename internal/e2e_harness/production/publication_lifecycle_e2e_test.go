//go:build e2e

package production

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lychee-technology/forma/internal/schemaledger/ledgertest"
)

const (
	lifecycleAnchorID   int16 = 103
	lifecycleSchemaID   int16 = 104
	lifecycleSchemaName       = "e2e_lifecycle"
)

// TestPublicationMatrix_FullLifecycleAndPagination exercises a single schema
// across every Protocol V2 lifecycle state and transition end to end:
// UNPUBLISHED -> flush -> init -> re-init -> compact -> reconcile --gc ->
// real `forma-tools drop-schema` (retire + purge) -> residue refusal ->
// real `forma-tools init-db` re-registration (epoch+2, UNPUBLISHED) ->
// fresh flush, checking filtered, sorted, offset-paginated, keyset-paginated,
// and unfiltered recreated-schema queries against the oracle at each stage.
func TestPublicationMatrix_FullLifecycleAndPagination(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	metrics := &combinedMetrics{}
	env := NewEnv(t, SharedCluster(t), WithBreaker(1, time.Minute), WithMetricEmitter(metrics))

	toolsBin := buildFormaToolsBinary(ctx, t)
	schemaDir, lifecycle := prepareLifecycleSchema(ctx, t, env, toolsBin)

	// Stage 1: UNPUBLISHED — filtered, sorted, offset and keyset queries
	// answer from PG alone with zero DuckDB statements.
	seed := seedMatrixRows(ctx, t, env, lifecycle, "alpha", 8, 100)
	deletedID := seed[0].RowID
	mustApplyEvents(ctx, t, env, "delete before publish", DeleteEvent(lifecycle, deletedID))
	assertFilteredAndPaginated(ctx, t, env, lifecycle, false)

	// Stage 2: First cdc-flush (UNPUBLISHED -> PUBLISHED, g1) + hot mutations.
	mustFlush(ctx, t, env)
	g1 := loadSchemaManifest(ctx, t, env, lifecycle)
	if g1.Generation != 1 {
		t.Fatalf("first flush generation = %d, want 1", g1.Generation)
	}
	seedMatrixRows(ctx, t, env, lifecycle, "alpha", 4, 200)
	mustApplyEvents(ctx, t, env, "update flushed row", UpdateEvent(lifecycle, seed[1].RowID, map[string]any{"title": "alpha-updated", "count": float64(999)}))
	assertFilteredAndPaginated(ctx, t, env, lifecycle, true)

	// Stage 3: cdc-init (g2) + delta flush (g3) + re-init (g4, new lineage).
	if _, err := env.RunInit(ctx, lifecycle); err != nil {
		t.Fatalf("first cdc-init: %v", err)
	}
	env.MarkOnboarded(ctx, lifecycle)
	g2 := loadSchemaManifest(ctx, t, env, lifecycle)
	seedMatrixRows(ctx, t, env, lifecycle, "beta", 5, 300)
	mustFlush(ctx, t, env)
	if _, err := env.RunInit(ctx, lifecycle); err != nil {
		t.Fatalf("re-init: %v", err)
	}
	env.MarkOnboarded(ctx, lifecycle)
	g4 := loadSchemaManifest(ctx, t, env, lifecycle)
	if g4.Lineage == g2.Lineage || g4.Generation <= g2.Generation {
		t.Fatalf("re-init must advance generation (%d -> %d) and lineage (%s -> %s)", g2.Generation, g4.Generation, g2.Lineage, g4.Lineage)
	}
	assertFilteredAndPaginated(ctx, t, env, lifecycle, true)

	// Stage 4: Flush delta + compaction + manifest-reconcile --gc.
	seedMatrixRows(ctx, t, env, lifecycle, "alpha", 4, 400)
	mustFlush(ctx, t, env)
	compRes, err := env.RunCompactionWith(ctx, lifecycle, CompactionOverrides{TargetBaseSizeBytes: 1})
	if err != nil {
		t.Fatalf("compaction: %v", err)
	}
	if compRes.Generation == 0 {
		t.Fatalf("expected compaction to commit a compact generation, got %+v", compRes)
	}
	if _, err := env.reconciler(t, lifecycle, reconcileOptions(true)).Run(ctx); err != nil {
		t.Fatalf("manifest-reconcile --gc retire pass: %v", err)
	}
	time.Sleep(reconcileGrace)
	recRep, err := env.reconciler(t, lifecycle, reconcileOptions(true)).Run(ctx)
	if err != nil {
		t.Fatalf("manifest-reconcile --gc collect pass: %v", err)
	}
	if len(recRep.Schemas) != 1 || recRep.Schemas[0].Collected == nil || len(recRep.Schemas[0].Collected.Manifests) == 0 {
		t.Fatalf("expected --gc to delete expired superseded manifests, got %+v", recRep)
	}
	assertFilteredAndPaginated(ctx, t, env, lifecycle, true)
	assertDeletedAbsent(ctx, t, env, lifecycle, deletedID)

	verifyDropAndRecreateLifecycle(ctx, t, env, toolsBin, schemaDir, lifecycle, metrics)
	metrics.assertClean(t, "full lifecycle")
}

// verifyDropAndRecreateLifecycle drives the real production `forma-tools
// drop-schema` and `forma-tools init-db` tools: retires the registration and
// purges all residue tables, proves Activate refuses leftover residue, resumes
// purge by --schema-id, re-registers under the same schema ID at epoch+2,
// confirms stale process pins stay fenced, and verifies full unfiltered row
// identities against the oracle before and after the first new-epoch flush.
func verifyDropAndRecreateLifecycle(ctx context.Context, t *testing.T, env *Env, toolsBin, schemaDir string, lifecycle SchemaRef, metrics *combinedMetrics) {
	t.Helper()
	preDrop := loadSchemaManifest(ctx, t, env, lifecycle)
	preDropIDs := collectPreDropRowIDs(t, env, lifecycle)

	// Step 1: Real production drop-schema (registry + ledger delete, Reset to DROPPED, chunked purge).
	out, err := runFormaTool(ctx, toolsBin, env, "drop-schema", "--schema-name", lifecycle.Name)
	if err != nil {
		t.Fatalf("forma-tools drop-schema failed: %v\n%s", err, out)
	}
	assertSchemaDroppedAndPurged(ctx, t, env, lifecycle, preDrop.Epoch+1, preDrop.Generation)
	assertPublicationFault(ctx, t, env, lifecycle, metrics, false)
	if _, err := env.RunCompaction(ctx, lifecycle); err == nil {
		t.Fatal("compactor against DROPPED schema must fail closed")
	}
	// drop-schema deleted lifecycle from schema_registry, so cdc-init must
	// publish nothing and leave the DROPPED record untouched.
	if initRep, err := env.RunInit(ctx, lifecycle); err == nil && (len(initRep.Schemas) != 0 || initRep.RowsExported != 0) {
		t.Fatalf("cdc-init against DROPPED unregistered schema published unexpectedly: %+v", initRep)
	}

	// Step 2: Simulate a stray write / interrupted purge while DROPPED:
	// cdc-flush must fail closed on the dirty entry without publishing, and
	// init-db must refuse re-registration via Activate's residue check.
	residueRowID := uuid.New()
	env.ExecSQL(ctx,
		"INSERT INTO entity_main (ltbase_schema_id, ltbase_row_id, ltbase_created_at, ltbase_updated_at) VALUES ($1, $2, 1, 1)",
		lifecycle.ID, residueRowID,
	)
	env.ExecSQL(ctx,
		"INSERT INTO change_log (schema_id, row_id, changed_at, flushed_at) VALUES ($1, $2, 1, 0)",
		lifecycle.ID, residueRowID,
	)
	if _, err := env.RunFlush(ctx); err == nil {
		t.Fatal("cdc-flush against DROPPED schema with dirty entry must fail closed")
	}
	out, err = runFormaTool(ctx, toolsBin, env, "init-db", "--schema-dir", schemaDir)
	if err == nil || !strings.Contains(out, "dropped schema's rows not purged") {
		t.Fatalf("init-db with leftover residue must fail with ErrResidue, err=%v\n%s", err, out)
	}
	assertSchemaDroppedAndPurgedCounts(ctx, t, env, lifecycle, preDrop.Epoch+1, preDrop.Generation, 1, 1)

	// Step 3: Resume purge via drop-schema --schema-id (registry row is already gone).
	out, err = runFormaTool(ctx, toolsBin, env, "drop-schema", "--schema-id", fmt.Sprintf("%d", lifecycle.ID))
	if err != nil {
		t.Fatalf("resume drop-schema --schema-id failed: %v\n%s", err, out)
	}
	assertSchemaDroppedAndPurged(ctx, t, env, lifecycle, preDrop.Epoch+1, preDrop.Generation)

	// Step 4: Real production init-db re-registers lifecycle under the exact same ID at epoch+2.
	out, err = runFormaTool(ctx, toolsBin, env, "init-db", "--schema-dir", schemaDir)
	if err != nil {
		t.Fatalf("forma-tools init-db re-registration failed: %v\n%s", err, out)
	}
	assertSchemaReRegistered(ctx, t, env, lifecycle, preDrop.Epoch+2, preDrop.Generation)

	// Stale process pin (loaded at epoch 1) still fails closed even under degraded mode.
	assertPublicationFault(ctx, t, env, lifecycle, metrics, false)

	// Step 5: Fresh process startup reloads registry/epochs/metadata and serves cleanly.
	if err := env.ReloadSchemas(ctx, schemaDir); err != nil {
		t.Fatalf("reload schemas after recreate: %v", err)
	}
	if err := env.ReopenDuckDB(); err != nil {
		t.Fatalf("reopen process after schema recreate: %v", err)
	}
	env.ResetSchemaEvents(lifecycle)

	verifyRecreatedSchemaIdentities(ctx, t, env, lifecycle, preDrop.Epoch+2, preDrop.Generation, preDropIDs)
}

func verifyRecreatedSchemaIdentities(ctx context.Context, t *testing.T, env *Env, lifecycle SchemaRef, wantEpoch, preDropGen int64, preDropIDs map[uuid.UUID]struct{}) {
	t.Helper()
	unfiltered := Query{Schema: lifecycle, Sorts: []Sort{{Attr: "count"}}, Limit: 200}

	// Empty recreated schema must return 0 rows with zero DuckDB statements.
	emptyRes := env.AssertQueryMatches(ctx, unfiltered)
	if len(emptyRes.Records) != 0 || planHasEngine(emptyRes.Plan, "duckdb") {
		t.Fatalf("empty recreated UNPUBLISHED query = %d rows (duckdb=%t), want 0 rows without DuckDB",
			len(emptyRes.Records), planHasEngine(emptyRes.Plan, "duckdb"))
	}

	// Seed 3 new rows whose counts deliberately overlap the old epoch (100..102)
	// so no range filter could hide resurrected pre-drop rows.
	rebornEvents := seedMatrixRows(ctx, t, env, lifecycle, "reborn", 3, 100)
	wantIDs := make(map[uuid.UUID]struct{}, len(rebornEvents))
	for _, ev := range rebornEvents {
		wantIDs[ev.RowID] = struct{}{}
	}

	beforeFlush := env.AssertQueryMatches(ctx, unfiltered)
	if planHasEngine(beforeFlush.Plan, "duckdb") {
		t.Fatalf("recreated UNPUBLISHED query unexpectedly executed DuckDB: %+v", beforeFlush.Plan.Sources)
	}
	assertExactRowIDs(t, "recreated UNPUBLISHED", beforeFlush, wantIDs, preDropIDs)

	mustFlush(ctx, t, env)
	postRecreate := loadSchemaManifest(ctx, t, env, lifecycle)
	if postRecreate.Epoch != wantEpoch || postRecreate.Generation != preDropGen+1 {
		t.Fatalf("post-recreate flush = (epoch=%d, gen=%d), want (epoch=%d, gen=%d)",
			postRecreate.Epoch, postRecreate.Generation, wantEpoch, preDropGen+1)
	}

	afterFlush := env.AssertQueryMatches(ctx, unfiltered)
	if !planHasEngine(afterFlush.Plan, "duckdb") {
		t.Fatalf("recreated PUBLISHED query did not scan DuckDB: %+v", afterFlush.Plan.Sources)
	}
	assertExactRowIDs(t, "recreated PUBLISHED", afterFlush, wantIDs, preDropIDs)
}

func assertExactRowIDs(t *testing.T, label string, res *QueryResult, wantIDs, preDropIDs map[uuid.UUID]struct{}) {
	t.Helper()
	if len(res.Records) != len(wantIDs) {
		t.Fatalf("%s: got %d rows, want %d", label, len(res.Records), len(wantIDs))
	}
	for _, rec := range res.Records {
		if _, old := preDropIDs[rec.RowID]; old {
			t.Fatalf("%s: pre-drop row %s resurrected in new epoch", label, rec.RowID)
		}
		if _, ok := wantIDs[rec.RowID]; !ok {
			t.Fatalf("%s: unexpected row %s not in new-epoch seed set", label, rec.RowID)
		}
	}
}

func collectPreDropRowIDs(t *testing.T, env *Env, schema SchemaRef) map[uuid.UUID]struct{} {
	t.Helper()
	ids := make(map[uuid.UUID]struct{})
	for _, ev := range env.Events() {
		if ev.Schema.ID == schema.ID && ev.RowID != uuid.Nil {
			ids[ev.RowID] = struct{}{}
		}
	}
	if len(ids) == 0 {
		t.Fatalf("expected non-empty pre-drop row IDs for schema %s", schema.Name)
	}
	return ids
}

func assertSchemaDroppedAndPurged(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, wantEpoch, wantGen int64) {
	t.Helper()
	assertSchemaDroppedAndPurgedCounts(ctx, t, env, schema, wantEpoch, wantGen, 0, 0)
}

func assertSchemaDroppedAndPurgedCounts(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, wantEpoch, wantGen, wantMainRows, wantChangeLogRows int64) {
	t.Helper()
	state, epoch, gen := readSchemaRecordState(ctx, t, env, schema)
	if state != "DROPPED" || epoch != wantEpoch || gen != wantGen {
		t.Fatalf("record = (state=%s, epoch=%d, gen=%d), want (DROPPED, %d, %d)", state, epoch, gen, wantEpoch, wantGen)
	}
	var regCount int64
	if err := env.Pool.QueryRow(ctx, "SELECT COUNT(*) FROM schema_registry WHERE schema_id = $1 OR schema_name = $2", schema.ID, schema.Name).Scan(&regCount); err != nil {
		t.Fatalf("count schema_registry rows: %v", err)
	}
	if regCount != 0 {
		t.Fatalf("dropped schema %s still has %d row(s) in schema_registry", schema.Name, regCount)
	}
	checks := []struct {
		table string
		col   string
		want  int64
	}{
		{"entity_main", "ltbase_schema_id", wantMainRows},
		{"eav_data", "schema_id", 0},
		{"change_log", "schema_id", wantChangeLogRows},
		{"entity_main_unique_key", "ltbase_schema_id", 0},
		{"entity_main_unique_key_state", "ltbase_schema_id", 0},
	}
	for _, c := range checks {
		var n int64
		if err := env.Pool.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = $1", c.table, c.col), schema.ID).Scan(&n); err != nil {
			t.Fatalf("count residue in %s: %v", c.table, err)
		}
		if n != c.want {
			t.Fatalf("residue table %s has %d row(s) for schema %d, want %d", c.table, n, schema.ID, c.want)
		}
	}
}

func assertSchemaReRegistered(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, wantEpoch, wantGen int64) {
	t.Helper()
	var gotID int16
	if err := env.Pool.QueryRow(ctx, "SELECT schema_id FROM schema_registry WHERE schema_name = $1", schema.Name).Scan(&gotID); err != nil {
		t.Fatalf("lookup re-registered schema %s: %v", schema.Name, err)
	}
	if gotID != schema.ID {
		t.Fatalf("re-registered schema %s got id %d, want %d", schema.Name, gotID, schema.ID)
	}
	state, epoch, gen := readSchemaRecordState(ctx, t, env, schema)
	if state != "UNPUBLISHED" || epoch != wantEpoch || gen != wantGen {
		t.Fatalf("re-registered record = (state=%s, epoch=%d, gen=%d), want (UNPUBLISHED, %d, %d)", state, epoch, gen, wantEpoch, wantGen)
	}
}

// prepareLifecycleSchema copies the bundled fixtures into a temp schema
// directory alongside e2e_anchor (registered at ID 103, outside the 100-102
// benchmark range) and e2e_lifecycle (a copy of e2e_wide), then runs real
// `forma-tools init-db` so e2e_lifecycle is registered at ID 104 with its
// ledger and UNPUBLISHED publication record. Dropping e2e_lifecycle leaves 103
// as the highest registered ID so a subsequent `init-db` deterministically
// re-registers e2e_lifecycle at the exact same ID 104.
func prepareLifecycleSchema(ctx context.Context, t *testing.T, env *Env, toolsBin string) (string, SchemaRef) {
	t.Helper()
	schemaDir := t.TempDir()
	entries, err := os.ReadDir(FixtureSchemasDir())
	if err != nil {
		t.Fatalf("read fixture schemas dir: %v", err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		copySchemaFile(t, filepath.Join(FixtureSchemasDir(), entry.Name()), filepath.Join(schemaDir, entry.Name()))
	}
	copySchemaFile(t, filepath.Join(FixtureSchemasDir(), "e2e_simple.json"), filepath.Join(schemaDir, "e2e_anchor.json"))
	copySchemaFile(t, filepath.Join(FixtureSchemasDir(), "e2e_simple_attributes.json"), filepath.Join(schemaDir, "e2e_anchor_attributes.json"))
	copySchemaFile(t, filepath.Join(FixtureSchemasDir(), "e2e_wide.json"), filepath.Join(schemaDir, lifecycleSchemaName+".json"))
	copySchemaFile(t, filepath.Join(FixtureSchemasDir(), "e2e_wide_attributes.json"), filepath.Join(schemaDir, lifecycleSchemaName+"_attributes.json"))

	anchor := SchemaRef{ID: lifecycleAnchorID, Name: "e2e_anchor"}
	if err := RegisterSchemas(ctx, env.Pool, anchor); err != nil {
		t.Fatalf("register lifecycle anchor schema: %v", err)
	}
	if err := ledgertest.Seed(ctx, env.Pool, env.Tables.SchemaRegistry, schemaDir); err != nil {
		t.Fatalf("seed lifecycle anchor ledger generation: %v", err)
	}
	out, err := runFormaTool(ctx, toolsBin, env, "init-db", "--schema-dir", schemaDir)
	if err != nil {
		t.Fatalf("initial forma-tools init-db failed: %v\n%s", err, out)
	}
	lifecycle := SchemaRef{ID: lifecycleSchemaID, Name: lifecycleSchemaName}
	assertSchemaReRegistered(ctx, t, env, lifecycle, 1, 0)
	if err := env.ReloadSchemas(ctx, schemaDir); err != nil {
		t.Fatalf("reload env schemas for lifecycle: %v", err)
	}
	return schemaDir, lifecycle
}

func copySchemaFile(t *testing.T, src, dst string) {
	t.Helper()
	raw, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read %s: %v", src, err)
	}
	if err := os.WriteFile(dst, raw, 0o600); err != nil {
		t.Fatalf("write %s: %v", dst, err)
	}
}

func buildFormaToolsBinary(ctx context.Context, t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "forma-tools")
	cmd := exec.CommandContext(ctx, "go", "build", "-o", bin, "github.com/lychee-technology/forma/cmd/tools")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build forma-tools binary: %v\n%s", err, string(out))
	}
	return bin
}

func runFormaTool(ctx context.Context, bin string, env *Env, subcommand string, extra ...string) (string, error) {
	c := env.Cluster
	args := []string{
		subcommand,
		"--db-host", c.PGHost,
		"--db-port", c.PGPort,
		"--db-name", env.DBName,
		"--db-user", c.PGUser,
		"--db-password", c.PGPassword,
		"--db-ssl-mode", c.PGSSLMode,
		"--schema-table", env.Tables.SchemaRegistry,
		"--entity-main-table", env.Tables.EntityMain,
		"--eav-table", env.Tables.EAVData,
		"--change-log-table", env.Tables.ChangeLog,
	}
	args = append(args, extra...)
	cmd := exec.CommandContext(ctx, bin, args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}
