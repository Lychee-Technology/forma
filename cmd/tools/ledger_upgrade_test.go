package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lychee-technology/forma/internal/schemaledger"
	"github.com/lychee-technology/forma/internal/schemameta"
	"github.com/lychee-technology/forma/internal/testdb"
)

// Declarations of orders that apply-schema allows from the deployment's: qty
// widened, and a third attribute added.
var (
	upgradeOrdersWidened = "{" + boundAttr("qty", 1, "integer", "smallint_01") + `,"note":{"attributeID":2,"valueType":"text"}}`
	upgradeOrdersAdded   = "{" + boundAttr("qty", 1, "smallint", "smallint_01") +
		`,"note":{"attributeID":2,"valueType":"text"},"flag":{"attributeID":3,"valueType":"text"}}`
)

// TestLedgerUpgradeDDLOnAPreLedgerRegistry is the runbook's first step: the
// exact schemaledger.Statements, one a transaction, on a registry that holds
// schemas and stored data. Before it, both control-plane tools name the
// upgrade; after it, twice over, the registry keeps its schemas, none of them
// has a generation, and the ledger is empty.
func TestLedgerUpgradeDDLOnAPreLedgerRegistry(t *testing.T) {
	u := newUpgradeTarget(t)
	if err := u.registryAnswers(t, schemaledger.PointerColumn); err == nil {
		t.Fatal("the pre-ledger registry already has the active_generation column: the fixture proves nothing")
	}
	out, err := u.baseline(t, false, nil)
	if err == nil {
		t.Fatalf("baseline-schema before the ledger DDL passed:\n%s", out)
	}
	mentionsAll(t, "baseline-schema's refusal", err.Error(), "has no generation ledger", "baseline-schema")
	out, err = u.apply(t, u.pool, u.schemaDir, false)
	if err == nil {
		t.Fatalf("apply-schema before the ledger DDL passed:\n%s", out)
	}
	mentionsAll(t, "apply-schema's refusal", err.Error(), "has no generation ledger")

	u.addLedger(t)
	u.addLedger(t) // IF NOT EXISTS: a rerun of the step changes nothing
	if err := u.registryAnswers(t, schemaledger.PointerColumn); err != nil {
		t.Fatalf("the registry has no active_generation column after the ledger DDL: %v", err)
	}
	u.requirePointers(t, 0, 0)
	if gens, orphans := u.ledgerRows(t); len(gens) != 0 || orphans != 0 {
		t.Fatalf("the ledger DDL recorded generations %v and %d attribute rows", gens, orphans)
	}
}

// TestLedgerUpgradeBaselineWithoutAParquetTier is the runbook's third step
// with --no-parquet-tier: a dry run beside the old release reports the value
// the declaration does not admit, the baseline then refuses and records
// nothing for any schema, and once the data is corrected it records
// generation 1 of each schema, once.
func TestLedgerUpgradeBaselineWithoutAParquetTier(t *testing.T) {
	u := newUpgradeTarget(t)
	u.addLedger(t)
	bad := u.storeVisit(t, "12.5")

	for _, dryRun := range []bool{true, false} {
		out, err := u.baseline(t, dryRun, nil)
		if err == nil {
			t.Fatalf("baseline (dry run %t) passed with 12.5 stored for an integer:\n%s", dryRun, out)
		}
		mentionsAll(t, "the refusal", out, "Refused", "schema visits (id 101) attribute count (id 1) declared integer", "12.5")
		mentionsAll(t, "the refusal", err.Error(), "no generation was recorded")
		u.requirePointers(t, 0, 0) // orders is clean, and is not recorded either
	}

	u.deleteVisit(t, bad)
	out, err := u.baseline(t, true, nil)
	if err != nil {
		t.Fatalf("dry run on corrected data: %v\n%s", err, out)
	}
	mentionsAll(t, "the dry run", out, "Dry run: no generation was recorded.")
	u.requirePointers(t, 0, 0)

	out, err = u.baseline(t, false, nil)
	if err != nil {
		t.Fatalf("baseline on corrected data: %v\n%s", err, out)
	}
	mentionsAll(t, "the baseline", out, "Recorded generation 1 of schema orders", "Recorded generation 1 of schema visits")
	u.requirePointers(t, 1, 1)
	u.requireActiveIs(t, upgradeOrders, "orders", u.schemaDir, 1)
	u.requireActiveIs(t, upgradeVisits, "visits", u.schemaDir, 1)

	out, err = u.baseline(t, false, nil)
	if err != nil || strings.Count(out, "nothing to baseline") != 2 {
		t.Fatalf("a second baseline must leave both generations alone: %v\n%s", err, out)
	}
	u.requireLedger(t, []int64{1}, []int64{1})
}

// baselined is a target upgraded through the baseline: the ledger added and
// generation 1 of each schema recorded from the deployment's declarations.
func baselined(t *testing.T) *upgradeTarget {
	t.Helper()
	u := newUpgradeTarget(t)
	u.addLedger(t)
	if out, err := u.baseline(t, false, nil); err != nil {
		t.Fatalf("baseline: %v\n%s", err, out)
	}
	return u
}

// requireLedger fails unless the ledger holds exactly these generations of
// orders and visits, and no attribute row outside them.
func (u *upgradeTarget) requireLedger(t *testing.T, orders, visits []int64) {
	t.Helper()
	gens, orphans := u.ledgerRows(t)
	want := map[int16][]int64{upgradeOrders: orders, upgradeVisits: visits}
	if fmt.Sprint(gens) != fmt.Sprint(want) || orphans != 0 {
		t.Fatalf("the ledger holds generations %v and %d orphan attribute rows, want %v and none", gens, orphans, want)
	}
}

// TestLedgerUpgradeApplySchemaAfterTheBaseline: after the upgrade, an allowed
// change records the next generation and a dry run records nothing; a run
// with one refused change records nothing for any schema.
func TestLedgerUpgradeApplySchemaAfterTheBaseline(t *testing.T) {
	u := baselined(t)
	widened := u.declarationDir(t, upgradeOrdersWidened, upgradeVisitsAttrs)

	out, err := u.apply(t, u.pool, widened, true)
	if err != nil {
		t.Fatalf("dry run: %v\n%s", err, out)
	}
	mentionsAll(t, "the dry run", out, "Schema orders (id 100): generation 1 to 2", "Dry run")
	u.requirePointers(t, 1, 1)

	out, err = u.apply(t, u.pool, widened, false)
	if err != nil {
		t.Fatalf("apply: %v\n%s", err, out)
	}
	mentionsAll(t, "the apply", out, "Recorded generation 2 of schema orders", "Schema visits (id 101): generation 1 is the declaration")
	u.requirePointers(t, 2, 1)
	u.requireActiveIs(t, upgradeOrders, "orders", widened, 2)
	if out, err := u.apply(t, u.pool, widened, false); err != nil || strings.Count(out, "nothing to apply") != 2 {
		t.Fatalf("a second apply of the same files = %v\n%s", err, out)
	}

	// qty back to smallint narrows; visits' count widening alone is allowed.
	refused := u.declarationDir(t, upgradeOrdersAttrs, `{"count":{"attributeID":1,"valueType":"bigint"}}`)
	out, err = u.apply(t, u.pool, refused, false)
	if err == nil {
		t.Fatalf("an apply with a narrowing passed:\n%s", out)
	}
	mentionsAll(t, "the refusal", out, "Refused:", "schema orders (id 100)", "qty (id 1)")
	u.requirePointers(t, 2, 1)
	u.requireActiveIs(t, upgradeOrders, "orders", widened, 2)
	u.requireLedger(t, []int64{1, 2}, []int64{1})
}

// TestLedgerUpgradeGatesTheDataPlane is the runbook's fourth step: the
// data-plane load refuses the deployment until it has a baseline, starts on
// it afterwards, and the generation re-read a CDC batch makes before it
// publishes (schemaledger.RequireUnchanged, called by cdc-flush and cdc-init
// per batch) sees an apply recorded after the start.
func TestLedgerUpgradeGatesTheDataPlane(t *testing.T) {
	u := newUpgradeTarget(t)
	_, err := schemameta.NewFileSchemaRegistryContext(u.ctx, u.pool, u.registry, u.schemaDir)
	if err == nil {
		t.Fatal("the data plane started on a registry without the ledger")
	}
	mentionsAll(t, "the refusal before the ledger", err.Error(), "init-db", "baseline-schema")
	u.addLedger(t)
	_, err = schemameta.NewFileSchemaRegistryContext(u.ctx, u.pool, u.registry, u.schemaDir)
	if !errors.Is(err, schemaledger.ErrNotActive) {
		t.Fatalf("a start before the baseline = %v, want ErrNotActive", err)
	}
	mentionsAll(t, "the refusal before the baseline", err.Error(), "schema orders (id 100)", "schema visits (id 101)", "no baseline")
	if out, err := u.baseline(t, false, nil); err != nil {
		t.Fatalf("baseline: %v\n%s", err, out)
	}

	running, err := schemameta.NewFileSchemaRegistryContext(u.ctx, u.pool, u.registry, u.schemaDir)
	if err != nil {
		t.Fatalf("a start after the baseline: %v", err)
	}
	tracker, ok := running.(schemaledger.Tracker)
	if !ok {
		t.Fatal("the data plane's registry is not a Tracker, so no CDC batch would re-read the generation")
	}
	for _, id := range []int16{upgradeOrders, upgradeVisits} {
		if n, ok := tracker.LoadedGeneration(id); !ok || n != 1 {
			t.Fatalf("schema %d started on generation %d (%t), want 1", id, n, ok)
		}
		if err := schemaledger.RequireUnchanged(u.ctx, running, id); err != nil {
			t.Fatalf("the batch re-read of schema %d before any apply: %v", id, err)
		}
	}

	widened := u.declarationDir(t, upgradeOrdersWidened, upgradeVisitsAttrs)
	if out, err := u.apply(t, u.pool, widened, false); err != nil {
		t.Fatalf("apply: %v\n%s", err, out)
	}
	if err := schemaledger.RequireUnchanged(u.ctx, running, upgradeOrders); !errors.Is(err, schemaledger.ErrGenerationChanged) {
		t.Fatalf("the batch re-read of orders after an apply = %v, want ErrGenerationChanged", err)
	}
	if err := schemaledger.RequireUnchanged(u.ctx, running, upgradeVisits); err != nil {
		t.Fatalf("the batch re-read of visits, which the apply left alone: %v", err)
	}
	_, err = schemameta.NewFileSchemaRegistryContext(u.ctx, u.pool, u.registry, u.schemaDir)
	if !errors.Is(err, schemaledger.ErrNotActive) {
		t.Fatalf("a restart on the superseded declaration = %v, want ErrNotActive", err)
	}
	mentionsAll(t, "the refusal of the superseded declaration", err.Error(), "smallint bound to smallint_01")
	if _, err := schemameta.NewMetadataLoader(u.pool, u.registry, widened).LoadMetadata(u.ctx); err != nil {
		t.Fatalf("a start on the applied declaration: %v", err)
	}
}

// TestLedgerUpgradeConcurrentAppliesCommitOneGeneration: two apply-schema
// runs judged against generation 1 both reach the compare-and-set before
// either writes. Whatever the target's conflict mechanism, exactly one
// commits a complete generation 2, the other fails having recorded nothing,
// and the ledger holds no partial or orphan rows. How the loser fails is a
// mechanism: see TestLedgerUpgradeConcurrentApplyLoserWaitsOnPostgres.
func TestLedgerUpgradeConcurrentAppliesCommitOneGeneration(t *testing.T) {
	u := baselined(t)
	winner, lost := u.raceApplies(t)
	if !errors.Is(lost, schemaledger.ErrGenerationConflict) && !isSerializationFailure(lost) {
		t.Fatalf("the losing apply failed with %v, want ErrGenerationConflict or SQLSTATE 40001", lost)
	}
	t.Logf("the losing apply: %v", lost)
	u.requirePointers(t, 2, 1)
	u.requireActiveIs(t, upgradeOrders, "orders", winner, 2)
	u.requireActiveIs(t, upgradeVisits, "visits", u.schemaDir, 1)
	u.requireLedger(t, []int64{1, 2}, []int64{1})
}

// TestLedgerUpgradeConcurrentApplyLoserWaitsOnPostgres is the PostgreSQL
// mechanism, apart from the invariant above: at READ COMMITTED the loser
// waits on the winner's pointer row lock, re-reads the row, matches nothing
// and answers ErrGenerationConflict. A REPEATABLE READ backend, MiniStack's,
// fails it with 40001 instead, and Aurora DSQL fails it at COMMIT (#821).
func TestLedgerUpgradeConcurrentApplyLoserWaitsOnPostgres(t *testing.T) {
	if _, kind := testdb.ResolveDSQL(t); kind != testdb.DSQLEmulated {
		t.Skip("mechanism assertion: PostgreSQL's READ COMMITTED lock wait, which the aurora-dsql target does not have")
	}
	u := baselined(t)
	_, lost := u.raceApplies(t)
	if !errors.Is(lost, schemaledger.ErrGenerationConflict) {
		t.Fatalf("the losing apply = %v, want ErrGenerationConflict after waiting on the pointer row", lost)
	}
	mentionsAll(t, "the losing apply", lost.Error(), "another control-plane run recorded a generation meanwhile")
}

// raceApplies runs two apply-schema runs, each allowed against generation 1,
// held at the pointer's compare-and-set until both have judged their plans
// against generation 1. It answers the winner's schema directory and the
// loser's error, and fails unless exactly one run succeeded.
func (u *upgradeTarget) raceApplies(t *testing.T) (string, error) {
	t.Helper()
	barrier := newPointerBarrier(u.registry, 2)
	pool := u.openPool(t, barrier)
	dirs := []string{
		u.declarationDir(t, upgradeOrdersWidened, upgradeVisitsAttrs),
		u.declarationDir(t, upgradeOrdersAdded, upgradeVisitsAttrs),
	}
	errs := make([]error, len(dirs))
	outs := make([]string, len(dirs))
	var wg sync.WaitGroup
	for i, dir := range dirs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			outs[i], errs[i] = u.apply(t, pool, dir, false)
		}()
	}
	wg.Wait()
	if barrier.timedOut.Load() {
		t.Fatalf("the two applies did not both reach the compare-and-set, so they did not race:\n%s\n%s", outs[0], outs[1])
	}
	switch {
	case errs[0] == nil && errs[1] != nil:
		return dirs[0], errs[1]
	case errs[1] == nil && errs[0] != nil:
		return dirs[1], errs[0]
	}
	t.Fatalf("want exactly one apply to succeed, got %v and %v:\n%s\n%s", errs[0], errs[1], outs[0], outs[1])
	return "", nil
}

func isSerializationFailure(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "40001"
}

// pointerBarrier holds every statement that moves the registry's active
// pointer until parties of them have arrived, or until a deadline, which it
// records. Each apply judges its plan before it moves the pointer, so the
// runs it holds have all read the same active generation.
type pointerBarrier struct {
	prefix   string
	parties  int
	mu       sync.Mutex
	arrived  int
	release  chan struct{}
	timedOut atomic.Bool
}

func newPointerBarrier(registry string, parties int) *pointerBarrier {
	return &pointerBarrier{
		prefix:  fmt.Sprintf("UPDATE %s SET %s", schemaledger.TablesFor(registry).Registry, schemaledger.PointerColumn),
		parties: parties,
		release: make(chan struct{}),
	}
}

func (b *pointerBarrier) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if !strings.HasPrefix(data.SQL, b.prefix) {
		return ctx
	}
	b.mu.Lock()
	b.arrived++
	if b.arrived == b.parties {
		close(b.release)
	}
	b.mu.Unlock()
	select {
	case <-b.release:
	case <-time.After(30 * time.Second):
		b.timedOut.Store(true)
	}
	return ctx
}

func (b *pointerBarrier) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}
