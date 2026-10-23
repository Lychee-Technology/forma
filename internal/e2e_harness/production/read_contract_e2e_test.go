//go:build e2e

package production

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	forma "github.com/lychee-technology/forma"
)

// TestReadContract pins the minimum consistency guarantee of each public
// read API (#635, docs/federated-query/design.md §3.3) through the real
// EntityManager, CDC flush and federated engine. One row moves through
// create → flush → update → delete → flush, and after each step:
//
//   - Get returns the committed state, although from the update on the
//     flushed parquet copy holds an older live version;
//   - Query returns the same state on the PostgreSQL-only route and on the
//     federated route. Query promises only eventual consistency, but that is
//     a minimum, not a licence to answer stale: the dirty-set anti-join
//     discards the stale parquet version and the hot one serves.
//
// Every federated request also sets consistency_mode "eventual", which is
// accepted and ignored until #715 deletes it.
//
// A baseline row is flushed first so the schema's manifest lists a parquet
// object before the row under test exists: a schema that was never flushed
// resolves the fallback glob, which matches nothing, and a DuckDB pass over
// it fails (#720).
func TestReadContract(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	simple := DefaultSchemaFixtures()[0]
	manager := env.EntityManager()

	baseline := createSimpleRow(ctx, t, manager, simple, "baseline")
	mustFlush(ctx, t, env)

	rowID := createSimpleRow(ctx, t, manager, simple, "v1")
	assertCommittedRows(ctx, t, env, simple, map[uuid.UUID]string{baseline: "baseline", rowID: "v1"})

	mustFlush(ctx, t, env) // v1 is now in a delta parquet and no longer dirty
	assertCommittedRows(ctx, t, env, simple, map[uuid.UUID]string{baseline: "baseline", rowID: "v1"})

	if _, err := manager.Update(ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: simple.Name, RowID: rowID},
		Type:             forma.OperationUpdate,
		Updates:          map[string]any{"name": "v2"},
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	assertCommittedRows(ctx, t, env, simple, map[uuid.UUID]string{baseline: "baseline", rowID: "v2"})

	if err := manager.Delete(ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: simple.Name, RowID: rowID},
		Type:             forma.OperationDelete,
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// The parquet tiers still hold a live v1.
	assertNotFound(ctx, t, env, simple, rowID)
	assertCommittedRows(ctx, t, env, simple, map[uuid.UUID]string{baseline: "baseline"})

	mustFlush(ctx, t, env) // the tombstone is now in a delta parquet
	assertNotFound(ctx, t, env, simple, rowID)
	assertCommittedRows(ctx, t, env, simple, map[uuid.UUID]string{baseline: "baseline"})
}

func createSimpleRow(ctx context.Context, t *testing.T, manager forma.EntityManager, schema SchemaRef, name string) uuid.UUID {
	t.Helper()
	created, err := manager.Create(ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: schema.Name},
		Type:             forma.OperationCreate,
		Data:             map[string]any{"name": name, "value": 1},
	})
	if err != nil {
		t.Fatalf("create %q: %v", name, err)
	}
	return created.RowID
}

// readContractRequests returns the two Query routes: PostgreSQL only, and
// federated with every tier declared, which reaches DuckDB whatever the page
// size (#468).
func readContractRequests(schema SchemaRef) map[string]*forma.QueryRequest {
	return map[string]*forma.QueryRequest{
		"postgres-only": {SchemaName: schema.Name, Page: 1, ItemsPerPage: 10},
		"federated": {SchemaName: schema.Name, Page: 1, ItemsPerPage: 10, Federated: &forma.FederatedQueryRequest{
			Enabled:              true,
			PreferredTiers:       []string{"hot", "warm", "cold"},
			IncludeExecutionPlan: true,
			ConsistencyMode:      "eventual",
		}},
	}
}

// assertCommittedRows checks that Get returns each committed row and that
// both Query routes return exactly the committed rows, each with its
// committed name.
func assertCommittedRows(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, want map[uuid.UUID]string) {
	t.Helper()
	for rowID, name := range want {
		got, err := env.EntityManager().Get(ctx, &forma.QueryRequest{SchemaName: schema.Name, RowID: &rowID})
		if err != nil {
			t.Fatalf("get %s: %v", rowID, err)
		}
		if got.Attributes["name"] != name {
			t.Errorf("get %s: name = %v, want the committed %q", rowID, got.Attributes["name"], name)
		}
	}
	for route, req := range readContractRequests(schema) {
		result := queryReadContract(ctx, t, env, route, req)
		if len(result.Data) != len(want) {
			t.Errorf("%s query: got %d rows, want %d", route, len(result.Data), len(want))
		}
		for _, record := range result.Data {
			name, ok := want[record.RowID]
			if !ok {
				t.Errorf("%s query: returned %s, which is not a committed live row", route, record.RowID)
				continue
			}
			if record.Attributes["name"] != name {
				t.Errorf("%s query: %s has name %v, want the committed %q, not a stale version",
					route, record.RowID, record.Attributes["name"], name)
			}
		}
	}
}

func assertNotFound(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, rowID uuid.UUID) {
	t.Helper()
	_, err := env.EntityManager().Get(ctx, &forma.QueryRequest{SchemaName: schema.Name, RowID: &rowID})
	if !errors.Is(err, forma.ErrNotFound) {
		t.Errorf("get after delete: err = %v, want ErrNotFound", err)
	}
}

// queryReadContract runs one route and checks the federated one really read
// the parquet tiers, so its answer is not a PostgreSQL-only answer by
// another name.
func queryReadContract(ctx context.Context, t *testing.T, env *Env, route string, req *forma.QueryRequest) *forma.QueryResult {
	t.Helper()
	result, err := env.EntityManager().Query(ctx, req)
	if err != nil {
		t.Fatalf("%s query: %v", route, err)
	}
	if req.Federated != nil && (result.ExecutionPlan == nil || !result.ExecutionPlan.Routing.UsedDuckDB) {
		t.Fatalf("%s query did not route through DuckDB: %+v", route, result.ExecutionPlan)
	}
	return result
}
