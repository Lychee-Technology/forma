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
// The route is the deployment's (#715): the federated route is a deployment
// that routes every query through DuckDB.
//
// The first step reads a schema that has never been flushed, whose fallback
// glob matches no object (#720).
func TestReadContract(t *testing.T) {
	ctx := context.Background()
	env := NewEnv(t, SharedCluster(t))
	simple := DefaultSchemaFixtures()[0]
	manager := env.EntityManager()

	rowID := createSimpleRow(ctx, t, manager, simple, "v1")
	assertCommittedRows(ctx, t, env, simple, map[uuid.UUID]string{rowID: "v1"})

	mustFlush(ctx, t, env) // v1 is now in a delta parquet and no longer dirty
	assertCommittedRows(ctx, t, env, simple, map[uuid.UUID]string{rowID: "v1"})

	if _, err := manager.Update(ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: simple.Name, RowID: rowID},
		Type:             forma.OperationUpdate,
		Updates:          map[string]any{"name": "v2"},
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	assertCommittedRows(ctx, t, env, simple, map[uuid.UUID]string{rowID: "v2"})

	if err := manager.Delete(ctx, &forma.EntityOperation{
		EntityIdentifier: forma.EntityIdentifier{SchemaName: simple.Name, RowID: rowID},
		Type:             forma.OperationDelete,
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// The parquet tiers still hold a live v1.
	assertNotFound(ctx, t, env, simple, rowID)
	assertCommittedRows(ctx, t, env, simple, map[uuid.UUID]string{})

	mustFlush(ctx, t, env) // the tombstone is now in a delta parquet
	assertNotFound(ctx, t, env, simple, rowID)
	assertCommittedRows(ctx, t, env, simple, map[uuid.UUID]string{})
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

// readContractRoute is one Query route: the deployment that takes it, and
// whether its answer reads the parquet tiers through DuckDB.
type readContractRoute struct {
	manager func(*Env) forma.EntityManager
	duckdb  bool
}

// readContractRoutes returns the two Query routes. The route is the
// deployment's (#715): hybrid routing answers a small page from PostgreSQL
// alone, and the DuckDB-routed deployment reads every tier whatever the page
// size.
func readContractRoutes() map[string]readContractRoute {
	hybrid := func(e *Env) forma.EntityManager { return e.RoutedEntityManager(forma.RoutingStrategyHybrid) }
	return map[string]readContractRoute{
		"postgres-only": {manager: hybrid},
		"federated":     {manager: (*Env).DuckDBRoutedEntityManager, duckdb: true},
	}
}

// assertCommittedRows checks that Get returns each committed row and that
// both Query routes return exactly the committed rows, each once and with
// its committed name.
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
	for routeName, route := range readContractRoutes() {
		got := queryRowNames(t, routeName, queryReadContract(ctx, t, env, schema, routeName, route))
		for rowID, name := range want {
			gotName, ok := got[rowID]
			if !ok {
				t.Errorf("%s query: committed row %s is missing", routeName, rowID)
				continue
			}
			if gotName != name {
				t.Errorf("%s query: %s has name %v, want the committed %q, not a stale version",
					routeName, rowID, gotName, name)
			}
		}
		for rowID := range got {
			if _, ok := want[rowID]; !ok {
				t.Errorf("%s query: returned %s, which is not a committed live row", routeName, rowID)
			}
		}
	}
}

// queryRowNames indexes a Query result's names by row ID. A row returned
// twice fails the test: merge-on-read must yield each row once, and letting
// a duplicate collapse into the map would hide a row it displaced.
func queryRowNames(t *testing.T, route string, result *forma.QueryResult) map[uuid.UUID]any {
	t.Helper()
	names := make(map[uuid.UUID]any, len(result.Data))
	for _, record := range result.Data {
		if _, dup := names[record.RowID]; dup {
			t.Errorf("%s query: returned %s more than once", route, record.RowID)
			continue
		}
		names[record.RowID] = record.Attributes["name"]
	}
	return names
}

func assertNotFound(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, rowID uuid.UUID) {
	t.Helper()
	_, err := env.EntityManager().Get(ctx, &forma.QueryRequest{SchemaName: schema.Name, RowID: &rowID})
	if !errors.Is(err, forma.ErrNotFound) {
		t.Errorf("get after delete: err = %v, want ErrNotFound", err)
	}
}

// queryReadContract runs one route and checks it took that route: the
// federated one really read the parquet tiers, so its answer is not a
// PostgreSQL-only answer by another name, and the other did not.
func queryReadContract(ctx context.Context, t *testing.T, env *Env, schema SchemaRef, name string, route readContractRoute) *forma.QueryResult {
	t.Helper()
	result, err := route.manager(env).Query(ctx, &forma.QueryRequest{
		SchemaName: schema.Name, Page: 1, ItemsPerPage: 10, IncludeExecutionPlan: true,
	})
	if err != nil {
		t.Fatalf("%s query: %v", name, err)
	}
	if result.ExecutionPlan == nil || result.ExecutionPlan.Routing.UsedDuckDB != route.duckdb {
		t.Fatalf("%s query routed use_duckdb=%v, want %v: %+v", name,
			result.ExecutionPlan != nil && result.ExecutionPlan.Routing.UsedDuckDB, route.duckdb, result.ExecutionPlan)
	}
	return result
}
