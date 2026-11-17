//go:build e2e

package production

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// The federated hot leg reads a hot row through the OLTP reader, and
// PostgreSQL evaluates the hot rows' filter (design.md §6.1, What a hot
// row reads as, Filtering splits by tier). So a hot image the write funnel
// refuses, planted by direct SQL, reads on the federated route as the
// PostgreSQL route reads it: both refuse the request, or both answer the
// same rows with the same values. The planted-image tests hold their hot
// phase to that.

// requireRoutesAgree answers q on the PostgreSQL route (PreferHot) and on
// the federated route, and requires the same outcome: the same refusal, or
// the same rows with the same stored images. It compares the engine's
// records, before the attribute decoding a public read applies, so equal
// records decode, or are refused, alike.
func requireRoutesAgree(ctx context.Context, t *testing.T, env *Env, label string, q Query) {
	t.Helper()
	pgQ := q
	pgQ.PreferHot, pgQ.PreferredTiers = true, nil
	fedQ := q
	fedQ.PreferHot = false
	pg, pgErr := env.Query(ctx, pgQ)
	fed, fedErr := env.Query(ctx, fedQ)
	if pgErr != nil || fedErr != nil {
		requireSameRefusal(t, label, fedErr, pgErr)
		t.Logf("%s: both routes refuse: %v", label, pgErr)
		return
	}
	if pg.Plan.Routing.UseDuckDB || !fed.Plan.Routing.UseDuckDB {
		t.Fatalf("%s: want the PostgreSQL and the federated route, got %+v and %+v", label, pg.Plan.Routing, fed.Plan.Routing)
	}
	requireSameRecords(t, label, pg.Records, fed.Records)
}

// requireProbesAgree runs requireRoutesAgree on base unfiltered and under
// each probe's filter.
func requireProbesAgree(ctx context.Context, t *testing.T, env *Env, label string, base Query, probes []widthProbe) {
	t.Helper()
	requireRoutesAgree(ctx, t, env, label+"/unfiltered", base)
	for _, p := range probes {
		q := base
		q.Filters = []Filter{p.filter}
		requireRoutesAgree(ctx, t, env, label+"/"+p.name, q)
	}
}

// requireSameRefusal requires both routes to refuse, in the same class: the
// caller's invalid input on both, or a read error on both. Each error must
// name every string in names.
func requireSameRefusal(t *testing.T, label string, fedErr, pgErr error, names ...string) {
	t.Helper()
	if fedErr == nil || pgErr == nil {
		t.Fatalf("%s: one route refuses and the other answers: federated %v, PostgreSQL %v", label, fedErr, pgErr)
	}
	if errors.Is(fedErr, forma.ErrInvalidInput) != errors.Is(pgErr, forma.ErrInvalidInput) {
		t.Fatalf("%s: the routes refuse in different classes: federated %v, PostgreSQL %v", label, fedErr, pgErr)
	}
	for _, name := range names {
		if !strings.Contains(fedErr.Error(), name) || !strings.Contains(pgErr.Error(), name) {
			t.Fatalf("%s: both refusals must name %q: federated %v, PostgreSQL %v", label, name, fedErr, pgErr)
		}
	}
}

// requireSameRecords compares two answers as row sets, record by record.
func requireSameRecords(t *testing.T, label string, pg, fed []*model.PersistentRecord) {
	t.Helper()
	byID := func(recs []*model.PersistentRecord) []*model.PersistentRecord {
		s := slices.Clone(recs)
		slices.SortFunc(s, func(a, b *model.PersistentRecord) int { return slices.Compare(a.RowID[:], b.RowID[:]) })
		return s
	}
	p, f := byID(pg), byID(fed)
	ids := func(recs []*model.PersistentRecord) []string {
		out := make([]string, len(recs))
		for i, r := range recs {
			out[i] = r.RowID.String()
		}
		return out
	}
	if !slices.Equal(ids(p), ids(f)) {
		t.Fatalf("%s: the PostgreSQL route answers %v, the federated route %v", label, ids(p), ids(f))
	}
	for i := range p {
		if a, b := storedImage(p[i]), storedImage(f[i]); a != b {
			t.Errorf("%s: row %s reads\n  %s on the PostgreSQL route,\n  %s on the federated route", label, p[i].RowID, a, b)
		}
	}
}

// storedImage renders every value a record carries, column by column and
// EAV row by EAV row, sorted. Values are printed, so a NaN equals itself. A
// live row's deleted_at reads as nil from PostgreSQL and as 0 from parquet
// (#274); both are left out.
func storedImage(rec *model.PersistentRecord) string {
	parts := []string{fmt.Sprintf("created_at=%d", rec.CreatedAt), fmt.Sprintf("updated_at=%d", rec.UpdatedAt)}
	if rec.DeletedAt != nil && *rec.DeletedAt != 0 {
		parts = append(parts, fmt.Sprintf("deleted_at=%d", *rec.DeletedAt))
	}
	add := func(kind string, col string, v any) { parts = append(parts, fmt.Sprintf("%s:%s=%v", kind, col, v)) }
	for k, v := range rec.TextItems {
		add("text", k, v)
	}
	for k, v := range rec.Int16Items {
		add("int16", k, v)
	}
	for k, v := range rec.Int32Items {
		add("int32", k, v)
	}
	for k, v := range rec.Int64Items {
		add("int64", k, v)
	}
	for k, v := range rec.Float64Items {
		add("float64", k, v)
	}
	for k, v := range rec.UUIDItems {
		add("uuid", k, v)
	}
	for _, a := range rec.OtherAttributes {
		col := fmt.Sprintf("%d[%s]", a.AttrID, a.ArrayIndices)
		switch {
		case a.ValueText != nil:
			add("eav_text", col, *a.ValueText)
		case a.ValueNumeric != nil:
			add("eav_numeric", col, *a.ValueNumeric)
		default:
			add("eav", col, nil)
		}
	}
	sort.Strings(parts)
	return strings.Join(parts, " ")
}

// postgresRouteQuery is federatedWideQuery's request on the PostgreSQL
// route: the same page of schema, on a deployment without DuckDB. Since a
// request no longer chooses its route (#715), the deployment is what makes
// this the PostgreSQL control; on EntityManager, which routes through
// DuckDB, the comparison would set the federated route against itself.
//
// The route is pinned, not assumed. The page itself is asked for the plan
// and must carry none, since only the federated engine records one; a page
// the PostgreSQL route refuses carries no plan either way, so the same
// manager first answers the empty page past it, which decodes no row and
// must come back plan-less too.
func postgresRouteQuery(ctx context.Context, t *testing.T, env *Env, schema SchemaRef) (*forma.QueryResult, error) {
	t.Helper()
	manager := env.PostgresEntityManager()
	query := func(page int) (*forma.QueryResult, error) {
		return manager.Query(ctx, &forma.QueryRequest{SchemaName: schema.Name, Page: page, ItemsPerPage: 100, IncludeExecutionPlan: true})
	}
	probe, err := query(2)
	if err != nil {
		t.Fatalf("PostgreSQL control: the empty page past %s: %v", schema.Name, err)
	}
	requireNoEnginePlan(t, "PostgreSQL control, the empty page past "+schema.Name, probe)
	res, err := query(1)
	if err == nil {
		requireNoEnginePlan(t, "PostgreSQL control, the page of "+schema.Name, res)
	}
	return res, err
}

// requireNoEnginePlan fails t if res, answered to a request that asked for
// the execution plan, carries one: it then came through the federated
// engine, not from a deployment without DuckDB.
func requireNoEnginePlan(t *testing.T, label string, res *forma.QueryResult) {
	t.Helper()
	if res.ExecutionPlan != nil {
		t.Fatalf("%s: answered through the federated engine (routing %+v), want a deployment without DuckDB", label, res.ExecutionPlan.Routing)
	}
}
