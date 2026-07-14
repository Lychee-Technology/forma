package production

import (
	"context"
	"fmt"

	forma "github.com/lychee-technology/forma"
	"github.com/lychee-technology/forma/internal/model"
)

// Filter is one typed filter leaf. Op defaults to "equals"; supported
// operators mirror forma.FilterType: equals, not_equals, gt, gte, lt, lte,
// starts_with, contains. Values are the string forms the public condition
// API uses ("1"/"0" for bools, unix-ms or ISO strings for dates).
type Filter struct {
	Attr  string `json:"attr"`
	Op    string `json:"op,omitempty"`
	Value string `json:"value"`
}

// Sort is one sort key; ties are broken by row_id ascending in the oracle,
// mirroring the engine's deterministic ordering.
type Sort struct {
	Attr string `json:"attr"`
	Desc bool   `json:"desc,omitempty"`
}

// Query is the single spec that drives BOTH the real engine and the oracle,
// preventing semantic drift between the two sides.
type Query struct {
	Schema         SchemaRef           `json:"schema"`
	Filters        []Filter            `json:"filters,omitempty"`
	Sorts          []Sort              `json:"sorts,omitempty"`
	Limit          int                 `json:"limit,omitempty"`
	Offset         int                 `json:"offset,omitempty"`
	PreferHot      bool                `json:"prefer_hot,omitempty"`
	PreferredTiers []model.DataTier    `json:"preferred_tiers,omitempty"`
	Keyset         *model.KeysetCursor `json:"keyset,omitempty"`

	// AllowPartialDegradedMode forwards the public degraded-mode flag (#185):
	// on a DuckDB-side failure the engine falls back to postgres-only instead
	// of failing the query. Non-degradable errors (missing schema metadata)
	// still surface.
	AllowPartialDegradedMode bool `json:"allow_partial_degraded_mode,omitempty"`

	// UseMainAsAnchor forwards the public anchor hint (#184 preference
	// contract: the hint must surface in the execution plan).
	UseMainAsAnchor bool `json:"use_main_as_anchor,omitempty"`
	// S3ParquetPathTemplate overrides the Env's default production glob when
	// non-empty, so tests can prove DuckDB reads the caller-specified path.
	S3ParquetPathTemplate string `json:"s3_parquet_path_template,omitempty"`
}

// QueryResult carries the engine-side outcome plus everything diagnostics
// need: the translated federated query and the full execution plan
// (SQL, parameters, routing, timings).
type QueryResult struct {
	Spec        Query
	Records     []*model.PersistentRecord
	Total       int64
	TotalPages  int
	CurrentPage int
	Plan        *model.ExecutionPlan
	FQ          *model.FederatedAttributeQuery
}

// ParquetGlob returns the production-layout parquet path template: one flat
// directory per schema covering both base (<min>_<max>.parquet) and delta
// (<uuid>.parquet) files; `*` does not cross `/` so _tmp/ is excluded.
func (e *Env) ParquetGlob() string {
	return fmt.Sprintf("s3://%s/%s/{{.SchemaID}}/*.parquet", e.Cluster.Bucket, e.S3Prefix)
}

// Query translates the spec, executes it through the real federated engine
// with execution-plan capture always on, and records the result for
// artifact dumps.
func (e *Env) Query(ctx context.Context, q Query) (*QueryResult, error) {
	fq, err := e.buildFederatedQuery(q)
	if err != nil {
		return nil, fmt.Errorf("build federated query (schema=%s): %w", q.Schema.Name, err)
	}

	opts := &model.FederatedQueryOptions{
		AllowPartialDegradedMode: q.AllowPartialDegradedMode,
		IncludeExecutionPlan:     true,
		ExecutionPlan:            &model.ExecutionPlan{Timings: map[string]int64{}, Notes: []string{}},
	}

	page, err := e.Engine().Query(ctx, e.Tables, fq, opts)
	if err != nil {
		return nil, fmt.Errorf("federated query (schema=%s): %w", q.Schema.Name, err)
	}

	result := &QueryResult{
		Spec:        q,
		Records:     page.Records,
		Total:       page.TotalRecords,
		TotalPages:  page.TotalPages,
		CurrentPage: page.CurrentPage,
		Plan:        opts.ExecutionPlan,
		FQ:          fq,
	}
	e.queryN++
	e.queries = append(e.queries, result)
	return result, nil
}

// buildFederatedQuery translates the harness spec into the engine's
// FederatedAttributeQuery: filters become the public condition tree
// ("op:value" leaves), sorts become metadata-resolved AttributeOrders, and
// the DuckDB hint carries the production single-glob parquet template.
func (e *Env) buildFederatedQuery(q Query) (*model.FederatedAttributeQuery, error) {
	condition, err := buildCondition(q.Filters)
	if err != nil {
		return nil, err
	}
	orders, err := e.buildAttributeOrders(q)
	if err != nil {
		return nil, err
	}

	tiers := q.PreferredTiers
	if len(tiers) == 0 {
		tiers = []model.DataTier{model.DataTierHot, model.DataTierWarm, model.DataTierCold}
	}
	glob := q.S3ParquetPathTemplate
	if glob == "" {
		glob = e.ParquetGlob()
	}

	fq := &model.FederatedAttributeQuery{
		AttributeQuery: model.AttributeQuery{
			SchemaID:        q.Schema.ID,
			Condition:       condition,
			AttributeOrders: orders,
			Limit:           q.Limit,
			Offset:          q.Offset,
		},
		PreferredTiers:  tiers,
		PreferHot:       q.PreferHot,
		UseMainAsAnchor: q.UseMainAsAnchor,
		KeysetCursor:    q.Keyset,
		DuckDBHints:     &model.DuckDBRenderHints{S3ParquetPathTemplate: glob},
	}
	return fq, nil
}

func buildCondition(filters []Filter) (forma.Condition, error) {
	if len(filters) == 0 {
		return nil, nil
	}
	leaves := make([]forma.Condition, 0, len(filters))
	for _, f := range filters {
		if f.Attr == "" || f.Value == "" {
			return nil, fmt.Errorf("filter needs attr and value, got %+v", f)
		}
		op := f.Op
		if op == "" {
			op = string(forma.FilterEquals)
		}
		// Always emit the explicit "op:value" form: values containing ':'
		// (ISO timestamps) would otherwise be misparsed as operators.
		leaves = append(leaves, &forma.KvCondition{Attr: f.Attr, Value: op + ":" + f.Value})
	}
	if len(leaves) == 1 {
		return leaves[0], nil
	}
	return &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: leaves}, nil
}

func (e *Env) buildAttributeOrders(q Query) ([]model.AttributeOrder, error) {
	if len(q.Sorts) == 0 {
		return nil, nil
	}
	cache, ok := e.Metadata.GetSchemaCacheByID(q.Schema.ID)
	if !ok {
		return nil, fmt.Errorf("no metadata cache for schema %d", q.Schema.ID)
	}
	orders := make([]model.AttributeOrder, 0, len(q.Sorts))
	for _, s := range q.Sorts {
		meta, found := cache[s.Attr]
		if !found {
			return nil, fmt.Errorf("sort attribute %q not in schema %s", s.Attr, q.Schema.Name)
		}
		order := model.AttributeOrder{
			AttrID:    meta.AttributeID,
			AttrName:  s.Attr,
			ValueType: meta.ValueType,
			SortOrder: forma.SortOrderAsc,
		}
		if s.Desc {
			order.SortOrder = forma.SortOrderDesc
		}
		if meta.ColumnBinding != nil {
			order.StorageLocation = forma.AttributeStorageLocationMain
			order.ColumnName = string(meta.ColumnBinding.ColumnName)
		} else {
			order.StorageLocation = forma.AttributeStorageLocationEAV
		}
		orders = append(orders, order)
	}
	return orders, nil
}
