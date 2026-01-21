# DuckDB Query Test Plan

## Overview
This comprehensive test plan covers the DuckDB federated query path, including query translation, dirty data exclusion, routing policy, and execution paths. Tests prioritize using in-memory DuckDB to avoid external dependencies on Postgres and S3.

## Test Objectives
1. **Query Translation**: Validate DuckDB WHERE clause generation for various condition types (composite, simple KV, type inference)
2. **Dirty Data Exclusion**: Ensure change_log records are correctly excluded via NOT IN clauses with proper parameter ordering
3. **Query Building**: Verify BuildDuckDBQuery correctly uses DualClauses and parameter injection
4. **Routing Policy**: Validate routing decisions based on config and query hints
5. **Client Lifecycle**: Test DuckDB client creation, configuration, and health checks
6. **Execution Path**: Lightweight integration test for StreamDuckDBFederatedQuery with ExecutionPlan instrumentation

## Test Scope

### Unit Tests (Priority: High)

#### 1. GenerateDuckDBWhereClause Tests
**File**: `duckdb_sql_generator_test.go` (new/enhanced)

**Test Cases**:
- **TC-1.1**: Simple KV condition with equals operator
  - Input: `KvCondition{Attr: "name", Value: "alice"}`
  - Expected: Clause = `"name = ?"`, Args = `[]any{"alice"}`

- **TC-1.2**: KV condition with operator prefix
  - Input: `KvCondition{Attr: "age", Value: "gt:30"}`
  - Expected: Clause contains `"age > CAST(? AS BIGINT)"`, Args = `[]any{30.0}`

- **TC-1.3**: LIKE operators (starts_with, contains)
  - Input: `KvCondition{Attr: "email", Value: "starts_with:test"}`
  - Expected: Clause = `"email LIKE ?"`, Args = `[]any{"test%"}`

- **TC-1.4**: Type inference and casting
  - UUID: `KvCondition{Value: "550e8400-e29b-41d4-a716-446655440000"}` → no CAST, treat as string
  - Numeric: `KvCondition{Value: "42.5"}` → CAST to DOUBLE
  - Boolean: `KvCondition{Value: "true"}` → CAST to BOOLEAN
  - DateTime (RFC3339): `KvCondition{Value: "2023-01-15T10:30:00Z"}` → no CAST, keep as string
  - DateTime (epoch millis): `KvCondition{Value: "1673779800000"}` → parsed as int, CAST to BIGINT

- **TC-1.5**: Composite conditions with AND
  - Input: `CompositeCondition{Logic: LogicAnd, Conditions: [KvCond("age>30"), KvCond("name=alice")]}`
  - Expected: Both conditions combined with AND, correct arg order

- **TC-1.6**: Composite conditions with OR
  - Input: `CompositeCondition{Logic: LogicOr, Conditions: [...]}`
  - Expected: Conditions combined with OR

- **TC-1.7**: Nested composite conditions
  - Input: `CompositeCondition{AND: [CompositeCondition{OR: [...]}]}`
  - Expected: Properly parenthesized and combined

- **TC-1.8**: Empty/nil conditions
  - Input: `nil` or empty Condition
  - Expected: Clause = `"1=1"`, Args = `[]any{}`

#### 2. GenerateDuckDBWhereClauseWithExclusions Tests
**File**: `duckdb_sql_generator_test.go` (new/enhanced)

**Test Cases**:
- **TC-2.1**: Dirty ID exclusion appended to simple WHERE
  - Input: Base WHERE = `"age > 30"`, dirtyIDs = [u1, u2]
  - Expected: Clause contains both `"age > 30"` AND `"row_id NOT IN (?,?)"`, Args = `[...age_arg, u1_str, u2_str]`

- **TC-2.2**: Parameter ordering preserved
  - Input: Query with 2 conditions + 3 dirty IDs
  - Expected: Args length = 2+3, verify order is queries first then dirty IDs

- **TC-2.3**: Empty dirtyIDs list
  - Input: dirtyIDs = `[]uuid.UUID{}`
  - Expected: Clause unchanged, no extra AND/NOT IN appended

- **TC-2.4**: Multiple dirty IDs
  - Input: 10 random UUIDs
  - Expected: Correct number of `?` placeholders, all UUIDs as string args

#### 3. BuildDuckDBQuery Tests (with DualClauses)
**File**: `duckdb_sql_generator_test.go` or new `duckdb_query_builder_test.go`

**Test Cases**:
- **TC-3.1**: DualClauses priority when provided
  - Input: `dual = DualClauses{DuckClause: "col1 > 50", DuckArgs: [100]}`
  - Expected: Anchor.Condition = `"col1 > 50"`, template params contain DuckClause not GenerateDuckDBWhereClause result

- **TC-3.2**: PgMainClause injection into template params
  - Input: `dual = DualClauses{..., PgMainClause: "ltbase_schema_id = 1", PgMainArgs: [...]}`
  - Expected: Template params["PgMainClause"] set, params["HasPgMainClause"] = true

- **TC-3.3**: Dirty ID exclusion with DualClauses
  - Input: dual.DuckClause = `"col1 > 50"`, dirtyIDs = [u1, u2]
  - Expected: Final Anchor.Condition = `"col1 > 50 AND row_id NOT IN (?,?)"`, args order correct

- **TC-3.4**: Legacy path (dual == nil)
  - Input: `dual = nil`, use GenerateDuckDBWhereClause
  - Expected: Anchor.Condition from GenerateDuckDBWhereClause result

- **TC-3.5**: Parameter merging order
  - Template whereArgs (from dual) + dirtyID exclusion args
  - Expected: where args first, then dirty ID string args

- **TC-3.6**: Empty params map creation
  - Input: params not a map, dirtyIDs present
  - Expected: New map created, Anchor initialized, MergeTemplateParamsWithDirtyIDs called

- **TC-3.7**: Template rendering failure
  - Input: Template with undefined variable, malformed syntax
  - Expected: Error returned and propagated

#### 4. Routing Policy & Configuration Tests
**File**: `postgres_duckdb_federated_integration_test.go` (enhanced) or new file

**Test Cases**:
- **TC-4.1**: ValidateDuckDBConfig - invalid memory limit
  - Input: `cfg.MemoryLimitMB = -1`
  - Expected: Error "invalid memory_limit_mb"

- **TC-4.2**: ValidateDuckDBConfig - invalid parallelism
  - Input: `cfg.MaxParallelism = -1`
  - Expected: Error "invalid max_parallelism"

- **TC-4.3**: ValidateDuckDBConfig - invalid max connections
  - Input: `cfg.MaxConnections = 0`
  - Expected: Error "max_connections must be >= 1"

- **TC-4.4**: ValidateDuckDBConfig - invalid query timeout
  - Input: `cfg.QueryTimeout = 0`
  - Expected: Error "query_timeout must be > 0"

- **TC-4.5**: ValidateDuckDBConfig - disabled is valid
  - Input: `cfg.Enabled = false`
  - Expected: No error

- **TC-4.6**: EvaluateRoutingPolicy - hybrid default
  - Input: `cfg.Routing.Strategy = "hybrid"`, no query hints
  - Expected: `dec.UseDuckDB = true`

- **TC-4.7**: EvaluateRoutingPolicy - PreferHot overrides
  - Input: `cfg.Routing.Strategy = "hybrid"`, `fq.PreferHot = true`
  - Expected: `dec.UseDuckDB = false`

- **TC-4.8**: EvaluateRoutingPolicy - cost-first large scan
  - Input: `cfg.Routing.Strategy = "cost-first"`, `opts.MaxRows = 100000`
  - Expected: `dec.UseDuckDB = true`

- **TC-4.9**: EvaluateRoutingPolicy - disabled globally
  - Input: `cfg.Enabled = false`
  - Expected: `dec.UseDuckDB = false`

#### 5. DuckDB Client Lifecycle Tests
**File**: `duckdb_conn_test.go` (enhanced)

**Test Cases**:
- **TC-5.1**: NewDuckDBClient - in-memory mode
  - Input: `cfg.DBPath = ":memory:"`, Enabled=true
  - Expected: Client created successfully, DB not nil

- **TC-5.2**: NewDuckDBClient - disabled
  - Input: `cfg.Enabled = false`
  - Expected: Error "duckdb disabled in config"

- **TC-5.3**: HealthCheck - success
  - Input: Valid client with in-memory DB
  - Expected: No error, SELECT 1 returns 1

- **TC-5.4**: HealthCheck - memory limit pragma
  - Input: `cfg.MemoryLimitMB = 512`
  - Expected: HealthCheck queries PRAGMA memory_limit without error (non-fatal if fails)

- **TC-5.5**: HealthCheck - parallelism pragma
  - Input: `cfg.MaxParallelism = 4`
  - Expected: HealthCheck queries PRAGMA threads without error

- **TC-5.6**: HealthCheck - S3 pragma (best-effort)
  - Input: `cfg.EnableS3 = true`, `cfg.S3Endpoint = "s3.example.com"`
  - Expected: No error, pragma queries logged as warnings if failed

- **TC-5.7**: Client Close
  - Input: Active client
  - Expected: No error on Close(), subsequent operations fail gracefully

#### 6. Query Rendering Tests
**File**: `duckdb_template_renderer_test.go` (new)

**Test Cases**:
- **TC-6.1**: RenderDuckDBQuery - where args + template args order
  - Input: whereArgs=[1,2], template with params that yield args=[3,4]
  - Expected: Final args=[1,2,3,4] in that order

- **TC-6.2**: RenderDuckDBQuery - empty where args
  - Input: whereArgs=[], template args=[5]
  - Expected: Final args=[5]

- **TC-6.3**: MergeTemplateParamsWithDirtyIDs - adds dirty set info
  - Input: dirtyIDs=[u1, u2, u3]
  - Expected: Params contain DirtyIDsCount and DirtyIDsCSV if used by template

### Integration Tests (Priority: Medium)

#### 7. StreamDuckDBFederatedQuery - Lightweight Path
**File**: `postgres_duckdb_federated_integration_test.go` (new) or `internal/duckdb_integration_test.go`

**Test Setup**:
- Use in-memory DuckDB (no postgres_scan/read_parquet)
- Create simple template that accepts Anchor.Condition:
  ```go
  tpl := template.Must(template.New("simple").Parse(`
    SELECT
      '550e8400-e29b-41d4-a716-446655440001'::UUID AS row_id,
      'alice' AS name,
      100 AS age,
      1673779800000 AS ltbase_created_at,
      1673779800000 AS ltbase_updated_at,
      NULL::BIGINT AS ltbase_deleted_at,
      5 AS total_records,
      1 AS total_pages,
      1 AS current_page,
      '[]'::TEXT AS attributes_json
    WHERE {{.Anchor.Condition}}
    UNION ALL
    SELECT
      '550e8400-e29b-41d4-a716-446655440002'::UUID AS row_id,
      'bob' AS name,
      25 AS age,
      1673779800000 AS ltbase_created_at,
      1673779800000 AS ltbase_updated_at,
      NULL::BIGINT AS ltbase_deleted_at,
      5 AS total_records,
      1 AS total_pages,
      1 AS current_page,
      '[]'::TEXT AS attributes_json
    WHERE {{.Anchor.Condition}}
  `))
  ```

**Test Cases**:
- **TC-7.1**: Basic execution with row handler
  - Input: dirtyIDs = [u1 (alice)], FederatedAttributeQuery with no filters
  - Expected: rowHandler called once for bob record, totalRecords=5, alice filtered out

- **TC-7.2**: ExecutionPlan instrumentation
  - Input: opts.IncludeExecutionPlan=true
  - Expected: ExecutionPlan.Sources has duckdb entry with SQL/ActualRows/DurationMs, Timings has translate/duckdb_fetch/total

- **TC-7.3**: Row handler error propagation
  - Input: rowHandler returns error on first record
  - Expected: StreamDuckDBFederatedQuery returns that error

- **TC-7.4**: Empty result set
  - Input: WHERE 1=0 injected, no records match
  - Expected: totalRecords=0, rowHandler not called, no error

- **TC-7.5**: Attribute JSON parsing
  - Input: Record with attributes_json = `[{"schema_id":1,"attr_id":205,"row_id":"...","value_text":"tag_value"}]`
  - Expected: OtherAttributes populated correctly

#### 8. Failure Paths
**File**: Distributed across test files

**Test Cases**:
- **TC-8.1**: ExecuteDuckDBFederatedQuery with nil client
  - Input: duckDBClient = nil
  - Expected: Error "duckdb client not available"

- **TC-8.2**: ExecuteDuckDBFederatedQuery with nil query
  - Input: q = nil
  - Expected: Error "query cannot be nil"

- **TC-8.3**: BuildDuckDBQuery with invalid template syntax
  - Input: Template with `{{.UndefinedField}}`
  - Expected: Error during rendering

- **TC-8.4**: Row scan error simulation
  - Input: DuckDB query returns malformed data (simulate via custom mock if possible)
  - Expected: Error from rows.Scan propagated

## Test Data Requirements

### UUIDs (Fixed for Deterministic Testing)
```go
const (
  dirtyID1 = "550e8400-e29b-41d4-a716-446655440001"
  dirtyID2 = "550e8400-e29b-41d4-a716-446655440002"
  recordID1 = "550e8400-e29b-41d4-a716-446655440003"
)
```

### Sample Query Conditions
```go
// Simple KV
kvSimple := &forma.KvCondition{Attr: "name", Value: "alice"}

// With operator
kvOperator := &forma.KvCondition{Attr: "age", Value: "gt:30"}

// Composite AND
compAnd := &forma.CompositeCondition{
  Logic: forma.LogicAnd,
  Conditions: []forma.Condition{kvSimple, kvOperator},
}
```

## Test Execution Environment

- **Database**: In-memory DuckDB (`:memory:` DSN)
- **Extensions**: Parquet (optional, for future S3 tests)
- **Configuration**: 
  - MemoryLimit: 256MB
  - MaxParallelism: 2
  - QueryTimeout: 5s
- **Coverage Target**: >85% of duckdb_*.go files
- **Test Framework**: testify/require for assertions

## Success Criteria

1. **Unit Tests**: All 30+ test cases pass with 100% assertion success
2. **Integration Tests**: StreamDuckDBFederatedQuery executes end-to-end without external dependencies
3. **Code Coverage**: duckdb_sql_generator.go, duckdb_template_renderer.go, duckdb_conn.go, postgres_duckdb_query.go all >80%
4. **Failure Modes**: Proper error messages and error propagation verified
5. **Performance**: No query executes >1s in test environment

## Timeline

- **Phase 1 (Unit Tests)**: Days 1-2
  - WHERE clause translation (TC-1.1 to 1.8)
  - Exclusion logic (TC-2.1 to 2.4)
  - Query building (TC-3.1 to 3.7)

- **Phase 2 (Config & Client)**: Day 2
  - Routing policy (TC-4.1 to 4.9)
  - Client lifecycle (TC-5.1 to 5.7)

- **Phase 3 (Integration & Failure)**: Day 3
  - Rendering (TC-6.1 to 6.3)
  - StreamDuckDBFederatedQuery (TC-7.1 to 7.5)
  - Failure paths (TC-8.1 to 8.4)

## Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| Postgres_scan unavailable in tests | Use in-memory DuckDB with simple SELECT; simulate dirty ID filtering via WHERE NOT IN |
| Type inference complexity | Test each type independently; use fixed string values for reproducibility |
| Template rendering variability | Pre-validate template syntax before use; test with minimal placeholder expressions |
| Parameter ordering issues | Log and assert arg counts/order at each step; use helper assertions |
