# Issue review and prioritization for Forma

**Session ID:** ses_21bbc99e6ffeQl1u1d62I1MNP5
**Created:** 5/1/2026, 8:58:35 AM
**Updated:** 5/1/2026, 9:16:48 AM

---

## User

review all issues https://github.com/Lychee-Technology/forma/issues and find out which can be closed and which to work on next

---

## Assistant (Plan · Claude Sonnet 4.6 · 3.3s)

**Tool: bash**

**Input:**
```json
{
  "command": "gh issue list --repo Lychee-Technology/forma --state open --limit 50",
  "description": "List all open issues"
}
```

**Output:**
```
54	OPEN	Optimize federated routing and concurrency behavior using benchmark evidence	enhancement, benchmark, federated-query	2026-04-18T19:45:40Z
51	OPEN	Optimize federated filter pushdown and EAV behavior using benchmark evidence	enhancement, benchmark, federated-query	2026-04-18T19:45:40Z

```

---

## Assistant (Plan · Claude Sonnet 4.6 · 2.8s)

**Tool: bash**

**Input:**
```json
{
  "command": "gh issue list --repo Lychee-Technology/forma --state all --limit 100 | head -60",
  "description": "List all issues including closed"
}
```

**Output:**
```
70	CLOSED	Align benchmark follow-up backlog with shipped runtime and remaining gaps	enhancement, benchmark, federated-query	2026-04-20T05:46:47Z
69	CLOSED	Define a benchmark readiness gate before optimization and autoresearch decisions	enhancement, benchmark, federated-query	2026-04-20T14:53:06Z
65	CLOSED	Harden benchmark stability checks and expose oracle provenance in reports	enhancement, benchmark, federated-query	2026-04-20T08:51:41Z
64	CLOSED	Expand the benchmark workload matrix with low-selectivity, mixed-filter, and tier-targeted window cases	enhancement, benchmark, federated-query	2026-04-20T06:01:59Z
63	CLOSED	Generalize selective-workload benchmark oracles beyond workload-specific truth passes	enhancement, benchmark, federated-query	2026-04-20T06:19:19Z
55	CLOSED	Track benchmark follow-up work for autoresearch-guided performance tuning	enhancement, benchmark, federated-query	2026-04-20T14:54:18Z
54	OPEN	Optimize federated routing and concurrency behavior using benchmark evidence	enhancement, benchmark, federated-query	2026-04-18T19:45:40Z
53	CLOSED	Add autoresearch performance targets and benchmark gate scripts	enhancement, benchmark, federated-query	2026-05-01T06:15:41Z
52	CLOSED	Add executable benchmark runtime backed by the live federated harness	enhancement, benchmark, federated-query	2026-04-18T21:18:17Z
51	OPEN	Optimize federated filter pushdown and EAV behavior using benchmark evidence	enhancement, benchmark, federated-query	2026-04-18T19:45:40Z
50	CLOSED	Complete filter fidelity and schema-scoped benchmark workloads	enhancement, benchmark, federated-query	2026-04-18T23:27:46Z
48	CLOSED	Harden benchmark result semantics and correctness assertions	enhancement, benchmark, federated-query	2026-04-18T22:16:19Z
49	CLOSED	Scaffold a performance-oriented autoresearch loop around benchmark evidence	enhancement, benchmark, federated-query	2026-04-20T16:40:26Z
47	CLOSED	Expand benchmark metrics, artifact schema, and baseline diff support	enhancement, benchmark, federated-query	2026-04-18T23:28:04Z
46	CLOSED	Optimize the federated deep-pagination path using benchmark evidence	enhancement, benchmark, federated-query	2026-05-01T06:15:39Z
45	CLOSED	Capture benchmark baselines and codify CI execution policy	enhancement, benchmark, federated-query	2026-04-20T14:53:06Z
39	CLOSED	Document CI and operator guidance for benchmark execution	documentation, benchmark, federated-query, phase-1	2026-04-18T19:30:28Z
38	CLOSED	Add benchmark reporting and baseline capture support	enhancement, benchmark, federated-query, phase-1	2026-04-18T17:04:29Z
37	CLOSED	Implement workload matrix and deep pagination benchmark cases	enhancement, benchmark, federated-query, phase-1	2026-04-18T17:04:28Z
36	CLOSED	Add tier preparation and overlap/delete dataset support	enhancement, benchmark, federated-query, phase-1	2026-04-18T17:04:26Z
35	CLOSED	Implement deterministic benchmark generator and distribution models	enhancement, benchmark, federated-query, phase-1	2026-04-18T17:04:25Z
34	CLOSED	Add benchmark scaffolding and TPC-E-inspired schema fixtures	enhancement, benchmark, federated-query, phase-1	2026-04-18T17:04:23Z
33	CLOSED	Add TPC-E-inspired federated query benchmark umbrella	enhancement, benchmark, federated-query, phase-1	2026-04-18T17:04:22Z
19	CLOSED	Fix pre-existing row handler streaming test fixture in postgres repo query tests		2026-04-18T19:30:27Z
18	CLOSED	Make DuckDB query template building injectable for federated query tests		2026-04-18T17:04:20Z
17	CLOSED	Add test seam for FetchDirtyRowIDs in postgres_duckdb_query		2026-04-18T17:04:19Z
15	CLOSED	refactor: consider extracting telemetry out of the catch-all package		2026-04-18T19:30:27Z
14	CLOSED	test: compactor mock diverges from manifest.Save advancement logic		2026-04-18T17:04:18Z
12	CLOSED	Make rewrite backlog observable when rewrite is pending		2026-04-18T19:30:27Z
11	CLOSED	Harden FileProvider.SaveManifest metadata contract		2026-03-04T01:01:01Z

```

---

## Assistant (Plan · Claude Sonnet 4.6 · 3.8s)

**Tool: bash**

**Input:**
```json
{
  "command": "gh issue view 54 --repo Lychee-Technology/forma && echo \"---\" && gh issue view 51 --repo Lychee-Technology/forma",
  "description": "Get details on the two open issues"
}
```

**Output:**
```
title:	Optimize federated routing and concurrency behavior using benchmark evidence
state:	OPEN
author:	iceboundrock (Ruoshi Li)
labels:	benchmark, enhancement, federated-query
comments:	0
assignees:	
projects:	
milestone:	
number:	54
--
## Summary

Optimize federated routing and concurrency behavior using benchmark evidence.

## Why

After the main pagination and filter paths are benchmarked and hardened, the next tuning step is to improve strategy selection and tail behavior. That includes routing heuristics between Postgres and DuckDB paths and fixed-concurrency benchmark behavior.

## Scope

- investigate routing-sensitive workloads and strategy-selection heuristics
- add or refine fixed-concurrency benchmark execution where needed
- tune workloads where tail latency or route instability dominates
- protect correctness and earlier optimization wins with the established benchmark guardrails

## Acceptance Criteria

- routing-sensitive workloads show measurable improvement or better guardrails under benchmark comparison
- concurrency-aware benchmark runs are reproducible enough for repeated evaluation
- benchmark artifacts clearly separate target wins from tail-latency regressions elsewhere

## References

- `internal/e2e_harness/federated/query.go`
- `internal/entity_query_service.go`
- `docs/federated-query/federated-query-benchmark-autoresearch-pr-plan.md`

---
title:	Optimize federated filter pushdown and EAV behavior using benchmark evidence
state:	OPEN
author:	iceboundrock (Ruoshi Li)
labels:	benchmark, enhancement, federated-query
comments:	0
assignees:	
projects:	
milestone:	
number:	51
--
## Summary

Optimize federated filter pushdown and EAV behavior using benchmark evidence.

## Why

After deep pagination is measurable and guarded, the next major opportunity is selective filtering across hot fields, EAV fields, and mixed predicates. This is where benchmark fidelity work should start paying off directly.

## Scope

- investigate hot-filter, EAV-filter, and mixed-filter workload costs
- optimize pushdown and tier-merge behavior where possible
- validate improvements with workload-level benchmark diffs and protected workload guardrails
- record trade-offs between target wins and non-target workload movement

## Acceptance Criteria

- targeted filter workloads improve under benchmark comparison
- deep-page and baseline workloads stay within agreed guardrails
- benchmark artifacts clearly show where the win came from and what risks remain

## References

- `internal/e2e_harness/federated/query.go`
- `internal/postgres_duckdb_query.go`
- `docs/federated-query/federated-query-benchmark-hld-en.md`


```

---

## Assistant (Plan · Claude Sonnet 4.6 · 9.4s)

Both open issues are legitimate enhancement work items — neither is stale or irrelevant. Here's the assessment:

---

## Issue Review

### Issues that can be closed: **None**

Both remaining open issues are valid, scoped, and not yet implemented:

| # | Title | Status |
|---|-------|--------|
| **51** | Optimize federated filter pushdown and EAV behavior using benchmark evidence | Open — not done |
| **54** | Optimize federated routing and concurrency behavior using benchmark evidence | Open — not done |

All other issues (the full backlog of ~30+) are already closed.

---

### Recommended next: **Issue #51**

**#51 should come before #54** — this matches the intended sequencing described in the issues themselves:

- #51 targets **filter pushdown and EAV** optimization — the "next major opportunity after deep pagination is guarded"
- #54 targets **routing heuristics and concurrency** — described as the step *after* filter paths are hardened

**#51 scope:**
- Investigate hot-filter, EAV-filter, and mixed-filter workload costs in `internal/postgres_duckdb_query.go`
- Optimize pushdown and tier-merge behavior
- Validate with benchmark diffs and guardrails

Would you like me to dig into the codebase to assess the current state of filter pushdown and EAV handling before we start on #51?

---

## User

OK, sound good, dig into the codebase to assess the current state of filter pushdown and EAV handling

---

## Assistant (Build · DeepSeek V4 Pro · 16.0s)

_Thinking:_

The user wants me to dig into the codebase to assess the current state of filter pushdown and EAV handling. Let me start by exploring the key files mentioned in issue #51 and understanding the overall architecture.

**Tool: todowrite**

**Input:**
```json
{
  "todos": [
    {
      "content": "Read benchmarks to understand current filter-related workloads and baselines",
      "status": "pending",
      "priority": "high"
    },
    {
      "content": "Read federated query harness (query.go) to understand EAV/filter benchmark setup",
      "status": "pending",
      "priority": "high"
    },
    {
      "content": "Read postgres_duckdb_query.go to assess filter pushdown implementation",
      "status": "pending",
      "priority": "high"
    },
    {
      "content": "Read entity_query_service.go to understand query routing and EAV query patterns",
      "status": "pending",
      "priority": "high"
    },
    {
      "content": "Check benchmark results / baselines for filter workloads",
      "status": "pending",
      "priority": "medium"
    },
    {
      "content": "Summarize current state, gaps, and optimization opportunities",
      "status": "pending",
      "priority": "medium"
    }
  ]
}
```

**Output:**
```
[
  {
    "content": "Read benchmarks to understand current filter-related workloads and baselines",
    "status": "pending",
    "priority": "high"
  },
  {
    "content": "Read federated query harness (query.go) to understand EAV/filter benchmark setup",
    "status": "pending",
    "priority": "high"
  },
  {
    "content": "Read postgres_duckdb_query.go to assess filter pushdown implementation",
    "status": "pending",
    "priority": "high"
  },
  {
    "content": "Read entity_query_service.go to understand query routing and EAV query patterns",
    "status": "pending",
    "priority": "high"
  },
  {
    "content": "Check benchmark results / baselines for filter workloads",
    "status": "pending",
    "priority": "medium"
  },
  {
    "content": "Summarize current state, gaps, and optimization opportunities",
    "status": "pending",
    "priority": "medium"
  }
]
```

---

## Assistant (Build · DeepSeek V4 Pro · 6.8s)

_Thinking:_

Let me start by finding the key files mentioned in the issue.

**Tool: glob**

**Input:**
```json
{
  "pattern": "**/e2e_harness/federated/query.go"
}
```

**Output:**
```
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/query.go
```

**Tool: glob**

**Input:**
```json
{
  "pattern": "**/postgres_duckdb_query.go"
}
```

**Output:**
```
/home/ruoshi/code/github/forma/internal/postgres_duckdb_query.go
```

**Tool: glob**

**Input:**
```json
{
  "pattern": "**/entity_query_service.go"
}
```

**Output:**
```
/home/ruoshi/code/github/forma/internal/entity_query_service.go
```

**Tool: glob**

**Input:**
```json
{
  "pattern": "**/federated-query-benchmark*.md"
}
```

**Output:**
```
/home/ruoshi/code/github/forma/docs/federated-query/federated-query-benchmark-autoresearch-pr-plan.md
/home/ruoshi/code/github/forma/docs/federated-query/federated-query-benchmark-autoresearch-issues.md
/home/ruoshi/code/github/forma/docs/federated-query/federated-query-benchmark-ci-and-ops-guide.md
/home/ruoshi/code/github/forma/docs/federated-query/federated-query-benchmark-baseline-runbook.md
/home/ruoshi/code/github/forma/docs/federated-query/federated-query-benchmark-reviewer-guide.md
/home/ruoshi/code/github/forma/docs/federated-query/federated-query-benchmark-issues.md
/home/ruoshi/code/github/forma/docs/federated-query/federated-query-benchmark-implementation-plan.md
/home/ruoshi/code/github/forma/docs/federated-query/federated-query-benchmark-hld-en.md
/home/ruoshi/code/github/forma/docs/cn/federated-query-benchmark-hld.md
```

---

## Assistant (Build · DeepSeek V4 Pro · 8.4s)

_Thinking:_

Let me read the key source files and benchmark-related files in parallel.

**Tool: read**

**Input:**
```json
{
  "filePath": "/home/ruoshi/code/github/forma/internal/postgres_duckdb_query.go"
}
```

**Output:**
```
<path>/home/ruoshi/code/github/forma/internal/postgres_duckdb_query.go</path>
<type>file</type>
<content>
1: package internal
2: 
3: import (
4: 	"context"
5: 	"fmt"
6: 	"strings"
7: 	"time"
8: 
9: 	"github.com/google/uuid"
10: 	"github.com/jackc/pgx/v5/pgxpool"
11: 	"github.com/lychee-technology/forma"
12: 	"github.com/lychee-technology/forma/internal/telemetry"
13: )
14: 
15: type duckDBRowsIterator interface {
16: 	Next() bool
17: 	Scan(dest ...any) error
18: 	Err() error
19: }
20: 
21: // ExecuteDuckDBFederatedQuery runs the DuckDB optimized query template using the provided
22: // FederatedAttributeQuery. It fetches dirty IDs from the Postgres change_log (if available),
23: // injects exclusions into the DuckDB WHERE clause, executes the query against the global
24: // DuckDB client, and returns matched PersistentRecords along with the total record count.
25: //
26: // Note: This implementation performs a best-effort scan of columns produced by the
27: // optimized query template. It mirrors the column ordering used by the Postgres template:
28: //   - main table projection (entity_main columns, order defined by entityMainColumnDescriptors)
29: //   - attributes_json (TEXT)
30: //   - total_records (bigint)
31: //   - total_pages (bigint)
32: //   - current_page (int)
33: func (r *DBPersistentRecordRepository) ExecuteDuckDBFederatedQuery(
34: 	ctx context.Context,
35: 	tables StorageTables,
36: 	q *FederatedAttributeQuery,
37: 	limit, offset int,
38: 	attributeOrders []AttributeOrder,
39: 	opts *FederatedQueryOptions,
40: ) ([]*PersistentRecord, int64, error) {
41: 	// Backwards-compatible wrapper that uses the streaming iterator internally
42: 	var recs []*PersistentRecord
43: 	total, err := r.StreamDuckDBFederatedQuery(ctx, tables, q, limit, offset, attributeOrders, opts, func(ctx context.Context, rp *PersistentRecord) error {
44: 		recs = append(recs, rp)
45: 		return nil
46: 	})
47: 	if err != nil {
48: 		return nil, 0, err
49: 	}
50: 	return recs, total, nil
51: }
52: 
53: // StreamDuckDBFederatedQuery streams DuckDB federated query results using a rowHandler callback.
54: // It reuses the same rowHandler semantics as Postgres' StreamOptimizedQuery to avoid loading the
55: // entire result set into memory.
56: func (r *DBPersistentRecordRepository) StreamDuckDBFederatedQuery(
57: 	ctx context.Context,
58: 	tables StorageTables,
59: 	q *FederatedAttributeQuery,
60: 	limit, offset int,
61: 	attributeOrders []AttributeOrder,
62: 	opts *FederatedQueryOptions,
63: 	rowHandler func(context.Context, *PersistentRecord) error,
64: ) (int64, error) {
65: 	if q == nil {
66: 		return 0, fmt.Errorf("query cannot be nil")
67: 	}
68: 
69: 	// Initialize execution plan tracking
70: 	planCtx := newDuckDBExecutionPlanContext(opts)
71: 
72: 	// Acquire DuckDB client
73: 	duck := r.duckDBClient
74: 	if duck == nil || duck.DB == nil {
75: 		planCtx.recordClientUnavailable()
76: 		return 0, fmt.Errorf("duckdb client not available")
77: 	}
78: 
79: 	// Fetch dirty IDs and record in execution plan
80: 	dirtyIDs, err := r.fetchAndRecordDirtyIDs(ctx, tables, q.SchemaID, planCtx)
81: 	if err != nil {
82: 		return 0, err
83: 	}
84: 
85: 	// Build and execute the query
86: 	sqlStr, args, translateMs, err := r.buildDuckDBQueryWithPlan(ctx, tables, q, dirtyIDs, attributeOrders, limit, offset, planCtx)
87: 	if err != nil {
88: 		return 0, err
89: 	}
90: 
91: 	// Record translation in plan
92: 	planCtx.recordTranslation(sqlStr, translateMs, q.UseMainAsAnchor)
93: 
94: 	// Execute query
95: 	planCtx.recordQueryStart()
96: 	rows, err := duck.DB.QueryContext(ctx, sqlStr, args...)
97: 	if err != nil {
98: 		planCtx.recordQueryFailure(err)
99: 		return 0, fmt.Errorf("execute duckdb query: %w", err)
100: 	}
101: 	defer rows.Close()
102: 
103: 	// Stream and process rows
104: 	totalRecords, rowCount, err := r.streamDuckDBRows(ctx, rows, rowHandler)
105: 	if err != nil {
106: 		return 0, err
107: 	}
108: 
109: 	// Finalize execution plan
110: 	r.finalizeDuckDBExecutionPlan(ctx, planCtx, dirtyIDs, totalRecords, rowCount)
111: 
112: 	return totalRecords, nil
113: }
114: 
115: // fetchAndRecordDirtyIDs fetches dirty row IDs from Postgres and records in execution plan.
116: func (r *DBPersistentRecordRepository) fetchAndRecordDirtyIDs(
117: 	ctx context.Context,
118: 	tables StorageTables,
119: 	schemaID int16,
120: 	planCtx *duckDBExecutionPlanContext,
121: ) ([]uuid.UUID, error) {
122: 	if tables.ChangeLog == "" {
123: 		return nil, nil
124: 	}
125: 
126: 	dirtyIDs, err := r.getDirtyIDFetcher()(ctx, tables.ChangeLog, schemaID)
127: 	if err != nil {
128: 		return nil, fmt.Errorf("fetch dirty ids: %w", err)
129: 	}
130: 
131: 	// Emit metric for dirty set size
132: 	telemetry.EmitRowCount(ctx, "pg", int64(len(dirtyIDs)))
133: 
134: 	// Record in execution plan
135: 	planCtx.recordDirtyIDSource(tables.ChangeLog, len(dirtyIDs))
136: 
137: 	return dirtyIDs, nil
138: }
139: 
140: // buildDuckDBQueryWithPlan builds the DuckDB query with execution plan recording.
141: func (r *DBPersistentRecordRepository) buildDuckDBQueryWithPlan(
142: 	ctx context.Context,
143: 	tables StorageTables,
144: 	q *FederatedAttributeQuery,
145: 	dirtyIDs []uuid.UUID,
146: 	attributeOrders []AttributeOrder,
147: 	limit, offset int,
148: 	planCtx *duckDBExecutionPlanContext,
149: ) (string, []any, int64, error) {
150: 	// Build template params
151: 	changeLogSchema, changeLogScanTable := duckDBPostgresScanLocation(tables.ChangeLog)
152: 	mainSchema, mainScanTable := duckDBPostgresScanLocation(tables.EntityMain)
153: 	eavSchema, eavScanTable := duckDBPostgresScanLocation(tables.EAVData)
154: 	sqlParams := map[string]any{
155: 		"EAVTable":             sanitizeIdentifier(tables.EAVData),
156: 		"MainTable":            sanitizeIdentifier(tables.EntityMain),
157: 		"ChangeLogTable":       sanitizeIdentifier(tables.ChangeLog),
158: 		"ChangeLogSchema":      changeLogSchema,
159: 		"ChangeLogScanTable":   changeLogScanTable,
160: 		"MainSchema":           mainSchema,
161: 		"MainScanTable":        mainScanTable,
162: 		"EAVSchema":            eavSchema,
163: 		"EAVScanTable":         eavScanTable,
164: 		"MainProjection":       entityMainProjection,
165: 		"SchemaID":             q.SchemaID,
166: 		"UseMainTableAsAnchor": q.UseMainAsAnchor,
167: 		"Anchor": map[string]any{
168: 			"Condition": "1=1", // BuildDuckDBQuery will overwrite with actual where clause
169: 		},
170: 		"SortKeys": attributeOrders,
171: 		"Limit":    limit,
172: 		"Offset":   offset,
173: 		"PageSize": limit,
174: 	}
175: 
176: 	if connStr := r.duckDBPostgresConnString(); connStr != "" {
177: 		sqlParams["DuckDBPGConnString"] = connStr
178: 	}
179: 	if paths := duckDBParquetPathsForQuery(q); len(paths) > 0 {
180: 		sqlParams["DuckDBS3Paths"] = paths
181: 	}
182: 
183: 	startTranslate := time.Now()
184: 
185: 	// Build dual clauses (PG pushdown + DuckDB logical) if metadata cache available
186: 	var cache forma.SchemaAttributeCache
187: 	if r.metadataCache != nil {
188: 		if c, ok := r.metadataCache.GetSchemaCacheByID(q.SchemaID); ok {
189: 			cache = c
190: 		}
191: 	}
192: 	paramIndex := 0
193: 	dc, err := ToDualClauses(q.Condition, sanitizeIdentifier(tables.EAVData), q.SchemaID, cache, &paramIndex)
194: 	if err != nil {
195: 		return "", nil, 0, fmt.Errorf("to dual clauses: %w", err)
196: 	}
197: 
198: 	// Record Postgres pushdown fragment
199: 	planCtx.recordPushdownFragment(dc.PgMainClause)
200: 
201: 	sqlStr, args, err := r.getDuckDBQueryBuilder()(r.getDuckDBTemplate(), sqlParams, q, dirtyIDs, &dc)
202: 	translateMs := time.Since(startTranslate).Milliseconds()
203: 	telemetry.EmitLatency(ctx, "translation", translateMs)
204: 	if err != nil {
205: 		return "", nil, 0, fmt.Errorf("build duckdb query: %w", err)
206: 	}
207: 
208: 	return sqlStr, args, translateMs, nil
209: }
210: 
211: func (r *DBPersistentRecordRepository) duckDBPostgresConnString() string {
212: 	if r.pool == nil {
213: 		return ""
214: 	}
215: 	if cfgPool, ok := r.pool.(*pgxpool.Pool); ok && cfgPool != nil {
216: 		cfg := cfgPool.Config()
217: 		if cfg != nil {
218: 			connCfg := cfg.ConnConfig
219: 			return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
220: 				connCfg.Host,
221: 				connCfg.Port,
222: 				connCfg.User,
223: 				connCfg.Password,
224: 				connCfg.Database,
225: 			)
226: 		}
227: 	}
228: 	return ""
229: }
230: 
231: func duckDBParquetPathsForQuery(q *FederatedAttributeQuery) []string {
232: 	if q == nil || q.DuckDBHints == nil || q.DuckDBHints.S3ParquetPathTemplate == "" {
233: 		return nil
234: 	}
235: 	rendered, err := RenderS3ParquetPath(q.DuckDBHints.S3ParquetPathTemplate, q.SchemaID)
236: 	if err != nil {
237: 		return nil
238: 	}
239: 	parts := strings.Split(rendered, ",")
240: 	paths := make([]string, 0, len(parts))
241: 	for _, part := range parts {
242: 		trimmed := strings.TrimSpace(part)
243: 		if trimmed != "" {
244: 			paths = append(paths, trimmed)
245: 		}
246: 	}
247: 	return paths
248: }
249: 
250: func duckDBPostgresScanLocation(name string) (string, string) {
251: 	parts := strings.Split(name, ".")
252: 	clean := make([]string, 0, len(parts))
253: 	for _, part := range parts {
254: 		trimmed := strings.Trim(part, " \"")
255: 		if trimmed != "" {
256: 			clean = append(clean, trimmed)
257: 		}
258: 	}
259: 	if len(clean) >= 2 {
260: 		return clean[0], clean[1]
261: 	}
262: 	if len(clean) == 1 {
263: 		return "public", clean[0]
264: 	}
265: 	return "public", ""
266: }
267: 
268: // streamDuckDBRows iterates through DuckDB rows and invokes the handler.
269: func (r *DBPersistentRecordRepository) streamDuckDBRows(
270: 	ctx context.Context,
271: 	rows duckDBRowsIterator,
272: 	rowHandler func(context.Context, *PersistentRecord) error,
273: ) (int64, int64, error) {
274: 	buffers := newDuckDBScanBuffers()
275: 
276: 	var totalRecords int64
277: 	totalSet := false
278: 	rowCount := int64(0)
279: 
280: 	for rows.Next() {
281: 		scanArgs, attrsJSON, totalRec, _, _ := buffers.buildScanArgs()
282: 
283: 		if err := rows.Scan(scanArgs...); err != nil {
284: 			return 0, 0, fmt.Errorf("scan duckdb row: %w", err)
285: 		}
286: 
287: 		// Build record from buffers
288: 		record := buffers.buildRecordFromBuffers()
289: 
290: 		// Parse attributes JSON
291: 		if attrsJSON.Valid {
292: 			if err := parseDuckDBAttributesJSON(attrsJSON.String, record); err != nil {
293: 				return 0, 0, err
294: 			}
295: 		}
296: 
297: 		// Clean up empty maps
298: 		cleanupEmptyMaps(record)
299: 
300: 		if !totalSet && totalRec.Valid {
301: 			totalRecords = totalRec.Int64
302: 			totalSet = true
303: 		}
304: 
305: 		// Invoke handler
306: 		if rowHandler != nil {
307: 			if err := rowHandler(ctx, record); err != nil {
308: 				return 0, 0, err
309: 			}
310: 		}
311: 
312: 		rowCount++
313: 	}
314: 
315: 	if err := rows.Err(); err != nil {
316: 		return 0, 0, fmt.Errorf("iterate duckdb rows: %w", err)
317: 	}
318: 
319: 	return totalRecords, rowCount, nil
320: }
321: 
322: // finalizeDuckDBExecutionPlan completes the execution plan with timing and metrics.
323: func (r *DBPersistentRecordRepository) finalizeDuckDBExecutionPlan(
324: 	ctx context.Context,
325: 	planCtx *duckDBExecutionPlanContext,
326: 	dirtyIDs []uuid.UUID,
327: 	totalRecords int64,
328: 	rowCount int64,
329: ) {
330: 	if planCtx.opts == nil || !planCtx.opts.IncludeExecutionPlan || planCtx.opts.ExecutionPlan == nil {
331: 		return
332: 	}
333: 
334: 	qMs := time.Since(planCtx.startQuery).Milliseconds()
335: 
336: 	// Update the last source with actual rows and duration
337: 	if len(planCtx.opts.ExecutionPlan.Sources) > 0 {
338: 		idx := len(planCtx.opts.ExecutionPlan.Sources) - 1
339: 		dp := planCtx.opts.ExecutionPlan.Sources[idx]
340: 		dp.ActualRows = rowCount
341: 		dp.DurationMs = qMs
342: 		planCtx.opts.ExecutionPlan.Sources[idx] = dp
343: 	}
344: 
345: 	planCtx.opts.ExecutionPlan.Timings["duckdb_fetch"] = qMs
346: 	planCtx.opts.ExecutionPlan.Timings["total"] = time.Since(planCtx.startTotal).Milliseconds()
347: 
348: 	// Emit telemetry
349: 	telemetry.EmitLatency(ctx, "execution", qMs)
350: 	streamMs := max(time.Since(planCtx.startQuery).Milliseconds()-qMs, 0)
351: 	telemetry.EmitLatency(ctx, "streaming", streamMs)
352: 	telemetry.EmitRowCount(ctx, "duckdb", rowCount)
353: 
354: 	// Compute pushdown efficiency
355: 	pgRows := computePgRowCount(planCtx.opts.ExecutionPlan, dirtyIDs)
356: 	finalRows := totalRecords
357: 	if finalRows <= 0 {
358: 		finalRows = rowCount
359: 	}
360: 	if finalRows <= 0 {
361: 		finalRows = 1
362: 	}
363: 	ratio := float64(pgRows) / float64(finalRows)
364: 	telemetry.EmitPushdownEfficiency(ctx, 0, ratio) // schemaID not available here, use 0
365: 
366: 	planCtx.opts.ExecutionPlan.Notes = append(planCtx.opts.ExecutionPlan.Notes,
367: 		fmt.Sprintf("pushdown_efficiency=%.3f (pg_rows=%d final_rows=%d)", ratio, pgRows, finalRows))
368: }
369: 
370: // computePgRowCount calculates the Postgres row count from execution plan sources.
371: func computePgRowCount(plan *ExecutionPlan, dirtyIDs []uuid.UUID) int64 {
372: 	var pgRows int64
373: 	for _, src := range plan.Sources {
374: 		if src.Engine == "postgres" {
375: 			if src.ActualRows > 0 {
376: 				pgRows += src.ActualRows
377: 			} else if src.RowEstimate > 0 {
378: 				pgRows += src.RowEstimate
379: 			}
380: 		}
381: 	}
382: 	// Fallback: use dirtyIDs size as a proxy
383: 	if pgRows == 0 {
384: 		pgRows = int64(len(dirtyIDs))
385: 	}
386: 	return pgRows
387: }

(End of file - total 387 lines)
</content>
```

**Tool: read**

**Input:**
```json
{
  "filePath": "/home/ruoshi/code/github/forma/internal/entity_query_service.go"
}
```

**Output:**
```
<path>/home/ruoshi/code/github/forma/internal/entity_query_service.go</path>
<type>file</type>
<content>
1: package internal
2: 
3: import (
4: 	"context"
5: 	"fmt"
6: 	"time"
7: 
8: 	"github.com/lychee-technology/forma"
9: 	"go.uber.org/zap"
10: )
11: 
12: var defaultFederatedPreferredTiers = []DataTier{DataTierHot, DataTierWarm, DataTierCold}
13: 
14: type dataRecordConverter func(context.Context, string, *PersistentRecord) (*forma.DataRecord, error)
15: type dataRecordEnricher func(context.Context, string, []string, ...*forma.DataRecord) error
16: type storageTablesResolver func() StorageTables
17: 
18: type entityQueryService struct {
19: 	repository        PersistentRecordRepository
20: 	registry          forma.SchemaRegistry
21: 	config            *forma.Config
22: 	toDataRecord      dataRecordConverter
23: 	enrichDataRecords dataRecordEnricher
24: 	storageTables     storageTablesResolver
25: }
26: 
27: func newEntityQueryService(em *entityManager) *entityQueryService {
28: 	if em == nil {
29: 		return &entityQueryService{}
30: 	}
31: 	return &entityQueryService{
32: 		repository:        em.repository,
33: 		registry:          em.registry,
34: 		config:            em.config,
35: 		toDataRecord:      em.toDataRecord,
36: 		enrichDataRecords: em.enrichDataRecords,
37: 		storageTables:     em.storageTables,
38: 	}
39: }
40: 
41: func (s *entityQueryService) Query(ctx context.Context, req *forma.QueryRequest) (*forma.QueryResult, error) {
42: 	if s.config == nil {
43: 		return nil, fmt.Errorf("entity manager config is required: %w", forma.ErrInvalidInput)
44: 	}
45: 	if s.registry == nil || s.repository == nil || s.toDataRecord == nil || s.enrichDataRecords == nil {
46: 		return nil, fmt.Errorf("entity query service is not initialized: %w", forma.ErrInvalidInput)
47: 	}
48: 
49: 	if req == nil {
50: 		return nil, fmt.Errorf("query request cannot be nil: %w", forma.ErrInvalidInput)
51: 	}
52: 
53: 	if req.SchemaName == "" {
54: 		return nil, fmt.Errorf("schema name is required: %w", forma.ErrInvalidInput)
55: 	}
56: 
57: 	if req.Page < 1 {
58: 		req.Page = 1
59: 	}
60: 
61: 	if req.ItemsPerPage < 1 {
62: 		req.ItemsPerPage = s.config.Query.DefaultPageSize
63: 	}
64: 
65: 	if req.ItemsPerPage > s.config.Query.MaxPageSize {
66: 		req.ItemsPerPage = s.config.Query.MaxPageSize
67: 	}
68: 
69: 	// Verify schema exists and get attribute metadata.
70: 	schemaID, schemaCache, err := s.registry.GetSchemaAttributeCacheByName(req.SchemaName)
71: 	if err != nil {
72: 		return nil, fmt.Errorf("failed to get schema: %w", err)
73: 	}
74: 
75: 	sortOrder := req.SortOrder
76: 	if sortOrder == "" {
77: 		sortOrder = forma.SortOrderAsc
78: 	}
79: 
80: 	attributeOrders := make([]AttributeOrder, 0, len(req.SortBy))
81: 	for _, sortAttr := range req.SortBy {
82: 		meta, ok := schemaCache[sortAttr]
83: 		if !ok {
84: 			return nil, fmt.Errorf("cannot sort by unknown attribute '%s' in schema '%s'", sortAttr, req.SchemaName)
85: 		}
86: 		order := AttributeOrder{
87: 			AttrID:    meta.AttributeID,
88: 			ValueType: meta.ValueType,
89: 			SortOrder: sortOrder,
90: 		}
91: 		// Check if attribute has column_binding to main table.
92: 		if meta.ColumnBinding != nil {
93: 			order.StorageLocation = forma.AttributeStorageLocationMain
94: 			order.ColumnName = string(meta.ColumnBinding.ColumnName)
95: 		} else {
96: 			order.StorageLocation = forma.AttributeStorageLocationEAV
97: 		}
98: 		attributeOrders = append(attributeOrders, order)
99: 	}
100: 
101: 	query := &PersistentRecordQuery{
102: 		Tables:          s.resolveTables(),
103: 		SchemaID:        schemaID,
104: 		Condition:       req.Condition,
105: 		AttributeOrders: attributeOrders,
106: 		Limit:           req.ItemsPerPage,
107: 		Offset:          (req.Page - 1) * req.ItemsPerPage,
108: 	}
109: 
110: 	startTime := time.Now()
111: 	page, err := s.queryRecords(ctx, query, req)
112: 	if err != nil {
113: 		return nil, fmt.Errorf("failed to query persistent records: %w", err)
114: 	}
115: 
116: 	records := make([]*forma.DataRecord, 0, len(page.Records))
117: 	for _, record := range page.Records {
118: 		dataRecord, err := s.toDataRecord(ctx, req.SchemaName, record)
119: 		if err != nil {
120: 			return nil, err
121: 		}
122: 		records = append(records, dataRecord)
123: 	}
124: 
125: 	if err := s.enrichDataRecords(ctx, req.SchemaName, req.Attrs, records...); err != nil {
126: 		return nil, err
127: 	}
128: 
129: 	applyProjection(records, req.Attrs)
130: 
131: 	totalPages := page.TotalPages
132: 	if totalPages == 0 && page.TotalRecords > 0 && req.ItemsPerPage > 0 {
133: 		totalPages = int((page.TotalRecords + int64(req.ItemsPerPage) - 1) / int64(req.ItemsPerPage))
134: 	}
135: 
136: 	zap.S().Infow("query results", "records", len(records), "totalPages", totalPages)
137: 
138: 	return &forma.QueryResult{
139: 		Data:          records,
140: 		TotalRecords:  int(page.TotalRecords),
141: 		TotalPages:    totalPages,
142: 		CurrentPage:   req.Page,
143: 		ItemsPerPage:  req.ItemsPerPage,
144: 		HasNext:       req.Page < totalPages,
145: 		HasPrevious:   req.Page > 1,
146: 		ExecutionTime: time.Since(startTime),
147: 	}, nil
148: }
149: 
150: func (s *entityQueryService) queryRecords(ctx context.Context, query *PersistentRecordQuery, req *forma.QueryRequest) (*PersistentRecordPage, error) {
151: 	if query == nil {
152: 		return nil, fmt.Errorf("query cannot be nil")
153: 	}
154: 	if req == nil || req.Federated == nil || !req.Federated.Enabled {
155: 		return s.repository.QueryPersistentRecords(ctx, query)
156: 	}
157: 
158: 	fq := &FederatedAttributeQuery{
159: 		AttributeQuery: AttributeQuery{
160: 			SchemaID:        query.SchemaID,
161: 			Condition:       query.Condition,
162: 			AttributeOrders: query.AttributeOrders,
163: 			Limit:           query.Limit,
164: 			Offset:          query.Offset,
165: 		},
166: 		PreferredTiers: federatedPreferredTiers(req.Federated.PreferredTiers),
167: 		PreferHot:      req.Federated.PreferHot,
168: 		UseMainAsAnchor: req.Federated.UseMainAsAnchor,
169: 	}
170: 	if req.Federated.S3ParquetPathTemplate != "" {
171: 		fq.DuckDBHints = &DuckDBRenderHints{S3ParquetPathTemplate: req.Federated.S3ParquetPathTemplate}
172: 	}
173: 
174: 	return s.repository.QueryPersistentRecordsFederated(ctx, query.Tables, fq, &FederatedQueryOptions{
175: 		AllowPartialDegradedMode: req.Federated.AllowPartialDegradedMode,
176: 	})
177: }
178: 
179: func federatedPreferredTiers(tiers []string) []DataTier {
180: 	if len(tiers) == 0 {
181: 		return append([]DataTier(nil), defaultFederatedPreferredTiers...)
182: 	}
183: 	out := make([]DataTier, 0, len(tiers))
184: 	for _, tier := range tiers {
185: 		switch DataTier(tier) {
186: 		case DataTierHot, DataTierWarm, DataTierCold:
187: 			out = append(out, DataTier(tier))
188: 		}
189: 	}
190: 	if len(out) == 0 {
191: 		return append([]DataTier(nil), defaultFederatedPreferredTiers...)
192: 	}
193: 	return out
194: }
195: 
196: func (s *entityQueryService) CrossSchemaSearch(ctx context.Context, req *forma.CrossSchemaRequest) (*forma.QueryResult, error) {
197: 	if s.config == nil {
198: 		return nil, fmt.Errorf("entity manager config is required: %w", forma.ErrInvalidInput)
199: 	}
200: 	if s.registry == nil || s.repository == nil || s.toDataRecord == nil || s.enrichDataRecords == nil {
201: 		return nil, fmt.Errorf("entity query service is not initialized: %w", forma.ErrInvalidInput)
202: 	}
203: 
204: 	if err := s.validateCrossSchemaRequest(req); err != nil {
205: 		return nil, err
206: 	}
207: 
208: 	startTime := time.Now()
209: 	tables := s.resolveTables()
210: 
211: 	// Build schema contexts with conditions.
212: 	schemaContexts, err := s.buildSchemaContexts(req.SchemaNames, req.Condition)
213: 	if err != nil {
214: 		return nil, err
215: 	}
216: 
217: 	// Count records per schema.
218: 	schemaTotals, err := s.countSchemaRecords(ctx, tables, schemaContexts)
219: 	if err != nil {
220: 		return nil, err
221: 	}
222: 
223: 	// Calculate total.
224: 	var totalRecords int64
225: 	for _, count := range schemaTotals {
226: 		totalRecords += count
227: 	}
228: 
229: 	// Return empty result if no records.
230: 	if totalRecords == 0 {
231: 		return s.emptyQueryResult(req.Page, req.ItemsPerPage, startTime), nil
232: 	}
233: 
234: 	// Fetch paginated results across schemas.
235: 	results, err := s.fetchCrossSchemaResults(ctx, tables, schemaContexts, schemaTotals, req)
236: 	if err != nil {
237: 		return nil, err
238: 	}
239: 
240: 	totalPages := int((totalRecords + int64(req.ItemsPerPage) - 1) / int64(req.ItemsPerPage))
241: 	return &forma.QueryResult{
242: 		Data:          results,
243: 		TotalRecords:  int(totalRecords),
244: 		TotalPages:    totalPages,
245: 		CurrentPage:   req.Page,
246: 		ItemsPerPage:  req.ItemsPerPage,
247: 		HasNext:       req.Page < totalPages,
248: 		HasPrevious:   req.Page > 1,
249: 		ExecutionTime: time.Since(startTime),
250: 	}, nil
251: }
252: 
253: // validateCrossSchemaRequest validates the cross schema search request parameters.
254: func (s *entityQueryService) validateCrossSchemaRequest(req *forma.CrossSchemaRequest) error {
255: 	if req == nil {
256: 		return fmt.Errorf("cross schema request cannot be nil: %w", forma.ErrInvalidInput)
257: 	}
258: 	if len(req.SchemaNames) == 0 {
259: 		return fmt.Errorf("schema names are required: %w", forma.ErrInvalidInput)
260: 	}
261: 	if req.SearchTerm == "" {
262: 		return fmt.Errorf("search term is required: %w", forma.ErrInvalidInput)
263: 	}
264: 	if req.Page < 1 {
265: 		req.Page = 1
266: 	}
267: 	if req.ItemsPerPage < 1 {
268: 		req.ItemsPerPage = s.config.Query.DefaultPageSize
269: 	}
270: 	if req.ItemsPerPage > s.config.Query.MaxPageSize {
271: 		req.ItemsPerPage = s.config.Query.MaxPageSize
272: 	}
273: 	return nil
274: }
275: 
276: // schemaContext holds schema information for cross-schema queries.
277: type schemaContext struct {
278: 	name      string
279: 	id        int16
280: 	condition forma.Condition
281: }
282: 
283: // buildSchemaContexts builds schema contexts from schema names.
284: func (s *entityQueryService) buildSchemaContexts(schemaNames []string, condition forma.Condition) ([]schemaContext, error) {
285: 	searchCondition := condition
286: 	if searchCondition == nil {
287: 		searchCondition = &forma.CompositeCondition{
288: 			Logic:      forma.LogicAnd,
289: 			Conditions: []forma.Condition{},
290: 		}
291: 	}
292: 
293: 	contexts := make([]schemaContext, 0, len(schemaNames))
294: 	for _, schemaName := range schemaNames {
295: 		schemaID, _, err := s.registry.GetSchemaAttributeCacheByName(schemaName)
296: 		if err != nil {
297: 			return nil, fmt.Errorf("failed to get schema %s: %w", schemaName, err)
298: 		}
299: 		contexts = append(contexts, schemaContext{
300: 			name:      schemaName,
301: 			id:        schemaID,
302: 			condition: searchCondition,
303: 		})
304: 	}
305: 	return contexts, nil
306: }
307: 
308: // countSchemaRecords counts total records for each schema.
309: func (s *entityQueryService) countSchemaRecords(ctx context.Context, tables StorageTables, contexts []schemaContext) ([]int64, error) {
310: 	totals := make([]int64, len(contexts))
311: 	for idx, schemaCtx := range contexts {
312: 		page, err := s.repository.QueryPersistentRecords(ctx, &PersistentRecordQuery{
313: 			Tables:    tables,
314: 			SchemaID:  schemaCtx.id,
315: 			Condition: schemaCtx.condition,
316: 			Limit:     1,
317: 			Offset:    0,
318: 		})
319: 		if err != nil {
320: 			return nil, fmt.Errorf("failed to count records for schema %s: %w", schemaCtx.name, err)
321: 		}
322: 		totals[idx] = page.TotalRecords
323: 	}
324: 	return totals, nil
325: }
326: 
327: // emptyQueryResult returns an empty query result.
328: func (s *entityQueryService) emptyQueryResult(page, itemsPerPage int, startTime time.Time) *forma.QueryResult {
329: 	return &forma.QueryResult{
330: 		Data:          []*forma.DataRecord{},
331: 		TotalRecords:  0,
332: 		TotalPages:    0,
333: 		CurrentPage:   page,
334: 		ItemsPerPage:  itemsPerPage,
335: 		HasNext:       false,
336: 		HasPrevious:   page > 1,
337: 		ExecutionTime: time.Since(startTime),
338: 	}
339: }
340: 
341: // fetchCrossSchemaResults fetches paginated results across multiple schemas.
342: func (s *entityQueryService) fetchCrossSchemaResults(
343: 	ctx context.Context,
344: 	tables StorageTables,
345: 	contexts []schemaContext,
346: 	schemaTotals []int64,
347: 	req *forma.CrossSchemaRequest,
348: ) ([]*forma.DataRecord, error) {
349: 	offset := (req.Page - 1) * req.ItemsPerPage
350: 	remaining := req.ItemsPerPage
351: 	results := make([]*forma.DataRecord, 0, req.ItemsPerPage)
352: 	skip := offset
353: 
354: 	for idx, schemaCtx := range contexts {
355: 		count := int(schemaTotals[idx])
356: 		if skip >= count {
357: 			skip -= count
358: 			continue
359: 		}
360: 
361: 		schemaOffset := skip
362: 		skip = 0
363: 		avail := count - schemaOffset
364: 		schemaLimit := min(avail, remaining)
365: 		if schemaLimit <= 0 {
366: 			continue
367: 		}
368: 
369: 		batchRecords, err := s.fetchSchemaBatch(ctx, tables, schemaCtx, schemaOffset, schemaLimit, req.Attrs)
370: 		if err != nil {
371: 			return nil, err
372: 		}
373: 
374: 		results = append(results, batchRecords...)
375: 		remaining -= len(batchRecords)
376: 		if remaining <= 0 {
377: 			break
378: 		}
379: 	}
380: 
381: 	return results, nil
382: }
383: 
384: // fetchSchemaBatch fetches a batch of records from a single schema.
385: func (s *entityQueryService) fetchSchemaBatch(
386: 	ctx context.Context,
387: 	tables StorageTables,
388: 	schemaCtx schemaContext,
389: 	offset, limit int,
390: 	attrs []string,
391: ) ([]*forma.DataRecord, error) {
392: 	page, err := s.repository.QueryPersistentRecords(ctx, &PersistentRecordQuery{
393: 		Tables:    tables,
394: 		SchemaID:  schemaCtx.id,
395: 		Condition: schemaCtx.condition,
396: 		Limit:     limit,
397: 		Offset:    offset,
398: 	})
399: 	if err != nil {
400: 		return nil, fmt.Errorf("failed to fetch records for schema %s: %w", schemaCtx.name, err)
401: 	}
402: 
403: 	batchRecords := make([]*forma.DataRecord, 0, len(page.Records))
404: 	for _, record := range page.Records {
405: 		dataRecord, err := s.toDataRecord(ctx, schemaCtx.name, record)
406: 		if err != nil {
407: 			return nil, err
408: 		}
409: 		batchRecords = append(batchRecords, dataRecord)
410: 	}
411: 
412: 	if err := s.enrichDataRecords(ctx, schemaCtx.name, attrs, batchRecords...); err != nil {
413: 		return nil, err
414: 	}
415: 
416: 	applyProjection(batchRecords, attrs)
417: 	return batchRecords, nil
418: }
419: 
420: func (s *entityQueryService) resolveTables() StorageTables {
421: 	if s.storageTables == nil {
422: 		return StorageTables{}
423: 	}
424: 	return s.storageTables()
425: }

(End of file - total 425 lines)
</content>
```

**Tool: read**

**Input:**
```json
{
  "filePath": "/home/ruoshi/code/github/forma/internal/e2e_harness/federated/query.go"
}
```

**Output:**
```
<path>/home/ruoshi/code/github/forma/internal/e2e_harness/federated/query.go</path>
<type>file</type>
<content>
1: package federated
2: 
3: import (
4: 	"context"
5: 	"database/sql"
6: 	"fmt"
7: 	"strings"
8: 	"time"
9: 
10: 	"github.com/google/uuid"
11: 	"github.com/lychee-technology/forma/internal"
12: )
13: 
14: const (
15: 	benchmarkSchemaIDCustomer int16 = 100
16: 	benchmarkSchemaIDSecurity int16 = 101
17: 	benchmarkSchemaIDTrade    int16 = 102
18: )
19: 
20: // ExecuteFederatedQuery executes a federated query using DuckDB.
21: func (h *FederatedTestHarness) ExecuteFederatedQuery(ctx context.Context, opts *QueryOptions) (*QueryResult, error) {
22: 	opts = normalizeQueryOptions(opts)
23: 	start := time.Now()
24: 	if opts.PreferHot {
25: 		result, err := h.ExecutePostgresQuery(ctx, opts)
26: 		if err != nil {
27: 			return nil, err
28: 		}
29: 		if result.Plan == nil {
30: 			result.Plan = &internal.ExecutionPlan{Notes: []string{}, Timings: map[string]int64{}}
31: 		}
32: 		result.Plan.Notes = append(result.Plan.Notes, "prefer_hot_override", "postgres_only_execution")
33: 		result.Plan.Timings["total"] = time.Since(start).Milliseconds()
34: 		result.Duration = time.Since(start)
35: 		return result, nil
36: 	}
37: 	benchmarkProjection := usesBenchmarkProjectionForSelect(opts)
38: 	tradeTimeOnlyProjection := usesTradeTimeOnlyBenchmarkProjectionForSelect(opts)
39: 	if needsBenchmarkDuckDBMacros(opts, benchmarkProjection, tradeTimeOnlyProjection) {
40: 		if err := prepareBenchmarkDuckDBMacros(ctx, h); err != nil {
41: 			return nil, err
42: 		}
43: 	}
44: 
45: 	// Check which tiers have parquet files
46: 	hasBaseFiles, hasDeltaFiles, err := h.checkTierFiles(ctx)
47: 	if err != nil {
48: 		return nil, err
49: 	}
50: 
51: 	// Get dirty IDs from change_log
52: 	dirtyIDs, err := h.getDirtyIDs(ctx)
53: 	if err != nil {
54: 		return nil, fmt.Errorf("get dirty ids: %w", err)
55: 	}
56: 
57: 	// If no parquet files exist and no hot records, fall back to Postgres-only query
58: 	if !hasBaseFiles && !hasDeltaFiles && len(dirtyIDs) == 0 {
59: 		return h.ExecutePostgresQuery(ctx, opts)
60: 	}
61: 
62: 	// Build the S3 paths
63: 	basePath := fmt.Sprintf("s3://%s/%s/%d/base/*.parquet", h.S3Bucket, h.S3Prefix, h.SchemaID)
64: 	deltaPath := fmt.Sprintf("s3://%s/%s/%d/delta/*.parquet", h.S3Bucket, h.S3Prefix, h.SchemaID)
65: 
66: 	// Build and execute the federated query
67: 	query := h.buildFederatedQuerySQLDynamic(basePath, deltaPath, hasBaseFiles, hasDeltaFiles, dirtyIDs, opts)
68: 	countQuery := h.buildFederatedQueryCountSQLDynamic(basePath, deltaPath, hasBaseFiles, hasDeltaFiles, dirtyIDs, opts)
69: 
70: 	var totalRecords int64
71: 	if err := h.Duck.DB.QueryRowContext(ctx, countQuery).Scan(&totalRecords); err != nil {
72: 		if isFederatedTierFileError(err) {
73: 			return h.ExecutePostgresQuery(ctx, opts)
74: 		}
75: 		return nil, fmt.Errorf("count query: %w", err)
76: 	}
77: 	if opts.CountOnly {
78: 		return &QueryResult{TotalRecords: totalRecords, Duration: time.Since(start), Plan: buildExecutionPlan(len(dirtyIDs), hasBaseFiles, hasDeltaFiles, time.Since(start))}, nil
79: 	}
80: 	if shouldSkipFederatedSelect(totalRecords, opts.Offset) {
81: 		plan := buildExecutionPlan(len(dirtyIDs), hasBaseFiles, hasDeltaFiles, time.Since(start))
82: 		plan.Notes = append(plan.Notes, "empty_page_short_circuit")
83: 		return &QueryResult{
84: 			Records:      nil,
85: 			TotalRecords: totalRecords,
86: 			Duration:     time.Since(start),
87: 			Plan:         plan,
88: 		}, nil
89: 	}
90: 
91: 	rows, err := h.Duck.DB.QueryContext(ctx, query)
92: 	if err != nil {
93: 		if isFederatedTierFileError(err) {
94: 			return h.ExecutePostgresQuery(ctx, opts)
95: 		}
96: 		return nil, fmt.Errorf("execute query: %w", err)
97: 	}
98: 	defer rows.Close()
99: 
100: 	records, err := h.scanQueryResults(rows, benchmarkProjection)
101: 	if err != nil {
102: 		return nil, err
103: 	}
104: 
105: 	duration := time.Since(start)
106: 	plan := buildExecutionPlan(len(dirtyIDs), hasBaseFiles, hasDeltaFiles, duration)
107: 
108: 	return &QueryResult{
109: 		Records:      records,
110: 		TotalRecords: totalRecords,
111: 		Duration:     duration,
112: 		Plan:         plan,
113: 	}, nil
114: }
115: 
116: // normalizeQueryOptions sets default values for query options.
117: func normalizeQueryOptions(opts *QueryOptions) *QueryOptions {
118: 	if opts == nil {
119: 		return &QueryOptions{Limit: 100}
120: 	}
121: 	if opts.Limit == 0 {
122: 		opts.Limit = 100
123: 	}
124: 	return opts
125: }
126: 
127: // checkTierFiles checks which tiers have parquet files.
128: func (h *FederatedTestHarness) checkTierFiles(ctx context.Context) (hasBase, hasDelta bool, err error) {
129: 	baseFiles, err := h.ListParquetFiles(ctx, "base")
130: 	if err != nil {
131: 		return false, false, fmt.Errorf("list base files: %w", err)
132: 	}
133: 	deltaFiles, err := h.ListParquetFiles(ctx, "delta")
134: 	if err != nil {
135: 		return false, false, fmt.Errorf("list delta files: %w", err)
136: 	}
137: 	return len(baseFiles) > 0, len(deltaFiles) > 0, nil
138: }
139: 
140: // scanQueryResults scans DuckDB query rows into PersistentRecords.
141: func (h *FederatedTestHarness) scanQueryResults(rows *sql.Rows, benchmarkProjection bool) ([]*internal.PersistentRecord, error) {
142: 	var records []*internal.PersistentRecord
143: 	for rows.Next() {
144: 		var rowID string
145: 		var schemaID int16
146: 		var changedAt, deletedAt int64
147: 		var name sql.NullString
148: 		var version sql.NullInt64
149: 		var symbol, exchange, region sql.NullString
150: 		var tradeType, tradeTime sql.NullInt64
151: 
152: 		if benchmarkProjection {
153: 			if err := rows.Scan(&rowID, &schemaID, &changedAt, &deletedAt, &name, &version, &symbol, &exchange, &region, &tradeType, &tradeTime); err != nil {
154: 				return nil, fmt.Errorf("scan row: %w", err)
155: 			}
156: 		} else {
157: 			if err := rows.Scan(&rowID, &schemaID, &changedAt, &deletedAt, &name, &version); err != nil {
158: 				return nil, fmt.Errorf("scan row: %w", err)
159: 			}
160: 		}
161: 
162: 		rec := &internal.PersistentRecord{
163: 			RowID:        uuid.MustParse(rowID),
164: 			SchemaID:     schemaID,
165: 			CreatedAt:    changedAt,
166: 			UpdatedAt:    changedAt,
167: 			TextItems:    make(map[string]string),
168: 			Float64Items: make(map[string]float64),
169: 		}
170: 		if deletedAt > 0 {
171: 			rec.DeletedAt = &deletedAt
172: 		}
173: 		if name.Valid {
174: 			rec.TextItems["name"] = name.String
175: 		}
176: 		if benchmarkProjection {
177: 			if symbol.Valid {
178: 				rec.TextItems["symbol"] = symbol.String
179: 			}
180: 			if exchange.Valid {
181: 				rec.TextItems["exchange"] = exchange.String
182: 			}
183: 			if region.Valid {
184: 				rec.TextItems["region"] = region.String
185: 			}
186: 		}
187: 		if version.Valid {
188: 			rec.Float64Items["version"] = float64(version.Int64)
189: 		}
190: 		if benchmarkProjection && (tradeType.Valid || tradeTime.Valid) {
191: 			rec.Int64Items = make(map[string]int64)
192: 			if tradeType.Valid {
193: 				rec.Int64Items["tradeType"] = tradeType.Int64
194: 			}
195: 			if tradeTime.Valid {
196: 				rec.Int64Items["tradeTime"] = tradeTime.Int64
197: 			}
198: 		}
199: 
200: 		records = append(records, rec)
201: 	}
202: 	return records, nil
203: }
204: 
205: // buildExecutionPlan creates an execution plan with tier and timing info.
206: func buildExecutionPlan(dirtyIDCount int, hasBase, hasDelta bool, duration time.Duration) *internal.ExecutionPlan {
207: 	planNotes := []string{fmt.Sprintf("dirty_ids_excluded:%d", dirtyIDCount)}
208: 	if hasBase {
209: 		planNotes = append(planNotes, "base_files_scanned")
210: 	}
211: 	if hasDelta {
212: 		planNotes = append(planNotes, "delta_files_scanned")
213: 	}
214: 	planNotes = append(planNotes, "hot_buffer_scanned")
215: 
216: 	return &internal.ExecutionPlan{
217: 		Notes: planNotes,
218: 		Timings: map[string]int64{
219: 			"total": duration.Milliseconds(),
220: 		},
221: 	}
222: }
223: 
224: func shouldSkipFederatedSelect(totalRecords int64, offset int) bool {
225: 	if totalRecords <= 0 {
226: 		return true
227: 	}
228: 	if offset < 0 {
229: 		return false
230: 	}
231: 	return int64(offset) >= totalRecords
232: }
233: 
234: func isFederatedTierFileError(err error) bool {
235: 	if err == nil {
236: 		return false
237: 	}
238: 	return strings.Contains(err.Error(), "No files found") || strings.Contains(err.Error(), "does not exist")
239: }
240: 
241: // buildFederatedQuerySQLDynamic builds the federated query SQL, only including tiers that have files.
242: func (h *FederatedTestHarness) buildFederatedQuerySQLDynamic(basePath, deltaPath string, hasBase, hasDelta bool, dirtyIDs []uuid.UUID, opts *QueryOptions) string {
243: 	benchmarkProjection := usesBenchmarkProjectionForSelect(opts)
244: 	combinedQuery := h.buildFederatedCombinedQuery(basePath, deltaPath, hasBase, hasDelta, dirtyIDs, opts, benchmarkProjection, usesTradeTimeOnlyBenchmarkProjectionForSelect(opts))
245: 	return buildFinalFederatedSelect(combinedQuery, opts, benchmarkProjection)
246: }
247: 
248: func (h *FederatedTestHarness) buildFederatedQueryCountSQLDynamic(basePath, deltaPath string, hasBase, hasDelta bool, dirtyIDs []uuid.UUID, opts *QueryOptions) string {
249: 	combinedQuery := h.buildFederatedCombinedQuery(basePath, deltaPath, hasBase, hasDelta, dirtyIDs, opts, usesBenchmarkProjectionForCount(opts), false)
250: 	return buildFinalFederatedCount(combinedQuery)
251: }
252: 
253: func (h *FederatedTestHarness) buildFederatedCombinedQuery(basePath, deltaPath string, hasBase, hasDelta bool, dirtyIDs []uuid.UUID, opts *QueryOptions, benchmarkProjection, tradeTimeOnlyProjection bool) string {
254: 	dirtyExclusion := buildDirtyExclusion(dirtyIDs)
255: 	rowIDFilter := buildRowIDFilter(opts)
256: 	hotRowIDFilter := buildHotRowIDFilter(opts)
257: 	attributeFilter := buildAttributeFilterClause(opts)
258: 	timeWindowFilter := buildTradeTimeFilterClause(opts)
259: 	hotAttributeFilter := buildHotAttributeFilterClauseTargeted(opts)
260: 	hotTimeWindowFilter := buildHotTradeTimeFilterClauseTargeted(opts)
261: 	pgConnStr := h.buildPGConnString()
262: 
263: 	// Build tier queries dynamically
264: 	var tierQueries []string
265: 
266: 	if hasBase {
267: 		baseQuery := buildParquetTierQuery(basePath, h.SchemaID, "base", dirtyExclusion, rowIDFilter, attributeFilter, timeWindowFilter, benchmarkProjection, tradeTimeOnlyProjection)
268: 		tierQueries = append(tierQueries, baseQuery)
269: 	}
270: 
271: 	if hasDelta {
272: 		deltaQuery := buildParquetTierQuery(deltaPath, h.SchemaID, "delta", dirtyExclusion, rowIDFilter, attributeFilter, timeWindowFilter, benchmarkProjection, tradeTimeOnlyProjection)
273: 		tierQueries = append(tierQueries, deltaQuery)
274: 	}
275: 
276: 	// Always include hot buffer (Postgres)
277: 	hotQuery := buildHotTierQuery(pgConnStr, h.SchemaID, hotRowIDFilter, hotAttributeFilter, hotTimeWindowFilter, benchmarkProjection, tradeTimeOnlyProjection)
278: 	tierQueries = append(tierQueries, hotQuery)
279: 
280: 	// Combine all tier queries with UNION ALL
281: 	combinedQuery := strings.Join(tierQueries, "\n\t\t\tUNION ALL\n")
282: 	return combinedQuery
283: }
284: 
285: func buildParquetTierQuery(path string, schemaID int16, tier, dirtyExclusion, rowIDFilter, attributeFilter, timeWindowFilter string, benchmarkProjection, tradeTimeOnlyProjection bool) string {
286: 	if benchmarkProjection {
287: 		projection := benchmarkParquetProjection(schemaID, tier, path, tradeTimeOnlyProjection)
288: 		return fmt.Sprintf(`
289: 			%s
290: 			WHERE 1 = 1 %s %s %s %s`, projection, dirtyExclusion, rowIDFilter, attributeFilter, timeWindowFilter)
291: 	}
292: 	return fmt.Sprintf(`
293: 			SELECT row_id, schema_id, changed_at, deleted_at, name, version, '%s' as tier
294: 			FROM read_parquet('%s')
295: 			WHERE 1 = 1 %s %s %s`, tier, path, dirtyExclusion, rowIDFilter, timeWindowFilter)
296: }
297: 
298: func benchmarkParquetProjection(schemaID int16, tier, path string, tradeTimeOnlyProjection bool) string {
299: 	switch schemaID {
300: 	case benchmarkSchemaIDCustomer:
301: 		return fmt.Sprintf(`SELECT row_id, schema_id, changed_at, deleted_at, name, version, '' as symbol, '' as exchange, region, 0 as tradeType, 0 as tradeTime, '%s' as tier FROM read_parquet('%s')`, tier, path)
302: 	case benchmarkSchemaIDSecurity:
303: 		return fmt.Sprintf(`SELECT row_id, schema_id, changed_at, deleted_at, name, version, symbol, '' as exchange, '' as region, 0 as tradeType, 0 as tradeTime, '%s' as tier FROM read_parquet('%s')`, tier, path)
304: 	default:
305: 		if tradeTimeOnlyProjection {
306: 			return fmt.Sprintf(`SELECT row_id, schema_id, changed_at, deleted_at, '' as name, version, '' as symbol, '' as exchange, '' as region, 0 as tradeType, epoch_ms(tradeTime) as tradeTime, '%s' as tier FROM read_parquet('%s')`, tier, path)
307: 		}
308: 		return fmt.Sprintf(`SELECT row_id, schema_id, changed_at, deleted_at, name, version, symbol, exchange, region, tradeType, epoch_ms(tradeTime) as tradeTime, '%s' as tier FROM read_parquet('%s')`, tier, path)
309: 	}
310: }
311: 
312: func buildHotTierQuery(pgConnStr string, schemaID int16, rowIDFilter, attributeFilter, timeWindowFilter string, benchmarkProjection, tradeTimeOnlyProjection bool) string {
313: 	if benchmarkProjection {
314: 		if tradeTimeOnlyProjection && schemaID == benchmarkSchemaIDTrade {
315: 			return buildHotTradeTimeOnlyQuery(pgConnStr, schemaID, rowIDFilter)
316: 		}
317: 		return buildHotTierQueryTargeted(pgConnStr, schemaID, rowIDFilter, attributeFilter, timeWindowFilter)
318: 	}
319: 	return fmt.Sprintf(`
320: 		SELECT 
321: 			cl.row_id::VARCHAR as row_id,
322: 			cl.schema_id,
323: 			cl.changed_at,
324: 			cl.deleted_at,
325: 			'' as name,
326: 			0 as version,
327: 			'hot' as tier
328: 		FROM postgres_scan('%s', 'public', 'change_log') cl
329: 		WHERE cl.flushed_at = 0 
330: 			AND cl.schema_id = %d
331: 			%s
332: 			%s`, pgConnStr, schemaID, rowIDFilter, timeWindowFilter)
333: }
334: 
335: func buildHotTradeTimeOnlyQuery(pgConnStr string, schemaID int16, rowIDFilter string) string {
336: 	tradeTimeAttrID := benchmarkAttributeID(schemaID, "tradeTime")
337: 	return fmt.Sprintf(`
338: 		SELECT 
339: 			cl.row_id::VARCHAR as row_id,
340: 			cl.schema_id,
341: 			cl.changed_at,
342: 			cl.deleted_at,
343: 			'' as name,
344: 			0 as version,
345: 			'' as symbol,
346: 			'' as exchange,
347: 			'' as region,
348: 			0 as tradeType,
349: 			COALESCE(hot_vals.trade_time, em.bigint_02, 0) as tradeTime,
350: 			'hot' as tier
351: 		FROM postgres_scan('%s', 'public', 'change_log') cl
352: 		LEFT JOIN postgres_scan('%s', 'public', 'entity_main') em
353: 			ON em.ltbase_schema_id = cl.schema_id AND em.ltbase_row_id::VARCHAR = cl.row_id::VARCHAR
354: 		LEFT JOIN (
355: 			SELECT row_id::VARCHAR as row_id, schema_id,
356: 				MAX(CASE WHEN attr_id = %d THEN value_numeric::BIGINT END) AS trade_time
357: 			FROM postgres_scan('%s', 'public', 'eav_data')
358: 			WHERE attr_id = %d
359: 			GROUP BY schema_id, row_id
360: 		) hot_vals ON hot_vals.schema_id = cl.schema_id AND hot_vals.row_id = cl.row_id::VARCHAR
361: 		WHERE cl.flushed_at = 0 
362: 			AND cl.schema_id = %d
363: 			%s`, pgConnStr, pgConnStr, tradeTimeAttrID, pgConnStr, tradeTimeAttrID, schemaID, rowIDFilter)
364: }
365: 
366: func buildFinalFederatedSelect(combinedQuery string, opts *QueryOptions, benchmarkProjection bool) string {
367: 	cte := buildFederatedDeduplicatedCTE(combinedQuery)
368: 	if benchmarkProjection {
369: 		return fmt.Sprintf(`
370: 		%s
371: 		SELECT row_id, schema_id, changed_at, deleted_at, name, version, symbol, exchange, region, tradeType, tradeTime
372: 		FROM deduplicated
373: 		WHERE rn = 1 AND (deleted_at = 0 OR deleted_at IS NULL)
374: 		ORDER BY %s
375: 		LIMIT %d OFFSET %d
376: 	`, cte, buildOrderByClause(opts), opts.Limit, opts.Offset)
377: 	}
378: 	return fmt.Sprintf(`
379: 		%s
380: 		SELECT row_id, schema_id, changed_at, deleted_at, name, version
381: 		FROM deduplicated
382: 		WHERE rn = 1 AND (deleted_at = 0 OR deleted_at IS NULL)
383: 		ORDER BY row_id
384: 		LIMIT %d OFFSET %d
385: 	`, cte, opts.Limit, opts.Offset)
386: }
387: 
388: func buildFinalFederatedCount(combinedQuery string) string {
389: 	return fmt.Sprintf(`
390: 		%s
391: 		SELECT COUNT(*)
392: 		FROM deduplicated
393: 		WHERE rn = 1 AND (deleted_at = 0 OR deleted_at IS NULL)
394: 	`, buildFederatedDeduplicatedCTE(combinedQuery))
395: }
396: 
397: func buildFederatedDeduplicatedCTE(combinedQuery string) string {
398: 	return fmt.Sprintf(`
399: 		WITH combined AS (
400: 			%s
401: 		),
402: 		deduplicated AS (
403: 			SELECT *, ROW_NUMBER() OVER (
404: 				PARTITION BY row_id
405: 				ORDER BY changed_at DESC,
406: 					CASE tier WHEN 'hot' THEN 3 WHEN 'delta' THEN 2 WHEN 'base' THEN 1 ELSE 0 END DESC,
407: 					version DESC,
408: 					deleted_at DESC,
409: 					row_id ASC
410: 			) as rn
411: 			FROM combined
412: 		)
413: 	`, combinedQuery)
414: }
415: 
416: func usesBenchmarkProjectionForSelect(opts *QueryOptions) bool {
417: 	if opts == nil {
418: 		return false
419: 	}
420: 	if requiresBenchmarkProjectedFilters(opts) {
421: 		return true
422: 	}
423: 	if opts.SortBy != "" && opts.SortBy != "row_id" {
424: 		return true
425: 	}
426: 	return false
427: }
428: 
429: func usesBenchmarkProjectionForCount(opts *QueryOptions) bool {
430: 	if requiresBenchmarkProjectedFilters(opts) {
431: 		return true
432: 	}
433: 	if opts == nil {
434: 		return false
435: 	}
436: 	if opts.Offset <= 0 {
437: 		return usesBenchmarkProjectionForSelect(opts)
438: 	}
439: 	return false
440: }
441: 
442: func usesTradeTimeOnlyBenchmarkProjectionForSelect(opts *QueryOptions) bool {
443: 	if !usesBenchmarkProjectionForSelect(opts) || opts == nil {
444: 		return false
445: 	}
446: 	if opts.SortBy != "tradeTime" || opts.Filter != nil {
447: 		return false
448: 	}
449: 	return opts.TradeTimeStart == 0 && opts.TradeTimeEnd == 0
450: }
451: 
452: type hotTierEAVMapping struct {
453: 	attrIDList   string
454: 	pivotColumns string
455: 	selectExprs  string
456: 	nameExpr     string
457: }
458: 
459: func hotTierEAVMappingForSchema(schemaID int16) hotTierEAVMapping {
460: 	switch schemaID {
461: 	case benchmarkSchemaIDTrade:
462: 		symbolID := benchmarkAttributeID(schemaID, "symbol")
463: 		exchangeID := benchmarkAttributeID(schemaID, "exchange")
464: 		regionID := benchmarkAttributeID(schemaID, "region")
465: 		tradeTypeID := benchmarkAttributeID(schemaID, "tradeType")
466: 		tradeTimeID := benchmarkAttributeID(schemaID, "tradeTime")
467: 		nameID := benchmarkAttributeID(schemaID, "name")
468: 		return hotTierEAVMapping{
469: 			attrIDList: fmt.Sprintf("%d, %d, %d, %d, %d, %d", symbolID, exchangeID, regionID, tradeTypeID, tradeTimeID, nameID),
470: 			pivotColumns: fmt.Sprintf(
471: 				"MAX(CASE WHEN attr_id = %d THEN value_text END) AS symbol,\n\t\t\t"+
472: 					"MAX(CASE WHEN attr_id = %d THEN value_text END) AS exchange,\n\t\t\t"+
473: 					"MAX(CASE WHEN attr_id = %d THEN value_text END) AS region,\n\t\t\t"+
474: 					"MAX(CASE WHEN attr_id = %d THEN value_numeric::BIGINT END) AS tradeType,\n\t\t\t"+
475: 					"MAX(CASE WHEN attr_id = %d THEN value_numeric::BIGINT END) AS tradeTime,\n\t\t\t"+
476: 					"MAX(CASE WHEN attr_id = %d THEN value_text END) AS name",
477: 				symbolID, exchangeID, regionID, tradeTypeID, tradeTimeID, nameID),
478: 			selectExprs: "COALESCE(hot_vals.symbol, em.text_01) as symbol,\n\t\t\t" +
479: 				"COALESCE(hot_vals.exchange, '') as exchange,\n\t\t\t" +
480: 				"COALESCE(hot_vals.region, em.text_02) as region,\n\t\t\t" +
481: 				"COALESCE(hot_vals.tradeType, em.smallint_01) as tradeType,\n\t\t\t" +
482: 				"COALESCE(hot_vals.tradeTime, em.bigint_02) as tradeTime",
483: 			nameExpr: "COALESCE(hot_vals.name, hot_vals.symbol, '')",
484: 		}
485: 	case benchmarkSchemaIDCustomer:
486: 		regionID := benchmarkAttributeID(schemaID, "region")
487: 		nameID := benchmarkAttributeID(schemaID, "name")
488: 		return hotTierEAVMapping{
489: 			attrIDList: fmt.Sprintf("%d, %d", regionID, nameID),
490: 			pivotColumns: fmt.Sprintf(
491: 				"MAX(CASE WHEN attr_id = %d THEN value_text END) AS region,\n\t\t\t"+
492: 					"MAX(CASE WHEN attr_id = %d THEN value_text END) AS name",
493: 				regionID, nameID),
494: 			selectExprs: "'' as symbol,\n\t\t\t" +
495: 				"'' as exchange,\n\t\t\t" +
496: 				"COALESCE(hot_vals.region, em.text_02) as region,\n\t\t\t" +
497: 				"0 as tradeType,\n\t\t\t" +
498: 				"0 as tradeTime",
499: 			nameExpr: "COALESCE(hot_vals.name, '')",
500: 		}
501: 	case benchmarkSchemaIDSecurity:
502: 		symbolID := benchmarkAttributeID(schemaID, "symbol")
503: 		nameID := benchmarkAttributeID(schemaID, "companyName")
504: 		return hotTierEAVMapping{
505: 			attrIDList: fmt.Sprintf("%d, %d", symbolID, nameID),
506: 			pivotColumns: fmt.Sprintf(
507: 				"MAX(CASE WHEN attr_id = %d THEN value_text END) AS symbol,\n\t\t\t"+
508: 					"MAX(CASE WHEN attr_id = %d THEN value_text END) AS name",
509: 				symbolID, nameID),
510: 			selectExprs: "COALESCE(hot_vals.symbol, em.text_01) as symbol,\n\t\t\t" +
511: 				"'' as exchange,\n\t\t\t" +
512: 				"'' as region,\n\t\t\t" +
513: 				"0 as tradeType,\n\t\t\t" +
514: 				"0 as tradeTime",
515: 			nameExpr: "COALESCE(hot_vals.name, hot_vals.symbol, '')",
516: 		}
517: 	default:
518: 		return hotTierEAVMapping{}
519: 	}
520: }
521: 
522: func buildHotTierQueryTargeted(pgConnStr string, schemaID int16, rowIDFilter, attributeFilter, timeWindowFilter string) string {
523: 	m := hotTierEAVMappingForSchema(schemaID)
524: 	return fmt.Sprintf(`
525: 		SELECT 
526: 			cl.row_id::VARCHAR as row_id,
527: 			cl.schema_id,
528: 			cl.changed_at,
529: 			cl.deleted_at,
530: 			%s as name,
531: 			0 as version,
532: 			%s,
533: 			'hot' as tier
534: 		FROM postgres_scan('%s', 'public', 'change_log') cl
535: 		LEFT JOIN postgres_scan('%s', 'public', 'entity_main') em
536: 			ON em.ltbase_schema_id = cl.schema_id AND em.ltbase_row_id::VARCHAR = cl.row_id::VARCHAR
537: 		LEFT JOIN (
538: 			SELECT row_id::VARCHAR as row_id, schema_id,
539: 				%s
540: 			FROM postgres_scan('%s', 'public', 'eav_data')
541: 			WHERE attr_id IN (%s)
542: 			GROUP BY schema_id, row_id
543: 		) hot_vals ON hot_vals.schema_id = cl.schema_id AND hot_vals.row_id = cl.row_id::VARCHAR
544: 		WHERE cl.flushed_at = 0 
545: 			AND cl.schema_id = %d
546: 			%s
547: 			%s
548: 			%s`,
549: 		m.nameExpr, m.selectExprs,
550: 		pgConnStr, pgConnStr,
551: 		m.pivotColumns, pgConnStr, m.attrIDList,
552: 		schemaID, rowIDFilter, attributeFilter, timeWindowFilter)
553: }
554: 
555: func needsBenchmarkDuckDBMacros(opts *QueryOptions, benchmarkProjection, tradeTimeOnlyProjection bool) bool {
556: 	return false
557: }
558: 
559: func requiresBenchmarkProjectedFilters(opts *QueryOptions) bool {
560: 	if opts == nil {
561: 		return false
562: 	}
563: 	if opts.TradeTimeStart > 0 || opts.TradeTimeEnd > 0 {
564: 		return true
565: 	}
566: 	if opts.Filter == nil {
567: 		return false
568: 	}
569: 	for key := range opts.Filter.Conditions {
570: 		if benchmarkQueryColumn(key) != "" {
571: 			return true
572: 		}
573: 	}
574: 	return false
575: }
576: 
577: // buildDirtyExclusion builds the dirty ID exclusion clause.
578: func buildDirtyExclusion(dirtyIDs []uuid.UUID) string {
579: 	if len(dirtyIDs) == 0 {
580: 		return ""
581: 	}
582: 	ids := make([]string, len(dirtyIDs))
583: 	for i, id := range dirtyIDs {
584: 		ids[i] = fmt.Sprintf("'%s'", id.String())
585: 	}
586: 	return fmt.Sprintf("AND row_id NOT IN (%s)", strings.Join(ids, ","))
587: }
588: 
589: // buildRowIDFilter builds the row ID filter clause.
590: func buildRowIDFilter(opts *QueryOptions) string {
591: 	if opts.Filter != nil && opts.Filter.RowID != uuid.Nil {
592: 		return fmt.Sprintf("AND row_id = '%s'", opts.Filter.RowID.String())
593: 	}
594: 	return ""
595: }
596: 
597: func buildHotRowIDFilter(opts *QueryOptions) string {
598: 	if opts.Filter != nil && opts.Filter.RowID != uuid.Nil {
599: 		return fmt.Sprintf("AND cl.row_id = '%s'", opts.Filter.RowID.String())
600: 	}
601: 	return ""
602: }
603: 
604: func buildAttributeFilterClause(opts *QueryOptions) string {
605: 	if opts == nil || opts.Filter == nil || len(opts.Filter.Conditions) == 0 {
606: 		return ""
607: 	}
608: 	parts := make([]string, 0, len(opts.Filter.Conditions))
609: 	for key, value := range opts.Filter.Conditions {
610: 		column := benchmarkQueryColumn(key)
611: 		if column == "" {
612: 			continue
613: 		}
614: 		parts = append(parts, fmt.Sprintf("AND %s = %s", column, benchmarkSQLLiteral(value)))
615: 	}
616: 	return strings.Join(parts, " ")
617: }
618: 
619: func buildTradeTimeFilterClause(opts *QueryOptions) string {
620: 	if opts == nil {
621: 		return ""
622: 	}
623: 	parts := make([]string, 0, 2)
624: 	expression := parquetTradeTimeFilterExpression()
625: 	if opts.TradeTimeStart > 0 {
626: 		parts = append(parts, fmt.Sprintf("AND %s >= %d", expression, opts.TradeTimeStart))
627: 	}
628: 	if opts.TradeTimeEnd > 0 {
629: 		parts = append(parts, fmt.Sprintf("AND %s <= %d", expression, opts.TradeTimeEnd))
630: 	}
631: 	return strings.Join(parts, " ")
632: }
633: 
634: func parquetTradeTimeFilterExpression() string {
635: 	return "epoch_ms(tradeTime)"
636: }
637: 
638: func buildHotTradeTimeFilterClauseTargeted(opts *QueryOptions) string {
639: 	if opts == nil {
640: 		return ""
641: 	}
642: 	parts := make([]string, 0, 2)
643: 	expression := targetedHotFilterExpression("tradeTime")
644: 	if opts.TradeTimeStart > 0 {
645: 		parts = append(parts, fmt.Sprintf("AND %s >= %d", expression, opts.TradeTimeStart))
646: 	}
647: 	if opts.TradeTimeEnd > 0 {
648: 		parts = append(parts, fmt.Sprintf("AND %s <= %d", expression, opts.TradeTimeEnd))
649: 	}
650: 	return strings.Join(parts, " ")
651: }
652: 
653: func benchmarkQueryColumn(attribute string) string {
654: 	switch attribute {
655: 	case "symbol":
656: 		return "symbol"
657: 	case "exchange":
658: 		return "exchange"
659: 	case "region":
660: 		return "region"
661: 	case "tradeType":
662: 		return "tradeType"
663: 	case "tradeTime":
664: 		return "tradeTime"
665: 	default:
666: 		return ""
667: 	}
668: }
669: 
670: func benchmarkSQLLiteral(value any) string {
671: 	switch v := value.(type) {
672: 	case string:
673: 		return fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
674: 	case int:
675: 		return fmt.Sprintf("%d", v)
676: 	case int64:
677: 		return fmt.Sprintf("%d", v)
678: 	case float64:
679: 		return fmt.Sprintf("%v", v)
680: 	default:
681: 		return fmt.Sprintf("'%v'", v)
682: 	}
683: }
684: 
685: func benchmarkAttrNameSQLCase() string {
686: 	type attrKey struct {
687: 		schemaID int16
688: 		name     string
689: 	}
690: 	mapping := map[attrKey]int{
691: 		{benchmarkSchemaIDTrade, "symbol"}:    benchmarkAttributeID(benchmarkSchemaIDTrade, "symbol"),
692: 		{benchmarkSchemaIDTrade, "exchange"}:  benchmarkAttributeID(benchmarkSchemaIDTrade, "exchange"),
693: 		{benchmarkSchemaIDTrade, "region"}:    benchmarkAttributeID(benchmarkSchemaIDTrade, "region"),
694: 		{benchmarkSchemaIDTrade, "tradeType"}: benchmarkAttributeID(benchmarkSchemaIDTrade, "tradeType"),
695: 		{benchmarkSchemaIDTrade, "tradeTime"}: benchmarkAttributeID(benchmarkSchemaIDTrade, "tradeTime"),
696: 		{benchmarkSchemaIDTrade, "name"}:      benchmarkAttributeID(benchmarkSchemaIDTrade, "name"),
697: 		{benchmarkSchemaIDCustomer, "region"}: benchmarkAttributeID(benchmarkSchemaIDCustomer, "region"),
698: 		{benchmarkSchemaIDCustomer, "name"}:   benchmarkAttributeID(benchmarkSchemaIDCustomer, "name"),
699: 		{benchmarkSchemaIDSecurity, "symbol"}: benchmarkAttributeID(benchmarkSchemaIDSecurity, "symbol"),
700: 		{benchmarkSchemaIDSecurity, "name"}:   benchmarkAttributeID(benchmarkSchemaIDSecurity, "companyName"),
701: 	}
702: 	parts := make([]string, 0, len(mapping))
703: 	for key, attrID := range mapping {
704: 		parts = append(parts, fmt.Sprintf("WHEN schema_id = %d AND attr_id = %d THEN '%s'", key.schemaID, attrID, key.name))
705: 	}
706: 	return strings.Join(parts, " ")
707: }
708: 
709: func benchmarkAttributeID(schemaID int16, name string) int {
710: 	hash := uint32(2166136261)
711: 	input := fmt.Sprintf("%d:%s", schemaID, name)
712: 	for i := 0; i < len(input); i++ {
713: 		hash ^= uint32(input[i])
714: 		hash *= 16777619
715: 	}
716: 	return int(hash%30000) + 1
717: }
718: 
719: func benchmarkFunctionsSQL() string {
720: 	return fmt.Sprintf(`
721: 		CREATE OR REPLACE MACRO benchmark_attr_name(schema_id, attr_id) AS (
722: 			CASE %s ELSE '' END
723: 		);
724: 		CREATE OR REPLACE MACRO benchmark_text(attr_map, attr_name, fallback_value) AS (
725: 			COALESCE(CAST(element_at(attr_map, attr_name) AS VARCHAR), fallback_value)
726: 		);
727: 		CREATE OR REPLACE MACRO benchmark_name(attr_map) AS (
728: 			COALESCE(CAST(element_at(attr_map, 'name') AS VARCHAR), CAST(element_at(attr_map, 'symbol') AS VARCHAR), '')
729: 		);
730: 		CREATE OR REPLACE MACRO benchmark_int(attr_map, attr_name, fallback_value) AS (
731: 			COALESCE(TRY_CAST(element_at(attr_map, attr_name) AS INTEGER), fallback_value)
732: 		);
733: 		CREATE OR REPLACE MACRO benchmark_bigint(attr_map, attr_name, fallback_value) AS (
734: 			COALESCE(TRY_CAST(element_at(attr_map, attr_name) AS BIGINT), fallback_value)
735: 		);
736: 	`, benchmarkAttrNameSQLCase())
737: }
738: 
739: func prepareBenchmarkDuckDBMacros(ctx context.Context, h *FederatedTestHarness) error {
740: 	_, err := h.Duck.DB.ExecContext(ctx, benchmarkFunctionsSQL())
741: 	if err != nil {
742: 		return fmt.Errorf("prepare benchmark duckdb macros: %w", err)
743: 	}
744: 	return nil
745: }
746: 
747: func buildHotAttributeFilterClauseTargeted(opts *QueryOptions) string {
748: 	if opts == nil || opts.Filter == nil || len(opts.Filter.Conditions) == 0 {
749: 		return ""
750: 	}
751: 	parts := make([]string, 0, len(opts.Filter.Conditions))
752: 	for key, value := range opts.Filter.Conditions {
753: 		expression := targetedHotFilterExpression(key)
754: 		if expression == "" {
755: 			continue
756: 		}
757: 		parts = append(parts, fmt.Sprintf("AND %s = %s", expression, benchmarkSQLLiteral(value)))
758: 	}
759: 	return strings.Join(parts, " ")
760: }
761: 
762: func targetedHotFilterExpression(attribute string) string {
763: 	switch attribute {
764: 	case "symbol":
765: 		return "COALESCE(hot_vals.symbol, em.text_01)"
766: 	case "exchange":
767: 		return "COALESCE(hot_vals.exchange, '')"
768: 	case "region":
769: 		return "COALESCE(hot_vals.region, em.text_02)"
770: 	case "tradeType":
771: 		return "COALESCE(hot_vals.tradeType, em.smallint_01)"
772: 	case "tradeTime":
773: 		return "COALESCE(hot_vals.tradeTime, em.bigint_02)"
774: 	default:
775: 		return ""
776: 	}
777: }
778: 
779: func buildOrderByClause(opts *QueryOptions) string {
780: 	prefix := ""
781: 	if opts == nil {
782: 		return prefix + "row_id ASC"
783: 	}
784: 	column := benchmarkQueryColumn(opts.SortBy)
785: 	if column == "" {
786: 		column = "row_id"
787: 	}
788: 	direction := "ASC"
789: 	if opts.SortDesc {
790: 		direction = "DESC"
791: 	}
792: 	return fmt.Sprintf("%s%s %s, %srow_id ASC", prefix, column, direction, prefix)
793: }
794: 
795: // buildPGConnString builds the Postgres connection string for DuckDB.
796: func (h *FederatedTestHarness) buildPGConnString() string {
797: 	host := h.PGHost
798: 	if host == "" {
799: 		host = "localhost"
800: 	}
801: 	port := h.PGPort
802: 	if port == "" {
803: 		port = "5432"
804: 	}
805: 	user := h.PGUser
806: 	if user == "" {
807: 		user = "postgres"
808: 	}
809: 	password := h.PGPassword
810: 	if password == "" {
811: 		password = "password"
812: 	}
813: 	database := h.PGDatabase
814: 	if database == "" {
815: 		database = "postgres"
816: 	}
817: 	sslMode := h.PGSSLMode
818: 	if sslMode == "" {
819: 		sslMode = "disable"
820: 	}
821: 	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
822: 		host, port, user, password, database, sslMode)
823: }
824: 
825: // getDirtyIDs fetches unflushed row IDs from change_log.
826: func (h *FederatedTestHarness) getDirtyIDs(ctx context.Context) ([]uuid.UUID, error) {
827: 	rows, err := h.PGDB.QueryContext(ctx, `
828: 		SELECT row_id FROM change_log 
829: 		WHERE schema_id = $1 AND flushed_at = 0
830: 	`, h.SchemaID)
831: 	if err != nil {
832: 		return nil, err
833: 	}
834: 	defer rows.Close()
835: 
836: 	var ids []uuid.UUID
837: 	for rows.Next() {
838: 		var id uuid.UUID
839: 		if err := rows.Scan(&id); err != nil {
840: 			return nil, err
841: 		}
842: 		ids = append(ids, id)
843: 	}
844: 	return ids, nil
845: }
846: 
847: // ExecutePostgresQuery executes a direct Postgres query (no DuckDB).
848: func (h *FederatedTestHarness) ExecutePostgresQuery(ctx context.Context, opts *QueryOptions) (*QueryResult, error) {
849: 	opts = normalizeQueryOptions(opts)
850: 	start := time.Now()
851: 	countQuery, countArgs := h.buildPostgresOnlyCountQuery(opts)
852: 	var total int64
853: 	if err := h.PGDB.QueryRowContext(ctx, countQuery, countArgs...).Scan(&total); err != nil {
854: 		return nil, err
855: 	}
856: 	if opts.CountOnly {
857: 		return &QueryResult{TotalRecords: total, Duration: time.Since(start), Plan: buildPostgresOnlyExecutionPlan(time.Since(start), opts.PreferHot)}, nil
858: 	}
859: 
860: 	query, args := h.buildPostgresOnlySelectQuery(opts)
861: 	rows, err := h.PGDB.QueryContext(ctx, query, args...)
862: 	if err != nil {
863: 		return nil, err
864: 	}
865: 	defer rows.Close()
866: 
867: 	benchmarkProjection := usesBenchmarkProjectionForSelect(opts)
868: 	var records []*internal.PersistentRecord
869: 	for rows.Next() {
870: 		var rowID string
871: 		var schemaID int16
872: 		var changedAt, deletedAt int64
873: 		var name sql.NullString
874: 		var version sql.NullInt64
875: 		var symbol, exchange, region sql.NullString
876: 		var tradeType, tradeTime sql.NullInt64
877: 		if benchmarkProjection {
878: 			if err := rows.Scan(&rowID, &schemaID, &changedAt, &deletedAt, &name, &version, &symbol, &exchange, &region, &tradeType, &tradeTime); err != nil {
879: 				return nil, err
880: 			}
881: 		} else {
882: 			if err := rows.Scan(&rowID, &schemaID, &changedAt, &deletedAt, &name, &version); err != nil {
883: 				return nil, err
884: 			}
885: 		}
886: 		rec := &internal.PersistentRecord{RowID: uuid.MustParse(rowID), SchemaID: schemaID, CreatedAt: changedAt, UpdatedAt: changedAt, TextItems: map[string]string{}, Float64Items: map[string]float64{}}
887: 		if deletedAt > 0 {
888: 			rec.DeletedAt = &deletedAt
889: 		}
890: 		if name.Valid {
891: 			rec.TextItems["name"] = name.String
892: 		}
893: 		if benchmarkProjection {
894: 			if symbol.Valid {
895: 				rec.TextItems["symbol"] = symbol.String
896: 			}
897: 			if exchange.Valid {
898: 				rec.TextItems["exchange"] = exchange.String
899: 			}
900: 			if region.Valid {
901: 				rec.TextItems["region"] = region.String
902: 			}
903: 		}
904: 		if version.Valid {
905: 			rec.Float64Items["version"] = float64(version.Int64)
906: 		}
907: 		if benchmarkProjection && (tradeType.Valid || tradeTime.Valid) {
908: 			rec.Int64Items = make(map[string]int64)
909: 			if tradeType.Valid {
910: 				rec.Int64Items["tradeType"] = tradeType.Int64
911: 			}
912: 			if tradeTime.Valid {
913: 				rec.Int64Items["tradeTime"] = tradeTime.Int64
914: 			}
915: 		}
916: 		records = append(records, rec)
917: 	}
918: 
919: 	return &QueryResult{
920: 		Records:      records,
921: 		TotalRecords: total,
922: 		Duration:     time.Since(start),
923: 		Plan:         buildPostgresOnlyExecutionPlan(time.Since(start), opts.PreferHot),
924: 	}, nil
925: }
926: 
927: func (h *FederatedTestHarness) buildPostgresOnlySelectQuery(opts *QueryOptions) (string, []any) {
928: 	args := []any{h.SchemaID}
929: 	attrIDs := benchmarkPostgresAttributeIDs()
930: 	query := strings.Builder{}
931: 	query.WriteString(`
932: 		SELECT
933: 			cl.row_id::VARCHAR,
934: 			cl.schema_id,
935: 			cl.changed_at,
936: 			COALESCE(cl.deleted_at, 0),
937: 			COALESCE(hot_vals.name, hot_vals.symbol, '') as name,
938: 			0 as version`)
939: 	if usesBenchmarkProjectionForSelect(opts) {
940: 		query.WriteString(`,
941: 			COALESCE(hot_vals.symbol, em.text_01, '') as symbol,
942: 			COALESCE(hot_vals.exchange, '') as exchange,
943: 			COALESCE(hot_vals.region, em.text_02, '') as region,
944: 			COALESCE(hot_vals.trade_type, em.smallint_01::BIGINT, 0) as tradeType,
945: 			COALESCE(hot_vals.trade_time, em.bigint_02, 0) as tradeTime`)
946: 	}
947: 	query.WriteString(fmt.Sprintf(`
948: 		FROM change_log cl
949: 		LEFT JOIN entity_main em
950: 			ON em.ltbase_schema_id = cl.schema_id AND em.ltbase_row_id = cl.row_id
951: 		LEFT JOIN (
952: 			SELECT schema_id, row_id,
953: 				MAX(CASE WHEN attr_id = %d THEN value_text END) AS symbol,
954: 				MAX(CASE WHEN attr_id = %d THEN value_text END) AS exchange,
955: 				MAX(CASE WHEN attr_id = %d THEN value_text END) AS region,
956: 				MAX(CASE WHEN attr_id = %d THEN value_numeric::BIGINT END) AS trade_type,
957: 				MAX(CASE WHEN attr_id = %d THEN value_numeric::BIGINT END) AS trade_time,
958: 				MAX(CASE WHEN attr_id = %d THEN value_text END) AS name
959: 			FROM eav_data
960: 			WHERE attr_id IN (%d, %d, %d, %d, %d, %d)
961: 			GROUP BY schema_id, row_id
962: 		) hot_vals ON hot_vals.schema_id = cl.schema_id AND hot_vals.row_id = cl.row_id
963: 		WHERE cl.schema_id = $1 AND cl.flushed_at = 0 AND (cl.deleted_at IS NULL OR cl.deleted_at = 0)`,
964: 		attrIDs.symbol, attrIDs.exchange, attrIDs.region, attrIDs.tradeType, attrIDs.tradeTime, attrIDs.name,
965: 		attrIDs.symbol, attrIDs.exchange, attrIDs.region, attrIDs.tradeType, attrIDs.tradeTime, attrIDs.name))
966: 	filterSQL, filterArgs := buildPostgresOnlyFilterClauses(opts, 2)
967: 	query.WriteString(filterSQL)
968: 	args = append(args, filterArgs...)
969: 	query.WriteString(fmt.Sprintf(" ORDER BY %s LIMIT $%d OFFSET $%d", buildPostgresOnlyOrderBy(opts), len(args)+1, len(args)+2))
970: 	args = append(args, opts.Limit, opts.Offset)
971: 	return query.String(), args
972: }
973: 
974: func (h *FederatedTestHarness) buildPostgresOnlyCountQuery(opts *QueryOptions) (string, []any) {
975: 	args := []any{h.SchemaID}
976: 	attrIDs := benchmarkPostgresAttributeIDs()
977: 	query := strings.Builder{}
978: 	query.WriteString(fmt.Sprintf(`
979: 		SELECT COUNT(*)
980: 		FROM change_log cl
981: 		LEFT JOIN entity_main em
982: 			ON em.ltbase_schema_id = cl.schema_id AND em.ltbase_row_id = cl.row_id
983: 		LEFT JOIN (
984: 			SELECT schema_id, row_id,
985: 				MAX(CASE WHEN attr_id = %d THEN value_text END) AS symbol,
986: 				MAX(CASE WHEN attr_id = %d THEN value_text END) AS exchange,
987: 				MAX(CASE WHEN attr_id = %d THEN value_text END) AS region,
988: 				MAX(CASE WHEN attr_id = %d THEN value_numeric::BIGINT END) AS trade_type,
989: 				MAX(CASE WHEN attr_id = %d THEN value_numeric::BIGINT END) AS trade_time,
990: 				MAX(CASE WHEN attr_id = %d THEN value_text END) AS name
991: 			FROM eav_data
992: 			WHERE attr_id IN (%d, %d, %d, %d, %d, %d)
993: 			GROUP BY schema_id, row_id
994: 		) hot_vals ON hot_vals.schema_id = cl.schema_id AND hot_vals.row_id = cl.row_id
995: 		WHERE cl.schema_id = $1 AND cl.flushed_at = 0 AND (cl.deleted_at IS NULL OR cl.deleted_at = 0)`,
996: 		attrIDs.symbol, attrIDs.exchange, attrIDs.region, attrIDs.tradeType, attrIDs.tradeTime, attrIDs.name,
997: 		attrIDs.symbol, attrIDs.exchange, attrIDs.region, attrIDs.tradeType, attrIDs.tradeTime, attrIDs.name))
998: 	filterSQL, filterArgs := buildPostgresOnlyFilterClauses(opts, 2)
999: 	query.WriteString(filterSQL)
1000: 	args = append(args, filterArgs...)
1001: 	return query.String(), args
1002: }
1003: 
1004: func buildPostgresOnlyFilterClauses(opts *QueryOptions, placeholderStart int) (string, []any) {
1005: 	if opts == nil {
1006: 		return "", nil
1007: 	}
1008: 	args := make([]any, 0)
1009: 	parts := make([]string, 0)
1010: 	placeholder := placeholderStart
1011: 	if opts.Filter != nil && opts.Filter.RowID != uuid.Nil {
1012: 		parts = append(parts, fmt.Sprintf("AND cl.row_id = $%d", placeholder))
1013: 		args = append(args, opts.Filter.RowID)
1014: 		placeholder++
1015: 	}
1016: 	if opts.Filter != nil {
1017: 		for key, value := range opts.Filter.Conditions {
1018: 			expression := postgresOnlyFilterExpression(key)
1019: 			if expression == "" {
1020: 				continue
1021: 			}
1022: 			parts = append(parts, fmt.Sprintf("AND %s = $%d", expression, placeholder))
1023: 			args = append(args, value)
1024: 			placeholder++
1025: 		}
1026: 	}
1027: 	if opts.TradeTimeStart > 0 {
1028: 		parts = append(parts, fmt.Sprintf("AND %s >= $%d", postgresOnlyFilterExpression("tradeTime"), placeholder))
1029: 		args = append(args, opts.TradeTimeStart)
1030: 		placeholder++
1031: 	}
1032: 	if opts.TradeTimeEnd > 0 {
1033: 		parts = append(parts, fmt.Sprintf("AND %s <= $%d", postgresOnlyFilterExpression("tradeTime"), placeholder))
1034: 		args = append(args, opts.TradeTimeEnd)
1035: 	}
1036: 	if len(parts) == 0 {
1037: 		return "", nil
1038: 	}
1039: 	return " " + strings.Join(parts, " "), args
1040: }
1041: 
1042: func buildPostgresOnlyOrderBy(opts *QueryOptions) string {
1043: 	if opts == nil || opts.SortBy == "" {
1044: 		return "cl.row_id ASC"
1045: 	}
1046: 	column := postgresOnlyFilterExpression(opts.SortBy)
1047: 	if column == "" {
1048: 		column = "cl.row_id"
1049: 	}
1050: 	direction := "ASC"
1051: 	if opts.SortDesc {
1052: 		direction = "DESC"
1053: 	}
1054: 	return fmt.Sprintf("%s %s, cl.row_id ASC", column, direction)
1055: }
1056: 
1057: type postgresBenchmarkAttributeIDs struct {
1058: 	symbol    int
1059: 	exchange  int
1060: 	region    int
1061: 	tradeType int
1062: 	tradeTime int
1063: 	name      int
1064: }
1065: 
1066: func benchmarkPostgresAttributeIDs() postgresBenchmarkAttributeIDs {
1067: 	return postgresBenchmarkAttributeIDs{
1068: 		symbol:    benchmarkAttributeID(benchmarkSchemaIDTrade, "symbol"),
1069: 		exchange:  benchmarkAttributeID(benchmarkSchemaIDTrade, "exchange"),
1070: 		region:    benchmarkAttributeID(benchmarkSchemaIDTrade, "region"),
1071: 		tradeType: benchmarkAttributeID(benchmarkSchemaIDTrade, "tradeType"),
1072: 		tradeTime: benchmarkAttributeID(benchmarkSchemaIDTrade, "tradeTime"),
1073: 		name:      benchmarkAttributeID(benchmarkSchemaIDTrade, "name"),
1074: 	}
1075: }
1076: 
1077: func postgresOnlyFilterExpression(attribute string) string {
1078: 	switch attribute {
1079: 	case "symbol":
1080: 		return "COALESCE(hot_vals.symbol, em.text_01, '')"
1081: 	case "exchange":
1082: 		return "COALESCE(hot_vals.exchange, '')"
1083: 	case "region":
1084: 		return "COALESCE(hot_vals.region, em.text_02, '')"
1085: 	case "tradeType":
1086: 		return "COALESCE(hot_vals.trade_type, em.smallint_01::BIGINT, 0)"
1087: 	case "tradeTime":
1088: 		return "COALESCE(hot_vals.trade_time, em.bigint_02, 0)"
1089: 	default:
1090: 		return ""
1091: 	}
1092: }
1093: 
1094: func buildPostgresOnlyExecutionPlan(duration time.Duration, preferHot bool) *internal.ExecutionPlan {
1095: 	notes := []string{"hot_buffer_scanned", "postgres_only_execution"}
1096: 	if preferHot {
1097: 		notes = append(notes, "prefer_hot_override")
1098: 	}
1099: 	return &internal.ExecutionPlan{Notes: notes, Timings: map[string]int64{"total": duration.Milliseconds()}}
1100: }
1101: 
1102: // StreamFederatedQuery streams query results with a handler callback.
1103: func (h *FederatedTestHarness) StreamFederatedQuery(ctx context.Context, opts *QueryOptions, handler func(*internal.PersistentRecord) error) error {
1104: 	result, err := h.ExecuteFederatedQuery(ctx, opts)
1105: 	if err != nil {
1106: 		return err
1107: 	}
1108: 
1109: 	for _, rec := range result.Records {
1110: 		if err := handler(rec); err != nil {
1111: 			return err
1112: 		}
1113: 	}
1114: 	return nil
1115: }

(End of file - total 1115 lines)
</content>
```

**Tool: read**

**Input:**
```json
{
  "filePath": "/home/ruoshi/code/github/forma/docs/federated-query/federated-query-benchmark-hld-en.md"
}
```

**Output:**
```
<path>/home/ruoshi/code/github/forma/docs/federated-query/federated-query-benchmark-hld-en.md</path>
<type>file</type>
<content>
1: # Federated Query Benchmark High-Level Design
2: 
3: Last updated: 2026-04-17  
4: Repository: `forma`
5: 
6: ## 1. Background
7: 
8: Forma already has federated querying, cold/hot tiering, CDC flush, compaction, basic pagination, and baseline performance tests. What it does not yet have is a dedicated benchmark that focuses on the hard parts of hybrid querying across cold and hot data.
9: 
10: The current performance suite is useful for regression coverage, but it does not systematically answer the following questions:
11: 
12: - How does the system behave under domain-shaped hybrid query workloads?
13: - How do different data distributions affect routing, predicate pushdown, and merge cost?
14: - How badly does deep pagination degrade as page numbers grow?
15: - What is the correctness and performance cost of overlap, soft deletes, and last-write-wins merging across tiers?
16: 
17: This design defines a TPC-E-inspired benchmark adapted to Forma's storage model and execution paths.
18: 
19: ## 2. Goals
20: 
21: - Provide a repeatable, extensible, and comparable benchmark for Forma hybrid queries.
22: - Use a financial trading domain inspired by TPC-E, centered on trade, customer, and security entities.
23: - Cover both hot-column access and long-tail EAV filters.
24: - Exercise pagination, sorting, filtering, deep page jumps, and cross-tier merge behavior under multiple data distributions.
25: - Produce a stable set of metrics for optimization work, including latency percentiles, throughput, tier hit ratios, dedup rate, and pushdown efficiency.
26: 
27: ## 3. Non-Goals
28: 
29: - Full TPC-E compliance is out of scope. This benchmark will not implement all 33 tables or the full transaction mix.
30: - This is not an official TPC benchmark publication framework and does not target auditability requirements.
31: - It does not replace the existing `internal/e2e_harness/federated` test suite. It extends it with benchmark-oriented workload coverage.
32: - Phase 1 does not target distributed execution or multi-host load generation.
33: 
34: ## 4. Design Principles
35: 
36: - Reuse the existing E2E harness wherever possible.
37: - Preserve TPC-E-style business semantics while fitting Forma's `entity_main + eav_data + change_log + parquet` architecture.
38: - Ensure reproducibility through deterministic data generation with explicit seeds.
39: - Cover both correctness and performance. The benchmark is not just a load test.
40: - Treat deep pagination as a first-class problem, and explicitly model `LIMIT/OFFSET` behavior alongside future keyset alternatives.
41: 
42: ## 5. Logical Architecture
43: 
44: The benchmark is split into six logical layers:
45: 
46: 1. Schema Mapping Layer
47: 2. Data Generation Layer
48: 3. Tier Preparation Layer
49: 4. Workload Definition Layer
50: 5. Benchmark Runner Layer
51: 6. Metrics and Report Layer
52: 
53: ### 5.1 Schema Mapping Layer
54: 
55: This layer maps TPC-E-inspired entities into Forma schemas.
56: 
57: The initial benchmark will define three core schemas:
58: 
59: - `trade`
60: - `customer`
61: - `security`
62: 
63: Each schema must include:
64: 
65: - hot fields suitable for indexed filtering and sorting
66: - EAV fields that force dynamic attribute access
67: - fields that participate in overlap and merge cases
68: 
69: Recommended `trade` schema:
70: 
71: - Hot: `symbol`, `trade_type`, `quantity`, `price`, `trade_time`, `customer_id`
72: - EAV: `exchange`, `commission`, `is_cash`, `broker_id`, `order_channel`
73: 
74: This shape allows the benchmark to cover:
75: 
76: - pure hot pushdown
77: - pure EAV filtering
78: - hybrid hot + EAV predicates
79: - time-ordered pagination
80: 
81: ### 5.2 Data Generation Layer
82: 
83: This layer generates benchmark datasets with configurable scale, distribution, and overlap patterns.
84: 
85: The benchmark must support these data distributions:
86: 
87: - `uniform`: baseline distribution
88: - `zipf`: hot customers, hot symbols, and hot regions
89: - `temporal`: higher density in recent time windows
90: - `partition-skew`: heavily imbalanced region or sector partitions
91: - `hotspot-overlap`: a controlled fraction of row IDs present in both cold and hot tiers
92: 
93: The generator output must guarantee:
94: 
95: - multiple versions of the same `row_id` can appear across tiers
96: - timestamps increase in a way that allows clear last-write-wins validation
97: - some records carry soft-delete markers
98: - selectivity can be calibrated for high-, medium-, and low-selectivity filters
99: 
100: ### 5.3 Tier Preparation Layer
101: 
102: This layer assigns generated rows into three tiers:
103: 
104: - Cold/Base: historical parquet, larger files, low churn
105: - Warm/Delta: recently flushed parquet, smaller files, newer data
106: - Hot: unflushed Postgres data in `entity_main`, `eav_data`, and `change_log`
107: 
108: Default tier mix:
109: 
110: - Base: 60%
111: - Delta: 30%
112: - Hot: 10%
113: 
114: The benchmark must also support alternative mixes such as:
115: 
116: - high-hotness: 40/20/40
117: - long-tail history: 85/10/5
118: - high-overlap: 5%-10% key overlap between Base and Hot
119: 
120: Tier preparation is responsible for:
121: 
122: - writing cold and warm data to base and delta parquet
123: - inserting hot rows into Postgres
124: - marking overlap, delete, update, and restore scenarios
125: 
126: ### 5.4 Workload Definition Layer
127: 
128: The initial workload set is organized into five categories.
129: 
130: #### A. Baseline Pagination
131: 
132: - unfiltered pagination ordered by `trade_time DESC`
133: - high-selectivity filter + pagination
134: - low-selectivity filter + pagination
135: - EAV filter + pagination
136: - mixed hot + EAV predicates + pagination
137: 
138: #### B. Deep Pagination
139: 
140: - `page = 1`
141: - `page = 100`
142: - `page = 1,000`
143: - `page = 100,000`
144: 
145: The benchmark must keep page size fixed, for example `20` or `50`, and explicitly record the offset size.
146: 
147: #### C. Tier Hit Modes
148: 
149: - hot-only hits
150: - cold-only hits
151: - cold + hot hits
152: - cold + warm + hot hits
153: 
154: #### D. Dedup and Override Cases
155: 
156: - same key in cold and hot with different versions
157: - hot-tier delete hiding a cold-tier row
158: - cold data partially overridden by hot-tier EAV updates
159: 
160: #### E. Routing and Strategy Comparisons
161: 
162: - `PreferHot = true`
163: - `RoutingStrategyHybrid`
164: - small result sets routed to Postgres
165: - large result sets routed to DuckDB
166: 
167: ### 5.5 Benchmark Runner Layer
168: 
169: The runner manages the full benchmark lifecycle:
170: 
171: 1. register schemas
172: 2. generate data
173: 3. prepare tiers
174: 4. warm up the system
175: 5. execute workloads repeatedly
176: 6. aggregate latency and correctness metrics
177: 7. export structured reports
178: 
179: The runner must support:
180: 
181: - serial execution
182: - fixed-concurrency execution
183: - running a subset of workloads
184: - explicit scale and distribution selection
185: - explicit random seed control
186: 
187: Recommended scales:
188: 
189: - `small`: 100K rows
190: - `medium`: 1M rows
191: - `large`: 10M rows
192: 
193: ### 5.6 Metrics and Report Layer
194: 
195: The benchmark must emit:
196: 
197: - `p50`, `p95`, `p99`, `max`, `avg`
198: - `qps`
199: - `result_count`
200: - `hot_hit_ratio`
201: - `cold_hit_ratio`
202: - `dedup_count`
203: - `delete_filtered_count`
204: - `pushdown_efficiency`
205: - `memory_peak_mb`
206: 
207: Output formats:
208: 
209: - console summary
210: - JSON result file
211: - Markdown report
212: 
213: This allows the same benchmark to serve local tuning, CI artifact generation, and regression analysis.
214: 
215: ## 6. Data Model
216: 
217: ### 6.1 TPC-E-Inspired Entities
218: 
219: The benchmark does not attempt a full TPC-E schema port. Instead, it borrows the domain shape:
220: 
221: - `trade`: high write frequency and strong time locality
222: - `customer`: medium update frequency and selective filtering value
223: - `security`: low update frequency and cold-reference behavior
224: 
225: ### 6.2 Hot and EAV Requirements
226: 
227: Each schema should include at least:
228: 
229: - 2 sortable hot fields
230: - 2 high-selectivity hot filter fields
231: - 1 low-selectivity hot filter field
232: - 2 EAV fields
233: - 1 field likely to participate in cross-tier override cases
234: 
235: ## 7. Pagination Design
236: 
237: Deep pagination is a primary focus of this benchmark.
238: 
239: ### 7.1 Baseline Path
240: 
241: Phase 1 uses the current pagination semantics:
242: 
243: - `LIMIT page_size OFFSET offset`
244: 
245: This gives a direct measurement of the current cost profile on merged results.
246: 
247: ### 7.2 Deep Pagination Concerns
248: 
249: The benchmark must measure:
250: 
251: - how DuckDB read and sort cost changes as offset grows
252: - the cost of merge-before-pagination when both hot and cold paths contribute rows
253: - whether small-result heuristics misroute some workloads
254: - whether total-count computation becomes the bottleneck on very large page jumps
255: 
256: ### 7.3 Large Page Jump Cases
257: 
258: The benchmark should include at least these fixed cases:
259: 
260: - `page_size=20, page=1`
261: - `page_size=20, page=100`
262: - `page_size=20, page=1,000`
263: - `page_size=20, page=100,000`
264: 
265: Equivalent keyset-style cases should be preserved as future comparison points, even if the keyset execution path is not implemented in phase 1.
266: 
267: ## 8. Correctness Requirements
268: 
269: The benchmark must validate semantics, not just latency.
270: 
271: It must check:
272: 
273: - correct merged row counts across cold, warm, and hot tiers
274: - correct winner selection for the latest version of the same `row_id`
275: - correct suppression of cold rows by hot soft deletes
276: - correct EAV attribute winner selection during cross-tier merges
277: - stable, ordered, and duplicate-free pagination results
278: 
279: ## 9. Observability Requirements
280: 
281: Each workload execution should capture:
282: 
283: - query name
284: - distribution
285: - scale
286: - page size
287: - offset or page number
288: - returned rows
289: - source tier mix
290: - duration
291: - error status
292: 
293: When execution-plan details are available, the benchmark should also capture:
294: 
295: - whether DuckDB was used
296: - whether `PreferHot` was enabled
297: - per-source row counts
298: - merge duration
299: 
300: ## 10. Integration with Existing Code
301: 
302: The benchmark should build on the existing components instead of creating a separate test system:
303: 
304: - `internal/e2e_harness/federated/fixtures.go`
305: - `internal/e2e_harness/federated/seeding.go`
306: - `internal/e2e_harness/federated/query.go`
307: - `internal/e2e_harness/federated/performance_test.go`
308: - `internal/federated_pagination.go`
309: - `internal/federated_merge.go`
310: - `internal/federated_routing.go`
311: 
312: ## 11. Risks and Tradeoffs
313: 
314: ### 11.1 Risks
315: 
316: - The current harness query model is relatively simple and may need extension for richer hybrid predicates.
317: - Large-scale dataset preparation may dominate benchmark runtime.
318: - Deep pagination may be expensive at large scale if it requires full merge before slicing.
319: - Matching exact selectivity targets across different distributions requires calibration support in the generator.
320: 
321: ### 11.2 Tradeoffs
322: 
323: - Phase 1 prioritizes a reproducible and useful benchmark over optimizer-level sophistication.
324: - Phase 1 reuses the current federated query stack rather than introducing a separate benchmark-only query DSL.
325: - Phase 1 should treat `medium` as the main regression scale and `large` as a controlled performance experiment scale.
326: 
327: ## 12. Milestones
328: 
329: Recommended milestones:
330: 
331: 1. define schemas, distributions, and workload matrix
332: 2. implement data generation, tier loading, and benchmark runner
333: 3. add deep-pagination, overlap, correctness checks, and reporting
334: 4. establish baseline results and wire them into CI or a repeatable performance review flow
335: 
336: ## 13. Deliverables
337: 
338: Phase 1 deliverables:
339: 
340: - benchmark HLD document
341: - benchmark implementation plan
342: - benchmark issue backlog
343: - benchmark scaffolding code
344: - at least one sample result set for `small` and `medium`
345: - a dedicated deep-pagination report

(End of file - total 345 lines)
</content>
```

---

## Assistant (Build · DeepSeek V4 Pro · 10.0s)

_Thinking:_

Now let me read the key files related to filter/EAV handling - especially the dual clauses, the DuckDB query builder, and the benchmark workloads that test filters.

**Tool: glob**

**Input:**
```json
{
  "pattern": "**/federated_*.go"
}
```

**Output:**
```
/home/ruoshi/code/github/forma/internal/federated_routing.go
/home/ruoshi/code/github/forma/internal/federated_result_processor_test.go
/home/ruoshi/code/github/forma/internal/federated_interfaces.go
/home/ruoshi/code/github/forma/internal/federated_pagination.go
/home/ruoshi/code/github/forma/internal/federated_merge.go
/home/ruoshi/code/github/forma/internal/federated_merge_test.go
```

**Tool: glob**

**Input:**
```json
{
  "pattern": "**/dual_clause*.go"
}
```

**Output:**
```
No files found
```

**Tool: glob**

**Input:**
```json
{
  "pattern": "**/benchmark/**/*.go"
}
```

**Output:**
```
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/report_test.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/report.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/workload.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/runner_test.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/runner.go
/home/ruoshi/code/github/forma/cmd/benchmark/main_test.go
/home/ruoshi/code/github/forma/cmd/benchmark/main.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/config.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/schema_test.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/schema.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/config_test.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/tier_test.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/tier.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/execute_test.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/dataset.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/generator_test.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/generator.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/calibration.go
```

**Tool: glob**

**Input:**
```json
{
  "pattern": "**/e2e_harness/federated/*.go"
}
```

**Output:**
```
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/query.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/query_unit_test.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/helpers.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/harness.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/seeding.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark_workload_execution_test.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/soft_delete_test.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/seeding_test.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/s3_operations.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark_scaffold_test.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark_tier_load_test.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/merge_on_read_test.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/fixtures.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/deduplication_test.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/data_tier_test.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/cdc.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/assertions.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/failure_modes_test.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/consistency_test.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/compaction_test.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/cdc_flush_test.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/suite_test.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/performance_test.go
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/validation.go
```

---

## Assistant (Build · DeepSeek V4 Pro · 10.8s)

_Thinking:_

Let me now read the critical files for understanding filter pushdown/EAV behavior:
1. The federated_merge.go for merge/EAV logic
2. The federated_routing.go for routing decisions
3. The DuckDB query template (where clauses for EAV/main)
4. The benchmark workload definitions
5. The dual clause / filter pushdown conversion (ToDualClauses is referenced in postgres_duckdb_query.go)

**Tool: grep**

**Input:**
```json
{
  "pattern": "ToDualClauses|DualClauses",
  "include": "*.go"
}
```

**Output:**
```
Found 34 matches
/home/ruoshi/code/github/forma/internal/health_and_renderer_test.go:
  Line 51: 	dual := &DualClauses{

  Line 69: 	dual := &DualClauses{

  Line 92: 	dual := &DualClauses{


/home/ruoshi/code/github/forma/internal/postgres_duckdb_query.go:
  Line 193: 	dc, err := ToDualClauses(q.Condition, sanitizeIdentifier(tables.EAVData), q.SchemaID, cache, &paramIndex)


/home/ruoshi/code/github/forma/internal/duckdb_template_renderer.go:
  Line 26: // It accepts optional DualClauses produced by ToDualClauses; when provided it will use

  Line 30: func BuildDuckDBQuery(tpl *template.Template, params any, q *FederatedAttributeQuery, dirtyIDs []uuid.UUID, dual *DualClauses) (string, []any, error) {

  Line 119: func injectDuckDBTemplateParams(params map[string]any, q *FederatedAttributeQuery, dual *DualClauses) {


/home/ruoshi/code/github/forma/internal/dualpath_sql_generator_test.go:
  Line 12: func TestToDualClauses_NilCondition(t *testing.T) {

  Line 16: 	dc, err := ToDualClauses(nil, "eav_table", 1, cache, &paramIndex)

  Line 24: func TestToDualClauses_SimpleKv_NoColumnBinding(t *testing.T) {

  Line 35: 	dc, err := ToDualClauses(cond, "eav_table", 1, cache, &paramIndex)

  Line 51: func TestToDualClauses_SimpleKv_WithColumnBinding(t *testing.T) {

  Line 64: 	dc, err := ToDualClauses(cond, "eav_table", 1, cache, &paramIndex)

  Line 76: func TestToDualClauses_NestedAndOr_GroupingAndOrdering(t *testing.T) {

  Line 130: func TestToDualClauses_EmptyComposite_NoOpBehavior(t *testing.T) {

  Line 155: func TestToDualClauses_UnknownAttribute_IgnoredForPgMain(t *testing.T) {

  Line 235: func TestToDualClauses_BoundAttributeUnsupportedOperator_ReturnsError(t *testing.T) {

  Line 321: func TestToDualClauses_DateMainColumnEncoding_UnixMsArgument(t *testing.T) {


/home/ruoshi/code/github/forma/internal/postgres_duckdb_federated_integration_test.go:
  Line 345: 	repo.buildDuckSQL = func(tpl *template.Template, params any, q *FederatedAttributeQuery, dirtyIDs []uuid.UUID, dual *DualClauses) (string, []any, error) {

  Line 422: 	repo.buildDuckSQL = func(tpl *template.Template, params any, q *FederatedAttributeQuery, dirtyIDs []uuid.UUID, dual *DualClauses) (string, []any, error) {

  Line 471: 	repo.buildDuckSQL = func(tpl *template.Template, params any, q *FederatedAttributeQuery, dirtyIDs []uuid.UUID, dual *DualClauses) (string, []any, error) {

  Line 490: 		t.Skipf("buildDuckDBQueryWithPlan requires ToDualClauses with valid condition: %v", err)

  Line 517: 	repo.buildDuckSQL = func(tpl *template.Template, params any, q *FederatedAttributeQuery, dirtyIDs []uuid.UUID, dual *DualClauses) (string, []any, error) {


/home/ruoshi/code/github/forma/internal/postgres_persistent_repository.go:
  Line 28: 	buildDuckSQL  func(*template.Template, any, *FederatedAttributeQuery, []uuid.UUID, *DualClauses) (string, []any, error)

  Line 50: func (r *DBPersistentRecordRepository) getDuckDBQueryBuilder() func(*template.Template, any, *FederatedAttributeQuery, []uuid.UUID, *DualClauses) (string, []any, error) {


/home/ruoshi/code/github/forma/internal/dualpath_sql_generator.go:
  Line 10: // DualClauses contains SQL fragments and argument lists for both Postgres and DuckDB.

  Line 11: type DualClauses struct {

  Line 21: // ToDualClauses generates Postgres and DuckDB WHERE fragments for the given condition.

  Line 26: func ToDualClauses(

  Line 32: ) (DualClauses, error) {

  Line 36: 		return DualClauses{}, fmt.Errorf("pg main generation: %w", err)

  Line 43: 		return DualClauses{}, fmt.Errorf("pg sql generation: %w", err)

  Line 49: 		return DualClauses{}, fmt.Errorf("duck sql generation: %w", err)

  Line 52: 	return DualClauses{

```

**Tool: read**

**Input:**
```json
{
  "filePath": "/home/ruoshi/code/github/forma/internal/federated_merge.go"
}
```

**Output:**
```
<path>/home/ruoshi/code/github/forma/internal/federated_merge.go</path>
<type>file</type>
<content>
1: package internal
2: 
3: import (
4: 	"fmt"
5: 	"sort"
6: )
7: 
8: // MergePersistentRecordsByTier performs a merge-on-read across multiple data tiers.
9: // Inputs are provided as a map from DataTier -> slice of *PersistentRecord.
10: // Last-write-wins semantics are applied using PersistentRecord.UpdatedAt and ChangeLog flushed state.
11: //
12: // Behavior:
13: //   - Records are deduplicated by (SchemaID, RowID).
14: //   - For each key, the record with the highest UpdatedAt is chosen. If equal and
15: //     preferHot==true, the record coming from the Hot tier is chosen.
16: //   - If a record originates from the ChangeLog buffer (flushed_at == 0) it is
17: //     considered the authoritative hot source and wins ties regardless of UpdatedAt.
18: //   - The chosen record is returned with OtherAttributes merged across all source
19: //     tiers for that (SchemaID, RowID) with attribute-level deduplication.
20: //   - Attributes are deduplicated by (AttrID, ArrayIndices).
21: //   - For an attribute present in multiple source records, the attribute from the
22: //     record with the latest UpdatedAt is chosen. Ties are resolved using preferHot
23: //     and deterministic tier ordering.
24: //   - Result slice is sorted by SchemaID then RowID for deterministic output.
25: func MergePersistentRecordsByTier(inputs map[DataTier][]*PersistentRecord, preferHot bool) ([]*PersistentRecord, error) {
26: 	if inputs == nil {
27: 		return nil, fmt.Errorf("inputs cannot be nil")
28: 	}
29: 
30: 	// Create an ordered tier priority used when preferHot=true and timestamps tie.
31: 	tierPriority := map[DataTier]int{
32: 		DataTierCold: 2,
33: 		DataTierWarm: 1,
34: 		DataTierHot:  0,
35: 	}
36: 
37: 	// Track winner per key (row-level LWW) as before, but also collect all seen records
38: 	// per key so we can merge OtherAttributes across tiers.
39: 	merged := make(map[string]*PersistentRecord)
40: 	mergedSourceTier := make(map[string]DataTier)
41: 
42: 	// recordsByKey holds all records seen for a particular merge key.
43: 	recordsByKey := make(map[string][]*PersistentRecord)
44: 	tiersByKey := make(map[string][]DataTier)
45: 
46: 	for tier, records := range inputs {
47: 		if records == nil {
48: 			continue
49: 		}
50: 		for _, rec := range records {
51: 			if rec == nil {
52: 				continue
53: 			}
54: 			key := mergeKey(rec)
55: 
56: 			// collect for attribute-level merging later
57: 			recordsByKey[key] = append(recordsByKey[key], rec)
58: 			tiersByKey[key] = append(tiersByKey[key], tier)
59: 
60: 			existing, ok := merged[key]
61: 			if !ok {
62: 				merged[key] = rec
63: 				mergedSourceTier[key] = tier
64: 				continue
65: 			}
66: 
67: 			// Choose winner between existing and rec
68: 			winner := chooseLWW(existing, mergedSourceTier[key], rec, tier, preferHot, tierPriority)
69: 			if winner == rec {
70: 				merged[key] = rec
71: 				mergedSourceTier[key] = tier
72: 			}
73: 		}
74: 	}
75: 
76: 	// Now merge OtherAttributes per key across all collected records.
77: 	for key, winner := range merged {
78: 		records := recordsByKey[key]
79: 		tiers := tiersByKey[key]
80: 		mergedAttrs := mergeOtherAttributes(records, tiers, preferHot, tierPriority)
81: 		if len(mergedAttrs) > 0 {
82: 			winner.OtherAttributes = mergedAttrs
83: 		} else {
84: 			winner.OtherAttributes = nil
85: 		}
86: 	}
87: 
88: 	// Collect results deterministically
89: 	results := make([]*PersistentRecord, 0, len(merged))
90: 	for _, v := range merged {
91: 		results = append(results, v)
92: 	}
93: 	sort.Slice(results, func(i, j int) bool {
94: 		if results[i].SchemaID != results[j].SchemaID {
95: 			return results[i].SchemaID < results[j].SchemaID
96: 		}
97: 		return results[i].RowID.String() < results[j].RowID.String()
98: 	})
99: 
100: 	return results, nil
101: }
102: 
103: func mergeKey(r *PersistentRecord) string {
104: 	return fmt.Sprintf("%d:%s", r.SchemaID, r.RowID.String())
105: }
106: 
107: // chooseLWW returns the record that should win based on UpdatedAt and preferences.
108: // existing and newRec are compared; existingTier / newTier indicate their source tiers.
109: func chooseLWW(existing *PersistentRecord, existingTier DataTier, newRec *PersistentRecord, newTier DataTier, preferHot bool, tierPriority map[DataTier]int) *PersistentRecord {
110: 	// If either record has a ChangeLog origin marker (OtherAttributes may include a special meta),
111: 	// prefer the record that indicates it's from the ChangeLog buffer. We represent this by a
112: 	// convention: repositories providing inputs should set UpdatedAt and DeletedAt accordingly,
113: 	// and mark Hot records coming from change_log with UpdatedAt and DeletedAt reflecting the buffer.
114: 	// For explicit handling, if UpdatedAt timestamps are equal but one record has a non-nil DeletedAt and
115: 	// the other doesn't, rely on UpdatedAt/DeletedAt comparison below.
116: 
117: 	// Compare UpdatedAt
118: 	if newRec.UpdatedAt > existing.UpdatedAt {
119: 		return newRec
120: 	}
121: 	if newRec.UpdatedAt < existing.UpdatedAt {
122: 		return existing
123: 	}
124: 
125: 	// If UpdatedAt equal, check DeletedAt presence (deleted should win as it is later change)
126: 	if existing.DeletedAt == nil && newRec.DeletedAt != nil {
127: 		return newRec
128: 	}
129: 	if existing.DeletedAt != nil && newRec.DeletedAt == nil {
130: 		return existing
131: 	}
132: 
133: 	// Timestamps equal -- apply PreferHot tiebreaker if requested.
134: 	if preferHot {
135: 		// Lower priority value means higher preference (0 = hot)
136: 		if tierPriority[newTier] < tierPriority[existingTier] {
137: 			return newRec
138: 		}
139: 		if tierPriority[newTier] > tierPriority[existingTier] {
140: 			return existing
141: 		}
142: 		// same tier, fallthrough to deterministic compare
143: 	}
144: 
145: 	// Deterministic tie-breaker: choose by lexicographic tier name then row id.
146: 	// This makes outcomes reproducible even without PreferHot.
147: 	if string(newTier) < string(existingTier) {
148: 		return newRec
149: 	}
150: 	if string(newTier) > string(existingTier) {
151: 		return existing
152: 	}
153: 
154: 	// As a final deterministic fallback, compare RowID strings (though same key)
155: 	if newRec.RowID.String() < existing.RowID.String() {
156: 		return newRec
157: 	}
158: 	return existing
159: }
160: 
161: // mergeOtherAttributes merges EAV attributes across multiple source records for the same row.
162: // Deduplication key: (AttrID, ArrayIndices).
163: // Selection: attribute from record with highest UpdatedAt; ties resolved with preferHot and deterministic tier ordering.
164: func mergeOtherAttributes(records []*PersistentRecord, tiers []DataTier, preferHot bool, tierPriority map[DataTier]int) []EAVRecord {
165: 	if len(records) == 0 {
166: 		return nil
167: 	}
168: 	type pickMeta struct {
169: 		attr      EAVRecord
170: 		updatedAt int64
171: 		tier      DataTier
172: 	}
173: 
174: 	attrMap := make(map[string]pickMeta) // key -> chosen attr meta
175: 	for i, rec := range records {
176: 		if rec == nil {
177: 			continue
178: 		}
179: 		tier := tiers[i]
180: 		for _, attr := range rec.OtherAttributes {
181: 			key := fmt.Sprintf("%d|%s", attr.AttrID, attr.ArrayIndices)
182: 			meta, ok := attrMap[key]
183: 			if !ok {
184: 				attrMap[key] = pickMeta{attr: attr, updatedAt: rec.UpdatedAt, tier: tier}
185: 				continue
186: 			}
187: 			// Compare rec.UpdatedAt vs meta.updatedAt
188: 			if rec.UpdatedAt > meta.updatedAt {
189: 				attrMap[key] = pickMeta{attr: attr, updatedAt: rec.UpdatedAt, tier: tier}
190: 				continue
191: 			}
192: 			if rec.UpdatedAt < meta.updatedAt {
193: 				continue
194: 			}
195: 			// UpdatedAt equal: tie-breaker using preferHot and tierPriority
196: 			if preferHot {
197: 				if tierPriority[tier] < tierPriority[meta.tier] {
198: 					attrMap[key] = pickMeta{attr: attr, updatedAt: rec.UpdatedAt, tier: tier}
199: 					continue
200: 				}
201: 				if tierPriority[tier] > tierPriority[meta.tier] {
202: 					continue
203: 				}
204: 				// same priority, fallthrough
205: 			}
206: 			// Deterministic fallback: lexicographic tier
207: 			if string(tier) < string(meta.tier) {
208: 				attrMap[key] = pickMeta{attr: attr, updatedAt: rec.UpdatedAt, tier: tier}
209: 			}
210: 			// else keep existing
211: 		}
212: 	}
213: 
214: 	// Collect and sort attributes for deterministic output
215: 	out := make([]EAVRecord, 0, len(attrMap))
216: 	for _, m := range attrMap {
217: 		out = append(out, m.attr)
218: 	}
219: 	sort.Slice(out, func(i, j int) bool {
220: 		if out[i].AttrID != out[j].AttrID {
221: 			return out[i].AttrID < out[j].AttrID
222: 		}
223: 		return out[i].ArrayIndices < out[j].ArrayIndices
224: 	})
225: 	return out
226: }

(End of file - total 226 lines)
</content>
```

**Tool: read**

**Input:**
```json
{
  "filePath": "/home/ruoshi/code/github/forma/internal/federated_routing.go"
}
```

**Output:**
```
<path>/home/ruoshi/code/github/forma/internal/federated_routing.go</path>
<type>file</type>
<content>
1: package internal
2: 
3: import (
4: 	"time"
5: 
6: 	"github.com/lychee-technology/forma"
7: )
8: 
9: // RoutingDecision indicates which tiers to query and whether to prefer DuckDB.
10: type RoutingDecision struct {
11: 	Tiers           []DataTier
12: 	UseDuckDB       bool
13: 	Reason          string
14: 	MaxScanRows     int
15: 	QueryTimeout    time.Duration
16: 	AllowS3Fallback bool
17: }
18: 
19: // EvaluateRoutingPolicy makes a routing decision based on config, query hints and options.
20: func EvaluateRoutingPolicy(cfg forma.DuckDBConfig, fq *FederatedAttributeQuery, opts *FederatedQueryOptions) RoutingDecision {
21: 	dec := RoutingDecision{
22: 		Tiers:           []DataTier{DataTierHot, DataTierWarm, DataTierCold},
23: 		UseDuckDB:       cfg.Enabled,
24: 		Reason:          "default",
25: 		MaxScanRows:     cfg.Routing.MaxDuckDBScanRows,
26: 		QueryTimeout:    cfg.QueryTimeout,
27: 		AllowS3Fallback: cfg.Routing.AllowS3Fallback,
28: 	}
29: 
30: 	// Honor explicit PreferredTiers
31: 	if fq != nil && len(fq.PreferredTiers) > 0 {
32: 		dec.Tiers = fq.PreferredTiers
33: 	}
34: 
35: 	// If DuckDB disabled, never use it
36: 	if !cfg.Enabled {
37: 		dec.UseDuckDB = false
38: 		dec.Reason = "duckdb disabled"
39: 		return dec
40: 	}
41: 
42: 	// Strategy-based heuristics
43: 	switch cfg.Routing.Strategy {
44: 	case forma.RoutingStrategyFreshnessFirst:
45: 		// prefer hot only if PreferHot or recent TTL required
46: 		if fq != nil && fq.PreferHot {
47: 			dec.UseDuckDB = false
48: 			dec.Tiers = []DataTier{DataTierHot}
49: 			dec.Reason = "prefer hot"
50: 		}
51: 	case forma.RoutingStrategyCostFirst:
52: 		// prefer DuckDB for large scans
53: 		if opts != nil && opts.MaxRows > 0 && opts.MaxRows > cfg.Routing.MaxDuckDBScanRows {
54: 			dec.UseDuckDB = true
55: 			dec.Reason = "cost-first large scan"
56: 		}
57: 	case forma.RoutingStrategyHybrid:
58: 		// hybrid: use duckdb unless PreferHot or small MaxRows
59: 		if fq != nil && fq.PreferHot {
60: 			dec.UseDuckDB = false
61: 			dec.Tiers = []DataTier{DataTierHot}
62: 			dec.Reason = "hybrid prefer hot"
63: 		} else if opts != nil && opts.MaxRows > 0 && opts.MaxRows < 1000 {
64: 			dec.UseDuckDB = false
65: 			dec.Reason = "hybrid small result set"
66: 		} else {
67: 			dec.UseDuckDB = true
68: 			dec.Reason = "hybrid use duckdb"
69: 		}
70: 	default:
71: 		// unknown strategy: keep defaults
72: 		dec.Reason = "unknown strategy - default"
73: 	}
74: 
75: 	// If DuckDB is disabled by decision, ensure tiers reflect that
76: 	if !dec.UseDuckDB {
77: 		dec.Tiers = []DataTier{DataTierHot}
78: 	}
79: 
80: 	return dec
81: }

(End of file - total 81 lines)
</content>
```

**Tool: read**

**Input:**
```json
{
  "filePath": "/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/workload.go"
}
```

**Output:**
```
<path>/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/workload.go</path>
<type>file</type>
<content>
1: package benchmark
2: 
3: import (
4: 	"fmt"
5: 	"strings"
6: )
7: 
8: // WorkloadCategory groups benchmark workloads by intent.
9: type WorkloadCategory string
10: 
11: // OracleMode declares how expected benchmark results are derived.
12: type OracleMode string
13: 
14: const (
15: 	WorkloadCategoryPagination WorkloadCategory = "pagination"
16: 	WorkloadCategoryFilter     WorkloadCategory = "filter"
17: 	WorkloadCategoryDeepPage   WorkloadCategory = "deep-pagination"
18: 	WorkloadCategoryTierMix    WorkloadCategory = "tier-mix"
19: 
20: 	OracleModeLoadedState OracleMode = "loaded-state"
21: 	OracleModeTruthPass   OracleMode = "truth-pass"
22: )
23: 
24: // WorkloadDefinition declares a benchmark workload.
25: type WorkloadDefinition struct {
26: 	Name                  string           `json:"name"`
27: 	Description           string           `json:"description"`
28: 	Category              WorkloadCategory `json:"category"`
29: 	TargetSchema          string           `json:"target_schema"`
30: 	ExecutionSource       string           `json:"execution_source,omitempty"`
31: 	FilterAttribute       string           `json:"filter_attribute,omitempty"`
32: 	FilterValue           string           `json:"filter_value,omitempty"`
33: 	FilterConditions      map[string]any   `json:"filter_conditions,omitempty"`
34: 	PageSize              int              `json:"page_size"`
35: 	PageNumber            int              `json:"page_number"`
36: 	SupportsDistributions []Distribution   `json:"supports_distributions"`
37: 	PreferHot             bool             `json:"prefer_hot,omitempty"`
38: 	UsesEAVFilter         bool             `json:"uses_eav_filter,omitempty"`
39: 	LargePageJump         bool             `json:"large_page_jump,omitempty"`
40: 	OracleMode            OracleMode       `json:"oracle_mode,omitempty"`
41: }
42: 
43: // DefaultWorkloads returns the initial declarative workload matrix.
44: func DefaultWorkloads() []WorkloadDefinition {
45: 	return []WorkloadDefinition{
46: 		{
47: 			Name:                  "baseline-page-1",
48: 			Description:           "Unfiltered first page ordered by trade time descending.",
49: 			Category:              WorkloadCategoryPagination,
50: 			TargetSchema:          "trade",
51: 			ExecutionSource:       "service",
52: 			PageSize:              20,
53: 			PageNumber:            1,
54: 			SupportsDistributions: allDistributions(),
55: 			OracleMode:            OracleModeLoadedState,
56: 		},
57: 		{
58: 			Name:                  "customer-region-page",
59: 			Description:           "Customer region filter to validate schema-scoped non-trade execution.",
60: 			Category:              WorkloadCategoryFilter,
61: 			TargetSchema:          "customer",
62: 			FilterAttribute:       "region",
63: 			FilterValue:           "NA",
64: 			PageSize:              20,
65: 			PageNumber:            1,
66: 			SupportsDistributions: allDistributions(),
67: 			OracleMode:            OracleModeLoadedState,
68: 		},
69: 		{
70: 			Name:                  "security-symbol-page",
71: 			Description:           "Security symbol filter to validate schema-scoped reference lookups.",
72: 			Category:              WorkloadCategoryFilter,
73: 			TargetSchema:          "security",
74: 			FilterAttribute:       "symbol",
75: 			FilterValue:           "SYM00001",
76: 			PageSize:              20,
77: 			PageNumber:            1,
78: 			SupportsDistributions: allDistributions(),
79: 			OracleMode:            OracleModeLoadedState,
80: 		},
81: 		{
82: 			Name:                  "hot-selective-page",
83: 			Description:           "High-selectivity hot-column filter with pagination on trade rows.",
84: 			Category:              WorkloadCategoryFilter,
85: 			TargetSchema:          "trade",
86: 			FilterAttribute:       "symbol",
87: 			FilterValue:           "SYM00001",
88: 			FilterConditions:      map[string]any{"symbol": "SYM00001"},
89: 			PageSize:              20,
90: 			PageNumber:            1,
91: 			SupportsDistributions: allDistributions(),
92: 		},
93: 		{
94: 			Name:                  "hot-low-selectivity-page",
95: 			Description:           "Lower-selectivity hot-column filter with pagination on trade rows.",
96: 			Category:              WorkloadCategoryFilter,
97: 			TargetSchema:          "trade",
98: 			FilterAttribute:       "tradeType",
99: 			FilterValue:           "0",
100: 			FilterConditions:      map[string]any{"tradeType": 0},
101: 			PageSize:              20,
102: 			PageNumber:            1,
103: 			SupportsDistributions: allDistributions(),
104: 			PreferHot:             true,
105: 		},
106: 		{
107: 			Name:                  "eav-selective-page",
108: 			Description:           "EAV-backed filter with paginated trade results.",
109: 			Category:              WorkloadCategoryFilter,
110: 			TargetSchema:          "trade",
111: 			FilterAttribute:       "exchange",
112: 			FilterValue:           "NYSE",
113: 			FilterConditions:      map[string]any{"exchange": "NYSE"},
114: 			PageSize:              20,
115: 			PageNumber:            1,
116: 			SupportsDistributions: allDistributions(),
117: 			UsesEAVFilter:         true,
118: 		},
119: 		{
120: 			Name:                  "mixed-hot-eav-page",
121: 			Description:           "Mixed hot and EAV filter workload with paginated trade results.",
122: 			Category:              WorkloadCategoryFilter,
123: 			TargetSchema:          "trade",
124: 			FilterAttribute:       "symbol",
125: 			FilterValue:           "SYM00001",
126: 			FilterConditions:      map[string]any{"symbol": "SYM00001", "exchange": "NYSE"},
127: 			PageSize:              20,
128: 			PageNumber:            1,
129: 			SupportsDistributions: allDistributions(),
130: 			PreferHot:             true,
131: 			UsesEAVFilter:         true,
132: 		},
133: 		{
134: 			Name:                  "mixed-tier-window",
135: 			Description:           "Time-window query expected to touch cold and hot tiers.",
136: 			Category:              WorkloadCategoryTierMix,
137: 			TargetSchema:          "trade",
138: 			PageSize:              50,
139: 			PageNumber:            1,
140: 			SupportsDistributions: []Distribution{DistributionTemporal, DistributionHotspot, DistributionUniform},
141: 			OracleMode:            OracleModeLoadedState,
142: 		},
143: 		{
144: 			Name:                  "hot-only-window",
145: 			Description:           "Recent time-window query expected to stay within hot rows.",
146: 			Category:              WorkloadCategoryTierMix,
147: 			TargetSchema:          "trade",
148: 			PageSize:              50,
149: 			PageNumber:            1,
150: 			SupportsDistributions: []Distribution{DistributionTemporal, DistributionHotspot, DistributionUniform},
151: 			PreferHot:             true,
152: 			OracleMode:            OracleModeLoadedState,
153: 		},
154: 		{
155: 			Name:                  "cold-only-window",
156: 			Description:           "Historical time-window query expected to stay within cold tiers.",
157: 			Category:              WorkloadCategoryTierMix,
158: 			TargetSchema:          "trade",
159: 			PageSize:              50,
160: 			PageNumber:            1,
161: 			SupportsDistributions: []Distribution{DistributionTemporal, DistributionHotspot, DistributionUniform},
162: 			OracleMode:            OracleModeLoadedState,
163: 		},
164: 		{
165: 			Name:                  "deep-page-1000",
166: 			Description:           "Deep pagination baseline at page 1,000 using LIMIT/OFFSET semantics.",
167: 			Category:              WorkloadCategoryDeepPage,
168: 			TargetSchema:          "trade",
169: 			PageSize:              20,
170: 			PageNumber:            1000,
171: 			SupportsDistributions: allDistributions(),
172: 			LargePageJump:         true,
173: 			OracleMode:            OracleModeLoadedState,
174: 		},
175: 		{
176: 			Name:                  "deep-page-100000",
177: 			Description:           "Large page jump benchmark at page 100,000.",
178: 			Category:              WorkloadCategoryDeepPage,
179: 			TargetSchema:          "trade",
180: 			PageSize:              20,
181: 			PageNumber:            100000,
182: 			SupportsDistributions: allDistributions(),
183: 			LargePageJump:         true,
184: 			OracleMode:            OracleModeLoadedState,
185: 		},
186: 	}
187: }
188: 
189: // SupportsDistribution reports whether a workload can run for a distribution.
190: func (w WorkloadDefinition) SupportsDistribution(dist Distribution) bool {
191: 	for _, supported := range w.SupportsDistributions {
192: 		if supported == dist {
193: 			return true
194: 		}
195: 	}
196: 	return false
197: }
198: 
199: // DerivedOffset returns the offset implied by page size and page number.
200: func (w WorkloadDefinition) DerivedOffset(defaultPageSize int) int {
201: 	pageSize := w.PageSize
202: 	if pageSize <= 0 {
203: 		pageSize = defaultPageSize
204: 	}
205: 	pageNumber := w.PageNumber
206: 	if pageNumber <= 1 {
207: 		return 0
208: 	}
209: 	return (pageNumber - 1) * pageSize
210: }
211: 
212: // UsesSimpleFilter reports whether the workload is representable via the current harness filter model.
213: func (w WorkloadDefinition) UsesSimpleFilter() bool {
214: 	return strings.TrimSpace(w.FilterAttribute) != ""
215: }
216: 
217: // ResolvedFilterConditions returns the filter map used by executable workloads.
218: func (w WorkloadDefinition) ResolvedFilterConditions() map[string]any {
219: 	if len(w.FilterConditions) > 0 {
220: 		conditions := make(map[string]any, len(w.FilterConditions))
221: 		for key, value := range w.FilterConditions {
222: 			conditions[key] = value
223: 		}
224: 		return conditions
225: 	}
226: 	if strings.TrimSpace(w.FilterAttribute) == "" {
227: 		return nil
228: 	}
229: 	return map[string]any{w.FilterAttribute: w.FilterValue}
230: }
231: 
232: // InferredOracleMode returns the workload-class default oracle mode.
233: func (w WorkloadDefinition) InferredOracleMode() OracleMode {
234: 	// Trade filter workloads use a truth-pass oracle because loaded-state-only
235: 	// reconstruction can diverge from executable federated filter semantics.
236: 	if w.Category == WorkloadCategoryFilter && w.TargetSchema == "trade" {
237: 		return OracleModeTruthPass
238: 	}
239: 	return OracleModeLoadedState
240: }
241: 
242: // ResolvedOracleMode returns the explicit oracle override or the inferred default.
243: func (w WorkloadDefinition) ResolvedOracleMode() OracleMode {
244: 	if w.OracleMode == "" {
245: 		return w.InferredOracleMode()
246: 	}
247: 	return w.OracleMode
248: }
249: 
250: // DefaultWorkloadNames returns the full default workload set.
251: func DefaultWorkloadNames() []string {
252: 	workloads := DefaultWorkloads()
253: 	names := make([]string, 0, len(workloads))
254: 	for _, workload := range workloads {
255: 		names = append(names, workload.Name)
256: 	}
257: 	return names
258: }
259: 
260: // ResolveWorkloads resolves named workloads from the default matrix.
261: func ResolveWorkloads(names []string) ([]WorkloadDefinition, error) {
262: 	all := DefaultWorkloads()
263: 	index := make(map[string]WorkloadDefinition, len(all))
264: 	for _, workload := range all {
265: 		index[workload.Name] = workload
266: 	}
267: 	resolved := make([]WorkloadDefinition, 0, len(names))
268: 	for _, name := range names {
269: 		workload, ok := index[name]
270: 		if !ok {
271: 			return nil, fmt.Errorf("unknown workload %q", name)
272: 		}
273: 		resolved = append(resolved, workload)
274: 	}
275: 	return resolved, nil
276: }
277: 
278: func allDistributions() []Distribution {
279: 	return []Distribution{
280: 		DistributionUniform,
281: 		DistributionZipf,
282: 		DistributionTemporal,
283: 		DistributionPartitionSkew,
284: 		DistributionHotspot,
285: 	}
286: }

(End of file - total 286 lines)
</content>
```

---

## Assistant (Build · DeepSeek V4 Pro · 11.5s)

_Thinking:_

Now let me read the critical remaining pieces:
1. `dualpath_sql_generator.go` - where ToDualClauses generates PG and DuckDB WHERE fragments (core pushdown logic)
2. `duckdb_template_renderer.go` - where the DuckDB SQL template is rendered with pushdown clauses
3. `federated_pagination.go` - pagination handling
4. The benchmark runner to see how workloads are executed
5. The DuckDB template SQL itself

**Tool: read**

**Input:**
```json
{
  "filePath": "/home/ruoshi/code/github/forma/internal/dualpath_sql_generator.go"
}
```

**Output:**
```
<path>/home/ruoshi/code/github/forma/internal/dualpath_sql_generator.go</path>
<type>file</type>
<content>
1: package internal
2: 
3: import (
4: 	"fmt"
5: 	"strings"
6: 
7: 	"github.com/lychee-technology/forma"
8: )
9: 
10: // DualClauses contains SQL fragments and argument lists for both Postgres and DuckDB.
11: type DualClauses struct {
12: 	PgClause     string // existing EAV-based clause (EXISTS...)
13: 	PgArgs       []any
14: 	PgMainClause string // predicates that can be pushed into entity_main (m.*)
15: 	PgMainArgs   []any
16: 
17: 	DuckClause string
18: 	DuckArgs   []any
19: }
20: 
21: // ToDualClauses generates Postgres and DuckDB WHERE fragments for the given condition.
22: // - PgClause reuses existing SQLGenerator (EAV-based EXISTS expressions).
23: // - PgMainClause contains predicates suitable for entity_main pushdown.
24: // - DuckClause maps attributes to column names when available and emits a simple DuckDB-style clause.
25: // Note: DuckDB placeholders are "?" and args are returned in order. Postgres uses $n placeholders.
26: func ToDualClauses(
27: 	condition forma.Condition,
28: 	eavTable string,
29: 	schemaID int16,
30: 	cache forma.SchemaAttributeCache,
31: 	paramIndex *int,
32: ) (DualClauses, error) {
33: 	// Build pushdown-capable main table predicates first so placeholders ($n) align.
34: 	pgMainClause, pgMainArgs, err := buildPgMainClause(condition, cache, paramIndex)
35: 	if err != nil {
36: 		return DualClauses{}, fmt.Errorf("pg main generation: %w", err)
37: 	}
38: 
39: 	// Postgres EAV side: reuse existing SQL generator for full condition
40: 	pgGen := NewSQLGenerator()
41: 	pgClause, pgArgs, err := pgGen.ToSQLClauses(condition, eavTable, schemaID, cache, paramIndex)
42: 	if err != nil {
43: 		return DualClauses{}, fmt.Errorf("pg sql generation: %w", err)
44: 	}
45: 
46: 	// DuckDB side: generate simple column-based predicates using attribute metadata
47: 	duckClause, duckArgs, err := buildDuckClause(condition, cache)
48: 	if err != nil {
49: 		return DualClauses{}, fmt.Errorf("duck sql generation: %w", err)
50: 	}
51: 
52: 	return DualClauses{
53: 		PgClause:     pgClause,
54: 		PgArgs:       pgArgs,
55: 		PgMainClause: pgMainClause,
56: 		PgMainArgs:   pgMainArgs,
57: 		DuckClause:   duckClause,
58: 		DuckArgs:     duckArgs,
59: 	}, nil
60: }
61: 
62: // classifyPredicate returns whether a KvCondition can be pushed to main table based on metadata.
63: func classifyPredicate(kv *forma.KvCondition, meta forma.AttributeMetadata) (bool, string) {
64: 	if meta.ColumnBinding == nil {
65: 		return false, "no column binding"
66: 	}
67: 
68: 	// Simple operator extraction
69: 	opPart := ""
70: 	valPart := ""
71: 	if idx := strings.Index(kv.Value, ":"); idx >= 0 {
72: 		opPart = kv.Value[:idx]
73: 		valPart = kv.Value[idx+1:]
74: 	}
75: 	opStr := "equals"
76: 	if opPart != "" && valPart != "" {
77: 		opStr = opPart
78: 	}
79: 
80: 	// Decide based on value type and operator
81: 	switch meta.ValueType {
82: 	case forma.ValueTypeText, forma.ValueTypeUUID:
83: 		// Text supports equals, starts_with, contains
84: 		if opStr == "equals" || opStr == "starts_with" || opStr == "contains" {
85: 			return true, "text supported"
86: 		}
87: 		return false, "text operator not supported"
88: 	case forma.ValueTypeNumeric, forma.ValueTypeInteger, forma.ValueTypeBigInt, forma.ValueTypeSmallInt:
89: 		// numeric supports comparison
90: 		switch opStr {
91: 		case "equals", "gt", "gte", "lt", "lte", "not_equals":
92: 			return true, "numeric supported"
93: 		default:
94: 			return false, "numeric operator not supported"
95: 		}
96: 	case forma.ValueTypeDate, forma.ValueTypeDateTime:
97: 		// date comparisons allowed; assume main column encoding supports it
98: 		switch opStr {
99: 		case "equals", "gt", "gte", "lt", "lte", "not_equals":
100: 			return true, "date supported"
101: 		default:
102: 			return false, "date operator not supported"
103: 		}
104: 	case forma.ValueTypeBool:
105: 		if opStr == "equals" || opStr == "not_equals" {
106: 			return true, "bool supported"
107: 		}
108: 		return false, "bool operator not supported"
109: 	default:
110: 		return false, "unknown value type"
111: 	}
112: }
113: 
114: // buildPgMainClause traverses the condition tree and emits a WHERE fragment targeting entity_main (m.*)
115: // It returns the clause string (with $n placeholders) and args slice, advancing paramIndex as needed.
116: func buildPgMainClause(cond forma.Condition, cache forma.SchemaAttributeCache, paramIndex *int) (string, []any, error) {
117: 	if cond == nil {
118: 		return "", nil, nil
119: 	}
120: 
121: 	switch c := cond.(type) {
122: 	case *forma.CompositeCondition:
123: 		return buildPgMainCompositeClause(c, cache, paramIndex)
124: 
125: 	case *forma.KvCondition:
126: 		return buildPgMainKvClause(c, cache, paramIndex)
127: 
128: 	default:
129: 		return "", nil, fmt.Errorf("unsupported condition type %T", cond)
130: 	}
131: }
132: 
133: // buildPgMainCompositeClause handles CompositeCondition for Postgres main table.
134: func buildPgMainCompositeClause(c *forma.CompositeCondition, cache forma.SchemaAttributeCache, paramIndex *int) (string, []any, error) {
135: 	if len(c.Conditions) == 0 {
136: 		return "", nil, nil
137: 	}
138: 
139: 	parts := make([]string, 0, len(c.Conditions))
140: 	args := []any{}
141: 	joiner := " AND "
142: 	if c.Logic == forma.LogicOr {
143: 		joiner = " OR "
144: 	}
145: 
146: 	for _, child := range c.Conditions {
147: 		p, a, err := buildPgMainClause(child, cache, paramIndex)
148: 		if err != nil {
149: 			return "", nil, err
150: 		}
151: 		if p != "" {
152: 			parts = append(parts, fmt.Sprintf("(%s)", p))
153: 			args = append(args, a...)
154: 		}
155: 	}
156: 
157: 	if len(parts) == 0 {
158: 		return "", nil, nil
159: 	}
160: 	if len(parts) == 1 {
161: 		return parts[0], args, nil
162: 	}
163: 	return "(" + strings.Join(parts, joiner) + ")", args, nil
164: }
165: 
166: // buildPgMainKvClause handles KvCondition for Postgres main table pushdown.
167: func buildPgMainKvClause(c *forma.KvCondition, cache forma.SchemaAttributeCache, paramIndex *int) (string, []any, error) {
168: 	meta, ok := cache[c.Attr]
169: 	if !ok {
170: 		// unknown attribute -> cannot push
171: 		return "", nil, nil
172: 	}
173: 
174: 	// Parse operator and value
175: 	opVal := parseOperatorValue(c.Value)
176: 
177: 	// Check if we can push to main table
178: 	useMain, _ := classifyPredicate(c, meta)
179: 	if !useMain {
180: 		if meta.ColumnBinding == nil {
181: 			return "", nil, nil
182: 		}
183: 		return "", nil, fmt.Errorf("unsupported operator: %s", opVal.op)
184: 	}
185: 	if meta.ColumnBinding == nil {
186: 		return "", nil, nil
187: 	}
188: 
189: 	// Convert operator to SQL
190: 	sqlOpResult, err := toSQLOperator(opVal.op, opVal.value)
191: 	if err != nil {
192: 		return "", nil, err
193: 	}
194: 
195: 	// Resolve column name
196: 	colName := resolveMainTableColumn(c.Attr, meta)
197: 
198: 	// Convert value based on metadata
199: 	parsedValue, err := convertPgMainValue(sqlOpResult.value, c.Attr, meta)
200: 	if err != nil {
201: 		return "", nil, err
202: 	}
203: 
204: 	// Create placeholder and clause
205: 	*paramIndex++
206: 	ph := fmt.Sprintf("$%d", *paramIndex)
207: 	sql := fmt.Sprintf("%s %s %s", colName, sqlOpResult.sqlOp, ph)
208: 	return sql, []any{parsedValue}, nil
209: }
210: 
211: // buildDuckClause traverses the condition tree and produces a DuckDB-compatible WHERE clause.
212: // This mirrors GenerateDuckDBWhereClause but uses attribute metadata to resolve column bindings.
213: func buildDuckClause(cond forma.Condition, cache forma.SchemaAttributeCache) (string, []any, error) {
214: 	if cond == nil {
215: 		return "1=1", nil, nil
216: 	}
217: 
218: 	switch c := cond.(type) {
219: 	case *forma.CompositeCondition:
220: 		return buildDuckCompositeClause(c, cache)
221: 
222: 	case *forma.KvCondition:
223: 		return buildDuckKvClause(c, cache)
224: 
225: 	default:
226: 		return "", nil, fmt.Errorf("unsupported condition type %T", cond)
227: 	}
228: }
229: 
230: // buildDuckCompositeClause handles CompositeCondition for DuckDB.
231: func buildDuckCompositeClause(c *forma.CompositeCondition, cache forma.SchemaAttributeCache) (string, []any, error) {
232: 	if len(c.Conditions) == 0 {
233: 		return "1=1", nil, nil
234: 	}
235: 
236: 	parts := make([]string, 0, len(c.Conditions))
237: 	args := []any{}
238: 	joiner := " AND "
239: 	if c.Logic == forma.LogicOr {
240: 		joiner = " OR "
241: 	}
242: 
243: 	for _, child := range c.Conditions {
244: 		p, a, err := buildDuckClause(child, cache)
245: 		if err != nil {
246: 			return "", nil, err
247: 		}
248: 		if p != "" {
249: 			parts = append(parts, fmt.Sprintf("(%s)", p))
250: 			args = append(args, a...)
251: 		}
252: 	}
253: 
254: 	if len(parts) == 0 {
255: 		return "1=1", nil, nil
256: 	}
257: 	return strings.Join(parts, joiner), args, nil
258: }
259: 
260: // buildDuckKvClause handles KvCondition for DuckDB.
261: func buildDuckKvClause(c *forma.KvCondition, cache forma.SchemaAttributeCache) (string, []any, error) {
262: 	// Parse operator and value
263: 	opVal := parseOperatorValue(c.Value)
264: 
265: 	// Convert operator to SQL
266: 	sqlOpResult, err := toSQLOperator(opVal.op, opVal.value)
267: 	if err != nil {
268: 		return "", nil, err
269: 	}
270: 
271: 	// Resolve column name using metadata
272: 	colName := resolveDuckDBColumn(c.Attr, cache)
273: 
274: 	// Get metadata and determine value type
275: 	var meta forma.AttributeMetadata
276: 	var hasMeta bool
277: 	if m, ok := cache[c.Attr]; ok {
278: 		meta = m
279: 		hasMeta = true
280: 	}
281: 
282: 	// If LIKE operator, keep simple text comparison
283: 	if sqlOpResult.sqlOp == "LIKE" {
284: 		clause := fmt.Sprintf("%s %s ?", colName, sqlOpResult.sqlOp)
285: 		return clause, []any{sqlOpResult.value}, nil
286: 	}
287: 
288: 	// Determine value type
289: 	var valueType forma.ValueType
290: 	if hasMeta {
291: 		valueType = meta.ValueType
292: 	} else {
293: 		valueType = detectValueType(sqlOpResult.value)
294: 	}
295: 
296: 	// For text type, simple comparison
297: 	if valueType == forma.ValueTypeText {
298: 		clause := fmt.Sprintf("%s %s ?", colName, sqlOpResult.sqlOp)
299: 		return clause, []any{sqlOpResult.value}, nil
300: 	}
301: 
302: 	// Use CastExpression to create CAST(? AS TYPE)
303: 	castExpr := CastExpression("?", valueType)
304: 	clause := fmt.Sprintf("%s %s %s", colName, sqlOpResult.sqlOp, castExpr)
305: 
306: 	// Parse param into typed form
307: 	rawParam, err := parseDuckDBRawParam(sqlOpResult.value, c.Attr, valueType)
308: 	if err != nil {
309: 		return "", nil, err
310: 	}
311: 
312: 	// Normalize for DuckDB
313: 	param, err := ToDuckDBParam(rawParam, valueType)
314: 	if err != nil {
315: 		return "", nil, fmt.Errorf("to duckdb param: %w", err)
316: 	}
317: 
318: 	return clause, []any{param}, nil
319: }

(End of file - total 319 lines)
</content>
```

**Tool: read**

**Input:**
```json
{
  "filePath": "/home/ruoshi/code/github/forma/internal/duckdb_template_renderer.go"
}
```

**Output:**
```
<path>/home/ruoshi/code/github/forma/internal/duckdb_template_renderer.go</path>
<type>file</type>
<content>
1: package internal
2: 
3: import (
4: 	"fmt"
5: 	"strings"
6: 	"text/template"
7: 
8: 	"github.com/google/uuid"
9: )
10: 
11: // RenderDuckDBQuery renders a DuckDB SQL template (which uses "?" placeholders)
12: // and combines the provided whereArgs (typically from GenerateDuckDBWhereClause)
13: // with the template-collected args. The order is: whereArgs first, then template args.
14: func RenderDuckDBQuery(tpl *template.Template, params any, whereArgs []any) (string, []any, error) {
15: 	sql, tplArgs, err := RenderSQLTemplate(tpl, params)
16: 	if err != nil {
17: 		return "", nil, err
18: 	}
19: 	combined := make([]any, 0, len(whereArgs)+len(tplArgs))
20: 	combined = append(combined, whereArgs...)
21: 	combined = append(combined, tplArgs...)
22: 	return sql, combined, nil
23: }
24: 
25: // BuildDuckDBQuery prepares a DuckDB SQL string and its arguments for a federated query.
26: // It accepts optional DualClauses produced by ToDualClauses; when provided it will use
27: // the DuckClause and DuckArgs as the base where clause and inject PgMainClause into template
28: // params so the template (or tests) can observe the pushdown fragment. Dirty-ID exclusions
29: // are appended to the DuckDB clause regardless of source.
30: func BuildDuckDBQuery(tpl *template.Template, params any, q *FederatedAttributeQuery, dirtyIDs []uuid.UUID, dual *DualClauses) (string, []any, error) {
31: 	isAdvancedTemplate := tpl == AdvancedQueryTemplateDuckDB
32: 
33: 	// Prepare where variables
34: 	var whereClause string
35: 	var whereArgs []any
36: 	var err error
37: 
38: 	// Ensure params is a map so we can inject Anchor.Condition, PgMainClause, and dirty helpers
39: 	m, ok := params.(map[string]any)
40: 	if !ok {
41: 		m = map[string]any{}
42: 	}
43: 
44: 	// Ensure Anchor map exists
45: 	anchor, ok := m["Anchor"].(map[string]any)
46: 	if !ok || anchor == nil {
47: 		anchor = map[string]any{}
48: 		m["Anchor"] = anchor
49: 	}
50: 
51: 	// If dual clauses provided, prefer them; otherwise fall back to legacy generator.
52: 	if dual != nil && dual.DuckClause != "" {
53: 		whereClause = dual.DuckClause
54: 		whereArgs = make([]any, 0, len(dual.DuckArgs)+len(dual.PgMainArgs)+len(dual.DuckArgs))
55: 		if len(dual.DuckArgs) > 0 {
56: 			whereArgs = append(whereArgs, dual.DuckArgs...)
57: 		}
58: 		// Generic templates need the dirty-id exclusion physically appended into the clause.
59: 		// The production DuckDB federated template manages dirty IDs via its own CTE/anti-join.
60: 		if !isAdvancedTemplate && len(dirtyIDs) > 0 {
61: 			var exclArgs []any
62: 			whereClause, exclArgs = AppendDirtyExclusion(whereClause, dirtyIDs)
63: 			whereArgs = append(whereArgs, exclArgs...)
64: 		}
65: 		anchor["Condition"] = whereClause
66: 
67: 		// Inject PgMainClause for inspection / postgres_scan integration
68: 		m["PgMainClause"] = dual.PgMainClause
69: 		m["PgMainArgs"] = dual.PgMainArgs
70: 		m["HasPgMainClause"] = dual.PgMainClause != ""
71: 		if isAdvancedTemplate {
72: 			m["LOGICAL_WHERE_CLAUSE"] = dual.DuckClause
73: 			m["PG_WHERE_CLAUSE"] = defaultIfEmpty(dual.PgMainClause, "1=1")
74: 			if len(dual.PgMainArgs) > 0 {
75: 				whereArgs = append(whereArgs, dual.PgMainArgs...)
76: 			}
77: 			if len(dual.DuckArgs) > 0 {
78: 				whereArgs = append(whereArgs, dual.DuckArgs...)
79: 			}
80: 		}
81: 		injectDuckDBTemplateParams(m, q, dual)
82: 		if !isAdvancedTemplate && len(dual.PgMainArgs) > 0 {
83: 			whereArgs = append(whereArgs, dual.PgMainArgs...)
84: 		}
85: 
86: 		merged := MergeTemplateParamsWithDirtyIDs(m, dirtyIDs)
87: 		return RenderDuckDBQuery(tpl, merged, whereArgs)
88: 	}
89: 
90: 	// Legacy path
91: 	if isAdvancedTemplate {
92: 		whereClause, whereArgs, err = GenerateDuckDBWhereClause(q)
93: 	} else {
94: 		whereClause, whereArgs, err = GenerateDuckDBWhereClauseWithExclusions(q, dirtyIDs)
95: 	}
96: 	if err != nil {
97: 		return "", nil, err
98: 	}
99: 	anchor["Condition"] = whereClause
100: 	if isAdvancedTemplate {
101: 		m["LOGICAL_WHERE_CLAUSE"] = whereClause
102: 		if len(whereArgs) > 0 {
103: 			whereArgs = append(whereArgs, whereArgs...)
104: 		}
105: 	}
106: 	injectDuckDBTemplateParams(m, q, nil)
107: 
108: 	merged := MergeTemplateParamsWithDirtyIDs(m, dirtyIDs)
109: 	return RenderDuckDBQuery(tpl, merged, whereArgs)
110: }
111: 
112: func defaultIfEmpty(s, fallback string) string {
113: 	if s == "" {
114: 		return fallback
115: 	}
116: 	return s
117: }
118: 
119: func injectDuckDBTemplateParams(params map[string]any, q *FederatedAttributeQuery, dual *DualClauses) {
120: 	if q == nil {
121: 		return
122: 	}
123: 
124: 	params["SCHEMA_ID"] = q.SchemaID
125: 	params["PAGE_SIZE"] = q.Limit
126: 	params["OFFSET"] = q.Offset
127: 
128: 	if _, ok := params["PG_WHERE_CLAUSE"]; !ok {
129: 		pgWhere := "1=1"
130: 		if dual != nil && dual.PgMainClause != "" {
131: 			pgWhere = dual.PgMainClause
132: 		}
133: 		params["PG_WHERE_CLAUSE"] = pgWhere
134: 	}
135: 
136: 	if _, ok := params["LOGICAL_WHERE_CLAUSE"]; !ok {
137: 		if anchor, ok := params["Anchor"].(map[string]any); ok {
138: 			if cond, ok := anchor["Condition"].(string); ok && cond != "" {
139: 				params["LOGICAL_WHERE_CLAUSE"] = cond
140: 			}
141: 		}
142: 	}
143: 
144: 	if _, ok := params["PG_CONN"]; !ok {
145: 		if raw, ok := params["DuckDBPGConnString"].(string); ok && raw != "" {
146: 			params["PG_CONN"] = raw
147: 		}
148: 	}
149: 
150: 	if _, ok := params["S3_PATHS"]; !ok {
151: 		if paths, ok := params["DuckDBS3Paths"].([]string); ok && len(paths) > 0 {
152: 			params["S3_PATHS"] = formatDuckDBPathList(paths)
153: 		}
154: 	}
155: }
156: 
157: func formatDuckDBPathList(paths []string) string {
158: 	quoted := make([]string, 0, len(paths))
159: 	for _, path := range paths {
160: 		quoted = append(quoted, fmt.Sprintf("'%s'", path))
161: 	}
162: 	if len(quoted) == 1 {
163: 		return quoted[0]
164: 	}
165: 	return "[" + strings.Join(quoted, ", ") + "]"
166: }

(End of file - total 166 lines)
</content>
```

**Tool: read**

**Input:**
```json
{
  "filePath": "/home/ruoshi/code/github/forma/internal/federated_pagination.go"
}
```

**Output:**
```
<path>/home/ruoshi/code/github/forma/internal/federated_pagination.go</path>
<type>file</type>
<content>
1: package internal
2: 
3: import (
4: 	"context"
5: 	"fmt"
6: 	"time"
7: )
8: 
9: // ExecuteFederatedPaginatedQuery performs a federated fetch across Postgres (hot) and DuckDB (cold/warm),
10: // merges results with last-write-wins semantics, and returns the requested page plus an accurate total
11: // deduplicated across sources.
12: //
13: // Notes:
14: // - This is an MVP coordinator: it caps per-source fetches (opts.MaxRows or default) to avoid OOM.
15: // - For very large result sets a keys-only two-phase approach should be implemented later.
16: func (r *DBPersistentRecordRepository) ExecuteFederatedPaginatedQuery(
17: 	ctx context.Context,
18: 	tables StorageTables,
19: 	fq *FederatedAttributeQuery,
20: 	limit, offset int,
21: 	attributeOrders []AttributeOrder,
22: 	opts *FederatedQueryOptions,
23: ) ([]*PersistentRecord, int64, error) {
24: 	if fq == nil {
25: 		return nil, 0, fmt.Errorf("federated query cannot be nil")
26: 	}
27: 	if limit <= 0 {
28: 		limit = defaultPageSize
29: 	}
30: 	if offset < 0 {
31: 		offset = 0
32: 	}
33: 
34: 	// Build shared hybrid WHERE clause
35: 	clause, args, err := r.buildHybridConditions(tables.EAVData, tables.EntityMain, fq.AttributeQuery, 0, fq.UseMainAsAnchor)
36: 	if err != nil {
37: 		return nil, 0, fmt.Errorf("build hybrid conditions: %w", err)
38: 	}
39: 
40: 	// Determine per-source fetch cap
41: 	maxRows := federatedMaxRows
42: 	if opts != nil && opts.MaxRows > 0 {
43: 		maxRows = opts.MaxRows
44: 	}
45: 
46: 	// Fetch from Postgres (hot)
47: 	startPg := time.Now()
48: 	pgRecs, _, err := r.runOptimizedQuery(ctx, tables, fq.SchemaID, clause, args, maxRows, 0, attributeOrders, fq.UseMainAsAnchor)
49: 	pgDuration := time.Since(startPg).Milliseconds()
50: 	if err != nil {
51: 		return nil, 0, fmt.Errorf("fetch postgres records: %w", err)
52: 	}
53: 	// Record Postgres source info if execution plan requested
54: 	if opts != nil && opts.IncludeExecutionPlan && opts.ExecutionPlan != nil {
55: 		dp := DataSourcePlan{
56: 			Tier:              DataTierHot,
57: 			Engine:            "postgres",
58: 			SQL:               "", // SQL not captured here
59: 			RowEstimate:       0,
60: 			PredicatePushdown: fq.UseMainAsAnchor,
61: 			ActualRows:        int64(len(pgRecs)),
62: 			DurationMs:        pgDuration,
63: 			Reason:            "postgres optimized query",
64: 		}
65: 		opts.ExecutionPlan.Sources = append(opts.ExecutionPlan.Sources, dp)
66: 		opts.ExecutionPlan.Timings["postgres_fetch"] = pgDuration
67: 	}
68: 
69: 	// Fetch from DuckDB (warm/cold)
70: 	duckRecs, _, err := r.ExecuteDuckDBFederatedQuery(ctx, tables, fq, maxRows, 0, attributeOrders, opts)
71: 	if err != nil {
72: 		return nil, 0, fmt.Errorf("fetch duckdb records: %w", err)
73: 	}
74: 
75: 	// Merge across tiers using existing merge logic
76: 	inputs := map[DataTier][]*PersistentRecord{
77: 		DataTierHot:  pgRecs,
78: 		DataTierWarm: nil,
79: 		DataTierCold: duckRecs,
80: 	}
81: 
82: 	startMerge := time.Now()
83: 	merged, err := MergePersistentRecordsByTier(inputs, fq.PreferHot)
84: 	mergeMs := time.Since(startMerge).Milliseconds()
85: 	if err != nil {
86: 		return nil, 0, fmt.Errorf("merge records by tier: %w", err)
87: 	}
88: 	// Record merge plan if requested
89: 	if opts != nil && opts.IncludeExecutionPlan && opts.ExecutionPlan != nil {
90: 		opts.ExecutionPlan.Merge = MergePlan{
91: 			Strategy:   MergeStrategyLastWriteWins,
92: 			PreferHot:  fq.PreferHot,
93: 			DedupKeys:  []string{"SchemaID:RowID"},
94: 			DurationMs: mergeMs,
95: 			Notes:      []string{"attribute-level deduplication applied"},
96: 		}
97: 		opts.ExecutionPlan.Timings["merge"] = mergeMs
98: 	}
99: 
100: 	total := int64(len(merged))
101: 
102: 	// Apply pagination on merged, which is deterministically ordered by MergePersistentRecordsByTier
103: 	start := offset
104: 	if start >= len(merged) {
105: 		return []*PersistentRecord{}, total, nil
106: 	}
107: 	end := min(start+limit, len(merged))
108: 	page := merged[start:end]
109: 
110: 	return page, total, nil
111: }

(End of file - total 111 lines)
</content>
```

**Tool: read**

**Input:**
```json
{
  "filePath": "/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/runner.go"
}
```

**Output:**
```
<path>/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/runner.go</path>
<type>file</type>
<content>
1: package benchmark
2: 
3: import (
4: 	"context"
5: 	"database/sql"
6: 	"fmt"
7: 	"sort"
8: 	"strconv"
9: 	"strings"
10: 	"time"
11: 
12: 	"github.com/google/uuid"
13: 	"github.com/jackc/pgx/v5/pgxpool"
14: 	forma "github.com/lychee-technology/forma"
15: 	"github.com/lychee-technology/forma/internal"
16: 	federated "github.com/lychee-technology/forma/internal/e2e_harness/federated"
17: )
18: 
19: // RunResult captures benchmark execution output.
20: type RunResult struct {
21: 	Config         Config               `json:"config"`
22: 	Generator      GeneratorConfig      `json:"generator"`
23: 	Metadata       ArtifactMetadata     `json:"metadata"`
24: 	StartedAt      time.Time            `json:"started_at"`
25: 	CompletedAt    time.Time            `json:"completed_at"`
26: 	ValidationOnly bool                 `json:"validation_only"`
27: 	Passed         bool                 `json:"passed"`
28: 	FailureCount   int                  `json:"failure_count,omitempty"`
29: 	InfraError     string               `json:"infra_error,omitempty"`
30: 	Schemas        []SchemaFixture      `json:"schemas"`
31: 	Workloads      []WorkloadDefinition `json:"workloads"`
32: 	Executions     []WorkloadRunResult  `json:"executions,omitempty"`
33: 	Notes          []string             `json:"notes"`
34: 	OracleModes    map[string]string    `json:"oracle_modes,omitempty"`
35: }
36: 
37: // WorkloadRunResult captures one workload execution result.
38: type WorkloadRunResult struct {
39: 	Name         string            `json:"name"`
40: 	Category     string            `json:"category"`
41: 	Distribution Distribution      `json:"distribution"`
42: 	PageSize     int               `json:"page_size"`
43: 	PageNumber   int               `json:"page_number"`
44: 	Offset       int               `json:"offset"`
45: 	PreferHot    bool              `json:"prefer_hot,omitempty"`
46: 	ResultCount  int               `json:"result_count"`
47: 	TotalRecords int64             `json:"total_records"`
48: 	Duration     time.Duration     `json:"duration"`
49: 	Passed       bool              `json:"passed"`
50: 	FailureKind  string            `json:"failure_kind,omitempty"`
51: 	OracleMode   string            `json:"oracle_mode,omitempty"`
52: 	FailureCount int               `json:"failure_count,omitempty"`
53: 	InfraError   string            `json:"infra_error,omitempty"`
54: 	RowIDs       []string          `json:"row_ids,omitempty"`
55: 	Assertions   []AssertionResult `json:"assertions,omitempty"`
56: 	PlanNotes    []string          `json:"plan_notes,omitempty"`
57: }
58: 
59: const (
60: 	FailureKindInfra       = "infra"
61: 	FailureKindCorrectness = "correctness"
62: )
63: 
64: // AssertionResult captures one correctness assertion outcome.
65: type AssertionResult struct {
66: 	Name    string `json:"name"`
67: 	Passed  bool   `json:"passed"`
68: 	Message string `json:"message,omitempty"`
69: }
70: 
71: // Runner validates benchmark inputs and prepares execution plans.
72: type Runner struct {
73: 	config    Config
74: 	registry  forma.SchemaRegistry
75: 	schemas   []SchemaFixture
76: 	workloads []WorkloadDefinition
77: 	genConfig GeneratorConfig
78: }
79: 
80: // NewRunner builds a runner using the benchmark fixture registry.
81: func NewRunner(cfg Config) (*Runner, error) {
82: 	resolved := cfg.WithDefaults()
83: 	if err := resolved.Validate(); err != nil {
84: 		return nil, err
85: 	}
86: 	registry, err := LoadFixtureRegistry()
87: 	if err != nil {
88: 		return nil, err
89: 	}
90: 	workloads, err := ResolveWorkloads(resolved.Workloads)
91: 	if err != nil {
92: 		return nil, err
93: 	}
94: 	return &Runner{
95: 		config:    resolved,
96: 		registry:  registry,
97: 		schemas:   DefaultSchemaFixtures(),
98: 		workloads: workloads,
99: 		genConfig: GeneratorConfigFromBenchmark(resolved),
100: 	}, nil
101: }
102: 
103: // RegisterSchemas loads the benchmark schema entries into a harness-backed registry table.
104: func (r *Runner) RegisterSchemas(registrar SchemaRegistrar) error {
105: 	return RegisterFixtureSchemas(registrar)
106: }
107: 
108: // Run executes the scaffolded benchmark flow.
109: func (r *Runner) Run(ctx context.Context) (*RunResult, error) {
110: 	if err := ctx.Err(); err != nil {
111: 		return nil, err
112: 	}
113: 	startedAt := time.Now()
114: 	if err := r.validateFixtures(); err != nil {
115: 		return nil, err
116: 	}
117: 	result := &RunResult{
118: 		Config:         r.config,
119: 		Generator:      r.genConfig,
120: 		Metadata:       BuildArtifactMetadata(r.config, r.genConfig, r.workloads),
121: 		StartedAt:      startedAt,
122: 		ValidationOnly: true,
123: 		Passed:         true,
124: 		Schemas:        append([]SchemaFixture(nil), r.schemas...),
125: 		Workloads:      append([]WorkloadDefinition(nil), r.workloads...),
126: 	}
127: 	switch r.config.Mode {
128: 	case ExecutionModeSmoke:
129: 		result.Notes = []string{
130: 			"validated benchmark configuration",
131: 			"loaded TPC-E-inspired schema fixtures",
132: 			"resolved workload matrix",
133: 			fmt.Sprintf("prepared generator for scale=%s distribution=%s", r.genConfig.Scale, r.genConfig.Distribution),
134: 			"smoke mode stops before query execution",
135: 		}
136: 	case ExecutionModePlan:
137: 		result.Notes = []string{
138: 			"validated benchmark configuration",
139: 			"loaded TPC-E-inspired schema fixtures",
140: 			fmt.Sprintf("prepared generator for scale=%s distribution=%s", r.genConfig.Scale, r.genConfig.Distribution),
141: 			"built execution plan only",
142: 		}
143: 	default:
144: 		return nil, fmt.Errorf("unsupported execution mode %q", r.config.Mode)
145: 	}
146: 	result.CompletedAt = time.Now()
147: 	return result, nil
148: }
149: 
150: // RunWithHarness executes supported benchmark workloads against a live federated harness.
151: func (r *Runner) RunWithHarness(ctx context.Context, h *federated.FederatedTestHarness, profile TierMixProfile) (*RunResult, error) {
152: 	if h == nil {
153: 		return nil, fmt.Errorf("federated harness cannot be nil")
154: 	}
155: 	if err := ctx.Err(); err != nil {
156: 		return nil, err
157: 	}
158: 	startedAt := time.Now()
159: 	if err := r.validateFixtures(); err != nil {
160: 		return nil, err
161: 	}
162: 	generator, err := NewGenerator(r.genConfig)
163: 	if err != nil {
164: 		return nil, err
165: 	}
166: 	dataset, err := generator.Generate()
167: 	if err != nil {
168: 		return nil, err
169: 	}
170: 	tiered, err := SplitIntoTiers(dataset, profile)
171: 	if err != nil {
172: 		return nil, err
173: 	}
174: 	if err := LoadTieredDataset(ctx, h, tiered); err != nil {
175: 		return nil, err
176: 	}
177: 	executions := make([]WorkloadRunResult, 0, len(r.workloads)*r.config.Iterations)
178: 	pageRuns := make(map[string]WorkloadRunResult)
179: 	previousRuns := make(map[string]WorkloadRunResult)
180: 	loadedRecords, err := buildLoadedStateSnapshot(ctx, h, tiered)
181: 	if err != nil {
182: 		return nil, fmt.Errorf("build loaded state snapshot: %w", err)
183: 	}
184: 	expectedByWorkload, oracleModes, oracleNotes, err := r.buildExpectedResults(ctx, h, loadedRecords)
185: 	if err != nil {
186: 		return nil, err
187: 	}
188: 	passed := true
189: 	failureCount := 0
190: 	for _, workload := range r.workloads {
191: 		if !workload.SupportsDistribution(r.genConfig.Distribution) {
192: 			continue
193: 		}
194: 		for iteration := 0; iteration < r.config.Iterations; iteration++ {
195: 			run, records, err := r.executeWorkload(ctx, h, workload)
196: 			if err != nil {
197: 				run = failedWorkloadRunResult(workload, r.genConfig.Distribution, r.config.PageSize, fmt.Sprintf("execute workload: %v", err))
198: 				executions = append(executions, run)
199: 				passed = false
200: 				failureCount++
201: 				continue
202: 			}
203: 			semantics := semanticsForWorkload(workload, r.genConfig)
204: 			run.Assertions = append(run.Assertions, validateBasicWorkloadAssertions(workload, run)...)
205: 			run.Assertions = append(run.Assertions, validateResultLevelAssertions(workload, run, records, semantics)...)
206: 			run.OracleMode = string(workload.ResolvedOracleMode())
207: 			if expected, ok := expectedByWorkload[workload.Name]; ok {
208: 				run.Assertions = append(run.Assertions, validateExpectedWorkloadOutcome(run, expected)...)
209: 			}
210: 			if workload.Category == WorkloadCategoryPagination || workload.Category == WorkloadCategoryDeepPage {
211: 				if previous, ok := pageRuns[workload.TargetSchema]; ok {
212: 					run.Assertions = append(run.Assertions, validatePaginationTransition(previous, run)...)
213: 				}
214: 				pageRuns[workload.TargetSchema] = run
215: 			}
216: 			if previous, ok := previousRuns[workload.Name]; ok {
217: 				run.Assertions = append(run.Assertions, validateRepeatedRunStability(previous, run)...)
218: 			}
219: 			previousRuns[workload.Name] = run
220: 			run.FailureCount = countFailedAssertions(run.Assertions)
221: 			run.FailureKind = failureKindForRun(run)
222: 			run.Passed = run.FailureCount == 0 && run.InfraError == ""
223: 			if !run.Passed {
224: 				passed = false
225: 				failureCount += maxInt(1, run.FailureCount)
226: 			}
227: 			executions = append(executions, run)
228: 		}
229: 	}
230: 	result := &RunResult{
231: 		Config:         r.config,
232: 		Generator:      r.genConfig,
233: 		Metadata:       BuildArtifactMetadata(r.config, r.genConfig, r.workloads),
234: 		StartedAt:      startedAt,
235: 		CompletedAt:    time.Now(),
236: 		ValidationOnly: false,
237: 		Passed:         passed,
238: 		FailureCount:   failureCount,
239: 		Schemas:        append([]SchemaFixture(nil), r.schemas...),
240: 		Workloads:      append([]WorkloadDefinition(nil), r.workloads...),
241: 		Executions:     executions,
242: 		OracleModes:    oracleModes,
243: 		Notes: []string{
244: 			"loaded TPC-E-inspired schema fixtures",
245: 			fmt.Sprintf("generated dataset with distribution=%s", r.genConfig.Distribution),
246: 			fmt.Sprintf("loaded tiered dataset profile=%s", profile.Name),
247: 			fmt.Sprintf("loaded-state snapshot rows=%d", len(loadedRecords)),
248: 			oracleNotes,
249: 			"prefer_hot expresses workload intent and report provenance, not hard execution routing",
250: 			"executed supported federated query workloads",
251: 		},
252: 	}
253: 	return result, nil
254: }
255: 
256: func (r *Runner) buildExpectedResults(ctx context.Context, h *federated.FederatedTestHarness, loadedRecords []GeneratedRecord) (map[string]expectedWorkloadResult, map[string]string, string, error) {
257: 	results := buildExpectedWorkloadResultsFromRecords(loadedRecords, r.workloads, r.config.PageSize, r.genConfig)
258: 	oracleModes := make(map[string]string, len(r.workloads))
259: 	loadedStateCount := 0
260: 	truthPassCount := 0
261: 	for _, workload := range r.workloads {
262: 		mode := string(workload.ResolvedOracleMode())
263: 		oracleModes[workload.Name] = mode
264: 		switch workload.ResolvedOracleMode() {
265: 		case OracleModeTruthPass:
266: 			expected, err := buildExpectedWorkloadResultFromFederatedTruth(ctx, h, workload, r.config.PageSize, loadedRecords, r.genConfig)
267: 			if err != nil {
268: 				return nil, nil, "", fmt.Errorf("build truth-pass expected result for %s: %w", workload.Name, err)
269: 			}
270: 			results[workload.Name] = expected
271: 			truthPassCount++
272: 		default:
273: 			loadedStateCount++
274: 		}
275: 	}
276: 	return results, oracleModes, fmt.Sprintf("oracle_modes loaded_state=%d truth_pass=%d", loadedStateCount, truthPassCount), nil
277: }
278: 
279: func (r *Runner) validateFixtures() error {
280: 	for _, fixture := range r.schemas {
281: 		id, _, err := r.registry.GetSchemaAttributeCacheByName(fixture.Name)
282: 		if err != nil {
283: 			return fmt.Errorf("validate fixture %s: %w", fixture.Name, err)
284: 		}
285: 		if id != fixture.ID {
286: 			return fmt.Errorf("fixture %s expected schema ID %d, got %d", fixture.Name, fixture.ID, id)
287: 		}
288: 		if _, _, err := r.registry.GetSchemaByName(fixture.Name); err != nil {
289: 			return fmt.Errorf("load JSON schema for %s: %w", fixture.Name, err)
290: 		}
291: 	}
292: 	return nil
293: }
294: 
295: func (r *Runner) executeWorkload(ctx context.Context, h *federated.FederatedTestHarness, workload WorkloadDefinition) (WorkloadRunResult, []*internal.PersistentRecord, error) {
296: 	if workload.ExecutionSource == "service" {
297: 		return r.executeServiceWorkload(ctx, h, workload)
298: 	}
299: 
300: 	pageSize := workload.PageSize
301: 	if pageSize <= 0 {
302: 		pageSize = r.config.PageSize
303: 	}
304: 	schemaID, err := workloadSchemaID(workload.TargetSchema)
305: 	if err != nil {
306: 		return WorkloadRunResult{}, nil, err
307: 	}
308: 	previousSchemaID := h.SchemaID
309: 	h.SchemaID = schemaID
310: 	defer func() {
311: 		h.SchemaID = previousSchemaID
312: 	}()
313: 	opts := queryOptionsForWorkloadWithConfig(workload, r.config.PageSize, r.genConfig)
314: 	opts.PreferHot = workload.PreferHot && workload.Category == WorkloadCategoryTierMix
315: 	if conditions := workload.ResolvedFilterConditions(); len(conditions) > 0 {
316: 		opts.Filter = &federated.Filter{Conditions: conditions}
317: 	}
318: 	result, err := h.ExecuteFederatedQuery(ctx, opts)
319: 	if err != nil {
320: 		return WorkloadRunResult{}, nil, err
321: 	}
322: 	run := WorkloadRunResult{
323: 		Name:         workload.Name,
324: 		Category:     string(workload.Category),
325: 		Distribution: r.genConfig.Distribution,
326: 		PageSize:     pageSize,
327: 		PageNumber:   workload.PageNumber,
328: 		Offset:       opts.Offset,
329: 		PreferHot:    workload.PreferHot,
330: 		ResultCount:  len(result.Records),
331: 		TotalRecords: result.TotalRecords,
332: 		Duration:     result.Duration,
333: 		Passed:       true,
334: 		RowIDs:       persistentRecordIDs(result.Records),
335: 	}
336: 	if result.Plan != nil {
337: 		run.PlanNotes = append([]string(nil), result.Plan.Notes...)
338: 	}
339: 	if workload.PreferHot {
340: 		run.PlanNotes = append(run.PlanNotes, "prefer_hot=true (intent/provenance only; no hard routing override yet)")
341: 		if opts.PreferHot {
342: 			run.PlanNotes = append(run.PlanNotes, "prefer_hot_execution=true (postgres-only override active for tier-mix workload)")
343: 		}
344: 	}
345: 	return run, result.Records, nil
346: }
347: 
348: func (r *Runner) executeServiceWorkload(ctx context.Context, h *federated.FederatedTestHarness, workload WorkloadDefinition) (WorkloadRunResult, []*internal.PersistentRecord, error) {
349: 	req, pageSize := queryRequestForWorkload(workload, r.config.PageSize)
350: 	start := time.Now()
351: 	result, records, err := executeServiceQuery(ctx, h, req, r.config.PageSize)
352: 	if err != nil {
353: 		return WorkloadRunResult{}, nil, err
354: 	}
355: 	run := WorkloadRunResult{
356: 		Name:         workload.Name,
357: 		Category:     string(workload.Category),
358: 		Distribution: r.genConfig.Distribution,
359: 		PageSize:     pageSize,
360: 		PageNumber:   workload.PageNumber,
361: 		Offset:       workload.DerivedOffset(r.config.PageSize),
362: 		PreferHot:    workload.PreferHot,
363: 		ResultCount:  len(records),
364: 		TotalRecords: int64(result.TotalRecords),
365: 		Duration:     time.Since(start),
366: 		Passed:       true,
367: 		RowIDs:       persistentRecordIDs(records),
368: 		PlanNotes:    []string{"entity_manager_query_service"},
369: 	}
370: 	return run, records, nil
371: }
372: 
373: func executeServiceQuery(ctx context.Context, h *federated.FederatedTestHarness, req *forma.QueryRequest, defaultPageSize int) (*forma.QueryResult, []*internal.PersistentRecord, error) {
374: 	if h == nil || h.PGDSN == "" {
375: 		return nil, nil, fmt.Errorf("benchmark harness postgres DSN is required")
376: 	}
377: 	pool, err := pgxpool.New(ctx, h.PGDSN)
378: 	if err != nil {
379: 		return nil, nil, fmt.Errorf("connect benchmark pgx pool: %w", err)
380: 	}
381: 	defer pool.Close()
382: 
383: 	if err := RegisterFixtureSchemas(h); err != nil {
384: 		return nil, nil, fmt.Errorf("register fixture schemas: %w", err)
385: 	}
386: 	schemaTable, err := ensureBenchmarkSchemaRegistry(ctx, pool)
387: 	if err != nil {
388: 		return nil, nil, fmt.Errorf("prepare benchmark schema registry: %w", err)
389: 	}
390: 
391: 	registry, err := internal.NewFileSchemaRegistry(pool, schemaTable, FixturesDir())
392: 	if err != nil {
393: 		return nil, nil, fmt.Errorf("build benchmark schema registry: %w", err)
394: 	}
395: 	metadata, err := internal.NewMetadataLoader(pool, schemaTable, FixturesDir()).LoadMetadata(ctx)
396: 	if err != nil {
397: 		return nil, nil, fmt.Errorf("load benchmark metadata: %w", err)
398: 	}
399: 
400: 	duckCfg := forma.DuckDBConfig{}
401: 	if h.Duck != nil {
402: 		duckCfg = forma.DuckDBConfig{
403: 			Enabled:        true,
404: 			DBPath:         ":memory:",
405: 			EnableS3:       true,
406: 			EnableParquet:  true,
407: 			S3Endpoint:     h.S3Endpoint,
408: 			S3AccessKey:    h.S3AccessKey,
409: 			S3SecretKey:    h.S3SecretKey,
410: 			S3Region:       h.S3Region,
411: 			MaxConnections: 4,
412: 			QueryTimeout:   60 * time.Second,
413: 			MaxParallelism: 4,
414: 		}
415: 	}
416: 
417: 	baseRepo := internal.NewDBPersistentRecordRepository(pool, metadata, h.Duck, duckCfg)
418: 	repo := newBenchmarkServiceRepository(baseRepo, h)
419: 	config := &forma.Config{
420: 		Database: forma.DatabaseConfig{
421: 			TableNames: forma.TableNames{
422: 				SchemaRegistry: schemaTable,
423: 				EntityMain:     h.CDCConfig.EntityMainTable,
424: 				EAVData:        h.CDCConfig.EAVDataTable,
425: 				ChangeLog:      h.CDCConfig.ChangeLogTable,
426: 			},
427: 		},
428: 		Query: forma.QueryConfig{
429: 			DefaultPageSize: benchmarkDefaultPageSize(defaultPageSize),
430: 			MaxPageSize:     maxInt(defaultPageSize, 1000),
431: 		},
432: 		Entity: forma.EntityConfig{
433: 			SchemaDirectory: FixturesDir(),
434: 		},
435: 		DuckDB: duckCfg,
436: 	}
437: 	transformer := internal.NewPersistentRecordTransformer(registry)
438: 	manager := internal.NewEntityManager(transformer, repo, registry, config)
439: 	if req != nil && req.Federated != nil && req.Federated.Enabled {
440: 		req.Federated.S3ParquetPathTemplate = benchmarkS3ParquetPathTemplate(h)
441: 	}
442: 	result, err := manager.Query(ctx, req)
443: 	if err != nil {
444: 		return nil, nil, err
445: 	}
446: 	records, err := persistentRecordsForQueryResult(ctx, result, registry)
447: 	if err != nil {
448: 		return nil, nil, err
449: 	}
450: 	return result, records, nil
451: }
452: 
453: func ensureBenchmarkSchemaRegistry(ctx context.Context, pool *pgxpool.Pool) (string, error) {
454: 	const tableName = "benchmark_schema_registry"
455: 	if _, err := pool.Exec(ctx, `
456: 		CREATE TABLE IF NOT EXISTS benchmark_schema_registry (
457: 			schema_id SMALLINT PRIMARY KEY,
458: 			schema_name TEXT NOT NULL UNIQUE,
459: 			created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW()) * 1000
460: 		)`); err != nil {
461: 		return "", err
462: 	}
463: 	if _, err := pool.Exec(ctx, `DELETE FROM benchmark_schema_registry`); err != nil {
464: 		return "", err
465: 	}
466: 	for _, fixture := range DefaultSchemaFixtures() {
467: 		if _, err := pool.Exec(ctx, `INSERT INTO benchmark_schema_registry (schema_id, schema_name) VALUES ($1, $2)`, fixture.ID, fixture.Name); err != nil {
468: 			return "", err
469: 		}
470: 	}
471: 	return tableName, nil
472: }
473: 
474: func queryRequestForWorkload(workload WorkloadDefinition, defaultPageSize int) (*forma.QueryRequest, int) {
475: 	pageSize := workload.PageSize
476: 	if pageSize <= 0 {
477: 		pageSize = defaultPageSize
478: 	}
479: 	req := &forma.QueryRequest{
480: 		SchemaName:   workload.TargetSchema,
481: 		Page:         maxInt(workload.PageNumber, 1),
482: 		ItemsPerPage: pageSize,
483: 	}
484: 	if workload.ExecutionSource == "service" {
485: 		req.Federated = &forma.FederatedQueryRequest{
486: 			Enabled:               true,
487: 			PreferredTiers:        []string{"hot", "warm", "cold"},
488: 		}
489: 	}
490: 	if workload.TargetSchema == "trade" {
491: 		req.SortBy = []string{"tradeTime"}
492: 		req.SortOrder = forma.SortOrderDesc
493: 	}
494: 	if cond := conditionForWorkload(workload); cond != nil {
495: 		req.Condition = cond
496: 	}
497: 	return req, pageSize
498: }
499: 
500: func benchmarkS3ParquetPathTemplate(h *federated.FederatedTestHarness) string {
501: 	if h == nil {
502: 		return ""
503: 	}
504: 	return fmt.Sprintf("s3://%s/%s/{{.SchemaID}}/base/*.parquet, s3://%s/%s/{{.SchemaID}}/delta/*.parquet", h.S3Bucket, h.S3Prefix, h.S3Bucket, h.S3Prefix)
505: }
506: 
507: func conditionForWorkload(workload WorkloadDefinition) forma.Condition {
508: 	conditions := make([]forma.Condition, 0, len(workload.ResolvedFilterConditions())+1)
509: 	for key, value := range workload.ResolvedFilterConditions() {
510: 		conditions = append(conditions, &forma.KvCondition{Attr: key, Value: fmt.Sprintf("equals:%v", value)})
511: 	}
512: 	if len(conditions) == 0 {
513: 		return nil
514: 	}
515: 	if len(conditions) == 1 {
516: 		return conditions[0]
517: 	}
518: 	return &forma.CompositeCondition{Logic: forma.LogicAnd, Conditions: conditions}
519: }
520: 
521: func persistentRecordsForQueryResult(ctx context.Context, result *forma.QueryResult, registry forma.SchemaRegistry) ([]*internal.PersistentRecord, error) {
522: 	if result == nil {
523: 		return nil, nil
524: 	}
525: 	transformer := internal.NewPersistentRecordTransformer(registry)
526: 	records := make([]*internal.PersistentRecord, 0, len(result.Data))
527: 	for _, data := range result.Data {
528: 		if data == nil {
529: 			continue
530: 		}
531: 		schemaID, _, err := registry.GetSchemaAttributeCacheByName(data.SchemaName)
532: 		if err != nil {
533: 			return nil, fmt.Errorf("resolve schema %s: %w", data.SchemaName, err)
534: 		}
535: 		record, err := transformer.ToPersistentRecord(ctx, schemaID, data.RowID, data.Attributes)
536: 		if err != nil {
537: 			return nil, fmt.Errorf("rebuild persistent record %s: %w", data.RowID, err)
538: 		}
539: 		if record != nil {
540: 			records = append(records, record)
541: 		}
542: 	}
543: 	return records, nil
544: }
545: 
546: type benchmarkServiceRepository struct {
547: 	*internal.DBPersistentRecordRepository
548: 	harness *federated.FederatedTestHarness
549: }
550: 
551: func newBenchmarkServiceRepository(base *internal.DBPersistentRecordRepository, h *federated.FederatedTestHarness) *benchmarkServiceRepository {
552: 	return &benchmarkServiceRepository{DBPersistentRecordRepository: base, harness: h}
553: }
554: 
555: func (r *benchmarkServiceRepository) QueryPersistentRecordsFederated(ctx context.Context, tables internal.StorageTables, fq *internal.FederatedAttributeQuery, opts *internal.FederatedQueryOptions) (*internal.PersistentRecordPage, error) {
556: 	if fq == nil || r == nil || r.harness == nil {
557: 		return r.DBPersistentRecordRepository.QueryPersistentRecordsFederated(ctx, tables, fq, opts)
558: 	}
559: 	queryOpts := benchmarkQueryOptionsFromFederatedQuery(fq)
560: 	result, err := r.harness.ExecuteFederatedQuery(ctx, queryOpts)
561: 	if err != nil {
562: 		return nil, err
563: 	}
564: 	limit := fq.Limit
565: 	if limit <= 0 {
566: 		limit = benchmarkDefaultPageSize(0)
567: 	}
568: 	currentPage := 1
569: 	if limit > 0 {
570: 		currentPage = fq.Offset/limit + 1
571: 	}
572: 	return &internal.PersistentRecordPage{
573: 		Records:      result.Records,
574: 		TotalRecords: result.TotalRecords,
575: 		TotalPages:   benchmarkComputeTotalPages(result.TotalRecords, limit),
576: 		CurrentPage:  currentPage,
577: 	}, nil
578: }
579: 
580: func benchmarkQueryOptionsFromFederatedQuery(fq *internal.FederatedAttributeQuery) *federated.QueryOptions {
581: 	if fq == nil {
582: 		return &federated.QueryOptions{}
583: 	}
584: 	queryOpts := &federated.QueryOptions{
585: 		Limit:     fq.Limit,
586: 		Offset:    fq.Offset,
587: 		PreferHot: fq.PreferHot,
588: 	}
589: 	if len(fq.AttributeOrders) > 0 {
590: 		order := fq.AttributeOrders[0]
591: 		queryOpts.SortDesc = order.SortOrder == forma.SortOrderDesc
592: 		if name := benchmarkSortAttributeName(order); name != "" {
593: 			queryOpts.SortBy = name
594: 		}
595: 	}
596: 	if fq.Condition != nil {
597: 		queryOpts.Filter = &federated.Filter{Conditions: benchmarkFilterConditionsFromCondition(fq.Condition)}
598: 	}
599: 	return queryOpts
600: }
601: 
602: func benchmarkSortAttributeName(order internal.AttributeOrder) string {
603: 	switch order.AttrID {
604: 	case 1:
605: 		return "symbol"
606: 	case 2:
607: 		return "tradeType"
608: 	case 5:
609: 		return "tradeTime"
610: 	case 7:
611: 		return "region"
612: 	default:
613: 		return ""
614: 	}
615: }
616: 
617: func benchmarkFilterConditionsFromCondition(condition forma.Condition) map[string]any {
618: 	conditions := make(map[string]any)
619: 	collectBenchmarkFilterConditions(condition, conditions)
620: 	if len(conditions) == 0 {
621: 		return nil
622: 	}
623: 	return conditions
624: }
625: 
626: func collectBenchmarkFilterConditions(condition forma.Condition, out map[string]any) {
627: 	if condition == nil {
628: 		return
629: 	}
630: 	switch c := condition.(type) {
631: 	case *forma.KvCondition:
632: 		value := fmt.Sprint(c.Value)
633: 		if strings.HasPrefix(value, "equals:") {
634: 			out[c.Attr] = strings.TrimPrefix(value, "equals:")
635: 			return
636: 		}
637: 		out[c.Attr] = value
638: 	case *forma.CompositeCondition:
639: 		for _, child := range c.Conditions {
640: 			collectBenchmarkFilterConditions(child, out)
641: 		}
642: 	}
643: }
644: 
645: func benchmarkComputeTotalPages(total int64, limit int) int {
646: 	if total == 0 || limit <= 0 {
647: 		return 0
648: 	}
649: 	return int((total + int64(limit) - 1) / int64(limit))
650: }
651: 
652: func benchmarkDefaultPageSize(pageSize int) int {
653: 	if pageSize > 0 {
654: 		return pageSize
655: 	}
656: 	return 20
657: }
658: 
659: func failedWorkloadRunResult(workload WorkloadDefinition, distribution Distribution, defaultPageSize int, infraError string) WorkloadRunResult {
660: 	pageSize := workload.PageSize
661: 	if pageSize <= 0 {
662: 		pageSize = defaultPageSize
663: 	}
664: 	return WorkloadRunResult{
665: 		Name:         workload.Name,
666: 		Category:     string(workload.Category),
667: 		Distribution: distribution,
668: 		PageSize:     pageSize,
669: 		PageNumber:   workload.PageNumber,
670: 		Offset:       workload.DerivedOffset(defaultPageSize),
671: 		PreferHot:    workload.PreferHot,
672: 		Passed:       false,
673: 		FailureKind:  FailureKindInfra,
674: 		FailureCount: 1,
675: 		InfraError:   infraError,
676: 	}
677: }
678: 
679: type expectedWorkloadResult struct {
680: 	TotalRecords int64
681: 	RowIDs       []string
682: }
683: 
684: type workloadSemantics struct {
685: 	TradeTimeStart int64
686: 	TradeTimeEnd   int64
687: }
688: 
689: func buildExpectedWorkloadResults(dataset *GeneratedDataset, workloads []WorkloadDefinition, defaultPageSize int) map[string]expectedWorkloadResult {
690: 	if dataset == nil {
691: 		return map[string]expectedWorkloadResult{}
692: 	}
693: 	return buildExpectedWorkloadResultsFromRecords(dataset.Records, workloads, defaultPageSize, dataset.Config)
694: }
695: 
696: func buildExpectedWorkloadResultsFromRecords(records []GeneratedRecord, workloads []WorkloadDefinition, defaultPageSize int, genCfg GeneratorConfig) map[string]expectedWorkloadResult {
697: 	results := make(map[string]expectedWorkloadResult, len(workloads))
698: 	visible := expectedVisibleRecords(records)
699: 	for _, workload := range workloads {
700: 		matching := filterExpectedRecordsForWorkload(visible, workload, semanticsForWorkload(workload, genCfg))
701: 		sortExpectedRecordsForWorkload(matching, workload)
702: 		pageSize := workload.PageSize
703: 		if pageSize <= 0 {
704: 			pageSize = defaultPageSize
705: 		}
706: 		offset := workload.DerivedOffset(defaultPageSize)
707: 		rowIDs := expectedPageRowIDs(matching, offset, pageSize)
708: 		results[workload.Name] = expectedWorkloadResult{TotalRecords: int64(len(matching)), RowIDs: rowIDs}
709: 	}
710: 	return results
711: }
712: 
713: func buildLoadedStateSnapshot(ctx context.Context, h *federated.FederatedTestHarness, dataset *TieredDataset) ([]GeneratedRecord, error) {
714: 	if h == nil || dataset == nil {
715: 		return nil, fmt.Errorf("harness and dataset are required")
716: 	}
717: 	hotRecords, hotKeys, err := loadHotStateRecords(ctx, h)
718: 	if err != nil {
719: 		return nil, err
720: 	}
721: 	records := make([]GeneratedRecord, 0, len(dataset.Base)+len(dataset.Delta)+len(hotRecords))
722: 	for _, bucket := range [][]GeneratedRecord{dataset.Base, dataset.Delta} {
723: 		for _, record := range bucket {
724: 			if _, ok := hotKeys[schemaRowKey(record.SchemaID, record.RowID)]; ok {
725: 				continue
726: 			}
727: 			records = append(records, cloneGeneratedRecord(record))
728: 		}
729: 	}
730: 	records = append(records, hotRecords...)
731: 	return records, nil
732: }
733: 
734: func loadHotStateRecords(ctx context.Context, h *federated.FederatedTestHarness) ([]GeneratedRecord, map[string]struct{}, error) {
735: 	rows, err := h.PGDB.QueryContext(ctx, `
736: 		SELECT cl.schema_id, cl.row_id, cl.changed_at, COALESCE(cl.deleted_at, 0),
737: 			em.text_01, em.text_02, em.smallint_01, em.bigint_02,
738: 			hot_vals.symbol, hot_vals.exchange, hot_vals.region, hot_vals.trade_type, hot_vals.trade_time, hot_vals.name
739: 		FROM change_log cl
740: 		LEFT JOIN entity_main em
741: 			ON em.ltbase_schema_id = cl.schema_id AND em.ltbase_row_id = cl.row_id
742: 		LEFT JOIN (
743: 			SELECT schema_id, row_id,
744: 				MAX(CASE WHEN attr_id = $1 THEN value_text END) AS symbol,
745: 				MAX(CASE WHEN attr_id = $2 THEN value_text END) AS exchange,
746: 				MAX(CASE WHEN attr_id = $3 THEN value_text END) AS region,
747: 				MAX(CASE WHEN attr_id = $4 THEN value_numeric END) AS trade_type,
748: 				MAX(CASE WHEN attr_id = $5 THEN COALESCE(value_text, CAST(value_numeric AS TEXT)) END) AS trade_time,
749: 				MAX(CASE WHEN attr_id = $6 THEN value_text END) AS name
750: 			FROM eav_data
751: 			GROUP BY schema_id, row_id
752: 		) hot_vals ON hot_vals.schema_id = cl.schema_id AND hot_vals.row_id = cl.row_id
753: 		WHERE cl.flushed_at = 0
754: 	`,
755: 		benchmarkAttributeID(SchemaIDTrade, "symbol"),
756: 		benchmarkAttributeID(SchemaIDTrade, "exchange"),
757: 		benchmarkAttributeID(SchemaIDTrade, "region"),
758: 		benchmarkAttributeID(SchemaIDTrade, "tradeType"),
759: 		benchmarkAttributeID(SchemaIDTrade, "tradeTime"),
760: 		benchmarkAttributeID(SchemaIDTrade, "name"),
761: 	)
762: 	if err != nil {
763: 		return nil, nil, fmt.Errorf("load hot state snapshot: %w", err)
764: 	}
765: 	defer rows.Close()
766: 	records := make([]GeneratedRecord, 0)
767: 	keys := make(map[string]struct{})
768: 	for rows.Next() {
769: 		record, err := scanLoadedHotRecord(rows)
770: 		if err != nil {
771: 			return nil, nil, err
772: 		}
773: 		records = append(records, record)
774: 		keys[schemaRowKey(record.SchemaID, record.RowID)] = struct{}{}
775: 	}
776: 	if err := rows.Err(); err != nil {
777: 		return nil, nil, fmt.Errorf("iterate hot state snapshot: %w", err)
778: 	}
779: 	return records, keys, nil
780: }
781: 
782: func scanLoadedHotRecord(rows *sql.Rows) (GeneratedRecord, error) {
783: 	var schemaID int16
784: 	var rowID uuid.UUID
785: 	var changedAt int64
786: 	var deletedAt int64
787: 	var text01 sql.NullString
788: 	var text02 sql.NullString
789: 	var smallint01 sql.NullInt16
790: 	var bigint02 sql.NullInt64
791: 	var symbol sql.NullString
792: 	var exchange sql.NullString
793: 	var region sql.NullString
794: 	var tradeType sql.NullFloat64
795: 	var tradeTime sql.NullString
796: 	var name sql.NullString
797: 	if err := rows.Scan(&schemaID, &rowID, &changedAt, &deletedAt, &text01, &text02, &smallint01, &bigint02, &symbol, &exchange, &region, &tradeType, &tradeTime, &name); err != nil {
798: 		return GeneratedRecord{}, fmt.Errorf("scan hot state row: %w", err)
799: 	}
800: 	attrs := make(map[string]any)
801: 	schemaName, err := schemaNameForID(schemaID)
802: 	if err != nil {
803: 		return GeneratedRecord{}, err
804: 	}
805: 	switch schemaID {
806: 	case SchemaIDTrade:
807: 		if symbol.Valid {
808: 			attrs["symbol"] = symbol.String
809: 		} else if text01.Valid {
810: 			attrs["symbol"] = text01.String
811: 		}
812: 		if exchange.Valid {
813: 			attrs["exchange"] = exchange.String
814: 		}
815: 		if region.Valid {
816: 			attrs["region"] = region.String
817: 		} else if text02.Valid {
818: 			attrs["region"] = text02.String
819: 		}
820: 		if tradeType.Valid {
821: 			attrs["tradeType"] = int64(tradeType.Float64)
822: 		} else if smallint01.Valid {
823: 			attrs["tradeType"] = int64(smallint01.Int16)
824: 		}
825: 		if tradeTime.Valid {
826: 			attrs["tradeTime"] = tradeTime.String
827: 		} else if bigint02.Valid {
828: 			attrs["tradeTime"] = strconv.FormatInt(bigint02.Int64, 10)
829: 		}
830: 		if name.Valid {
831: 			attrs["name"] = name.String
832: 		} else if symbol.Valid {
833: 			attrs["name"] = symbol.String
834: 		}
835: 	case SchemaIDCustomer:
836: 		if text02.Valid {
837: 			attrs["region"] = text02.String
838: 		}
839: 		if name.Valid {
840: 			attrs["name"] = name.String
841: 		} else if text01.Valid {
842: 			attrs["name"] = text01.String
843: 		}
844: 	case SchemaIDSecurity:
845: 		if symbol.Valid {
846: 			attrs["symbol"] = symbol.String
847: 		} else if text01.Valid {
848: 			attrs["symbol"] = text01.String
849: 		}
850: 		if name.Valid {
851: 			attrs["companyName"] = name.String
852: 		}
853: 	}
854: 	return GeneratedRecord{SchemaID: schemaID, SchemaName: schemaName, RowID: rowID, Version: 0, ChangedAt: changedAt, DeletedAt: deletedAt, Attributes: attrs}, nil
855: }
856: 
857: func schemaNameForID(schemaID int16) (string, error) {
858: 	for _, fixture := range DefaultSchemaFixtures() {
859: 		if fixture.ID == schemaID {
860: 			return fixture.Name, nil
861: 		}
862: 	}
863: 	return "", fmt.Errorf("unknown benchmark schema id %d", schemaID)
864: }
865: 
866: func buildExpectedWorkloadResultFromFederatedTruth(ctx context.Context, h *federated.FederatedTestHarness, workload WorkloadDefinition, defaultPageSize int, loadedRecords []GeneratedRecord, genCfg GeneratorConfig) (expectedWorkloadResult, error) {
867: 	if h == nil {
868: 		return expectedWorkloadResult{}, fmt.Errorf("harness cannot be nil")
869: 	}
870: 	semantics := semanticsForWorkload(workload, genCfg)
871: 	candidates := filterExpectedRecordsForWorkload(expectedVisibleRecords(loadedRecords), workload, semantics)
872: 	sortExpectedRecordsForWorkload(candidates, workload)
873: 	matching := make([]GeneratedRecord, 0, len(candidates))
874: 	pageSize := workload.PageSize
875: 	if pageSize <= 0 {
876: 		pageSize = defaultPageSize
877: 	}
878: 	previousSchemaID := h.SchemaID
879: 	schemaID, err := workloadSchemaID(workload.TargetSchema)
880: 	if err != nil {
881: 		return expectedWorkloadResult{}, err
882: 	}
883: 	h.SchemaID = schemaID
884: 	defer func() {
885: 		h.SchemaID = previousSchemaID
886: 	}()
887: 	for _, candidate := range candidates {
888: 		conditions := workload.ResolvedFilterConditions()
889: 		result, err := h.ExecuteFederatedQuery(ctx, &federated.QueryOptions{
890: 			Limit: 1,
891: 			Filter: &federated.Filter{
892: 				RowID:      candidate.RowID,
893: 				Conditions: conditions,
894: 			},
895: 			SortBy:   "tradeTime",
896: 			SortDesc: true,
897: 		})
898: 		if err != nil {
899: 			return expectedWorkloadResult{}, err
900: 		}
901: 		if result.TotalRecords > 0 {
902: 			matching = append(matching, candidate)
903: 		}
904: 	}
905: 	rowIDs := expectedPageRowIDs(matching, workload.DerivedOffset(defaultPageSize), pageSize)
906: 	return expectedWorkloadResult{TotalRecords: int64(len(matching)), RowIDs: rowIDs}, nil
907: }
908: 
909: func expectedVisibleRecords(records []GeneratedRecord) []GeneratedRecord {
910: 	latest := make(map[string]GeneratedRecord)
911: 	for _, record := range records {
912: 		key := schemaRowKey(record.SchemaID, record.RowID)
913: 		existing, ok := latest[key]
914: 		if ok && !recordWinsExpectedRecord(record, existing) {
915: 			continue
916: 		}
917: 		latest[key] = cloneGeneratedRecord(record)
918: 	}
919: 	out := make([]GeneratedRecord, 0, len(latest))
920: 	for _, record := range latest {
921: 		if record.DeletedAt > 0 {
922: 			continue
923: 		}
924: 		out = append(out, cloneGeneratedRecord(record))
925: 	}
926: 	return out
927: }
928: 
929: func recordWinsExpectedRecord(candidate, current GeneratedRecord) bool {
930: 	if candidate.ChangedAt != current.ChangedAt {
931: 		return candidate.ChangedAt > current.ChangedAt
932: 	}
933: 	if candidate.Version != current.Version {
934: 		return candidate.Version > current.Version
935: 	}
936: 	if candidate.DeletedAt != current.DeletedAt {
937: 		return candidate.DeletedAt > current.DeletedAt
938: 	}
939: 	return false
940: }
941: 
942: func filterExpectedRecordsForWorkload(records []GeneratedRecord, workload WorkloadDefinition, semantics workloadSemantics) []GeneratedRecord {
943: 	out := make([]GeneratedRecord, 0)
944: 	for _, record := range records {
945: 		if record.SchemaName != workload.TargetSchema {
946: 			continue
947: 		}
948: 		if semantics.TradeTimeStart > 0 || semantics.TradeTimeEnd > 0 {
949: 			tradeTime := generatedRecordTradeTime(record)
950: 			if semantics.TradeTimeStart > 0 && tradeTime < semantics.TradeTimeStart {
951: 				continue
952: 			}
953: 			if semantics.TradeTimeEnd > 0 && tradeTime > semantics.TradeTimeEnd {
954: 				continue
955: 			}
956: 		}
957: 		if len(workload.ResolvedFilterConditions()) > 0 && !generatedRecordMatchesFilterForWorkload(record, workload) {
958: 			continue
959: 		}
960: 		out = append(out, cloneGeneratedRecord(record))
961: 	}
962: 	return out
963: }
964: 
965: func semanticsForWorkload(workload WorkloadDefinition, genCfg GeneratorConfig) workloadSemantics {
966: 	if workload.TargetSchema != "trade" {
967: 		return workloadSemantics{}
968: 	}
969: 	baseTime := genCfg.BaseTime
970: 	if baseTime.IsZero() {
971: 		baseTime = defaultBaseTime
972: 	}
973: 	windowDays := genCfg.TimeWindowDays
974: 	if windowDays <= 0 {
975: 		windowDays = DefaultGeneratorConfig().TimeWindowDays
976: 	}
977: 	windowMillis := int64(windowDays) * 24 * int64(time.Hour/time.Millisecond)
978: 	var start, end int64
979: 	switch workload.Name {
980: 	case "mixed-tier-window":
981: 		start = baseTime.UnixMilli() - (windowMillis * 4 / 5)
982: 		end = baseTime.UnixMilli() - windowMillis/5
983: 	case "hot-only-window":
984: 		start = baseTime.UnixMilli() - windowMillis/5
985: 		end = baseTime.UnixMilli()
986: 	case "cold-only-window":
987: 		start = baseTime.UnixMilli() - windowMillis
988: 		end = baseTime.UnixMilli() - (windowMillis * 4 / 5)
989: 	default:
990: 		return workloadSemantics{}
991: 	}
992: 	if start > end {
993: 		start, end = end, start
994: 	}
995: 	return workloadSemantics{TradeTimeStart: start, TradeTimeEnd: end}
996: }
997: 
998: func benchmarkAttributeID(schemaID int16, name string) int {
999: 	hash := uint32(2166136261)
1000: 	input := fmt.Sprintf("%d:%s", schemaID, name)
1001: 	for i := 0; i < len(input); i++ {
1002: 		hash ^= uint32(input[i])
1003: 		hash *= 16777619
1004: 	}
1005: 	return int(hash%30000) + 1
1006: }
1007: 
1008: func generatedRecordMatchesFilterForWorkload(record GeneratedRecord, workload WorkloadDefinition) bool {
1009: 	for key, expected := range workload.ResolvedFilterConditions() {
1010: 		value, ok := benchmarkVisibleAttributeValue(record, key)
1011: 		if !ok {
1012: 			return false
1013: 		}
1014: 		if key == "tradeType" {
1015: 			if fmt.Sprintf("%v", value) != fmt.Sprintf("%v", expected) {
1016: 				return false
1017: 			}
1018: 			continue
1019: 		}
1020: 		if fmt.Sprint(value) != fmt.Sprint(expected) {
1021: 			return false
1022: 		}
1023: 	}
1024: 	return true
1025: }
1026: 
1027: func benchmarkVisibleAttributeValue(record GeneratedRecord, attribute string) (any, bool) {
1028: 	if value, ok := record.Attributes[attribute]; ok {
1029: 		return value, true
1030: 	}
1031: 	if record.SchemaName != "trade" {
1032: 		return nil, false
1033: 	}
1034: 	switch attribute {
1035: 	case "symbol", "name":
1036: 		value, ok := record.Attributes["symbol"]
1037: 		return value, ok
1038: 	case "tradeType":
1039: 		value, ok := record.Attributes["tradeType"]
1040: 		return value, ok
1041: 	case "tradeTime":
1042: 		value, ok := record.Attributes["tradeTime"]
1043: 		return value, ok
1044: 	default:
1045: 		return nil, false
1046: 	}
1047: }
1048: 
1049: func sortExpectedRecordsForWorkload(records []GeneratedRecord, workload WorkloadDefinition) {
1050: 	if workload.TargetSchema == "trade" {
1051: 		sort.Slice(records, func(i, j int) bool {
1052: 			left := generatedRecordTradeTime(records[i])
1053: 			right := generatedRecordTradeTime(records[j])
1054: 			if left != right {
1055: 				return left > right
1056: 			}
1057: 			return records[i].RowID.String() < records[j].RowID.String()
1058: 		})
1059: 		return
1060: 	}
1061: 	sort.Slice(records, func(i, j int) bool {
1062: 		return records[i].RowID.String() < records[j].RowID.String()
1063: 	})
1064: }
1065: 
1066: func generatedRecordTradeTime(record GeneratedRecord) int64 {
1067: 	value, ok := record.Attributes["tradeTime"]
1068: 	if !ok {
1069: 		return 0
1070: 	}
1071: 	switch v := value.(type) {
1072: 	case int64:
1073: 		return v
1074: 	case int:
1075: 		return int64(v)
1076: 	case string:
1077: 		parsed, err := time.Parse(time.RFC3339, v)
1078: 		if err == nil {
1079: 			return parsed.UnixMilli()
1080: 		}
1081: 		if unixMillis, convErr := strconv.ParseInt(v, 10, 64); convErr == nil {
1082: 			return unixMillis
1083: 		}
1084: 	}
1085: 	return 0
1086: }
1087: 
1088: func expectedPageRowIDs(records []GeneratedRecord, offset, pageSize int) []string {
1089: 	if offset >= len(records) {
1090: 		return nil
1091: 	}
1092: 	end := offset + pageSize
1093: 	if end > len(records) {
1094: 		end = len(records)
1095: 	}
1096: 	rowIDs := make([]string, 0, end-offset)
1097: 	for _, record := range records[offset:end] {
1098: 		rowIDs = append(rowIDs, record.RowID.String())
1099: 	}
1100: 	return rowIDs
1101: }
1102: 
1103: func validateExpectedWorkloadOutcome(run WorkloadRunResult, expected expectedWorkloadResult) []AssertionResult {
1104: 	assertions := []AssertionResult{{
1105: 		Name:    "total-records-match-expected",
1106: 		Passed:  run.TotalRecords == expected.TotalRecords,
1107: 		Message: fmt.Sprintf("actual=%d expected=%d", run.TotalRecords, expected.TotalRecords),
1108: 	}}
1109: 	actualRows := append([]string(nil), run.RowIDs...)
1110: 	expectedRows := append([]string(nil), expected.RowIDs...)
1111: 	assertions = append(assertions, AssertionResult{
1112: 		Name:    "page-row-ids-match-expected",
1113: 		Passed:  stringSlicesEqual(actualRows, expectedRows),
1114: 		Message: fmt.Sprintf("actual=%v expected=%v", actualRows, expectedRows),
1115: 	})
1116: 	return assertions
1117: }
1118: 
1119: func stringSlicesEqual(left, right []string) bool {
1120: 	if len(left) != len(right) {
1121: 		return false
1122: 	}
1123: 	for i := range left {
1124: 		if left[i] != right[i] {
1125: 			return false
1126: 		}
1127: 	}
1128: 	return true
1129: }
1130: 
1131: func failureKindForRun(run WorkloadRunResult) string {
1132: 	if run.InfraError != "" {
1133: 		return FailureKindInfra
1134: 	}
1135: 	if countFailedAssertions(run.Assertions) > 0 {
1136: 		return FailureKindCorrectness
1137: 	}
1138: 	return ""
1139: }
1140: 
1141: func validateBasicWorkloadAssertions(workload WorkloadDefinition, run WorkloadRunResult) []AssertionResult {
1142: 	assertions := []AssertionResult{
1143: 		{
1144: 			Name:    "non-negative-total-records",
1145: 			Passed:  run.TotalRecords >= 0,
1146: 			Message: fmt.Sprintf("total_records=%d", run.TotalRecords),
1147: 		},
1148: 		{
1149: 			Name:    "page-size-bound",
1150: 			Passed:  run.ResultCount <= run.PageSize,
1151: 			Message: fmt.Sprintf("result_count=%d page_size=%d", run.ResultCount, run.PageSize),
1152: 		},
1153: 		{
1154: 			Name:    "result-count-within-total-records",
1155: 			Passed:  int64(run.ResultCount) <= run.TotalRecords,
1156: 			Message: fmt.Sprintf("result_count=%d total_records=%d", run.ResultCount, run.TotalRecords),
1157: 		},
1158: 		{
1159: 			Name:    "empty-page-only-when-offset-reaches-total",
1160: 			Passed:  run.ResultCount > 0 || run.Offset >= int(run.TotalRecords) || run.TotalRecords == 0,
1161: 			Message: fmt.Sprintf("offset=%d total=%d result_count=%d", run.Offset, run.TotalRecords, run.ResultCount),
1162: 		},
1163: 	}
1164: 	if workload.Category == WorkloadCategoryDeepPage {
1165: 		assertions = append(assertions, AssertionResult{
1166: 			Name:    "deep-page-empty-when-offset-exceeds-total",
1167: 			Passed:  run.Offset < int(run.TotalRecords) || run.ResultCount == 0,
1168: 			Message: fmt.Sprintf("offset=%d total=%d result_count=%d", run.Offset, run.TotalRecords, run.ResultCount),
1169: 		})
1170: 	}
1171: 	return assertions
1172: }
1173: 
1174: func validatePaginationTransition(previous, current WorkloadRunResult) []AssertionResult {
1175: 	assertions := []AssertionResult{{
1176: 		Name:    "non-decreasing-offsets-across-pagination-runs",
1177: 		Passed:  current.Offset >= previous.Offset,
1178: 		Message: fmt.Sprintf("previous_offset=%d current_offset=%d", previous.Offset, current.Offset),
1179: 	}}
1180: 	if current.Offset > previous.Offset {
1181: 		overlap := hasRowIDOverlap(previous.RowIDs, current.RowIDs)
1182: 		assertions = append(assertions, AssertionResult{
1183: 			Name:    "no-overlap-across-page-slices",
1184: 			Passed:  !overlap,
1185: 			Message: fmt.Sprintf("previous_rows=%d current_rows=%d", len(previous.RowIDs), len(current.RowIDs)),
1186: 		})
1187: 	}
1188: 	return assertions
1189: }
1190: 
1191: func validateRepeatedRunStability(previous, current WorkloadRunResult) []AssertionResult {
1192: 	assertions := []AssertionResult{{
1193: 		Name:    "repeated-run-failure-kind-stable",
1194: 		Passed:  previous.FailureKind == current.FailureKind,
1195: 		Message: fmt.Sprintf("previous=%s current=%s", previous.FailureKind, current.FailureKind),
1196: 	}}
1197: 	assertions = append(assertions, AssertionResult{
1198: 		Name:    "repeated-run-total-records-stable",
1199: 		Passed:  previous.TotalRecords == current.TotalRecords,
1200: 		Message: fmt.Sprintf("previous=%d current=%d", previous.TotalRecords, current.TotalRecords),
1201: 	})
1202: 	assertions = append(assertions, AssertionResult{
1203: 		Name:    "repeated-run-page-row-ids-stable",
1204: 		Passed:  stringSlicesEqual(previous.RowIDs, current.RowIDs),
1205: 		Message: fmt.Sprintf("previous=%v current=%v", previous.RowIDs, current.RowIDs),
1206: 	})
1207: 	return assertions
1208: }
1209: 
1210: func validateResultLevelAssertions(workload WorkloadDefinition, run WorkloadRunResult, records []*internal.PersistentRecord, semantics workloadSemantics) []AssertionResult {
1211: 	assertions := []AssertionResult{validateUniqueRows(run), validateSchemaScope(workload, records)}
1212: 	if len(workload.ResolvedFilterConditions()) > 0 {
1213: 		assertions = append(assertions, validateFilterMatch(workload, records))
1214: 	}
1215: 	if semantics.TradeTimeStart > 0 || semantics.TradeTimeEnd > 0 {
1216: 		assertions = append(assertions, validateTradeTimeWindow(records, semantics))
1217: 	}
1218: 	if workload.TargetSchema == "trade" && len(records) > 1 {
1219: 		assertions = append(assertions, validateSortOrder(records, "tradeTime", true))
1220: 	}
1221: 	return assertions
1222: }
1223: 
1224: func validateSchemaScope(workload WorkloadDefinition, records []*internal.PersistentRecord) AssertionResult {
1225: 	expectedSchemaID, err := workloadSchemaID(workload.TargetSchema)
1226: 	if err != nil {
1227: 		return AssertionResult{Name: "schema-scoped-results-match-target", Passed: false, Message: err.Error()}
1228: 	}
1229: 	for _, record := range records {
1230: 		if record.SchemaID != expectedSchemaID {
1231: 			return AssertionResult{Name: "schema-scoped-results-match-target", Passed: false, Message: fmt.Sprintf("expected_schema=%d actual_schema=%d row=%s", expectedSchemaID, record.SchemaID, record.RowID)}
1232: 		}
1233: 	}
1234: 	return AssertionResult{Name: "schema-scoped-results-match-target", Passed: true, Message: fmt.Sprintf("schema=%s rows=%d", workload.TargetSchema, len(records))}
1235: }
1236: 
1237: func queryOptionsForWorkload(workload WorkloadDefinition, defaultPageSize int) *federated.QueryOptions {
1238: 	return queryOptionsForWorkloadWithConfig(workload, defaultPageSize, DefaultGeneratorConfig())
1239: }
1240: 
1241: func queryOptionsForWorkloadWithConfig(workload WorkloadDefinition, defaultPageSize int, genCfg GeneratorConfig) *federated.QueryOptions {
1242: 	pageSize := workload.PageSize
1243: 	if pageSize <= 0 {
1244: 		pageSize = defaultPageSize
1245: 	}
1246: 	opts := &federated.QueryOptions{Limit: pageSize, Offset: workload.DerivedOffset(defaultPageSize)}
1247: 	if workload.TargetSchema == "trade" {
1248: 		opts.SortBy = "tradeTime"
1249: 		opts.SortDesc = true
1250: 	}
1251: 	semantics := semanticsForWorkload(workload, genCfg)
1252: 	opts.TradeTimeStart = semantics.TradeTimeStart
1253: 	opts.TradeTimeEnd = semantics.TradeTimeEnd
1254: 	return opts
1255: }
1256: 
1257: func validateUniqueRows(run WorkloadRunResult) AssertionResult {
1258: 	seen := make(map[string]struct{}, len(run.RowIDs))
1259: 	for _, rowID := range run.RowIDs {
1260: 		if _, ok := seen[rowID]; ok {
1261: 			return AssertionResult{Name: "unique-row-ids-within-page", Passed: false, Message: rowID}
1262: 		}
1263: 		seen[rowID] = struct{}{}
1264: 	}
1265: 	return AssertionResult{Name: "unique-row-ids-within-page", Passed: true, Message: fmt.Sprintf("rows=%d", len(run.RowIDs))}
1266: }
1267: 
1268: func validateFilterMatch(workload WorkloadDefinition, records []*internal.PersistentRecord) AssertionResult {
1269: 	for _, record := range records {
1270: 		for key, expected := range workload.ResolvedFilterConditions() {
1271: 			if !recordMatchesFilter(record, key, fmt.Sprint(expected)) {
1272: 				return AssertionResult{Name: "filter-results-match-request", Passed: false, Message: fmt.Sprintf("attribute=%s expected=%v row=%s", key, expected, record.RowID)}
1273: 			}
1274: 		}
1275: 	}
1276: 	return AssertionResult{Name: "filter-results-match-request", Passed: true, Message: fmt.Sprintf("conditions=%v", workload.ResolvedFilterConditions())}
1277: }
1278: 
1279: func validateTradeTimeWindow(records []*internal.PersistentRecord, semantics workloadSemantics) AssertionResult {
1280: 	for _, record := range records {
1281: 		tradeTime, ok := record.Int64Items["tradeTime"]
1282: 		if !ok {
1283: 			return AssertionResult{Name: "tradeTime-window-match-request", Passed: false, Message: fmt.Sprintf("missing tradeTime row=%s", record.RowID)}
1284: 		}
1285: 		if semantics.TradeTimeStart > 0 && tradeTime < semantics.TradeTimeStart {
1286: 			return AssertionResult{Name: "tradeTime-window-match-request", Passed: false, Message: fmt.Sprintf("row=%s tradeTime=%d start=%d", record.RowID, tradeTime, semantics.TradeTimeStart)}
1287: 		}
1288: 		if semantics.TradeTimeEnd > 0 && tradeTime > semantics.TradeTimeEnd {
1289: 			return AssertionResult{Name: "tradeTime-window-match-request", Passed: false, Message: fmt.Sprintf("row=%s tradeTime=%d end=%d", record.RowID, tradeTime, semantics.TradeTimeEnd)}
1290: 		}
1291: 	}
1292: 	return AssertionResult{Name: "tradeTime-window-match-request", Passed: true, Message: fmt.Sprintf("rows=%d start=%d end=%d", len(records), semantics.TradeTimeStart, semantics.TradeTimeEnd)}
1293: }
1294: 
1295: func validateSortOrder(records []*internal.PersistentRecord, attribute string, desc bool) AssertionResult {
1296: 	values := make([]int64, 0, len(records))
1297: 	for _, record := range records {
1298: 		value, ok := record.Int64Items[attribute]
1299: 		if !ok {
1300: 			continue
1301: 		}
1302: 		values = append(values, value)
1303: 	}
1304: 	if len(values) <= 1 {
1305: 		return AssertionResult{Name: "sorted-by-tradeTime-desc", Passed: true, Message: "insufficient comparable rows"}
1306: 	}
1307: 	if desc {
1308: 		for i := 1; i < len(values); i++ {
1309: 			if values[i] > values[i-1] {
1310: 				return AssertionResult{Name: "sorted-by-tradeTime-desc", Passed: false, Message: fmt.Sprintf("index=%d prev=%d curr=%d", i, values[i-1], values[i])}
1311: 			}
1312: 		}
1313: 	}
1314: 	return AssertionResult{Name: "sorted-by-tradeTime-desc", Passed: true, Message: fmt.Sprintf("comparable_rows=%d", len(values))}
1315: }
1316: 
1317: func recordMatchesFilter(record *internal.PersistentRecord, attribute, expected string) bool {
1318: 	switch attribute {
1319: 	case "symbol", "exchange", "region", "name":
1320: 		return record.TextItems[attribute] == expected
1321: 	case "tradeType":
1322: 		return fmt.Sprintf("%d", record.Int64Items[attribute]) == expected
1323: 	default:
1324: 		return true
1325: 	}
1326: }
1327: 
1328: func persistentRecordIDs(records []*internal.PersistentRecord) []string {
1329: 	ids := make([]string, 0, len(records))
1330: 	for _, record := range records {
1331: 		ids = append(ids, record.RowID.String())
1332: 	}
1333: 	return ids
1334: }
1335: 
1336: func hasRowIDOverlap(a, b []string) bool {
1337: 	seen := make(map[string]struct{}, len(a))
1338: 	for _, rowID := range a {
1339: 		seen[rowID] = struct{}{}
1340: 	}
1341: 	for _, rowID := range b {
1342: 		if _, ok := seen[rowID]; ok {
1343: 			return true
1344: 		}
1345: 	}
1346: 	return false
1347: }
1348: 
1349: func countFailedAssertions(assertions []AssertionResult) int {
1350: 	failed := 0
1351: 	for _, assertion := range assertions {
1352: 		if !assertion.Passed {
1353: 			failed++
1354: 		}
1355: 	}
1356: 	return failed
1357: }
1358: 
1359: func maxInt(a, b int) int {
1360: 	if a > b {
1361: 		return a
1362: 	}
1363: 	return b
1364: }

(End of file - total 1364 lines)
</content>
```

---

## Assistant (Build · DeepSeek V4 Pro · 12.3s)

_Thinking:_

Now let me look at the DuckDB SQL template to understand how filter clauses are injected, and look at the benchmark schema definitions to understand EAV vs main column mappings for the three schemas. Let me also check the `federated_interfaces.go` to understand the query patterns, and look at the calibration file to understand selectivity tuning.

**Tool: glob**

**Input:**
```json
{
  "pattern": "**/*.sql"
}
```

**Output:**
```
No files found
```

**Tool: read**

**Input:**
```json
{
  "filePath": "/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/calibration.go"
}
```

**Output:**
```
<path>/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/calibration.go</path>
<type>file</type>
<content>
1: package benchmark
2: 
3: import (
4: 	"fmt"
5: 	"math"
6: 	"sort"
7: )
8: 
9: // SelectivityBand classifies a filter candidate by match ratio.
10: type SelectivityBand string
11: 
12: const (
13: 	SelectivityBandHigh   SelectivityBand = "high"
14: 	SelectivityBandMedium SelectivityBand = "medium"
15: 	SelectivityBandLow    SelectivityBand = "low"
16: )
17: 
18: // CalibrationCandidate represents an attribute/value pair suitable for a filter benchmark.
19: type CalibrationCandidate struct {
20: 	Attribute string          `json:"attribute"`
21: 	Value     string          `json:"value"`
22: 	Matches   int             `json:"matches"`
23: 	Ratio     float64         `json:"ratio"`
24: 	Band      SelectivityBand `json:"band"`
25: }
26: 
27: // TradeCalibration groups selectivity candidates for trade workloads.
28: type TradeCalibration struct {
29: 	High   CalibrationCandidate `json:"high"`
30: 	Medium CalibrationCandidate `json:"medium"`
31: 	Low    CalibrationCandidate `json:"low"`
32: }
33: 
34: // CalibrateTradeFilters computes representative trade filter values for the three selectivity bands.
35: func CalibrateTradeFilters(records []GeneratedRecord) (TradeCalibration, error) {
36: 	tradeRecords := LatestRecords(filterRecordsBySchema(records, "trade"))
37: 	if len(tradeRecords) == 0 {
38: 		return TradeCalibration{}, fmt.Errorf("no trade records available for calibration")
39: 	}
40: 	high, err := selectCandidate(tradeRecords, []string{"symbol"}, SelectivityBandHigh)
41: 	if err != nil {
42: 		return TradeCalibration{}, err
43: 	}
44: 	medium, err := selectCandidate(tradeRecords, []string{"tradeType", "exchange"}, SelectivityBandMedium)
45: 	if err != nil {
46: 		return TradeCalibration{}, err
47: 	}
48: 	low, err := selectCandidate(tradeRecords, []string{"region", "orderChannel"}, SelectivityBandLow)
49: 	if err != nil {
50: 		return TradeCalibration{}, err
51: 	}
52: 	return TradeCalibration{High: high, Medium: medium, Low: low}, nil
53: }
54: 
55: func selectCandidate(records []GeneratedRecord, attributes []string, band SelectivityBand) (CalibrationCandidate, error) {
56: 	total := len(records)
57: 	if total == 0 {
58: 		return CalibrationCandidate{}, fmt.Errorf("cannot calibrate with no records")
59: 	}
60: 	minRatio, maxRatio, target := bandRange(band)
61: 	best := CalibrationCandidate{}
62: 	bestDistance := math.MaxFloat64
63: 	fallback := CalibrationCandidate{}
64: 	fallbackDistance := math.MaxFloat64
65: 	found := false
66: 	fallbackFound := false
67: 	for _, attribute := range attributes {
68: 		counts := make(map[string]int)
69: 		for _, record := range records {
70: 			value, ok := record.Attributes[attribute]
71: 			if !ok {
72: 				continue
73: 			}
74: 			counts[fmt.Sprint(value)]++
75: 		}
76: 		keys := make([]string, 0, len(counts))
77: 		for key := range counts {
78: 			keys = append(keys, key)
79: 		}
80: 		sort.Strings(keys)
81: 		for _, key := range keys {
82: 			matches := counts[key]
83: 			ratio := float64(matches) / float64(total)
84: 			distance := math.Abs(target - ratio)
85: 			if !fallbackFound || distance < fallbackDistance {
86: 				fallback = CalibrationCandidate{Attribute: attribute, Value: key, Matches: matches, Ratio: ratio, Band: band}
87: 				fallbackDistance = distance
88: 				fallbackFound = true
89: 			}
90: 			if ratio < minRatio || ratio > maxRatio {
91: 				continue
92: 			}
93: 			if !found || distance < bestDistance {
94: 				best = CalibrationCandidate{Attribute: attribute, Value: key, Matches: matches, Ratio: ratio, Band: band}
95: 				bestDistance = distance
96: 				found = true
97: 			}
98: 		}
99: 	}
100: 	if !found {
101: 		if fallbackFound {
102: 			return fallback, nil
103: 		}
104: 		return CalibrationCandidate{}, fmt.Errorf("no candidate found for %s selectivity", band)
105: 	}
106: 	return best, nil
107: }
108: 
109: func bandRange(band SelectivityBand) (float64, float64, float64) {
110: 	switch band {
111: 	case SelectivityBandHigh:
112: 		return 0, 0.02, 0.01
113: 	case SelectivityBandMedium:
114: 		return 0.02, 0.20, 0.10
115: 	case SelectivityBandLow:
116: 		return 0.20, 1, 0.30
117: 	default:
118: 		return 0, 1, 0.10
119: 	}
120: }
121: 
122: func filterRecordsBySchema(records []GeneratedRecord, schemaName string) []GeneratedRecord {
123: 	out := make([]GeneratedRecord, 0)
124: 	for _, record := range records {
125: 		if record.SchemaName == schemaName {
126: 			out = append(out, cloneGeneratedRecord(record))
127: 		}
128: 	}
129: 	return out
130: }

(End of file - total 130 lines)
</content>
```

**Tool: read**

**Input:**
```json
{
  "filePath": "/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/schema.go"
}
```

**Output:**
```
<path>/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/schema.go</path>
<type>file</type>
<content>
1: package benchmark
2: 
3: import (
4: 	"fmt"
5: 	"path/filepath"
6: 	"runtime"
7: 
8: 	forma "github.com/lychee-technology/forma"
9: 	"github.com/lychee-technology/forma/internal"
10: )
11: 
12: const (
13: 	SchemaIDCustomer int16 = 100
14: 	SchemaIDSecurity int16 = 101
15: 	SchemaIDTrade    int16 = 102
16: )
17: 
18: // SchemaFixture describes a benchmark schema fixture.
19: type SchemaFixture struct {
20: 	ID          int16  `json:"id"`
21: 	Name        string `json:"name"`
22: 	Description string `json:"description"`
23: }
24: 
25: // SchemaRegistrar is the narrow harness contract needed by the benchmark scaffold.
26: type SchemaRegistrar interface {
27: 	SetupSchema(schemaID int16, schemaName string) error
28: }
29: 
30: // DefaultSchemaFixtures returns the TPC-E-inspired schema set for the benchmark.
31: func DefaultSchemaFixtures() []SchemaFixture {
32: 	return []SchemaFixture{
33: 		{ID: SchemaIDCustomer, Name: "customer", Description: "customer dimension with hot filter and EAV profile fields"},
34: 		{ID: SchemaIDSecurity, Name: "security", Description: "security reference data with hot symbol lookup and cold metrics"},
35: 		{ID: SchemaIDTrade, Name: "trade", Description: "trade fact entity with hybrid pagination and filter attributes"},
36: 	}
37: }
38: 
39: // FixturesDir returns the on-disk schema fixture directory.
40: func FixturesDir() string {
41: 	_, file, _, ok := runtime.Caller(0)
42: 	if !ok {
43: 		return ""
44: 	}
45: 	return filepath.Join(filepath.Dir(file), "schemas")
46: }
47: 
48: // LoadFixtureRegistry loads the benchmark schema fixtures through the standard file registry.
49: func LoadFixtureRegistry() (forma.SchemaRegistry, error) {
50: 	registry, err := internal.NewFileSchemaRegistryFromDirectory(FixturesDir())
51: 	if err != nil {
52: 		return nil, fmt.Errorf("load fixture registry: %w", err)
53: 	}
54: 	for _, fixture := range DefaultSchemaFixtures() {
55: 		id, _, err := registry.GetSchemaAttributeCacheByName(fixture.Name)
56: 		if err != nil {
57: 			return nil, fmt.Errorf("load fixture %s: %w", fixture.Name, err)
58: 		}
59: 		if id != fixture.ID {
60: 			return nil, fmt.Errorf("fixture %s expected schema ID %d, got %d", fixture.Name, fixture.ID, id)
61: 		}
62: 	}
63: 	return registry, nil
64: }
65: 
66: // RegisterFixtureSchemas wires the benchmark fixtures into a harness-backed schema registry table.
67: func RegisterFixtureSchemas(registrar SchemaRegistrar) error {
68: 	for _, fixture := range DefaultSchemaFixtures() {
69: 		if err := registrar.SetupSchema(fixture.ID, fixture.Name); err != nil {
70: 			return fmt.Errorf("register fixture %s: %w", fixture.Name, err)
71: 		}
72: 	}
73: 	return nil
74: }
75: 
76: func workloadSchemaID(schemaName string) (int16, error) {
77: 	for _, fixture := range DefaultSchemaFixtures() {
78: 		if fixture.Name == schemaName {
79: 			return fixture.ID, nil
80: 		}
81: 	}
82: 	return 0, fmt.Errorf("unknown benchmark schema %q", schemaName)
83: }

(End of file - total 83 lines)
</content>
```

**Tool: read**

**Input:**
```json
{
  "filePath": "/home/ruoshi/code/github/forma/internal/federated_interfaces.go",
  "limit": 100
}
```

**Output:**
```
<path>/home/ruoshi/code/github/forma/internal/federated_interfaces.go</path>
<type>file</type>
<content>
1: package internal
2: 
3: // Federated query related types and options.
4: // These extend the existing AttributeQuery to carry hints for
5: // multi-tier federated execution (Postgres + DuckDB/S3).
6: 
7: type DataTier string
8: 
9: const (
10: 	DataTierHot  DataTier = "hot"
11: 	DataTierWarm DataTier = "warm"
12: 	DataTierCold DataTier = "cold"
13: )
14: 
15: // FederatedAttributeQuery extends AttributeQuery with federated-specific hints.
16: // It embeds the existing AttributeQuery so it remains compatible with existing code paths.
17: type FederatedAttributeQuery struct {
18: 	AttributeQuery
19: 
20: 	// PreferredTiers is an ordered list of preferred data tiers to query.
21: 	// Example: []DataTier{DataTierHot, DataTierWarm, DataTierCold}
22: 	PreferredTiers []DataTier
23: 
24: 	// PreferHot indicates strong preference for reading from the hot (Postgres) tier
25: 	// when the same data exists in multiple tiers.
26: 	PreferHot bool
27: 
28: 	// UseMainAsAnchor controls whether the main table (entity_main) should be used
29: 	// as the anchor for predicate pushdown. This mirrors existing repository logic.
30: 	UseMainAsAnchor bool
31: 
32: 	// DuckDBHints carries optional DuckDB-specific rendering hints, e.g. external
33: 	// parquet path templates or casting preferences.
34: 	DuckDBHints *DuckDBRenderHints
35: }
36: 
37: // DuckDBRenderHints provides optional parameters that guide DuckDB SQL generation.
38: type DuckDBRenderHints struct {
39: 	// S3ParquetPathTemplate is a template (with placeholders) for locating parquet files in S3.
40: 	// Example: "s3://bucket/path/schema_{{.SchemaID}}/data.parquet"
41: 	S3ParquetPathTemplate string
42: 
43: 	// TimeEncodingHint indicates how date/time values should be encoded in DuckDB side.
44: 	// e.g. "unix_ms" or "iso8601"
45: 	TimeEncodingHint string
46: }
47: 
48: // FederatedQueryOptions contains runtime options for federated execution.
49: type FederatedQueryOptions struct {
50: 	// MaxRows limits the number of rows read from remote/columnar sources per shard.
51: 	MaxRows int
52: 
53: 	// Parallelism controls how many parallel DuckDB scan workers to use.
54: 	Parallelism int
55: 
56: 	// AllowPartialDegradedMode if true will allow executing the query with only a subset
57: 	// of data tiers available (useful for the early MVP).
58: 	AllowPartialDegradedMode bool
59: 
60: 	// IncludeExecutionPlan when true instructs the repository to collect an execution plan
61: 	// for debugging/observability. If set, the repository will allocate and populate
62: 	// ExecutionPlan and assign it to ExecutionPlan (below) so callers may inspect it.
63: 	IncludeExecutionPlan bool
64: 
65: 	// ExecutionPlan is populated by the repository when IncludeExecutionPlan==true.
66: 	// Callers should pass a non-nil opts pointer and inspect this field after call.
67: 	ExecutionPlan *ExecutionPlan
68: }
69: 
70: // ExecutionPlan is a diagnostic structure capturing the federated query execution
71: // choices and timings. It is intended for debugging and observability only.
72: type ExecutionPlan struct {
73: 	// Routing decision snapshot (which tiers were considered/selected)
74: 	Routing RoutingDecision
75: 
76: 	// Per-source plans for each data source touched by the federated execution.
77: 	Sources []DataSourcePlan
78: 
79: 	// Merge describes the merge-on-read strategy applied to results across tiers.
80: 	Merge MergePlan
81: 
82: 	// Timings: coarse-grained durations in milliseconds for major stages.
83: 	// Keys typically: "translate", "postgres_fetch", "duckdb_fetch", "merge", "total"
84: 	Timings map[string]int64
85: 
86: 	// Notes and warnings captured during planning/execution.
87: 	Notes []string
88: }
89: 
90: // DataSourcePlan captures per-source execution details.
91: type DataSourcePlan struct {
92: 	// Tier indicates the logical data tier (hot/warm/cold).
93: 	Tier DataTier
94: 
95: 	// Engine indicates the execution engine, e.g., "postgres" or "duckdb".
96: 	Engine string
97: 
98: 	// SQL optionally contains the generated SQL fragment or rendered template used.
99: 	// For privacy/performance reasons this may be truncated by the repository.
100: 	SQL string

(Showing lines 1-100 of 144. Use offset=101 to continue.)
</content>
```

---

## Assistant (Build · DeepSeek V4 Pro · 7.7s)

_Thinking:_

Now let me look at the fixture schema JSON files to understand the EAV vs main table column bindings for the three benchmark schemas, and also look at the DuckDB template file if available.

**Tool: glob**

**Input:**
```json
{
  "pattern": "**/benchmark/schemas/**"
}
```

**Output:**
```
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/schemas/trade_attributes.json
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/schemas/trade.json
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/schemas/security_attributes.json
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/schemas/security.json
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/schemas/customer_attributes.json
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/schemas/customer.json
```

**Tool: glob**

**Input:**
```json
{
  "pattern": "**/*.sql.tpl"
}
```

**Output:**
```
No files found
```

**Tool: glob**

**Input:**
```json
{
  "pattern": "**/*_template*"
}
```

**Output:**
```
/home/ruoshi/code/github/forma/internal/advanced_query_template_duckdb.go
/home/ruoshi/code/github/forma/internal/duckdb_template_renderer.go
/home/ruoshi/code/github/forma/internal/sql_template_renderer_test.go
/home/ruoshi/code/github/forma/internal/sql_template_renderer.go
/home/ruoshi/code/github/forma/internal/advanced_query_template.go
/home/ruoshi/code/github/forma/internal/advanced_query_template_test.go
```

---

## Assistant (Build · DeepSeek V4 Pro · 7.6s)

**Tool: read**

**Input:**
```json
{
  "filePath": "/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/schemas/trade_attributes.json"
}
```

**Output:**
```
<path>/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/schemas/trade_attributes.json</path>
<type>file</type>
<content>
1: {
2:   "symbol": {
3:     "attributeID": 1,
4:     "valueType": "text",
5:     "column_binding": {
6:       "col_name": "text_01"
7:     }
8:   },
9:   "tradeType": {
10:     "attributeID": 2,
11:     "valueType": "smallint",
12:     "column_binding": {
13:       "col_name": "smallint_01"
14:     }
15:   },
16:   "quantity": {
17:     "attributeID": 3,
18:     "valueType": "bigint",
19:     "column_binding": {
20:       "col_name": "bigint_01"
21:     }
22:   },
23:   "price": {
24:     "attributeID": 4,
25:     "valueType": "numeric",
26:     "column_binding": {
27:       "col_name": "double_01"
28:     }
29:   },
30:   "tradeTime": {
31:     "attributeID": 5,
32:     "valueType": "datetime",
33:     "column_binding": {
34:       "col_name": "bigint_02",
35:       "encoding": "unix_ms"
36:     }
37:   },
38:   "customerId": {
39:     "attributeID": 6,
40:     "valueType": "uuid",
41:     "column_binding": {
42:       "col_name": "uuid_01"
43:     }
44:   },
45:   "region": {
46:     "attributeID": 7,
47:     "valueType": "text",
48:     "column_binding": {
49:       "col_name": "text_02"
50:     }
51:   },
52:   "exchange": {
53:     "attributeID": 8,
54:     "valueType": "text"
55:   },
56:   "commission": {
57:     "attributeID": 9,
58:     "valueType": "numeric"
59:   },
60:   "isCash": {
61:     "attributeID": 10,
62:     "valueType": "bool"
63:   },
64:   "brokerId": {
65:     "attributeID": 11,
66:     "valueType": "text"
67:   },
68:   "orderChannel": {
69:     "attributeID": 12,
70:     "valueType": "text"
71:   }
72: }

(End of file - total 72 lines)
</content>
```

**Tool: read**

**Input:**
```json
{
  "filePath": "/home/ruoshi/code/github/forma/internal/advanced_query_template_duckdb.go"
}
```

**Output:**
```
<path>/home/ruoshi/code/github/forma/internal/advanced_query_template_duckdb.go</path>
<type>file</type>
<content>
1: package internal
2: 
3: import "text/template"
4: 
5: // AdvancedQueryTemplateDuckDB is the DuckDB SQL template used for federated queries.
6: // Placeholders expected to be substituted by the renderer via Go template fields.
7: var AdvancedQueryTemplateDuckDB = template.Must(template.New("optimizedQueryDuckDB").Funcs(template.FuncMap{
8: 	"add": func(a, b int) int { return a + b },
9: }).Parse(`
10: -- PRAGMA & tuning
11: PRAGMA memory_limit='4GB';
12: PRAGMA threads=4;
13: 
14: -- Parameters:
15: -- .SCHEMA_ID             : integer
16: -- .PG_CONN               : postgres connection string for postgres_scan
17: -- .PG_WHERE_CLAUSE       : pushdown predicate for entity_main (physical columns)
18: -- .LOGICAL_WHERE_CLAUSE  : logical predicate for final filtering (DuckDB / Parquet columns)
19: -- .S3_PATHS              : comma-separated paths for read_parquet()
20: -- .PAGE_SIZE, .OFFSET
21: 
22: WITH
23: -- Dirty set: rows currently present/dirty in PG change_log (flushed_at = 0)
24: dirty_ids AS (
25:   SELECT row_id
26:   FROM postgres_scan('{{.PG_CONN}}', '{{.ChangeLogSchema}}', '{{.ChangeLogScanTable}}')
27:   WHERE schema_id = {{.SCHEMA_ID}}
28:     AND flushed_at = 0
29: ),
30: 
31: -- S3 source (Cold/Warm). Read Parquet files and apply logical filters + anti-join
32: s3_source AS (
33:   SELECT
34:     row_id,
35:     ltbase_created_at AS created_at,
36:     ltbase_updated_at AS ver_ts,
37:     ltbase_deleted_at AS deleted_ts,
38: 
39:     -- Logical columns (native in Parquet)
40:     name,
41:     age,
42:     tag
43: 
44:   FROM read_parquet({{.S3_PATHS}})
45:   WHERE
46:     ({{.LOGICAL_WHERE_CLAUSE}})
47:     -- Anti-join: exclude rows that are present in the Dirty Set (PG hot buffer)
48:     AND CAST(row_id AS UUID) NOT IN (SELECT row_id FROM dirty_ids)
49: ),
50: 
51: -- PG source (Hot). Use postgres_scan with pushdown for entity_main, pivot EAV attributes.
52: pg_source AS (
53:   SELECT
54:     m.ltbase_row_id AS row_id,
55:     m.ltbase_created_at AS created_at,
56:     cl.changed_at AS ver_ts,
57:     cl.deleted_at AS deleted_ts,
58: 
59:     -- Explicit casts to align PG types with Parquet schema
60:     CAST(m.text_01 AS VARCHAR) AS name,
61:     CAST(m.integer_01 AS INTEGER) AS age,
62: 
63:     -- EAV pivot (explicit casts). Replace attr_id constants with dynamic mapping if needed.
64:     MAX(CASE WHEN e.attr_id = 205 THEN CAST(e.value_text AS VARCHAR) END) AS tag
65: 
66:   FROM postgres_scan('{{.PG_CONN}}', '{{.ChangeLogSchema}}', '{{.ChangeLogScanTable}}') cl
67: 
68:   -- Pushdown: restrict entity_main at the scan-level using .PG_WHERE_CLAUSE
69:   JOIN postgres_scan('{{.PG_CONN}}',
70:     '{{.MainSchema}}',
71:     '{{.MainScanTable}}'
72:   ) m
73:     ON cl.schema_id = m.ltbase_schema_id
74:     AND cl.row_id = m.ltbase_row_id
75: 
76:   LEFT JOIN postgres_scan('{{.PG_CONN}}', '{{.EAVSchema}}', '{{.EAVScanTable}}') e
77:     ON cl.schema_id = e.schema_id AND cl.row_id = e.row_id
78: 
79:   WHERE cl.schema_id = {{.SCHEMA_ID}}
80:     AND cl.flushed_at = 0
81:     AND m.ltbase_schema_id = {{.SCHEMA_ID}}
82:     AND ({{.PG_WHERE_CLAUSE}})
83:   GROUP BY m.ltbase_row_id, m.ltbase_created_at, cl.changed_at, cl.deleted_at, m.text_01, m.integer_01
84: ),
85: 
86: -- Union warm/cold S3 data with hot PG data
87: unified AS (
88:   SELECT * FROM s3_source
89:   UNION ALL
90:   SELECT * FROM pg_source
91: )
92: 
93: -- Final selection:
94: -- - Apply final logical filters to ensure EAV & other logical predicates are respected
95: -- - Remove soft-deleted rows
96: -- - Deduplicate using latest version timestamp per row_id
97: SELECT
98:   {{.SCHEMA_ID}}::SMALLINT AS ltbase_schema_id,
99:   CAST(row_id AS UUID) AS ltbase_row_id,
100:   created_at AS ltbase_created_at,
101:   ver_ts AS ltbase_updated_at,
102:   deleted_ts AS ltbase_deleted_at,
103:   name AS text_01,
104:   age AS integer_01,
105:   '[]'::TEXT AS attributes_json,
106:   COUNT(DISTINCT row_id) OVER() AS total_records,
107:   CEIL(COUNT(DISTINCT row_id) OVER()::DOUBLE / NULLIF({{.PAGE_SIZE}}, 0))::BIGINT AS total_pages,
108:   (FLOOR({{.OFFSET}}::DOUBLE / NULLIF({{.PAGE_SIZE}}, 0)) + 1)::BIGINT AS current_page
109: FROM unified
110: WHERE
111:   ({{.LOGICAL_WHERE_CLAUSE}})
112:   AND (deleted_ts IS NULL OR deleted_ts = 0)
113: QUALIFY ROW_NUMBER() OVER (PARTITION BY row_id ORDER BY ver_ts DESC) = 1
114: ORDER BY created_at DESC
115: LIMIT {{.PAGE_SIZE}} OFFSET {{.OFFSET}};
116: `))

(End of file - total 116 lines)
</content>
```

**Tool: read**

**Input:**
```json
{
  "filePath": "/home/ruoshi/code/github/forma/internal/advanced_query_template.go",
  "limit": 100
}
```

**Output:**
```
<path>/home/ruoshi/code/github/forma/internal/advanced_query_template.go</path>
<type>file</type>
<content>
1: package internal
2: 
3: import "text/template"
4: 
5: var optimizedQuerySQLTemplate = template.Must(template.New("optimizedQuery").Funcs(template.FuncMap{
6: 	"add": func(a, b int) int { return a + b },
7: }).Parse(`
8: 	        WITH anchor AS (
9: 	            {{- if .UseMainTableAsAnchor }}
10: 	            SELECT m.ltbase_row_id AS row_id
11: 	            FROM {{.MainTable}} m
12: 	            WHERE m.ltbase_schema_id = {{.SchemaID}} AND {{.Anchor.Condition}}
13: 	            {{- else }}
14: 	            SELECT DISTINCT t.row_id
15: 	            FROM {{.EAVTable}} t
16: 	            WHERE t.schema_id = {{.SchemaID}} AND {{.Anchor.Condition}}
17: 	            {{- end }}
18: 	            {{- if .ChangeLogTable }}
19: 	            UNION
20: 	            -- Include real-time buffer rows from change_log (flushed_at = 0)
21: 	            -- while preserving the same anchor filter semantics.
22: 	            SELECT cl.row_id
23: 	            FROM {{.ChangeLogTable}} cl
24: 	            WHERE cl.schema_id = {{.SchemaID}}
25: 	                AND cl.flushed_at = 0
26: 	                {{- if .UseMainTableAsAnchor }}
27: 	                AND EXISTS (
28: 	                    SELECT 1
29: 	                    FROM {{.MainTable}} m
30: 	                    WHERE m.ltbase_schema_id = {{.SchemaID}}
31: 	                        AND m.ltbase_row_id = cl.row_id
32: 	                        AND {{.Anchor.Condition}}
33: 	                )
34: 	                {{- else }}
35: 	                AND EXISTS (
36: 	                    SELECT 1
37: 	                    FROM {{.EAVTable}} t
38: 	                    WHERE t.schema_id = {{.SchemaID}}
39: 	                        AND t.row_id = cl.row_id
40: 	                        AND {{.Anchor.Condition}}
41: 	                )
42: 	                {{- end }}
43: 	            {{- end }}
44: 	        ),
45:         keys AS (
46:             SELECT
47:                 a.row_id
48:                 {{- if gt (len .SortKeys) 0 }}
49:                 {{- range $i, $k := .SortKeys }}
50:                 ,
51:                 {{- if $k.IsMainColumn }}
52:                 (
53:                     SELECT m.{{ $k.MainColumnName }}
54:                     FROM {{$.MainTable}} m
55:                     WHERE m.ltbase_schema_id = {{$.SchemaID}}
56:                         AND m.ltbase_row_id = a.row_id
57:                     LIMIT 1
58:                 ) AS k{{$i}}
59:                 {{- else }}
60:                 (
61:                     SELECT d.{{ $k.ValueColumn }}
62:                     FROM {{$.EAVTable}} d
63:                     WHERE d.schema_id = {{$.SchemaID}}
64:                         AND d.row_id = a.row_id
65:                         AND d.attr_id = {{ $k.AttrIDInt }}
66:                     ORDER BY d.array_indices NULLS FIRST
67:                     LIMIT 1
68:                 ) AS k{{$i}}
69:                 {{- end }}
70:                 {{- end }}
71:                 {{- end }},
72:                 COUNT(*) OVER() AS total
73:             FROM anchor a
74:         ),
75:         ordered AS (
76:             SELECT
77:                 row_id
78:                 {{- if gt (len .SortKeys) 0 }}
79:                 {{- range $i, $_ := .SortKeys }}
80:                 , k{{$i}}
81:                 {{- end }}
82:                 {{- end }},
83:                 total
84:             FROM keys
85:             ORDER BY
86:                 {{- if gt (len .SortKeys) 0 }}
87:                 {{- range $i, $k := .SortKeys }}
88:                 k{{$i}} {{ if $k.Desc }}DESC{{ else }}ASC{{ end }}{{ if lt (add $i 1) (len $.SortKeys) }},{{ end }}
89:                 {{- end }}
90:                 {{- if gt (len .SortKeys) 0 }},{{ end }}
91:                 {{- end }}
92:                 row_id
93:             LIMIT {{.Limit}} OFFSET {{.Offset}}
94:         ),
95:         main_data AS (
96:             SELECT 
97:                 {{.MainProjection}},
98:                 o.total
99:                 {{- if gt (len .SortKeys) 0 }}
100:                 {{- range $i, $_ := .SortKeys }}

(Showing lines 1-100 of 143. Use offset=101 to continue.)
</content>
```

---

## Assistant (Build · DeepSeek V4 Pro · 11.8s)

_Thinking:_

Now let me look at the remaining template, the hot-filter building logic in the query.go, and check how the benchmark schema attributes are registered for the customer and security schemas.

**Tool: read**

**Input:**
```json
{
  "filePath": "/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/schemas/customer_attributes.json"
}
```

**Output:**
```
<path>/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/schemas/customer_attributes.json</path>
<type>file</type>
<content>
1: {
2:   "taxId": {
3:     "attributeID": 1,
4:     "valueType": "text",
5:     "column_binding": {
6:       "col_name": "text_01"
7:     }
8:   },
9:   "status": {
10:     "attributeID": 2,
11:     "valueType": "smallint",
12:     "column_binding": {
13:       "col_name": "smallint_01"
14:     }
15:   },
16:   "region": {
17:     "attributeID": 3,
18:     "valueType": "text",
19:     "column_binding": {
20:       "col_name": "text_02"
21:     }
22:   },
23:   "name": {
24:     "attributeID": 4,
25:     "valueType": "text"
26:   },
27:   "email": {
28:     "attributeID": 5,
29:     "valueType": "text"
30:   },
31:   "creditRating": {
32:     "attributeID": 6,
33:     "valueType": "numeric"
34:   }
35: }

(End of file - total 35 lines)
</content>
```

**Tool: read**

**Input:**
```json
{
  "filePath": "/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/schemas/security_attributes.json"
}
```

**Output:**
```
<path>/home/ruoshi/code/github/forma/internal/e2e_harness/federated/benchmark/schemas/security_attributes.json</path>
<type>file</type>
<content>
1: {
2:   "symbol": {
3:     "attributeID": 1,
4:     "valueType": "text",
5:     "column_binding": {
6:       "col_name": "text_01"
7:     }
8:   },
9:   "sector": {
10:     "attributeID": 2,
11:     "valueType": "smallint",
12:     "column_binding": {
13:       "col_name": "smallint_01"
14:     }
15:   },
16:   "companyName": {
17:     "attributeID": 3,
18:     "valueType": "text"
19:   },
20:   "dividend": {
21:     "attributeID": 4,
22:     "valueType": "numeric"
23:   },
24:   "marketCap": {
25:     "attributeID": 5,
26:     "valueType": "numeric"
27:   }
28: }

(End of file - total 28 lines)
</content>
```

**Tool: read**

**Input:**
```json
{
  "filePath": "/home/ruoshi/code/github/forma/internal/advanced_query_template.go",
  "offset": 100
}
```

**Output:**
```
<path>/home/ruoshi/code/github/forma/internal/advanced_query_template.go</path>
<type>file</type>
<content>
100:                 {{- range $i, $_ := .SortKeys }}
101:                 , o.k{{$i}}
102:                 {{- end }}
103:                 {{- end }}
104:             FROM ordered o
105:             INNER JOIN {{.MainTable}} m 
106:                 ON m.ltbase_schema_id = {{.SchemaID}} 
107:                 AND m.ltbase_row_id = o.row_id
108:         ),
109:         eav_aggregated AS (
110:             SELECT 
111:                 e.row_id,
112:                 JSON_AGG(
113:                     JSON_BUILD_OBJECT(
114:                         'schema_id', e.schema_id,
115:                         'row_id', e.row_id,
116:                         'attr_id', e.attr_id,
117:                         'array_indices', e.array_indices,
118:                         'value_text', e.value_text,
119:                         'value_numeric', e.value_numeric
120:                     ) ORDER BY e.attr_id, e.array_indices
121:                 )::TEXT AS attributes_json
122:             FROM ordered o
123:             INNER JOIN {{.EAVTable}} e 
124:                 ON e.schema_id = {{.SchemaID}} 
125:                 AND e.row_id = o.row_id
126:             GROUP BY e.row_id
127:         )
128:         SELECT 
129:             {{.MainProjection}},
130:             COALESCE(e.attributes_json, '[]') AS attributes_json,
131:             m.total AS total_records,
132:             CEIL(m.total::numeric / NULLIF({{.PageSize}}::numeric, 0)) AS total_pages,
133:             (FLOOR({{.Offset}}::numeric / NULLIF({{.Limit}}::numeric, 0)) + 1)::int AS current_page
134:         FROM main_data m
135:         LEFT JOIN eav_aggregated e ON e.row_id = m.ltbase_row_id
136:         ORDER BY
137:             {{- if gt (len .SortKeys) 0 }}
138:             {{- range $i, $k := .SortKeys }}
139:             m.k{{$i}} {{ if $k.Desc }}DESC{{ else }}ASC{{ end }}{{ if lt (add $i 1) (len $.SortKeys) }},{{ end }}
140:             {{- end }}
141:             {{- if gt (len .SortKeys) 0 }},{{ end }}
142:             {{- end }}
143:             m.ltbase_row_id;`))

(End of file - total 143 lines)
</content>
```

**Tool: grep**

**Input:**
```json
{
  "pattern": "buildHybridConditions|buildHotConditions|GenerateDuckDBWhereClause",
  "include": "*.go"
}
```

**Output:**
```
Found 87 matches
/home/ruoshi/code/github/forma/internal/duckdb_template_renderer.go:
  Line 12: // and combines the provided whereArgs (typically from GenerateDuckDBWhereClause)

  Line 92: 		whereClause, whereArgs, err = GenerateDuckDBWhereClause(q)

  Line 94: 		whereClause, whereArgs, err = GenerateDuckDBWhereClauseWithExclusions(q, dirtyIDs)


/home/ruoshi/code/github/forma/internal/duckdb_sql_generator.go:
  Line 129: // GenerateDuckDBWhereClause produces a minimal DuckDB WHERE clause for a FederatedAttributeQuery.

  Line 137: func GenerateDuckDBWhereClause(q *FederatedAttributeQuery) (string, []any, error) {

  Line 335: // GenerateDuckDBWhereClauseWithExclusions builds a DuckDB WHERE clause for the query

  Line 337: func GenerateDuckDBWhereClauseWithExclusions(q *FederatedAttributeQuery, dirtyIDs []uuid.UUID) (string, []any, error) {

  Line 338: 	where, args, err := GenerateDuckDBWhereClause(q)


/home/ruoshi/code/github/forma/internal/duckdb_sql_generator_test.go:
  Line 337: func TestGenerateDuckDBWhereClause_GivenBareRFC3339Literal_WhenClauseBuilt_ThenItDefaultsToEqualsTimestamp(t *testing.T) {

  Line 347: 	clause, args, err := GenerateDuckDBWhereClause(q)

  Line 354: func TestGenerateDuckDBWhereClause_GivenEpochMillisLiteral_WhenClauseBuilt_ThenItUsesTimestampCast(t *testing.T) {

  Line 364: 	clause, args, err := GenerateDuckDBWhereClause(q)

  Line 371: func TestGenerateDuckDBWhereClause_GivenNestedAndOrConditions_WhenClauseBuilt_ThenGroupingAndArgumentOrderArePreserved(t *testing.T) {

  Line 390: 	clause, args, err := GenerateDuckDBWhereClause(q)


/home/ruoshi/code/github/forma/internal/postgres_persistent_repository.go:
  Line 286: 	conditions, args, err := r.buildHybridConditions(


/home/ruoshi/code/github/forma/internal/postgres_persistent_repository_test.go:
  Line 332: 	clause, args, err := repo.buildHybridConditions("eav_table", "main_table", query, 1, true)

  Line 337: 	clause, args, err = repo.buildHybridConditions("eav_table", "main_table", query, 1, false)

  Line 347: 	clause, args, err = repo.buildHybridConditions("eav_table", "main_table", query, 1, true)

  Line 371: 	clause, args, err := repo.buildHybridConditions("eav_table", "main_table", query, 1, true)


/home/ruoshi/code/github/forma/internal/postgres_persistent_repository_query.go:
  Line 158: func (r *DBPersistentRecordRepository) buildHybridConditions(


/home/ruoshi/code/github/forma/internal/federated_pagination.go:
  Line 35: 	clause, args, err := r.buildHybridConditions(tables.EAVData, tables.EntityMain, fq.AttributeQuery, 0, fq.UseMainAsAnchor)


/home/ruoshi/code/github/forma/internal/duckdb_query_comprehensive_test.go:
  Line 14: // TC-1: GenerateDuckDBWhereClause Tests

  Line 17: func TestGenerateDuckDBWhereClause_SimpleKVEquals(t *testing.T) {

  Line 27: 	clause, args, err := GenerateDuckDBWhereClause(q)

  Line 34: func TestGenerateDuckDBWhereClause_KVWithGTOperator(t *testing.T) {

  Line 44: 	clause, args, err := GenerateDuckDBWhereClause(q)

  Line 51: func TestGenerateDuckDBWhereClause_KVWithStartsWith(t *testing.T) {

  Line 61: 	clause, args, err := GenerateDuckDBWhereClause(q)

  Line 68: func TestGenerateDuckDBWhereClause_KVWithContains(t *testing.T) {

  Line 78: 	clause, args, err := GenerateDuckDBWhereClause(q)

  Line 85: func TestGenerateDuckDBWhereClause_CompositeAND(t *testing.T) {

  Line 98: 	clause, args, err := GenerateDuckDBWhereClause(q)

  Line 105: func TestGenerateDuckDBWhereClause_CompositeOR(t *testing.T) {

  Line 118: 	clause, args, err := GenerateDuckDBWhereClause(q)

  Line 124: func TestGenerateDuckDBWhereClause_NilCondition(t *testing.T) {

  Line 129: 	clause, args, err := GenerateDuckDBWhereClause(q)

  Line 135: func TestGenerateDuckDBWhereClause_NilQuery(t *testing.T) {

  Line 136: 	clause, args, err := GenerateDuckDBWhereClause(nil)

  Line 143: // TC-2: GenerateDuckDBWhereClauseWithExclusions Tests

  Line 146: func TestGenerateDuckDBWhereClauseWithExclusions_AppendsDirtyIDs(t *testing.T) {

  Line 159: 	clause, args, err := GenerateDuckDBWhereClauseWithExclusions(q, []uuid.UUID{u1, u2})

  Line 166: func TestGenerateDuckDBWhereClauseWithExclusions_ParameterOrder(t *testing.T) {

  Line 183: 	_, args, err := GenerateDuckDBWhereClauseWithExclusions(q, []uuid.UUID{u1, u2, u3})

  Line 197: func TestGenerateDuckDBWhereClauseWithExclusions_EmptyDirtyIDs(t *testing.T) {

  Line 207: 	clause, args, err := GenerateDuckDBWhereClauseWithExclusions(q, []uuid.UUID{})

  Line 214: func TestGenerateDuckDBWhereClauseWithExclusions_SingleDirtyID(t *testing.T) {

  Line 221: 	clause, args, err := GenerateDuckDBWhereClauseWithExclusions(q, []uuid.UUID{u1})

  Line 229: func TestGenerateDuckDBWhereClauseWithExclusions_ManyDirtyIDs(t *testing.T) {

  Line 245: 	clause, args, err := GenerateDuckDBWhereClauseWithExclusions(q, dirtyIDs)


/home/ruoshi/code/github/forma/internal/duckdb_conn_test.go:
  Line 24: func TestGenerateDuckDBWhereClause_SimpleKv(t *testing.T) {

  Line 33: 	where, args, err := GenerateDuckDBWhereClause(q)

  Line 35: 		t.Fatalf("GenerateDuckDBWhereClause error: %v", err)


/home/ruoshi/code/github/forma/internal/dualpath_sql_generator.go:
  Line 212: // This mirrors GenerateDuckDBWhereClause but uses attribute metadata to resolve column bindings.


/home/ruoshi/code/github/forma/internal/duckdb_query_edge_cases_test.go:
  Line 16: func TestGenerateDuckDBWhereClause_DeeplyNestedComposite(t *testing.T) {

  Line 54: 	clause, args, err := GenerateDuckDBWhereClause(query)

  Line 63: func TestGenerateDuckDBWhereClause_TriplyNestedComposite(t *testing.T) {

  Line 101: 	clause, args, err := GenerateDuckDBWhereClause(query)

  Line 107: func TestGenerateDuckDBWhereClause_ManyTopLevelConditions(t *testing.T) {

  Line 126: 	clause, args, err := GenerateDuckDBWhereClause(query)

  Line 138: func TestGenerateDuckDBWhereClause_UnicodeCharacters(t *testing.T) {

  Line 148: 	clause, args, err := GenerateDuckDBWhereClause(query)

  Line 156: func TestGenerateDuckDBWhereClause_SpecialCharactersInValue(t *testing.T) {

  Line 166: 	clause, args, err := GenerateDuckDBWhereClause(query)

  Line 172: func TestGenerateDuckDBWhereClause_QuotesAndApostrophes(t *testing.T) {

  Line 182: 	clause, args, err := GenerateDuckDBWhereClause(query)

  Line 189: func TestGenerateDuckDBWhereClause_SQLSpecialCharacters(t *testing.T) {

  Line 200: 	clause, args, err := GenerateDuckDBWhereClause(query)

  Line 207: func TestGenerateDuckDBWhereClause_EmptyStringValue(t *testing.T) {

  Line 217: 	clause, args, err := GenerateDuckDBWhereClause(query)

  Line 224: func TestGenerateDuckDBWhereClause_VeryLongStringValue(t *testing.T) {

  Line 236: 	clause, args, err := GenerateDuckDBWhereClause(query)

  Line 242: func TestGenerateDuckDBWhereClause_NewlinesAndWhitespace(t *testing.T) {

  Line 252: 	clause, args, err := GenerateDuckDBWhereClause(query)

  Line 262: func TestGenerateDuckDBWhereClauseWithExclusions_LargeDirtyIDSet100(t *testing.T) {

  Line 278: 	clause, args, err := GenerateDuckDBWhereClauseWithExclusions(query, dirtyIDs)

  Line 287: func TestGenerateDuckDBWhereClauseWithExclusions_LargeDirtyIDSet1000(t *testing.T) {

  Line 303: 	clause, args, err := GenerateDuckDBWhereClauseWithExclusions(query, dirtyIDs)

  Line 312: func TestGenerateDuckDBWhereClauseWithExclusions_MaxInt16DirtyIDs(t *testing.T) {

  Line 329: 	clause, args, err := GenerateDuckDBWhereClauseWithExclusions(query, dirtyIDs)

  Line 337: func TestGenerateDuckDBWhereClauseWithExclusions_DuplicateDirtyIDs(t *testing.T) {

  Line 359: 	clause, args, err := GenerateDuckDBWhereClauseWithExclusions(query, dirtyIDs)

  Line 370: func TestGenerateDuckDBWhereClause_ComplexNestedWithUnicodeAndLargeDirtySet(t *testing.T) {

  Line 404: 	clause, args, err := GenerateDuckDBWhereClauseWithExclusions(query, dirtyIDs)

  Line 411: func TestGenerateDuckDBWhereClause_EdgeCaseWithNilCondition(t *testing.T) {

  Line 418: 	clause, _, err := GenerateDuckDBWhereClause(query)

  Line 424: func TestGenerateDuckDBWhereClause_EmptyCompositeCondition(t *testing.T) {

  Line 434: 	clause, _, err := GenerateDuckDBWhereClause(query)

```

---

## Assistant (Build · DeepSeek V4 Pro · 11.4s)

_Thinking:_

Now let me check the hot-trade-time-path that seems to bypass pushdown entirely, and also look at how the DuckDB template parameters map to the actual production query used in the benchmark test harness. Let me also look at the `buildHybridConditions` function and how conditions are translated in the Postgres path.

**Tool: read**

**Input:**
```json
{
  "filePath": "/home/ruoshi/code/github/forma/internal/postgres_persistent_repository_query.go",
  "offset": 150,
  "limit": 100
}
```

**Output:**
```
<path>/home/ruoshi/code/github/forma/internal/postgres_persistent_repository_query.go</path>
<type>file</type>
<content>
150: 	eavTable             string
151: 	mainTable            string
152: 	schemaID             int16
153: 	cache                forma.SchemaAttributeCache
154: 	argCounter           int
155: 	useMainTableAsAnchor bool
156: }
157: 
158: func (r *DBPersistentRecordRepository) buildHybridConditions(
159: 	eavTable, mainTable string,
160: 	query AttributeQuery,
161: 	initArgIndex int,
162: 	useMainTableAsAnchor bool,
163: ) (string, []any, error) {
164: 	if query.Condition == nil {
165: 		return "1=1", nil, nil
166: 	}
167: 
168: 	builder := &hybridConditionBuilder{
169: 		r:                    r,
170: 		eavTable:             eavTable,
171: 		mainTable:            mainTable,
172: 		schemaID:             query.SchemaID,
173: 		argCounter:           initArgIndex,
174: 		useMainTableAsAnchor: useMainTableAsAnchor,
175: 	}
176: 	builder.initCache()
177: 
178: 	return builder.build(query.Condition)
179: }
180: 
181: func (b *hybridConditionBuilder) initCache() {
182: 	if b.schemaID > 0 && b.r.metadataCache != nil {
183: 		if schemaName, ok := b.r.metadataCache.GetSchemaName(b.schemaID); ok {
184: 			if cache, ok := b.r.metadataCache.GetSchemaCache(schemaName); ok {
185: 				b.cache = cache
186: 			}
187: 		}
188: 	}
189: }
190: 
191: func (b *hybridConditionBuilder) build(c forma.Condition) (string, []any, error) {
192: 	switch cond := c.(type) {
193: 	case *forma.CompositeCondition:
194: 		return b.buildComposite(cond)
195: 	case *forma.KvCondition:
196: 		return b.buildKv(cond)
197: 	default:
198: 		return "", nil, fmt.Errorf("unsupported condition type %T", c)
199: 	}
200: }
201: 
202: func (b *hybridConditionBuilder) buildComposite(cond *forma.CompositeCondition) (string, []any, error) {
203: 	if len(cond.Conditions) == 0 {
204: 		return "", nil, nil
205: 	}
206: 	var parts []string
207: 	var args []any
208: 	joiner := " AND "
209: 	if cond.Logic == forma.LogicOr {
210: 		joiner = " OR "
211: 	}
212: 
213: 	for _, child := range cond.Conditions {
214: 		p, a, err := b.build(child)
215: 		if err != nil {
216: 			return "", nil, err
217: 		}
218: 		if p != "" {
219: 			parts = append(parts, fmt.Sprintf("(%s)", p))
220: 			args = append(args, a...)
221: 		}
222: 	}
223: 	if len(parts) == 0 {
224: 		return "", nil, nil
225: 	}
226: 	return strings.Join(parts, joiner), args, nil
227: }
228: 
229: func (b *hybridConditionBuilder) buildKv(cond *forma.KvCondition) (string, []any, error) {
230: 	colName := b.resolveColumnName(cond.Attr)
231: 	if colName != "" {
232: 		return b.buildMainTableCondition(cond, colName)
233: 	}
234: 	return b.buildEAVCondition(cond)
235: }
236: 
237: func (b *hybridConditionBuilder) resolveColumnName(attr string) string {
238: 	if isMainTableColumn(attr) {
239: 		return attr
240: 	}
241: 	if b.cache != nil {
242: 		if meta, ok := b.cache[attr]; ok && meta.ColumnBinding != nil {
243: 			return string(meta.ColumnBinding.ColumnName)
244: 		}
245: 	}
246: 	return ""
247: }
248: 
249: func (b *hybridConditionBuilder) buildMainTableCondition(cond *forma.KvCondition, colName string) (string, []any, error) {

(Showing lines 150-249 of 291. Use offset=250 to continue.)
</content>
```

**Tool: read**

**Input:**
```json
{
  "filePath": "/home/ruoshi/code/github/forma/internal/duckdb_sql_generator.go",
  "offset": 125,
  "limit": 60
}
```

**Output:**
```
<path>/home/ruoshi/code/github/forma/internal/duckdb_sql_generator.go</path>
<type>file</type>
<content>
125: 	}
126: 	return buf.String(), nil
127: }
128: 
129: // GenerateDuckDBWhereClause produces a minimal DuckDB WHERE clause for a FederatedAttributeQuery.
130: // This is an intentionally small helper for the initial integration: it supports CompositeCondition
131: // with KvCondition children and translates simple operators. It returns the clause and a list of args
132: // suitable for use with database/sql parameter placeholders ($1, $2 style are left for later templating).
133: //
134: // NOTE: This is a minimal implementation to allow compilation and unit testing of rendering logic.
135: // Full query translation (including EAV-to-column mapping and proper parameter indexing) will be
136: // implemented in follow-up tasks.
137: func GenerateDuckDBWhereClause(q *FederatedAttributeQuery) (string, []any, error) {
138: 	if q == nil || q.Condition == nil {
139: 		return "1=1", nil, nil
140: 	}
141: 	return generateDuckDBCondition(q.Condition)
142: }
143: 
144: // generateDuckDBCondition recursively builds DuckDB WHERE clause from a condition tree.
145: func generateDuckDBCondition(c forma.Condition) (string, []any, error) {
146: 	switch cond := c.(type) {
147: 	case *forma.CompositeCondition:
148: 		return generateDuckDBCompositeCondition(cond)
149: 	case *forma.KvCondition:
150: 		return generateDuckDBKvCondition(cond)
151: 	default:
152: 		return "", nil, fmt.Errorf("unsupported condition type %T", c)
153: 	}
154: }
155: 
156: // generateDuckDBCompositeCondition handles CompositeCondition for DuckDB WHERE generation.
157: func generateDuckDBCompositeCondition(cond *forma.CompositeCondition) (string, []any, error) {
158: 	if len(cond.Conditions) == 0 {
159: 		return "1=1", nil, nil
160: 	}
161: 
162: 	parts := make([]string, 0, len(cond.Conditions))
163: 	args := []any{}
164: 	joiner := " AND "
165: 	if cond.Logic == forma.LogicOr {
166: 		joiner = " OR "
167: 	}
168: 
169: 	for _, child := range cond.Conditions {
170: 		p, a, err := generateDuckDBCondition(child)
171: 		if err != nil {
172: 			return "", nil, err
173: 		}
174: 		if p != "" {
175: 			parts = append(parts, fmt.Sprintf("(%s)", p))
176: 			args = append(args, a...)
177: 		}
178: 	}
179: 
180: 	if len(parts) == 0 {
181: 		return "1=1", nil, nil
182: 	}
183: 	return joinStrings(parts, joiner), args, nil
184: }

(Showing lines 125-184 of 348. Use offset=185 to continue.)
</content>
```

**Tool: grep**

**Input:**
```json
{
  "pattern": "buildHotTradeTimeOnlyQuery|hotTradeTime",
  "include": "*.go"
}
```

**Output:**
```
Found 2 matches
/home/ruoshi/code/github/forma/internal/e2e_harness/federated/query.go:
  Line 315: 			return buildHotTradeTimeOnlyQuery(pgConnStr, schemaID, rowIDFilter)

  Line 335: func buildHotTradeTimeOnlyQuery(pgConnStr string, schemaID int16, rowIDFilter string) string {

```

---

## Assistant (Build · DeepSeek V4 Pro · 10.3s)

_Thinking:_

Now I have a very thorough picture of the filter pushdown and EAV system. Let me update the todos and synthesize findings.

**Tool: todowrite**

**Input:**
```json
{
  "todos": [
    {
      "priority": "high",
      "content": "Read benchmarks to understand current filter-related workloads and baselines",
      "status": "completed"
    },
    {
      "priority": "high",
      "content": "Read federated query harness (query.go) to understand EAV/filter benchmark setup",
      "status": "completed"
    },
    {
      "priority": "high",
      "content": "Read postgres_duckdb_query.go to assess filter pushdown implementation",
      "status": "completed"
    },
    {
      "priority": "high",
      "content": "Read entity_query_service.go to understand query routing and EAV query patterns",
      "status": "completed"
    },
    {
      "priority": "medium",
      "content": "Check benchmark results / baselines for filter workloads",
      "status": "completed"
    },
    {
      "priority": "medium",
      "content": "Summarize current state, gaps, and optimization opportunities",
      "status": "in_progress"
    }
  ]
}
```

**Output:**
```
[
  {
    "content": "Read benchmarks to understand current filter-related workloads and baselines",
    "status": "completed",
    "priority": "high"
  },
  {
    "content": "Read federated query harness (query.go) to understand EAV/filter benchmark setup",
    "status": "completed",
    "priority": "high"
  },
  {
    "content": "Read postgres_duckdb_query.go to assess filter pushdown implementation",
    "status": "completed",
    "priority": "high"
  },
  {
    "content": "Read entity_query_service.go to understand query routing and EAV query patterns",
    "status": "completed",
    "priority": "high"
  },
  {
    "content": "Check benchmark results / baselines for filter workloads",
    "status": "completed",
    "priority": "medium"
  },
  {
    "content": "Summarize current state, gaps, and optimization opportunities",
    "status": "in_progress",
    "priority": "medium"
  }
]
```

---

## Assistant (Build · DeepSeek V4 Pro · 70.2s)

_Thinking:_

Let me now synthesize a comprehensive summary of the current state, gaps, and optimization opportunities.

## Architecture Summary

The system has **two parallel query execution paths**:

### Path A: Benchmark Harness (e2e_harness/federated/query.go)
Used by the benchmark runner. Builds SQL directly with hardcoded tier-aware projections and attribute filters. 
- **For parquet tiers (base/delta)**: Simple `WHERE 1=1 AND attributeFilter AND timeWindowFilter` using projected column names (e.g., `symbol = 'SYM00001'`)
- **For hot tier (Postgres)**: Complex JOINs: `change_log ← entity_main ← eav_data pivot` with EAV attributes pivoted via `MAX(CASE WHEN attr_id = ...)`
- **Filter handling**: Uses `buildAttributeFilterClause` for parquet tiers (column-level pushdown like `AND symbol = '...'`), and `buildHotAttributeFilterClauseTargeted` for hot tier (using `COALESCE(hot_vals.X, em.column)` expressions).
- **Important**: The tradeTime filter for parquet uses `epoch_ms(tradeTime)`, which means tradeTime is stored as a timestamp in Parquet, not an epoch millisecond.

### Path B: Production (postgres_duckdb_query.go)
Used by the entity_query_service for real workload. Goes through `ToDualClauses` → `BuildDuckDBQuery` → `AdvancedQueryTemplateDuckDB`.
- **Filter pushdown**: `ToDualClauses` generates three SQL fragments:
  - `PgMainClause`: Pushdown predicates to Postgres `entity_main` table (column-bound attributes only)
  - `PgClause`: Full EAV-based condition using EXISTS subqueries (legacy, comprehensive but expensive)
  - `DuckClause`: DuckDB logical condition using attribute metadata
- **Template**: `AdvancedQueryTemplateDuckDB` uses `PG_WHERE_CLAUSE` for pushdown and `LOGICAL_WHERE_CLAUSE` for final DuckDB logical filtering
- **Pushdown decision**: `classifyPredicate` in dualpath_sql_generator.go decides whether an attribute can be pushed to main table based on column binding + value type + operator support.

## Key Findings - Gaps and Issues

### 1. Benchmark harness queries ignore the production DuckDB template entirely
The benchmark harness (`e2e_harness/federated/query.go`) builds its own hardcoded SQL that doesn't use the `ToDualClauses` pushdown mechanism at all. It uses simple column-level filters for parquet tiers and hardcoded `COALESCE` expressions for the hot tier. This means:
- **Benchmark optimization won't translate to production improvements** unless both paths are aligned.
- The benchmark harness doesn't exercise the `ToDualClauses` logic or the advanced DuckDB template.
- *Note: "service" workloads DO go through the production path (entity_query_service), but most filter workloads use the harness's ExecuteFederatedQuery directly.*

### 2. EAV filter pushdown in parquet tiers doesn't exist in the benchmark harness
Looking at `buildParquetTierQuery`, it only applies `attributeFilter` and `timeWindowFilter` - these are only for column-projected attributes (symbol, tradeType, etc.). For EAV attributes like `exchange`:
- In the benchmark harness: `exchange` is NOT projected as a column in parquet (it's only in the EAV attributes JSON), so the filter `AND exchange = 'NYSE'` wouldn't even match anything in parquet since there's no column named `exchange`.
- Wait, let me re-check. The benchmark projection `benchmarkParquetProjection` for trade schema DOES project `exchange` as a column. Let me re-examine...

Actually looking more carefully at `benchmarkParquetProjection` for the trade schema (default case):
```sql
SELECT row_id, schema_id, changed_at, deleted_at, name, version, symbol, exchange, region, tradeType, epoch_ms(tradeTime) as tradeTime, '%s' as tier FROM read_parquet('%s')
```

So `exchange` IS projected for parquet reads. But the question is whether it's a real column in the Parquet files or synthesized. Looking at the data generator, the Parquet format has these columns, so they exist.

But for the **hot tier** EAV filter, the situation is different. For EAV-only attributes (like `exchange` which has NO column binding in trade_attributes.json), the harness uses `buildHotAttributeFilterClauseTargeted` which generates `COALESCE(hot_vals.exchange, '')`. The `hot_vals` subquery pivots eav_data using `MAX(CASE WHEN attr_id = ... THEN value_text END) AS exchange`.

### 3. DuckDB EAV attribute filtering is NOT available in cold/warm tiers
In the benchmark harness query, the parquet tiers have projected columns, but EAV attributes stored only in the `attributes_json` column or individual column bindings. However, looking at the data generator more carefully:

Looking at `benchmark/generator.go`, the Parquet files are generated with these columns directly present. So filtering on `exchange` in parquet tiers works because the column IS in the file. But this is only possible because the benchmark datagen puts all attributes as explicit Parquet columns - this wouldn't work for generic form schemas where EAV attributes aren't flat columns in Parquet.

### 4. The production pushdown path has gaps

Looking at `ToDualClauses` / `dualpath_sql_generator.go`:
- `PgMainClause`: Only pushes to Postgres columns with column bindings. Operators supported: equals, starts_with, contains for text; equals, gt, gte, lt, lte, not_equals for numeric; same for date/datetime.
- BUT: The `PgMainClause` conditions are not actually evaluated in any query I can see except through the template! The template uses `{{.PG_WHERE_CLAUSE}}` which only filters `entity_main` rows. The benchmark harness doesn't use this at all.
- `DuckClause`: Uses `resolveDuckDBColumn` which maps EAV attribute names to DuckDB column names using metadata. But this only works for column-bound attributes. For EAV-only attributes (no column binding), the DuckDB clause can't produce meaningful filter predicates.

### 5. `resolveDuckDBColumn` for DuckDB side
Looking at what `resolveDuckDBColumn` does - I need to find it.

Actually, looking at `buildDuckKvClause` more carefully:
```go
colName := resolveDuckDBColumn(c.Attr, cache)
```

For attributes without column bindings (like `exchange`), this would resolve to... the attribute name itself. Let me find the implementation.

Actually, I haven't seen `resolveDuckDBColumn` yet. Let me search for it...

But the key insight is: the DuckDB logical clause maps attributes to column names. For EAV-only attributes without column bindings, these would either:
1. Fail to resolve to any column
2. Use the attribute name directly (which might match if the parquet has it)

### 6. The "service" execution source does exercise the production path
In `runner.go`, workloads with `ExecutionSource: "service"` (customer-region-page, security-symbol-page, baseline-page-1) go through `executeServiceWorkload` which creates a full `entity_query_service`, which goes through the `ToDualClauses` → `advanced_query_template_duckdb` production path. Other filter workloads (hot-selective-page, eav-selective-page, mixed-hot-eav-page, hot-low-selectivity-page) use the direct harness path with hardcoded SQL.

### 7. Specific gaps for optimization

1. **Cold-tier Parquet EAV filtering**: Parquet files in the benchmark have all attributes as columns, but in production, only `entity_main` columns are flat. EAV attributes would need to be filtered through JSON extraction, which is expensive. The benchmark harness currently masks this issue by always projecting all attributes as columns.

2. **Hot-tier scan overhead**: The hot tier always performs 3-table JOIN (change_log → entity_main → eav_data pivot), even for simple queries. The optimized Postgres template in `advanced_query_template.go` also scans eav_data unconditionally.

3. **Filter pushdown not applied to the full cold/warm tier scan**: The benchmark harness applies attribute filters at the tier level, but only for projected columns. There's no mechanism to push EAV-type predicates into the initial scan (reducing rows before the UNION/MERGE).

4. **Missing `orderChannel` and other low-selectivity EAV filters in benchmark workloads**: The calibration file considers `orderChannel` as a low-selectivity EAV candidate, but no workload exercises it.

5. **The AdvancedQueryTemplateDuckDB has hardcoded column names**: The template assumes specific column names (name, age, tag) and fixed attr_id values (205 for tag). This is generic in the template but not dynamically mapped from the schema's actual attributes.

6. **Pushdown efficiency telemetry exists but is limited**: `telemetry.EmitPushdownEfficiency` records a ratio, but the computation uses dirty ID size as a fallback, which skews the metric.

7. **The mixed-filter workload uses `PreferHot: true`**: The `mixed-hot-eav-page` workload forces PreferHot, meaning it never exercises the DuckDB path for mixed filter queries. This should probably be benchmarked both ways.

Let me now write a concise summary.

**Tool: todowrite**

**Input:**
```json
{
  "todos": [
    {
      "priority": "high",
      "content": "Read benchmarks to understand current filter-related workloads and baselines",
      "status": "completed"
    },
    {
      "priority": "high",
      "content": "Read federated query harness (query.go) to understand EAV/filter benchmark setup",
      "status": "completed"
    },
    {
      "priority": "high",
      "content": "Read postgres_duckdb_query.go to assess filter pushdown implementation",
      "status": "completed"
    },
    {
      "priority": "high",
      "content": "Read entity_query_service.go to understand query routing and EAV query patterns",
      "status": "completed"
    },
    {
      "priority": "medium",
      "content": "Check benchmark results / baselines for filter workloads",
      "status": "completed"
    },
    {
      "priority": "medium",
      "content": "Summarize current state, gaps, and optimization opportunities",
      "status": "completed"
    }
  ]
}
```

**Output:**
```
[
  {
    "content": "Read benchmarks to understand current filter-related workloads and baselines",
    "status": "completed",
    "priority": "high"
  },
  {
    "content": "Read federated query harness (query.go) to understand EAV/filter benchmark setup",
    "status": "completed",
    "priority": "high"
  },
  {
    "content": "Read postgres_duckdb_query.go to assess filter pushdown implementation",
    "status": "completed",
    "priority": "high"
  },
  {
    "content": "Read entity_query_service.go to understand query routing and EAV query patterns",
    "status": "completed",
    "priority": "high"
  },
  {
    "content": "Check benchmark results / baselines for filter workloads",
    "status": "completed",
    "priority": "medium"
  },
  {
    "content": "Summarize current state, gaps, and optimization opportunities",
    "status": "completed",
    "priority": "medium"
  }
]
```

---

## Assistant (Build · DeepSeek V4 Pro · 36.6s)

_Thinking:_

Let me provide a comprehensive summary.

## Assessment: Filter Pushdown & EAV Handling State

### Architecture — Two Parallel Query Paths

| Path | Used By | Pushdown Mechanism |
|------|---------|-------------------|
| **Benchmark harness** (`e2e_harness/federated/query.go`) | 9 of 12 workloads (non-"service") | Hardcoded SQL: column-level filters (`AND symbol = '...'`) on parquet; `COALESCE(hot_vals.X, em.column)` on hot tier |
| **Production** (`postgres_duckdb_query.go` → `ToDualClauses` → `AdvancedQueryTemplateDuckDB`) | 3 "service" workloads + real API traffic | `PgMainClause` pushes column-bound attributes to `entity_main`; `DuckClause` applies logical filter via DuckDB; `PgClause` uses EXISTS subqueries for EAV |

### Schema Attribute Layout (Trade)

| Attribute | Column Binding | Type |
|-----------|---------------|------|
| `symbol` | `text_01` | Hot (pushdown-capable) |
| `tradeType` | `smallint_01` | Hot (pushdown-capable) |
| `quantity` | `bigint_01` | Hot |
| `price` | `double_01` | Hot |
| `tradeTime` | `bigint_02` | Hot |
| `customerId` | `uuid_01` | Hot |
| `region` | `text_02` | Hot |
| **`exchange`** | **None** | **EAV-only** |
| `commission` | None | EAV-only |
| `isCash` | None | EAV-only |
| `brokerId` | None | EAV-only |
| `orderChannel` | None | EAV-only |

### Current Filter Workloads in the Benchmark

| Workload | Schema | Attribute | Type | Harness Path |
|----------|--------|-----------|------|-------------|
| `hot-selective-page` | trade | `symbol` | Hot-only | Direct SQL |
| `hot-low-selectivity-page` | trade | `tradeType` | Hot-only (PreferHot) | Direct SQL |
| `eav-selective-page` | trade | `exchange` | **EAV** | Direct SQL |
| `mixed-hot-eav-page` | trade | `symbol`+`exchange` | Mixed (PreferHot) | Direct SQL |
| `customer-region-page` | customer | `region` | Hot | **Service** (prod path) |
| `security-symbol-page` | security | `symbol` | Hot | **Service** (prod path) |

### Key Gaps & Optimization Opportunities

**1. Benchmark harness bypasses production pushdown** — The direct-SQL workloads (hot-selective, eav-selective, mixed) use `buildAttributeFilterClause` with hardcoded column projections. They never exercise `ToDualClauses` or the advanced DuckDB template. Improvements tested here won't validate production path behavior.

**2. EAV filtering in cold tiers is fragile** — The benchmark parquet projection (`query.go:298-308`) hardcodes `exchange`, `region`, etc. as explicit columns in `read_parquet()`. This works for the benchmark schema but masks that real Parquet data from production would store EAV attributes only in `attributes_json`, requiring expensive JSON extraction in DuckDB.

**3. `mixed-hot-eav-page` forces PreferHot** — It never exercises the DuckDB merge path for mixed hot+EAV filters. Should be benchmarked both ways.

**4. No EAV-only low-selectivity workload** — `orderChannel` (attribute ID 12) has no column binding, making it a pure EAV filter. No benchmark exercises it, so pushdown of EAV-only predicates through cold tiers is untested.

**5. Hot-tier scan always does 3-table JOIN** — Even for queries that only need hot-column attributes, the benchmark harness's `buildHotTierQueryTargeted` always joins `change_log ← entity_main ← eav_data pivot`. The production template (`AdvancedQueryTemplateDuckDB`) has the same issue: the `pg_source` CTE always LEFT JOINs eav_data.

**6. Pushdown efficiency metric uses dirty IDs as fallback** — In `postgres_duckdb_query.go:383`, `computePgRowCount` falls back to `len(dirtyIDs)` when no Postgres source rows are recorded, skewing the pushdown ratio metric.

**7. Template has hardcoded schema** — `AdvancedQueryTemplateDuckDB` assumes columns `name, age, tag` and fixed `attr_id = 205`, not dynamically mapped from the actual schema attribute cache.

**8. Missing benchmark for tier-targeted EAV pushdown** — The `buildHotAttributeFilterClauseTargeted` in the harness supports EAV filter expressions like `COALESCE(hot_vals.exchange, '') = 'NYSE'`, but there's no workload measuring how well this predicate reduces rows from each tier independently.

### Recommended Sequence for Issue #51

1. **Add service-path filter workloads** — Convert `eav-selective-page`, `hot-selective-page`, and `mixed-hot-eav-page` to use `ExecutionSource: "service"` so they exercise the `ToDualClauses` → template production path, and add variants without `PreferHot` for mixed-EAV.

2. **Add EAV-only & low-selectivity benchmark workloads** — Add `eav-low-selectivity-page` (e.g., `orderChannel = 'WEB'`) to measure pure EAV filter cost on cold tiers.

3. **Optimize hot-tier EAV scan** — Make the `eav_data` LEFT JOIN conditional in the hot-tier query: skip it when all requested filters and sort keys are column-bound (no EAV attributes needed). The hot tier currently always joins eav_data even when filtering by `symbol` only.

4. **Add tier-specific pushdown benchmarks** — Measure how `attributeFilter` in `buildAttributeFilterClause` vs. `LOGICAL_WHERE_CLAUSE` in the DuckDB template differ in row reduction per tier.

5. **Fix pushdown efficiency metric** — Remove the dirty IDs fallback in `computePgRowCount`.

---

