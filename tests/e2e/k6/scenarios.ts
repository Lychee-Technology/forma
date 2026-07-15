/**
 * K6 Load Test Scenarios for Forma Federated Query
 * 
 * Scenarios:
 *   - smoke: 5 VUs for 30s (sanity check)
 *   - full: 30 VUs for 2m (standard load)
 *   - perf: 100 VUs for 5m (performance validation)
 * 
 * Target SLA: p95 < 200ms
 * 
 * Build: bun run build-k6
 * Run:   k6 run -e SCENARIO=smoke k6/dist/bundle.js
 */

import http from 'k6/http';
import { check, sleep, group } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// Custom metrics.
// Route split is derived from the response's execution_plan (only the
// advanced_query path reports one), never from a client-side guess:
//   - forma_federated_query_duration: queries the engine routed to DuckDB
//   - forma_pg_routed_query_duration:  queries the engine served Postgres-only
const queryDuration = new Trend('forma_query_duration', true);
const queryErrors = new Counter('forma_query_errors');
const querySuccess = new Rate('forma_query_success');
const fedQueryDuration = new Trend('forma_federated_query_duration', true);
const pgRoutedQueryDuration = new Trend('forma_pg_routed_query_duration', true);
const routeDuckDB = new Counter('forma_route_duckdb');
const routePostgres = new Counter('forma_route_postgres');

// Configuration from environment
const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const AUTH_TOKEN = __ENV.AUTH_TOKEN || '';
const SCHEMAS = (__ENV.SCHEMAS || 'lead,visit,log').split(',');
// When set, the run must observe at least one DuckDB-routed query (a real
// federated read). Requires the server to be DUCKDB_ENABLED and data flushed.
const REQUIRE_DUCKDB = __ENV.REQUIRE_DUCKDB === '1' || __ENV.REQUIRE_DUCKDB === 'true';
// Schema used for DuckDB-forced requests. Must be a flat schema — nested
// dotted attributes (lead/visit) currently hit a federated-projection binder
// bug in DuckDB; 'log' is flat and reads cleanly.
const DUCKDB_SCHEMA = __ENV.DUCKDB_SCHEMA || 'log';
// Where the flushed parquet lives, for the DuckDB read (matches cdc-flush).
const S3_BUCKET = __ENV.S3_BUCKET || 'forma-cdc';
const S3_PREFIX = __ENV.S3_PREFIX || 'delta';

// Scenario definitions
interface ScenarioConfig {
  vus: number;
  duration: string;
  rampUp?: string;
  rampDown?: string;
}

const scenarios: Record<string, ScenarioConfig> = {
  smoke: {
    vus: 5,
    duration: '30s',
  },
  full: {
    vus: 30,
    duration: '2m',
    rampUp: '30s',
    rampDown: '30s',
  },
  perf: {
    vus: 100,
    duration: '5m',
    rampUp: '1m',
    rampDown: '1m',
  },
};

// Select scenario
const scenarioName = __ENV.SCENARIO || 'smoke';
const scenario = scenarios[scenarioName] || scenarios.smoke;

// K6 options
export const options = {
  scenarios: {
    [scenarioName]: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: buildStages(scenario),
      gracefulRampDown: '10s',
    },
  },
  thresholds: {
    // Primary SLA: p95 < 200ms for DuckDB-routed federated queries
    'forma_federated_query_duration': ['p(95)<200'],
    // Postgres-routed advanced queries should stay on the OLTP budget
    'forma_pg_routed_query_duration': ['p(95)<100'],
    // General query p95 < 100ms
    'forma_query_duration': ['p(95)<100'],
    // Success rate > 99%
    'forma_query_success': ['rate>0.99'],
    // Standard k6 thresholds
    'http_req_duration': ['p(95)<500'],
    'http_req_failed': ['rate<0.01'],
    // When required, at least one query must actually route through DuckDB —
    // otherwise the "federated" load never left Postgres.
    ...(REQUIRE_DUCKDB ? { 'forma_route_duckdb': ['count>0'] } : {}),
  },
};

function buildStages(config: ScenarioConfig): Array<{ duration: string; target: number }> {
  const stages: Array<{ duration: string; target: number }> = [];
  
  // Ramp up
  if (config.rampUp) {
    stages.push({ duration: config.rampUp, target: config.vus });
  } else {
    stages.push({ duration: '10s', target: config.vus });
  }
  
  // Steady state
  stages.push({ duration: config.duration, target: config.vus });
  
  // Ramp down
  if (config.rampDown) {
    stages.push({ duration: config.rampDown, target: 0 });
  } else {
    stages.push({ duration: '10s', target: 0 });
  }
  
  return stages;
}

// Helper to build request headers
function getHeaders(): Record<string, string> {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
  };
  
  if (AUTH_TOKEN) {
    headers['Authorization'] = `Bearer ${AUTH_TOKEN}`;
  }
  
  return headers;
}

// Random selection helper
function randomElement<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)];
}

function randomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

// Test: Simple paginated query (hot path)
function testSimpleQuery(schema: string): void {
  const page = randomInt(1, 10);
  const itemsPerPage = randomElement([10, 20, 50, 100]);
  
  const url = `${BASE_URL}/api/v1/${schema}?page=${page}&items_per_page=${itemsPerPage}`;
  const start = Date.now();
  
  const res = http.get(url, { headers: getHeaders() });
  const duration = Date.now() - start;
  
  queryDuration.add(duration);
  
  const success = check(res, {
    'status is 200': (r) => r.status === 200,
    'response has data': (r) => {
      try {
        const body = JSON.parse(r.body as string);
        return Array.isArray(body.data);
      } catch {
        return false;
      }
    },
  });
  
  if (success) {
    querySuccess.add(1);
  } else {
    querySuccess.add(0);
    queryErrors.add(1);
  }
}

// Test: Query with sorting (exercises index paths)
function testSortedQuery(schema: string): void {
  const sortFields = ['created_at', 'updated_at'];
  const sortOrders = ['asc', 'desc'];
  
  const sortBy = randomElement(sortFields);
  const sortOrder = randomElement(sortOrders);
  const itemsPerPage = randomElement([20, 50]);
  
  const url = `${BASE_URL}/api/v1/${schema}?page=1&items_per_page=${itemsPerPage}&sort_by=${sortBy}&sort_order=${sortOrder}`;
  const start = Date.now();
  
  const res = http.get(url, { headers: getHeaders() });
  const duration = Date.now() - start;
  
  // Sorted list is an OLTP GET (no execution plan); it is not a federated route.
  queryDuration.add(duration);

  const success = check(res, {
    'sorted query status 200': (r) => r.status === 200,
    'sorted query has data': (r) => {
      try {
        const body = JSON.parse(r.body as string);
        return body.data && body.data.length >= 0;
      } catch {
        return false;
      }
    },
  });
  
  if (success) {
    querySuccess.add(1);
  } else {
    querySuccess.add(0);
    queryErrors.add(1);
  }
}

// Build a leaf condition for a schema using the Forma DSL: KvCondition is
// { a: <attr>, v: <op:value> }; a bare value defaults to the equals operator.
function conditionFor(schema: string): Record<string, unknown> {
  switch (schema) {
    case 'lead':
      return { a: 'status', v: randomElement(['open', 'won', 'lost']) };
    case 'visit':
      return { a: 'status', v: randomElement(['scheduled', 'visited', 'no_show']) };
    case 'log':
      return { a: 'type', v: randomElement(['audio/mp3', 'text/plain', 'image/jpeg']) };
    default:
      // Empty AND composite renders to "1=1" (match all).
      return { l: 'and', c: [] };
  }
}

// Test: Advanced query with conditions on the federated path.
// When forceDuckDB is set, preferred_tiers excludes hot so the hybrid router
// serves the query from DuckDB/S3 (the only way to reach the federated path via
// the API at page sizes <1000); requires flushed warm/cold data.
function testAdvancedQuery(schema: string, forceDuckDB: boolean): void {
  const federated: Record<string, unknown> = { enabled: true, include_execution_plan: true };
  if (forceDuckDB) {
    federated.preferred_tiers = ['warm', 'cold'];
    federated.s3_parquet_path_template = `s3://${S3_BUCKET}/${S3_PREFIX}/{{.SchemaID}}/*.parquet`;
  }
  const payload = {
    schema_name: schema,
    condition: conditionFor(schema),
    page: randomInt(1, 5),
    items_per_page: randomElement([20, 50, 100]),
    // Route through the federated engine and ask it to report the route it took.
    federated,
  };

  const url = `${BASE_URL}/api/v1/advanced_query`;
  const start = Date.now();

  const res = http.post(url, JSON.stringify(payload), { headers: getHeaders() });
  const duration = Date.now() - start;

  queryDuration.add(duration);

  let usedDuckDB: boolean | null = null;
  const success = check(res, {
    'advanced query status 200': (r) => r.status === 200,
    'advanced query has results': (r) => {
      try {
        const body = JSON.parse(r.body as string);
        if (body.execution_plan && body.execution_plan.routing) {
          usedDuckDB = body.execution_plan.routing.used_duckdb === true;
        }
        return body.data !== undefined;
      } catch {
        return false;
      }
    },
  });

  // Classify by the route the engine actually reported, not by which function ran.
  if (usedDuckDB === true) {
    fedQueryDuration.add(duration);
    routeDuckDB.add(1);
  } else if (usedDuckDB === false) {
    pgRoutedQueryDuration.add(duration);
    routePostgres.add(1);
  }

  if (success) {
    querySuccess.add(1);
  } else {
    querySuccess.add(0);
    queryErrors.add(1);
  }
}

// Test: Cross-schema search
function testCrossSchemaSearch(): void {
  const searchTerms = ['contact', 'user', 'property', 'visit', 'lead'];
  const searchTerm = randomElement(searchTerms);
  
  const url = `${BASE_URL}/api/v1/search?q=${encodeURIComponent(searchTerm)}&page=1&items_per_page=20`;
  const start = Date.now();
  
  const res = http.get(url, { headers: getHeaders() });
  const duration = Date.now() - start;
  
  // Cross-schema search is its own endpoint (no execution plan); not a federated route.
  queryDuration.add(duration);

  const success = check(res, {
    'search status 200': (r) => r.status === 200,
  });
  
  if (success) {
    querySuccess.add(1);
  } else {
    querySuccess.add(0);
    queryErrors.add(1);
  }
}

// Test: Get single record by ID (requires pre-fetched IDs)
function testGetSingleRecord(schema: string): void {
  // First, get a page of records to extract an ID
  const listUrl = `${BASE_URL}/api/v1/${schema}?page=1&items_per_page=10`;
  const listRes = http.get(listUrl, { headers: getHeaders() });
  
  if (listRes.status !== 200) {
    queryErrors.add(1);
    querySuccess.add(0);
    return;
  }
  
  let rowId: string | null = null;
  try {
    const body = JSON.parse(listRes.body as string);
    if (body.data && body.data.length > 0) {
      rowId = body.data[randomInt(0, body.data.length - 1)].row_id;
    }
  } catch {
    queryErrors.add(1);
    querySuccess.add(0);
    return;
  }
  
  if (!rowId) {
    // No records found, skip
    return;
  }
  
  // Get single record
  const url = `${BASE_URL}/api/v1/${schema}/${rowId}`;
  const start = Date.now();
  
  const res = http.get(url, { headers: getHeaders() });
  const duration = Date.now() - start;
  
  queryDuration.add(duration);
  
  const success = check(res, {
    'get single status 200': (r) => r.status === 200,
    'get single has row_id': (r) => {
      try {
        const body = JSON.parse(r.body as string);
        return body.row_id === rowId;
      } catch {
        return false;
      }
    },
  });
  
  if (success) {
    querySuccess.add(1);
  } else {
    querySuccess.add(0);
    queryErrors.add(1);
  }
}

// Main VU function
export default function (): void {
  const schema = randomElement(SCHEMAS);
  
  // Mix of test scenarios with weighted distribution
  const testRoll = Math.random();
  
  group('forma_queries', () => {
    if (testRoll < 0.40) {
      // 40%: Simple paginated queries (most common)
      testSimpleQuery(schema);
    } else if (testRoll < 0.60) {
      // 20%: Sorted queries
      testSortedQuery(schema);
    } else if (testRoll < 0.70) {
      // 10%: Advanced queries (hybrid routing, typically Postgres for small pages)
      testAdvancedQuery(schema, false);
    } else if (testRoll < 0.80) {
      // 10%: Advanced queries forced onto the DuckDB/S3 federated path (flat schema)
      testAdvancedQuery(DUCKDB_SCHEMA, true);
    } else if (testRoll < 0.90) {
      // 10%: Single record fetches
      testGetSingleRecord(schema);
    } else {
      // 10%: Cross-schema search
      testCrossSchemaSearch();
    }
  });
  
  // Small random sleep to simulate realistic user behavior
  sleep(randomInt(1, 3) / 10);
}

// Setup function - runs once before test
export function setup(): Record<string, unknown> {
  console.log(`Starting ${scenarioName} scenario`);
  console.log(`Base URL: ${BASE_URL}`);
  console.log(`Schemas: ${SCHEMAS.join(', ')}`);
  console.log(`VUs: ${scenario.vus}`);
  console.log(`Duration: ${scenario.duration}`);
  
  // Verify the API is reachable AND that data has been seeded — an empty DB
  // makes every check vacuously true, so fail fast and tell the operator to
  // seed first (run-k6.sh seeds before invoking k6).
  const healthRes = http.get(`${BASE_URL}/api/v1/lead?page=1&items_per_page=1`, {
    headers: getHeaders(),
  });

  if (healthRes.status !== 200) {
    throw new Error(`API health check failed: ${healthRes.status} — is the server up at ${BASE_URL}?`);
  }

  let totalRecords = 0;
  try {
    totalRecords = JSON.parse(healthRes.body as string).total_records ?? 0;
  } catch {
    throw new Error('API health check returned an unparseable body');
  }
  if (totalRecords <= 0) {
    throw new Error(
      "No 'lead' records found — the database is empty. Run 'bun run gen-data' before the load test " +
        '(run-k6.sh does this automatically).',
    );
  }
  console.log(`Health check OK: ${totalRecords} lead record(s) present`);

  return {
    startTime: Date.now(),
    scenario: scenarioName,
  };
}

// Teardown function - runs once after test
export function teardown(data: Record<string, unknown>): void {
  const duration = Date.now() - (data.startTime as number);
  console.log(`\n${data.scenario} scenario completed in ${Math.round(duration / 1000)}s`);
}
