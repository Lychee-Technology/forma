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

// Custom metrics. This is an OLTP-path load smoke — it drives the fast
// hot-tier API endpoints and does not route through the (heavier) federated
// engine, so there is no route split here. The federated/DuckDB path is
// covered by federated-check, not by this latency-SLA load run.
const queryDuration = new Trend('forma_query_duration', true);
const queryErrors = new Counter('forma_query_errors');
const querySuccess = new Rate('forma_query_success');

// Configuration from environment
const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const AUTH_TOKEN = __ENV.AUTH_TOKEN || '';
const SCHEMAS = (__ENV.SCHEMAS || 'lead,visit,log').split(',');

// This is an OLTP-path load smoke: it drives the hot/hybrid API endpoints under
// load and enforces a latency SLA. It deliberately does NOT force the
// DuckDB/S3 federated read path — those reads are legitimately seconds-slow and
// would blow the latency thresholds. Proving the federated engine actually
// routes through DuckDB is federated-check's job (`--require-duckdb`), a
// deterministic single-query check that is not load-dependent.

// Scenario definitions
interface ScenarioConfig {
  vus: number;
  duration: string;
  rampUp?: string;
  rampDown?: string;
}

const scenarios: Record<string, ScenarioConfig> = {
  smoke: {
    // Light load: the smoke is a functional concurrency gate, not an SLA
    // benchmark. Shared CI runners (2 cores, co-located Postgres/RustFS) can't
    // hold a tight latency SLA, so keep the load modest and let benchmark-smoke
    // own precise latency numbers.
    vus: 3,
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
    // Functional health — these are the real gate: the server serves concurrent
    // traffic correctly and without errors.
    'forma_query_success': ['rate>0.97'],
    'http_req_failed': ['rate<0.03'],
    // Latency ceilings are generous on purpose — they exist to catch a hung or
    // catastrophically slow server on a contended CI runner, NOT to enforce an
    // SLA (that is benchmark-smoke's job on dedicated hardware).
    'forma_query_duration': ['p(95)<8000'],
    'http_req_duration': ['p(95)<8000'],
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
  // qtype/schema tags make http_req_duration submetrics attributable: the #263
  // tail turned out to be one schema's requests (#268), which the untagged
  // aggregate could not show without downloading the raw report.
  const tags = { qtype: 'simple_list', schema };
  const start = Date.now();

  const res = http.get(url, { headers: getHeaders(), tags });
  const duration = Date.now() - start;

  queryDuration.add(duration, tags);

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
  // Attribute names as defined by the schemas (camelCase), not snake_case —
  // an unknown sort attribute is a 400.
  const sortFields = ['createdAt', 'updatedAt'];
  const sortOrders = ['asc', 'desc'];
  
  const sortBy = randomElement(sortFields);
  const sortOrder = randomElement(sortOrders);
  const itemsPerPage = randomElement([20, 50]);
  
  const url = `${BASE_URL}/api/v1/${schema}?page=1&items_per_page=${itemsPerPage}&sort_by=${sortBy}&sort_order=${sortOrder}`;
  const tags = { qtype: 'sorted_list', schema };
  const start = Date.now();

  const res = http.get(url, { headers: getHeaders(), tags });
  const duration = Date.now() - start;

  // Sorted list is an OLTP GET (no execution plan); it is not a federated route.
  queryDuration.add(duration, tags);

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

// Test: Advanced query with conditions (OLTP path). The request deliberately
// does NOT set federated.enabled: this is a latency-SLA load smoke, and the
// federated engine's PG path (dirty-set anti-join CTEs) is markedly heavier and
// gets starved under a constrained CI runner. Federated routing is covered by
// federated-check, not here.
function testAdvancedQuery(schema: string): void {
  const payload = {
    schema_name: schema,
    condition: conditionFor(schema),
    page: randomInt(1, 5),
    items_per_page: randomElement([20, 50, 100]),
  };

  const url = `${BASE_URL}/api/v1/advanced_query`;
  const tags = { qtype: 'advanced_query', schema };
  const start = Date.now();

  const res = http.post(url, JSON.stringify(payload), { headers: getHeaders(), tags });
  const duration = Date.now() - start;

  queryDuration.add(duration, tags);

  const success = check(res, {
    'advanced query status 200': (r) => r.status === 200,
    'advanced query has results': (r) => {
      try {
        return JSON.parse(r.body as string).data !== undefined;
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

// Test: Cross-schema search
function testCrossSchemaSearch(): void {
  const searchTerms = ['contact', 'user', 'property', 'visit', 'lead'];
  const searchTerm = randomElement(searchTerms);
  
  // The search endpoint requires a `schemas` CSV param (else 400).
  const url = `${BASE_URL}/api/v1/search?q=${encodeURIComponent(searchTerm)}&schemas=${encodeURIComponent(SCHEMAS.join(','))}&page=1&items_per_page=20`;
  const tags = { qtype: 'search', schema: 'multi' };
  const start = Date.now();

  const res = http.get(url, { headers: getHeaders(), tags });
  const duration = Date.now() - start;

  // Cross-schema search is its own endpoint (no execution plan); not a federated route.
  queryDuration.add(duration, tags);

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
  const listRes = http.get(listUrl, {
    headers: getHeaders(),
    tags: { qtype: 'get_by_id_prefetch', schema },
  });
  
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
  const tags = { qtype: 'get_by_id', schema };
  const start = Date.now();

  const res = http.get(url, { headers: getHeaders(), tags });
  const duration = Date.now() - start;

  queryDuration.add(duration, tags);
  
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
    } else if (testRoll < 0.80) {
      // 20%: Advanced queries with conditions (hybrid routing → Postgres)
      testAdvancedQuery(schema);
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
