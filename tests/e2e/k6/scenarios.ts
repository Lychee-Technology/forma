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

// Custom metrics
const queryDuration = new Trend('forma_query_duration', true);
const queryErrors = new Counter('forma_query_errors');
const querySuccess = new Rate('forma_query_success');
const fedQueryDuration = new Trend('forma_federated_query_duration', true);

// Configuration from environment
const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const AUTH_TOKEN = __ENV.AUTH_TOKEN || '';
const SCHEMAS = (__ENV.SCHEMAS || 'lead,visit,log').split(',');

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
    // Primary SLA: p95 < 200ms for federated queries
    'forma_federated_query_duration': ['p(95)<200'],
    // General query p95 < 100ms
    'forma_query_duration': ['p(95)<100'],
    // Success rate > 99%
    'forma_query_success': ['rate>0.99'],
    // Standard k6 thresholds
    'http_req_duration': ['p(95)<500'],
    'http_req_failed': ['rate<0.01'],
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
  
  queryDuration.add(duration);
  fedQueryDuration.add(duration);
  
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

// Test: Advanced query with conditions (federated path)
function testAdvancedQuery(schema: string): void {
  // Build a simple condition based on schema
  let condition: Record<string, unknown>;
  
  switch (schema) {
    case 'lead':
      condition = {
        field: 'status',
        operator: 'eq',
        value: randomElement(['open', 'won', 'lost']),
      };
      break;
    case 'visit':
      condition = {
        field: 'status',
        operator: 'eq',
        value: randomElement(['scheduled', 'visited', 'no_show']),
      };
      break;
    case 'log':
      condition = {
        field: 'type',
        operator: 'eq',
        value: randomElement(['audio/mp3', 'text/plain', 'image/jpeg']),
      };
      break;
    default:
      condition = {};
  }
  
  const payload = {
    schema_name: schema,
    condition,
    page: randomInt(1, 5),
    items_per_page: randomElement([20, 50, 100]),
  };
  
  const url = `${BASE_URL}/api/v1/advanced_query`;
  const start = Date.now();
  
  const res = http.post(url, JSON.stringify(payload), { headers: getHeaders() });
  const duration = Date.now() - start;
  
  queryDuration.add(duration);
  fedQueryDuration.add(duration);
  
  const success = check(res, {
    'advanced query status 200': (r) => r.status === 200,
    'advanced query has results': (r) => {
      try {
        const body = JSON.parse(r.body as string);
        return body.data !== undefined;
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
  
  const url = `${BASE_URL}/api/v1/search?q=${encodeURIComponent(searchTerm)}&page=1&items_per_page=20`;
  const start = Date.now();
  
  const res = http.get(url, { headers: getHeaders() });
  const duration = Date.now() - start;
  
  queryDuration.add(duration);
  fedQueryDuration.add(duration);
  
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
    } else if (testRoll < 0.80) {
      // 20%: Advanced queries with conditions
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
  
  // Verify API is reachable
  const healthRes = http.get(`${BASE_URL}/api/v1/lead?page=1&items_per_page=1`, {
    headers: getHeaders(),
  });
  
  if (healthRes.status !== 200) {
    console.error(`API health check failed: ${healthRes.status}`);
  }
  
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
