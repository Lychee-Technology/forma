#!/usr/bin/env bun
/**
 * Federated Query Validation
 *
 * Validates that the Forma federated read path agrees with Postgres. It queries
 * /api/v1/advanced_query with federated.enabled=true (so the federated engine,
 * not the plain OLTP list endpoint, serves the read) and compares the result
 * against records reconstructed directly from entity_main + eav_data.
 *
 * Sampling is by identity: a seeded, reproducible set of row IDs is drawn from
 * Postgres, then the same rows are located in the federated result — so the two
 * sides compare the SAME rows and overlap is meaningful (the old script sampled
 * different rows on each side and passed vacuously with zero overlap).
 *
 * Fails on: count mismatch, checksum mismatch, zero sample overlap, or any
 * missing / mismatched record.
 *
 * Usage:
 *   bun run scripts/federated-check.ts --schema lead --sample-size 100
 *   bun run scripts/federated-check.ts --schema all --seed my-seed
 *   bun run scripts/federated-check.ts --full-scan
 */

import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import postgres from 'postgres';
import { config } from '../lib/env';
import { post } from '../lib/http';
import {
  simpleHash,
  compareAttributes,
  getSchemaId,
  getPostgresCount,
  sampleRowIds,
  attributesForRowIds,
  type AttributeMismatch,
} from '../lib/oracle';

const __dirname = dirname(fileURLToPath(import.meta.url));

const DEFAULT_SEED = 'forma-e2e';
const API_PAGE_SIZE = 100; // items_per_page is capped at 100 by the API.

interface Args {
  schema: string;
  sampleSize: number;
  seed: string;
  fullScan: boolean;
}

function parseArgs(): Args {
  const args = process.argv.slice(2);
  let schema = 'all';
  let sampleSize = 100;
  let seed = DEFAULT_SEED;
  let fullScan = false;

  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--schema' && args[i + 1]) {
      schema = args[++i];
    } else if (args[i] === '--sample-size' && args[i + 1]) {
      sampleSize = parseInt(args[++i], 10);
    } else if (args[i] === '--seed' && args[i + 1]) {
      seed = args[++i];
    } else if (args[i] === '--full-scan') {
      fullScan = true;
    }
  }
  return { schema, sampleSize, seed, fullScan };
}

interface DataRecord {
  row_id: string;
  attributes: Record<string, unknown>;
}
interface ExecutionRouting {
  used_duckdb: boolean;
  tiers?: string[];
  reason?: string;
}
interface FederatedQueryResult {
  data: DataRecord[];
  total_records: number;
  execution_plan?: { routing: ExecutionRouting };
}

interface ComparisonResult {
  schema: string;
  timestamp: string;
  formaCount: number;
  postgresCount: number;
  sampleSize: number;
  matchingRecords: number;
  missingInForma: string[];
  formaChecksum: string;
  postgresChecksum: string;
  attributeMismatches: (AttributeMismatch & { rowId: string })[];
  route: string;
  passed: boolean;
  notes: string[];
}

/** POST advanced_query on the federated path with a match-all condition. */
async function queryFederated(
  schemaName: string,
  page: number,
  itemsPerPage: number,
): Promise<FederatedQueryResult | null> {
  const response = await post<FederatedQueryResult>('/api/v1/advanced_query', {
    schema_name: schemaName,
    // Empty AND composite renders to "1=1" server-side (match all).
    condition: { l: 'and', c: [] },
    page,
    items_per_page: itemsPerPage,
    federated: { enabled: true, include_execution_plan: true },
  });
  if (!response.ok || !response.data) {
    console.error(`Federated API error for ${schemaName}: ${response.error}`);
    return null;
  }
  return response.data;
}

function routeLabel(plan?: { routing: ExecutionRouting }): string {
  if (!plan) return 'unknown';
  return plan.routing.used_duckdb ? 'duckdb' : 'postgres-only';
}

/**
 * Page the federated result and keep only rows whose row_id is in targetSet,
 * stopping once all are found or the pages are exhausted. Returns the collected
 * attribute map and the execution route reported for the query.
 */
async function collectFederatedRows(
  schemaName: string,
  targetSet: Set<string>,
  formaCount: number,
): Promise<{ records: Map<string, Record<string, unknown>>; route: string }> {
  const records = new Map<string, Record<string, unknown>>();
  let route = 'unknown';
  const maxPages = Math.ceil(formaCount / API_PAGE_SIZE) + 2;

  for (let page = 1; page <= maxPages && records.size < targetSet.size; page++) {
    const pageResult = await queryFederated(schemaName, page, API_PAGE_SIZE);
    if (!pageResult) break;
    if (page === 1) route = routeLabel(pageResult.execution_plan);
    if (pageResult.data.length === 0) break;
    for (const record of pageResult.data) {
      if (targetSet.has(record.row_id)) {
        records.set(record.row_id, record.attributes);
      }
    }
  }
  return { records, route };
}

async function compareSchema(
  sql: postgres.Sql,
  schemaName: string,
  args: Args,
): Promise<ComparisonResult> {
  console.log(`  Comparing schema: ${schemaName}`);
  const result: ComparisonResult = {
    schema: schemaName,
    timestamp: new Date().toISOString(),
    formaCount: 0,
    postgresCount: 0,
    sampleSize: 0,
    matchingRecords: 0,
    missingInForma: [],
    formaChecksum: '',
    postgresChecksum: '',
    attributeMismatches: [],
    route: 'unknown',
    passed: false,
    notes: [],
  };

  const schemaId = await getSchemaId(sql, schemaName);
  if (schemaId === null) {
    result.notes.push(`Schema '${schemaName}' not found in database`);
    return result;
  }

  result.postgresCount = await getPostgresCount(sql, schemaId);
  const countProbe = await queryFederated(schemaName, 1, 1);
  if (countProbe) {
    result.formaCount = countProbe.total_records;
    result.route = routeLabel(countProbe.execution_plan);
  }

  const countMatch = result.formaCount === result.postgresCount;

  // Draw the identity-based sample from Postgres, then locate the same rows in
  // the federated result so both sides compare exactly these rows.
  const limit = args.fullScan ? 0 : Math.min(args.sampleSize, result.postgresCount);
  const targetIds = await sampleRowIds(sql, schemaId, limit, args.seed);
  const targetSet = new Set(targetIds);
  result.sampleSize = targetIds.length;

  if (targetIds.length === 0) {
    // No rows to sample: the only meaningful check is that both counts agree.
    result.passed = countMatch;
    result.notes.push(countMatch ? 'No records to compare; counts match' : 'Count mismatch with empty sample');
    if (!countMatch) result.notes.push(`Forma=${result.formaCount}, Postgres=${result.postgresCount}`);
    return result;
  }

  const pgRecords = await attributesForRowIds(sql, schemaName, schemaId, targetIds);
  const { records: formaRecords, route } = await collectFederatedRows(schemaName, targetSet, result.formaCount);
  if (route !== 'unknown') result.route = route;

  const formaPresent: string[] = [];
  for (const rowId of targetIds) {
    const formaAttrs = formaRecords.get(rowId);
    const pgAttrs = pgRecords.get(rowId) ?? {};
    if (!formaAttrs) {
      result.missingInForma.push(rowId);
      continue;
    }
    formaPresent.push(rowId);
    const mismatches = compareAttributes(formaAttrs, pgAttrs);
    if (mismatches.length === 0) {
      result.matchingRecords++;
    } else {
      for (const m of mismatches.slice(0, 10)) {
        result.attributeMismatches.push({ rowId, ...m });
      }
    }
  }

  result.formaChecksum = simpleHash([...formaPresent].sort().join(','));
  result.postgresChecksum = simpleHash([...targetIds].sort().join(','));
  const checksumMatch = result.formaChecksum === result.postgresChecksum;
  const hasOverlap = result.matchingRecords > 0;

  // Trim for report readability (after all assertions are computed).
  result.missingInForma = result.missingInForma.slice(0, 20);
  result.attributeMismatches = result.attributeMismatches.slice(0, 50);

  result.passed =
    countMatch && checksumMatch && hasOverlap && result.missingInForma.length === 0 && result.attributeMismatches.length === 0;

  if (!countMatch) result.notes.push(`Count mismatch: Forma=${result.formaCount}, Postgres=${result.postgresCount}`);
  if (!checksumMatch) result.notes.push(`Checksum mismatch: Forma=${result.formaChecksum}, Postgres=${result.postgresChecksum}`);
  if (!hasOverlap) result.notes.push('Zero sample overlap: federated result returned none of the sampled rows');
  if (result.missingInForma.length > 0) result.notes.push(`${result.missingInForma.length} sampled record(s) missing from the federated result`);
  if (result.attributeMismatches.length > 0) result.notes.push(`${result.attributeMismatches.length} attribute mismatch(es)`);
  result.notes.push(`Federated route: ${result.route}`);

  return result;
}

async function main() {
  const args = parseArgs();
  const startedAt = new Date();
  const runId = startedAt.toISOString().replace(/[:.]/g, '-');

  console.log('='.repeat(60));
  console.log('Forma E2E: Federated Query Validation');
  console.log('='.repeat(60));
  console.log(`Forma API: ${config.baseUrl}`);
  console.log(`Postgres: ${config.pg.host}:${config.pg.port}/${config.pg.database}`);
  console.log(`Schema: ${args.schema}`);
  console.log(`Sample Size: ${args.fullScan ? 'full scan' : args.sampleSize}`);
  console.log(`Seed: ${args.seed}`);
  console.log(`Run ID: ${runId}`);
  console.log('');

  const sql = postgres({
    host: config.pg.host,
    port: config.pg.port,
    user: config.pg.user,
    password: config.pg.password,
    database: config.pg.database,
    max: 5,
  });

  const report = {
    runId,
    timestamp: startedAt.toISOString(),
    config: {
      baseUrl: config.baseUrl,
      pgHost: config.pg.host,
      pgDatabase: config.pg.database,
      sampleSize: args.sampleSize,
      seed: args.seed,
      fullScan: args.fullScan,
    },
    results: [] as ComparisonResult[],
    summary: { totalSchemas: 0, passedSchemas: 0, failedSchemas: 0, totalMismatches: 0 },
  };

  try {
    const schemasToCheck = args.schema === 'all' ? ['lead', 'visit', 'log'] : [args.schema];

    for (const schemaName of schemasToCheck) {
      console.log(`\nChecking schema: ${schemaName}`);
      console.log('-'.repeat(40));

      const result = await compareSchema(sql, schemaName, args);
      report.results.push(result);
      report.summary.totalSchemas++;
      report.summary.totalMismatches += result.attributeMismatches.length;
      if (result.passed) {
        report.summary.passedSchemas++;
        console.log('  Status: PASSED');
      } else {
        report.summary.failedSchemas++;
        console.log('  Status: FAILED');
      }
      console.log(`  Forma count: ${result.formaCount}, Postgres count: ${result.postgresCount}`);
      console.log(`  Sample: ${result.sampleSize}, matching: ${result.matchingRecords}, route: ${result.route}`);
      for (const note of result.notes) console.log(`    - ${note}`);
    }

    console.log('');
    console.log('='.repeat(60));
    console.log('Summary');
    console.log('='.repeat(60));
    console.log(`Total schemas: ${report.summary.totalSchemas}`);
    console.log(`Passed: ${report.summary.passedSchemas}`);
    console.log(`Failed: ${report.summary.failedSchemas}`);
    console.log(`Total attribute mismatches: ${report.summary.totalMismatches}`);

    // Versioned, reproducible report filename (run ID + timestamp).
    const reportPath = resolve(__dirname, '..', 'reports', `federated-check-${runId}.json`);
    await Bun.write(reportPath, JSON.stringify(report, null, 2));
    console.log(`\nReport written to: ${reportPath}`);

    if (report.summary.failedSchemas > 0) process.exit(1);
  } finally {
    await sql.end();
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
