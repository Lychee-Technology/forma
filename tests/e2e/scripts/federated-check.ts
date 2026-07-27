#!/usr/bin/env bun
/**
 * Federated Query Validation
 *
 * Validates that the Forma federated read path agrees with Postgres at the
 * row-identity level. It queries /api/v1/advanced_query with
 * federated.enabled=true (so the federated engine, not the plain OLTP list
 * endpoint, serves the read) and checks total count, row-ID set membership, and
 * a checksum over row IDs against an independent Postgres source.
 *
 * Sampling is by identity: a seeded, reproducible set of row IDs is drawn from
 * Postgres, then the same rows are located in the federated result — so the two
 * sides compare the SAME rows and overlap is meaningful (the old script sampled
 * different rows on each side and passed vacuously with zero overlap).
 *
 * Deep per-attribute comparison is intentionally out of scope (the EAV store
 * would need Forma's full read path to reconstruct nested/array/typed values);
 * attribute-level correctness across tiers is owned by the Go production
 * harness. This is a smoke-level agreement check.
 *
 * Fails on: count mismatch, checksum mismatch, zero sample overlap, or any
 * sampled row missing from the federated result.
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
import { checksumRowIds, getSchemaId, getPostgresCount, sampleRowIds } from '../lib/oracle';

const __dirname = dirname(fileURLToPath(import.meta.url));

const DEFAULT_SEED = 'forma-e2e';
const API_PAGE_SIZE = 100; // items_per_page is capped at 100 by the API.

interface Args {
  schema: string;
  sampleSize: number;
  seed: string;
  fullScan: boolean;
  requireDuckDB: boolean;
  manifestReads: boolean;
}

function parseArgs(): Args {
  const args = process.argv.slice(2);
  let schema = 'all';
  let sampleSize = 100;
  let seed = DEFAULT_SEED;
  let fullScan = false;
  // Also honor an env flag so CI can require the DuckDB route without args.
  let requireDuckDB = process.env.REQUIRE_DUCKDB === '1' || process.env.REQUIRE_DUCKDB === 'true';
  // Same idea for the manifest-driven read mode (see queryFederated).
  let manifestReads = process.env.MANIFEST_READS === '1' || process.env.MANIFEST_READS === 'true';

  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--schema' && args[i + 1]) {
      schema = args[++i];
    } else if (args[i] === '--sample-size' && args[i + 1]) {
      sampleSize = parseInt(args[++i], 10);
    } else if (args[i] === '--seed' && args[i + 1]) {
      seed = args[++i];
    } else if (args[i] === '--full-scan') {
      fullScan = true;
    } else if (args[i] === '--require-duckdb') {
      requireDuckDB = true;
    } else if (args[i] === '--manifest-reads') {
      manifestReads = true;
    }
  }
  return { schema, sampleSize, seed, fullScan, requireDuckDB, manifestReads };
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
  route: string;
  passed: boolean;
  notes: string[];
}

/**
 * POST advanced_query on the federated path with a match-all condition. When
 * forceDuckDB is set, preferred_tiers excludes hot ("warm","cold"), which the
 * hybrid router treats as cold-only and serves from DuckDB regardless of page
 * size — the only way to reach the DuckDB/S3 path via the API (hot page sizes
 * <1000 otherwise route to Postgres). Requires the rows to be flushed to
 * warm/cold parquet first.
 */
async function queryFederated(
  schemaName: string,
  page: number,
  itemsPerPage: number,
  forceDuckDB: boolean,
  manifestReads: boolean,
): Promise<FederatedQueryResult | null> {
  const federated: Record<string, unknown> = { enabled: true, include_execution_plan: true };
  if (forceDuckDB) {
    federated.preferred_tiers = ['warm', 'cold'];
    if (!manifestReads) {
      // The DuckDB read needs to know where the parquet lives. Point read_parquet
      // at the flushed delta layout (s3://<bucket>/<prefix>/<schemaID>/*.parquet),
      // mirroring how the Go production harness supplies its glob. Skipped under
      // --manifest-reads: an explicit hint always wins over the server's manifest
      // source (duckdb_query_build.go), so sending it would leave the
      // manifest-driven path untested (#302).
      federated.s3_parquet_path_template = `s3://${config.s3.bucket}/${config.s3.prefix}/{{.SchemaID}}/*.parquet`;
    }
  }
  const response = await post<FederatedQueryResult>('/api/v1/advanced_query', {
    schema_name: schemaName,
    // Empty AND composite renders to "1=1" server-side (match all).
    condition: { l: 'and', c: [] },
    page,
    items_per_page: itemsPerPage,
    federated,
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
 * Page the federated result and collect which target row IDs are present,
 * stopping once all are found or the pages are exhausted. Returns the set of
 * found row IDs and the execution route reported for the query.
 */
async function collectFederatedRowIds(
  schemaName: string,
  targetSet: Set<string>,
  formaCount: number,
  forceDuckDB: boolean,
  manifestReads: boolean,
): Promise<{ found: Set<string>; route: string }> {
  const found = new Set<string>();
  let route = 'unknown';
  const maxPages = Math.ceil(formaCount / API_PAGE_SIZE) + 2;

  for (let page = 1; page <= maxPages && found.size < targetSet.size; page++) {
    const pageResult = await queryFederated(schemaName, page, API_PAGE_SIZE, forceDuckDB, manifestReads);
    if (!pageResult) break;
    if (page === 1) route = routeLabel(pageResult.execution_plan);
    if (pageResult.data.length === 0) break;
    for (const record of pageResult.data) {
      if (targetSet.has(record.row_id)) found.add(record.row_id);
    }
  }
  return { found, route };
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
  const countProbe = await queryFederated(schemaName, 1, 1, args.requireDuckDB, args.manifestReads);
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

  const { found, route } = await collectFederatedRowIds(
    schemaName,
    targetSet,
    result.formaCount,
    args.requireDuckDB,
    args.manifestReads,
  );
  if (route !== 'unknown') result.route = route;

  for (const rowId of targetIds) {
    if (found.has(rowId)) result.matchingRecords++;
    else result.missingInForma.push(rowId);
  }

  // Checksum over the row-ID set each side presents for the sample.
  result.formaChecksum = checksumRowIds([...found]);
  result.postgresChecksum = checksumRowIds(targetIds);
  const checksumMatch = result.formaChecksum === result.postgresChecksum;
  const hasOverlap = result.matchingRecords > 0;
  const allFound = result.missingInForma.length === 0;
  // When required, the read must have actually gone through DuckDB/S3 — a
  // postgres-only route would silently reduce this to a hot-tier check.
  const routeOk = !args.requireDuckDB || result.route === 'duckdb';

  result.passed = countMatch && checksumMatch && hasOverlap && allFound && routeOk;

  if (!countMatch) result.notes.push(`Count mismatch: Forma=${result.formaCount}, Postgres=${result.postgresCount}`);
  if (!checksumMatch) result.notes.push(`Checksum mismatch: Forma=${result.formaChecksum}, Postgres=${result.postgresChecksum}`);
  if (!hasOverlap) result.notes.push('Zero sample overlap: federated result returned none of the sampled rows');
  if (!allFound) result.notes.push(`${result.missingInForma.length} sampled row(s) missing from the federated result`);
  if (!routeOk) result.notes.push(`Expected DuckDB route but got '${result.route}' (is the server DUCKDB_ENABLED and are rows flushed?)`);
  // Trim for report readability (after assertions are computed).
  result.missingInForma = result.missingInForma.slice(0, 20);
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
    summary: { totalSchemas: 0, passedSchemas: 0, failedSchemas: 0, totalMissing: 0 },
  };

  try {
    const schemasToCheck = args.schema === 'all' ? ['lead', 'visit', 'log'] : [args.schema];

    for (const schemaName of schemasToCheck) {
      console.log(`\nChecking schema: ${schemaName}`);
      console.log('-'.repeat(40));

      const result = await compareSchema(sql, schemaName, args);
      report.results.push(result);
      report.summary.totalSchemas++;
      report.summary.totalMissing += result.missingInForma.length;
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
    console.log(`Total sampled rows missing from federated result: ${report.summary.totalMissing}`);

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
