#!/usr/bin/env bun
/**
 * Federated Query Validation Script
 * Queries both the Forma federated endpoint and Postgres directly to validate data consistency.
 * 
 * Usage:
 *   bun run scripts/federated-check.ts --schema lead --sample-size 100
 *   bun run scripts/federated-check.ts --schema all --sample-size 50
 *   bun run scripts/federated-check.ts --full-scan
 */

import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { config } from '../lib/env';
import { get, ApiResponse } from '../lib/http';
import postgres from 'postgres';

const __dirname = dirname(fileURLToPath(import.meta.url));

interface Args {
  schema: string;
  sampleSize: number;
  fullScan: boolean;
}

function parseArgs(): Args {
  const args = process.argv.slice(2);
  let schema = 'all';
  let sampleSize = 100;
  let fullScan = false;

  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--schema' && args[i + 1]) {
      schema = args[i + 1];
      i++;
    } else if (args[i] === '--sample-size' && args[i + 1]) {
      sampleSize = parseInt(args[i + 1], 10);
      i++;
    } else if (args[i] === '--full-scan') {
      fullScan = true;
    }
  }

  return { schema, sampleSize, fullScan };
}

interface SchemaInfo {
  schemaId: number;
  name: string;
}

interface EntityRecord {
  row_id: string;
  schema_name: string;
  attributes: Record<string, unknown>;
}

interface QueryResult {
  data: EntityRecord[];
  page: number;
  items_per_page: number;
  total_records: number;
}

interface ComparisonResult {
  schema: string;
  timestamp: string;
  
  // Counts
  formaCount: number;
  postgresCount: number;
  
  // Sample comparison
  sampleSize: number;
  matchingRecords: number;
  missingInForma: string[];
  missingInPostgres: string[];
  attributeMismatches: {
    rowId: string;
    field: string;
    formaValue: unknown;
    postgresValue: unknown;
  }[];
  
  // Checksums (simple hash of sorted row_ids)
  formaChecksum: string;
  postgresChecksum: string;
  
  // Timing
  formaQueryMs: number;
  postgresQueryMs: number;
  
  // Status
  passed: boolean;
  notes: string[];
}

interface FederatedCheckReport {
  timestamp: string;
  config: {
    baseUrl: string;
    pgHost: string;
    pgPort: number;
    pgDatabase: string;
    sampleSize: number;
    fullScan: boolean;
  };
  results: ComparisonResult[];
  summary: {
    totalSchemas: number;
    passedSchemas: number;
    failedSchemas: number;
    totalMismatches: number;
  };
}

// Simple hash function for checksums
function simpleHash(str: string): string {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    const char = str.charCodeAt(i);
    hash = ((hash << 5) - hash) + char;
    hash = hash & hash; // Convert to 32bit integer
  }
  return Math.abs(hash).toString(16).padStart(8, '0');
}

// Normalize attribute value for comparison
function normalizeValue(value: unknown): unknown {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value === 'object') {
    if (Array.isArray(value)) {
      return value.map(normalizeValue);
    }
    const sorted: Record<string, unknown> = {};
    for (const key of Object.keys(value as object).sort()) {
      sorted[key] = normalizeValue((value as Record<string, unknown>)[key]);
    }
    return sorted;
  }
  return value;
}

// Compare two attribute objects
function compareAttributes(
  rowId: string,
  formaAttrs: Record<string, unknown>,
  pgAttrs: Record<string, unknown>
): { field: string; formaValue: unknown; postgresValue: unknown }[] {
  const mismatches: { field: string; formaValue: unknown; postgresValue: unknown }[] = [];
  
  const allKeys = new Set([...Object.keys(formaAttrs), ...Object.keys(pgAttrs)]);
  
  for (const key of allKeys) {
    // Skip internal tracking fields
    if (key.startsWith('_')) continue;
    
    const formaVal = normalizeValue(formaAttrs[key]);
    const pgVal = normalizeValue(pgAttrs[key]);
    
    if (JSON.stringify(formaVal) !== JSON.stringify(pgVal)) {
      mismatches.push({
        field: key,
        formaValue: formaVal,
        postgresValue: pgVal,
      });
    }
  }
  
  return mismatches;
}

async function getSchemaId(sql: postgres.Sql, schemaName: string): Promise<number | null> {
  console.log(`    Fetching schema ID for '${schemaName}', table name: ${config.tables.schemaRegistry}`);
  const result = await sql`
    SELECT schema_id FROM ${sql(config.tables.schemaRegistry)} WHERE schema_name = ${schemaName} LIMIT 1
  `;
  return result.length > 0 ? Number(result[0].schema_id) : null;
}

async function queryFormaAPI(schemaName: string, page: number, itemsPerPage: number): Promise<QueryResult | null> {
  const response = await get<QueryResult>(`/api/v1/${schemaName}`, {
    page,
    items_per_page: itemsPerPage,
  });
  
  if (!response.ok || !response.data) {
    console.error(`Forma API error for ${schemaName}: ${response.error}`);
    return null;
  }
  
  return response.data;
}

async function queryPostgresDirectly(
  sql: postgres.Sql,
  schemaId: number,
  limit: number,
  offset: number
): Promise<{ rowId: string; attributes: Record<string, unknown> }[]> {
  // Query entity_main joined with eav_data to get full records
  const rows = await sql`
    SELECT 
      em.row_id::text as row_id,
      jsonb_object_agg(ed.attr_key, ed.attr_value) as attributes
    FROM ${sql(config.tables.entityMain)} em
    JOIN ${sql(config.tables.eavData)} ed ON em.row_id = ed.row_id
    WHERE em.schema_id = ${schemaId}
      AND em.ltbase_deleted_at IS NULL
    GROUP BY em.row_id
    ORDER BY em.created_at DESC
    LIMIT ${limit}
    OFFSET ${offset}
  `;
  
  return rows.map((row) => ({
    rowId: row.row_id,
    attributes: row.attributes as Record<string, unknown>,
  }));
}

async function getPostgresCount(sql: postgres.Sql, schemaId: number): Promise<number> {
  const result = await sql`
    SELECT COUNT(*) as count 
    FROM ${sql(config.tables.entityMain)} 
    WHERE ltbase_schema_id = ${schemaId} AND deleted_at IS NULL
  `;
  return parseInt(result[0].count as string, 10);
}

async function compareSchema(
  sql: postgres.Sql,
  schemaName: string,
  sampleSize: number,
  fullScan: boolean
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
    missingInPostgres: [],
    attributeMismatches: [],
    formaChecksum: '',
    postgresChecksum: '',
    formaQueryMs: 0,
    postgresQueryMs: 0,
    passed: false,
    notes: [],
  };

  // Get schema ID
  const schemaId = await getSchemaId(sql, schemaName);
  if (schemaId === null) {
    result.notes.push(`Schema '${schemaName}' not found in database`);
    return result;
  }

  // Get counts
  const pgCountStart = Date.now();
  result.postgresCount = await getPostgresCount(sql, schemaId);
  
  // Query Forma API for count
  const formaCountStart = Date.now();
  const formaCountResult = await queryFormaAPI(schemaName, 1, 1);
  if (formaCountResult) {
    result.formaCount = formaCountResult.total_records;
  }
  
  // Determine sample size
  const effectiveSampleSize = fullScan 
    ? Math.max(result.formaCount, result.postgresCount)
    : Math.min(sampleSize, result.formaCount, result.postgresCount);
  result.sampleSize = effectiveSampleSize;

  if (effectiveSampleSize === 0) {
    result.notes.push('No records to compare');
    result.passed = result.formaCount === result.postgresCount;
    return result;
  }

  // Fetch sample from Forma API
  const formaStart = Date.now();
  const formaRecords = new Map<string, Record<string, unknown>>();
  const formaRowIds: string[] = [];
  
  // Paginate through Forma results
  const pageSize = Math.min(500, effectiveSampleSize);
  for (let page = 1; formaRecords.size < effectiveSampleSize; page++) {
    const pageResult = await queryFormaAPI(schemaName, page, pageSize);
    if (!pageResult || pageResult.data.length === 0) break;
    
    for (const record of pageResult.data) {
      if (formaRecords.size >= effectiveSampleSize) break;
      formaRecords.set(record.row_id, record.attributes);
      formaRowIds.push(record.row_id);
    }
  }
  result.formaQueryMs = Date.now() - formaStart;

  // Fetch sample from Postgres directly
  const pgStart = Date.now();
  const pgRecords = new Map<string, Record<string, unknown>>();
  const pgRowIds: string[] = [];
  
  const pgResults = await queryPostgresDirectly(sql, schemaId, effectiveSampleSize, 0);
  for (const row of pgResults) {
    pgRecords.set(row.rowId, row.attributes);
    pgRowIds.push(row.rowId);
  }
  result.postgresQueryMs = Date.now() - pgStart;

  // Calculate checksums
  result.formaChecksum = simpleHash(formaRowIds.sort().join(','));
  result.postgresChecksum = simpleHash(pgRowIds.sort().join(','));

  // Compare records
  const allRowIds = new Set([...formaRowIds, ...pgRowIds]);
  
  for (const rowId of allRowIds) {
    const formaAttrs = formaRecords.get(rowId);
    const pgAttrs = pgRecords.get(rowId);
    
    if (!formaAttrs && pgAttrs) {
      result.missingInForma.push(rowId);
    } else if (formaAttrs && !pgAttrs) {
      result.missingInPostgres.push(rowId);
    } else if (formaAttrs && pgAttrs) {
      const mismatches = compareAttributes(rowId, formaAttrs, pgAttrs);
      if (mismatches.length === 0) {
        result.matchingRecords++;
      } else {
        // Only keep first 10 mismatches per record to avoid huge reports
        for (const mismatch of mismatches.slice(0, 10)) {
          result.attributeMismatches.push({
            rowId,
            ...mismatch,
          });
        }
      }
    }
  }

  // Limit arrays for report readability
  result.missingInForma = result.missingInForma.slice(0, 20);
  result.missingInPostgres = result.missingInPostgres.slice(0, 20);
  result.attributeMismatches = result.attributeMismatches.slice(0, 50);

  // Determine pass/fail
  const countMatch = result.formaCount === result.postgresCount;
  const noMissing = result.missingInForma.length === 0 && result.missingInPostgres.length === 0;
  const noMismatches = result.attributeMismatches.length === 0;
  
  result.passed = countMatch && noMissing && noMismatches;
  
  if (!countMatch) {
    result.notes.push(`Count mismatch: Forma=${result.formaCount}, Postgres=${result.postgresCount}`);
  }
  if (result.missingInForma.length > 0) {
    result.notes.push(`${result.missingInForma.length} records missing from Forma API response`);
  }
  if (result.missingInPostgres.length > 0) {
    result.notes.push(`${result.missingInPostgres.length} records missing from Postgres query`);
  }
  if (result.attributeMismatches.length > 0) {
    result.notes.push(`${result.attributeMismatches.length} attribute mismatches found`);
  }

  return result;
}

async function main() {
  const { schema, sampleSize, fullScan } = parseArgs();

  console.log('='.repeat(60));
  console.log('Forma E2E: Federated Query Validation');
  console.log('='.repeat(60));
  console.log(`Forma API: ${config.baseUrl}`);
  console.log(`Postgres: ${config.pg.host}:${config.pg.port}/${config.pg.database}`);
  console.log(`Schema: ${schema}`);
  console.log(`Sample Size: ${fullScan ? 'full scan' : sampleSize}`);
  console.log('');

  // Connect to Postgres
  const sql = postgres({
    host: config.pg.host,
    port: config.pg.port,
    user: config.pg.user,
    password: config.pg.password,
    database: config.pg.database,
    max: 5,
  });

  const report: FederatedCheckReport = {
    timestamp: new Date().toISOString(),
    config: {
      baseUrl: config.baseUrl,
      pgHost: config.pg.host,
      pgPort: config.pg.port,
      pgDatabase: config.pg.database,
      sampleSize,
      fullScan,
    },
    results: [],
    summary: {
      totalSchemas: 0,
      passedSchemas: 0,
      failedSchemas: 0,
      totalMismatches: 0,
    },
  };

  try {
    const schemasToCheck = schema === 'all' ? ['lead', 'visit'] : [schema];

    for (const schemaName of schemasToCheck) {
      console.log(`\nChecking schema: ${schemaName}`);
      console.log('-'.repeat(40));

      const result = await compareSchema(sql, schemaName, sampleSize, fullScan);
      report.results.push(result);
      report.summary.totalSchemas++;

      if (result.passed) {
        report.summary.passedSchemas++;
        console.log(`  Status: PASSED`);
      } else {
        report.summary.failedSchemas++;
        console.log(`  Status: FAILED`);
      }

      console.log(`  Forma count: ${result.formaCount}`);
      console.log(`  Postgres count: ${result.postgresCount}`);
      console.log(`  Sample compared: ${result.sampleSize}`);
      console.log(`  Matching records: ${result.matchingRecords}`);
      console.log(`  Forma query time: ${result.formaQueryMs}ms`);
      console.log(`  Postgres query time: ${result.postgresQueryMs}ms`);

      if (result.notes.length > 0) {
        console.log(`  Notes:`);
        for (const note of result.notes) {
          console.log(`    - ${note}`);
        }
      }

      report.summary.totalMismatches += result.attributeMismatches.length;
    }

    console.log('');
    console.log('='.repeat(60));
    console.log('Summary');
    console.log('='.repeat(60));
    console.log(`Total schemas: ${report.summary.totalSchemas}`);
    console.log(`Passed: ${report.summary.passedSchemas}`);
    console.log(`Failed: ${report.summary.failedSchemas}`);
    console.log(`Total attribute mismatches: ${report.summary.totalMismatches}`);

    // Write report
    const reportPath = resolve(__dirname, '..', 'reports', 'federated-check.json');
    await Bun.write(reportPath, JSON.stringify(report, null, 2));
    console.log(`\nReport written to: ${reportPath}`);

    // Exit with appropriate code
    if (report.summary.failedSchemas > 0) {
      process.exit(1);
    }
  } catch (err) {
    console.error('Fatal error:', err);
  } finally {
    await sql.end();
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
