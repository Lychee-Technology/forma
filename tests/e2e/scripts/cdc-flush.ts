#!/usr/bin/env bun
/**
 * CDC Flush Script
 * Triggers the CDC flush process to export change_log data to S3 as Parquet files
 * 
 * Usage: bun run scripts/cdc-flush.ts [--dry-run]
 */

import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { config } from '../lib/env';
import { runTool, redactArgs } from '../lib/proc';
import { validateCDCOutput, ensureBucket, amzDate, type CDCValidation } from '../lib/s3';

const __dirname = dirname(fileURLToPath(import.meta.url));

interface CDCFlushReport {
  timestamp: string;
  config: {
    pgHost: string;
    pgPort: number;
    pgDatabase: string;
    pgSslMode: string;
    s3Bucket: string;
    s3Prefix: string;
    s3Endpoint: string;
    minRecords: number;
    maxAgeMs: number;
    batchSize: number;
    schemaRegistryTable: string;
    schemaDir: string;
    dryRun: boolean;
  };
  execution: {
    exitCode: number;
    duration_ms: number;
    stdout: string;
    stderr: string;
  };
  result: {
    success: boolean;
    rowsFlushed?: number;
    schemasProcessed?: number;
    parquetFiles?: string[];
    error?: string;
  };
  s3Validation?: CDCValidation;
}

function parseArgs(): { dryRun: boolean } {
  const args = process.argv.slice(2);
  return {
    dryRun: args.includes('--dry-run'),
  };
}

async function runCDCFlush(dryRun: boolean): Promise<CDCFlushReport> {
  const startTime = Date.now();
  
  const report: CDCFlushReport = {
    timestamp: new Date().toISOString(),
    config: {
      pgHost: config.pg.host,
      pgPort: config.pg.port,
      pgDatabase: config.pg.database,
      pgSslMode: config.pg.sslMode,
      s3Bucket: config.s3.bucket,
      s3Prefix: config.s3.prefix,
      s3Endpoint: config.s3.endpoint,
      minRecords: config.cdc.minRecords,
      maxAgeMs: config.cdc.maxAgeMs,
      batchSize: config.cdc.batchSize,
      schemaRegistryTable: config.tables.schemaRegistry,
      schemaDir: config.schemaDir,
      dryRun,
    },
    execution: {
      exitCode: -1,
      duration_ms: 0,
      stdout: '',
      stderr: '',
    },
    result: {
      success: false,
    },
  };

  // Build command arguments
  const args = [
    'cdc-flush',
    '--pg-host', config.pg.host,
    '--pg-port', String(config.pg.port),
    '--pg-user', config.pg.user,
    '--pg-password', config.pg.password,
    '--pg-db', config.pg.database,
    '--pg-ssl-mode', config.pg.sslMode,
    '--change-log-table', config.tables.changeLog,
    '--entity-main-table', config.tables.entityMain,
    '--eav-table', config.tables.eavData,
    '--s3-bucket', config.s3.bucket,
    '--s3-prefix', config.s3.prefix,
    '--s3-endpoint', config.s3.endpoint,
    '--s3-region', 'us-east-1',
    '--s3-use-ssl=' + String(config.s3.useSsl),
    '--s3-use-path=' + String(config.s3.usePath),
    '--min-records', String(config.cdc.minRecords),
    '--max-age-ms', String(config.cdc.maxAgeMs),
    '--batch-size', String(config.cdc.batchSize),
    '--schema-registry-table', config.tables.schemaRegistry,
    '--schema-dir', config.schemaDir,
  ];

  if (config.cdc.estimatedRowBytes > 0) {
    args.push('--estimated-row-bytes', String(config.cdc.estimatedRowBytes));
  }
  if (config.cdc.maxBatchBytes > 0) {
    args.push('--max-batch-bytes', String(config.cdc.maxBatchBytes));
  }

  // Enable manifest tracking
  args.push('--manifest-template', 'manifest/{{.SchemaID}}.json');

  if (dryRun) {
    args.push('--dry-run');
  }

  // Log the command with the pg password redacted — never echo secrets.
  console.log(`Executing: ${config.toolsPath} ${redactArgs(args)}`);

  try {
    // Check if tools binary exists
    const toolsFile = Bun.file(config.toolsPath);
    if (!(await toolsFile.exists())) {
      throw new Error(`CDC tools binary not found at: ${config.toolsPath}. Run 'make build-tools link' first.`);
    }

    // Set environment variables for AWS credentials
    const env = {
      ...process.env,
      AWS_ACCESS_KEY_ID: config.s3.accessKey,
      AWS_SECRET_ACCESS_KEY: config.s3.secretKey,
      AWS_REGION: 'us-east-1',
      PGPASSWORD: config.pg.password,
    };

    const res = await runTool(config.toolsPath, args, {
      env,
      timeoutMs: config.toolTimeoutMs,
      redactValues: [config.pg.password, config.s3.secretKey, config.s3.accessKey],
    });

    report.execution = {
      exitCode: res.exitCode,
      duration_ms: res.durationMs,
      stdout: res.stdout.slice(-10000), // Keep last 10KB
      stderr: res.stderr.slice(-10000),
    };

    if (res.timedOut) {
      report.result.success = false;
      report.result.error = `CDC flush timed out after ${config.toolTimeoutMs}ms and was killed`;
      return report;
    }
    if (res.exitCode !== 0) {
      report.result.success = false;
      report.result.error = res.stderr || res.stdout || `Exit code: ${res.exitCode}`;
      return report;
    }

    // A flush legitimately writes nothing when no schema meets the flush
    // thresholds (min-records / max-age). That is a valid no-op, not a silent
    // failure, so only require parquet when a flush was actually expected.
    const flushSkipped = /skip flush: thresholds not met/i.test(res.stdout + res.stderr);

    // Exit 0 is necessary but not sufficient: validate the actual S3 state.
    // A dry run writes nothing, so skip the object requirement there.
    const validation = await validateCDCOutput({
      dataPrefix: config.s3.prefix,
      manifestPrefix: 'manifest',
      requireParquet: !dryRun && !flushSkipped,
    });
    if (flushSkipped && validation.parquetCount === 0) {
      report.result.error = undefined;
      report.result.success = true;
      report.s3Validation = validation;
      console.log('Note: flush skipped (no schema met thresholds); nothing to validate.');
      return report;
    }
    report.s3Validation = validation;
    report.result.parquetFiles = validation.parquetKeys;
    report.result.schemasProcessed = Object.keys(validation.parquetBySchema).length;

    if (!validation.ok) {
      report.result.success = false;
      report.result.error = `S3 validation failed: ${validation.errors.join('; ')}`;
      return report;
    }

    report.result.success = true;
  } catch (err) {
    report.execution.duration_ms = Date.now() - startTime;
    report.result.success = false;
    report.result.error = String(err);
  }

  return report;
}

async function main() {
  const { dryRun } = parseArgs();

  console.log('='.repeat(60));
  console.log('Forma E2E: CDC Flush');
  console.log('='.repeat(60));
  console.log(`Tools Path: ${config.toolsPath}`);
  console.log(`Postgres: ${config.pg.host}:${config.pg.port}/${config.pg.database}`);
  console.log(`S3 Endpoint: ${config.s3.endpoint}`);
  console.log(`S3 Bucket: ${config.s3.bucket}/${config.s3.prefix}`);
  console.log(`Dry Run: ${dryRun}`);
  console.log('');

  // The CDC flush writes parquet to S3; make sure the bucket exists first so a
  // fresh RustFS volume does not fail the flush.
  await ensureBucket(amzDate(new Date()));

  const report = await runCDCFlush(dryRun);

  console.log('');
  console.log('='.repeat(60));
  console.log('Results');
  console.log('='.repeat(60));
  console.log(`Success: ${report.result.success}`);
  console.log(`Exit Code: ${report.execution.exitCode}`);
  console.log(`Duration: ${report.execution.duration_ms}ms`);
  
  if (report.result.rowsFlushed !== undefined) {
    console.log(`Rows Flushed: ${report.result.rowsFlushed}`);
  }
  if (report.result.schemasProcessed !== undefined) {
    console.log(`Schemas Processed: ${report.result.schemasProcessed}`);
  }
  if (report.result.parquetFiles && report.result.parquetFiles.length > 0) {
    console.log(`Parquet Files (from S3 listing): ${report.result.parquetFiles.length}`);
    report.result.parquetFiles.slice(0, 5).forEach((f) => console.log(`  - ${f}`));
  }
  if (report.s3Validation) {
    const v = report.s3Validation;
    console.log(`S3 Validation: ${v.ok ? 'PASS' : 'FAIL'} — ${v.parquetCount} parquet across ${Object.keys(v.parquetBySchema).length} schema(s), ${v.manifestCount} manifest(s)`);
    if (v.missingFiles.length > 0) {
      console.log(`  Missing (manifest-referenced): ${v.missingFiles.slice(0, 5).join(', ')}`);
    }
  }
  if (report.result.error) {
    console.log(`Error: ${report.result.error}`);
  }

  // Show truncated output
  if (report.execution.stdout) {
    console.log('\n--- stdout (last 500 chars) ---');
    console.log(report.execution.stdout.slice(-500));
  }
  if (report.execution.stderr) {
    console.log('\n--- stderr (last 500 chars) ---');
    console.log(report.execution.stderr.slice(-500));
  }

  // Write report
  const reportPath = resolve(__dirname, '..', 'reports', 'cdc-flush.json');
  await Bun.write(reportPath, JSON.stringify(report, null, 2));
  console.log(`\nReport written to: ${reportPath}`);

  // Exit with appropriate code
  if (!report.result.success) {
    process.exit(1);
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
