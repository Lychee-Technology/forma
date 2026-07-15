#!/usr/bin/env bun
/**
 * CDC Init Script
 * Initializes S3 parquet base files from existing entity_main + eav_data
 * This is used when enabling CDC on an existing Forma deployment
 * 
 * Usage: bun run scripts/cdc-init.ts [--dry-run] [--schema-id <id>] [--target-file-size-mb <mb>]
 * 
 * Options:
 *   --dry-run               Run without actually exporting files
 *   --schema-id <id>        Filter to a specific schema ID (default: all)
 *   --target-file-size-mb   Target parquet file size in MB (default: 256)
 */

import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { config } from '../lib/env';
import { runTool, redactArgs } from '../lib/proc';
import { validateCDCOutput, ensureBucket, amzDate, type CDCValidation } from '../lib/s3';

const __dirname = dirname(fileURLToPath(import.meta.url));

interface CDCInitReport {
  timestamp: string;
  config: {
    pgHost: string;
    pgPort: number;
    pgDatabase: string;
    pgSslMode: string;
    s3Bucket: string;
    s3Prefix: string;
    s3Endpoint: string;
    targetFileSizeMB: number;
    schemaRegistryTable: string;
    schemaDir: string;
    schemaIdFilter: number;
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
    rowsExported?: number;
    filesCreated?: number;
    schemasProcessed?: number;
    parquetFiles?: string[];
    error?: string;
  };
  s3Validation?: CDCValidation;
}

function parseArgs(): { dryRun: boolean; schemaId: number; targetFileSizeMB: number } {
  const args = process.argv.slice(2);
  let schemaId = 0;
  let targetFileSizeMB = 256; // Default: 256MB target file size
  
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--schema-id' && args[i + 1]) {
      schemaId = parseInt(args[i + 1], 10);
      i++;
    }
    if (args[i] === '--target-file-size-mb' && args[i + 1]) {
      targetFileSizeMB = parseInt(args[i + 1], 10);
      i++;
    }
  }
  
  return {
    dryRun: args.includes('--dry-run'),
    schemaId,
    targetFileSizeMB,
  };
}

async function runCDCInit(dryRun: boolean, schemaId: number, targetFileSizeMB: number): Promise<CDCInitReport> {
  const startTime = Date.now();
  
  const report: CDCInitReport = {
    timestamp: new Date().toISOString(),
    config: {
      pgHost: config.pg.host,
      pgPort: config.pg.port,
      pgDatabase: config.pg.database,
      pgSslMode: config.pg.sslMode,
      s3Bucket: config.s3.bucket,
      s3Prefix: 'base', // CDC init uses 'base' prefix by default
      s3Endpoint: config.s3.endpoint,
      targetFileSizeMB,
      schemaRegistryTable: config.tables.schemaRegistry,
      schemaDir: config.schemaDir,
      schemaIdFilter: schemaId,
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
    'cdc-init',
    '--pg-host', config.pg.host,
    '--pg-port', String(config.pg.port),
    '--pg-user', config.pg.user,
    '--pg-password', config.pg.password,
    '--pg-db', config.pg.database,
    '--pg-ssl-mode', config.pg.sslMode,
    '--entity-main-table', config.tables.entityMain,
    '--eav-table', config.tables.eavData,
    '--s3-bucket', config.s3.bucket,
    '--s3-prefix', 'base',
    '--s3-endpoint', config.s3.endpoint,
    '--s3-region', 'us-east-1',
    '--s3-use-ssl=' + String(config.s3.useSsl),
    '--s3-use-path=' + String(config.s3.usePath),
    '--target-file-size-mb', String(targetFileSizeMB),
    '--schema-registry-table', config.tables.schemaRegistry,
    '--schema-dir', config.schemaDir,
  ];

  if (schemaId > 0) {
    args.push('--schema-id', String(schemaId));
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

    const res = await runTool(config.toolsPath, args, { env, timeoutMs: config.toolTimeoutMs });

    report.execution = {
      exitCode: res.exitCode,
      duration_ms: res.durationMs,
      stdout: res.stdout.slice(-10000), // Keep last 10KB
      stderr: res.stderr.slice(-10000),
    };

    if (res.timedOut) {
      report.result.success = false;
      report.result.error = `CDC init timed out after ${config.toolTimeoutMs}ms and was killed`;
      return report;
    }
    if (res.exitCode !== 0) {
      report.result.success = false;
      report.result.error = res.stderr || res.stdout || `Exit code: ${res.exitCode}`;
      return report;
    }

    // Validate the actual base-tier S3 state rather than trusting exit code.
    // cdc-init writes to the 'base' prefix; a dry run writes nothing.
    const validation = await validateCDCOutput({
      dataPrefix: 'base',
      manifestPrefix: 'manifest',
      requireParquet: !dryRun,
    });
    report.s3Validation = validation;
    report.result.parquetFiles = validation.parquetKeys;
    report.result.filesCreated = validation.parquetCount;
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
  const { dryRun, schemaId, targetFileSizeMB } = parseArgs();

  console.log('='.repeat(60));
  console.log('Forma E2E: CDC Init');
  console.log('='.repeat(60));
  console.log(`Tools Path: ${config.toolsPath}`);
  console.log(`Postgres: ${config.pg.host}:${config.pg.port}/${config.pg.database}`);
  console.log(`S3 Endpoint: ${config.s3.endpoint}`);
  console.log(`S3 Bucket: ${config.s3.bucket}/base`);
  console.log(`Target File Size: ${targetFileSizeMB}MB`);
  console.log(`Schema ID Filter: ${schemaId === 0 ? 'all' : schemaId}`);
  console.log(`Dry Run: ${dryRun}`);
  console.log('');

  // cdc-init writes base parquet to S3; ensure the bucket exists first.
  await ensureBucket(amzDate(new Date()));

  const report = await runCDCInit(dryRun, schemaId, targetFileSizeMB);

  console.log('');
  console.log('='.repeat(60));
  console.log('Results');
  console.log('='.repeat(60));
  console.log(`Success: ${report.result.success}`);
  console.log(`Exit Code: ${report.execution.exitCode}`);
  console.log(`Duration: ${report.execution.duration_ms}ms`);
  
  if (report.result.rowsExported !== undefined) {
    console.log(`Rows Exported: ${report.result.rowsExported}`);
  }
  if (report.result.filesCreated !== undefined) {
    console.log(`Files Created: ${report.result.filesCreated}`);
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
  const reportPath = resolve(__dirname, '..', 'reports', 'cdc-init.json');
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
