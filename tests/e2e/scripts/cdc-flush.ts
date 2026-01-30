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

  console.log(`Executing: ${config.toolsPath} ${args.join(' ')}`);

  try {
    // Check if tools binary exists
    const toolsFile = Bun.file(config.toolsPath);
    if (!(await toolsFile.exists())) {
      throw new Error(`CDC tools binary not found at: ${config.toolsPath}. Run 'make build-tools' first.`);
    }

    // Set environment variables for AWS credentials
    const env = {
      ...process.env,
      AWS_ACCESS_KEY_ID: config.s3.accessKey,
      AWS_SECRET_ACCESS_KEY: config.s3.secretKey,
      AWS_REGION: 'us-east-1',
      PGPASSWORD: config.pg.password,
    };

    // Run the CDC flush command
    const proc = Bun.spawn([config.toolsPath, ...args], {
      env,
      stdout: 'pipe',
      stderr: 'pipe',
    });

    // Collect output
    const stdoutChunks: Uint8Array[] = [];
    const stderrChunks: Uint8Array[] = [];

    // Read stdout
    const stdoutReader = proc.stdout.getReader();
    while (true) {
      const { done, value } = await stdoutReader.read();
      if (done) break;
      stdoutChunks.push(value);
    }

    // Read stderr
    const stderrReader = proc.stderr.getReader();
    while (true) {
      const { done, value } = await stderrReader.read();
      if (done) break;
      stderrChunks.push(value);
    }

    // Wait for process to exit
    const exitCode = await proc.exited;

    const stdout = new TextDecoder().decode(Buffer.concat(stdoutChunks));
    const stderr = new TextDecoder().decode(Buffer.concat(stderrChunks));

    report.execution = {
      exitCode,
      duration_ms: Date.now() - startTime,
      stdout: stdout.slice(-10000), // Keep last 10KB
      stderr: stderr.slice(-10000),
    };

    // Parse output for metrics
    if (exitCode === 0) {
      report.result.success = true;
      
      // Try to extract metrics from logs
      const rowsMatch = stdout.match(/rows_flushed["\s:=]+(\d+)/i) || stderr.match(/rows_flushed["\s:=]+(\d+)/i);
      if (rowsMatch) {
        report.result.rowsFlushed = parseInt(rowsMatch[1], 10);
      }

      const schemasMatch = stdout.match(/processing schema/gi) || stderr.match(/processing schema/gi);
      if (schemasMatch) {
        report.result.schemasProcessed = schemasMatch.length;
      }

      // Look for parquet file paths
      const parquetMatches = [...(stdout + stderr).matchAll(/delta\/\d+\/[a-f0-9-]+\.parquet/gi)];
      if (parquetMatches.length > 0) {
        report.result.parquetFiles = parquetMatches.map((m) => m[0]);
      }
    } else {
      report.result.success = false;
      report.result.error = stderr || stdout || `Exit code: ${exitCode}`;
    }
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
    console.log(`Parquet Files: ${report.result.parquetFiles.length}`);
    report.result.parquetFiles.slice(0, 5).forEach((f) => console.log(`  - ${f}`));
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
