#!/usr/bin/env bun
/**
 * Compactor Script
 * Runs compaction on S3 parquet files to merge delta files into base tier
 * 
 * Usage: bun run scripts/compactor.ts [--schema-id <id>] [--all]
 * 
 * Options:
 *   --schema-id <id>  Compact a specific schema (default: 101)
 *   --all             Compact all known schemas (101, 102)
 */

import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { config } from '../lib/env';
import { runTool, redactArgs } from '../lib/proc';
import { validateCDCOutput, type CDCValidation } from '../lib/s3';

const __dirname = dirname(fileURLToPath(import.meta.url));

interface CompactorReport {
  timestamp: string;
  config: {
    s3Bucket: string;
    s3Endpoint: string;
    manifestPrefix: string;
    manifestTemplate: string;
    targetBaseSizeMB: number;
    maxDeltaSizeMB: number;
    dirtyRatioPct: number;
    dataPrefix: string;
  };
  schemas: SchemaCompactionResult[];
  summary: {
    totalSchemas: number;
    successCount: number;
    failCount: number;
    totalDuration_ms: number;
  };
}

interface SchemaCompactionResult {
  schemaId: number;
  success: boolean;
  exitCode: number;
  duration_ms: number;
  stdout: string;
  stderr: string;
  error?: string;
  s3Validation?: CDCValidation;
}

function parseArgs(): { schemaIds: number[] } {
  const args = process.argv.slice(2);
  const schemaIds: number[] = [];
  
  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--schema-id' && args[i + 1]) {
      schemaIds.push(parseInt(args[i + 1], 10));
      i++;
    } else if (args[i] === '--all') {
      // Default known schemas
      return { schemaIds: [101, 102] };
    }
  }
  
  // Default to schema 101 if none specified
  if (schemaIds.length === 0) {
    schemaIds.push(101);
  }
  
  return { schemaIds };
}

async function runCompactorForSchema(schemaId: number, reportConfig: CompactorReport['config']): Promise<SchemaCompactionResult> {
  const startTime = Date.now();
  
  const result: SchemaCompactionResult = {
    schemaId,
    success: false,
    exitCode: -1,
    duration_ms: 0,
    stdout: '',
    stderr: '',
  };

  // Build command arguments
  const args = [
    'compactor',
    '--schema-id', String(schemaId),
    '--s3-bucket', config.s3.bucket,
    '--s3-endpoint', config.s3.endpoint,
    '--s3-region', 'us-east-1',
    '--s3-use-path=' + String(config.s3.usePath),
    '--manifest-prefix', reportConfig.manifestPrefix,
    '--manifest-template', reportConfig.manifestTemplate,
    '--target-base-size-mb', String(reportConfig.targetBaseSizeMB),
    '--max-delta-size-mb', String(reportConfig.maxDeltaSizeMB),
    '--dirty-ratio-pct', String(reportConfig.dirtyRatioPct),
    '--data-prefix', reportConfig.dataPrefix,
  ];

  console.log(`\n[Schema ${schemaId}] Executing: ${config.toolsPath} ${redactArgs(args)}`);

  try {
    // Set environment variables for AWS credentials
    const env = {
      ...process.env,
      AWS_ACCESS_KEY_ID: config.s3.accessKey,
      AWS_SECRET_ACCESS_KEY: config.s3.secretKey,
      AWS_REGION: 'us-east-1',
    };

    const res = await runTool(config.toolsPath, args, { env, timeoutMs: config.toolTimeoutMs });

    result.exitCode = res.exitCode;
    result.duration_ms = res.durationMs;
    result.stdout = res.stdout.slice(-5000); // Keep last 5KB
    result.stderr = res.stderr.slice(-5000);

    if (res.timedOut) {
      result.success = false;
      result.error = `compactor timed out after ${config.toolTimeoutMs}ms and was killed`;
      return result;
    }
    if (res.exitCode !== 0) {
      result.success = false;
      result.error = res.stderr || res.stdout || `Exit code: ${res.exitCode}`;
      return result;
    }

    // Compaction merges delta into base, so file counts may shrink; the
    // invariant that must hold is manifest consistency — every parquet the
    // manifest still references must exist on S3.
    const validation = await validateCDCOutput({
      dataPrefix: '',
      manifestPrefix: 'manifest',
      requireParquet: true,
    });
    result.s3Validation = validation;
    if (!validation.ok) {
      result.success = false;
      result.error = `S3 validation failed: ${validation.errors.join('; ')}`;
      return result;
    }

    result.success = true;
  } catch (err) {
    result.duration_ms = Date.now() - startTime;
    result.error = String(err);
  }

  return result;
}

async function main() {
  const { schemaIds } = parseArgs();
  const startTime = Date.now();

  console.log('='.repeat(60));
  console.log('Forma E2E: Compactor');
  console.log('='.repeat(60));
  console.log(`Tools Path: ${config.toolsPath}`);
  console.log(`S3 Endpoint: ${config.s3.endpoint}`);
  console.log(`S3 Bucket: ${config.s3.bucket}`);
  console.log(`Schemas to compact: ${schemaIds.join(', ')}`);
  console.log('');

  // Check if tools binary exists
  const toolsFile = Bun.file(config.toolsPath);
  if (!(await toolsFile.exists())) {
    console.error(`CDC tools binary not found at: ${config.toolsPath}. Run 'make build-tools link' first.`);
    process.exit(1);
  }

  const reportConfig: CompactorReport['config'] = {
    s3Bucket: config.s3.bucket,
    s3Endpoint: config.s3.endpoint,
    manifestPrefix: '', // Root of bucket
    manifestTemplate: 'manifest/{{.SchemaID}}.json',
    targetBaseSizeMB: 256,
    maxDeltaSizeMB: 50,
    dirtyRatioPct: 5,
    dataPrefix: 'data',
  };

  const report: CompactorReport = {
    timestamp: new Date().toISOString(),
    config: reportConfig,
    schemas: [],
    summary: {
      totalSchemas: schemaIds.length,
      successCount: 0,
      failCount: 0,
      totalDuration_ms: 0,
    },
  };

  // Run compaction for each schema
  for (const schemaId of schemaIds) {
    const result = await runCompactorForSchema(schemaId, reportConfig);
    report.schemas.push(result);

    if (result.success) {
      report.summary.successCount++;
      console.log(`[Schema ${schemaId}] SUCCESS (${result.duration_ms}ms)`);
    } else {
      report.summary.failCount++;
      console.log(`[Schema ${schemaId}] FAILED: ${result.error}`);
    }
  }

  report.summary.totalDuration_ms = Date.now() - startTime;

  console.log('');
  console.log('='.repeat(60));
  console.log('Results');
  console.log('='.repeat(60));
  console.log(`Total Schemas: ${report.summary.totalSchemas}`);
  console.log(`Success: ${report.summary.successCount}`);
  console.log(`Failed: ${report.summary.failCount}`);
  console.log(`Total Duration: ${report.summary.totalDuration_ms}ms`);

  // Show details for each schema
  for (const schema of report.schemas) {
    console.log(`\n--- Schema ${schema.schemaId} ---`);
    console.log(`  Status: ${schema.success ? 'SUCCESS' : 'FAILED'}`);
    console.log(`  Exit Code: ${schema.exitCode}`);
    console.log(`  Duration: ${schema.duration_ms}ms`);
    if (schema.s3Validation) {
      const v = schema.s3Validation;
      console.log(`  S3 Validation: ${v.ok ? 'PASS' : 'FAIL'} — ${v.parquetCount} parquet, ${v.manifestCount} manifest(s), ${v.missingFiles.length} missing`);
    }
    if (schema.stderr) {
      console.log(`  Log (last 300 chars): ${schema.stderr.slice(-300)}`);
    }
  }

  // Write report
  const reportPath = resolve(__dirname, '..', 'reports', 'compactor.json');
  await Bun.write(reportPath, JSON.stringify(report, null, 2));
  console.log(`\nReport written to: ${reportPath}`);

  // Exit with appropriate code
  if (report.summary.failCount > 0) {
    process.exit(1);
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
