#!/usr/bin/env bun
/**
 * Data Generation Script
 * Generates test data for lead, visit, and log schemas via Forma API
 * 
 * Usage:
 *   bun run scripts/gen-data.ts --schema lead --count 10000 --batch-size 500
 *   bun run scripts/gen-data.ts --schema visit --count 5000 --batch-size 500
 *   bun run scripts/gen-data.ts --schema log --count 5000 --batch-size 500
 *   bun run scripts/gen-data.ts --schema all --count 10000 --batch-size 500
 */

import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { config } from '../lib/env';
import { post, ApiResponse } from '../lib/http';

const __dirname = dirname(fileURLToPath(import.meta.url));

// Parse CLI arguments
function parseArgs(): { schema: string; count: number; batchSize: number } {
  const args = process.argv.slice(2);
  let schema = 'all';
  let count = config.datasetSize;
  let batchSize = config.batchSize;

  for (let i = 0; i < args.length; i++) {
    if (args[i] === '--schema' && args[i + 1]) {
      schema = args[i + 1];
      i++;
    } else if (args[i] === '--count' && args[i + 1]) {
      count = parseInt(args[i + 1], 10);
      i++;
    } else if (args[i] === '--batch-size' && args[i + 1]) {
      batchSize = parseInt(args[i + 1], 10);
      i++;
    }
  }

  return { schema, count, batchSize };
}

// UUID v4 generator (simple implementation)
function uuid(): string {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

// Random helpers
function randomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randomElement<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)];
}

function randomDate(startYear: number = 2023, endYear: number = 2025): string {
  const start = new Date(startYear, 0, 1).getTime();
  const end = new Date(endYear, 11, 31).getTime();
  return new Date(start + Math.random() * (end - start)).toISOString();
}

// Store generated IDs for cross-referencing
const generatedLeadIds: string[] = [];
const generatedVisitIds: string[] = [];

// Lead payload generator
function generateLead(seq: number): Record<string, unknown> {
  const id = uuid();
  generatedLeadIds.push(id);

  const now = new Date().toISOString();
  const pipelines: string[] = ['buy', 'rent', 'sell', 'landlord'] as const;
  const stages: string[] = ['new', 'contacted', 'need_defined', 'viewing', 'offer', 'contract', 'closed'] as const;
  const statuses: string[] = ['open', 'won', 'lost', 'junk'] as const;
  const temperatures: string[] = ['Hot', 'Warm', 'New', 'Qualified'] as const;
  const channels: string[] = ['portal', 'walk_in', 'referral', 'phone', 'web_form', 'event', 'other'] as const;
  const languages: string[] = ['ja', 'en', 'zh', 'ko', 'other'] as const;

  return {
    id,
    tenantId: config.tenantId,
    ownerUserId: `user-${randomInt(1, 100)}`,
    pipeline: randomElement(pipelines),
    stage: randomElement(stages),
    status: randomElement(statuses),
    temperature: randomElement(temperatures),
    score: randomInt(0, 100),
    tags: [`tag-${randomInt(1, 10)}`, `segment-${randomInt(1, 5)}`],
    source: {
      channel: randomElement(channels),
      name: `Source-${randomInt(1, 20)}`,
      campaign: `Campaign-${randomInt(1, 10)}`,
    },
    contact: {
      name: `Contact-${seq}`,
      nameNative: `連絡先-${seq}`,
      email: `contact${seq}@example.com`,
      primaryPhone: `090-${randomInt(1000, 9999)}-${randomInt(1000, 9999)}`,
      phones: [`090-${randomInt(1000, 9999)}-${randomInt(1000, 9999)}`],
      company: `Company-${randomInt(1, 50)}`,
      occupation: randomElement(['Engineer', 'Manager', 'Sales', 'Executive', 'Other']),
      annualIncome: randomInt(3000000, 20000000),
      annualIncomeCurrency: 'JPY',
      familySize: randomInt(0, 5),
      preferredLanguage: randomElement(languages),
    },
    requirement: {
      intentType: randomElement(['buy', 'rent']),
      purpose: randomElement(['primary_residence', 'investment', 'secondary_residence']),
      propertyType: randomElement(['1LDK', '2LDK', '3LDK', '4LDK', 'House']),
      budget: {
        min: randomInt(10000000, 30000000),
        max: randomInt(30000000, 100000000),
        currency: 'JPY',
      },
      areas: [
        {
          prefecture: '東京都',
          city: randomElement(['渋谷区', '港区', '新宿区', '世田谷区', '目黒区']),
        },
      ],
    },
    firstTouchAt: randomDate(2023, 2024),
    createdAt: now,
    updatedAt: now,
    // E2E tracking fields
    _trace_id: `lead-${seq}`,
    _seq: seq,
  };
}

// Visit payload generator
function generateVisit(seq: number): Record<string, unknown> {
  const id = uuid();
  generatedVisitIds.push(id);

  const now = new Date().toISOString();
  const statuses: string[] = ['scheduled', 'visited', 'no_show', 'canceled', 'rescheduled'] as const;

  // Use a random existing lead ID or generate a placeholder
  const leadId = generatedLeadIds.length > 0
    ? randomElement(generatedLeadIds)
    : uuid();

  return {
    id,
    leadId,
    userId: `user-${randomInt(1, 100)}`,
    propertyId: `property-${randomInt(1, 1000)}`,
    propertySnapshot: {
      code: `P-${randomInt(10000, 99999)}`,
      title: `Property ${seq}`,
      address: `${randomInt(1, 50)}-${randomInt(1, 20)}-${randomInt(1, 10)}, Tokyo`,
      price: randomInt(20000000, 200000000),
    },
    scheduledStartAt: randomDate(2024, 2025),
    scheduledEndAt: randomDate(2024, 2025),
    status: randomElement(statuses),
    feedback: seq % 5 === 0 ? `Feedback for visit ${seq}` : undefined,
    attendees: [`user-${randomInt(1, 100)}`],
    createdAt: now,
    updatedAt: now,
    // E2E tracking fields
    _trace_id: `visit-${seq}`,
    _seq: seq,
  };
}

// Log payload generator
function generateLog(seq: number): Record<string, unknown> {
  const now = new Date().toISOString();
  const types: string[] = ['audio/mp3', 'audio/wav', 'image/jpeg', 'image/png', 'text/plain'] as const;

  // Use random existing IDs or generate placeholders
  const leadId = generatedLeadIds.length > 0 ? randomElement(generatedLeadIds) : undefined;
  const visitId = generatedVisitIds.length > 0 ? randomElement(generatedVisitIds) : undefined;

  return {
    id: uuid(),
    ownerId: `user-${randomInt(1, 100)}`,
    leadId,
    visitId: seq % 3 === 0 ? visitId : undefined,
    type: randomElement(types),
    summary: `Log entry ${seq}: ${randomElement(['Call notes', 'Meeting summary', 'Follow-up', 'Property feedback', 'Client request'])}`,
    createdAt: now,
    updatedAt: now,
    // E2E tracking fields
    _trace_id: `log-${seq}`,
    _seq: seq,
  };
}

// Batch create entities
async function createBatch(schemaName: string, entities: Record<string, unknown>[]): Promise<{ succeeded: number; failed: number; errors: string[] }> {
  const response = await post(`/api/v1/${schemaName}`, entities);

  if (response.ok) {
    // Check for partial success in batch response
    const data = response.data as { Successful?: unknown[]; Failed?: { Error: string }[] } | null;
    if (data && 'Successful' in data) {
      const succeeded = data.Successful?.length ?? entities.length;
      const failed = data.Failed?.length ?? 0;
      const errors = data.Failed?.map((f) => f.Error) ?? [];
      return { succeeded, failed, errors };
    }
    return { succeeded: entities.length, failed: 0, errors: [] };
  }

  return { succeeded: 0, failed: entities.length, errors: [response.error ?? `HTTP ${response.status}`] };
}

// Generate data for a schema
async function generateData(
  schemaName: string,
  count: number,
  batchSize: number,
  generator: (seq: number) => Record<string, unknown>
): Promise<{ schema: string; requested: number; succeeded: number; failed: number; duration_ms: number; errors: string[] }> {
  console.log(`\nGenerating ${count} ${schemaName} records in batches of ${batchSize}...`);

  const startTime = Date.now();
  let totalSucceeded = 0;
  let totalFailed = 0;
  const allErrors: string[] = [];

  for (let offset = 0; offset < count; offset += batchSize) {
    const batchCount = Math.min(batchSize, count - offset);
    const batch: Record<string, unknown>[] = [];

    for (let i = 0; i < batchCount; i++) {
      batch.push(generator(offset + i + 1));
    }

    const result = await createBatch(schemaName, batch);
    totalSucceeded += result.succeeded;
    totalFailed += result.failed;
    allErrors.push(...result.errors.slice(0, 5)); // Keep first 5 errors per batch

    const progress = Math.min(100, Math.round(((offset + batchCount) / count) * 100));
    process.stdout.write(`\r  Progress: ${progress}% (${offset + batchCount}/${count}) - Success: ${totalSucceeded}, Failed: ${totalFailed}`);
  }

  console.log(''); // New line after progress

  return {
    schema: schemaName,
    requested: count,
    succeeded: totalSucceeded,
    failed: totalFailed,
    duration_ms: Date.now() - startTime,
    errors: allErrors.slice(0, 20), // Keep first 20 errors total
  };
}

interface DataGenReport {
  timestamp: string;
  config: {
    baseUrl: string;
    tenantId: string;
    datasetSize: number;
    batchSize: number;
  };
  results: {
    schema: string;
    requested: number;
    succeeded: number;
    failed: number;
    duration_ms: number;
    errors: string[];
  }[];
  summary: {
    total_requested: number;
    total_succeeded: number;
    total_failed: number;
    total_duration_ms: number;
  };
}

async function main() {
  const { schema, count, batchSize } = parseArgs();

  console.log('='.repeat(60));
  console.log('Forma E2E: Data Generation');
  console.log('='.repeat(60));
  console.log(`Base URL: ${config.baseUrl}`);
  console.log(`Schema: ${schema}`);
  console.log(`Count per schema: ${count}`);
  console.log(`Batch size: ${batchSize}`);
  console.log(`Tenant ID: ${config.tenantId}`);

  const report: DataGenReport = {
    timestamp: new Date().toISOString(),
    config: {
      baseUrl: config.baseUrl,
      tenantId: config.tenantId,
      datasetSize: count,
      batchSize,
    },
    results: [],
    summary: {
      total_requested: 0,
      total_succeeded: 0,
      total_failed: 0,
      total_duration_ms: 0,
    },
  };

  const schemasToGenerate = schema === 'all' ? ['lead', 'visit'] : [schema];

  for (const schemaName of schemasToGenerate) {
    let generator: (seq: number) => Record<string, unknown>;

    switch (schemaName) {
      case 'lead':
        generator = generateLead;
        break;
      case 'visit':
        generator = generateVisit;
        break;
      case 'log':
        generator = generateLog;
        break;
      default:
        console.error(`Unknown schema: ${schemaName}`);
        continue;
    }

    const result = await generateData(schemaName, count, batchSize, generator);
    report.results.push(result);
    report.summary.total_requested += result.requested;
    report.summary.total_succeeded += result.succeeded;
    report.summary.total_failed += result.failed;
    report.summary.total_duration_ms += result.duration_ms;

    console.log(`  Completed: ${result.succeeded}/${result.requested} in ${result.duration_ms}ms`);
    if (result.errors.length > 0) {
      console.log(`  Errors (first ${Math.min(5, result.errors.length)}):`);
      result.errors.slice(0, 5).forEach((e) => console.log(`    - ${e}`));
    }
  }

  console.log('');
  console.log('='.repeat(60));
  console.log('Summary');
  console.log('='.repeat(60));
  console.log(`Total requested: ${report.summary.total_requested}`);
  console.log(`Total succeeded: ${report.summary.total_succeeded}`);
  console.log(`Total failed: ${report.summary.total_failed}`);
  console.log(`Total duration: ${report.summary.total_duration_ms}ms`);
  console.log(`Throughput: ${Math.round(report.summary.total_succeeded / (report.summary.total_duration_ms / 1000))} records/sec`);

  // Write report
  const reportPath = resolve(__dirname, '..', 'reports', 'data-gen.json');
  await Bun.write(reportPath, JSON.stringify(report, null, 2));
  console.log(`\nReport written to: ${reportPath}`);

  // Exit with error if significant failures
  const failureRate = report.summary.total_failed / report.summary.total_requested;
  if (failureRate > 0.1) {
    console.error(`\nError: Failure rate ${(failureRate * 100).toFixed(1)}% exceeds 10% threshold`);
    process.exit(1);
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
