#!/usr/bin/env bun
/**
 * Clean CDC S3 state for repeatable runs.
 *
 * Deletes every object under the CDC-managed prefixes (delta/, base/,
 * manifest/) so a local or CI run starts from a clean slate instead of
 * accumulating parquet across runs. Destructive — run only against a test
 * bucket. Not part of the default `bun run test` chain; invoke explicitly.
 *
 * Usage: bun run scripts/clean-s3.ts
 */

import { config } from '../lib/env';
import { deleteUnderPrefixes } from '../lib/s3';

async function main() {
  console.log('='.repeat(60));
  console.log('Forma E2E: Clean CDC S3 state');
  console.log('='.repeat(60));
  console.log(`S3 Endpoint: ${config.s3.endpoint}`);
  console.log(`S3 Bucket: ${config.s3.bucket}`);

  const prefixes = ['delta', 'base', 'manifest'];
  console.log(`Prefixes: ${prefixes.join(', ')}`);

  const deleted = await deleteUnderPrefixes(prefixes);
  console.log(`Deleted ${deleted} object(s).`);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
