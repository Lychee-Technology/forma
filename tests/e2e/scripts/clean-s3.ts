#!/usr/bin/env bun
/**
 * Clean CDC S3 state for repeatable runs.
 *
 * Deletes every object under the configured data prefix (S3_PREFIX, where
 * cdc-flush and cdc-init publish their Protocol V2 generations).
 * Destructive — run only against a test bucket. Not part of the default
 * `bun run test` chain; invoke explicitly.
 *
 * Protocol V2 (#896): a schema's published parquet is what its PostgreSQL
 * publication record names, and the marks of its flushed change_log entries
 * commit with that record. Deleting the objects a PUBLISHED record points at
 * would leave every read of the schema failing closed on a lost generation,
 * and its flushed rows nowhere else. So this script refuses while the
 * database holds a PUBLISHED record. Reset in this order instead:
 *
 *   1. drop and recreate the E2E database (fresh, isolated state), or point
 *      PG_DB at a new one;
 *   2. run `forma-tools init-db` against it with the schema directory, which
 *      writes every schema's record UNPUBLISHED;
 *   3. run this script, then seed and flush again.
 *
 * Usage: bun run scripts/clean-s3.ts
 */

import { config } from '../lib/env';
import { deleteUnderPrefixes } from '../lib/s3';
import { cleanPrefixes, publishedSchemaIds } from '../lib/publication';
import { openDatabase, readPublicationRecords } from '../lib/publication_io';

/** The ids of the schemas whose record points at a committed generation. */
async function livePointers(): Promise<number[]> {
  const sql = openDatabase();
  try {
    return publishedSchemaIds(await readPublicationRecords(sql));
  } finally {
    await sql.end();
  }
}

async function main() {
  console.log('='.repeat(60));
  console.log('Forma E2E: Clean CDC S3 state');
  console.log('='.repeat(60));
  console.log(`S3 Endpoint: ${config.s3.endpoint}`);
  console.log(`S3 Bucket: ${config.s3.bucket}`);
  console.log(`Postgres: ${config.pg.host}:${config.pg.port}/${config.pg.database}`);

  const live = await livePointers();
  if (live.length > 0) {
    console.error(
      `Refusing to clean S3: the publication records of schemas ${live.join(', ')} are PUBLISHED and point at ` +
        `generations under the bucket. Deleting their objects would make every read of those schemas fail ` +
        `closed and lose their flushed rows. Recreate the E2E database and run forma-tools init-db on it ` +
        `first, then run this script again.`,
    );
    process.exit(2);
  }

  const prefixes = cleanPrefixes(config.s3.prefix);
  console.log(`Prefixes: ${prefixes.join(', ')}`);

  const deleted = await deleteUnderPrefixes(prefixes);
  console.log(`Deleted ${deleted} object(s).`);
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
