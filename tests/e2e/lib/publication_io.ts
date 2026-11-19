/**
 * PostgreSQL and S3 access for the Protocol V2 publication checks
 * (publication.ts): the records a deployment's init-db wrote, and the
 * manifests and objects in the CDC bucket.
 */

import postgres from 'postgres';
import { config } from './env';
import { createS3Client } from './s3';
import { publicationRecordTable, type PublicationRecord, type PublicationStore } from './publication';

/** Open a connection to the E2E database. The caller ends it. */
export function openDatabase(): postgres.Sql {
  return postgres({
    host: config.pg.host,
    port: config.pg.port,
    user: config.pg.user,
    password: config.pg.password,
    database: config.pg.database,
    max: 1,
  });
}

/** PostgreSQL's undefined_table SQLSTATE. */
const UNDEFINED_TABLE = '42P01';

/**
 * Read every publication record of the deployment's entity main table, by
 * schema id. A database without the publication table (init-db never ran
 * there) has none.
 */
export async function readPublicationRecords(sql: postgres.Sql): Promise<Map<number, PublicationRecord>> {
  const table = publicationRecordTable(config.tables.entityMain);
  const out = new Map<number, PublicationRecord>();
  let rows: any[];
  try {
    rows = await sql.unsafe(
      `SELECT schema_id, epoch, state, generation, manifest_key, manifest_sha256 FROM ${table}`,
    );
  } catch (err: any) {
    if (err?.code === UNDEFINED_TABLE) return out;
    throw err;
  }
  for (const r of rows) {
    out.set(Number(r.schema_id), {
      schemaId: Number(r.schema_id),
      epoch: Number(r.epoch),
      state: String(r.state),
      generation: Number(r.generation),
      manifestKey: String(r.manifest_key),
      manifestSha256: String(r.manifest_sha256),
    });
  }
  return out;
}

/** Read the ids of every schema in the registry table. */
export async function readRegisteredSchemaIds(sql: postgres.Sql): Promise<number[]> {
  const rows = await sql.unsafe(`SELECT schema_id FROM ${config.tables.schemaRegistry} ORDER BY schema_id`);
  return rows.map((r: any) => Number(r.schema_id));
}

/** The CDC bucket, as the publication checks reach it. */
export function bucketStore(): PublicationStore {
  const client = createS3Client();
  return {
    readManifest: async (key) => ((await client.exists(key)) ? await client.file(key).text() : null),
    objectExists: async (key) => await client.exists(key),
  };
}
