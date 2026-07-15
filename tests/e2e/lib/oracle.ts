/**
 * Postgres-side oracle helpers for federated-check.
 *
 * These read entity_main directly so the federated API can be validated
 * against an independent source at the row-identity level: total count, the
 * set of live row IDs, and a checksum over them. Deep per-attribute comparison
 * is intentionally NOT done here — the EAV store keeps nested objects as dotted
 * sub-attributes, arrays as multiple rows, and typed/hot-field values, so a
 * faithful reconstruction would duplicate Forma's read path. Attribute-level
 * correctness across tiers is owned by the Go production harness.
 *
 * Sampling is seeded (md5(row_id || seed)) so a run is reproducible and covers
 * the whole dataset rather than only the most-recent rows.
 */

import postgres from 'postgres';
import { config } from './env';

/** 32-bit rolling hash rendered as zero-padded hex, used for the row-id checksum. */
export function simpleHash(str: string): string {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = (hash << 5) - hash + str.charCodeAt(i);
    hash = hash & hash;
  }
  return Math.abs(hash).toString(16).padStart(8, '0');
}

/** Checksum over a set of row IDs (order-insensitive). */
export function checksumRowIds(rowIds: string[]): string {
  return simpleHash([...rowIds].sort().join(','));
}

export async function getSchemaId(sql: postgres.Sql, schemaName: string): Promise<number | null> {
  const result = await sql`
    SELECT schema_id FROM ${sql(config.tables.schemaRegistry)}
    WHERE schema_name = ${schemaName} LIMIT 1
  `;
  return result.length > 0 ? Number(result[0].schema_id) : null;
}

export async function getPostgresCount(sql: postgres.Sql, schemaId: number): Promise<number> {
  const result = await sql`
    SELECT COUNT(*) as count FROM ${sql(config.tables.entityMain)}
    WHERE ltbase_schema_id = ${schemaId} AND ltbase_deleted_at IS NULL
  `;
  return parseInt(result[0].count as string, 10);
}

/**
 * Select a deterministic, seed-reproducible sample of live row IDs. Ordering by
 * md5(row_id || seed) spreads the sample across the whole dataset (not just the
 * newest rows) while staying identical across runs with the same seed. A limit
 * of 0 returns every live row (full-scan mode).
 */
export async function sampleRowIds(
  sql: postgres.Sql,
  schemaId: number,
  limit: number,
  seed: string,
): Promise<string[]> {
  const rows =
    limit > 0
      ? await sql`
          SELECT ltbase_row_id::text as row_id
          FROM ${sql(config.tables.entityMain)}
          WHERE ltbase_schema_id = ${schemaId} AND ltbase_deleted_at IS NULL
          ORDER BY md5(ltbase_row_id::text || ${seed})
          LIMIT ${limit}
        `
      : await sql`
          SELECT ltbase_row_id::text as row_id
          FROM ${sql(config.tables.entityMain)}
          WHERE ltbase_schema_id = ${schemaId} AND ltbase_deleted_at IS NULL
          ORDER BY md5(ltbase_row_id::text || ${seed})
        `;
  return rows.map((r) => r.row_id as string);
}
