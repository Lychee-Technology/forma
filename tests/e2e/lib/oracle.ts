/**
 * Postgres-side oracle helpers for federated-check.
 *
 * These read entity_main + eav_data directly and reconstruct records, so the
 * federated API can be validated against an independent source. Sampling is
 * seeded (md5(row_id || seed)) so a run is reproducible and covers the whole
 * dataset rather than only the most-recent rows.
 */

import postgres from 'postgres';
import { resolve } from 'path';
import { config } from './env';

export interface AttributeMapping {
  [attrName: string]: { attributeID: number; valueType: string };
}

const attributeMappingCache = new Map<string, AttributeMapping>();

export async function loadAttributeMapping(schemaName: string): Promise<AttributeMapping> {
  const cached = attributeMappingCache.get(schemaName);
  if (cached) return cached;

  const attrFilePath = resolve(config.schemaDir, `${schemaName}_attributes.json`);
  try {
    const content = (await Bun.file(attrFilePath).json()) as AttributeMapping;
    attributeMappingCache.set(schemaName, content);
    return content;
  } catch (err) {
    console.error(`Failed to load attribute mapping for ${schemaName}: ${err}`);
    return {};
  }
}

function buildReverseMapping(mapping: AttributeMapping): Map<number, { name: string; valueType: string }> {
  const reverse = new Map<number, { name: string; valueType: string }>();
  for (const [name, info] of Object.entries(mapping)) {
    reverse.set(info.attributeID, { name, valueType: info.valueType });
  }
  return reverse;
}

/** 32-bit rolling hash rendered as zero-padded hex, used for the row-id checksum. */
export function simpleHash(str: string): string {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = (hash << 5) - hash + str.charCodeAt(i);
    hash = hash & hash;
  }
  return Math.abs(hash).toString(16).padStart(8, '0');
}

/** Recursively normalize a value (sort object keys) so comparison is order-insensitive. */
export function normalizeValue(value: unknown): unknown {
  if (value === null || value === undefined) return null;
  if (typeof value === 'object') {
    if (Array.isArray(value)) return value.map(normalizeValue);
    const sorted: Record<string, unknown> = {};
    for (const key of Object.keys(value as object).sort()) {
      sorted[key] = normalizeValue((value as Record<string, unknown>)[key]);
    }
    return sorted;
  }
  return value;
}

export interface AttributeMismatch {
  field: string;
  formaValue: unknown;
  postgresValue: unknown;
}

/** Compare two attribute maps, skipping internal `_`-prefixed tracking fields. */
export function compareAttributes(
  formaAttrs: Record<string, unknown>,
  pgAttrs: Record<string, unknown>,
): AttributeMismatch[] {
  const mismatches: AttributeMismatch[] = [];
  const allKeys = new Set([...Object.keys(formaAttrs), ...Object.keys(pgAttrs)]);
  for (const key of allKeys) {
    if (key.startsWith('_')) continue;
    const formaVal = normalizeValue(formaAttrs[key]);
    const pgVal = normalizeValue(pgAttrs[key]);
    if (JSON.stringify(formaVal) !== JSON.stringify(pgVal)) {
      mismatches.push({ field: key, formaValue: formaVal, postgresValue: pgVal });
    }
  }
  return mismatches;
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

/** Reconstruct attribute maps for exactly the given row IDs from the EAV table. */
export async function attributesForRowIds(
  sql: postgres.Sql,
  schemaName: string,
  schemaId: number,
  rowIds: string[],
): Promise<Map<string, Record<string, unknown>>> {
  const out = new Map<string, Record<string, unknown>>();
  if (rowIds.length === 0) return out;

  const reverseMapping = buildReverseMapping(await loadAttributeMapping(schemaName));

  const eavRows = await sql`
    SELECT row_id::text as row_id, attr_id, value_text, value_numeric
    FROM ${sql(config.tables.eavData)}
    WHERE schema_id = ${schemaId} AND row_id = ANY(${rowIds}::uuid[])
  `;

  for (const id of rowIds) out.set(id, {});
  for (const row of eavRows) {
    const attrInfo = reverseMapping.get(row.attr_id as number);
    if (!attrInfo) continue;
    const attrs = out.get(row.row_id as string);
    if (!attrs) continue;
    attrs[attrInfo.name] = attrInfo.valueType === 'numeric' ? row.value_numeric : row.value_text;
  }
  return out;
}
