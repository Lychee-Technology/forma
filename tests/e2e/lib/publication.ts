/**
 * Protocol V2 publication checks for the E2E wrappers (#896).
 *
 * A schema's published parquet is exactly what its PostgreSQL publication
 * record names: the record points at one immutable manifest generation, and
 * that manifest lists every object a read scans (docs/publication-protocol-v2.md).
 * Objects under the data prefix that no committed generation lists are
 * invisible, and a generation may list nothing at all (a zero-row init). So a
 * wrapper checks the generation the record committed, not what a listing of
 * the prefix happens to hold.
 *
 * This module holds the pure checks; the callers pass in how to query
 * PostgreSQL and how to reach S3, so the checks run in unit tests without
 * either.
 */

import { createHash } from 'crypto';

/** The record suffix init-db gives the entity main table (publication.RecordTableSuffix). */
const RECORD_TABLE_SUFFIX = '_publication';
/** PostgreSQL's identifier limit and the digest length sqlutil.SuffixedIdentifier uses past it. */
const MAX_IDENTIFIER_LEN = 63;
const DIGEST_LEN = 16;

/**
 * The publication record table of entityMain, spelled as
 * internal/sqlutil.SuffixedIdentifier spells it: the suffix appended, or,
 * past PostgreSQL's 63-byte limit, the name cut and a digest of it inserted.
 * A schema qualifier is kept as given.
 */
export function publicationRecordTable(entityMain: string): string {
  const dot = entityMain.lastIndexOf('.');
  const qualifier = dot >= 0 ? entityMain.slice(0, dot + 1) : '';
  let name = dot >= 0 ? entityMain.slice(dot + 1) : entityMain;
  const quoted = name.startsWith('"') && name.endsWith('"') && name.length >= 2;
  if (quoted) name = name.slice(1, -1);
  let bounded = name + RECORD_TABLE_SUFFIX;
  if (Buffer.byteLength(bounded) > MAX_IDENTIFIER_LEN) {
    const digest = '_' + createHash('sha256').update(name, 'utf8').digest('hex').slice(0, DIGEST_LEN);
    const keep = MAX_IDENTIFIER_LEN - digest.length - RECORD_TABLE_SUFFIX.length;
    bounded = Buffer.from(name, 'utf8').subarray(0, keep).toString('utf8') + digest + RECORD_TABLE_SUFFIX;
  }
  return qualifier + (quoted ? `"${bounded}"` : bounded);
}

/** One schema's publication record, as the checks need it. */
export interface PublicationRecord {
  schemaId: number;
  epoch: number;
  state: string;
  generation: number;
  manifestKey: string;
  manifestSha256: string;
}

/** How the checks reach S3: a manifest's bytes (null when absent), and whether an object exists. */
export interface PublicationStore {
  readManifest(key: string): Promise<string | null>;
  objectExists(key: string): Promise<boolean>;
}

/** What checkCommittedGeneration found for one schema. */
export interface GenerationCheck {
  schemaId: number;
  ok: boolean;
  kind?: string;
  files: string[];
  errors: string[];
}

interface ManifestFile {
  key?: string;
}

interface ManifestDoc {
  schema_id?: number;
  epoch?: number;
  generation?: number;
  kind?: string;
  files?: ManifestFile[];
}

/**
 * Check the generation a schema's record committed: the record is PUBLISHED,
 * its manifest is there with the digest the record names and the identity
 * the record holds, and every object the manifest lists exists. A manifest
 * that lists nothing is valid for an init (the parquet tiers hold nothing)
 * and refused for a flush, which never commits an empty generation.
 */
export async function checkCommittedGeneration(
  record: PublicationRecord | undefined,
  schemaId: number,
  store: PublicationStore,
): Promise<GenerationCheck> {
  const check: GenerationCheck = { schemaId, ok: false, files: [], errors: [] };
  if (!record) {
    check.errors.push(`schema ${schemaId} has no publication record`);
    return check;
  }
  if (record.state !== 'PUBLISHED') {
    check.errors.push(`schema ${schemaId} is ${record.state} at epoch ${record.epoch}: no generation is committed`);
    return check;
  }
  const text = await store.readManifest(record.manifestKey);
  if (text === null) {
    check.errors.push(`schema ${schemaId} generation ${record.generation}: the committed manifest ${record.manifestKey} is missing`);
    return check;
  }
  const digest = createHash('sha256').update(text, 'utf8').digest('hex');
  if (digest !== record.manifestSha256) {
    check.errors.push(
      `schema ${schemaId} generation ${record.generation}: manifest ${record.manifestKey} has digest ${digest}, the record names ${record.manifestSha256}`,
    );
    return check;
  }
  let doc: ManifestDoc;
  try {
    doc = JSON.parse(text);
  } catch (err) {
    check.errors.push(`schema ${schemaId}: manifest ${record.manifestKey} is not valid JSON: ${String(err)}`);
    return check;
  }
  if (doc.schema_id !== schemaId || doc.epoch !== record.epoch || doc.generation !== record.generation) {
    check.errors.push(
      `schema ${schemaId}: manifest ${record.manifestKey} names schema ${doc.schema_id} epoch ${doc.epoch} generation ${doc.generation}, ` +
        `the record epoch ${record.epoch} generation ${record.generation}`,
    );
    return check;
  }
  check.kind = doc.kind;
  const files = doc.files ?? [];
  if (files.length === 0 && doc.kind !== 'init') {
    check.errors.push(`schema ${schemaId} generation ${record.generation}: a ${doc.kind} generation lists no object`);
  }
  for (const f of files) {
    if (!f.key) {
      check.errors.push(`schema ${schemaId}: manifest ${record.manifestKey} lists an entry without a key`);
      continue;
    }
    check.files.push(f.key);
    if (!(await store.objectExists(f.key))) {
      check.errors.push(`schema ${schemaId} generation ${record.generation}: listed object ${f.key} is missing`);
    }
  }
  check.ok = check.errors.length === 0;
  return check;
}

/** The ids of the schemas whose record points at a committed generation, sorted. */
export function publishedSchemaIds(records: Map<number, PublicationRecord>): number[] {
  return [...records.values()]
    .filter((r) => r.state === 'PUBLISHED')
    .map((r) => r.schemaId)
    .sort((a, b) => a - b);
}

/**
 * The prefixes an S3 reset clears: the configured data prefix, where the V2
 * writers publish, and the legacy delta/, base/ and manifest/ prefixes, each
 * once and without trailing slashes. An empty data prefix (the bucket root)
 * is never one of them.
 */
export function cleanPrefixes(dataPrefix: string): string[] {
  const prefixes = [dataPrefix, 'delta', 'base', 'manifest']
    .map((p) => p.replace(/\/+$/, ''))
    .filter((p) => p !== '');
  return [...new Set(prefixes)];
}
