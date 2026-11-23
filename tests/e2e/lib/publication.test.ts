import { describe, expect, test } from 'bun:test';
import { createHash } from 'crypto';
import {
  checkCommittedGeneration,
  cleanPrefixes,
  publicationRecordTable,
  publishedSchemaIds,
  type PublicationRecord,
  type PublicationStore,
} from './publication';

function fakeStore(objects: Record<string, string>): PublicationStore {
  return {
    readManifest: async (key) => (key in objects ? objects[key] : null),
    objectExists: async (key) => key in objects,
  };
}

function sha(text: string): string {
  return createHash('sha256').update(text, 'utf8').digest('hex');
}

function published(manifestKey: string, manifest: string, generation = 1): PublicationRecord {
  return { schemaId: 101, epoch: 1, state: 'PUBLISHED', generation, manifestKey, manifestSha256: sha(manifest) };
}

const manifestKey = 'delta/101/_manifests/e1/g00000000000000000001-t.json';

function manifest(kind: string, keys: string[]): string {
  return JSON.stringify({ schema_id: 101, epoch: 1, generation: 1, kind, files: keys.map((key) => ({ key })) });
}

describe('checkCommittedGeneration', () => {
  test('accepts a verified empty init generation, with no parquet anywhere', async () => {
    const text = manifest('init', []);
    const check = await checkCommittedGeneration(published(manifestKey, text), 101, fakeStore({ [manifestKey]: text }));
    expect(check.errors).toEqual([]);
    expect(check.ok).toBe(true);
    expect(check.kind).toBe('init');
    expect(check.files).toEqual([]);
  });

  test('accepts an init generation whose listed objects all exist', async () => {
    const text = manifest('init', ['delta/101/e1/base/a.parquet']);
    const store = fakeStore({ [manifestKey]: text, 'delta/101/e1/base/a.parquet': 'PAR1' });
    const check = await checkCommittedGeneration(published(manifestKey, text), 101, store);
    expect(check.ok).toBe(true);
    expect(check.files).toEqual(['delta/101/e1/base/a.parquet']);
  });

  test('rejects a committed manifest that is missing', async () => {
    const text = manifest('init', []);
    const check = await checkCommittedGeneration(published(manifestKey, text), 101, fakeStore({}));
    expect(check.ok).toBe(false);
    expect(check.errors.join(' ')).toContain('is missing');
  });

  test('rejects a listed object that is missing', async () => {
    const text = manifest('init', ['delta/101/e1/base/gone.parquet']);
    const check = await checkCommittedGeneration(published(manifestKey, text), 101, fakeStore({ [manifestKey]: text }));
    expect(check.ok).toBe(false);
    expect(check.errors.join(' ')).toContain('listed object delta/101/e1/base/gone.parquet is missing');
  });

  test('rejects a manifest whose digest is not the one the record names', async () => {
    const text = manifest('init', []);
    const record = { ...published(manifestKey, text), manifestSha256: sha('other') };
    const check = await checkCommittedGeneration(record, 101, fakeStore({ [manifestKey]: text }));
    expect(check.ok).toBe(false);
    expect(check.errors.join(' ')).toContain('digest');
  });

  test('rejects a schema with no committed generation', async () => {
    const unpublished = { schemaId: 101, epoch: 1, state: 'AWAITING_INIT', generation: 0, manifestKey: '', manifestSha256: '' };
    expect((await checkCommittedGeneration(unpublished, 101, fakeStore({}))).ok).toBe(false);
    expect((await checkCommittedGeneration(undefined, 101, fakeStore({}))).ok).toBe(false);
  });

  test('rejects an empty flush generation', async () => {
    const text = manifest('flush', []);
    const check = await checkCommittedGeneration(published(manifestKey, text), 101, fakeStore({ [manifestKey]: text }));
    expect(check.ok).toBe(false);
  });
});

describe('publicationRecordTable', () => {
  test('appends the record suffix', () => {
    expect(publicationRecordTable('entity_main_dev')).toBe('entity_main_dev_publication');
    expect(publicationRecordTable('tenant.entity_main')).toBe('tenant.entity_main_publication');
  });

  test('cuts a long name and inserts its digest, as init-db does', () => {
    const name = 'e'.repeat(60);
    const got = publicationRecordTable(name);
    expect(Buffer.byteLength(got)).toBe(63);
    expect(got.endsWith('_publication')).toBe(true);
    const digest = createHash('sha256').update(name, 'utf8').digest('hex').slice(0, 16);
    expect(got).toBe('e'.repeat(63 - 17 - 12) + '_' + digest + '_publication');
  });
});

describe('S3 reset', () => {
  test('a PUBLISHED record is a live pointer that refuses an S3-only reset', () => {
    const records = new Map<number, PublicationRecord>([
      [102, { schemaId: 102, epoch: 1, state: 'PUBLISHED', generation: 3, manifestKey: 'k', manifestSha256: 's' }],
      [100, { schemaId: 100, epoch: 1, state: 'UNPUBLISHED', generation: 0, manifestKey: '', manifestSha256: '' }],
      [101, { schemaId: 101, epoch: 2, state: 'PUBLISHED', generation: 1, manifestKey: 'k', manifestSha256: 's' }],
      [103, { schemaId: 103, epoch: 1, state: 'AWAITING_INIT', generation: 0, manifestKey: '', manifestSha256: '' }],
    ]);
    expect(publishedSchemaIds(records)).toEqual([101, 102]);
    expect(publishedSchemaIds(new Map())).toEqual([]);
  });

  test('the reset clears the data prefix only', () => {
    expect(cleanPrefixes('cdc/v2/')).toEqual(['cdc/v2']);
    expect(cleanPrefixes('data')).toEqual(['data']);
    expect(cleanPrefixes('')).toEqual([]);
  });
});
