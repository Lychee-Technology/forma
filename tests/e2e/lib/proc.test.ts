import { describe, expect, test } from 'bun:test';
import { redactArgs, redactSecrets } from './proc';

describe('redactArgs', () => {
  test('redacts the value after --pg-password (space form)', () => {
    const out = redactArgs(['cdc-flush', '--pg-user', 'postgres', '--pg-password', 'super-secret', '--pg-db', 'forma']);
    expect(out).not.toContain('super-secret');
    expect(out).toContain('--pg-password ***');
    expect(out).toContain('--pg-user postgres');
  });

  test('redacts the =value form', () => {
    const out = redactArgs(['--s3-secret-key=topsecret', '--foo=bar']);
    expect(out).not.toContain('topsecret');
    expect(out).toContain('--s3-secret-key=***');
    expect(out).toContain('--foo=bar');
  });

  test('leaves non-secret args untouched', () => {
    const out = redactArgs(['--s3-endpoint', 'http://localhost:9000', '--batch-size', '500']);
    expect(out).toBe('--s3-endpoint http://localhost:9000 --batch-size 500');
  });
});

describe('redactSecrets', () => {
  test('masks every occurrence of each secret value', () => {
    const out = redactSecrets('connecting password=hunter2 ... retry password=hunter2', ['hunter2', 'miniokey']);
    expect(out).not.toContain('hunter2');
    expect(out).toBe('connecting password=*** ... retry password=***');
  });

  test('ignores empty/short values to avoid masking noise', () => {
    expect(redactSecrets('abc def', ['', 'x'])).toBe('abc def');
  });
});
