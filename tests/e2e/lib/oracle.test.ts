import { describe, expect, test } from 'bun:test';
import { simpleHash, normalizeValue, compareAttributes } from './oracle';

describe('oracle comparison', () => {
  test('simpleHash is deterministic and order-sensitive', () => {
    expect(simpleHash('a,b,c')).toBe(simpleHash('a,b,c'));
    expect(simpleHash('a,b,c')).not.toBe(simpleHash('c,b,a'));
    expect(simpleHash('a,b,c')).toMatch(/^[0-9a-f]{8}$/);
  });

  test('normalizeValue sorts object keys so comparison is order-insensitive', () => {
    expect(JSON.stringify(normalizeValue({ b: 1, a: 2 }))).toBe(JSON.stringify({ a: 2, b: 1 }));
    expect(normalizeValue(undefined)).toBeNull();
  });

  test('compareAttributes reports mismatches and skips _-prefixed fields', () => {
    const forma = { status: 'open', score: 5, _trace_id: 'x' };
    const pg = { status: 'won', score: 5, _trace_id: 'y' };
    const mismatches = compareAttributes(forma, pg);
    expect(mismatches).toHaveLength(1);
    expect(mismatches[0].field).toBe('status');
    expect(mismatches[0].formaValue).toBe('open');
    expect(mismatches[0].postgresValue).toBe('won');
  });

  test('compareAttributes finds a field present on only one side', () => {
    const mismatches = compareAttributes({ a: 1 }, { a: 1, b: 2 });
    expect(mismatches).toHaveLength(1);
    expect(mismatches[0].field).toBe('b');
  });

  test('compareAttributes returns empty when equal', () => {
    expect(compareAttributes({ a: 1, b: 'x' }, { b: 'x', a: 1 })).toHaveLength(0);
  });
});
