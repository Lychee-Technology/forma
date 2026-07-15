import { describe, expect, test } from 'bun:test';
import { simpleHash, checksumRowIds } from './oracle';

describe('oracle checksums', () => {
  test('simpleHash is deterministic and order-sensitive', () => {
    expect(simpleHash('a,b,c')).toBe(simpleHash('a,b,c'));
    expect(simpleHash('a,b,c')).not.toBe(simpleHash('c,b,a'));
    expect(simpleHash('a,b,c')).toMatch(/^[0-9a-f]{8}$/);
  });

  test('checksumRowIds is order-insensitive over the id set', () => {
    expect(checksumRowIds(['a', 'b', 'c'])).toBe(checksumRowIds(['c', 'a', 'b']));
    expect(checksumRowIds(['a', 'b'])).not.toBe(checksumRowIds(['a', 'b', 'c']));
  });
});
