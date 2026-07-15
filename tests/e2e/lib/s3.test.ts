import { describe, expect, test } from 'bun:test';
import { deriveSigningKey, amzDate } from './s3';

describe('SigV4', () => {
  // AWS's published signing-key derivation vector
  // (docs.aws.amazon.com "Examples of how to derive a signing key").
  test('deriveSigningKey matches the AWS reference vector', () => {
    const key = deriveSigningKey(
      'wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY',
      '20120215',
      'us-east-1',
      'iam',
    );
    expect(key.toString('hex')).toBe(
      'f4780e2d9f65fa895f9c67b32ce1baf0b0d8a43505a000a1a9e090d414db404d',
    );
  });

  test('amzDate strips separators to the compact stamp', () => {
    expect(amzDate(new Date('2026-07-14T10:15:30.123Z'))).toBe('20260714T101530Z');
  });
});
