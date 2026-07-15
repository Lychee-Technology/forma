/**
 * S3/RustFS helper for the E2E suite.
 *
 * Reads (list/get/exists) go through Bun's built-in S3Client, which signs
 * requests and uses path-style addressing for custom endpoints (verified
 * against RustFS on :9000). Bucket creation is the one operation S3Client does
 * not expose, so ensureBucket issues its own SigV4-signed PUT to the bucket
 * root. Both share the same credentials/endpoint from config.
 */

import { createHash, createHmac } from 'crypto';
import { S3Client } from 'bun';
import { config } from './env';

export interface S3Object {
  key: string;
  size: number;
}

/** Create an S3Client bound to the given bucket (defaults to the CDC bucket). */
export function createS3Client(bucket: string = config.s3.bucket): S3Client {
  return new S3Client({
    accessKeyId: config.s3.accessKey,
    secretAccessKey: config.s3.secretKey,
    bucket,
    endpoint: config.s3.endpoint,
    region: 'us-east-1',
  });
}

/** List every object under prefix, following continuation tokens. */
export async function listObjects(client: S3Client, prefix: string): Promise<S3Object[]> {
  const objects: S3Object[] = [];
  let continuationToken: string | undefined;
  do {
    const res: any = await client.list({ prefix, maxKeys: 1000, continuationToken });
    for (const c of res?.contents ?? []) {
      objects.push({ key: c.key, size: c.size ?? 0 });
    }
    continuationToken = res?.isTruncated ? res?.nextContinuationToken : undefined;
  } while (continuationToken);
  return objects;
}

/** Read an object's body as text (e.g. a manifest JSON). */
export async function readObjectText(client: S3Client, key: string): Promise<string> {
  return await client.file(key).text();
}

/** True if the object exists. */
export async function objectExists(client: S3Client, key: string): Promise<boolean> {
  return await client.exists(key);
}

export interface CDCValidation {
  ok: boolean;
  parquetCount: number;
  /** Parquet object counts keyed by schema-id segment (e.g. "101"). */
  parquetBySchema: Record<string, number>;
  parquetKeys: string[];
  manifestCount: number;
  /** manifest-referenced parquet paths not found in the S3 listing. */
  missingFiles: string[];
  errors: string[];
}

function basename(path: string): string {
  const cleaned = path.replace(/\/+$/, '');
  const idx = cleaned.lastIndexOf('/');
  return idx === -1 ? cleaned : cleaned.slice(idx + 1);
}

/** Extract the schema-id segment from a CDC key like "delta/101/uuid.parquet". */
function schemaSegment(key: string): string {
  const parts = key.split('/');
  return parts.length >= 2 ? parts[1] : 'unknown';
}

/**
 * Validate CDC output on S3 after a tool run: confirm parquet objects exist
 * under the data prefix and that every file the manifest references is
 * actually present. This is the real post-run check the wrappers use instead
 * of trusting the tool's exit code and scraping counts from log text.
 *
 * File matching is by basename so it is robust to whether the manifest stores
 * a bucket-relative key, a prefixed key, or a full path.
 */
export async function validateCDCOutput(opts: {
  dataPrefix: string;
  manifestPrefix: string;
  requireParquet: boolean;
}): Promise<CDCValidation> {
  const client = createS3Client();
  const result: CDCValidation = {
    ok: false,
    parquetCount: 0,
    parquetBySchema: {},
    parquetKeys: [],
    manifestCount: 0,
    missingFiles: [],
    errors: [],
  };

  // An empty data prefix means "list the whole bucket" (used post-compaction,
  // where files span base and delta and only manifest consistency matters).
  const dataPrefix = opts.dataPrefix ? opts.dataPrefix.replace(/\/+$/, '') + '/' : '';
  const dataObjects = await listObjects(client, dataPrefix);
  const parquet = dataObjects.filter((o) => o.key.endsWith('.parquet'));
  result.parquetKeys = parquet.map((o) => o.key);
  result.parquetCount = parquet.length;
  for (const o of parquet) {
    const seg = schemaSegment(o.key);
    result.parquetBySchema[seg] = (result.parquetBySchema[seg] ?? 0) + 1;
  }

  const presentBasenames = new Set(parquet.map((o) => basename(o.key)));

  const manifests = (await listObjects(client, opts.manifestPrefix.replace(/\/+$/, '') + '/')).filter((o) =>
    o.key.endsWith('.json'),
  );
  result.manifestCount = manifests.length;

  for (const m of manifests) {
    let parsed: { files?: Array<{ path?: string }> };
    try {
      parsed = JSON.parse(await readObjectText(client, m.key));
    } catch (err) {
      result.errors.push(`manifest ${m.key} is not valid JSON: ${String(err)}`);
      continue;
    }
    for (const f of parsed.files ?? []) {
      if (!f.path) continue;
      if (!presentBasenames.has(basename(f.path))) {
        result.missingFiles.push(`${m.key} -> ${f.path}`);
      }
    }
  }

  if (opts.requireParquet && result.parquetCount === 0) {
    result.errors.push(`no parquet objects found under ${opts.dataPrefix || '(bucket root)'}`);
  }
  // Parquet without a manifest is a broken flush/init: the read path is
  // manifest-driven, so unmanifested objects are invisible. Require one.
  if (result.parquetCount > 0 && result.manifestCount === 0) {
    result.errors.push('parquet objects exist but no manifest was written');
  }
  if (result.missingFiles.length > 0) {
    result.errors.push(`${result.missingFiles.length} manifest-referenced file(s) missing from S3`);
  }

  result.ok = result.errors.length === 0;
  return result;
}

/**
 * Delete every object under the given prefixes. Used for opt-in run isolation
 * so repeated local/CI runs start from a clean CDC state instead of
 * accumulating parquet across runs. Only the CDC-managed prefixes should be
 * passed — this is a destructive operation.
 */
export async function deleteUnderPrefixes(prefixes: string[]): Promise<number> {
  const client = createS3Client();
  let deleted = 0;
  for (const prefix of prefixes) {
    const objects = await listObjects(client, prefix.replace(/\/+$/, '') + '/');
    for (const o of objects) {
      await client.delete(o.key);
      deleted++;
    }
  }
  return deleted;
}

// --- SigV4 (bucket creation only) -----------------------------------------

function sha256Hex(payload: string): string {
  return createHash('sha256').update(payload, 'utf8').digest('hex');
}

function hmac(key: Buffer | string, data: string): Buffer {
  return createHmac('sha256', key).update(data, 'utf8').digest();
}

/**
 * Derive the AWS SigV4 signing key. Exposed for unit testing against AWS's
 * published derivation vector.
 */
export function deriveSigningKey(
  secretKey: string,
  dateStamp: string,
  region: string,
  service: string,
): Buffer {
  const kDate = hmac('AWS4' + secretKey, dateStamp);
  const kRegion = hmac(kDate, region);
  const kService = hmac(kRegion, service);
  return hmac(kService, 'aws4_request');
}

/**
 * Ensure the CDC bucket exists, creating it with a SigV4-signed PUT to the
 * bucket root if missing. Idempotent: an already-existing bucket (checked via
 * a cheap list, or a BucketAlreadyOwnedByYou/409 on create) is treated as
 * success. The amz-date stamp (e.g. 20260714T101530Z) is injected for
 * deterministic testing; callers pass the current time.
 */
export async function ensureBucket(amzDate: string, bucket: string = config.s3.bucket): Promise<void> {
  // Fast path: if we can list the bucket, it already exists.
  const client = createS3Client(bucket);
  try {
    await client.list({ maxKeys: 1 });
    return;
  } catch {
    // fall through to create
  }

  const region = 'us-east-1';
  const service = 's3';
  const dateStamp = amzDate.slice(0, 8);
  const url = new URL(`${config.s3.endpoint.replace(/\/$/, '')}/${bucket}`);
  const host = url.host;
  const payloadHash = sha256Hex('');

  const canonicalHeaders = `host:${host}\nx-amz-content-sha256:${payloadHash}\nx-amz-date:${amzDate}\n`;
  const signedHeaders = 'host;x-amz-content-sha256;x-amz-date';
  const canonicalRequest = [
    'PUT',
    url.pathname,
    '',
    canonicalHeaders,
    signedHeaders,
    payloadHash,
  ].join('\n');

  const scope = `${dateStamp}/${region}/${service}/aws4_request`;
  const stringToSign = [
    'AWS4-HMAC-SHA256',
    amzDate,
    scope,
    sha256Hex(canonicalRequest),
  ].join('\n');

  const signingKey = deriveSigningKey(config.s3.secretKey, dateStamp, region, service);
  const signature = createHmac('sha256', signingKey).update(stringToSign, 'utf8').digest('hex');

  const authorization =
    `AWS4-HMAC-SHA256 Credential=${config.s3.accessKey}/${scope}, ` +
    `SignedHeaders=${signedHeaders}, Signature=${signature}`;

  const res = await fetch(url.toString(), {
    method: 'PUT',
    headers: {
      Authorization: authorization,
      'x-amz-content-sha256': payloadHash,
      'x-amz-date': amzDate,
    },
  });

  // 200 = created; 409 with BucketAlreadyOwnedByYou/BucketAlreadyExists is a
  // benign race — the bucket is there, which is all we need.
  if (res.ok || res.status === 409) {
    return;
  }
  const body = await res.text().catch(() => '');
  throw new Error(`failed to create bucket ${bucket}: ${res.status} ${body}`);
}

/** Format a Date as an AWS SigV4 amz-date stamp, e.g. 20260714T101530Z. */
export function amzDate(now: Date): string {
  return now.toISOString().replace(/[:-]|\.\d{3}/g, '');
}
