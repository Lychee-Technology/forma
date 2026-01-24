/**
 * Environment configuration loader for E2E tests
 * Reads from .env file in the tests/e2e directory
 */

import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));

// Load .env file
const envPath = resolve(__dirname, '..', '.env');
const envFile = Bun.file(envPath);

let envVars: Record<string, string> = {};

if (await envFile.exists()) {
  const content = await envFile.text();
  for (const line of content.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eqIdx = trimmed.indexOf('=');
    if (eqIdx === -1) continue;
    const key = trimmed.slice(0, eqIdx).trim();
    const value = trimmed.slice(eqIdx + 1).trim();
    envVars[key] = value;
  }
}

function getEnv(key: string, defaultValue?: string): string {
  const value = process.env[key] ?? envVars[key] ?? defaultValue;
  if (value === undefined) {
    throw new Error(`Missing required environment variable: ${key}`);
  }
  return value;
}

function getEnvInt(key: string, defaultValue: number): number {
  const raw = process.env[key] ?? envVars[key];
  if (!raw) return defaultValue;
  const parsed = parseInt(raw, 10);
  return isNaN(parsed) ? defaultValue : parsed;
}

function getEnvBool(key: string, defaultValue: boolean): boolean {
  const raw = process.env[key] ?? envVars[key];
  if (!raw) return defaultValue;
  return raw.toLowerCase() === 'true' || raw === '1';
}

export interface Config {
  // Forma API
  baseUrl: string;
  authToken: string;

  // Postgres
  pg: {
    host: string;
    port: number;
    user: string;
    password: string;
    database: string;
    sslMode: string;
  };

  // Table names
  tables: {
    schemaRegistry: string;
    entityMain: string;
    eavData: string;
    changeLog: string;
  };

  // S3/RustFS
  s3: {
    endpoint: string;
    bucket: string;
    prefix: string;
    accessKey: string;
    secretKey: string;
    useSsl: boolean;
    usePath: boolean;
  };

  // Test parameters
  datasetSize: number;
  batchSize: number;
  tenantId: string;

  // CDC parameters
  cdc: {
    minRecords: number;
    maxAgeMs: number;
    batchSize: number;
  };

  // Paths
  toolsPath: string;
  schemaDir: string;
}

export function loadConfig(): Config {
  return {
    baseUrl: getEnv('BASE_URL', 'http://localhost:8080'),
    authToken: getEnv('AUTH_TOKEN', ''),

    pg: {
      host: getEnv('PG_HOST', 'localhost'),
      port: getEnvInt('PG_PORT', 5432),
      user: getEnv('PG_USER', 'postgres'),
      password: getEnv('PG_PASSWORD', 'postgres'),
      database: getEnv('PG_DB', 'forma'),
      sslMode: getEnv('PG_SSL_MODE', 'disable'),
    },

    tables: {
      schemaRegistry: getEnv('SCHEMA_TABLE', 'schema_registry_dev'),
      entityMain: getEnv('ENTITY_MAIN_TABLE', 'entity_main_dev'),
      eavData: getEnv('EAV_TABLE', 'eav_data_dev'),
      changeLog: getEnv('CHANGE_LOG_TABLE', 'change_log_dev'),
    },

    s3: {
      endpoint: getEnv('S3_ENDPOINT', 'http://localhost:19000'),
      bucket: getEnv('S3_BUCKET', 'forma-cdc'),
      prefix: getEnv('S3_PREFIX', 'delta'),
      accessKey: getEnv('S3_ACCESS_KEY', 'minio'),
      secretKey: getEnv('S3_SECRET_KEY', 'minio_password'),
      useSsl: getEnvBool('S3_USE_SSL', false),
      usePath: getEnvBool('S3_USE_PATH', true),
    },

    datasetSize: getEnvInt('DATASET_SIZE', 10000),
    batchSize: getEnvInt('BATCH_SIZE', 500),
    tenantId: getEnv('TENANT_ID', 'test-tenant'),

    cdc: {
      minRecords: getEnvInt('CDC_MIN_RECORDS', 1000),
      maxAgeMs: getEnvInt('CDC_MAX_AGE_MS', 60000),
      batchSize: getEnvInt('CDC_BATCH_SIZE', 10000),
    },

    toolsPath: resolve(__dirname, '..', getEnv('TOOLS_PATH', '../../build/tools')),
    schemaDir: resolve(__dirname, '..', getEnv('SCHEMA_DIR', '../../cmd/server/schemas')),
  };
}

// Export singleton config
export const config = loadConfig();
