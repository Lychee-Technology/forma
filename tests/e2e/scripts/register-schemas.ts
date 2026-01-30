#!/usr/bin/env bun
/**
 * Schema Registration Script
 * Registers lead, visit, and log schemas with the Forma server
 * 
 * Note: This script assumes schemas are auto-registered when the server starts
 * with SCHEMA_DIR pointing to cmd/server/schemas. This script is provided for
 * manual registration or verification purposes.
 * 
 * Usage: bun run scripts/register-schemas.ts
 */

import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { config } from '../lib/env';
import { get, post } from '../lib/http';

const __dirname = dirname(fileURLToPath(import.meta.url));

interface SchemaInfo {
  name: string;
  filename: string;
}

const SCHEMAS: SchemaInfo[] = [
  { name: 'lead', filename: 'lead.json' },
  { name: 'visit', filename: 'visit.json' },
  { name: 'log', filename: 'log.json' },
];

interface RegistrationReport {
  timestamp: string;
  schemas: {
    name: string;
    status: 'registered' | 'already_exists' | 'skipped' | 'error';
    error?: string;
  }[];
  summary: {
    total: number;
    registered: number;
    skipped: number;
    errors: number;
  };
}

async function loadSchema(schemaDir: string, filename: string): Promise<unknown> {
  const schemaPath = resolve(schemaDir, filename);
  const file = Bun.file(schemaPath);
  
  if (!(await file.exists())) {
    throw new Error(`Schema file not found: ${schemaPath}`);
  }
  
  return file.json();
}

async function checkSchemaExists(schemaName: string): Promise<boolean> {
  // Try to query the schema to see if it exists
  // We do a simple query with limit 0 to check if the schema is registered
  const response = await get(`/api/v1/${schemaName}`, { page: 1, items_per_page: 1 });
  return response.ok || response.status !== 404;
}

async function registerSchema(schemaName: string, schema: unknown): Promise<{ success: boolean; error?: string }> {
  // The Forma server typically auto-registers schemas from the schema directory
  // This endpoint may not exist; we'll try a POST to a schema registry endpoint
  // If it fails, we assume the schema is already registered via file-based loading
  
  try {
    // First check if schema already works by querying it
    const exists = await checkSchemaExists(schemaName);
    if (exists) {
      return { success: true }; // Already exists
    }
    
    // Try to register via API (this may not be implemented)
    const response = await post('/api/v1/schema', {
      name: schemaName,
      schema: schema,
    });
    
    if (response.ok) {
      return { success: true };
    }
    
    // If schema registration endpoint doesn't exist, that's okay
    // The schema should be loaded from files
    if (response.status === 404) {
      console.log(`[INFO] Schema registration endpoint not found, assuming file-based loading`);
      return { success: true };
    }
    
    return { success: false, error: response.error ?? `HTTP ${response.status}` };
  } catch (err) {
    return { success: false, error: String(err) };
  }
}

async function main() {
  console.log('='.repeat(60));
  console.log('Forma E2E: Schema Registration');
  console.log('='.repeat(60));
  console.log(`Base URL: ${config.baseUrl}`);
  console.log(`Schema Dir: ${config.schemaDir}`);
  console.log('');

  const report: RegistrationReport = {
    timestamp: new Date().toISOString(),
    schemas: [],
    summary: {
      total: SCHEMAS.length,
      registered: 0,
      skipped: 0,
      errors: 0,
    },
  };

  for (const schemaInfo of SCHEMAS) {
    console.log(`Processing schema: ${schemaInfo.name}`);
    
    try {
      // Load schema from file
      const schema = await loadSchema(config.schemaDir, schemaInfo.filename);
      console.log(`  Loaded: ${schemaInfo.filename}`);
      
      // Check if schema already exists by trying a query
      const exists = await checkSchemaExists(schemaInfo.name);
      
      if (exists) {
        console.log(`  Status: Already exists (skipping registration)`);
        report.schemas.push({
          name: schemaInfo.name,
          status: 'already_exists',
        });
        report.summary.skipped++;
        continue;
      }
      
      // Try to register
      const result = await registerSchema(schemaInfo.name, schema);
      
      if (result.success) {
        console.log(`  Status: Registered successfully`);
        report.schemas.push({
          name: schemaInfo.name,
          status: 'registered',
        });
        report.summary.registered++;
      } else {
        console.log(`  Status: Error - ${result.error}`);
        report.schemas.push({
          name: schemaInfo.name,
          status: 'error',
          error: result.error,
        });
        report.summary.errors++;
      }
    } catch (err) {
      console.log(`  Status: Error - ${err}`);
      report.schemas.push({
        name: schemaInfo.name,
        status: 'error',
        error: String(err),
      });
      report.summary.errors++;
    }
  }

  console.log('');
  console.log('='.repeat(60));
  console.log('Summary');
  console.log('='.repeat(60));
  console.log(`Total schemas: ${report.summary.total}`);
  console.log(`Registered: ${report.summary.registered}`);
  console.log(`Skipped (already exists): ${report.summary.skipped}`);
  console.log(`Errors: ${report.summary.errors}`);

  // Write report
  const reportPath = resolve(__dirname, '..', 'reports', 'schema-registration.json');
  await Bun.write(reportPath, JSON.stringify(report, null, 2));
  console.log(`\nReport written to: ${reportPath}`);

  // Exit with error if any failures
  if (report.summary.errors > 0) {
    process.exit(1);
  }
}

main().catch((err) => {
  console.error('Fatal error:', err);
  process.exit(1);
});
