/**
 * HTTP client wrapper for Forma API
 * Provides JSON encoding/decoding, retry logic, and optional auth
 */

import { config } from './env';

export interface RequestOptions {
  method: 'GET' | 'POST' | 'PUT' | 'DELETE';
  path: string;
  body?: unknown;
  query?: Record<string, string | number | boolean | undefined>;
  headers?: Record<string, string>;
  retries?: number;
}

export interface ApiResponse<T = unknown> {
  status: number;
  ok: boolean;
  data: T | null;
  error: string | null;
  headers: Headers;
}

const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 1000;

function buildUrl(base: string, path: string, query?: Record<string, string | number | boolean | undefined>): string {
  const url = new URL(path, base);
  if (query) {
    for (const [key, value] of Object.entries(query)) {
      if (value !== undefined) {
        url.searchParams.append(key, String(value));
      }
    }
  }
  return url.toString();
}

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function request<T = unknown>(options: RequestOptions): Promise<ApiResponse<T>> {
  const { method, path, body, query, headers: extraHeaders, retries = MAX_RETRIES } = options;

  const url = buildUrl(config.baseUrl, path, query);

  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    Accept: 'application/json',
    ...extraHeaders,
  };

  // Add auth token if configured
  if (config.authToken) {
    headers['Authorization'] = `Bearer ${config.authToken}`;
  }

  let lastError: Error | null = null;

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const response = await fetch(url, {
        method,
        headers,
        body: body !== undefined ? JSON.stringify(body) : undefined,
      });

      // Retry on 429 or 5xx
      if ((response.status === 429 || response.status >= 500) && attempt < retries) {
        const delay = RETRY_DELAY_MS * Math.pow(2, attempt);
        console.warn(`[HTTP] ${method} ${path} returned ${response.status}, retrying in ${delay}ms (attempt ${attempt + 1}/${retries})`);
        await sleep(delay);
        continue;
      }

      let data: T | null = null;
      let error: string | null = null;

      const contentType = response.headers.get('content-type') ?? '';
      if (contentType.includes('application/json')) {
        try {
          const json = await response.json();
          if (response.ok) {
            data = json as T;
          } else {
            error = typeof json === 'object' && json !== null && 'error' in json
              ? String((json as { error: unknown }).error)
              : JSON.stringify(json);
          }
        } catch {
          error = await response.text();
        }
      } else {
        const text = await response.text();
        if (!response.ok) {
          error = text;
        }
      }

      return {
        status: response.status,
        ok: response.ok,
        data,
        error,
        headers: response.headers,
      };
    } catch (err) {
      lastError = err as Error;
      if (attempt < retries) {
        const delay = RETRY_DELAY_MS * Math.pow(2, attempt);
        console.warn(`[HTTP] ${method} ${path} failed: ${lastError.message}, retrying in ${delay}ms (attempt ${attempt + 1}/${retries})`);
        await sleep(delay);
        continue;
      }
    }
  }

  return {
    status: 0,
    ok: false,
    data: null,
    error: lastError?.message ?? 'Unknown error',
    headers: new Headers(),
  };
}

// Convenience methods
export async function get<T = unknown>(path: string, query?: Record<string, string | number | boolean | undefined>): Promise<ApiResponse<T>> {
  return request<T>({ method: 'GET', path, query });
}

export async function post<T = unknown>(path: string, body?: unknown): Promise<ApiResponse<T>> {
  return request<T>({ method: 'POST', path, body });
}

export async function put<T = unknown>(path: string, body?: unknown): Promise<ApiResponse<T>> {
  return request<T>({ method: 'PUT', path, body });
}

export async function del<T = unknown>(path: string): Promise<ApiResponse<T>> {
  return request<T>({ method: 'DELETE', path });
}

// Helper to assert success
export function assertOk<T>(response: ApiResponse<T>, context: string): T {
  if (!response.ok || response.data === null) {
    throw new Error(`${context}: ${response.status} - ${response.error ?? 'No data'}`);
  }
  return response.data;
}
