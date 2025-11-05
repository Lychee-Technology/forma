import { FormaError } from "./errors.js";
import type {
  AdvancedQueryOptions,
  BatchResult,
  CreateResponse,
  CrossSchemaSearchOptions,
  DataRecord,
  FormaClientOptions,
  QueryOptions,
  QueryResult,
} from "./types.js";

type QueryRecord = Record<string, string | number | undefined | null>;

export class FormaClient {
  private readonly baseUrl: string;
  private readonly fetchImpl: typeof fetch;
  private readonly defaultHeaders: Record<string, string>;

  constructor(options: FormaClientOptions = {}) {
    this.baseUrl = (options.baseUrl ?? "http://localhost:8080").replace(/\/+$/, "");
    this.fetchImpl = options.fetch ?? globalThis.fetch;
    if (typeof this.fetchImpl !== "function") {
      throw new Error(
        "No fetch implementation available. Provide one in FormaClientOptions.fetch or use Node.js >= 18.",
      );
    }
    this.defaultHeaders = options.defaultHeaders ?? {};
  }

  async create(schemaName: string, payload: Record<string, unknown> | Record<string, unknown>[]): Promise<CreateResponse> {
    return this.request<CreateResponse>(`/api/v1/${schemaName}`, {
      method: "POST",
      body: JSON.stringify(payload),
    });
  }

  async get(schemaName: string, rowId: string): Promise<DataRecord> {
    return this.request<DataRecord>(`/api/v1/${schemaName}/${rowId}`);
  }

  async update(schemaName: string, rowId: string, payload: Record<string, unknown>): Promise<DataRecord> {
    return this.request<DataRecord>(`/api/v1/${schemaName}/${rowId}`, {
      method: "PUT",
      body: JSON.stringify(payload),
    });
  }

  async delete(schemaName: string, rowIds: string[] | string): Promise<BatchResult> {
    const ids = Array.isArray(rowIds) ? rowIds : [rowIds];
    return this.request<BatchResult>(`/api/v1/${schemaName}`, {
      method: "DELETE",
      body: JSON.stringify(ids),
    });
  }

  async query(schemaName: string, options: QueryOptions = {}): Promise<QueryResult> {
    const query: QueryRecord = {};
    if (options.page) query.page = options.page;
    if (options.itemsPerPage) query.items_per_page = options.itemsPerPage;
    if (options.sortBy) query.sort_by = options.sortBy;
    if (options.sortOrder) query.sort_order = options.sortOrder;
    if (options.filters) {
      for (const [key, value] of Object.entries(options.filters)) {
        query[key] = value;
      }
    }
    return this.request<QueryResult>(`/api/v1/${schemaName}`, undefined, query);
  }

  async crossSchemaSearch(options: CrossSchemaSearchOptions = {}): Promise<QueryResult> {
    const query: QueryRecord = {};
    if (options.page) query.page = options.page;
    if (options.itemsPerPage) query.items_per_page = options.itemsPerPage;
    if (options.searchTerm) query.q = options.searchTerm;
    if (options.schemas?.length) query.schemas = options.schemas.join(",");
    if (options.filters) {
      for (const [key, value] of Object.entries(options.filters)) {
        query[key] = value;
      }
    }
    return this.request<QueryResult>("/api/v1/search", undefined, query);
  }

  async advancedQuery(options: AdvancedQueryOptions): Promise<QueryResult> {
    const payload: Record<string, unknown> = {
      schema_name: options.schemaName,
      condition: options.condition,
    };
    if (options.page !== undefined) payload.page = options.page;
    if (options.itemsPerPage !== undefined) payload.items_per_page = options.itemsPerPage;

    return this.request<QueryResult>("/api/v1/advanced_query", {
      method: "POST",
      body: JSON.stringify(payload),
    });
  }

  private async request<T>(path: string, init: RequestInit = {}, query?: QueryRecord): Promise<T> {
    const url = this.buildUrl(path, query);
    const headers = this.prepareHeaders(init.headers, init.body);

    const response = await this.fetchImpl(url, { ...init, headers });
    const text = await response.text();
    let parsed: unknown = undefined;

    if (text) {
      try {
        parsed = JSON.parse(text);
      } catch {
        parsed = text;
      }
    }

    if (!response.ok) {
      const errorMessage =
        typeof parsed === "object" && parsed !== null && "error" in parsed && typeof (parsed as any).error === "string"
          ? (parsed as any).error
          : response.statusText || "Request failed";
      throw new FormaError(errorMessage, response.status, parsed);
    }

    return parsed as T;
  }

  private buildUrl(path: string, query?: QueryRecord): string {
    const normalizedPath = path.startsWith("/") ? path : `/${path}`;
    const url = new URL(normalizedPath, this.baseUrl);
    if (query) {
      for (const [key, value] of Object.entries(query)) {
        if (value === undefined || value === null) continue;
        url.searchParams.set(key, String(value));
      }
    }
    return url.toString();
  }

  private prepareHeaders(headers: HeadersInit | undefined, body: BodyInit | null | undefined): Record<string, string> {
    const resolved: Record<string, string> = { ...this.defaultHeaders };
    if (headers) {
      if (headers instanceof Headers) {
        headers.forEach((value, key) => {
          resolved[key] = value;
        });
      } else if (Array.isArray(headers)) {
        for (const [key, value] of headers) {
          resolved[key] = value;
        }
      } else {
        Object.assign(resolved, headers);
      }
    }
    if (body && !Object.keys(resolved).some((key) => key.toLowerCase() === "content-type")) {
      resolved["Content-Type"] = "application/json";
    }
    return resolved;
  }
}
