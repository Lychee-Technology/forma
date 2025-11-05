export type LogicOperator = "and" | "or";

export interface KvCondition {
  a: string;
  v: string;
}

export interface CompositeCondition {
  l: LogicOperator;
  c: ConditionNode[];
}

export type ConditionNode = CompositeCondition | KvCondition;

export interface AdvancedQueryOptions {
  schemaName: string;
  condition: CompositeCondition;
  page?: number;
  itemsPerPage?: number;
}

export interface DataRecord {
  schemaName: string;
  rowId: string;
  attributes: Record<string, unknown>;
}

export interface OperationError {
  operation: Record<string, unknown>;
  error: string;
  code: string;
  details?: Record<string, unknown>;
}

export interface BatchResult {
  successful: DataRecord[];
  failed: OperationError[];
  totalCount: number;
  duration: number;
}

export interface QueryResult {
  data: DataRecord[];
  total_records: number;
  total_pages: number;
  current_page: number;
  items_per_page: number;
  has_next: boolean;
  has_previous: boolean;
  execution_time: number;
}

export interface QueryOptions {
  page?: number;
  itemsPerPage?: number;
  sortBy?: string;
  sortOrder?: "asc" | "desc";
  filters?: Record<string, string>;
}

export interface CrossSchemaSearchOptions {
  page?: number;
  itemsPerPage?: number;
  searchTerm?: string;
  schemas?: string[];
  filters?: Record<string, string>;
}

export type CreateResponse = DataRecord | BatchResult;

export interface FormaClientOptions {
  baseUrl?: string;
  fetch?: typeof fetch;
  defaultHeaders?: Record<string, string>;
}
