# Forma API

## HTTP Endpoints

Base URL: `http://localhost:8080` (configurable via `PORT` env var).

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/{schema}` | Create records (single JSON object or JSON array). Returns `201`. |
| `GET` | `/api/v1/{schema}/{row_id}` | Get a single record by row_id. Returns `200` or `404`. |
| `GET` | `/api/v1/{schema}` | Query records with pagination, sorting, and attribute projection. |
| `PUT` | `/api/v1/{schema}/{row_id}` | Update a record (full object replacement). |
| `DELETE` | `/api/v1/{schema}` | Batch delete. JSON body: array of row_id strings. Transactional. |
| `DELETE` | `/api/v1/{schema}/{row_id}` | Single record delete. |
| `GET` | `/api/v1/search` | Cross-schema full-text search. `?schemas=a,b&q=term&page=1&items_per_page=20` |
| `POST` | `/api/v1/advanced_query` | Advanced query with condition DSL (JSON body). |

## Query Parameters (List/Query)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `page` | int | 1 | Page number |
| `items_per_page` | int | 20 | Items per page (max 100) |
| `sort_by` | string | — | Comma-separated attribute names for sorting |
| `sort_order` | string | `asc` | Sort direction (`asc` or `desc`) |
| `attrs` | string | — | Comma-separated attribute names for projection |

**NULL placement in sorted results:** records whose sort attribute is NULL always appear at the end of the result set (NULLS LAST), for both `asc` and `desc`, and regardless of whether the query is served by the Postgres OLTP path or the DuckDB federated path (adopted in #183; before that change, DESC sorts on the OLTP path followed the PostgreSQL default of NULLS FIRST).

## Advanced Query DSL

Reference: [Advanced Query documentation](./advanced_query.md)

### Sorting (advanced_query)

`POST /api/v1/advanced_query` bodies support two mutually exclusive sort surfaces:

| Field | Type | Description |
|-------|------|-------------|
| `sort_by` + `sort_order` | `string[]` + `string` | Legacy: every key shares the one direction (`asc` default), case-insensitive; other values → 400, even when `sort_by` is empty (#296). |
| `sort` | `{attribute, sort_order}[]` | Per-key directions (#240). `sort_order` per entry: `asc` (default when omitted) or `desc`, case-insensitive. |

Mixed-direction example — `status ASC, created_at DESC`:

```json
{
  "schema_name": "orders",
  "condition": {"a": "status", "v": "not_equals:archived"},
  "sort": [
    {"attribute": "status"},
    {"attribute": "created_at", "sort_order": "desc"}
  ]
}
```

Supplying `sort` together with `sort_by`/`sort_order` is rejected with `400`. The GET list endpoint keeps the legacy `sort_by`/`sort_order` query parameters only (uniform direction). NULL placement (NULLS LAST on every path and direction) applies per key.

## Error Codes

| Status | Meaning |
|--------|---------|
| `201` | Record(s) created |
| `200` | Success |
| `400` | Invalid request (bad path, missing params, invalid JSON) |
| `404` | Record not found |
| `405` | Method not allowed |
| `409` | Conflict (e.g., duplicate) |
| `500` | Internal server error |

## References

- Full RFC: [API Specs RFC](https://github.com/Lychee-Technology/rfc/blob/main/CN/API-specs.cn.md)
