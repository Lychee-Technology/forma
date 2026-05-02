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

## Advanced Query DSL

Reference: [Advanced Query documentation](./advanced_query.md)

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
