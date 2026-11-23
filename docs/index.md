---
layout: home

hero:
  name: Forma
  text: Entities whose shape changes often, on PostgreSQL
  tagline: JSON Schema data modeling, a typed main table plus EAV, and an optional Parquet + DuckDB analytical tier. No DDL migrations.
  actions:
    - theme: brand
      text: Get Started
      link: https://github.com/Lychee-Technology/forma#quick-start
    - theme: alt
      text: Engineering Blog
      link: /blog-series/
    - theme: alt
      text: GitHub
      link: https://github.com/Lychee-Technology/forma

features:
  - icon: 🔄
    title: Schema Evolution Without DDL
    details: Add or remove an attribute by editing the entity's JSON Schema and recording it with apply-schema; no ALTER TABLE. A rename is a removal plus an addition under a new attribute ID, so stored values stay with the old ID. apply-schema allows only widening type changes that keep stored values exact, such as integer to bigint, and refuses the rest. The one exception is bigint to numeric on a bigint_* column, allowed with a warning that values beyond ±2^53 then read rounded.
  - icon: 🤖
    title: JSON Schema Data Modeling
    details: Each entity type is a JSON Schema file, the format LLM structured-output APIs also take. A create that violates the schema answers 400 naming the attribute.
  - icon: ⚡
    title: One Statement per Page
    details: CTE + JSON_AGG reads a page of entities and their EAV attributes in one statement instead of one query per entity. Column-bound attributes can carry declared composite indexes (x-forma-indexes).
  - icon: 🏠
    title: Optional Lakehouse Tier
    details: CDC exports changes to Parquet on S3 and DuckDB queries it, discarding any Parquet row that PostgreSQL still holds unexported and reading that row from PostgreSQL instead (anti-join dirty set). Off by default.
  - icon: 🦆
    title: Embedded DuckDB
    details: DuckDB runs inside the Forma server or AWS Lambda handler, so the analytical tier needs no warehouse cluster of its own. PostgreSQL keeps the complete current state of every live entity; S3 holds an exported copy.
  - icon: 🔒
    title: Transactions on PostgreSQL
    details: Each write is a PostgreSQL transaction. Go callers group reads and writes across schemas with Transactor.InTx; there is no HTTP transaction endpoint yet (#656).
---

## Why Forma?

When your AI agent outputs 12 fields today and 30 tomorrow, a migration per new field does not keep up. A conventional relational schema gives you types, constraints and indexes, but every new field is a migration; a document or schema-less store makes new fields free but gives up declared types and validation. Forma sits between the two:

| | Conventional tables | Forma |
|---|---|---|
| Add an attribute | `ALTER TABLE` migration | Edit the JSON Schema, record it with `apply-schema`, restart |
| Validation | Column types and constraints | JSON Schema validation of writes, plus declared value types |
| Indexed access | Any column | Built-in EAV indexes keyed by attribute and value; composite indexes you declare (`x-forma-indexes`) on column-bound attributes |
| Analytics | A separate pipeline | Optional CDC export to Parquet, queried through DuckDB |

The costs are real. EAV reads are more complex than reading one row, which is why Forma generates one CTE + `JSON_AGG` statement per page. The main table has a fixed set of typed columns, so only some attributes can be column-bound. The per-column indexes `init-db` gives the main table cannot seek on a column's value; a column-bound attribute needs an index declared with [`x-forma-indexes`](./schema-indexes.md) for that, and only column-bound attributes can be declared. EAV attributes go through `init-db`'s `eav_data` indexes instead, keyed by attribute and value; the text one seeks equality and ranges for text and uuid attributes ([Text ranges](./schema-indexes.md#text-ranges)). Schema changes are not free-form either ([Schema Evolution](./schema-evolution.md)).

## Quick Start

This runs Forma against a local PostgreSQL 16 with the bundled sample schemas. It needs Go 1.26+ and a C toolchain (the server links DuckDB through CGO), Docker or Podman with Docker Compose, and `curl`. The [README's Quick Start](https://github.com/Lychee-Technology/forma#quick-start) explains each step and goes on to create, read and update a record.

One script starts the Compose stack, builds, initializes the database and runs the server on `:8080`:

```bash
git clone https://github.com/Lychee-Technology/forma.git
cd forma
./scripts/local_server.sh
```

Or step by step:

```bash
git clone https://github.com/Lychee-Technology/forma.git
cd forma

# PostgreSQL 16 on :5432 (postgres/postgres) and RustFS, an S3-compatible
# store on :9000 that only the optional CDC/federated tier uses.
docker compose -f deploy/docker-compose.yml up -d --wait

make build-all

# Create the tables and register the sample schemas. The table names match
# the server's defaults; init-db's own defaults differ.
./build/tools init-db \
  --db-host localhost --db-port 5432 --db-name forma \
  --db-user postgres --db-password postgres --db-ssl-mode disable \
  --schema-table schema_registry_dev \
  --eav-table eav_data_dev \
  --entity-main-table entity_main_dev \
  --change-log-table change_log_dev \
  --schema-dir cmd/server/schemas

DB_PASSWORD=postgres SCHEMA_DIR=cmd/server/schemas ./build/server
```

In another terminal:

```bash
curl -s localhost:8080/health
# {"status":"healthy"}

curl -s -X POST localhost:8080/api/v1/contact \
  -H 'Content-Type: application/json' \
  -d '{"id":"c-001","name":"Ada Lovelace","email":"ada@example.com","company":"Analytical Engines"}'
```

## Learn More

- [Engineering Blog Series](/blog-series/) - Deep dive into Forma's architecture
- [README](https://github.com/Lychee-Technology/forma#readme) - Quick Start, HTTP API, read consistency, configuration, and the full documentation index
- [Federated query design](./federated-query/design.md) - The three tiers, the dirty-set anti-join, read consistency by API, and degraded modes
- [Error Handling](./error-handling.md) - Write-path vs read-path error semantics
- [Schema Evolution and Declared Types](./schema-evolution.md) - What a declared `valueType` guarantees for stored data, and the fail-closed policy for changing one
- [Transactions](./transactions.md) - Several reads and writes across schemas in one transaction with `Transactor.InTx`, row fences with `Tx.AssertRow`, predicate mutation with `Tx.UpdateWhere`/`DeleteWhere`, and claims with `Tx.Claim`
- [Conditional Writes](./conditional-writes.md) - Preconditions, `increment` and `server_time` on Update and Delete
- [Condition DSL](./condition-dsl.md) - Filter operators, `in`/`not_in`, null tests and `$now`
- [Declared Indexes](./schema-indexes.md) - Composite indexes on column-bound attributes via `x-forma-indexes`
- [Parquet publication protocol V2](./publication-protocol-v2.md) - The per-schema PostgreSQL publication record, immutable manifest generations, the one commit that publishes a generation and marks its rows, reader snapshots and GC retention
- [Protocol V2 close-out evidence ledger](./publication-protocol-v2-evidence.md) - Invariant-to-test matrix, P95/cost deltas, and separation of local vs live Aurora DSQL verification (#893)
- [Manifest reconcile](./manifest-reconcile.md) - Read-only Protocol V2 diagnosis, `--gc` retention/orphan collection under `--bind-storage`, and `--verify-checksums`
- [Text collation](./text-collation.md) - Why Forma's text columns are `COLLATE "C"`, what that changes for queries, and how to migrate tables created before it
- [Text ranges on mixed EAV attributes](./eav-text-range-plans.md) - PostgreSQL plans for text and uuid ranges sharing `eav_data.value_text`: what the planner estimates, what it costs, and why it is not a collation effect, and what removing the anchor's outer `eav_data` scan saved (#881), with the recorded runs' SQL and full plans
- [Row IDs and Unique Keys](./unique-keys.md) - Caller-supplied row IDs on create, unique keys via `x-forma-unique-keys`, `Tx.Upsert`, and `build-unique-keys`
- [Claims](./claims.md) - `Tx.Claim`: take up to a limit of matching rows without waiting for rows others hold, with caller-defined leases and generation fencing, and per-group FIFO through group heads
- [中文 README](./cn/README.cn.md) - Chinese overview. It has not been brought in line with the English README; for a first run, follow the Quick Start above
- [GitHub](https://github.com/Lychee-Technology/forma) - Source code and issues
