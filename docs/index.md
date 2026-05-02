---
layout: home

hero:
  name: Forma
  text: Flexible Data Storage for the AI Era
  tagline: EAV + JSON Schema + Hot Table — Zero DDL, Type-Safe, AI-Ready
  actions:
    - theme: brand
      text: Get Started
      link: /cn/README.cn
    - theme: alt
      text: Engineering Blog
      link: /blog-series/
    - theme: alt
      text: GitHub
      link: https://github.com/ruoshui/forma

features:
  - icon: 🔄
    title: Zero DDL Schema Evolution
    details: Add or modify fields instantly through JSON Schema updates. No ALTER TABLE, no downtime, no migrations.
  - icon: 🤖
    title: AI-Native Integration
    details: JSON Schema is the de facto standard for LLM structured outputs. Forma speaks the same language as your AI.
  - icon: ⚡
    title: 40x Query Performance
    details: CTE + JSON_AGG eliminates N+1 queries. Hot table design puts 80% of queries on B-tree indexed columns.
  - icon: 🏠
    title: Zero Dirty Read Lakehouse
    details: PostgreSQL for OLTP, DuckDB + Parquet for OLAP. Anti-Join + Dirty Set ensures consistency across federated queries.
  - icon: 💰
    title: Serverless Cost Model
    details: DuckDB embedded execution means zero idle cost. Pay only for what you query.
  - icon: 🔒
    title: ACID Guaranteed
    details: Built on PostgreSQL. Inherit decades of battle-tested transactional guarantees.
---

## Why Forma?

Traditional databases weren't built for the AI era. When your AI Agent outputs 12 fields today and 30 fields tomorrow, waiting 3-7 days for DDL approval isn't an option.

Forma solves this with a modern take on the EAV pattern:

| Problem | Traditional DB | Forma |
|---------|---------------|-------|
| New field | ALTER TABLE (days) | JSON Schema update (seconds) |
| Schema change | Downtime required | Zero downtime |
| AI output | Manual adaptation | Direct JSON Schema mapping |
| N+1 queries | 101 round-trips | 1 round-trip |
| Historical data | Same table, same cost | Cold storage on S3 |

## Quick Start

```bash
# Clone the repository
git clone https://github.com/ruoshui/forma.git
cd forma

# Start PostgreSQL
docker compose -f deploy/docker-compose.yml up -d

# Build all binaries
make build-all

# Initialize database
./build/tools init-db \
  --db-host localhost --db-port 5432 --db-name forma \
  --db-user postgres --db-password postgres --db-ssl-mode disable \
  --schema-dir cmd/server/schemas

# Start the server
SCHEMA_DIR=cmd/server/schemas ./build/server

# Run tests
make test
```

## Learn More

- [Engineering Blog Series](/blog-series/) - Deep dive into Forma's architecture
- [Documentation](/cn/README.cn) - API reference and guides
- [GitHub](https://github.com/ruoshui/forma) - Source code and issues
