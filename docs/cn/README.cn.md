# Forma

Forma 是一个基于 PostgreSQL 构建的通用数据管理系统。它使用 JSON Schema 进行数据定义，采用双存储模型（热字段表 + EAV 表）来处理高度动态的数据结构，无需频繁修改数据库架构。

## 环境依赖

- **Go** 1.26+
- **Docker & Docker Compose**（用于本地 PostgreSQL 和 S3 兼容存储）
- **Bun**（用于 E2E 测试脚本）
- **k6**（用于压力测试；可通过 Docker 替代）

## 快速开始

```bash
# 克隆项目
git clone https://github.com/lychee-technology/forma.git
cd forma

# 启动 PostgreSQL（Docker Compose）
docker compose -f deploy/docker-compose.yml up -d

# 编译所有二进制文件
make build-all

# 初始化数据库表（首次运行必需）
./build/tools init-db \
  --db-host localhost \
  --db-port 5432 \
  --db-name forma \
  --db-user postgres \
  --db-password postgres \
  --db-ssl-mode disable \
  --schema-dir cmd/server/schemas

# 启动服务器
SCHEMA_DIR=cmd/server/schemas ./build/server
```

或使用一键脚本：

```bash
./scripts/local_server.sh
```

服务默认监听 `8080` 端口。通过环境变量配置：

| 变量 | 默认值 | 说明 |
|----------|---------|-------------|
| `DB_HOST` | `localhost` | PostgreSQL 主机 |
| `DB_PORT` | `5432` | PostgreSQL 端口 |
| `DB_NAME` | `forma` | 数据库名 |
| `DB_USER` | `postgres` | 数据库用户 |
| `DB_PASSWORD` | `` | 数据库密码 |
| `DB_SSL_MODE` | `disable` | SSL 模式 |
| `SCHEMA_DIR` | `` | Schema JSON 文件目录 |
| `PORT` | `8080` | HTTP 监听端口 |

## API 参考

| 方法 | 路径 | 说明 |
|--------|------|-------------|
| `POST` | `/api/v1/{schema}` | 创建记录（支持单对象或数组） |
| `GET` | `/api/v1/{schema}/{row_id}` | 获取单条记录 |
| `GET` | `/api/v1/{schema}` | 分页查询（`?page=&items_per_page=&sort_by=&sort_order=&attrs=`） |
| `PUT` | `/api/v1/{schema}/{row_id}` | 更新记录 |
| `DELETE` | `/api/v1/{schema}` | 批量删除（JSON body：row_id 字符串数组） |
| `GET` | `/api/v1/search` | 跨 Schema 搜索（`?schemas=&q=&page=&items_per_page=`） |
| `POST` | `/api/v1/advanced_query` | 高级查询，使用条件 DSL（JSON body） |

## 测试

### 单元测试与集成测试

```bash
# 运行所有单元和集成测试
make test

# 运行测试并生成覆盖率报告
make coverage

# 运行 Lint 检查
make lint
```

### Go E2E Harness（基于容器）

需要 Docker。验证三层联邦查询架构（Postgres Hot + S3 Delta/Base → DuckDB merge-on-read）。

```bash
# 冒烟测试：验证基础设施是否正常启动
go test -v ./internal/e2e_harness/... -timeout=5m

# 完整联邦测试套件（功能 + 一致性 + 故障模式）
go test -v ./internal/e2e_harness/federated/... -tags=e2e -timeout=30m

# 仅性能测试（需要更长超时时间）
go test -v ./internal/e2e_harness/federated/... -run TestPerformance -tags=e2e -timeout=60m
```

### Bun E2E（黑盒 API 验证）

需要运行中的 Forma 服务器和 PostgreSQL。

```bash
cd tests/e2e
cp .env.example .env
bun install

# 默认流程：注册 Schema → 生成数据 → CDC 刷新 → 联邦查询校验
bun run test

# 单独步骤
bun run register-schemas
bun run gen-data -- --schema all --count 10000
bun run cdc-flush
bun run federated-check

# 扩展步骤
bun run cdc-init          # 回填 Base Parquet
bun run compactor -- --all # 合并 Delta 到 Base
```

### k6 压力测试

```bash
cd tests/e2e
bun run build-k6

bun run k6-smoke   # 5 VUs, 30s
bun run k6-full    # 30 VUs, 2m
bun run k6-perf    # 100 VUs, 5m
```

### 性能基准测试

```bash
make benchmark-smoke       # CI 冒烟验证
make benchmark-regression  # 小规模实时数据子集
make benchmark-heavy       # 大规模规划数据
```

## 文档

- [文档索引](docs/index.md)
- [E2E 测试矩阵](docs/e2e-tests-cn.md)
- [集成测试用例](docs/integ-tests-cn.md)
- [Go E2E Harness 说明](internal/e2e_harness/README.md)
- [Bun E2E 说明](tests/e2e/README.md)

## 为什么选择 Forma？

Forma 弥补了传统 RDBMS 刚性 Schema 与无 Schema NoSQL 之间的空白：

- **零停机 Schema 演进** — 通过更新 JSON Schema 元数据即可增删字段，无需 `ALTER TABLE`。
- **ACID 事务保障** — 基于 PostgreSQL，继承完整的 ACID 事务特性。
- **智能 SQL 生成** — CTE + JSON_AGG 消除 EAV 模型常见的 N+1 查询问题。
- **联邦查询 (Lakehouse)** — PostgreSQL 负责 OLTP，DuckDB + S3 Parquet 负责 OLAP。Anti-Join + Dirty Set 保证跨层数据一致性。
