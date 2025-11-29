# Forma

## 为什么要开发 Forma？


Forma 是一个通用数据管理系统，旨在解决传统关系型数据库在处理高度动态和多样化数据结构时的局限性。随着现代应用程序对灵活性和可扩展性的需求不断增加，Forma 提供了一种高效且易于扩展的解决方案，使开发者能够轻松地存储、查询和管理各种类型的数据，而无需频繁修改数据库架构。

Forma 选择了基于JSON Schema作为数据定义的核心方式，这使得用户可以灵活地定义和调整数据结构，而无需担心传统数据库中的表结构变更带来的复杂性。通过将数据存储在“热字段表 + EAV 表”的双存储结构中，Forma 能够在保持高性能的同时，支持复杂的查询和排序操作。



## 为什么选择 Forma？

### Forma vs RDBMS

* **动态架构 (Schema Evolution)**：传统 RDBMS 修改表结构（Schema Migration）往往伴随着锁表和停机风险。Forma 利用 **JSON Schema** 定义逻辑结构，底层采用 **“热字段表 (Entity Main) + EAV 表”** 的双存储模式。新增或修改属性只需更新元数据，无需触碰物理表结构，实现零停机演进。
* **复杂结构支持**：传统 RDBMS 处理嵌套对象或数组通常需要多表关联（Join）。Forma 的转换层（Transformer）能自动将嵌套 JSON 展平存储，同时保持对数组和深层属性的查询能力。

### Forma vs MongoDB

* **ACID 事务保障**：Forma 建立在 **PostgreSQL** 之上，天然继承了严格的 ACID 事务特性，确保核心业务数据的强一致性，避免了部分 NoSQL 数据库在强一致性场景下的短板。
* **SQL 优化器的力量**：Forma 不仅仅是存储 JSON，它通过内置的 `SQLGenerator` 将复杂的 JSON 过滤条件编译为高效的 SQL 查询（利用 CTE 和 Exists 子查询），充分利用 Postgres 强大的查询优化器，避免了 EAV 模型常见的 N+1 查询问题。

### Forma vs KV Store

* **多维检索能力**：KV Store 擅长通过 Key 获取 Value，但难以处理复杂的条件筛选（如：`age > 20 AND status = 'active'`）。Forma 支持任意属性的组合过滤、排序和分页，甚至跨 Schema 搜索。
* **数据校验与类型安全**：KV Store 通常视 Value 为黑盒。Forma 严格遵循 JSON Schema 进行数据校验（类型、格式、必填项），确保入库数据的质量。

## 使用场景

### OLTP 应用

* **动态业务系统**：适合 CRM、ERP 或 CMS 等需要频繁调整数据模型或支持用户自定义字段（Custom Fields）的系统。
* **RESTful API 自动化**：开发者只需定义 JSON Schema，Forma 即可自动提供标准的 CRUD 接口（创建、查询、更新、删除），极大缩短后端开发周期。
* **高性能读写**：核心高频字段存储在“热字段表”中，保证了关键路径的读写性能媲美原生 SQL 表，同时支持高并发的事务处理。

### OLAP 应用

* **实时运营分析**：基于 Postgres 的查询能力，支持对业务数据进行实时的统计和聚合分析。
* **数据湖集成**：支持将结构化数据导出为 Parquet 等列式格式并存储到 S3，对接大数据平台进行离线分析和报表生成。Parquet 文件可按照 `schema_id` 和 `row_id` 进行分区，方便高效更新和查询。