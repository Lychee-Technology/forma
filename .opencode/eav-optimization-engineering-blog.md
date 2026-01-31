# Forma 工程博客：如何在 Serverless 湖仓架构中优化 EAV 模式的查询性能

> 本文介绍 Forma 项目如何通过单查询 + JSON 聚合、联邦查询引擎、以及 Serverless 湖仓分层设计，将 EAV（Entity-Attribute-Value）模式的查询性能提升一到两个数量级，同时保持数据一致性与成本可控。

---

## 一、背景与动机

### 1.1 为什么选择 EAV 模式

在 SaaS 和多租户场景下，不同客户往往有截然不同的数据结构需求：A 客户的"订单"包含 12 个字段，B 客户的"订单"包含 30 个字段且名称各异；C 客户每周都要新增或废弃若干字段以适应业务变化。传统关系型表设计需要预先定义 schema，每次变更都要执行 DDL 并迁移数据——在拥有数百租户、每月上千次 schema 变更的业务中，DDL 迁移的人力和停机窗口成本是不可接受的。

EAV（Entity-Attribute-Value）模式将属性存储为行而非列，其基本形态如下：

```
| row_id | attr_id | value_text | value_numeric |
|--------|---------|------------|---------------|
| 1      | 101     | "John"     | NULL          |
| 1      | 102     | NULL       | 25            |
| 2      | 101     | "Jane"     | NULL          |
```

EAV 的核心优势在于极高的 schema 灵活性——新增属性只需插入行而无需执行 DDL，多租户隔离也很简单，通过 `schema_id` 即可区分不同租户的数据，同时稀疏属性不会浪费存储空间。然而，这种灵活性带来了两个核心挑战：

1. **查询复杂度高**：需要 pivot 或聚合操作才能还原"宽表"视图，过滤、排序、分页操作常常导致 N+1 查询或全表扫描，数据量增长后性能问题尤为突出。
2. **属性标识依赖字符串**：如果在运行时用字符串（如 `"name"`、`"age"`）作为属性 key，会增加解析成本、索引匹配难度以及拼写错误风险。

### 1.2 为什么需要弹性计算与查询加速层

在 EAV 模式解决了 schema 灵活性问题之后，下一个核心挑战是**查询性能与成本的平衡**。典型的业务数据呈现明显的时间局部性：最近 7 天的数据占查询量的 80% 以上，但只占总数据量的 5%；而 90 天前的历史数据几乎只在月末报表或审计时才会访问。EAV 表的行数随时间线性增长，大表上的排序、聚合、pivot 操作会严重拖慢查询延迟。

我们对运行时架构有以下核心需求：

1. **弹性计算**：希望在不预先分配服务器的前提下，按查询量或扫描量付费，空闲时成本归零，峰值时自动扩容。
2. **查询加速层**：将 EAV 数据 flatten（展平）为宽表格式的列式文件（Parquet），利用列式扫描和向量化执行加速过滤、排序、聚合操作。
3. **统一查询视图**：无论数据来源于 PostgreSQL 还是 Parquet，查询 API 都应返回一致的、去重后的结果，应用层无需感知底层分层逻辑。
4. **一致性保证**：当同一记录同时存在于 PostgreSQL 和 Parquet 时，必须有明确的冲突解决语义，避免返回脏数据或重复记录。

需要特别说明的是，Forma 的设计中 **PostgreSQL 始终保留全量数据**——冷数据不会从 PG 中删除。Parquet 文件仅作为"查询加速层"而非"归档层"：它是 EAV 数据的 flatten 副本，用于在高级查询场景中替代 PG 的 EAV 扫描，而不是为了节省 PG 存储空间。这种设计简化了数据一致性管理，也便于回溯、校验和运营查询。

这些需求促使我们探索一种"CDC + 联邦查询"的架构：通过 CDC（Change Data Capture）将 PG 变更增量导出为 Parquet，再由嵌入式 OLAP 引擎（DuckDB）在查询时联邦读取 PG 热数据与 Parquet 加速层。

### 1.3 为什么需要"热属性"与"长尾属性"分离

纯 EAV 模式虽然灵活，但在查询时需要对 EAV 表进行大量 JOIN 和聚合操作。假设一个 schema 有 50 个属性、100 万条记录，每条记录平均存储 30 个属性，那么 EAV 表将包含约 3000 万行。如果每次查询都需要扫描并聚合这些行，即使有索引支持，排序和分页操作的代价仍然很高。

然而，业务访问模式往往呈现"二八定律"：80% 的过滤、排序、分页操作只涉及 5–10 个高频属性（如 `created_at`、`status`、`owner_id`），而剩余的长尾属性（如 `description`、`custom_field_42`）只在详情页或导出场景才需要。如果将高频属性与长尾属性混在同一张 EAV 表中，每次查询都需要扫描大量无关行，浪费 I/O 和 CPU。

我们的需求是：**在保留 EAV 灵活性的同时，为高频属性提供接近宽表的查询性能**。这意味着需要一种机制，将高频属性"提升"到可以利用 B-tree 索引的位置，而长尾属性仍然留在 EAV 表中按需聚合。

### 1.4 为什么选择 JSON Schema 作为 Schema 定义语言

1.1 节提到的"属性标识依赖字符串"问题，本质上是缺乏一种规范化的属性定义机制。我们需要一种 schema 定义语言，能够声明属性的名称、类型、约束和关联关系，并在写入时执行校验、在查询时提供元数据支持。

JSON Schema 是描述 JSON 数据结构的行业标准（当前版本为 Draft 2020-12）。它提供了类型声明（`type`）、格式校验（`format`）、范围约束（`minimum`/`maximum`）、必填检查（`required`）等丰富的关键字，能够以声明式、与编程语言无关的方式表达数据契约。成熟的验证库（如 JavaScript 的 ajv、Python 的 jsonschema、Go 的 gojsonschema）和编辑器支持（VS Code、JetBrains 的自动补全与错误提示）使得 JSON Schema 的集成成本很低。

更重要的是，JSON Schema 已成为配置大语言模型（LLM）结构化输出的事实标准。OpenAI 的 Structured Outputs、Anthropic 的 Tool Use、Google Gemini 的 Function Declarations 等主流 LLM 接口，均采用 JSON Schema 来定义 AI 应返回的数据结构。这意味着，当 Forma 选择 JSON Schema 作为 schema 定义语言时，用户为 AI 编写的输出定义可以直接复用于 Forma 的数据建模——AI Agent 产出的结构化数据能够无缝写入 Forma 存储，形成"AI 生成 → Schema 校验 → 持久化"的闭环。

---

## 二、实现路线概览

针对上述背景与需求，Forma 采用分层优化策略：

1. **热表（entity_main）设计**：将高频属性以列的形式存储在主表中，利用 B-tree 索引加速过滤和排序。
2. **JSON Schema 编译**：通过预编译 JSON Schema 实现字段验证、关联约束和 attr_id 映射，消除运行时的字符串匹配开销，并驱动热表列位分配。
3. **单查询 + JSON 聚合**：在 PostgreSQL 端通过 CTE 一次性完成过滤、排序、分页和 EAV 聚合，消灭 N+1 查询。
4. **Serverless 联邦查询引擎**：使用嵌入式 DuckDB 作为计算层，通过 `postgres_scanner` 访问 PostgreSQL 热数据，通过 `httpfs` 访问 S3 Parquet 冷数据，实现弹性计算与冷热统一查询。

本章首先介绍热表设计，然后介绍 JSON Schema 编译机制如何驱动热表列位分配，最后详细展开单库 EAV 优化方案。联邦查询架构将在第三章展开。

---

### 2.1 热表设计：entity_main

基于 1.3 节提出的"热属性与长尾属性分离"需求，Forma 引入了 `entity_main` 热表，将高频过滤、排序、分页所依赖的属性以列的形式存储在主表中。

#### 2.1.1 热表结构

当前设计中，热表包含以下列：

| 类型 | 列数 | 列名示例 | 预建索引 |
|------|------|----------|----------|
| text | 10 | text_01 ~ text_10 | text_01, text_02, text_03 |
| smallint | 3 | smallint_01 ~ smallint_03 | smallint_01 |
| integer | 3 | integer_01 ~ integer_03 | integer_01 |
| bigint | 3 | bigint_01 ~ bigint_03 | bigint_01 |
| double precision | 5 | double_01 ~ double_05 | double_01, double_02 |
| uuid | 2 | uuid_01 ~ uuid_02 | uuid_01 |

共计 26 个热属性列位和约 10 个预建索引。

#### 2.1.2 性能收益量化

以一个包含 10 万条记录、平均每条记录 20 个 EAV 属性的 schema 为例：

- **纯 EAV 路径**：过滤条件涉及的 3 个属性全部落在 EAV 表，查询需要扫描约 200 万行 EAV 数据（10 万 × 20）并执行多次 JOIN 或 EXISTS 子查询。
- **热表路径**：这 3 个属性位于热表且有索引覆盖，过滤操作可以直接在主表完成。假设过滤后剩余 1000 条记录，后续 EAV 聚合只需处理 2 万行（1000 × 20），扫描量下降约 99%。

在实际测试中，当常用筛选字段位于热表且有索引时，相同条件下的查询延迟可从 200–500ms 降至 20–50ms，降幅达 80–90%。

#### 2.1.3 类型 Fallback 编码

由于热表列的类型是固定的，部分属性需要进行类型 fallback 编码：

- `boolean` 值存储在 `smallint` 列中（0/1）。
- `date` 值存储在 `bigint` 列中（Unix timestamp）。
- `uuid` 值如果列位不足，可以存储在 `text` 列中（需确保格式统一以保证索引生效）。

这种编码需要与查询优化器保持一致，确保生成的 SQL 谓词使用正确的类型转换。

---

### 2.2 JSON Schema：字段验证与热表列位分配

热表解决了"高频属性需要快速访问"的存储问题，但随之而来的问题是：如何决定哪些属性应该放入热表？属性的类型、约束如何定义？如何在写入时执行校验？这就需要一种规范化的 schema 定义机制。

#### 2.2.1 为什么需要 JSON Schema

在 EAV 模式下，属性的"定义"与"数据"是分离的：数据行只包含 `(row_id, attr_id, value)`，而属性的名称、类型、约束等元信息需要另行存储。如果 API 层允许任意字符串作为属性标识（如 `{"name": "John", "age": 25}`），会带来以下问题：

- **无验证**：写入时无法校验类型（`age` 是 string 还是 number？）、格式（email、uuid、date）、必填性、枚举范围等。
- **无关联**：无法表达属性间的引用关系，如 `owner_id` 应指向另一个 schema 的记录。
- **字符串匹配开销**：查询时需要将用户提交的 `"age"` 映射到数据库中的 `attr_id`，每次请求都需要查元数据表或维护缓存。
- **拼写错误风险**：用户写成 `"agee"` 或 `"Age"` 时，系统无法检测到错误。

#### 2.2.2 JSON Schema 定义示例

Forma 要求每个 schema 在创建时提供符合 JSON Schema 规范（Draft 2020-12）的定义。以下是一个典型示例：

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://forma.io/schemas/contact",
  "type": "object",
  "properties": {
    "name": {
      "type": "string",
      "minLength": 1,
      "maxLength": 255,
      "x-forma-index": true,
      "x-forma-hot": true
    },
    "email": {
      "type": "string",
      "format": "email"
    },
    "age": {
      "type": "integer",
      "minimum": 0,
      "maximum": 150,
      "x-forma-hot": true
    },
    "owner_id": {
      "type": "string",
      "format": "uuid",
      "x-forma-ref": "user"
    },
    "tags": {
      "type": "array",
      "items": { "type": "string" }
    }
  },
  "required": ["name", "email"]
}
```

其中：
- 标准 JSON Schema 关键字（`type`、`format`、`minimum`、`required` 等）用于写入时的字段验证。
- 扩展关键字 `x-forma-hot: true` 标记该属性应映射到热表列（如 2.1 节介绍的 `text_01`、`integer_01` 等列位）。
- 扩展关键字 `x-forma-index: true` 标记该属性需要在热表列上建立索引。
- 扩展关键字 `x-forma-ref: "user"` 声明引用关系，Forma 会在写入时校验目标记录存在性。

#### 2.2.3 编译流程与 attr_id 分配

当用户创建或更新 schema 时，Forma 会执行以下编译流程：

1. **解析与验证**：解析 JSON Schema 并验证其合法性（如循环引用检测、类型兼容性检查）。
2. **attr_id 分配**：为每个属性分配一个 schema 内唯一的整型 `attr_id`。新增属性分配新 ID，删除属性标记为 tombstone（保留 ID 不复用以防止历史数据混淆），重命名属性保持 ID 不变。
3. **热表列映射**：对标记了 `x-forma-hot` 的属性，根据其类型分配热表列位（如 `text_01`、`integer_02`）。列位分配记录在 `schema_column_map` 表中。
4. **索引声明记录**：对标记了 `x-forma-index` 的属性，记录索引需求。实际索引由 DBA 或自动化流程创建。
5. **关联图构建**：解析 `x-forma-ref` 声明，构建 schema 间的引用图，用于级联校验和 JOIN 提示。
6. **版本快照**：将编译结果持久化为 `schema_version`，支持历史回溯和计划缓存失效。

编译产物示例（内部元数据）：

```
schema_id: 42
version: 7
attributes:
  - attr_id: 1, name: "name",     type: string,  hot_column: text_01,    indexed: true
  - attr_id: 2, name: "email",    type: string,  hot_column: null,       indexed: false
  - attr_id: 3, name: "age",      type: integer, hot_column: integer_01, indexed: false
  - attr_id: 4, name: "owner_id", type: uuid,    hot_column: null,       indexed: false, ref: user
  - attr_id: 5, name: "tags",     type: array,   hot_column: null,       indexed: false
```

#### 2.2.4 运行时查询优化

编译后的 schema 元数据在查询执行时带来以下收益：

- **O(1) 属性查找**：用户提交的 JSON 查询 `{"filter": {"age": {"$gt": 18}}}` 可以直接映射到 `attr_id = 3`，无需字符串比较或哈希查找。
- **热表谓词下推**：查询优化器检测到 `age` 映射到 `integer_01`，直接生成 `entity_main.integer_01 > 18`，利用索引扫描。
- **类型强制转换**：查询值 `18` 被强制转换为 `integer`，避免类型不匹配导致的全表扫描。
- **计划缓存命中**：相同 query shape + schema_version 可以复用已编译的执行计划，减少冷启动开销。

#### 2.2.5 字段验证与写入流程

写入时，Forma 根据编译后的 schema 执行验证：

1. **Required 检查**：确保 `name` 和 `email` 字段存在。
2. **类型检查**：`age` 必须是整数，`email` 必须符合 email 格式。
3. **范围检查**：`age` 必须在 0–150 之间。
4. **引用校验**：`owner_id` 如果提供，必须在 `user` schema 中存在对应记录。
5. **热表写入**：将 `name` 写入 `entity_main.text_01`，将 `age` 写入 `entity_main.integer_01`。
6. **EAV 写入**：将所有属性（包括热属性的冗余副本或仅长尾属性，取决于配置）写入 EAV 表。

验证失败时返回结构化错误，包含字段路径和违反的约束。

---

### 2.3 消灭 N+1 查询：单查询 + JSON 聚合

#### 2.3.1 原始实现的性能问题

`QueryPersistentRecords` 方法的原始实现存在典型的 N+1 查询问题。第一步查询 EAV 表获取符合条件的 `rowIDs`（1 次查询），第二步对每个 `rowID` 循环查询 `entity_main` 表以获取主表数据（N 次查询）：

```go
// 步骤 1: 查询 EAV 表，获取 rowIDs (1 次查询)
rowIDs, attributeMap, totalRecords, err := r.runAdvancedAttributeQuery(...)

// 步骤 2: 对每个 rowID 循环查询 entity_main 表 (N 次查询)
for _, rowID := range rowIDs {
    record, err := r.loadMainRecord(ctx, tables.EntityMain, schemaID, rowID)
    // 每次循环执行一次 SELECT！
}
```

这种实现的性能影响非常直接：查询 100 条记录需要 101 次数据库往返，假设每次往返延迟 10ms，总耗时将超过 1 秒；在高并发场景下，数据库连接池会迅速耗尽，系统吞吐量急剧下降。

#### 2.3.2 优化方案：单查询 + JSON 聚合

核心思想是在数据库端完成所有数据的关联和聚合，应用层只需执行一次查询。通过 CTE 链式处理，依次完成过滤、排序、分页、回主表、聚合 EAV 等步骤，最终使用 `JSON_AGG` 和 `JSON_BUILD_OBJECT` 将多行 EAV 数据聚合为单个 JSON 数组，所有数据在一次查询中返回，应用层只需解析 JSON 即可还原完整记录。

```sql
WITH anchor AS (
    -- 根据条件筛选符合条件的 row_id
    SELECT DISTINCT t.row_id
    FROM eav_table t
    WHERE t.schema_id = $1 AND [conditions]
),
keys AS (
    -- 提取排序键值，计算总数
    SELECT a.row_id, COUNT(*) OVER() AS total
    FROM anchor a
),
ordered AS (
    -- 排序和分页
    SELECT row_id, total
    FROM keys
    ORDER BY ... LIMIT ... OFFSET ...
),
main_data AS (
    -- JOIN entity_main 表获取主表数据
    SELECT m.*, o.total
    FROM ordered o
    INNER JOIN entity_main m 
        ON m.ltbase_schema_id = $1 AND m.ltbase_row_id = o.row_id
),
eav_aggregated AS (
    -- 使用 JSON_AGG 聚合 EAV 数据
    SELECT 
        e.row_id,
        JSON_AGG(
            JSON_BUILD_OBJECT(
                'attr_id', e.attr_id,
                'value_text', e.value_text,
                'value_numeric', e.value_numeric
            ) ORDER BY e.attr_id
        )::TEXT AS attributes_json
    FROM ordered o
    INNER JOIN eav_table e ON e.row_id = o.row_id
    GROUP BY e.row_id
)
SELECT 
    m.*,
    COALESCE(e.attributes_json, '[]') AS attributes_json
FROM main_data m
LEFT JOIN eav_aggregated e ON e.row_id = m.ltbase_row_id;
```

#### 2.3.3 性能提升

优化前后的查询次数对比如下表所示，假设每次数据库往返延迟为 10ms：

| 记录数 | 原实现查询次数 | 优化后查询次数 | 原实现总延迟 | 优化后总延迟 | 延迟降幅 |
|--------|---------------|---------------|-------------|-------------|---------|
| 10 条 | 11 次 | 1 次 | 110ms | ~15ms | 86% |
| 50 条 | 51 次 | 1 次 | 510ms | ~20ms | 96% |
| 100 条 | 101 次 | 1 次 | 1010ms | ~25ms | 97% |

优化后的单次查询虽然 SQL 更复杂，但由于所有操作都在数据库端完成，网络往返次数从 N+1 降至 1，总延迟大幅下降。在高延迟网络环境（如跨区域部署）下，收益更为明显：假设单次往返延迟为 50ms，查询 100 条记录的延迟将从 5050ms 降至约 80ms，降幅达 98%。

#### 2.3.4 实现细节

实现层面新增了三个核心组件：`optimizedQuerySQLTemplate` 是 CTE 模板，覆盖过滤、排序、分页、主表回填、EAV 聚合等完整流程；`runOptimizedQuery` 方法负责构建并执行单查询，直接返回 `PersistentRecord` 列表；`scanOptimizedRow` 方法负责扫描主表列和聚合 JSON，将 JSON 反序列化为 `[]Attribute` 结构。为保证向后兼容，原有的 `runAdvancedAttributeQuery` 和 `hydrateRecords` 方法仍然保留作为 fallback，外部 API 接口完全不变。该优化依赖 PostgreSQL 9.4+ 版本提供的 `JSON_AGG` 和 `JSON_BUILD_OBJECT` 函数。

---

## 三、CDC 与联邦查询引擎

单库优化解决了 OLTP 场景的 N+1 问题，但面对海量历史数据（百万至亿级），PostgreSQL 的 EAV 表扫描仍然是性能瓶颈——即使有索引支持，排序和分页操作在大表上的代价仍然很高。为此，我们引入 CDC（Change Data Capture）机制将 EAV 数据增量导出为 flatten 的 Parquet 文件，再由联邦查询引擎（DuckDB）在查询时统一读取 PG 热数据与 Parquet 加速层。

### 3.1 CDC 过程：从 EAV 到 Flatten Parquet

#### 3.1.1 整体数据流

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           写入路径                                       │
│  Application ──▶ entity_main + eav_data ──▶ change_log (flushed_at=0)  │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         CDC 导出作业                                     │
│  1. 扫描 change_log WHERE flushed_at = 0                                │
│  2. JOIN entity_main + eav_data，按 schema 投影                          │
│  3. Flatten EAV → 宽表 (row_id, attr_1, attr_2, ..., attr_N)            │
│  4. 写入 S3 Delta Parquet                                                │
│  5. 回写 change_log SET flushed_at = now()                              │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         对象存储布局                                     │
│  s3://bucket/delta/<schema_id>/<uuid_v7>.parquet  (增量文件)             │
│  s3://bucket/base/<schema_id>/<min>_<max>.parquet (合并后的基准文件)     │
└─────────────────────────────────────────────────────────────────────────┘
```

#### 3.1.2 change_log 表结构

`change_log` 是 CDC 的核心：每次写入 `entity_main`/`eav_data` 时，同步插入一条变更记录。

| 列名 | 类型 | 说明 |
|------|------|------|
| id | bigserial | 自增主键，保证变更顺序 |
| schema_id | uuid | 所属 schema |
| row_id | uuid | 变更的记录 ID |
| op | smallint | 操作类型：1=INSERT, 2=UPDATE, 3=DELETE |
| created_at | bigint | 变更时间戳（Unix ms） |
| flushed_at | bigint | 导出时间戳；0 表示未导出 |

关键索引：`CREATE INDEX idx_changelog_pending ON change_log (schema_id, row_id) WHERE flushed_at = 0;`

#### 3.1.3 增量导出（Delta Export）

CDC 导出作业周期性运行（默认每分钟或达到阈值行数时触发），执行以下步骤：

1. **扫描待导出变更**：`SELECT DISTINCT row_id FROM change_log WHERE schema_id = ? AND flushed_at = 0`。
2. **读取完整记录**：JOIN `entity_main` 和 `eav_data`，按 schema 的 attr_id 列表投影为宽表结构。
3. **类型对齐**：根据 JSON Schema 编译产物，将热表列和 EAV 值转换为 Parquet 目标类型（如 `text_01` → `VARCHAR`，`integer_01` → `INT32`）。
4. **写入 Delta Parquet**：使用 DuckDB 的 `COPY ... TO` 写入 S3，文件名为 `<uuid_v7>.parquet`（UUID v7 保证时间有序）。
5. **标记已刷盘**：`UPDATE change_log SET flushed_at = ? WHERE row_id IN (?) AND flushed_at = 0`。

导出 SQL 示例（简化）：

```sql
COPY (
    SELECT
        m.ltbase_row_id AS row_id,
        m.ltbase_created_at AS created_at,
        m.ltbase_updated_at AS updated_at,
        m.ltbase_deleted_at AS deleted_at,
        CAST(m.text_01 AS VARCHAR) AS name,
        CAST(m.integer_01 AS INT32) AS age,
        MAX(CASE WHEN e.attr_id = 205 THEN e.value_text END) AS email,
        -- ... 其他属性
    FROM entity_main m
    LEFT JOIN eav_data e ON m.ltbase_row_id = e.row_id
    WHERE m.ltbase_schema_id = $SCHEMA_ID
      AND m.ltbase_row_id IN ($ROW_IDS)
    GROUP BY m.ltbase_row_id, m.ltbase_created_at, ...
) TO 's3://bucket/delta/<schema_id>/<uuid>.parquet'
  (FORMAT PARQUET, COMPRESSION ZSTD);
```

#### 3.1.4 全量初始化（Base Init）

对于已有存量数据的部署，提供 `cdc-init` 工具一次性导出全量数据为 Base Parquet：

- 直接从 `entity_main` + `eav_data` 读取，无需 `change_log`。
- 按 `ltbase_row_id` 分页，每批 50,000 行。
- 输出为 Base 文件：`s3://bucket/base/<schema_id>/<min_row_id>_<max_row_id>.parquet`，目标大小 ~256MB。
- 后续增量变更由 Delta 路径补齐。

#### 3.1.5 Compaction（合并）

Delta 文件数量增长后会影响查询性能（每个文件都需要打开和扫描）。Compaction 作业定期将多个 Delta 合并为新的 Base 文件：

- 触发条件：Delta 文件数 > 10，或 Delta 总大小 > 1GB，或最老 Delta 文件 > 24h。
- 合并逻辑：读取所有 Delta + 现有 Base，按 `row_id` 去重（保留最新版本），过滤软删除，写入新 Base。
- 原子发布：写入临时路径，成功后 rename 替换旧文件。
- 清理旧 Delta：确认新 Base 可读后删除已合并的 Delta 文件。

### 3.2 架构总览

```
┌─────────────────────────────────────────────────────────────┐
│                      Client Request                          │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                       Query Router                           │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────┐  │
│  │ 简单查询?   │───▶│ PostgreSQL  │    │ 联邦查询引擎   │  │
│  │ AND-only    │    │ 直接执行    │    │ (DuckDB)       │  │
│  │ 索引排序列  │    └─────────────┘    └─────────────────┘  │
│  └─────────────┘                                             │
└─────────────────────────────────────────────────────────────┘
                              │
         ┌────────────────────┴────────────────────┐
         │                                         │
         ▼                                         ▼
┌─────────────────────┐              ┌─────────────────────────┐
│   Simple Path       │              │    Advanced Path        │
│   PostgreSQL Only   │              │    DuckDB Federated     │
│                     │              │                         │
│   • OFFSET 分页     │              │   ┌─────────────────┐   │
│   • 精确 COUNT      │              │   │ postgres_scanner│   │
│   • 随机跳页        │              │   │ (Hot: PG)       │   │
└─────────────────────┘              │   └─────────────────┘   │
                                     │   ┌─────────────────┐   │
                                     │   │ httpfs / S3     │   │
                                     │   │ (Warm: Delta)   │   │
                                     │   │ (Cold: Base)    │   │
                                     │   └─────────────────┘   │
                                     └─────────────────────────┘
```

### 3.3 数据分层与查询路由

在联邦查询中，数据按"是否已导出到 Parquet"分为以下层级：

| 层级 | 存储位置 | 数据状态 | 说明 |
|------|----------|----------|------|
| Hot | PG `change_log` | 未刷盘（`flushed_at=0`） | 最新变更，可变，需实时读取 |
| Warm | S3 Delta Parquet | 已刷盘、未合并 | 近期导出的增量文件，不可变 |
| Cold | S3 Base Parquet | 已合并 | Compaction 后的基准文件，不可变 |

需要再次强调：**PostgreSQL 始终保留全量数据**。Hot/Warm/Cold 的分层是针对 Parquet 加速层而言的，PG 侧的 `entity_main` 和 `eav_data` 不会因为 CDC 导出而删除任何数据。这种"全量 PG + 增量 Parquet"的设计有以下优势：

- **回退安全**：如果 Parquet 文件损坏或 CDC 作业异常，可以随时从 PG 重新导出。
- **一致性简化**：PG 始终是 Source of Truth，Parquet 只是"物化视图"。
- **运营查询**：DBA 或运营人员可以直接查询 PG，无需通过联邦引擎。

查询路由器根据查询复杂度选择执行路径：

- **Simple Path（PG-only）**：AND-only 条件、排序列有索引、支持 OFFSET 分页——直接由 PostgreSQL 执行，利用 2.3 节的单查询 + JSON 聚合优化。
- **Advanced Path（DuckDB 联邦）**：包含 OR 条件、EAV 属性过滤、或需要对大表做全局排序——由 DuckDB 联邦读取 PG Hot 层 + S3 Parquet，执行去重、过滤、排序后返回结果。

### 3.4 一致性模型：Anti-Join 与 Last-Write-Wins

当同一 `row_id` 存在于多个层级时，必须保证一致性。单纯的时间戳比较不够可靠，因为可能存在时钟偏差或竞态条件。Forma 采用 DirtySet 反连接策略：将 PostgreSQL 中未刷盘的 `row_id` 集合（`flushed_at = 0`）视为"脏数据集"，任何在 S3 中发现的、同时存在于脏数据集中的记录都会被立即丢弃，无论其时间戳如何。用公式表示：$Result = (S3_{Data} \setminus DirtySet) \cup PG_{HotData}$。

```sql
-- 脏数据集：PostgreSQL 中未刷盘的 row_id
dirty_ids AS (
    SELECT row_id
    FROM change_log
    WHERE flushed_at = 0 AND schema_id = $SCHEMA_ID
)

-- S3 数据：排除脏数据集中的 row_id
s3_source AS (
    SELECT ...
    FROM read_parquet($S3_PATHS)
    WHERE row_id NOT IN (SELECT row_id FROM dirty_ids)
)
```

对于合并后仍可能存在的重复记录（例如 Warm 和 Cold 中同一 `row_id` 的不同版本），使用 `QUALIFY ROW_NUMBER()` 实现 Last-Write-Wins 语义，按优先级排序（Hot=3, Warm=2, Cold=1）取第一条，同时过滤软删除记录（`deleted_at IS NULL OR deleted_at = 0`）。

```sql
SELECT *
FROM unified
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY row_id 
    ORDER BY precedence DESC
) = 1
AND (deleted_at IS NULL OR deleted_at = 0)
```

### 3.5 两阶段合并策略

为避免对海量 Cold 数据做全局排序（复杂度 O(N log N)，N 可能达到亿级），采用两阶段合并策略。Phase 1 先合并 Hot 和 Warm 数据（有界：≤ 250K 行），执行去重、排序后取 M 条候选记录，同时记录这 M 条记录的排序键边界值（min/max sort_key）。Phase 2 利用边界值和游标对 Cold 数据进行剪枝，只扫描可能进入最终结果的 Parquet 文件和行，然后与 Phase 1 结果合并、去重、排序，取最终的 page_size 条记录。

M 值的启发式设置为 `page_size × 3`，可根据实际 selectivity 动态调整。如果 Phase 1 已经返回足够多的高排序键记录，Phase 2 可以跳过大部分 Cold 文件，扫描量可能下降 80–95%。

### 3.6 查询翻译：双路径 SQL 生成

用户提交的 JSON DSL 查询需要翻译为可执行的 SQL。由于查询可能同时涉及 PostgreSQL 和 DuckDB 两个引擎，Query Translator 会生成两种 SQL 片段：PG Pushdown 片段用于 `postgres_scan` 的参数，使用物理列名（如 `integer_01 > 18 AND text_01 LIKE 'John%'`），只包含映射到 `entity_main` 的属性；DuckDB 逻辑片段用于 CTE 的 WHERE 子句，使用逻辑列名（如 `age > 18 AND name LIKE 'John%' AND tag = 'developer'`），包含所有属性。EAV 属性无法下推到 PostgreSQL，需要在 DuckDB 端通过 pivot 后过滤。

### 3.7 完整执行模板示例

```sql
PRAGMA memory_limit='4GB';
PRAGMA threads=4;

WITH
-- CTE 1: 脏数据集
dirty_ids AS (
    SELECT row_id
    FROM postgres_scan($PG_CONN, 
        'SELECT row_id FROM change_log 
         WHERE flushed_at = 0 AND schema_id = ' || $SCHEMA_ID)
),

-- CTE 2: S3 数据源（排除脏数据）
s3_source AS (
    SELECT
        row_id,
        ltbase_created_at AS created_at,
        name, age, tag,
        2 AS precedence
    FROM read_parquet($S3_PATHS)
    WHERE 
        row_id NOT IN (SELECT row_id FROM dirty_ids)
        AND age > 18
        AND name LIKE 'John%'
        AND tag = 'developer'
        AND (ltbase_deleted_at IS NULL OR ltbase_deleted_at = 0)
),

-- CTE 3: PostgreSQL 数据源
pg_source AS (
    SELECT
        m.ltbase_row_id AS row_id,
        m.ltbase_created_at AS created_at,
        CAST(m.text_01 AS VARCHAR) AS name,
        CAST(m.integer_01 AS INTEGER) AS age,
        MAX(CASE WHEN e.attr_id = 205 THEN e.value_text END) AS tag,
        3 AS precedence
    FROM postgres_scan($PG_CONN, 'change_log') cl
    JOIN postgres_scan($PG_CONN, 'entity_main') m 
        ON cl.row_id = m.ltbase_row_id
    LEFT JOIN postgres_scan($PG_CONN, 'eav_data') e 
        ON cl.row_id = e.row_id
    WHERE cl.flushed_at = 0 AND cl.schema_id = $SCHEMA_ID
    GROUP BY m.ltbase_row_id, m.ltbase_created_at, m.text_01, m.integer_01
    HAVING m.integer_01 > 18 AND m.text_01 LIKE 'John%'
),

-- CTE 4: 合并
unified AS (
    SELECT * FROM s3_source
    UNION ALL
    SELECT * FROM pg_source
)

-- 最终查询：去重 + 排序 + 分页
SELECT row_id, created_at, name, age, tag
FROM unified
QUALIFY ROW_NUMBER() OVER (PARTITION BY row_id ORDER BY precedence DESC) = 1
ORDER BY created_at DESC
LIMIT $PAGE_SIZE OFFSET $OFFSET;
```

---

## 四、Serverless 运行时考量

### 4.1 计算引擎选型

在 Serverless 湖仓架构中，计算引擎的选择直接影响冷启动延迟、内存限制和成本模型。DuckDB 是一个嵌入式 OLAP 引擎，冷启动时间低于 100ms，适合 Lambda 或容器环境；通过 `postgres_scanner` 扩展可以直连 PostgreSQL，无需数据同步；内存限制在 Hot+Warm ≤ 250K 行的约束下可以接受（通常 4GB 足够）。相比之下，Trino/Presto 需要集群部署且冷启动较慢（秒级），ClickHouse Cloud 和 BigQuery/Athena 虽然完全托管，但延迟通常在秒级，不适合低延迟场景。Forma 当前选择 DuckDB 嵌入式方案。

### 4.2 连接与资源管理

Serverless 环境下短连接开销较大，每次 `postgres_scanner` 查询都会建立新连接，建议使用 PgBouncer 或 RDS Proxy 来复用连接并控制并发数。内存管理方面，应设置 `PRAGMA memory_limit`（如 4GB）防止 OOM，大结果集应使用流式返回而非一次性加载。临时存储方面，DuckDB 会溢写到 `/tmp` 目录，Serverless 函数需确保该目录可用且有足够空间（通常 512MB–10GB）。

### 4.3 成本模型

Serverless 湖仓的主要成本项包括：S3 请求费用（与文件数量相关，可通过合并小文件和文件剪枝降低）、S3 扫描费用（与数据量相关，可通过谓词下推和列裁剪降低）、计算时长费用（与查询复杂度相关，可通过缓存计划和限制 M 值降低）、PostgreSQL 连接费用（与并发数相关，可通过连接池和批量查询降低）。在典型工作负载下，一个每秒处理 100 次查询、每次扫描 10MB Parquet 数据的系统，S3 月成本约为 $30–50，计算成本取决于具体的 Serverless 平台定价。

---

## 五、设计改进建议

基于当前实现，以下是可进一步优化的方向。

当前的 Simple/Advanced 路由基于静态规则（如是否包含 OR 条件、排序列是否有索引等），但缺乏运行时反馈。建议暴露实际执行路径给调用方（通过 response header 或 debug 字段），并记录路由命中率和错误率以驱动规则调优；当 DuckDB 出现 OOM 或超时时，应自动回退到 PG-only 路径（受一致性约束）。

计划缓存目前按 `(schema_id, query_shape_hash)` 进行，但缺乏版本失效机制。建议引入 `schema_version` 作为缓存键的一部分，schema 变更时自动清理相关缓存；M 值（Phase1 缓冲）目前固定为 `page_size × 3`，可以基于历史 selectivity 和实际扫描行数动态调整，以平衡内存使用和剪枝效果；双路径 SQL 片段也可以缓存，减少冷启动时的编译成本。

文件剪枝目前基于 cursor 和 Phase1 边界，但未充分利用 Parquet 文件元数据。建议利用 Parquet 文件的 min/max 统计信息进行列值范围剪枝，记录文件热度以优先扫描高命中率文件；对常用 EAV 属性可在 Parquet 侧做宽表物化，减少 DuckDB 端的 pivot 成本——例如，将 top 10 高频 EAV 属性展平为 Parquet 顶层列，实测可将 Phase1 处理时间降低 30–50%。

类型与算子一致性方面，当前存在 bool/uuid/date 的 fallback 转换，但缺乏系统性验证。建议建立 PG → DuckDB → Parquet 的类型映射矩阵，明确每种类型的转换规则；对 LIKE/prefix 的转义规则和大小写敏感性应统一约定；增加跨引擎算子一致性回归测试，确保同一谓词在不同引擎上返回相同结果。

安全方面，`postgres_scan` 目前存在字符串拼接，有 SQL 注入风险。建议尽量静态化 SQL 模板，动态部分（如 schema_id、attr_id）严格参数化或做白名单校验；列名和表名应基于元数据白名单校验；S3 路径构造只接受校验过的 schema_id 和 project_id，防止路径穿越。

观测方面，当前定义了指标（如 `pushdown_efficiency`、扫描行数）但未与路由/剪枝策略联动。建议当 `pushdown_efficiency`（PG 扫描行数 / 最终结果行数）超过阈值（如 100）时自动告警，提示可能缺少索引；当 hot/warm/cold 扫描行数出现异常偏移时触发 compaction 或分区调整；为 debug 请求返回完整"计划解释"，包括驱动选择、剪枝原因、M 值等。

断路器当前设置为 5 次失败/30s 触发，但恢复探测缺乏抖动。建议恢复探测引入随机 jitter（如 5–15s），避免多个实例同时探活导致雪崩重试；区分瞬时错误（超时）与持久错误（配置错误），采用不同的回退和恢复策略；Serverless 冷启动场景下，首次请求的超时阈值可适当放宽（如从 5s 增至 10s）。

冷数据 Compaction 与 TTL 方面，Delta → Base 的 compaction 策略目前未详细定义。建议明确 compaction 触发条件（如 Delta 文件数 > 10 或总大小 > 1GB 或最老文件 > 24h），监控 DirtySet 的"擦除延迟"（即已删除记录从 S3 消失的时间），确保不超过配置的 SLA（如 1h）；支持 schema 级别的 TTL 策略，自动清理过期数据以控制存储成本。

---

## 六、总结

Forma 通过分层优化解决了 EAV 模式的性能瓶颈。在 schema 管理层面，通过 JSON Schema 预编译实现了字段验证、关联约束和 attr_id 映射，消除了运行时的字符串匹配开销，同时为热表列位分配和查询优化提供了元数据基础。在单库 OLTP 层面，通过热表设计将高频属性提升为可索引列，再通过单查询 + JSON 聚合将查询次数从 N+1 降至 1，100 条记录的查询延迟从 1000ms+ 降至 25ms 左右。在联邦 OLAP 层面，通过 DuckDB + Anti-Join + 两阶段合并实现了实时湖仓式查询，热数据和冷数据可以统一查询而无需数据同步。在 Serverless 层面，通过嵌入式计算引擎、有界数据层约束和流式返回实现了零预配、弹性伸缩和成本可控。

核心设计原则包括：JSON Schema 驱动（编译时确定 attr_id、列位、索引和关联，减少运行时开销）；有界假设（Hot + Warm ≤ 250K 行）使得全扫描策略可行，简化了查询执行逻辑；一致性优先通过 DirtySet 反连接和 Last-Write-Wins 语义确保正确性；渐进降级从联邦查询回退到 PG-only，保证系统始终有响应；可观测驱动通过指标、日志和计划解释形成闭环调优。

后续方向包括：自适应 M 值与路由策略以平衡内存使用和剪枝效果，Parquet 宽表物化常用 EAV 属性以减少 pivot 成本，跨引擎类型/算子一致性测试矩阵以确保查询正确性，JSON Schema 增量编译以支持更高频的 schema 变更，以及 Serverless 原生的连接池与内存管理以降低资源开销。

---

## 附录：索引建议

PostgreSQL 侧需要建立以下索引以支持高效查询。`entity_main` 表应在 `(ltbase_schema_id, ltbase_row_id)` 上建立主键或唯一索引以支持快速主键查找，在 `(ltbase_schema_id, ltbase_created_at DESC)` 上建立索引以支持按创建时间排序的分页查询。`change_log` 表应在 `(schema_id, row_id)` 上建立部分索引（`WHERE flushed_at = 0`）以支持脏数据集的快速查找。`eav_data` 表应在 `(schema_id, attr_id, row_id)` 上建立索引以支持属性查找，在 `(schema_id, attr_id, value_int)` 和 `(schema_id, attr_id, value_text)` 上建立部分索引（`WHERE value_int/value_text IS NOT NULL`）以支持值过滤。

```sql
CREATE INDEX idx_entity_schema_row 
    ON entity_main (ltbase_schema_id, ltbase_row_id);
CREATE INDEX idx_entity_schema_created 
    ON entity_main (ltbase_schema_id, ltbase_created_at DESC);

CREATE INDEX idx_changelog_dirty 
    ON change_log (schema_id, row_id) 
    WHERE flushed_at = 0;

CREATE INDEX idx_eav_lookup 
    ON eav_data (schema_id, attr_id, row_id);
CREATE INDEX idx_eav_int_value 
    ON eav_data (schema_id, attr_id, value_int) 
    WHERE value_int IS NOT NULL;
CREATE INDEX idx_eav_text_value 
    ON eav_data (schema_id, attr_id, value_text) 
    WHERE value_text IS NOT NULL;
```

Parquet 文件布局方面，建议按 `schema_id` 分目录存储，文件内按 `ltbase_created_at, ltbase_row_id` 排序以支持范围剪枝，使用 ZSTD 压缩以平衡压缩率和解压速度，目标文件大小控制在 256–512MB 以平衡元数据开销和并行度。常用 EAV 属性应展平为 Parquet 顶层列以支持谓词下推，长尾属性可保留为 `map<string, variant>` 或 `list<struct>` 结构。

---

*本文基于 Forma 项目的设计文档与实现经验整理，欢迎反馈与讨论。*
