# Data Model

用户数据存储在[AWS DSQL](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-supported-data-types.html)中。AWS DSQL是一个Serverless的数据库，兼容`PostgreSQL 16`。

Forma 同时支持 PostgreSQL 16 和 Aurora DSQL。DSQL 没有 advisory lock，也从不让语句等待锁，所以数据库引擎需要显式配置，不做自动探测：写路径用 `DB_ENGINE`（`forma.DatabaseConfig.Engine`），CDC 工具（`cdc-flush`、`cdc-init`、`manifest-reconcile`）用 `--pg-engine`。同一数据库的所有写入方和 CDC 工具必须配置同一个引擎。`aurora-dsql` 引擎下，写路径用 `<entity_main>_row_guard` 表中的 guard row 互斥，CDC 的 per-schema 锁是 `<entity_main>_schema_lease` 表中的租约（#633）。两者目前只在模拟 DSQL 的 PostgreSQL 上验证过，尚未在真实集群上运行。`init-db` 的 DDL 还不能在 DSQL 上执行（#690），DuckDB `postgres` 扩展对 DSQL 尚未验证（#691）。详见 [Aurora DSQL](../aurora-dsql.md)。

每个项目还有元数据访问接口：

```golang
type SchemaRegistry interface {
    // GetSchemaAttributeCacheByName retrieves schema ID and attribute cache by schema name
    GetSchemaAttributeCacheByName(name string) (int16, SchemaAttributeCache, error)
    // GetSchemaAttributeCacheByID retrieves schema name and attribute cache by schema ID
    GetSchemaAttributeCacheByID(id int16) (string, SchemaAttributeCache, error)
    // GetSchemaByName retrieves schema details by name
    GetSchemaByName(name string) (int16, JSONSchema, error)
    // GetSchemaByID retrieves schema details by ID
    GetSchemaByID(id int16) (string, JSONSchema, error)
    // ListSchemas returns a list of all registered schema names
    ListSchemas() []string
}

type ValueType string

const (
    ValueTypeText     ValueType = "text"
    ValueTypeSmallInt ValueType = "smallint"
    ValueTypeInteger  ValueType = "integer"
    ValueTypeBigInt   ValueType = "bigint"
    ValueTypeNumeric  ValueType = "numeric"  // double precision
    ValueTypeDate     ValueType = "date"     // for JSON attributes with format `date`
    ValueTypeDateTime ValueType = "datetime" // for JSON attributes with format `date-time`
    ValueTypeUUID     ValueType = "uuid"
    ValueTypeBool     ValueType = "bool"
)

type AttributeMetadata struct {
    AttributeName string             `json:"attr_name"`  // attr_name, JSON Path
    AttributeID   int16              `json:"attr_id"`    // attr_id
    ValueType     ValueType          `json:"value_type"` // 'text', 'numeric', 'date', 'bool'...
    Required      bool               `json:"required,omitempty"`
    ColumnBinding *MainColumnBinding `json:"column_binding,omitempty"`
}

type MainColumnBinding struct {
    ColumnName MainColumn         `json:"col_name"`
    Encoding   MainColumnEncoding `json:"encoding,omitempty"`
}
```

每个项目有3个核心DSQL Tables: `entity main` (主表), `EAV Data` (扩展属性表) 和 `Change Log` (变更日志表)。另有 `aurora-dsql` 引擎使用的两张锁表 `<entity_main>_row_guard` 和 `<entity_main>_schema_lease`，在 PostgreSQL 上它们保持为空。

### 时间日期处理

系统字段如创建时间（`ltbase_created_at`）和更新时间（`ltbase_updated_at`）存储为`BIGINT`（Unix时间戳，毫秒级别）。

### Bool处理

布尔类型通常存储为`smallint`类型（`1`表示`true`，`0`表示`false`），或者作为文本存储（取决于 `MainColumnEncoding` 配置）。

## Primary Table

`entity main`表用于存储高频访问的属性，以提升查询性能。表名格式为`entity_main_<base32(client_id)>_<project_id>`（实际部署时可配置）。

| Name              | Type             | Note         |
| :---------------- | :--------------- | :----------- |
| ltbase_schema_id  | smallint         | From 100     |
| ltbase_row_id     | uuid             | UUID v7      |
| text_01 ~ 10      | text             | 文本列       |
| smallint_01 ~ 03  | smallint         | smallint列   |
| integer_01 ~ 03   | integer          | integer列    |
| bigint_01 ~ 03    | bigint           | bigint列     |
| double_01 ~ 03    | double precision | 数字列       |
| uuid_01 ~ 02      | uuid             | uuid列       |
| ltbase_created_at | bigint           | 记录创建时间 |
| ltbase_updated_at | bigint           | 记录更新时间 |
| ltbase_deleted_at | bigint           | 记录删除时间 |

索引配置：

```sql
-- 索引配置可能根据实际部署调整，以下为典型配置
CREATE INDEX ... ON ... (ltbase_schema_id, ltbase_row_id, text_01);
CREATE INDEX ... ON ... (ltbase_schema_id, ltbase_row_id, text_02);
CREATE INDEX ... ON ... (ltbase_schema_id, ltbase_row_id, text_03);
CREATE INDEX ... ON ... (ltbase_schema_id, ltbase_row_id, smallint_01);
CREATE INDEX ... ON ... (ltbase_schema_id, ltbase_row_id, integer_01);
CREATE INDEX ... ON ... (ltbase_schema_id, ltbase_row_id, bigint_01);
CREATE INDEX ... ON ... (ltbase_schema_id, ltbase_row_id, bigint_02);
CREATE INDEX ... ON ... (ltbase_schema_id, ltbase_row_id, double_01);
CREATE INDEX ... ON ... (ltbase_schema_id, ltbase_row_id, double_02);
CREATE INDEX ... ON ... (ltbase_schema_id, ltbase_row_id, uuid_01);
```

这些单列索引的第二列是 `ltbase_row_id`，无法按列值定位。需要按列值查找或排序的访问路径（如“到期的待处理消息”），
由 schema 在顶层用 `x-forma-indexes` 声明组合索引：`init-db` 为每个声明以 `CREATE INDEX CONCURRENTLY`
建立 `(ltbase_schema_id, <绑定列…>, ltbase_row_id)` 索引，不阻塞写入，且从不自动删除索引。
只能声明 column-bound 的非 bool 属性，每个索引 1 到 4 个属性。详见 [Declared Indexes](../schema-indexes.md)。

## EAV Data Table

EAV数据表使用类型化的值列。表名格式为`eav_<base32(client_id)>_<project_id>`。

```sql
CREATE TABLE eav_data_<base32_client_id>_<project_id>
(
    schema_id     smallint                   NOT NULL,
    row_id        uuid                       NOT NULL,
    attr_id       smallint                   NOT NULL,
    array_indices text      default ''::text NOT NULL,
    value_text    text,
    value_numeric numeric,
    primary key (schema_id, row_id, attr_id, array_indices)
);

create index ..._numeric_idx on ... (schema_id, attr_id, value_numeric, row_id) where (value_numeric IS NOT NULL);
create index ..._text_idx    on ... (schema_id, attr_id, value_text, row_id)    where (value_text IS NOT NULL);
```

| Name          | Type             | Note               |
| :------------ | :--------------- | :----------------- |
| schema_id     | smallint         | Starts From 100    |
| row_id        | uuid             | UUID v7            |
| attr_id       | smallint         | 引用 attributes    |
| array_indices | text             | 数组索引           |
| value_text    | text             | 文本类型值         |
| value_numeric | numeric          | 数字类型值         |

## Change Log Table

用于记录变更日志，支持数据同步和审计。

```sql
CREATE TABLE change_log_<base32_client_id>_<project_id> (
    schema_id  SMALLINT NOT NULL,
    row_id     UUID     NOT NULL,
    flushed_at BIGINT   NOT NULL DEFAULT 0,
    changed_at BIGINT   NOT NULL,
    deleted_at BIGINT,
    primary key (schema_id, row_id, flushed_at)
);
```
