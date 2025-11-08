# Data Model

用户数据存储在[AWS DSQL](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-supported-data-types.html)中。AWS DSQL是一个Serverless的数据库，兼容`PostgreSQL 16`。

每个项目还有两个原数据访问接口：

```golang
type SchemaRegistry interface {
    GetSchemaID(schemaName string) (int16, error)
}

type ValueType string

const (
	ValueTypeText    ValueType = "text"
	ValueTypeNumeric ValueType = "numeric"
	ValueTypeDate    ValueType = "date"
	ValueTypeBool    ValueType = "bool"
)


struct AttributeMeta {
    SchemaID    int16
    Path        string // 属性唯一标识符，是属性在实体的JSON序列化结构中的路径
    EavAttrID   int16  // 属性在EAV 表中的attr_id
    EntityColumnName string // 属性在Entity Main表中的字段名
}

type AttributeRegistry interface {
    GetAttributeMetadata(schemaID int16, path string) *AttributeMeta, error
    GetSchemaAttributeCache(schemaID int16) map[string]*AttributeMeta, error
}

```

每个项目有2个独立的DSQL Tables: ``entity main`` 和 `EAV Data`。

为了减少字段数量，我们把时间和日期类型都存储为数字类型（Unix时间戳，毫秒级别的整数）。布尔类型存储为文本类型，`1`表示`true`，`0`表示`false`。
### 时间日期处理

在`entity main`表中，日期类型的属性存储为Unix时间戳（毫秒秒级别的整数）。例如，`created_at`属性存储为一个整数列`num_01`，表示自1970年1月1日以来的毫秒数。

### bool处理

在`entity main`表中，布尔类型的属性存储为smallint列，`1`表示`true`，`0`表示`false`。例如，`is_active`属性存储为一个整数列`smallint_01`。bool类型不应该存储在索引列。


## Primary Table

`entity main`表用于存储高频访问的属性，以提升查询性能。表名格式为`hot_attributes_<base32(client_id)>_<project_id>`。

| Name              | Type             | Note         |
| :---------------- | :--------------- | :----------- |
| ltbase_schema_id  | int2             | From 100     |
| ltbase_row_id     | uuid             | UUID v7      |
| text_01           | text             | 文本列1      |
| text_02           | text             | 文本列2      |
| text_03           | text             | 文本列3      |
| text_04           | text             | 文本列4      |
| text_05           | text             | 文本列5      |
| text_06           | text             | 文本列6      |
| text_07           | text             | 文本列7      |
| text_08           | text             | 文本列8      |
| text_09           | text             | 文本列9      |
| text_10           | text             | 文本列10     |
| smallint_01       | smallint         | smallint列1  |
| smallint_02       | smallint         | smallint列2  |
| integer_01        | integer          | integer列1   |
| integer_02        | integer          | integer列2   |
| integer_03        | integer          | integer列3   |
| bigint_01         | bigint           | bigint列1    |
| bigint_02         | bigint           | bigint列2    |
| bigint_03         | bigint           | bigint列3    |
| bigint_04         | bigint           | bigint列4    |
| bigint_05         | bigint           | bigint列5    |
| double_01         | double precision | 数字列1      |
| double_02         | double precision | 数字列2      |
| double_03         | double precision | 数字列3      |
| double_04         | double precision | 数字列4      |
| double_05         | double precision | 数字列5      |
| uuid_01           | uuid             | uuid列1      |
| uuid_02           | uuid             | uuid列1      |
| ltbase_created_at | bigint           | 记录创建时间 |
| ltbase_updated_at | bigint           | 记录更新时间 |
| ltbase_deleted_at | bigint           | 记录删除时间 |

字段名示例：`text_01`表示第一个文本类型的属性列，`smallint_01`表示第一个smallint类型的属性列，依此类推。
其中text_01~text_05, smallint_01,integer_01,bigint_01, bigint_02, double_01, double_02, uuid_01 为索引字段。

```sql
CRETAE INDEX ASYNC idx_entity_main_<base32_client_id>_<project_id>_text_01
    ON entity_main_<base32_client_id>_<project_id> (ltbase_schema_id, ltbase_row_id, text_01);
CRETAE INDEX ASYNC idx_entity_main_<base32_client_id>_<project_id>_text_02
    ON entity_main_<base32_client_id>_<project_id> (ltbase_schema_id, ltbase_row_id, text_02);
CRETAE INDEX ASYNC idx_entity_main_<base32_client_id>_<project_id>_text_03
    ON entity_main_<base32_client_id>_<project_id> (ltbase_schema_id, ltbase_row_id, text_03);
CRETAE INDEX ASYNC idx_entity_main_<base32_client_id>_<project_id>_smallint_01
    ON entity_main_<base32_client_id>_<project_id> (ltbase_schema_id, ltbase_row_id, smallint_01);
CRETAE INDEX ASYNC idx_entity_main_<base32_client_id>_<project_id>_integer_01
    ON entity_main_<base32_client_id>_<project_id> (ltbase_schema_id, ltbase_row_id, integer_01);
CRETAE INDEX ASYNC idx_entity_main_<base32_client_id>_<project_id>_bigint_01
    ON entity_main_<base32_client_id>_<project_id> (ltbase_schema_id, ltbase_row_id, bigint_01);
CRETAE INDEX ASYNC idx_entity_main_<base32_client_id>_<project_id>_bigint_02
    ON entity_main_<base32_client_id>_<project_id> (ltbase_schema_id, ltbase_row_id, bigint_02);
CRETAE INDEX ASYNC idx_entity_main_<base32_client_id>_<project_id>_double_01
    ON entity_main_<base32_client_id>_<project_id> (ltbase_schema_id, ltbase_row_id, double_01);
CRETAE INDEX ASYNC idx_entity_main_<base32_client_id>_<project_id>_double_02
    ON entity_main_<base32_client_id>_<project_id> (ltbase_schema_id, ltbase_row_id, double_02);
CRETAE INDEX ASYNC idx_entity_main_<base32_client_id>_<project_id>_uuid_01
    ON entity_main_<base32_client_id>_<project_id> (ltbase_schema_id, ltbase_row_id, uuid_01);
```

## EAV Data Table

优化后的EAV数据表使用类型化的值列。表名格式为`eav_<base32(client_id)>_<project_id>`。

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

create index eav_data_<base32_client_id>_<project_id>_numeric_idx
    on eav_data (schema_id, attr_id, value_numeric, row_id)
    where (value_numeric IS NOT NULL);

create index eav_data_<base32_client_id>_<project_id>_text_idx
    on eav_data (schema_id, attr_id, value_text, row_id)
    where (value_text IS NOT NULL);
```

| Name          | Type             | Note               |
| :------------ | :--------------- | :----------------- |
| schema_id     | smallint         | Starts From 100    |
| row_id        | uuid             | UUID v7            |
| attr_id       | smallint         | 引用 attributes 表 |
| value_text    | text             | 文本类型值         |
| value_numeric | double precision | 数字类型值         |
