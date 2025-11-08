# 查询数据功能

LTBase 支持基于JSON Schema自动生成数据CRUD API功能。基于 (Model)[./model.md] 中定义的数据模型，系统支持CRUD查询操作。



## Control Plane 功能

### 定义Schema

JSON Schema定义了实体的属性和数据类型。用户可以通过Control Plane API创建和管理Schema。LTBase扩展了JSON Schema，允许用户指定：

1. 哪些属性需要存储在`entity main`表中以优化查询性能。
2. 哪些属性需要索引以支持高效的过滤查询。

### 字段类型fallback机制

由于`entity main`表的字段数量有限，而系统查询和/或排序存储在EAV表中的属性时会有严重的性能损失。因此用户在定义Schema时可以指定某些属性存储在`entity main`表中。用户可以选择把某些类型的字段存储在另外类型的字段中，以增加存储的灵活性和字段利用率。系统支持如下规则进行类型fallback：


#### 数字字段
数字类型的属性优先存储在匹配精度的列中。如果匹配的精度的列已经用尽，则向上cast到更高精度的列中存储。例如：smallint -> integer -> bigint -> double。但是由于IEEE浮点数的精度问题，使用double类型的列存储整数时，查询的SQL需要做适当调整：
* 对于等值查询，假设要查询的整数值为`t`，则需要转换为区间`(t-0.1, t+0.1)`进行比较。
* 对于范围查询，
  * `> t`，则转换为`> t + 0.1`
  * `>= t`，则转换为`>= t - 0.1`
  * `< t`，则转换为`< t - 0.1`
  * `<= t`，则转换为`<= t + 0.1` 

#### 日期字段

日期类型的属性优先存储在bigint列中，表示Unix时间戳（毫秒级别的整数）。如果bigint列已经用尽，则按如下顺序fallback：

* double，由于IEEE浮点数误差问题。使用double表示Unix时间戳时，我们使用精确到毫秒级别的整数，小数部分可以忽略，查询时需要按照 [数字字段](./#数字字段) 中定义的规则调整SQL。
* text, ISO格式的日期字符串，精确到毫秒级别，UTC时区，例如`2024-06-01T12:34:56.789Z`

#### UUID字段

UUID类型的属性优先存储在uuid列中。如果uuid列已经用尽，则按如下顺序fallback：
* text, UUID字符串格式，例如`550e8400-e29b-41d4-a716-446655440000`

#### 布尔字段

布尔类型的属性优先存储在smallint列中，`1`表示`true`，`0`表示`false`。如果smallint列已用尽，则按如下顺序fallback：
* integer
* bigint
* double,`> 0.9`表示`true`，`< 0.1`表示`false`
* text

### 元数据映射

LTBase自己也维护了一些元数据：
* `RowID`，每条记录的唯一标识符，UUIDv7格式。
* `CreatedAt`，记录创建时间，Unix时间戳，毫秒级别的整数（bigint）
* `UpdatedAt`，记录最后更新时间，Unix时间戳，毫秒级别的整数（bigint）
* `DeletedAt`，记录删除时间，Unix时间戳，毫秒级别的整数（bigint），未删除记录该字段为null

用户不能直接更新这些元数据字段，但是如果用户的Schema中可以利用这些元数据字段而不需要额外占用字段。例如，用户可以在Schema中使用这些`ltbase_`前缀的属性：
* `ltbase_row_id`
* `ltbase_created_at`
* `ltbase_updated_at`
* `ltbase_deleted_at`

### 为Schema 的 Metadata 

为了记录Schema中的字段与数据库中字段的映射关系，LTBase自动根据用户对Schema的配置生成Schema Metadata信息。Schema Metadata结构如下：

```json
{
  "schema_name": "<schema_name>",
  "schema_id": <schema_id>,
  "schema_version": <schema_version>,
  "attributes": [
    {
      "id": <attribute_id>,
      "name": "<attribute_json_path>",
      "type": "<attribute_type>",
      "storage": {
        "table": "<entity_main or eav_data>",
        "column": "<column_name of entity_main table, null if eav_data>",
      }
    }
  ]
}
```

## Data Plane 功能

Data Plane提供基于Schema的CRUD查询API。

## 字段映射

一个entity中的属性根据Schema的定义会被映射到数据库中的`entity main`表或者`eav data`表中。LTBase会根据Schema的Metadata信息来决定每个属性的存储位置。LTBase会根据Schema Metadata来生成相应的SQL语句以实现CRUD操作。


### 从JSON对象到PersistentRecord的转换




### 从PersistentRecord到JSON对象的转换


## 创建Schema的记录

### 创建流程 (CreateRecord)

1. 根据用户定义的JSON Schema验证输入的JSON对象。
2. 调整输入对象，添加元数据字段：
    * 生成一个新的`UUIDv7`作为`ltbase_row_id`
    * 设置`ltbase_created_at`和`ltbase_updated_at`为当前时间的Unix时间戳（毫秒级别的整数）
    * 设置`ltbase_deleted_at`为null
3. 根据 schema 的 metadatata来执行下面的两个步骤：
    * 使用 `PersistentRecordTransformer` 根据输入的 JSON object 变换为一组 PersistentRecord
    * 生成SQL
4. 执行SQL插入数据到数据库中的`entity main`和`eav data`表中。

### 输入参数
- `record` (DataRecord, 必填): 包含要插入的属性值的对象。record中必须包含`schema_id`字段，但是`row_id`字段将会被覆盖为一个新的UUID。

### 创建单条记录

### 批量创建多条记录

### 输出结果

## 查询指定RowID对应的entity记录

这个查询不需要分页，因为最终只会返回最多一个`DataRecord`

### 输入参数
- `row_id` (string, 必填): 记录的RowID，UUID格式

### 输出结果
- `DataRecord` 对象，包含该RowID的所有属性值

## 删除指定RowID的所有属性


## 查询指定Schema的记录 （分页）




## 更新指定RowID的属性值

