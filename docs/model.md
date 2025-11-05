# Data Model

用户数据存储在[AWS DSQL](https://docs.aws.amazon.com/aurora-dsql/latest/userguide/working-with-postgresql-compatibility-supported-data-types.html)中。AWS DSQL是一个Serverless的数据库，兼容`PostgreSQL 16`。
每个项目有两个独立的DSQL Tables: Schema Registry 和 EAV Data。

## Schema Registry Table

Schema注册表用于将`schema_name`映射到`schema_id`。表名格式为`schema_registry_<base32(client_id)>_<project_id>`。

| Name        | Type | Note                                                                                                                                   |
| :---------- | :--- | :------------------------------------------------------------------------------------------------------------------------------------- |
| schema_name | Text | 有可能单数和复数形式各占一条记录，但是映射到同一个`schema_id`, <br> 例如event和events 都映射到100。但是目前的API并不支持自动识别单复数 |
| schema_id   | int2 | From 100                                                                                                                               |

* **主键 (PK)**: `schema_name` 

---

## EAV Data Table 设计演进

### 当前设计存在的问题

当前的EAV实现使用单一的`attr_value TEXT`列存储所有类型的数据，这会导致严重的性能和数据一致性问题：

#### 1. 数据类型丢失
- **问题**: 无法在数据库层面保证数据类型的正确性
- **影响**: 可能存储 "一百块" 或 "banana" 这样的无效数据在数字字段中
- **后果**: 应用层需要额外的验证逻辑，容易出错

#### 2. 查询性能极差
- **范围查询失效**: `WHERE attr_value > '100'` 使用文本比较，`'99'` 会大于 `'100'`
- **CAST性能灾难**: `CAST(attr_value AS numeric) > 100` 无法使用索引，导致全表扫描
- **无法进行高效的日期/时间范围查询**

#### 3. 索引效率低下
- `idx_attr_value` 索引几乎无用，因为：
  - 文本索引比数字/日期索引慢
  - 索引选择性差（不同属性共享相同值，如 'true', 'US'）
  - 实际查询很少单独使用 `attr_value`，通常需要配合 `schema_id` 和 `attr_name`
  - 只拖慢写入性能（INSERT/UPDATE）

---

## 优化后的 EAV 设计（推荐实现）

### Attributes Table（属性注册表）

用于规范化属性名称并定义属性的数据类型。表名格式为`attributes_<base32(client_id)>_<project_id>`。

```sql
CREATE TABLE public.attributes
(
    attr_id     smallint    NOT NULL,
    schema_id   smallint    NOT NULL REFERENCES public.schema_registry (schema_id),
    attr_name   text        NOT NULL,
    value_type  text        NOT NULL CHECK (value_type IN ('text', 'numeric', 'date', 'bool', 'jsonb')),
    PRIMARY KEY (attr_id, schema_id),
    UNIQUE (schema_id, attr_name)
);
```

| Name       | Type     | Note                                               |
| :--------- | :------- | :------------------------------------------------- |
| attr_id    | smallint | 属性唯一标识符                                     |
| schema_id  | smallint | 关联的schema                                       |
| attr_name  | text     | 属性名称（如 'price', 'email', 'created_at'）      |
| value_type | text     | 值类型：'text', 'numeric', 'date', 'bool', 'jsonb' |

**优势**:
- 避免 `attr_name` 在 `eav_data` 表中重复存储千百万次
- 节省大量存储空间
- 提供类型元数据，应用层可查询此表决定如何处理值

---

### EAV Data Table（多值列方案）

优化后的EAV数据表使用类型化的值列。表名格式为`eav_<base32(client_id)>_<project_id>`。

```sql
CREATE TABLE public.eav_data
(
    schema_id     smallint    NOT NULL,
    row_id        uuid        NOT NULL,
    attr_id       smallint    NOT NULL,
    value_text    text,
    value_numeric numeric,
    value_date    timestamptz,
    value_bool    boolean,
    value_jsonb   jsonb,
    PRIMARY KEY (schema_id, row_id, attr_id),
    FOREIGN KEY (attr_id, schema_id) REFERENCES public.attributes (attr_id, schema_id)
);
```

| Name          | Type        | Note                     |
| :------------ | :---------- | :----------------------- |
| schema_id     | smallint    | Starts From 100          |
| row_id        | uuid        | UUID v7                  |
| attr_id       | smallint    | 引用 attributes 表       |
| value_text    | text        | 文本类型值               |
| value_numeric | numeric     | 数字类型值               |
| value_date    | timestamptz | 日期时间类型值           |
| value_bool    | boolean     | 布尔类型值               |
| value_jsonb   | jsonb       | 复杂类型（数组、对象等） |

**设计原则**:
- 每行只有一个 `value_*` 列有值，其他列为 `NULL`
- 根据 `attributes.value_type` 决定使用哪个值列
- 每个类型的值使用正确的数据库类型存储

---

### 主键设计优化

**当前设计**: `(row_id, attr_name, schema_id)`
**优化设计**: `(schema_id, row_id, attr_id)`

**优化原因**:
1. **逻辑更清晰**: 按 schema → entity → attribute 的层次组织
2. **物理聚集**: 同类型实体的数据物理上相邻，提升缓存命中率
3. **支持分区**: 未来可按 `schema_id` 进行表分区
4. **范围扫描优化**: 更高效地扫描某个 schema 下的所有数据

---

### 索引策略

#### 核心索引

```sql
-- 主键索引（自动创建）
-- PRIMARY KEY (schema_id, row_id, attr_id)
-- 用途: 查找特定实体的特定属性
```

#### 类型化值索引（通用）

```sql
-- 数字类型值索引
CREATE INDEX eav_data_numeric_values_idx
    ON public.eav_data (schema_id, attr_id, value_numeric)
    INCLUDE (row_id);

-- 文本类型值索引
CREATE INDEX eav_data_text_values_idx
    ON public.eav_data (schema_id, attr_id, value_text)
    INCLUDE (row_id);

-- 日期类型值索引
CREATE INDEX eav_data_date_values_idx
    ON public.eav_data (schema_id, attr_id, value_date)
    INCLUDE (row_id);

-- 布尔类型值索引
CREATE INDEX eav_data_bool_values_idx
    ON public.eav_data (schema_id, attr_id, value_bool)
    INCLUDE (row_id);


**用途**: 支持 "在某个schema下，查找某个属性符合条件的所有实体"
- 示例查询: `WHERE schema_id = 1 AND attr_id = 10 AND value_numeric > 100`

#### 部分索引（高频查询优化）

```sql
-- 示例: 为 "price" 属性（假设 attr_id = 10）创建专用索引
CREATE INDEX eav_data_price_idx
    ON public.eav_data (schema_id, value_numeric)
    WHERE attr_id = 10;

-- 示例: 为 "email" 属性（假设 attr_id = 5）创建专用索引
CREATE INDEX eav_data_email_idx
    ON public.eav_data (schema_id, value_text)
    WHERE attr_id = 5;
```

**优势**:
- 索引更小，查询更快
- 针对特定高频查询属性定制
- 根据实际查询模式创建

#### 文本前缀查询优化

```sql
-- 对于需要前缀匹配的文本属性（如 LIKE 'prefix%'）
CREATE INDEX eav_data_text_pattern_idx
    ON public.eav_data (value_text text_pattern_ops)
    WHERE attr_id IN (5, 7, 12); -- 指定需要前缀查询的属性
```

---

## 性能对比

| 操作类型   | 当前设计（TEXT列） | 优化设计（类型化列） | 性能提升        |
| :--------- | :----------------- | :------------------- | :-------------- |
| 范围查询   | 全表扫描 + CAST    | 索引范围扫描         | **100-1000x**   |
| 精确匹配   | 低效文本索引       | 高效类型化索引       | **10-50x**      |
| 排序       | CAST + 内存排序    | 索引有序扫描         | **50-200x**     |
| 存储空间   | attr_name 重复     | 使用 attr_id         | **节省 30-50%** |
| 数据完整性 | 应用层验证         | 数据库类型约束       | **更可靠**      |

---


## 查询示例

### 查找特定属性值的实体

```sql
-- 查找价格大于100的所有产品
SELECT row_id
FROM eav_data
WHERE schema_id = 100 
  AND attr_id = 10  -- price
  AND value_numeric > 100;
```

### 组合条件查询

```sql
-- 查找价格在100-500之间且状态为active的产品
SELECT e1.row_id
FROM eav_data e1
JOIN eav_data e2 ON e1.row_id = e2.row_id AND e1.schema_id = e2.schema_id
WHERE e1.schema_id = 100
  AND e1.attr_id = 10  -- price
  AND e1.value_numeric BETWEEN 100 AND 500
  AND e2.attr_id = 15  -- status
  AND e2.value_text = 'active';
```

### 获取实体的所有属性

```sql
-- 获取特定产品的所有属性
SELECT 
    a.attr_name,
    a.value_type,
    e.value_text,
    e.value_numeric,
    e.value_date,
    e.value_bool,
    e.value_jsonb
FROM eav_data e
JOIN attributes a ON e.attr_id = a.attr_id AND e.schema_id = a.schema_id
WHERE e.schema_id = 100
  AND e.row_id = '01234567-89ab-cdef-0123-456789abcdef';
```

---

## 最佳实践

### 1. 索引创建原则
- 只为高频查询的属性创建部分索引
- 监控查询模式，动态调整索引
- 避免过度索引（影响写入性能）

### 2. 数据写入
- 写入时根据 `attributes.value_type` 选择正确的值列
- 确保只有一个 `value_*` 列非空
- 使用事务保证数据一致性

### 3. 查询优化
- 优先使用 `attr_id` 而非 `attr_name`（JOIN `attributes` 表获取名称）
- 利用覆盖索引（INCLUDE列）避免回表
- 对于复杂查询，考虑使用物化视图

### 4. 维护建议
- 定期 VACUUM 和 ANALYZE
- 监控索引使用情况（`pg_stat_user_indexes`）
- 删除未使用的索引

---

## 总结

优化后的EAV设计解决了原始设计的核心问题：

✅ **数据类型安全**: 数据库层面的类型约束  
✅ **查询性能**: 高效的类型化索引，支持范围查询  
✅ **存储优化**: 规范化属性名，节省30-50%空间  
✅ **可维护性**: 清晰的数据模型，更容易理解和优化  

这是关系型数据库中处理EAV模型的**专业标准方案**，在性能、数据完整性和可维护性之间达到最佳平衡。
