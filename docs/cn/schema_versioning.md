# Schema 演进的支持

## 允许的Schema变更操作类型 （backwards compatible）

当用户进行允许的Schema变更时，LTBase会支持对已有数据的平滑迁移，确保数据的一致性和可用性。允许的Schema变更类型包括但不限于：

1.  **添加新的可选字段 (Add Optional Field)**
    *   在 `properties` 中增加新的字段定义。
    *   新字段**不能**出现在 `required` 列表中。
    *   读取旧数据时，该字段将不存在（或为 `null`/默认值）。

2.  **将必填字段改为可选 (Make Field Optional)**
    *   从 `required` 列表中移除某个字段的名称。
    *   这放宽了对数据的要求，旧数据（包含该字段）仍然有效。

3.  **放宽验证约束 (Relax Validation Constraints)**
    *   **增加** `maxLength`（字符串最大长度）。
    *   **减小** `minLength`（字符串最小长度）。
    *   **增加** `maximum` / `exclusiveMaximum`（数值最大值）。
    *   **减小** `minimum` / `exclusiveMinimum`（数值最小值）。
    *   向 `enum`（枚举）列表中**添加**新的选项。
    *   移除或放宽 `pattern`（正则表达式）约束。

4.  **添加元数据 (Add Metadata)**
    *   添加 `description`, `title`, `examples` 等描述性字段。

## 不允许的Schema变更操作类型 （NOT backwards compatible）

如果用户尝试进行不允许的Schema变更，LTBase会拒绝该变更请求，并返回相应的错误信息。不允许的Schema变更类型包括但不限于：

1.  **添加新的必填字段 (Add Required Field)**
    *   在 `properties` 中增加新字段，并将其加入 `required` 列表。
    *   **原因**：历史数据中不存在该字段，会导致反序列化或校验失败。

2.  **删除字段 (Remove Field)**
    *   从 `properties` 中彻底删除某个字段的定义。
    *   **原因**：虽然 JSON Schema 允许这样做（取决于 `additionalProperties`），但这会导致无法读取或写入该字段的历史数据，造成数据“丢失”或应用逻辑错误。建议使用 `deprecated` 标记代替删除。

3.  **重命名字段 (Rename Field)**
    *   修改 `properties` 中的字段名称。
    *   **原因**：系统将其视为“删除旧字段”和“添加新字段”的组合，导致旧字段的数据无法通过新名称访问。

4.  **修改字段类型 (Change Field Type)**
    *   修改字段的 `type` 属性（例如从 `"string"` 改为 `"integer"`，或 `"array"` 改为 `"object"`）。
    *   **原因**：历史数据的存储格式与新定义不兼容，导致解析错误。

5.  **收紧验证约束 (Tighten Validation Constraints)**
    *   **减小** `maxLength`。
    *   **增加** `minLength`。
    *   **减小** `maximum` / `exclusiveMaximum`。
    *   **增加** `minimum` / `exclusiveMinimum`。
    *   从 `enum` 列表中**删除**选项。
    *   添加或修改 `pattern` 使得原本合法的数据变为非法。
    *   **原因**：合法的历史数据在更新或重新校验时会失败，导致数据无法写入。

6.  **修改系统扩展属性 (Change System Extensions)**
    *   修改 `x-relation`（关联关系）的目标或类型。
    *   修改 `x-storage` 存储提示。
    *   **原因**：这些属性影响底层存储结构或数据完整性约束，修改可能导致引用失效或查询错误。