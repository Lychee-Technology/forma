# Schema 演进的支持

## 允许的Schema变更操作类型 （backwards compatible）

当用户进行允许的Schema变更时，Forma会支持对已有数据的平滑迁移，确保数据的一致性和可用性。允许的Schema变更类型包括但不限于：

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

以下变更不向后兼容。注意：目前 Forma **并不会**自动拒绝其中大部分变更，进程启动时直接加载 `SCHEMA_DIR` 中的新定义，不与旧定义比较；避免这些变更是运维方的责任。已实现的检查仅限于：已退役（`retired`）属性的 ID、列绑定与类型复用（#342），以及 `valueType` 与列编码的绑定校验（#459）。不允许的Schema变更类型包括但不限于：

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
    *   修改属性的 `valueType`（例如 `numeric` 改为 `bigint`）同样适用。既定策略是 fail-closed，且是否允许只看新旧类型和绑定的存储，不看已存储的值：收窄（如 `numeric` 改为 `bigint`）和跨类型族的变更一律拒绝，即使现有数据恰好都满足新类型；整数族内拓宽和改为 `numeric` 允许，其中绑定 `bigint_*` 列的 `bigint` 改为 `numeric` 会给出警告，因为超过 ±2^53 的值会被舍入。例外：在 #729 修复之前，未绑定主表列（仅存于 EAV）的 `smallint`/`integer` 属性的任何类型变更一律拒绝，包括拓宽和改为 `numeric`，因为仍在运行的旧进程会把新允许的值（如 `1001.5`）转换后读出，并在无关的更新中写回。有损转换（舍入、截断、置 NULL）必须通过显式迁移完成（#730）。该策略尚未完全实现（#710、#733）。实测在 `numeric` 改为 `bigint` 后，含 `1e30` 的旧 parquet 会使该 schema 上过滤条件保留了该行的联邦查询失败（即使该行不在返回的页内，未过滤的列表查询都会失败），而 `1000.5` 会被静默读成 `1000`；自 #732 起，联邦读取对这两种情况都会明确拒绝。详见 [Schema Evolution and Declared Types](../schema-evolution.md)。

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