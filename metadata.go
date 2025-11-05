package forma

type ValueType string

const (
	ValueTypeText    ValueType = "text"
	ValueTypeNumeric ValueType = "numeric"
	ValueTypeDate    ValueType = "date"
	ValueTypeBool    ValueType = "bool"
)

// AttributeMeta 存储从 'attributes' 表中缓存的元数据
type AttributeMeta struct {
	AttributeID int16     // attr_id
	ValueType   ValueType // 'text', 'numeric', 'date', 'bool'
	InsideArray bool      // 如果属性存储在数组中，则为 true; 对于存储在数组中的属性，数组的索引会在`indices`字段中指定
}

// SchemaAttributeCache 是 (attr_name -> meta) 的映射
// 强烈建议在应用程序启动时为每个 schema_id 填充此缓存。
type SchemaAttributeCache map[string]AttributeMeta
