package forma

// JSONSchema represents a schema definition.
type JSONSchema struct {
	ID         int16                      `json:"id"`
	Name       string                     `json:"name"`
	Version    int                        `json:"version"`
	Schema     string                     `json:"schema"`
	Properties map[string]*PropertySchema `json:"properties"`
	Required   []string                   `json:"required"`
	CreatedAt  int64                      `json:"created_at"`
}

// PropertySchema defines the schema for a single property.
type PropertySchema struct {
	Name       string                     `json:"name"`
	Type       string                     `json:"type"` // "string", "integer", "number", "boolean", "array", "object", "null"
	Format     string                     `json:"format,omitempty"`
	Items      *PropertySchema            `json:"items,omitempty"`
	Properties map[string]*PropertySchema `json:"properties,omitempty"`
	Required   bool                       `json:"required"`
	Default    any                        `json:"default,omitempty"`
	Enum       []any                      `json:"enum,omitempty"`
	Minimum    *float64                   `json:"minimum,omitempty"`
	Maximum    *float64                   `json:"maximum,omitempty"`
	MinLength  *int                       `json:"minLength,omitempty"`
	MaxLength  *int                       `json:"maxLength,omitempty"`
	Pattern    string                     `json:"pattern,omitempty"`
	Relation   *RelationSchema            `json:"x-relation,omitempty"`
}

// RelationSchema defines reference relationships between objects.
type RelationSchema struct {
	Target      string `json:"target"`       // Target schema name
	Type        string `json:"type"`         // "reference" for foreign key relationships
	KeyProperty string `json:"key_property"` // child-side foreign key attribute
}
