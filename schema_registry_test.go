package forma

import "testing"

func TestMainColumnBindingColumnType(t *testing.T) {
	tests := []struct {
		name     string
		column   MainColumn
		expected MainColumnType
	}{
		{"text prefix", MainColumnText01, MainColumnTypeText},
		{"smallint prefix", MainColumnSmallint02, MainColumnTypeSmallint},
		{"integer prefix", MainColumnInteger03, MainColumnTypeInteger},
		{"bigint prefix", MainColumnBigint04, MainColumnTypeBigint},
		{"double prefix", MainColumnDouble02, MainColumnTypeDouble},
		{"uuid prefix", MainColumnUUID01, MainColumnTypeUUID},
		{"created at special", MainColumnCreatedAt, MainColumnTypeBigint},
		{"updated at special", MainColumnUpdatedAt, MainColumnTypeBigint},
		{"deleted at special", MainColumnDeletedAt, MainColumnTypeBigint},
		{"schema id special", MainColumnSchemaID, MainColumnTypeSmallint},
		{"row id special", MainColumnRowID, MainColumnTypeUUID},
		{"case-insensitive text", MainColumn("TEXT_CUSTOM"), MainColumnTypeText},
		{"default fallback", MainColumn("custom_col"), MainColumnTypeText},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			binding := MainColumnBinding{ColumnName: tt.column}
			if got := binding.ColumnType(); got != tt.expected {
				t.Fatalf("ColumnType() = %s, want %s", got, tt.expected)
			}
		})
	}
}

func TestAttributeMetadataIsInsideArray(t *testing.T) {
	tests := []struct {
		name string
		attr AttributeMetadata
		want bool
	}{
		{"in array", AttributeMetadata{AttributeName: "items[].id"}, true},
		{"not in array", AttributeMetadata{AttributeName: "user.id"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.attr.IsInsideArray(); got != tt.want {
				t.Fatalf("IsInsideArray() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAttributeMetadataLocation(t *testing.T) {
	mainBinding := &MainColumnBinding{ColumnName: MainColumnText01}

	tests := []struct {
		name string
		meta AttributeMetadata
		want AttributeStorageLocation
	}{
		{"main storage", AttributeMetadata{ColumnBinding: mainBinding}, AttributeStorageLocationMain},
		{"eav storage", AttributeMetadata{ColumnBinding: nil}, AttributeStorageLocationEAV},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.meta.Location(); got != tt.want {
				t.Fatalf("Location() = %s, want %s", got, tt.want)
			}
		})
	}
}
