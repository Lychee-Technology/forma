package internal

// MainColumnType enumerates physical column families inside the hot attributes table.
type MainColumnType string

const (
	MainColumnTypeText     MainColumnType = "text"
	MainColumnTypeSmallint MainColumnType = "smallint"
	MainColumnTypeInteger  MainColumnType = "integer"
	MainColumnTypeBigint   MainColumnType = "bigint"
	MainColumnTypeDouble   MainColumnType = "double"
	MainColumnTypeUUID     MainColumnType = "uuid"
)

// MainColumnEncoding captures how logical values are encoded into the physical column.
type MainColumnEncoding string

const (
	MainColumnEncodingDefault  MainColumnEncoding = "default"
	MainColumnEncodingUnixMs   MainColumnEncoding = "unix_ms"
	MainColumnEncodingBoolInt  MainColumnEncoding = "bool_smallint"
	MainColumnEncodingBoolText MainColumnEncoding = "bool_text"
	MainColumnEncodingISO8601  MainColumnEncoding = "iso8601"
)

// MainColumn enumerates available hot columns.
type MainColumn string

const (
	MainColumnText01     MainColumn = "text_01"
	MainColumnText02     MainColumn = "text_02"
	MainColumnText03     MainColumn = "text_03"
	MainColumnText04     MainColumn = "text_04"
	MainColumnText05     MainColumn = "text_05"
	MainColumnText06     MainColumn = "text_06"
	MainColumnText07     MainColumn = "text_07"
	MainColumnText08     MainColumn = "text_08"
	MainColumnText09     MainColumn = "text_09"
	MainColumnText10     MainColumn = "text_10"
	MainColumnSmallint01 MainColumn = "smallint_01"
	MainColumnSmallint02 MainColumn = "smallint_02"
	MainColumnInteger01  MainColumn = "integer_01"
	MainColumnInteger02  MainColumn = "integer_02"
	MainColumnInteger03  MainColumn = "integer_03"
	MainColumnBigint01   MainColumn = "bigint_01"
	MainColumnBigint02   MainColumn = "bigint_02"
	MainColumnBigint03   MainColumn = "bigint_03"
	MainColumnBigint04   MainColumn = "bigint_04"
	MainColumnBigint05   MainColumn = "bigint_05"
	MainColumnDouble01   MainColumn = "double_01"
	MainColumnDouble02   MainColumn = "double_02"
	MainColumnDouble03   MainColumn = "double_03"
	MainColumnDouble04   MainColumn = "double_04"
	MainColumnDouble05   MainColumn = "double_05"
	MainColumnUUID01     MainColumn = "uuid_01"
	MainColumnUUID02     MainColumn = "uuid_02"
)
