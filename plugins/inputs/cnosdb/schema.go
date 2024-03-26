package cnosdb

type TskvTableSchema struct {
	Tenant       string          `json:"tenant"`
	Db           string          `json:"db"`
	Name         string          `json:"name"`
	SchemaID     uint32          `json:"schema_id"`
	NextColumnID uint64          `json:"next_column_id"`
	Columns      []TableColumn   `json:"columns"`
	ColumnsIndex map[string]uint `json:"columns_index"`
}

type TableColumn struct {
	ID         uint64 `json:"id"`
	Name       string `json:"name"`
	ColumnType string `json:"column_type"`
	Encoding   string `json:"encoding"`
}
