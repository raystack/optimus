package resource

type Kind string

const (
	KindDataset       Kind = "dataset"
	KindTable         Kind = "table"
	KindView          Kind = "view"
	KindExternalTable Kind = "external_table"
)

type Metadata struct {
	Version     int
	Description string
	Labels      map[string]string
}

type Schema []Field

type Field struct {
	Name        string
	Type        string
	Description string
	Mode        string

	// optional sub-schema, when record type
	Schema Schema
}
