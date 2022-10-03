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
	Owner       string
}
