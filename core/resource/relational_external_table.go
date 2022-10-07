package resource

type ExternalTable struct {
	Name    Name
	Dataset Dataset

	Description string
	Schema      Schema
	Source      ExternalSource
}

type ExternalSource struct {
	SourceType string
	SourceURIs []string

	Config map[string]any
}
