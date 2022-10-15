package resource

import "github.com/odpf/optimus/internal/errors"

const (
	EntityExternalTable = "resource_external_table"
)

type ExternalTable struct {
	Name    Name
	Dataset Dataset

	Description string         `mapstructure:"description,omitempty"`
	Schema      Schema         `mapstructure:"schema,omitempty"`
	Source      ExternalSource `mapstructure:"source,omitempty"`

	ExtraConfig map[string]interface{} `mapstructure:",remain"`
}

func (e *ExternalTable) FullName() string {
	return e.Dataset.FullName() + "." + e.Name.String()
}

func (e *ExternalTable) URN() string {
	return e.Dataset.URN() + "." + e.Name.String()
}

func (e *ExternalTable) Validate() error {
	if len(e.Schema) == 0 {
		return errors.InvalidArgument(EntityExternalTable, "invalid schema for "+e.FullName())
	}

	err := e.Schema.Validate()
	if err != nil {
		return errors.AddErrContext(err, EntityExternalTable, "error in schema for "+e.FullName())
	}

	err = e.Source.Validate()
	if err != nil {
		return errors.AddErrContext(err, EntityExternalTable, "error in source for "+e.FullName())
	}
	return nil
}

type ExternalSource struct {
	SourceType string   `mapstructure:"type,omitempty"`
	SourceURIs []string `mapstructure:"uris,omitempty"`

	// Additional configs for CSV, GoogleSheets, Bigtable, and Parquet formats.
	Config map[string]interface{} `mapstructure:"config"`
}

func (e ExternalSource) Validate() error {
	if e.SourceType == "" {
		return errors.InvalidArgument(EntityExternalTable, "source type is empty")
	}
	if len(e.SourceURIs) == 0 {
		return errors.InvalidArgument(EntityExternalTable, "source uri list is empty")
	}

	for _, uri := range e.SourceURIs {
		if uri == "" {
			return errors.InvalidArgument(EntityExternalTable, "uri is empty")
		}
	}

	return nil
}
