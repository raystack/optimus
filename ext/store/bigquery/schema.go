package bigquery

import (
	"strings"

	"github.com/odpf/optimus/internal/errors"
)

const (
	ModeNullable = "nullable"
	ModeRequired = "required"
	ModeRepeated = "repeated"

	EntityResourceSchema = "bigquery_schema"
)

const (
	KindDataset       string = "dataset"
	KindTable         string = "table"
	KindView          string = "view"
	KindExternalTable string = "external_table"
)

type Schema []Field

func (s Schema) Validate() error {
	for _, f := range s {
		err := f.Validate()
		if err != nil {
			return err
		}
	}
	return nil
}

type Field struct {
	Name        string `mapstructure:"name,omitempty"`
	Type        string `mapstructure:"type,omitempty"`
	Description string `mapstructure:"description,omitempty"`
	Mode        string `mapstructure:"mode,omitempty"`

	// optional sub-schema, when record type
	Schema Schema `mapstructure:"schema,omitempty"`
}

func (f Field) Validate() error { // nolint:gocritic
	if strings.TrimSpace(f.Name) == "" {
		return errors.InvalidArgument(EntityResourceSchema, "field name is empty")
	}
	if strings.TrimSpace(f.Type) == "" {
		return errors.InvalidArgument(EntityResourceSchema, "field type is empty for "+f.Name)
	}
	modeLower := strings.ToLower(f.Mode)
	if modeLower != "" && modeLower != ModeNullable && modeLower != ModeRepeated && modeLower != ModeRequired {
		return errors.InvalidArgument(EntityResourceSchema, "unknown field mode "+modeLower+" for "+f.Name)
	}
	if f.Schema != nil {
		err := f.Schema.Validate()
		if err != nil {
			return err
		}
	}
	return nil
}
