package resource

import (
	"strings"

	"github.com/odpf/optimus/internal/errors"
)

const (
	ModeNullable = "nullable"
	ModeRequired = "required"
	ModeRepeated = "repeated"

	EntityResourceSchema = "resource_schema"
)

type Kind string

const (
	KindDataset       Kind = "dataset"
	KindTable         Kind = "table"
	KindView          Kind = "view"
	KindExternalTable Kind = "external_table"
)

func (k Kind) String() string {
	return string(k)
}

func FromStringToKind(name string) (Kind, error) {
	switch name {
	case string(KindDataset):
		return KindDataset, nil
	case string(KindTable):
		return KindTable, nil
	case string(KindView):
		return KindView, nil
	case string(KindExternalTable):
		return KindExternalTable, nil
	default:
		return "", errors.InvalidArgument(EntityResource, "unknown kind "+name)
	}
}

type Status string

const (
	StatusUnknown  Status = "unknown"
	StatusToCreate Status = "to_create"
	StatusToUpdate Status = "to_update"

	StatusSuccess Status = "sync_success"
	StatusFailure Status = "sync_failure"
)

func (s Status) String() string {
	return string(s)
}

type Metadata struct {
	Version     int32
	Description string
	Labels      map[string]string
}

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

func (f Field) Validate() error {
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
