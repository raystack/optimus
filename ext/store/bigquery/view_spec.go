package bigquery

import (
	"github.com/raystack/optimus/core/resource"
	"github.com/raystack/optimus/internal/errors"
)

const (
	EntityView = "resource_view"
)

type View struct {
	Name resource.Name

	Description string `mapstructure:"description,omitempty"`
	ViewQuery   string `mapstructure:"view_query,omitempty"`

	ExtraConfig map[string]interface{} `mapstructure:",remain"`
}

func (v *View) Validate() error {
	if v.ViewQuery == "" {
		return errors.InvalidArgument(EntityView, "view query is empty for "+v.Name.String())
	}
	return nil
}
