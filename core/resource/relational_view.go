package resource

import "github.com/odpf/optimus/internal/errors"

const (
	EntityView = "resource_view"
)

type View struct {
	Name    Name
	Dataset Dataset

	Description string `mapstructure:"description,omitempty"`
	ViewQuery   string `mapstructure:"view_query,omitempty"`

	ExtraConfig map[string]interface{} `mapstructure:",remain"`
}

func (v *View) FullName() string {
	return v.Dataset.FullName() + "." + v.Name.String()
}

func (v *View) URN() string {
	return v.Dataset.URN() + "." + v.Name.String()
}

func (v *View) Validate() error {
	if v.ViewQuery == "" {
		return errors.InvalidArgument(EntityView, "view query is empty for "+v.FullName())
	}
	return nil
}
