package bigquery

import (
	"fmt"

	"github.com/odpf/optimus/models"
)

const (
	ViewQueryFile = "view.sql"
)

type standardViewSpec struct{}

func (s standardViewSpec) Adapter() models.DatastoreSpecAdapter {
	return &tableSpecHandler{}
}

func (s standardViewSpec) Validator() models.DatastoreSpecValidator {
	return func(spec models.ResourceSpec) error {
		if !tableNameParseRegex.MatchString(spec.Name) {
			return fmt.Errorf("for example 'project_name.dataset_name.table_name'")
		}
		parsedNames := tableNameParseRegex.FindStringSubmatch(spec.Name)
		if len(parsedNames) < 3 || len(parsedNames[1]) == 0 || len(parsedNames[2]) == 0 || len(parsedNames[3]) == 0 {
			return fmt.Errorf("for example 'project_name.dataset_name.table_name'")
		}
		return nil
	}
}

func (s standardViewSpec) DefaultAssets() map[string]string {
	return map[string]string{
		ViewQueryFile: `-- view query goes here`,
	}
}
