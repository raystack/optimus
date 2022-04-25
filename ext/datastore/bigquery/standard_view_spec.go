package bigquery

import (
	"errors"
	"fmt"

	"github.com/odpf/optimus/models"
)

const (
	ViewQueryFile = "view.sql"
)

type standardViewSpec struct{}

func (standardViewSpec) Adapter() models.DatastoreSpecAdapter {
	return &tableSpecHandler{}
}

func (standardViewSpec) Validator() models.DatastoreSpecValidator {
	return func(spec models.ResourceSpec) error {
		if !tableNameParseRegex.MatchString(spec.Name) {
			return fmt.Errorf("for example 'project_name.dataset_name.table_name'")
		}
		parsedNames := tableNameParseRegex.FindStringSubmatch(spec.Name)
		if len(parsedNames) < 3 || parsedNames[1] == "" || parsedNames[2] == "" || parsedNames[3] == "" {
			return fmt.Errorf("for example 'project_name.dataset_name.table_name'")
		}
		return nil
	}
}

func (standardViewSpec) GenerateURN(tableConfig interface{}) (string, error) {
	bqTable, ok := tableConfig.(BQTable)
	if !ok {
		return "", errors.New("failed to read standard view spec for bigquery")
	}
	return fmt.Sprintf(tableURNFormat, BigQuery{}.Name(), bqTable.Project, bqTable.Dataset, bqTable.Table), nil
}

func (standardViewSpec) DefaultAssets() map[string]string {
	return map[string]string{
		ViewQueryFile: `-- view query goes here`,
	}
}
