package resource

import "github.com/odpf/optimus/internal/errors"

const (
	BigQuery Store = "bigquery"
)

// Store represents the type of datasource, resource corresponds to
type Store string

func (s Store) String() string {
	return string(s)
}

func FromStringToStore(name string) (Store, error) {
	switch name {
	case string(BigQuery):
		return BigQuery, nil
	default:
		return "", errors.InvalidArgument(EntityResource, "unknown store "+name)
	}
}

type Dataset struct {
	Store Store

	Database string
	Schema   string
}

func DataSetFrom(store Store, database string, schema string) (Dataset, error) {
	if database == "" {
		return Dataset{}, errors.InvalidArgument(EntityResource, "database/project name is empty")
	}

	if schema == "" {
		return Dataset{}, errors.InvalidArgument(EntityResource, "schema/dataset name is empty")
	}

	return Dataset{
		Store:    store,
		Database: database,
		Schema:   schema,
	}, nil
}

func (d Dataset) FullName() string {
	return d.Database + "." + d.Schema
}

func (d Dataset) IsSame(d2 Dataset) bool {
	return d.URN() == d2.URN()
}

func (d Dataset) URN() string {
	// formats name as bigquery://project:dataset
	return string(d.Store) + "://" + d.Database + ":" + d.Schema
}

type DatasetDetails struct {
	Dataset Dataset

	Description string                 `mapstructure:"description,omitempty"`
	ExtraConfig map[string]interface{} `mapstructure:",remain"`
}

func (d DatasetDetails) FullName() string {
	return d.Dataset.FullName()
}

func (d DatasetDetails) URN() string {
	return d.Dataset.URN()
}

func (DatasetDetails) Validate() error {
	return nil
}
