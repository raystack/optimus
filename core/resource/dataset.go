package resource

import "github.com/odpf/optimus/internal/errors"

const (
	BigQuery Store = "bigquery"
)

// Store represents the type of datasource, resource corresponds to
type Store string

func FromString(name string) (Store, error) {
	switch name {
	case string(BigQuery):
		return BigQuery, nil
	default:
		return "", errors.InvalidArgument(EntityResource, "unknown store"+name)
	}
}

type Dataset struct {
	Store Store

	Database string
	Schema   string
}

func DataSetFrom(store string, database string, schema string) (Dataset, error) {
	dataStore, err := FromString(store)
	if err != nil {
		return Dataset{}, err
	}

	if database == "" {
		return Dataset{}, errors.InvalidArgument(EntityResource, "database name is empty")
	}

	if schema == "" {
		return Dataset{}, errors.InvalidArgument(EntityResource, "schema name is empty")
	}

	return Dataset{
		Store:    dataStore,
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
