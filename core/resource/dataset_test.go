package resource_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/resource"
)

func TestDataStore(t *testing.T) {
	t.Run("returns error when unknown type", func(t *testing.T) {
		_, err := resource.FromStringToStore("invalid")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "invalid argument for entity resource: unknown store invalid")
	})
	t.Run("converts a string to store when correct", func(t *testing.T) {
		bq, err := resource.FromStringToStore("bigquery")
		assert.Nil(t, err)
		assert.Equal(t, "bigquery", string(bq))
	})
}

func TestDataSet(t *testing.T) {
	t.Run("returns error on empty database/project name", func(t *testing.T) {
		_, err := resource.DataSetFrom(resource.BigQuery, "", "schema")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "invalid argument for entity resource: database/project name is empty")
	})
	t.Run("returns error on empty schema/dataset name", func(t *testing.T) {
		_, err := resource.DataSetFrom(resource.BigQuery, "t-optimus", "")
		assert.NotNil(t, err)
		assert.EqualError(t, err, "invalid argument for entity resource: schema/dataset name is empty")
	})
	t.Run("creates dataset", func(t *testing.T) {
		ds, err := resource.DataSetFrom(resource.BigQuery, "t-optimus", "playground")
		assert.Nil(t, err)

		assert.Equal(t, "t-optimus.playground", ds.FullName())
		assert.Equal(t, "bigquery://t-optimus:playground", ds.URN())
	})
	t.Run("can be compared for equality", func(t *testing.T) {
		ds1, err := resource.DataSetFrom(resource.BigQuery, "t-optimus", "playground")
		assert.Nil(t, err)

		ds2, err := resource.DataSetFrom(resource.BigQuery, "t-optimus", "playground")
		assert.Nil(t, err)

		assert.True(t, ds1.IsSame(ds2))
	})
	t.Run("is not same when values are different", func(t *testing.T) {
		ds1, err := resource.DataSetFrom(resource.BigQuery, "t-optimus", "playground")
		assert.Nil(t, err)

		ds2, err := resource.DataSetFrom(resource.BigQuery, "t-optimus-1", "playground")
		assert.Nil(t, err)

		assert.False(t, ds1.IsSame(ds2))
	})
}

func TestDatasetDetails(t *testing.T) {
	t.Run("returns dataset details", func(t *testing.T) {
		ds, err := resource.DataSetFrom(resource.BigQuery, "t-optimus", "playground")
		assert.Nil(t, err)

		datasetDetails := resource.DatasetDetails{
			Dataset:     ds,
			Description: "description",
			ExtraConfig: nil,
		}

		assert.Equal(t, "t-optimus.playground", datasetDetails.FullName())
		assert.Equal(t, "bigquery://t-optimus:playground", datasetDetails.URN())
		assert.Nil(t, datasetDetails.Validate())
	})
}
