package bigquery_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/odpf/optimus/core/resource"
	"github.com/odpf/optimus/core/tenant"
	"github.com/odpf/optimus/ext/store/bigquery"
)

func TestDatasetDetails(t *testing.T) {
	t.Run("returns dataset details", func(t *testing.T) {
		datasetDetails := bigquery.DatasetDetails{
			Name:        "t-optimus.playground",
			Description: "description",
			ExtraConfig: nil,
		}

		assert.Equal(t, "t-optimus.playground", datasetDetails.FullName())
		assert.Nil(t, datasetDetails.Validate())
	})
}

func TestDataSet(t *testing.T) {
	t.Run("when invalid", func(t *testing.T) {
		t.Run("returns error on empty database/project name", func(t *testing.T) {
			_, err := bigquery.DataSetFrom("", "schema")
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "bigquery project name is empty")
		})
		t.Run("returns error on empty schema/dataset name", func(t *testing.T) {
			_, err := bigquery.DataSetFrom("t-optimus", "")
			assert.NotNil(t, err)
			assert.ErrorContains(t, err, "bigquery dataset name is empty")
		})
	})
	t.Run("when invalid", func(t *testing.T) {
		t.Run("creates dataset", func(t *testing.T) {
			ds, err := bigquery.DataSetFrom("t-optimus", "playground")
			assert.Nil(t, err)

			assert.Equal(t, "t-optimus.playground", ds.FullName())
		})
	})
	t.Run("DatasetFor", func(t *testing.T) {
		bqStore := resource.Bigquery
		tnnt, _ := tenant.NewTenant("proj", "ns")
		metadata := resource.Metadata{
			Version:     1,
			Description: "resource description",
			Labels:      map[string]string{"owner": "optimus"},
		}
		spec := map[string]any{"description": []string{"a", "b"}}
		res, err := resource.NewResource("proj.dataset.extTable1", resource.KindExternalTable, bqStore, tnnt, &metadata, spec)
		assert.Nil(t, err)

		t.Run("returns error when name not valid", func(t *testing.T) {
			invalidRes, err := resource.NewResource("proj.", resource.KindExternalTable, bqStore, tnnt, &metadata, spec)
			assert.Nil(t, err)

			_, err = bigquery.DataSetFor(invalidRes)
			assert.Error(t, err)
		})
		t.Run("returns dataset when valid", func(t *testing.T) {
			ds, err := bigquery.DataSetFor(res)
			assert.NoError(t, err)

			assert.Equal(t, "proj", ds.Project)
			assert.Equal(t, "dataset", ds.DatasetName)
		})
	})
}

func TestResourceName(t *testing.T) {
	bqStore := resource.Bigquery
	tnnt, _ := tenant.NewTenant("proj", "ns")
	metadata := resource.Metadata{
		Version:     1,
		Description: "resource description",
		Labels:      map[string]string{"owner": "optimus"},
	}
	spec := map[string]any{"description": []string{"a", "b"}}
	res, err := resource.NewResource("proj.dataset.extTable1", resource.KindExternalTable, bqStore, tnnt, &metadata, spec)
	assert.Nil(t, err)

	t.Run("for dataset", func(t *testing.T) {
		t.Run("return error when invalid", func(t *testing.T) {
			dsRes, dsErr := resource.NewResource("proj", resource.KindDataset, bqStore, tnnt, &metadata, spec)
			assert.NoError(t, dsErr)

			_, err := bigquery.ResourceNameFor(dsRes)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "invalid resource name: proj")
		})
		t.Run("name when valid", func(t *testing.T) {
			dsRes, dsErr := resource.NewResource("proj.dataset", resource.KindDataset, bqStore, tnnt, &metadata, spec)
			assert.NoError(t, dsErr)

			name, err := bigquery.ResourceNameFor(dsRes)
			assert.NoError(t, err)
			assert.Equal(t, "dataset", name)
		})
	})
	t.Run("for other resources", func(t *testing.T) {
		t.Run("return error when invalid", func(t *testing.T) {
			dsRes, dsErr := resource.NewResource("proj.dataset", resource.KindTable, bqStore, tnnt, &metadata, spec)
			assert.NoError(t, dsErr)

			_, err := bigquery.ResourceNameFor(dsRes)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "invalid resource name")
		})
		t.Run("name when valid", func(t *testing.T) {
			name, err := bigquery.ResourceNameFor(res)
			assert.NoError(t, err)
			assert.Equal(t, "extTable1", name)
		})
	})
}

func TestValidateName(t *testing.T) {
	bqStore := resource.Bigquery
	tnnt, _ := tenant.NewTenant("proj", "ns")
	metadata := resource.Metadata{
		Version:     1,
		Description: "resource description",
		Labels:      map[string]string{"owner": "optimus"},
	}
	spec := map[string]any{"description": []string{"a", "b"}}

	t.Run("when invalid", func(t *testing.T) {
		t.Run("return error for not enough sections", func(t *testing.T) {
			res, err := resource.NewResource("proj", resource.KindDataset, bqStore, tnnt, &metadata, spec)
			assert.NoError(t, err)

			err = bigquery.ValidateName(res)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "invalid sections in name: proj")
		})
		t.Run("return error for invalid character in project name", func(t *testing.T) {
			res, err := resource.NewResource("proj@.dataset", resource.KindDataset, bqStore, tnnt, &metadata, spec)
			assert.NoError(t, err)

			err = bigquery.ValidateName(res)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "invalid character in project name proj@.dataset")
		})
		t.Run("return error for invalid character in dataset name", func(t *testing.T) {
			res, err := resource.NewResource("p-project.data-set", resource.KindDataset, bqStore, tnnt, &metadata, spec)
			assert.NoError(t, err)

			err = bigquery.ValidateName(res)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "invalid character in dataset name p-project.data-set")
		})
		t.Run("returns error when sections in table name are less", func(t *testing.T) {
			res, err := resource.NewResource("p-project.dataset", resource.KindTable, bqStore, tnnt, &metadata, spec)
			assert.NoError(t, err)

			err = bigquery.ValidateName(res)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "invalid resource name sections: p-project.dataset")
		})
		t.Run("returns error when table name is invalid", func(t *testing.T) {
			res, err := resource.NewResource("p-project.dataset.tab@tab1", resource.KindTable, bqStore, tnnt, &metadata, spec)
			assert.NoError(t, err)

			err = bigquery.ValidateName(res)
			assert.Error(t, err)
			assert.ErrorContains(t, err, "invalid character in resource name p-project.dataset.tab@tab1")
		})
	})
	t.Run("when valid", func(t *testing.T) {
		t.Run("return no error for dataset", func(t *testing.T) {
			res, err := resource.NewResource("p-project.dataset", resource.KindDataset, bqStore, tnnt, &metadata, spec)
			assert.NoError(t, err)

			err = bigquery.ValidateName(res)
			assert.NoError(t, err)
		})
		t.Run("returns no error when table name is valid", func(t *testing.T) {
			res, err := resource.NewResource("p-project.dataset.tab1", resource.KindTable, bqStore, tnnt, &metadata, spec)
			assert.NoError(t, err)

			err = bigquery.ValidateName(res)
			assert.NoError(t, err)
		})
	})
}
