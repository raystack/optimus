package bigquery

import (
	"context"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/googleapi"
	"github.com/odpf/optimus/models"
)

func TestTable(t *testing.T) {
	testingContext := context.Background()
	testingProject := "project"
	testingDataset := "dataset"
	testingTable := "table"
	errNotFound := &googleapi.Error{
		Code: 404,
	}
	bQTableMetadata := BQTableMetadata{
		Schema: BQSchema{
			{
				Name:   "message",
				Type:   "STRING",
				Mode:   "nullable",
				Schema: BQSchema{},
			},
			{
				Name:   "message_type",
				Type:   "STRING",
				Mode:   "nullable",
				Schema: BQSchema{},
			},
			{
				Name:   "recipient",
				Type:   "STRING",
				Mode:   "repeated",
				Schema: BQSchema{},
			},
			{
				Name:   "time",
				Type:   "TIME",
				Mode:   "nullable",
				Schema: BQSchema{},
			},
		},
		Cluster: &BQClusteringInfo{
			Using: []string{"message_type"},
		},
	}
	bQResource := BQTable{
		Project:  testingProject,
		Dataset:  testingDataset,
		Table:    testingTable,
		Metadata: bQTableMetadata,
	}
	createTableMeta := &bigquery.TableMetadata{
		Name: bQResource.Table,
		Schema: bigquery.Schema{
			{
				Name: "message",
				Type: "STRING",
			},
			{
				Name: "message_type",
				Type: "STRING",
			},
			{
				Name:     "recipient",
				Type:     "STRING",
				Repeated: true,
			},
			{
				Name: "time",
				Type: "TIME",
			},
		},
		Clustering: &bigquery.Clustering{
			Fields: []string{"message_type"},
		},
	}

	t.Run("ensureTable", func(t *testing.T) {
		t.Run("should create table if it does not exist", func(t *testing.T) {
			upsert := false

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQTable.On("Metadata", testingContext).Return((*bigquery.TableMetadata)(nil), errNotFound)
			bQTable.On("Create", testingContext, createTableMeta).Return(nil)

			err := ensureTable(testingContext, bQTable, bQResource, upsert)
			assert.Nil(t, err)
		})
		t.Run("should not do insert nor update if table is exist and not an upsert call", func(t *testing.T) {
			upsert := false

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQTable.On("Metadata", testingContext).Return(createTableMeta, nil)

			err := ensureTable(testingContext, bQTable, bQResource, upsert)
			assert.Nil(t, err)
		})
		t.Run("should return any error encountered, except for an *googleapi.Error{Code: 404}", func(t *testing.T) {
			otherErrors := []error{
				&googleapi.Error{Code: 401},
				errors.New("unexpected"),
			}
			upsert := false
			for _, e := range otherErrors {
				bQTable := new(BqTableMock)
				defer bQTable.AssertExpectations(t)

				bQTable.On("Metadata", testingContext).Return((*bigquery.TableMetadata)(nil), e)
				err := ensureTable(testingContext, bQTable, bQResource, upsert)
				assert.Equal(t, e, err)
			}
		})
		t.Run("should update table if is already exist and an upsert call", func(t *testing.T) {
			upsert := true

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			tableMeta := &bigquery.TableMetadata{
				ETag: "etag-0000",
			}
			updateTableMeta := bigquery.TableMetadataToUpdate{
				Name:   bQResource.Table,
				Schema: createTableMeta.Schema,
			}

			bQTable.On("Metadata", testingContext).Return(tableMeta, nil)
			bQTable.On("Update", testingContext, updateTableMeta, tableMeta.ETag).Return(tableMeta, nil)

			err := ensureTable(testingContext, bQTable, bQResource, upsert)
			assert.Nil(t, err)
		})
		t.Run("should return an error if bigquery field specification is invalid (on create)", func(t *testing.T) {
			upsert := false
			invalidTable := BQTable{
				Project: testingProject,
				Dataset: testingDataset,
				Table:   testingTable,
				Metadata: BQTableMetadata{
					Schema: BQSchema{
						{
							Name: "recipient",
							Type: "STRING",
							Mode: "abcd",
						},
					},
				},
			}

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQTable.On("Metadata", testingContext).Return((*bigquery.TableMetadata)(nil), errNotFound)

			err := ensureTable(testingContext, bQTable, invalidTable, upsert)
			assert.NotNil(t, err)
		})
		t.Run("should return an error if bigquery field specification is invalid (on update)", func(t *testing.T) {
			upsert := true
			invalidTable := BQTable{
				Project: testingProject,
				Dataset: testingDataset,
				Table:   testingTable,
				Metadata: BQTableMetadata{
					Schema: BQSchema{
						{
							Name: "recipient",
							Type: "STRING",
							Mode: "abcd",
						},
					},
				},
			}

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQTable.On("Metadata", testingContext).Return((*bigquery.TableMetadata)(nil), nil)

			err := ensureTable(testingContext, bQTable, invalidTable, upsert)
			assert.NotNil(t, err)
		})
	})

	t.Run("createTable", func(t *testing.T) {
		t.Run("should create table if given valid input", func(t *testing.T) {
			upsert := false
			resourceSpec := models.ResourceSpec{
				Spec: bQResource,
			}

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQDatasetHandle := new(BqDatasetMock)
			defer bQDatasetHandle.AssertExpectations(t)

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQClient.On("DatasetInProject", bQResource.Project, bQResource.Dataset).Return(bQDatasetHandle)
			eTag := "uniqueID"
			datasetMetadata := bqiface.DatasetMetadata{
				DatasetMetadata: bigquery.DatasetMetadata{
					ETag: eTag,
				},
			}
			bQDatasetHandle.On("Metadata", testingContext).Return(&datasetMetadata, nil)
			bQTable.On("Metadata", testingContext).Return((*bigquery.TableMetadata)(nil), errNotFound)
			bQDatasetHandle.On("Table", bQResource.Table).Return(bQTable, nil)
			bQTable.On("Create", testingContext, createTableMeta).Return(nil)

			err := createTable(testingContext, resourceSpec, bQClient, upsert)
			assert.Nil(t, err)
		})
		t.Run("should return error if read BQ table spec is failed", func(t *testing.T) {
			upsert := false
			resourceSpec := models.ResourceSpec{
				Spec: "non bq table",
			}

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			err := createTable(testingContext, resourceSpec, bQClient, upsert)
			assert.NotNil(t, err)
		})
		t.Run("should return error if ensuring dataset is failed", func(t *testing.T) {
			upsert := false
			resourceSpec := models.ResourceSpec{
				Spec: bQResource,
			}

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQDatasetHandle := new(BqDatasetMock)
			defer bQDatasetHandle.AssertExpectations(t)

			bQClient.On("DatasetInProject", bQResource.Project, bQResource.Dataset).Return(bQDatasetHandle)
			bQDatasetHandle.On("Metadata", testingContext).Return((*bqiface.DatasetMetadata)(nil), errors.New("some error"))

			err := createTable(testingContext, resourceSpec, bQClient, upsert)
			assert.NotNil(t, err)
		})
	})

	t.Run("getTable", func(t *testing.T) {
		t.Run("should retrieve BQ table information", func(t *testing.T) {
			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			resourceSpec := models.ResourceSpec{
				Spec: bQResource,
			}
			datasetMetadata := bqiface.DatasetMetadata{
				DatasetMetadata: bigquery.DatasetMetadata{},
			}

			bQDatasetHandle := new(BqDatasetMock)
			defer bQDatasetHandle.AssertExpectations(t)

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQClient.On("DatasetInProject", bQResource.Project, bQResource.Dataset).Return(bQDatasetHandle)
			bQDatasetHandle.On("Metadata", testingContext).Return(&datasetMetadata, nil)
			bQTable.On("Metadata", testingContext).Return(createTableMeta, nil)
			bQDatasetHandle.On("Table", bQResource.Table).Return(bQTable, nil)

			actualResourceSpec, err := getTable(testingContext, resourceSpec, bQClient)
			assert.Equal(t, resourceSpec, actualResourceSpec)
			assert.Nil(t, err)
		})
		t.Run("should return error if read BQ table spec is failed", func(t *testing.T) {
			resourceSpec := models.ResourceSpec{
				Spec: "non bq table",
			}

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			actualResourceSpec, err := getTable(testingContext, resourceSpec, bQClient)
			assert.Equal(t, models.ResourceSpec{}, actualResourceSpec)
			assert.NotNil(t, err)
		})
	})

	t.Run("deleteTable", func(t *testing.T) {
		t.Run("should able to delete table if given bq table", func(t *testing.T) {
			resourceSpec := models.ResourceSpec{
				Spec: bQResource,
			}

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQDatasetHandle := new(BqDatasetMock)
			defer bQDatasetHandle.AssertExpectations(t)

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQClient.On("DatasetInProject", bQResource.Project, bQResource.Dataset).Return(bQDatasetHandle)
			bQDatasetHandle.On("Metadata", testingContext).Return(&bqiface.DatasetMetadata{}, nil)
			bQDatasetHandle.On("Table", bQResource.Table).Return(bQTable, nil)
			bQTable.On("Delete", testingContext).Return(nil)

			err := deleteTable(testingContext, resourceSpec, bQClient)
			assert.Nil(t, err)
		})
		t.Run("should return error if read BQ table spec is failed", func(t *testing.T) {
			resourceSpec := models.ResourceSpec{
				Spec: "non bq table",
			}

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			err := deleteTable(testingContext, resourceSpec, bQClient)
			assert.NotNil(t, err)
		})
		t.Run("should return error if checking dataset metadata is failed", func(t *testing.T) {
			resourceSpec := models.ResourceSpec{
				Spec: bQResource,
			}

			bQClient := new(BqClientMock)
			defer bQClient.AssertExpectations(t)

			bQDatasetHandle := new(BqDatasetMock)
			defer bQDatasetHandle.AssertExpectations(t)

			bQTable := new(BqTableMock)
			defer bQTable.AssertExpectations(t)

			bQClient.On("DatasetInProject", bQResource.Project, bQResource.Dataset).Return(bQDatasetHandle)
			bQDatasetHandle.On("Metadata", testingContext).Return(&bqiface.DatasetMetadata{}, errors.New("some error"))

			err := deleteTable(testingContext, resourceSpec, bQClient)
			assert.NotNil(t, err)
		})
	})
}
